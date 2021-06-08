// Copyright 2020-2021 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package analyzer

import (
	"reflect"
	"strings"

	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/expression/function/aggregation"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

func resolveHaving(ctx *sql.Context, a *Analyzer, node sql.Node, scope *Scope) (sql.Node, error) {
	return plan.TransformUp(node, func(node sql.Node) (sql.Node, error) {
		having, ok := node.(*plan.Having)
		if !ok {
			return node, nil
		}

		if !having.Child.Resolved() {
			return node, nil
		}

		originalSchema := having.Schema()

		var requiresProjection bool
		if containsAggregation(having.Cond) {
			var err error
			having, requiresProjection, err = replaceAggregations(ctx, having)
			if err != nil {
				return nil, err
			}
		}

		missingCols := findMissingColumns(having, having.Cond)
		// If any columns required by the having aren't available, pull them up.
		if len(missingCols) > 0 {
			var err error
			// TODO: this should be an error for most queries. having expressions must appear in the group-by clause (even
			//  in non-strict mode)
			having, err = pullMissingColumnsUp(having, missingCols)
			if err != nil {
				return nil, err
			}
			requiresProjection = true
		}

		if !requiresProjection {
			return having, nil
		}

		return projectOriginalAggregation(having, originalSchema), nil
	})
}

func findMissingColumns(node sql.Node, expr sql.Expression) []string {
	var schemaCols []string
	for _, col := range node.Schema() {
		schemaCols = append(schemaCols, strings.ToLower(col.Name))
	}

	var missingCols []string
	for _, n := range findExprNameables(expr) {
		name := strings.ToLower(n.Name())
		if !stringContains(schemaCols, name) {
			missingCols = append(missingCols, n.Name())
		}
	}

	return missingCols
}

func projectOriginalAggregation(having *plan.Having, schema sql.Schema) *plan.Project {
	var projection []sql.Expression
	for i, col := range schema {
		projection = append(
			projection,
			expression.NewGetFieldWithTable(i, col.Type, col.Source, col.Name, col.Nullable),
		)
	}

	return plan.NewProject(projection, having)
}

var errHavingChildMissingRef = errors.NewKind("cannot find column %s referenced in HAVING clause in either GROUP BY or its child")

func pullMissingColumnsUp(having *plan.Having, missingCols []string) (*plan.Having, error) {
	groupBy, err := findGroupBy(having)
	if err != nil {
		return nil, err
	}

	schema := groupBy.Child.Schema()
	var newAggregate []sql.Expression
	for _, c := range missingCols {
		idx := -1
		for i, col := range schema {
			if strings.ToLower(c) == strings.ToLower(col.Name) {
				idx = i
				break
			}
		}
		if idx < 0 {
			return nil, errHavingChildMissingRef.New(c)
		}
		col := schema[idx]
		newAggregate = append(
			newAggregate,
			expression.NewGetFieldWithTable(idx, col.Type, col.Source, col.Name, col.Nullable),
		)
	}

	node, err := addColumnsToGroupBy(having, newAggregate)
	if err != nil {
		return nil, err
	}

	return node.(*plan.Having), nil
}

func findGroupBy(n sql.Node) (*plan.GroupBy, error) {
	children := n.Children()
	if len(children) != 1 {
		return nil, errHavingNeedsGroupBy.New()
	}

	if g, ok := children[0].(*plan.GroupBy); ok {
		return g, nil
	}

	return findGroupBy(children[0])
}

func addColumnsToGroupBy(node sql.Node, columns []sql.Expression) (sql.Node, error) {
	switch node := node.(type) {
	case *plan.Project:
		child, err := addColumnsToGroupBy(node.Child, columns)
		if err != nil {
			return nil, err
		}

		var newProjections = make([]sql.Expression, len(columns))
		for i, col := range columns {
			var name = col.String()
			var table string
			if n, ok := col.(sql.Nameable); ok {
				name = n.Name()
			}

			if t, ok := col.(sql.Tableable); ok {
				table = t.Table()
			}

			newProjections[i] = expression.NewGetFieldWithTable(
				len(child.Schema())-len(columns)+i,
				col.Type(),
				table,
				name,
				col.IsNullable(),
			)
		}

		return plan.NewProject(append(node.Projections, newProjections...), child), nil
	case *plan.Filter,
		*plan.Sort,
		*plan.Limit,
		*plan.Offset,
		*plan.Distinct,
		*plan.Having:
		child, err := addColumnsToGroupBy(node.Children()[0], columns)
		if err != nil {
			return nil, err
		}
		return node.WithChildren(child)
	case *plan.GroupBy:
		return plan.NewGroupBy(append(node.SelectedExprs, columns...), node.GroupByExprs, node.Child), nil
	default:
		return nil, errHavingNeedsGroupBy.New()
	}
}

// pushColumnsUp pushes up the group by columns with the given indexes.
// It returns the resultant node, the indexes of those pushed up columns in the
// resultant node and an error, if any.
func pushColumnsUp(node sql.Node, columns []int) (sql.Node, []int, error) {
	switch node := node.(type) {
	case *plan.Project:
		child, columns, err := pushColumnsUp(node.Child, columns)
		if err != nil {
			return nil, nil, err
		}

		var seen = make(map[int]int)
		for i, col := range node.Projections {
			switch col := col.(type) {
			case *expression.Alias:
				if f, ok := col.Child.(*expression.GetField); ok {
					seen[f.Index()] = i
				}
			case *expression.GetField:
				seen[col.Index()] = i
			}
		}

		var newProjections = make([]sql.Expression, len(node.Projections))
		copy(newProjections, node.Projections)
		schema := child.Schema()
		var newColumns []int

		for _, idx := range columns {
			if newIdx, ok := seen[idx]; ok {
				newColumns = append(newColumns, newIdx)
				continue
			}

			col := schema[idx]
			newIdx := len(newProjections)
			newProjections = append(newProjections, expression.NewGetFieldWithTable(
				newIdx,
				col.Type,
				col.Source,
				col.Name,
				col.Nullable,
			))
			newColumns = append(newColumns, newIdx)
		}

		return plan.NewProject(newProjections, child), newColumns, nil
	case *plan.Filter:
		child, columns, err := pushColumnsUp(node.Child, columns)
		if err != nil {
			return nil, nil, err
		}
		return plan.NewFilter(node.Expression, child), columns, nil
	case *plan.Sort:
		child, columns, err := pushColumnsUp(node.Child, columns)
		if err != nil {
			return nil, nil, err
		}
		return plan.NewSort(node.SortFields, child), columns, nil
	case *plan.Limit:
		child, columns, err := pushColumnsUp(node.Child, columns)
		if err != nil {
			return nil, nil, err
		}

		n, err := node.WithChildren(child)
		if err != nil {
			return nil, nil, err
		}
		return n, columns, nil
	case *plan.Offset:
		child, columns, err := pushColumnsUp(node.Child, columns)
		if err != nil {
			return nil, nil, err
		}
		return plan.NewOffset(node.Offset, child), columns, nil
	case *plan.Distinct:
		child, columns, err := pushColumnsUp(node.Child, columns)
		if err != nil {
			return nil, nil, err
		}
		return plan.NewDistinct(child), columns, nil
	case *plan.GroupBy:
		return node, columns, nil
	case *plan.Having:
		child, columns, err := pushColumnsUp(node.Child, columns)
		if err != nil {
			return nil, nil, err
		}
		return plan.NewHaving(node.Cond, child), columns, nil
	default:
		return nil, nil, errHavingNeedsGroupBy.New()
	}
}

func replaceAggregations(ctx *sql.Context, having *plan.Having) (*plan.Having, bool, error) {
	groupBy, err := findGroupBy(having)
	if err != nil {
		return nil, false, err
	}

	var newAggregate []sql.Expression

	var pushUp []int
	var tokenToIdx = make(map[int]int)
	var pushUpToken = -1

	// We need to find all aggregations inside the having condition. The ones
	// that are already present in the group by will be pushed up and the ones
	// that are not, will be added to the group by and pushed up.
	//
	// To push up already existing aggregations we need to change all possible
	// projections between the having and the group by, so we will need to
	// assign some fake token indexes to replace later with the actual column
	// indexes after they have been pushed up. This is because some of these
	// may have already been projected in some projection and we cannot ensure
	// from here what the final index will be.
	cond, err := expression.TransformUp(ctx, having.Cond, func(e sql.Expression) (sql.Expression, error) {
		agg, ok := e.(sql.Aggregation)
		if !ok {
			return e, nil
		}

		for i, expr := range groupBy.SelectedExprs {
			if aggregationEquals(ctx, agg, expr) {
				token := pushUpToken
				pushUpToken--
				pushUp = append(pushUp, i)
				tokenToIdx[token] = len(pushUp) - 1
				return expression.NewGetField(
					token,
					expr.Type(),
					expr.String(),
					expr.IsNullable(),
				), nil
			}
		}

		newAggregate = append(newAggregate, agg)
		return expression.NewGetField(
			len(having.Child.Schema())+len(newAggregate)-1,
			agg.Type(),
			agg.String(),
			agg.IsNullable(),
		), nil
	})
	if err != nil {
		return nil, false, err
	}

	// The new aggregations will be added to the group by and pushed up until
	// the topmost node.
	having = plan.NewHaving(cond, having.Child)
	node, err := addColumnsToGroupBy(having, newAggregate)
	if err != nil {
		return nil, false, err
	}

	// Then, the ones that already existed are pushed up and we get the final
	// indexes at the topmost node (the having) in the same order.
	node, pushedUpColumns, err := pushColumnsUp(node, pushUp)
	if err != nil {
		return nil, false, err
	}

	newSchema := node.Schema()
	requiresProjection := len(newSchema) != len(having.Schema())
	having = node.(*plan.Having)

	// Now, the tokens are replaced with the actual columns, now that we know
	// what the indexes are.
	cond, err = expression.TransformUp(ctx, having.Cond, func(e sql.Expression) (sql.Expression, error) {
		f, ok := e.(*expression.GetField)
		if !ok {
			return e, nil
		}

		idx, ok := tokenToIdx[f.Index()]
		if !ok {
			return e, nil
		}

		idx = pushedUpColumns[idx]
		col := newSchema[idx]
		return expression.NewGetFieldWithTable(idx, col.Type, col.Source, col.Name, col.Nullable), nil
	})
	if err != nil {
		return nil, false, err
	}

	return plan.NewHaving(cond, having.Child), requiresProjection, nil
}

func aggregationEquals(ctx *sql.Context, a, b sql.Expression) bool {
	// First unwrap aliases
	if alias, ok := b.(*expression.Alias); ok {
		b = alias.Child
	} else if alias, ok := a.(*expression.Alias); ok {
		a = alias.Child
	}

	switch a := a.(type) {
	case *aggregation.Count:
		// it doesn't matter what's inside a Count, the result will be
		// the same.
		_, ok := b.(*aggregation.Count)
		return ok
	case *aggregation.CountDistinct:
		// it doesn't matter what's inside a Count, the result will be
		// the same.
		_, ok := b.(*aggregation.CountDistinct)
		return ok
	case *aggregation.Sum:
		b, ok := b.(*aggregation.Sum)
		if !ok {
			return false
		}

		return aggregationChildEquals(ctx, a.Child, b.Child)
	case *aggregation.Avg:
		b, ok := b.(*aggregation.Avg)
		if !ok {
			return false
		}

		return aggregationChildEquals(ctx, a.Child, b.Child)
	case *aggregation.Min:
		b, ok := b.(*aggregation.Min)
		if !ok {
			return false
		}

		return aggregationChildEquals(ctx, a.Child, b.Child)
	case *aggregation.Max:
		b, ok := b.(*aggregation.Max)
		if !ok {
			return false
		}

		return aggregationChildEquals(ctx, a.Child, b.Child)
	case *aggregation.First:
		b, ok := b.(*aggregation.First)
		if !ok {
			return false
		}

		return aggregationChildEquals(ctx, a.Child, b.Child)
	case *aggregation.Last:
		b, ok := b.(*aggregation.Last)
		if !ok {
			return false
		}

		return aggregationChildEquals(ctx, a.Child, b.Child)
	case *aggregation.JSONArrayAgg:
		b, ok := b.(*aggregation.JSONArrayAgg)
		if !ok {
			return false
		}

		return aggregationChildEquals(ctx, a.Child, b.Child)
	default:
		return false
	}
}

// aggregationChildEquals checks if expression a coming from the having
// matches expression b coming from the group by. To do that, columns in
// a need to be replaced to match the ones in b if their name or table and
// name match.
func aggregationChildEquals(ctx *sql.Context, a, b sql.Expression) bool {
	var fieldsByName = make(map[string]sql.Expression)
	var fieldsByTableCol = make(map[tableCol]sql.Expression)
	sql.Inspect(b, func(e sql.Expression) bool {
		gf, ok := e.(*expression.GetField)
		if ok {
			fieldsByTableCol[tableCol{
				strings.ToLower(gf.Table()),
				strings.ToLower(gf.Name()),
			}] = e
			fieldsByName[strings.ToLower(gf.Name())] = e
		}
		return true
	})

	a, err := expression.TransformUp(ctx, a, func(e sql.Expression) (sql.Expression, error) {
		var table, name string
		switch e := e.(type) {
		case column:
			table = strings.ToLower(e.Table())
			name = strings.ToLower(e.Name())
		case *expression.GetField:
			table = strings.ToLower(e.Table())
			name = strings.ToLower(e.Name())
		}

		if table == "" {
			f, ok := fieldsByName[name]
			if !ok {
				return e, nil
			}
			return f, nil
		}

		f, ok := fieldsByTableCol[tableCol{table, name}]
		if !ok {
			return e, nil
		}
		return f, nil
	})
	if err != nil {
		return false
	}

	return reflect.DeepEqual(a, b)
}

var errHavingNeedsGroupBy = errors.NewKind("found HAVING clause with no GROUP BY")
