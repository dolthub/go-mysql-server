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
	"github.com/dolthub/go-mysql-server/sql/transform"
)

func resolveHaving(ctx *sql.Context, a *Analyzer, node sql.Node, scope *Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	scopeLen := len(scope.Schema())
	return transform.Node(node, func(node sql.Node) (sql.Node, transform.TreeIdentity, error) {
		having, ok := node.(*plan.Having)
		if !ok {
			return node, transform.SameTree, nil
		}

		if !having.Child.Resolved() {
			return node, transform.SameTree, nil
		}

		originalSchema := having.Schema()

		var requiresProjection bool
		//same := sql.SameTree
		if containsAggregation(having.Cond) {
			//same = sql.NewTree
			var err error
			having, requiresProjection, err = replaceAggregations(ctx, having, scopeLen)
			if err != nil {
				return nil, transform.SameTree, err
			}
		}

		missingCols := findMissingColumns(having, scope, having.Cond)
		// If any columns required by the having aren't available, pull them up.
		if len(missingCols) > 0 {
			var err error
			// TODO: this should be an error for most queries. having expressions must appear in the group-by clause (even
			//  in non-strict mode)
			having, err = pullMissingColumnsUp(having, missingCols, scopeLen)
			if err != nil {
				return nil, transform.SameTree, err
			}
			//same = sql.NewTree
			requiresProjection = true
		}

		if !requiresProjection {
			return having, transform.NewTree, nil
		}

		return projectOriginalAggregation(having, originalSchema, scopeLen), transform.NewTree, nil
	})
}

func findMissingColumns(node sql.Node, scope *Scope, expr sql.Expression) map[string]bool {
	var schemaCols []string
	for _, col := range node.Schema() {
		schemaCols = append(schemaCols, strings.ToLower(col.Name))
	}
	for _, col := range scope.Schema() {
		schemaCols = append(schemaCols, strings.ToLower(col.Name))
	}

	var missingCols = make(map[string]bool)
	for _, n := range findExprNameables(expr) {
		name := strings.ToLower(n.Name())
		if !stringContains(schemaCols, name) {
			missingCols[n.Name()] = true
		}
	}

	return missingCols
}

func projectOriginalAggregation(having *plan.Having, schema sql.Schema, scopeLen int) *plan.Project {
	var projection []sql.Expression
	for i, col := range schema {
		projection = append(
			projection,
			expression.NewGetFieldWithTable(scopeLen+i, col.Type, col.Source, col.Name, col.Nullable),
		)
	}

	return plan.NewProject(projection, having)
}

var errHavingChildMissingRef = errors.NewKind("cannot find column %s referenced in HAVING clause in either GROUP BY or its child")

// pullMissingColumnsUp will attempt to find given missing columns. It will traverse on plan.Having node and scan
// its children's schema to find the missing columns. The columns that are found will be added in Projections of
// underlying plan.Project node and SelectExprs of underlying plan.GroupBy node.
func pullMissingColumnsUp(having *plan.Having, missingCols map[string]bool, scopeLen int) (*plan.Having, error) {
	var newAggregate []sql.Expression
	loopSchema := func(schema sql.Schema) {
		for i, c := range schema {
			if _, ok := missingCols[strings.ToLower(c.Name)]; ok {
				col := schema[i]
				delete(missingCols, c.Name)
				newAggregate = append(
					newAggregate,
					expression.NewGetFieldWithTable(scopeLen+i, col.Type, col.Source, col.Name, col.Nullable),
				)
			}
		}
	}

	transform.Inspect(having.Child, func(n sql.Node) bool {
		if n == nil {
			return false
		}
		loopSchema(n.Schema())
		return true
	})

	if len(missingCols) > 0 {
		var cs []string
		for c, _ := range missingCols {
			cs = append(cs, c)
		}
		return nil, errHavingChildMissingRef.New(strings.Join(cs, ", "))
	}

	node, err := addColumnsToGroupByAndProjectNodes(having, newAggregate, scopeLen)
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

// addColumnsToGroupByAndProjectNodes will add the given columns to Projections of every plan.Project and
// SelectExprs of plan.GroupBy nodes to expose these columns to schema of plan.Having node for its condition
// expressions to refer to.
func addColumnsToGroupByAndProjectNodes(node sql.Node, columns []sql.Expression, scopeLen int) (sql.Node, error) {
	switch node := node.(type) {
	case *plan.Project:
		child, err := addColumnsToGroupByAndProjectNodes(node.Child, columns, scopeLen)
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
				scopeLen+len(child.Schema())-len(columns)+i,
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
		child, err := addColumnsToGroupByAndProjectNodes(node.Children()[0], columns, scopeLen)
		if err != nil {
			return nil, err
		}
		return node.WithChildren(child)
	case *plan.GroupBy:
		return plan.NewGroupBy(append(node.SelectedExprs, columns...), node.GroupByExprs, node.Child), nil
	default:
		return node, nil
	}
}

// pushColumnsUp pushes up the group by columns with the given indexes.
// It returns the resultant node, the indexes of those pushed up columns in the
// resultant node and an error, if any.
func pushColumnsUp(node sql.Node, columns []int, scopeLen int) (sql.Node, []int, error) {
	switch node := node.(type) {
	case *plan.Project:
		child, columns, err := pushColumnsUp(node.Child, columns, scopeLen)
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
				scopeLen+newIdx,
				col.Type,
				col.Source,
				col.Name,
				col.Nullable,
			))
			newColumns = append(newColumns, newIdx)
		}

		return plan.NewProject(newProjections, child), newColumns, nil
	case *plan.Filter:
		child, columns, err := pushColumnsUp(node.Child, columns, scopeLen)
		if err != nil {
			return nil, nil, err
		}
		return plan.NewFilter(node.Expression, child), columns, nil
	case *plan.Sort:
		child, columns, err := pushColumnsUp(node.Child, columns, scopeLen)
		if err != nil {
			return nil, nil, err
		}
		return plan.NewSort(node.SortFields, child), columns, nil
	case *plan.Limit:
		child, columns, err := pushColumnsUp(node.Child, columns, scopeLen)
		if err != nil {
			return nil, nil, err
		}

		n, err := node.WithChildren(child)
		if err != nil {
			return nil, nil, err
		}
		return n, columns, nil
	case *plan.Offset:
		child, columns, err := pushColumnsUp(node.Child, columns, scopeLen)
		if err != nil {
			return nil, nil, err
		}
		return plan.NewOffset(node.Offset, child), columns, nil
	case *plan.Distinct:
		child, columns, err := pushColumnsUp(node.Child, columns, scopeLen)
		if err != nil {
			return nil, nil, err
		}
		return plan.NewDistinct(child), columns, nil
	case *plan.GroupBy:
		return node, columns, nil
	case *plan.Having:
		child, columns, err := pushColumnsUp(node.Child, columns, scopeLen)
		if err != nil {
			return nil, nil, err
		}
		return plan.NewHaving(node.Cond, child), columns, nil
	default:
		return nil, nil, errHavingNeedsGroupBy.New()
	}
}

func replaceAggregations(ctx *sql.Context, having *plan.Having, scopeLen int) (*plan.Having, bool, error) {
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
	cond, _, err := transform.Expr(having.Cond, func(e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
		agg, ok := e.(sql.Aggregation)
		if !ok {
			return e, transform.SameTree, nil
		}

		for i, expr := range groupBy.SelectedExprs {
			if aggregationEquals(ctx, agg, expr) {
				token := pushUpToken
				pushUpToken--
				pushUp = append(pushUp, i)
				tokenToIdx[token] = len(pushUp) - 1
				return expression.NewGetField(
					scopeLen+token,
					expr.Type(),
					expr.String(),
					expr.IsNullable(),
				), transform.NewTree, nil
			}
		}

		newAggregate = append(newAggregate, agg)
		return expression.NewGetField(
			scopeLen+len(having.Child.Schema())+len(newAggregate)-1,
			agg.Type(),
			agg.String(),
			agg.IsNullable(),
		), transform.NewTree, nil
	})
	if err != nil {
		return nil, false, err
	}

	// The new aggregations will be added to the group by and pushed up until
	// the topmost node.
	having = plan.NewHaving(cond, having.Child)
	node, err := addColumnsToGroupByAndProjectNodes(having, newAggregate, scopeLen)
	if err != nil {
		return nil, false, err
	}

	// Then, the ones that already existed are pushed up and we get the final
	// indexes at the topmost node (the having) in the same order.
	node, pushedUpColumns, err := pushColumnsUp(node, pushUp, scopeLen)
	if err != nil {
		return nil, false, err
	}

	newSchema := node.Schema()
	requiresProjection := len(newSchema) != len(having.Schema())
	having = node.(*plan.Having)

	// Now, the tokens are replaced with the actual columns, now that we know
	// what the indexes are.
	cond, _, err = transform.Expr(having.Cond, func(e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
		f, ok := e.(*expression.GetField)
		if !ok {
			return e, transform.SameTree, nil
		}

		idx, ok := tokenToIdx[f.Index()]
		if !ok {
			return e, transform.SameTree, nil
		}

		idx = pushedUpColumns[idx]
		col := newSchema[idx]
		return expression.NewGetFieldWithTable(idx, col.Type, col.Source, col.Name, col.Nullable), transform.NewTree, nil
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
	case *aggregation.JsonArray:
		b, ok := b.(*aggregation.JsonArray)
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

	a, _, err := transform.Expr(a, func(e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
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
				return e, transform.SameTree, nil
			}
			return f, transform.NewTree, nil
		}

		f, ok := fieldsByTableCol[tableCol{table, name}]
		if !ok {
			return e, transform.SameTree, nil
		}
		return f, transform.NewTree, nil
	})
	if err != nil {
		return false
	}

	return reflect.DeepEqual(a, b)
}

var errHavingNeedsGroupBy = errors.NewKind("found HAVING clause with no GROUP BY")
