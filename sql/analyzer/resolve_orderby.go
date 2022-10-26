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
	"strings"

	errors "gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
)

// pushdownSort pushes the Sort node underneath the Project or GroupBy node in the case that columns needed to
// sort would be projected away before sorting. This can also alter the projection in some cases.
func pushdownSort(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	span, ctx := ctx.Span("pushdownSort")
	defer span.End()

	return transform.Node(n, func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		sort, ok := n.(*plan.Sort)
		if !ok {
			return n, transform.SameTree, nil
		}

		if !sort.Child.Resolved() {
			return n, transform.SameTree, nil
		}

		if sort.Child == plan.EmptyTable {
			return n, transform.SameTree, nil
		}

		childAliases := aliasesDefinedInNode(sort.Child)
		var schemaCols []tableCol
		for _, col := range sort.Child.Schema() {
			schemaCols = append(schemaCols, tableCol{
				table: strings.ToLower(col.Source),
				col:   strings.ToLower(col.Name),
			})
		}

		var colsFromChild []string
		var missingCols []string
		for _, f := range sort.SortFields {
			ns := findExprNameables(f.Column)

			for _, n := range ns {
				name := strings.ToLower(n.Name())
				if stringContains(childAliases, name) {
					colsFromChild = append(colsFromChild, n.Name())
				} else {
					col := tableColFromNameable(n)
					if !tableColsContains(schemaCols, col) {
						missingCols = append(missingCols, strings.ToLower(col.String()))
					}
				}
			}
		}

		// If all the columns required by the order by are available, do nothing about it.
		if len(missingCols) == 0 {
			a.Log("no missing columns, skipping")
			return n, transform.SameTree, nil
		}

		// If all the missing expressions are aliased, swap in the alias name and don't move the sort node
		expressionToAliasName := aliasedExpressionsInNode(sort.Child)
		if allMissingColsAreAliasedExpressions(expressionToAliasName, missingCols) {
			a.Log("swapping in alias names for missing columns: %s", strings.Join(missingCols, ", "))
			return replaceOrderByExpressionsWithAliasReferences(sort, expressionToAliasName, missingCols)
		}

		// If there are no columns required by the order by available, then move the order by below its child.
		if len(colsFromChild) == 0 {
			a.Log("pushing down sort, missing columns: %s", strings.Join(missingCols, ", "))
			return pushSortDown(sort)
		}

		a.Log("fixing sort dependencies, missing columns: %s", strings.Join(missingCols, ", "))

		// If there are some columns required by the order by on the child but some are missing
		// we have to do some more complex logic and split the projection in two.
		n, err := reorderSort(sort, missingCols)
		return n, transform.NewTree, err
	})
}

// reorderSort replaces the sort node by adding necessary missing columns to the child node and then reordering the
// sort with its child:
// sort(project(a)) becomes project(sort(project(a)))
// sort(groupBy(a)) becomes project(sort(groupby(a)))
func reorderSort(sort *plan.Sort, missingCols []string) (sql.Node, error) {
	var expressions []sql.Expression
	switch child := sort.Child.(type) {
	case *plan.Project:
		expressions = child.Projections
	case *plan.GroupBy:
		expressions = child.SelectedExprs
	case *plan.Window:
		expressions = child.SelectExprs
	default:
		return nil, errSortPushdown.New(child)
	}

	var newExpressions = append([]sql.Expression{}, expressions...)
	for _, col := range missingCols {
		newExpressions = append(newExpressions, expression.NewUnresolvedColumn(col))
	}

	for i, e := range expressions {
		var name string
		if n, ok := e.(sql.Nameable); ok {
			name = n.Name()
		} else {
			name = e.String()
		}

		var table string
		if t, ok := e.(sql.Tableable); ok {
			table = t.Table()
		}
		expressions[i] = expression.NewGetFieldWithTable(
			i, e.Type(), table, name, e.IsNullable(),
		)
	}

	switch child := sort.Child.(type) {
	case *plan.Project:
		return plan.NewProject(
			expressions,
			plan.NewSort(
				sort.SortFields,
				plan.NewProject(newExpressions, child.Child),
			),
		), nil
	case *plan.GroupBy:
		return plan.NewProject(
			expressions,
			plan.NewSort(
				sort.SortFields,
				plan.NewGroupBy(newExpressions, child.GroupByExprs, child.Child),
			),
		), nil
	case *plan.Window:
		return plan.NewProject(
			expressions,
			plan.NewSort(
				sort.SortFields,
				plan.NewWindow(newExpressions, child.Child),
			),
		), nil
	default:
		return nil, errSortPushdown.New(child)
	}
}

var errSortPushdown = errors.NewKind("unable to push plan.Sort node below %T")

func pushSortDown(sort *plan.Sort) (sql.Node, transform.TreeIdentity, error) {
	switch child := sort.Child.(type) {
	case *plan.Project:
		return plan.NewProject(
			child.Projections,
			plan.NewSort(sort.SortFields, child.Child),
		), transform.NewTree, nil
	case *plan.GroupBy:
		return plan.NewGroupBy(
			child.SelectedExprs,
			child.GroupByExprs,
			plan.NewSort(sort.SortFields, child.Child),
		), transform.NewTree, nil
	case *plan.Window:
		return plan.NewWindow(
			child.SelectExprs,
			plan.NewSort(sort.SortFields, child.Child),
		), transform.NewTree, nil
	case *plan.ResolvedTable, *plan.Union:
		return sort, transform.SameTree, nil
	default:
		children := child.Children()
		if len(children) == 1 {
			newChild, same, err := pushSortDown(plan.NewSort(sort.SortFields, children[0]))
			if err != nil {
				return nil, transform.SameTree, err
			}
			if same {
				return sort, transform.SameTree, nil
			}
			child, err = child.WithChildren(newChild)
			if err != nil {
				return nil, transform.SameTree, err
			}
			return child, transform.NewTree, nil
		}

		// If the child has more than one child we don't know to which side
		// the sort must be pushed down.
		return nil, transform.SameTree, errSortPushdown.New(child)
	}
}

// allMissingColsAreAliasedExpressions returns true if all the missing expression strings in |missingCols| are defined
// as aliases in the map of aliased expressions to their alias name in |expressionToAliasName|.
func allMissingColsAreAliasedExpressions(expressionToAliasName map[string]string, missingCols []string) bool {
	for _, missingCol := range missingCols {
		if _, ok := expressionToAliasName[missingCol]; !ok {
			return false
		}
	}

	return true
}

// replaceOrderByExpressionsWithAliasReferences transforms the specified |sort| node, by replacing the specified
// expression strings in |missingCols| with their aliased names from |expressionToAliasName|.
func replaceOrderByExpressionsWithAliasReferences(sort *plan.Sort, expressionToAliasName map[string]string, missingCols []string) (sql.Node, transform.TreeIdentity, error) {
	var newSortFields []sql.Expression
	var replacedCols []string
	for i, sortField := range sort.SortFields {
		exprString := strings.ToLower(sortField.Column.String())

		// if exprString is one of our missing columns and there's an alias we can reference, swap it in
		if stringContains(missingCols, exprString) {
			if aliasName, ok := expressionToAliasName[exprString]; ok {
				if newSortFields == nil {
					newSortFields = make([]sql.Expression, len(sort.SortFields))
					copy(newSortFields, sort.SortFields.ToExpressions())
				}
				newSortFields[i] = expression.NewAliasReference(aliasName)
				replacedCols = append(replacedCols, exprString)
			}
		}
	}

	newSort, err := sort.WithExpressions(newSortFields...)
	if err != nil {
		return sort, transform.SameTree, err
	}
	return newSort, transform.NewTree, nil
}

func resolveOrderByLiterals(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	return transform.Node(n, func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		sort, ok := n.(*plan.Sort)
		if !ok {
			return n, transform.SameTree, nil
		}

		// wait for the child to be resolved
		if !sort.Child.Resolved() {
			return n, transform.SameTree, nil
		}

		fields, same, err := resolveSortFields(a, sort.SortFields, sort.Child.Schema())
		if err != nil {
			return n, transform.SameTree, err
		}
		if same {
			return sort, transform.SameTree, nil
		}
		return plan.NewSort(fields, sort.Child), transform.NewTree, nil
	})
}

func resolveSortFields(a *Analyzer, sfs sql.SortFields, schema sql.Schema) (sql.SortFields, transform.TreeIdentity, error) {
	ret := make([]sql.SortField, len(sfs))
	same := transform.SameTree
	var err error
	sameF := transform.SameTree
	for i, f := range sfs {
		ret[i], sameF, err = resolveSortField(a, f, schema)
		if err != nil {
			return nil, transform.SameTree, err
		}
		same = same && sameF
	}
	return ret, same, nil
}

func resolveSortField(a *Analyzer, f sql.SortField, schema sql.Schema) (sql.SortField, transform.TreeIdentity, error) {
	if lit, ok := f.Column.(*expression.Literal); ok && sql.IsNumber(f.Column.Type()) {
		v, err := lit.Eval(nil, nil)
		if err != nil {
			return sql.SortField{}, transform.SameTree, err
		}

		v, err = sql.Int64.Convert(v)
		if err != nil {
			return sql.SortField{}, transform.SameTree, err
		}

		// column access is 1-indexed
		idx := int(v.(int64)) - 1
		if idx >= len(schema) || idx < 0 {
			return sql.SortField{}, transform.SameTree, ErrOrderByColumnIndex.New(idx + 1)

		}

		// If there is more than one alias with this name, we can't handle it yet. This is because we rewrite
		// field indexes based on names at various points during the analysis, and we might choose the wrong
		// index at some later step based on name ambiguity.
		// TODO: fix this by not rewriting field indexes based on names anymore
		if columnAliasRepeated(schema, idx) {
			return sql.SortField{}, transform.SameTree, sql.ErrAmbiguousColumnInOrderBy.New(schema[idx].Name)
		}
		uc := expression.NewUnresolvedQualifiedColumn(schema[idx].Source, schema[idx].Name)
		return sql.SortField{
			Column:       uc,
			Column2:      uc,
			Order:        f.Order,
			NullOrdering: f.NullOrdering,
		}, transform.NewTree, nil
		a.Log("replaced order by column %d with %v", idx+1, schema[idx])
	} else if agg, ok := f.Column.(sql.Aggregation); ok {
		name := agg.String()
		if nameable, ok := f.Column.(sql.Nameable); ok {
			name = nameable.Name()
		}
		uc := expression.NewUnresolvedColumn(name)
		return sql.SortField{
			Column:       uc,
			Column2:      uc,
			Order:        f.Order,
			NullOrdering: f.NullOrdering,
		}, transform.NewTree, nil
	}
	return f, transform.SameTree, nil
}

// columnAliasRepeated returns whether the column in the schema given with the index given is an alias that is repeated
// elsewhere in the schema, making it ambiguous
func columnAliasRepeated(cols sql.Schema, idx int) bool {
	target := cols[idx]
	// this analysis doesn't apply to qualified columns
	if len(target.Source) > 0 {
		return false
	}
	for i, col := range cols {
		if i == idx {
			continue
		}
		if len(col.Source) > 0 {
			continue
		}
		if strings.ToLower(target.Name) == strings.ToLower(col.Name) {
			return true
		}
	}
	return false
}

func findExprNameables(e sql.Expression) []sql.Nameable {
	var result []sql.Nameable
	sql.Inspect(e, func(e sql.Expression) bool {
		n, ok := e.(sql.Nameable)
		if ok {
			result = append(result, n)
			return false
		}
		return true
	})
	return result
}

func tableColFromNameable(n sql.Nameable) tableCol {
	var tbl string
	if t, ok := n.(sql.Tableable); ok {
		tbl = t.Table()
	}
	return newTableCol(tbl, n.Name())
}
