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
)

// pushdownSort pushes the Sort node underneath the Project or GroupBy node in the case that columns needed to
// sort would be projected away before sorting. This can also alter the projection in some cases.
func pushdownSort(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	span, _ := ctx.Span("pushdownSort")
	defer span.Finish()

	return plan.TransformUp(n, func(n sql.Node) (sql.Node, error) {
		sort, ok := n.(*plan.Sort)
		if !ok {
			return n, nil
		}

		if !sort.Child.Resolved() {
			return n, nil
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
				} else if !tableColsContains(schemaCols, tableColFromNameable(n)) {
					missingCols = append(missingCols, n.Name())
				}
			}
		}

		// If all the columns required by the order by are available, do nothing about it.
		if len(missingCols) == 0 {
			a.Log("no missing columns, skipping")
			return n, nil
		}

		// If there are no columns required by the order by available, then move the order by
		// below its child.
		if len(colsFromChild) == 0 {
			a.Log("pushing down sort, missing columns: %s", strings.Join(missingCols, ", "))
			return pushSortDown(sort)
		}

		a.Log("fixing sort dependencies, missing columns: %s", strings.Join(missingCols, ", "))

		// If there are some columns required by the order by on the child but some are missing
		// we have to do some more complex logic and split the projection in two.
		return reorderSort(sort, missingCols)
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

func pushSortDown(sort *plan.Sort) (sql.Node, error) {
	switch child := sort.Child.(type) {
	case *plan.Project:
		return plan.NewProject(
			child.Projections,
			plan.NewSort(sort.SortFields, child.Child),
		), nil
	case *plan.GroupBy:
		return plan.NewGroupBy(
			child.SelectedExprs,
			child.GroupByExprs,
			plan.NewSort(sort.SortFields, child.Child),
		), nil
	case *plan.Window:
		return plan.NewWindow(
			child.SelectExprs,
			plan.NewSort(sort.SortFields, child.Child),
		), nil
	case *plan.ResolvedTable:
		return sort, nil
	default:
		children := child.Children()
		if len(children) == 1 {
			newChild, err := pushSortDown(plan.NewSort(sort.SortFields, children[0]))
			if err != nil {
				return nil, err
			}

			return child.WithChildren(newChild)
		}

		// If the child has more than one child we don't know to which side
		// the sort must be pushed down.
		return nil, errSortPushdown.New(child)
	}
}

func resolveOrderByLiterals(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	return plan.TransformUp(n, func(n sql.Node) (sql.Node, error) {
		sort, ok := n.(*plan.Sort)
		if !ok {
			return n, nil
		}

		// wait for the child to be resolved
		if !sort.Child.Resolved() {
			return n, nil
		}

		schema := sort.Child.Schema()
		var (
			fields = make([]sql.SortField, len(sort.SortFields))
		)
		for i, f := range sort.SortFields {
			if lit, ok := f.Column.(*expression.Literal); ok && sql.IsNumber(f.Column.Type()) {
				// it is safe to eval literals with no context and/or row
				v, err := lit.Eval(nil, nil)
				if err != nil {
					return nil, err
				}

				v, err = sql.Int64.Convert(v)
				if err != nil {
					return nil, err
				}

				// column access is 1-indexed
				idx := int(v.(int64)) - 1
				if idx >= len(schema) || idx < 0 {
					return nil, ErrOrderByColumnIndex.New(idx + 1)
				}

				// If there is more than one alias with this name, we can't handle it yet. This is because we rewrite
				// field indexes based on names at various points during the analysis, and we might choose the wrong
				// index at some later step based on name ambiguity.
				// TODO: fix this by not rewriting field indexes based on names anymore
				if columnAliasRepeated(schema, idx) {
					return nil, sql.ErrAmbiguousColumnInOrderBy.New(schema[idx].Name)
				}

				fields[i] = sql.SortField{
					Column:       expression.NewUnresolvedQualifiedColumn(schema[idx].Source, schema[idx].Name),
					Order:        f.Order,
					NullOrdering: f.NullOrdering,
				}

				a.Log("replaced order by column %d with %v", idx+1, schema[idx])
			} else {
				if agg, ok := f.Column.(sql.Aggregation); ok {
					name := agg.String()
					if nameable, ok := f.Column.(sql.Nameable); ok {
						name = nameable.Name()
					}

					fields[i] = sql.SortField{
						Column:       expression.NewUnresolvedColumn(name),
						Order:        f.Order,
						NullOrdering: f.NullOrdering,
					}
				} else {
					fields[i] = f
				}
			}
		}

		return plan.NewSort(fields, sort.Child), nil
	})
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
