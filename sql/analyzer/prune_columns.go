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
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

type usedColumns map[string]map[string]struct{}

func (uc usedColumns) add(table, col string) {
	if _, ok := uc[table]; !ok {
		uc[table] = make(map[string]struct{})
	}
	uc[table][col] = struct{}{}
}

func (uc usedColumns) has(table, col string) bool {
	if _, ok := uc[table]; !ok {
		return false
	}
	_, ok := uc[table][col]
	return ok
}

// pruneColumns removes unneeded columns from Project and GroupBy nodes. It also rewrites field indexes as necessary,
// even if no columns were pruned. This is especially important for subqueries -- this function handles fixing field
// indexes when the outer scope schema changes as a result of other analyzer functions.
func pruneColumns(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	if !n.Resolved() {
		return n, nil
	}

	// Skip pruning columns for insert statements. For inserts involving a select (INSERT INTO table1 SELECT a,b FROM
	// table2), all columns from the select are used for the insert, and error checking for schema compatibility
	// happens at execution time. Otherwise the logic below will convert a Project to a ResolvedTable for the selected
	// table, which can alter the column order of the select.
	switch n := n.(type) {
	case *plan.InsertInto, *plan.CreateTrigger:
		return n, nil
	}

	if !pruneColumnsIsSafe(n) {
		a.Log("not pruning columns because it is not safe.")
		return n, nil
	}

	columns := columnsUsedByNode(n)
	findUsedColumns(columns, n)

	n, err := pruneUnusedColumns(a, n, columns)
	if err != nil {
		return nil, err
	}

	n, err = pruneSubqueries(ctx, a, n, columns)
	if err != nil {
		return nil, err
	}

	return fixRemainingFieldsIndexes(ctx, a, n, scope)
}

func pruneColumnsIsSafe(n sql.Node) bool {
	isSafe := true
	// We do not run pruneColumns if there is a Subquery
	// expression, because the field rewrites and scope handling
	// in here are not principled.
	plan.InspectExpressions(n, func(e sql.Expression) bool {
		if _, ok := e.(*plan.Subquery); ok {
			isSafe = false
		}
		return isSafe
	})
	return isSafe
}

func columnsUsedByNode(n sql.Node) usedColumns {
	columns := make(usedColumns)

	for _, col := range n.Schema() {
		columns.add(col.Source, col.Name)
	}

	return columns
}

func canPruneChild(parent, child sql.Node, idx int) bool {
	_, isIndexedJoin := parent.(*plan.IndexedJoin)
	return !isIndexedJoin
}

func pruneSubqueryColumns(
	ctx *sql.Context,
	a *Analyzer,
	n *plan.SubqueryAlias,
	parentColumns usedColumns,
) (sql.Node, error) {
	a.Log("pruning columns of subquery with alias %q", n.Name())

	columns := make(usedColumns)

	// The columns coming from the parent have the subquery alias name as the source. We need to find the real table in
	// order to prune the subquery correctly. The columns might also have been renamed.
	tableByCol := make(map[string]string)
	for i, col := range n.Child.Schema() {
		name := col.Name
		if len(n.Columns) > 0 {
			name = n.Columns[i]
		}
		tableByCol[name] = col.Source
	}

	for col := range parentColumns[n.Name()] {
		table, ok := tableByCol[col]
		if !ok {
			return nil, fmt.Errorf("this is likely a bug: missing projected column %q on subquery %q", col, n.Name())
		}
		columns.add(table, col)
	}

	findUsedColumns(columns, n.Child)

	node, err := pruneUnusedColumns(a, n.Child, columns)
	if err != nil {
		return nil, err
	}

	node, err = pruneSubqueries(ctx, a, node, columns)
	if err != nil {
		return nil, err
	}

	// There is no need to fix the field indexes after pruning here
	// because the main query will take care of fixing the indexes of all the
	// nodes in the tree.

	return n.WithChildren(node)
}

func findUsedColumns(columns usedColumns, n sql.Node) {
	plan.Inspect(n, func(n sql.Node) bool {
		switch n := n.(type) {
		case *plan.Project:
			addUsedProjectColumns(columns, n.Projections)
			return true
		case *plan.GroupBy:
			addUsedProjectColumns(columns, n.SelectedExprs)
			addUsedColumns(columns, n.GroupByExprs)
			return true
		case *plan.SubqueryAlias:
			// TODO: inspect subquery for references to outer scope nodes
			return false
		}

		exp, ok := n.(sql.Expressioner)
		if ok {
			addUsedColumns(columns, exp.Expressions())
		}

		return true
	})
}

func addUsedProjectColumns(columns usedColumns, projection []sql.Expression) {
	var candidates []sql.Expression
	for _, e := range projection {
		sql.Inspect(e, func(e sql.Expression) bool {
			if e == nil {
				return false
			}

			// TODO: not all of the columns mentioned in the subquery are relevant, just the ones that reference the outer scope
			if sub, ok := e.(*plan.Subquery); ok {
				findUsedColumns(columns, sub.Query)
				return false
			}
			// Only check for expressions that are not directly a GetField. This
			// is because in a projection we only care about those that were used
			// to compute new columns, such as aliases and so on. The fields that
			// are just passed up in the tree will already be in some other part
			// if they are really used.
			if _, ok := e.(*expression.GetField); !ok {
				candidates = append(candidates, e)
			}
			return true
		})
	}

	addUsedColumns(columns, candidates)
}

func addUsedColumns(columns usedColumns, exprs []sql.Expression) {
	for _, e := range exprs {
		sql.Inspect(e, func(e sql.Expression) bool {
			if gf, ok := e.(*expression.GetField); ok {
				columns.add(gf.Table(), gf.Name())
			}
			return true
		})
	}
}

func pruneSubqueries(
	ctx *sql.Context,
	a *Analyzer,
	n sql.Node,
	parentColumns usedColumns,
) (sql.Node, error) {
	return plan.TransformUpWithSelector(n, canPruneChild, func(n sql.Node) (sql.Node, error) {
		subq, ok := n.(*plan.SubqueryAlias)
		if !ok {
			return n, nil
		}

		return pruneSubqueryColumns(ctx, a, subq, parentColumns)
	})
}

func pruneUnusedColumns(a *Analyzer, n sql.Node, columns usedColumns) (sql.Node, error) {
	return plan.TransformUpWithSelector(n, canPruneChild, func(n sql.Node) (sql.Node, error) {
		switch n := n.(type) {
		case *plan.Project:
			return pruneProject(a, n, columns), nil
		case *plan.GroupBy:
			return pruneGroupBy(a, n, columns), nil
		default:
			return n, nil
		}
	})
}

func pruneProject(a *Analyzer, n *plan.Project, columns usedColumns) sql.Node {
	var remaining []sql.Expression
	for _, e := range n.Projections {
		if !shouldPruneExpr(e, columns) {
			remaining = append(remaining, e)
		} else {
			a.Log("Pruned project expression %s", e)
		}
	}

	if len(remaining) == 0 {
		a.Log("Replacing empty project %s node with child %s", n, n.Child)
		return n.Child
	}

	return plan.NewProject(remaining, n.Child)
}

func pruneGroupBy(a *Analyzer, n *plan.GroupBy, columns usedColumns) sql.Node {
	var remaining []sql.Expression
	for _, e := range n.SelectedExprs {
		if !shouldPruneExpr(e, columns) {
			remaining = append(remaining, e)
		} else {
			a.Log("Pruned groupby expression %s", e)
		}
	}

	if len(remaining) == 0 {
		a.Log("Replacing empty groupby %s node with child %s", n, n.Child)
		// TODO: this seems wrong, even if all projections are now gone we still need to do a grouping
		return n.Child
	}

	return plan.NewGroupBy(remaining, n.GroupByExprs, n.Child)
}

func shouldPruneExpr(e sql.Expression, cols usedColumns) bool {
	gf, ok := e.(*expression.GetField)
	if !ok {
		return false
	}

	if gf.Table() == "" {
		return false
	}

	return !cols.has(gf.Table(), gf.Name())
}

func fixRemainingFieldsIndexes(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	return plan.TransformUpWithSelector(n, canPruneChild, func(n sql.Node) (sql.Node, error) {
		switch n := n.(type) {
		case *plan.SubqueryAlias:
			child, err := fixRemainingFieldsIndexes(ctx, a, n.Child, nil)
			if err != nil {
				return nil, err
			}

			return n.WithChildren(child)
		default:
			if _, ok := n.(sql.Expressioner); !ok {
				return n, nil
			}

			indexedCols, err := indexColumns(ctx, a, n, scope)
			if err != nil {
				return nil, err
			}

			if len(indexedCols) == 0 {
				return n, nil
			}

			return plan.TransformExpressions(ctx, n, func(e sql.Expression) (sql.Expression, error) {
				gf, ok := e.(*expression.GetField)
				if !ok {
					return e, nil
				}

				idx, ok := indexedCols[newTableCol(gf.Table(), gf.Name())]
				if !ok {
					return nil, sql.ErrTableColumnNotFound.New(gf.Table(), gf.Name())
				}

				return gf.WithIndex(idx.index), nil
			})
		}
	})
}
