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

	"github.com/dolthub/go-mysql-server/sql/transform"

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
// TODO: as a result of reordering this rule to facilitate join planning, pruneColumns will not be called
// in a scope with a join node.
// TODO: this should be deprecated in favor of pruneTables (which will be renamed pruneColumns)
func pruneColumns(ctx *sql.Context, a *Analyzer, node sql.Node, scope *plan.Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	if !node.Resolved() {
		return node, transform.SameTree, nil
	}

	// Skip pruning columns for insert statements. For inserts involving a select (INSERT INTO table1 SELECT a,b FROM
	// table2), all columns from the select are used for the insert, and error checking for schema compatibility
	// happens at execution time. Otherwise the logic below will convert a Project to a ResolvedTable for the selected
	// table, which can alter the column order of the select.
	switch n := node.(type) {
	case *plan.InsertInto, *plan.CreateTrigger:
		return n, transform.SameTree, nil
	}

	if !pruneColumnsIsSafe(node) {
		a.Log("not pruning columns because it is not safe.")
		return node, transform.SameTree, nil
	}

	columns := columnsUsedByNode(node)
	findUsedColumns(columns, node)

	n, sameC, err := pruneUnusedColumns(a, node, columns)
	if err != nil {
		return nil, transform.SameTree, err
	}

	n, sameSq, err := pruneSubqueries(ctx, a, n, columns)
	if err != nil {
		return nil, transform.SameTree, err
	}

	n, sameFi, err := fixRemainingFieldsIndexes(ctx, a, n, scope)
	return n, sameC && sameSq && sameFi, err
}

func pruneColumnsIsSafe(n sql.Node) bool {
	isSafe := true
	// We do not run pruneColumns if there is a Subquery
	// expression, because the field rewrites and scope handling
	// in here are not principled.
	transform.InspectExpressions(n, func(e sql.Expression) bool {
		if _, ok := e.(*plan.Subquery); ok {
			isSafe = false
		}
		return isSafe
	})
	if !isSafe {
		return false
	}
	// We cannot eliminate projections in RecursiveCte's,
	// they are used implicitly in the recursive execution
	// if not explicitly in the outer scope.
	transform.Inspect(n, func(n sql.Node) bool {
		switch n.(type) {
		case *plan.RecursiveCte, *plan.Update, *plan.JoinNode:
			isSafe = false
		default:
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

func canPruneChild(c transform.Context) bool {
	j, ok := c.Parent.(*plan.JoinNode)
	if !ok {
		return true
	}
	return j.Op.IsPhysical()
}

func pruneSubqueryColumns(
	ctx *sql.Context,
	a *Analyzer,
	n *plan.SubqueryAlias,
	parentColumns usedColumns,
) (sql.Node, transform.TreeIdentity, error) {
	a.Log("pruning columns of subquery with alias %q", n.Name())

	columns := make(usedColumns)

	// The columns coming from the parent have the subquery alias name as the source. We need to find the real table in
	// order to prune the subquery correctly. The columns might also have been renamed.
	tableByCol := make(map[string]tableCol)
	for i, col := range n.Child.Schema() {
		name := col.Name
		if len(n.Columns) > 0 {
			name = n.Columns[i]
		}
		tableByCol[name] = tableCol{table: col.Source, col: col.Name}
	}

	for col := range parentColumns[n.Name()] {
		table, ok := tableByCol[col]
		if !ok {
			return nil, transform.SameTree, fmt.Errorf("this is likely a bug: missing projected column %q on subquery %q", col, n.Name())
		}
		columns.add(table.table, table.col)
	}

	findUsedColumns(columns, n.Child)

	node, sameCols, err := pruneUnusedColumns(a, n.Child, columns)
	if err != nil {
		return nil, transform.SameTree, err
	}

	node, sameSq, err := pruneSubqueries(ctx, a, node, columns)
	if err != nil {
		return nil, transform.SameTree, err
	}

	same := sameCols && sameSq
	if len(n.Columns) > 0 {
		schemaLen := schemaLength(node)
		if schemaLen != len(n.Columns) {
			n = n.WithColumns(n.Columns[:schemaLen])
			same = transform.NewTree
		}
	}

	// There is no need to fix the field indexes after pruning here
	// because the main query will take care of fixing the indexes of all the
	// nodes in the tree.
	if same {
		return n, transform.SameTree, err
	}
	newn, err := n.WithChildren(node)
	return newn, transform.NewTree, err
}

func findUsedColumns(columns usedColumns, n sql.Node) {
	transform.Inspect(n, func(n sql.Node) bool {
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
) (sql.Node, transform.TreeIdentity, error) {
	return transform.NodeWithCtx(n, canPruneChild, func(c transform.Context) (sql.Node, transform.TreeIdentity, error) {
		subq, ok := c.Node.(*plan.SubqueryAlias)
		if !ok {
			return c.Node, transform.SameTree, nil
		}
		return pruneSubqueryColumns(ctx, a, subq, parentColumns)
	})
}

func pruneUnusedColumns(a *Analyzer, n sql.Node, columns usedColumns) (sql.Node, transform.TreeIdentity, error) {
	return transform.NodeWithCtx(n, canPruneChild, func(c transform.Context) (sql.Node, transform.TreeIdentity, error) {
		switch n := c.Node.(type) {
		case *plan.Project:
			return pruneProject(a, n, columns)
		case *plan.GroupBy:
			return pruneGroupBy(a, n, columns)
		default:
			return n, transform.SameTree, nil
		}
	})
}

func pruneProject(a *Analyzer, n *plan.Project, columns usedColumns) (sql.Node, transform.TreeIdentity, error) {
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
		return n.Child, transform.NewTree, nil
	}

	if len(remaining) == len(n.Projections) {
		return n, transform.SameTree, nil
	}
	return plan.NewProject(remaining, n.Child), transform.NewTree, nil
}

func pruneGroupBy(a *Analyzer, n *plan.GroupBy, columns usedColumns) (sql.Node, transform.TreeIdentity, error) {
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
		return n.Child, transform.NewTree, nil
	}

	if len(remaining) == len(n.SelectedExprs) {
		return n, transform.SameTree, nil
	}
	return plan.NewGroupBy(remaining, n.GroupByExprs, n.Child), transform.NewTree, nil
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

// TODO: figure out why FixFieldIndexes cannot be used instead of this,
// otherwise SystemDiff tests break.
func fixRemainingFieldsIndexes(ctx *sql.Context, a *Analyzer, node sql.Node, scope *plan.Scope) (sql.Node, transform.TreeIdentity, error) {
	return transform.NodeWithCtx(node, canPruneChild, func(c transform.Context) (sql.Node, transform.TreeIdentity, error) {
		switch n := c.Node.(type) {
		case sql.SchemaTarget:
			// do nothing, column defaults have already been resolved
			return node, transform.SameTree, nil
		case *plan.SubqueryAlias:
			if !n.OuterScopeVisibility {
				return n, transform.SameTree, nil
			}
			child, same, err := fixRemainingFieldsIndexes(ctx, a, n.Child, scope)
			if err != nil {
				return nil, transform.SameTree, err
			}
			if same {
				return n, transform.SameTree, nil
			}

			node, err := n.WithChildren(child)
			if err != nil {
				return nil, transform.SameTree, err
			}
			return node, transform.NewTree, nil
		case *plan.IndexedTableAccess:
			return node, transform.SameTree, nil
		default:
			if _, ok := n.(sql.Expressioner); !ok {
				return n, transform.SameTree, nil
			}

			indexedCols, err := indexColumns(ctx, a, n, scope)
			if err != nil {
				return nil, transform.SameTree, err
			}

			if len(indexedCols) == 0 {
				return n, transform.SameTree, nil
			}

			// IndexedTableAccess contains expressions in its lookupBuilder that we don't need to fix up, so skip them
			if _, ok := n.(*plan.IndexedTableAccess); ok {
				return n, transform.SameTree, nil
			}

			return transform.OneNodeExprsWithNode(n, func(_ sql.Node, e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
				gf, ok := e.(*expression.GetField)
				if !ok {
					return e, transform.SameTree, nil
				}

				idx, ok := indexedCols[newTableCol(gf.Table(), gf.Name())]
				if !ok {
					return nil, transform.SameTree, sql.ErrTableColumnNotFound.New(gf.Table(), gf.Name())
				}

				if idx.index == gf.Index() {
					return e, transform.SameTree, nil
				}
				return gf.WithIndex(idx.index), transform.NewTree, nil
			})
		}
	})
}
