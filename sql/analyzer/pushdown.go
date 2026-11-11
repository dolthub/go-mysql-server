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
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
	"strings"
)

// pushFilters moves filter nodes down to their appropriate relations.
// Filters that reference a single relation will wrap their target tables.
// as is appropriate. We never move a filter without deleting from the source.
// Related rules: hoistOutOfScopeFilters, pushdownSubqueryAliasFilters
func pushFilters(ctx *sql.Context, a *Analyzer, n sql.Node, scope *plan.Scope, sel RuleSelector, qFlags *sql.QueryFlags) (sql.Node, transform.TreeIdentity, error) {
	span, ctx := ctx.Span("push_filters")
	defer span.End()

	if !canDoPushdown(n) {
		return n, transform.SameTree, nil
	}
	filters := newEmptyFilterSet(getProjectionExpressions(n))

	return pushdownFiltersAboveTables(ctx, a, n, scope, filters)
}

func pushdownFiltersAboveTables(ctx *sql.Context, a *Analyzer, n sql.Node, scope *plan.Scope, parentFilters *filterSet) (sql.Node, transform.TreeIdentity, error) {
	if sql.IsOpaque(n) {
		return n, transform.SameTree, nil
	}

	switch node := n.(type) {
	case *plan.Filter:
		filterExpressions := expression.SplitConjunction(ctx, node.Expression)
		filters := parentFilters.newChildFilterSet(ctx, filterExpressions, scope)

		child, childSame, err := pushdownFiltersAboveTables(ctx, a, node.Child, scope, filters)
		if err != nil {
			return node, transform.SameTree, err
		}
		parentFilters.markFiltersHandled(filters.handledFilters...)
		return updateFilterNode(ctx, a, node, child, childSame, filterExpressions, filters)
	case *plan.JoinNode:
		joinOp := node.JoinType()
		isLeftOuterOrAnti := joinOp.IsLeftOuter() || joinOp.IsAnti()
		isFullOuterOrMerge := joinOp.IsMerge() || joinOp.IsFullOuter()

		leftChild := node.Left()
		rightChild := node.Right()
		leftSame := transform.SameTree
		rightSame := transform.SameTree
		joinCondition := node.JoinCond()
		var err error

		if isLeftOuterOrAnti || isFullOuterOrMerge {
			if isLeftOuterOrAnti {
				// Parent filters can be pushed down into the left child of a left outer or anti join
				leftChild, leftSame, err = pushdownFiltersAboveTables(ctx, a, leftChild, scope, parentFilters)
			} else {
				// In a full outer or merge join, parent filters should not be pushed down, but any subsequent child
				// filters should still get pushed down
				leftChild, leftSame, err = pushdownFiltersAboveTables(ctx, a, leftChild, scope, newEmptyFilterSet(parentFilters.projectionExpressions))
			}
			if err != nil {
				return node, transform.SameTree, err
			}
			// Do not push down parent filters into right child, but any subsequent child filters should still get
			// pushed down to the table level
			rightChild, rightSame, err = pushdownFiltersAboveTables(ctx, a, rightChild, scope, newEmptyFilterSet(parentFilters.projectionExpressions))
			if err != nil {
				return node, transform.SameTree, err
			}
		} else {
			filterExpressions := expression.SplitConjunction(ctx, node.Filter)
			filters := parentFilters.newChildFilterSet(ctx, filterExpressions, scope)
			leftChild, leftSame, err = pushdownFiltersAboveTables(ctx, a, leftChild, scope, filters)
			if err != nil {
				return node, transform.SameTree, err
			}
			rightChild, rightSame, err = pushdownFiltersAboveTables(ctx, a, rightChild, scope, filters)
			if err != nil {
				return node, transform.SameTree, err
			}

			parentFilters.markFiltersHandled(filters.handledFilters...)
			unhandled := subtractExprSet(filterExpressions, filters.handledFilters)
			joinCondition = expression.JoinAnd(unhandled...)
		}

		// TODO: Certain parent filters that only reference tables in the join children can also be added to the join
		//  condition, thus transforming the join node regardless if the children stayed the same. To do this, we would
		//  need to be able to fetch filters that reference multiple tables and we would also need to what the child
		//  tables are.
		if leftSame && rightSame {
			return node, transform.SameTree, nil
		}

		return plan.NewJoin(ctx, leftChild, rightChild, joinOp, joinCondition).WithComment(node.Comment()), transform.NewTree, nil
	case *plan.TableAlias, *plan.ResolvedTable, *plan.ValueDerivedTable:
		return filteredTableNode(ctx, a, node.(plan.TableIdNode), parentFilters)
	case *plan.Limit, *plan.Window:
		// Parent filters shouldn't be pushed through Limit or Window nodes but child filters should still get pushed
		// down to the table level.
		newChild, same, err := pushdownFiltersAboveTables(ctx, a, node.Children()[0], scope, newEmptyFilterSet(parentFilters.projectionExpressions))
		if err != nil {
			return node, transform.SameTree, err
		}
		if !same {
			newNode, err := node.WithChildren(ctx, newChild)
			return newNode, transform.NewTree, err
		}
	default:
		children := node.Children()
		allSame := true
		for i, child := range children {
			newChild, same, err := pushdownFiltersAboveTables(ctx, a, child, scope, parentFilters)
			if err != nil {
				return node, transform.SameTree, err
			}
			if !same {
				children[i] = newChild
				allSame = false
			}
		}
		if !allSame {
			newNode, err := node.WithChildren(ctx, children...)
			return newNode, transform.NewTree, err
		}
	}
	return n, transform.SameTree, nil
}

// pushdownSubqueryAliasFilters attempts to push conditions in filters down to
// individual subquery aliases.
func pushdownSubqueryAliasFilters(ctx *sql.Context, a *Analyzer, n sql.Node, scope *plan.Scope, sel RuleSelector, qFlags *sql.QueryFlags) (sql.Node, transform.TreeIdentity, error) {
	span, ctx := ctx.Span("pushdown_subquery_alias_filters")
	defer span.End()

	if !canDoPushdown(n) {
		return n, transform.SameTree, nil
	}

	if !hasSubqueryAlias(ctx, n) {
		return n, transform.SameTree, nil
	}

	n, same, err := transformPushdownSubqueryAliasFilters(ctx, a, n, scope)
	if err != nil {
		return n, transform.SameTree, err
	}
	n, sameJoins, err := pushJoinKeysIntoUnions(ctx, n)
	return n, same && sameJoins, err
}

// pushJoinKeysIntoUnions turns an inner join of a table to a view or derived table over a UNION into a lateral join,
// when the join equates union columns with expressions of the left side's columns. Those equalities move into each
// branch (pushdownFilterIntoUnion), so each left row reads every branch through an index, where the join would
// otherwise scan every branch in full to build its hash table. It applies only when, in every branch, an index starts
// with each pushed column, so the lateral side is a set of index lookups; otherwise the join is left as it is.
func pushJoinKeysIntoUnions(ctx *sql.Context, n sql.Node) (sql.Node, transform.TreeIdentity, error) {
	return transform.Node(ctx, n, func(ctx *sql.Context, n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		j, ok := n.(*plan.JoinNode)
		if !ok || !j.Op.IsInner() || j.Op.IsLateral() || j.Filter == nil {
			return n, transform.SameTree, nil
		}
		sa, ok := j.Right().(*plan.SubqueryAlias)
		if !ok || sa.IsLateral || sa.ScopeMapping == nil {
			return n, transform.SameTree, nil
		}
		// filters the union could not take stay above it, inside the view
		var above []*plan.Filter
		child := sa.Child
		for {
			f, ok := child.(*plan.Filter)
			if !ok {
				break
			}
			above, child = append(above, f), f.Child
		}
		u, ok := child.(*plan.SetOp)
		if !ok {
			return n, transform.SameTree, nil
		}
		lCols, rCols := j.Left().Schema(ctx), sa.Schema(ctx)
		var remaining []sql.Expression
		outer := sa.Correlated
		pushed := false
		for _, c := range expression.SplitConjunction(ctx, j.Filter) {
			eq, isEq := c.(*expression.Equals)
			if !isEq {
				remaining = append(remaining, c)
				continue
			}
			var view *expression.GetField
			var key sql.Expression
			if gf, ok := eq.Left().(*expression.GetField); ok && rCols.Contains(gf.Name(), gf.Table()) {
				view, key = gf, eq.Right()
			} else if gf, ok := eq.Right().(*expression.GetField); ok && rCols.Contains(gf.Name(), gf.Table()) {
				view, key = gf, eq.Left()
			}
			if view == nil {
				remaining = append(remaining, c)
				continue
			}
			viewCol, mapped := sa.ScopeMapping[view.Id()]
			if left, ok := joinSideOf(ctx, key, lCols, rCols); !mapped || !ok || !left {
				remaining = append(remaining, c)
				continue
			}
			keyCols := sql.ColSet{}
			sql.Inspect(ctx, key, func(ctx *sql.Context, e sql.Expression) bool {
				if gf, ok := e.(*expression.GetField); ok {
					keyCols.Add(gf.Id())
				}
				return true
			})
			filter := expression.NewEquals(viewCol, key)
			next, ok := pushdownFilterIntoUnion(ctx, u, filter, outer.Union(keyCols))
			if !ok || !unionBranchesIndexed(ctx, next, filter) {
				remaining = append(remaining, c)
				continue
			}
			u, outer, pushed = next, outer.Union(keyCols), true
		}
		if !pushed {
			return n, transform.SameTree, nil
		}
		var rebuilt sql.Node = u
		for i := len(above) - 1; i >= 0; i-- {
			rebuilt = plan.NewFilter(ctx, above[i].Expression, rebuilt)
		}
		lateral := sa.WithChild(rebuilt).WithCorrelated(outer)
		lateral.IsLateral = true
		op := plan.JoinTypeLateralCross
		var cond sql.Expression
		if len(remaining) > 0 {
			op, cond = plan.JoinTypeLateralInner, expression.JoinAnd(remaining...)
		}
		return plan.NewJoin(ctx, j.Left(), lateral, op, cond).WithComment(j.Comment()), transform.NewTree, nil
	})
}

// unionBranchesIndexed reports whether, in each branch of |u|, the filter placed under the branch's projections
// equates a column with the outer key and that column's table has an index starting with it.
func unionBranchesIndexed(ctx *sql.Context, u *plan.SetOp, filter sql.Expression) bool {
	for _, branch := range []sql.Node{u.Left(), u.Right()} {
		if s, ok := branch.(*plan.SetOp); ok {
			if !unionBranchesIndexed(ctx, s, filter) {
				return false
			}
			continue
		}
		p, ok := branch.(*plan.Project)
		for ok {
			var next *plan.Project
			if next, ok = p.Child.(*plan.Project); ok {
				p = next
			}
		}
		if p == nil {
			return false
		}
		f, ok := p.Child.(*plan.Filter)
		if !ok {
			return false
		}
		eq, ok := f.Expression.(*expression.Equals)
		if !ok {
			return false
		}
		col, ok := eq.Left().(*expression.GetField)
		if !ok {
			return false
		}
		below := f.Child
		for {
			next, ok := below.(*plan.Filter)
			if !ok {
				break
			}
			below = next.Child
		}
		table := getTable(ctx, below)
		it, ok := table.(sql.IndexAddressableTable)
		if !ok {
			return false
		}
		indexes, err := it.GetIndexes(ctx)
		if err != nil {
			return false
		}
		found := false
		for _, idx := range indexes {
			if exprs := idx.Expressions(); len(exprs) > 0 && strings.EqualFold(exprs[0], table.Name()+"."+col.Name()) {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func hasSubqueryAlias(ctx *sql.Context, n sql.Node) bool {
	return transform.InspectUp(ctx, n, func(ctx *sql.Context, n sql.Node) bool {
		_, isSubq := n.(*plan.SubqueryAlias)
		return isSubq
	})
}

// canDoPushdown returns whether the node given can safely be analyzed for pushdown
func canDoPushdown(n sql.Node) bool {
	if plan.IsNoRowNode(n) {
		return false
	}

	// The direct child of an InsertInto is its destination. InsertInto.Source is analyzed separately during
	// resolveInsertRows
	switch n.(type) {
	case *plan.InsertInto:
		return false
	}

	return true
}

// filterPushdownSelector determines if it's valid to push a filter down into a node
func filterPushdownSelector(ctx *sql.Context, c transform.Context) bool {
	switch n := c.Parent.(type) {
	case *plan.TableAlias:
		return false
	case *plan.JoinNode:
		if !n.Op.IsPreservedTable(c.ChildNum) {
			return false
		}
	}

	switch n := c.Node.(type) {
	case *plan.Limit, *plan.Window:
		// Limit and Window operate across the rows they see and cannot have filters pushed below them.
		return false
	case *plan.JoinNode:
		// Filters cannot be pushed down into FullOuter joins because it is not null-safe and must be evaluated
		// after join result is computed. Filters cannot be pushed down into Merge join because they might result into
		// an index lookup that is not monotonically sorted on the join condition
		return !(n.Op.IsFullOuter() || n.Op.IsMerge())
	}
	return true
}

func transformPushdownSubqueryAliasFilters(ctx *sql.Context, a *Analyzer, n sql.Node, scope *plan.Scope) (sql.Node, transform.TreeIdentity, error) {
	var filters *filterSet
	projectionExpressions := getProjectionExpressions(n)

	transformFilterNode := func(n *plan.Filter) (sql.Node, transform.TreeIdentity, error) {
		return transform.NodeWithCtx(ctx, n, filterPushdownSelector, func(ctx *sql.Context, c transform.Context) (sql.Node, transform.TreeIdentity, error) {
			switch node := c.Node.(type) {
			case *plan.Filter:
				return updateFilterNode(ctx, a, node, node.Child, transform.SameTree, expression.SplitConjunction(ctx, node.Expression), filters)
			case *plan.SubqueryAlias:
				// TODO: We probably could push filters into a RecursiveCTE to get an IndexedTableAccess where
				//  applicable. But we currently don't push any filters through at all so pushing filters past the
				//  SubqueryAlias node doesn't actually do anything except possibly make them uncacheable, which we
				//  don't want.
				if _, ok := node.Child.(*plan.RecursiveCte); ok {
					return node, transform.SameTree, nil
				}
				return pushdownFiltersUnderSubqueryAlias(ctx, a, node, filters)
			default:
				return node, transform.SameTree, nil
			}
		})
	}

	// For each filter node, we want to push its predicates as low as possible.
	// TODO: Having two nested transform.Node/transform.NodeCtx ends up traversing the node tree multiple times for each
	//  filter node. Rewrite transformPushdownSubqueryAliasFilters to be a single node traversal, similar to pushFilters
	return transform.Node(ctx, n, func(ctx *sql.Context, n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		switch n := n.(type) {
		case *plan.Filter:
			// First step is to find all col exprs and group them by the table they mention.
			filters = newFilterSet(ctx, n, scope, projectionExpressions)
			return transformFilterNode(n)
		default:
			return n, transform.SameTree, nil
		}
	})
}

// filteredTableNode wraps a table node in applicable filters
func filteredTableNode(
	ctx *sql.Context,
	a *Analyzer,
	tableNode plan.TableIdNode,
	filters *filterSet,
) (sql.Node, transform.TreeIdentity, error) {
	table := getTable(ctx, tableNode)
	if table == nil || plan.IsDualTable(table) {
		return tableNode, transform.SameTree, nil
	}

	// Move any remaining filters for the table directly above the table itself
	var pushedDownFilterExpression sql.Expression
	if tableFilters := filters.availableFiltersForTable(tableNode.Id()); len(tableFilters) > 0 {
		pushedFilters := make([]sql.Expression, 0, len(tableFilters))
		expressionsForTable := make([]sql.Expression, 0, len(tableFilters))
		for _, filter := range tableFilters {
			// If a filter contains a reference to a projection alias, pushing the filter will move it below the
			// Project node. We need to replace the reference with the underlying expression.
			tableExpression, _, err := transform.Expr(ctx, filter, func(ctx *sql.Context, e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
				if gt, ok := e.(*expression.GetField); ok {
					if aliasedExpression, ok := filters.projectionExpressions[gt.Id()]; ok {
						return aliasedExpression, transform.NewTree, nil
					}
				}
				return e, transform.SameTree, nil
			})
			if err != nil {
				return tableNode, transform.SameTree, err
			}
			// A set-returning function's output must be filtered after the projection expands it into rows.
			// Substitution may expose an SRF even when subquery-alias pushdown only saw a GetField.
			if transform.InspectExpr(ctx, tableExpression, func(ctx *sql.Context, e sql.Expression) bool {
				rowIter, ok := e.(sql.RowIterExpression)
				return ok && rowIter.ReturnsRowIter()
			}) {
				continue
			}
			pushedFilters = append(pushedFilters, filter)
			expressionsForTable = append(expressionsForTable, tableExpression)
		}
		if len(pushedFilters) == 0 {
			return tableNode, transform.SameTree, nil
		}
		filters.markFiltersHandled(pushedFilters...)
		pushedDownFilterExpression = expression.JoinAnd(expressionsForTable...)

		a.Log(
			"pushed down filters %s above table %q, %d filters handled of %d",
			expressionsForTable,
			tableNode.Name(),
			len(pushedFilters),
			len(tableFilters),
		)
	}

	switch tableNode.(type) {
	case *plan.ResolvedTable, *plan.TableAlias, *plan.ValueDerivedTable:
		if pushedDownFilterExpression != nil {
			return plan.NewFilter(ctx, pushedDownFilterExpression, tableNode), transform.NewTree, nil
		}

		return tableNode, transform.SameTree, nil
	default:
		return nil, transform.SameTree, ErrInvalidNodeType.New("pushdownFiltersToAboveTable", tableNode)
	}
}

// pushdownFiltersUnderSubqueryAlias takes |filters| applying to the subquery
// alias a moves them under the subquery alias. Because the subquery alias is
// Opaque, it behaves a little bit like a FilteredTable, and pushing the
// filters down below it can help find index usage opportunities later in the
// analysis phase.
func pushdownFiltersUnderSubqueryAlias(ctx *sql.Context, a *Analyzer, sa *plan.SubqueryAlias, filters *filterSet) (sql.Node, transform.TreeIdentity, error) {
	if sa.ScopeMapping == nil {
		return sa, transform.SameTree, nil
	}
	handled := filters.availableFiltersForTable(sa.Id())
	if len(handled) == 0 {
		return sa, transform.SameTree, nil
	}
	// |handled| is in terms of the parent schema, and in particular the
	// |Source| is the alias name. Rewrite it to refer to the |sa.Child|
	// schema instead.
	expressionsForChild := make([]sql.Expression, 0, len(handled))
	pushedFilters := make([]sql.Expression, 0, len(handled))
	for _, h := range handled {
		var correlated sql.ColSet
		var tf transform.ExprFunc
		tf = func(ctx *sql.Context, e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
			// If a filter contains a reference to a projection alias, pushing the filter will move it below the
			// Project node. We need to replace the reference with the underlying expression.
			if gt, ok := e.(*expression.GetField); ok {
				if aliasedExpression, ok := filters.projectionExpressions[gt.Id()]; ok {
					// Replacing the GetField with its underlying expression is always a change to the tree,
					// regardless of whether the recursive substitution below finds further changes.
					newExpr, _, err := transform.Expr(ctx, aliasedExpression, tf)
					return newExpr, transform.NewTree, err
				}
				gf, ok := sa.ScopeMapping[gt.Id()]
				if !ok {
					// The GetField must be referencing an outer or lateral scope.
					// Record its correlation only if this predicate is pushed.
					correlated.Add(gt.Id())
					return e, transform.NewTree, nil
				}
				return gf, transform.NewTree, nil
			}
			return e, transform.SameTree, nil
		}
		childExpression, _, err := transform.Expr(ctx, h, tf)
		if err != nil {
			return sa, transform.SameTree, err
		}
		// A predicate on a set-returning function's output must consume the rows expanded by the projection.
		// Substituting the SRF would instead evaluate its iterator as a scalar value. Check
		// expressions directly: IncludesNestedIters may not be set yet at this analyzer stage.
		if transform.InspectExpr(ctx, childExpression, func(ctx *sql.Context, e sql.Expression) bool {
			rowIter, ok := e.(sql.RowIterExpression)
			return ok && rowIter.ReturnsRowIter()
		}) {
			continue
		}
		if !correlated.Empty() {
			sa.Correlated.UnionWith(correlated)
			// A pushed predicate may now reference a lateral scope.
			sa.IsLateral = true
		}
		expressionsForChild = append(expressionsForChild, childExpression)
		pushedFilters = append(pushedFilters, h)
	}
	if len(pushedFilters) == 0 {
		return sa, transform.SameTree, nil
	}
	filters.markFiltersHandled(pushedFilters...)

	child := sa.Child
	if u, ok := child.(*plan.SetOp); ok {
		var remaining []sql.Expression
		for _, e := range expressionsForChild {
			if pushed, ok := pushdownFilterIntoUnion(ctx, u, e, sa.Correlated); ok {
				u = pushed
			} else {
				remaining = append(remaining, e)
			}
		}
		child, expressionsForChild = u, remaining
	}
	if len(expressionsForChild) > 0 {
		child = plan.NewFilter(ctx, expression.JoinAnd(expressionsForChild...), child)
	}
	n, err := sa.WithChildren(ctx, child)
	if err != nil {
		return nil, transform.SameTree, err
	}
	return n, transform.NewTree, nil
}

// pushdownFilterIntoUnion moves |filter|, an expression of |u|'s output columns, into each branch of the UNION |u|,
// below the branch's projections, where later analysis can push it to the branch's tables and use their indexes.
// Filtering each branch's rows before the union gives the same rows as filtering its output, with or without
// DISTINCT. It applies only to a UNION without its own ORDER BY, LIMIT or OFFSET whose branches are projections, to a
// filter without subqueries or non-deterministic functions, and only when every output column the filter reads is a
// plain column of the same type in every branch (so no conversion to the union's type changes the comparison); any
// other column must be an outer scope column in |outer|. Otherwise it reports false.
func pushdownFilterIntoUnion(ctx *sql.Context, u *plan.SetOp, filter sql.Expression, outer sql.ColSet) (*plan.SetOp, bool) {
	if u.SetOpType != plan.UnionType || u.Limit != nil || u.Offset != nil || len(u.SortConditions) > 0 || !pushableExpression(ctx, filter) {
		return nil, false
	}
	if transform.InspectExpr(ctx, filter, func(ctx *sql.Context, e sql.Expression) bool {
		rowIter, ok := e.(sql.RowIterExpression)
		return ok && rowIter.ReturnsRowIter()
	}) {
		return nil, false
	}
	leftColumns, leftIds, leftBottom := branchColumns(u.Left())
	rightColumns, _, rightBottom := branchColumns(u.Right())
	if leftColumns == nil || rightColumns == nil || len(leftColumns) != len(rightColumns) {
		return nil, false
	}
	ok := true
	var leftFilter, rightFilter sql.Expression
	for i, branch := range [][]sql.Expression{leftColumns, rightColumns} {
		rewritten, _, err := transform.Expr(ctx, filter, func(ctx *sql.Context, e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
			gf, isField := e.(*expression.GetField)
			if !isField {
				return e, transform.SameTree, nil
			}
			at, isOutput := leftIds[gf.Id()]
			if !isOutput {
				// an outer or lateral scope column stays as it is; any other column cannot be placed in the branch
				if !outer.Contains(gf.Id()) {
					ok = false
				}
				return e, transform.SameTree, nil
			}
			l, lok := leftColumns[at].(*expression.GetField)
			r, rok := rightColumns[at].(*expression.GetField)
			if !lok || !rok || !l.Type(ctx).Equals(r.Type(ctx)) {
				ok = false
				return e, transform.SameTree, nil
			}
			return branch[at], transform.NewTree, nil
		})
		if err != nil || !ok {
			return nil, false
		}
		if i == 0 {
			leftFilter = rewritten
		} else {
			rightFilter = rewritten
		}
	}
	left, err := leftBottom(ctx, leftFilter)
	if err != nil {
		return nil, false
	}
	right, err := rightBottom(ctx, rightFilter)
	if err != nil {
		return nil, false
	}
	n, err := u.WithChildren(ctx, left, right)
	if err != nil {
		return nil, false
	}
	return n.(*plan.SetOp), true
}

// branchColumns follows a union branch's projections down to the node below the last of them (a branch may project
// twice, once more to convert its columns to the union's types). It returns each output column as an expression of
// that node's columns, the column ids that name the outputs at any of those projections (by ordinal), and a function
// that rebuilds the branch with a filter placed under its projections. It returns nil for a branch that is not a
// projection.
func branchColumns(branch sql.Node) ([]sql.Expression, map[sql.ColumnId]int, func(ctx *sql.Context, filter sql.Expression) (sql.Node, error)) {
	top, ok := branch.(*plan.Project)
	if !ok {
		return nil, nil, nil
	}
	var chain []*plan.Project
	for p := top; p != nil; p, _ = p.Child.(*plan.Project) {
		if len(p.Projections) != len(top.Projections) {
			break
		}
		chain = append(chain, p)
	}
	ids := make(map[sql.ColumnId]int)
	columns := make([]sql.Expression, len(top.Projections))
	for i := range columns {
		columns[i] = top.Projections[i]
	}
	// substitute each projection's definitions into the columns above it, from the top down
	for depth, p := range chain {
		defined := make(map[sql.ColumnId]sql.Expression, len(p.Projections))
		for i, e := range p.Projections {
			if id, ok := e.(sql.IdExpression); ok {
				defined[id.Id()] = unaliased(e)
				if _, seen := ids[id.Id()]; !seen {
					ids[id.Id()] = i
				}
			}
		}
		for i, c := range columns {
			if depth == 0 {
				columns[i] = unaliased(c)
				continue
			}
			if gf, ok := c.(*expression.GetField); ok {
				if d, ok := defined[gf.Id()]; ok {
					columns[i] = d
				}
			}
		}
	}
	rebuild := func(ctx *sql.Context, filter sql.Expression) (sql.Node, error) {
		last := chain[len(chain)-1]
		n, err := last.WithChildren(ctx, plan.NewFilter(ctx, filter, last.Child))
		if err != nil {
			return nil, err
		}
		for i := len(chain) - 2; i >= 0; i-- {
			if n, err = chain[i].WithChildren(ctx, n); err != nil {
				return nil, err
			}
		}
		return n, nil
	}
	return columns, ids, rebuild
}

func unaliased(e sql.Expression) sql.Expression {
	if alias, ok := e.(*expression.Alias); ok {
		return alias.Child
	}
	return e
}

// pushableExpression reports whether |e| may be evaluated in another place than where it is written: it has no
// subquery, aggregate, window or non-deterministic function.
func pushableExpression(ctx *sql.Context, e sql.Expression) bool {
	return !transform.InspectExpr(ctx, e, func(ctx *sql.Context, e sql.Expression) bool {
		switch e.(type) {
		case *plan.Subquery, sql.NonDeterministicExpression, sql.Aggregation, sql.WindowAggregation:
			return true
		}
		return false
	})
}

// updateFilterNode updates the filter node based on the filter predicates handled. Any handled filter predicates are
// removed from the filter node. If all filter predicates have been handled and there are no unhandled predicates, the
// filter node is removed.
func updateFilterNode(ctx *sql.Context, a *Analyzer, node *plan.Filter, nodeChild sql.Node, childSame transform.TreeIdentity, filterExpressions []sql.Expression, filters *filterSet) (sql.Node, transform.TreeIdentity, error) {
	unhandled := subtractExprSet(filterExpressions, filters.handledFilters)

	if len(unhandled) == 0 {
		a.Log("filter node has no unhandled filters, so it will be removed")
		return nodeChild, transform.NewTree, nil
	}
	filters.markFiltersHandled(unhandled...)

	if f, ok := nodeChild.(*plan.Filter); ok {
		unhandled = append(unhandled, f.Expression)
		nodeChild = f.Child
		childSame = transform.NewTree
	}

	if childSame && len(unhandled) == len(filterExpressions) {
		a.Log("no handled filters, leaving filter untouched")
		return node, transform.SameTree, nil
	}

	a.Log(
		"filters removed from filter node: %s\nfilter has now %d filters: %s",
		filters.handledFilters,
		len(unhandled),
		unhandled,
	)

	return plan.NewFilter(ctx, expression.JoinAnd(unhandled...), nodeChild), transform.NewTree, nil
}
