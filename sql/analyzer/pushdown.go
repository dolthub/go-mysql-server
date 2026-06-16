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
)

// pushFilters moves filter nodes down to their appropriate relations.
// Filters that reference a single relation will wrap their target tables.
// as is appropriate. We never move a filter without deleting from the source.
// Related rules: hoistOutOfScopeFilters, moveJoinConditionsToFilter.
func pushFilters(ctx *sql.Context, a *Analyzer, n sql.Node, scope *plan.Scope, sel RuleSelector, qFlags *sql.QueryFlags) (sql.Node, transform.TreeIdentity, error) {
	span, ctx := ctx.Span("push_filters")
	defer span.End()

	if !canDoPushdown(n) {
		return n, transform.SameTree, nil
	}
	filters := &filterSet{
		projectionExpressions: getProjectionExpressions(n),
		filtersByTable:        newFiltersByTable(),
	}

	n, same, err := pushdownFiltersAboveTables(ctx, a, n, scope, filters)
	// TODO: assert that there are no unhandled filters? this should never happen so error out if it does
	return n, same, err
}

func pushdownFiltersAboveTables(ctx *sql.Context, a *Analyzer, n sql.Node, scope *plan.Scope, filters *filterSet) (sql.Node, transform.TreeIdentity, error) {
	if sql.IsOpaque(n) {
		return n, transform.SameTree, nil
	}

	switch node := n.(type) {
	case *plan.Filter:
		filterExpressions := expression.SplitConjunction(ctx, node.Expression)

		// TODO: refactor this into its own function that takes an list of expressions (only need to call split conjunction once)
		filters.filtersByTable.merge(exprToTableFilters(ctx, node.Expression, scope, filters.projectionExpressions))
		child, same, err := pushdownFiltersAboveTables(ctx, a, node.Child, scope, filters)
		if err != nil {
			return node, transform.SameTree, err
		}
		// TODO: this is very similar to updateFilterNode and can be refactored to avoid repeated code
		unhandled := subtractExprSet(filterExpressions, filters.handledFilters)
		filters.markFiltersHandled(unhandled...)
		if f, ok := child.(*plan.Filter); ok {
			return plan.NewFilter(ctx, expression.JoinAnd(expression.JoinAnd(unhandled...), f.Expression), f.Child), transform.NewTree, nil
		}
		if same {
			return node, transform.SameTree, nil
		}
		if len(unhandled) > 0 {
			return plan.NewFilter(ctx, expression.JoinAnd(unhandled...), child), transform.NewTree, nil
		} else {
			return child, transform.NewTree, nil
		}
	case *plan.JoinNode:
		joinOp := node.JoinType()
		// TODO: filters and join conditions can be pushed past Left and Anti joins but only under certain conditions.
		//  It's safe to push filters through the left child but we have to consider join order hints that may change.
		//  how the join is replanned. It is also safe to push filters through the right child if the condition is null-
		//  rejecting.
		if joinOp.IsMerge() || joinOp.IsOuter() || joinOp.IsAnti() {
			return node, transform.SameTree, nil
		}

		filterExpressions := expression.SplitConjunction(ctx, node.Filter)
		// TODO: replace with refactored new function
		filters.filtersByTable.merge(exprToTableFilters(ctx, node.Filter, scope, filters.projectionExpressions))

		leftChild, leftSame, err := pushdownFiltersAboveTables(ctx, a, node.Left(), scope, filters)
		if err != nil {
			return node, transform.SameTree, err
		}
		rightChild, rightSame, err := pushdownFiltersAboveTables(ctx, a, node.Right(), scope, filters)
		if err != nil {
			return node, transform.SameTree, err
		}
		if leftSame && rightSame {
			return node, transform.SameTree, nil
		}

		// TODO: depending on the join type, filters from parent nodes that only reference the join children can also be
		//  added to the join condition. To do this, we would need to update filtersByTable to use a FastIntSet key
		//  instead of a TableId key and we would also need to propagate up a set of the child TableIds.
		unhandled := subtractExprSet(filterExpressions, filters.handledFilters)
		filters.markFiltersHandled(unhandled...)
		return plan.NewJoin(ctx, leftChild, rightChild, joinOp, expression.JoinAnd(unhandled...)).WithComment(node.Comment()), transform.NewTree, nil
	case *plan.TableAlias, *plan.ResolvedTable, *plan.ValueDerivedTable:
		return filteredTableNode(ctx, a, node.(plan.TableIdNode), filters)
	case *plan.Limit, *plan.Window:
		return n, transform.SameTree, nil
	default:
		children := node.Children()
		allSame := true
		for i, child := range children {
			newChild, same, err := pushdownFiltersAboveTables(ctx, a, child, scope, filters)
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

	// TODO: this involves calling InspectUp on entire node tree. If there is a SQA, it stops traversing and returns
	//  true, but otherwise, it will inspect the entire node tree just to return false. Only the leaves need to be
	//  inspected to determine if there's an SQA.
	if !hasSubqueryAlias(ctx, n) {
		return n, transform.SameTree, nil
	}

	return transformPushdownSubqueryAliasFilters(ctx, a, n, scope)
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

	// The values of an insert are analyzed in isolation, so they do get pushdown treatment. But no other DML
	// statements should get pushdown to their target tables.
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
		// Pushing down a filter is incompatible with the secondary table in a Left or Right join. If we push a
		// predicate on the secondary table below the join, we end up not evaluating it in all cases (since the
		// secondary table result is sometimes null in these types of joins). It must be evaluated only after the join
		// result is computed.
		if n.Op.IsLeftOuter() && c.ChildNum != 0 {
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
	projectionExpressions := getProjectionExpressions(n)

	var filters *filterSet

	transformFilterNode := func(n *plan.Filter) (sql.Node, transform.TreeIdentity, error) {
		return transform.NodeWithCtx(ctx, n, filterPushdownSelector, func(ctx *sql.Context, c transform.Context) (sql.Node, transform.TreeIdentity, error) {
			switch node := c.Node.(type) {
			case *plan.Filter:
				return updateFilterNode(ctx, a, node, filters)
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
	if tableFilters := filters.availableFiltersForTable(ctx, tableNode.Id()); len(tableFilters) > 0 {
		filters.markFiltersHandled(tableFilters...)
		for i, filter := range tableFilters {
			// If a filter contains a reference to a projection alias, pushing the filter will move it below the
			// Project node. We need to replace the reference with the underlying expression.
			tableFilters[i], _, _ = transform.Expr(ctx, filter, func(ctx *sql.Context, e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
				if gt, ok := e.(*expression.GetField); ok {
					if aliasedExpression, ok := filters.projectionExpressions[gt.Id()]; ok {
						return aliasedExpression, transform.NewTree, nil
					}
				}
				return e, transform.SameTree, nil
			})
		}
		pushedDownFilterExpression = expression.JoinAnd(tableFilters...)

		a.Log(
			"pushed down filters %s above table %q, %d filters handled of %d",
			tableFilters,
			tableNode.Name(),
			len(tableFilters),
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
	handled := filters.availableFiltersForTable(ctx, sa.Id())
	if len(handled) == 0 {
		return sa, transform.SameTree, nil
	}
	filters.markFiltersHandled(handled...)
	// |handled| is in terms of the parent schema, and in particular the
	// |Source| is the alias name. Rewrite it to refer to the |sa.Child|
	// schema instead.
	expressionsForChild := make([]sql.Expression, len(handled))
	var err error
	for i, h := range handled {
		var tf transform.ExprFunc
		tf = func(ctx *sql.Context, e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
			// If a filter contains a reference to a projection alias, pushing the filter will move it below the
			// Project node. We need to replace the reference with the underlying expression.
			if gt, ok := e.(*expression.GetField); ok {
				if aliasedExpression, ok := filters.projectionExpressions[gt.Id()]; ok {
					return transform.Expr(ctx, aliasedExpression, tf)
				}
				gf, ok := sa.ScopeMapping[gt.Id()]
				if !ok {
					// The GetField must be referencing an outer or lateral scope.
					// We need to add this to the subquery alias's list of correlated columns
					sa.Correlated.Add(gt.Id())
					// There now may be a reference to a lateral scope, so we mark the alias as lateral just in case.
					// This shouldn't break anything, but it might inhibit optimizations that check this.
					sa.IsLateral = true
					return e, transform.NewTree, nil
				}
				return gf, transform.NewTree, nil
			}
			return e, transform.SameTree, nil
		}
		expressionsForChild[i], _, err = transform.Expr(ctx, h, tf)
		if err != nil {
			return sa, transform.SameTree, err
		}
	}

	n, err := sa.WithChildren(ctx, plan.NewFilter(ctx, expression.JoinAnd(expressionsForChild...), sa.Child))
	if err != nil {
		return nil, transform.SameTree, err
	}
	return n, transform.NewTree, nil
}

// updateFilterNode updates the filter node based on the filter predicates handled. Any handled filter predicates are
// removed from the filter node. If all filter predicates have been handled and there are no unhandled predicates, the
// filter node is removed.
func updateFilterNode(ctx *sql.Context, a *Analyzer, node *plan.Filter, filters *filterSet) (sql.Node, transform.TreeIdentity, error) {
	filterExpressions := expression.SplitConjunction(ctx, node.Expression)
	unhandled := subtractExprSet(filterExpressions, filters.handledFilters)

	if len(unhandled) == 0 {
		a.Log("filter node has no unhandled filters, so it will be removed")
		return node.Child, transform.NewTree, nil
	}

	if filters.handledCount() == 0 || len(unhandled) == len(filterExpressions) {
		a.Log("no handled filters, leaving filter untouched")
		return node, transform.SameTree, nil
	}

	a.Log(
		"filters removed from filter node: %s\nfilter has now %d filters: %s",
		filters.handledFilters,
		len(unhandled),
		unhandled,
	)

	return plan.NewFilter(ctx, expression.JoinAnd(unhandled...), node.Child), transform.NewTree, nil
}
