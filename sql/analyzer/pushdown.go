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
	"github.com/dolthub/go-mysql-server/sql/transform"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// pushFilters moves filter nodes down to their appropriate relations.
// Filters that reference a single relation will wrap their target tables.
// Filters that reference multiple tables will move as low in the join tree
// as is appropriate. We never move a filter without deleting from the source.
// Related rules: hoistOutOfScopeFilters, moveJoinConditionsToFilter.
func pushFilters(ctx *sql.Context, a *Analyzer, n sql.Node, scope *plan.Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	span, ctx := ctx.Span("push_filters")
	defer span.End()

	if !canDoPushdown(n) {
		return n, transform.SameTree, nil
	}

	pushdownAboveTables := func(n sql.Node, filters *filterSet) (sql.Node, transform.TreeIdentity, error) {
		return transform.NodeWithCtx(n, filterPushdownChildSelector, func(c transform.Context) (sql.Node, transform.TreeIdentity, error) {
			switch node := c.Node.(type) {
			case *plan.Filter:
				// Notably, filters are allowed to be pushed through other filters.
				// This prevents filters hoisted from join conditions from being
				// orphaned in the middle of join trees.
				if f, ok := node.Child.(*plan.Filter); ok {
					if node.Expression == f.Expression {
						return f, transform.NewTree, nil
					}
					return plan.NewFilter(expression.JoinAnd(node.Expression, f.Expression), f.Child), transform.NewTree, nil
				}
				return node, transform.SameTree, nil
			case *plan.TableAlias, *plan.ResolvedTable, *plan.ValueDerivedTable:
				table, same, err := pushdownFiltersToAboveTable(ctx, a, node.(sql.NameableNode), scope, filters)
				if err != nil {
					return nil, transform.SameTree, err
				}
				if same {
					return node, transform.SameTree, nil
				}
				return table, transform.NewTree, nil
			default:
				return node, transform.SameTree, nil
			}
		})
	}

	tableAliases, err := getTableAliases(n, scope)
	if err != nil {
		return nil, transform.SameTree, err
	}

	// For each filter node, we want to push its predicates as low as possible.
	return transform.Node(n, func(node sql.Node) (sql.Node, transform.TreeIdentity, error) {
		switch n := node.(type) {
		case *plan.Filter:
			switch n.Child.(type) {
			case *plan.TableAlias, *plan.ResolvedTable, *plan.IndexedTableAccess, *plan.ValueDerivedTable:
				// can't push any lower
				return n, transform.SameTree, nil
			default:
			}
			// Find all col exprs and group them by the table they mention so that we can keep track of which ones
			// have been pushed down and need to be removed from the parent filter
			filtersByTable := getFiltersByTable(n)
			filters := newFilterSet(n.Expression, filtersByTable, tableAliases)

			// move filter predicates directly above their respective tables in joins
			ret, same, err := pushdownAboveTables(n, filters)
			if same || err != nil {
				return n, transform.SameTree, err
			}

			retF, ok := ret.(*plan.Filter)
			if !ok {
				return n, transform.SameTree, fmt.Errorf("pushdown mistakenly converted filter to non-filter: %T", ret)
			}
			// remove handled
			newF := removePushedDownPredicates(ctx, a, retF, filters)
			if newF != nil {
				same = transform.NewTree
				ret = newF
			}
			return ret, same, nil
		default:
			return n, transform.SameTree, nil
		}
	})
}

// pushdownSubqueryAliasFilters attempts to push conditions in filters down to
// individual subquery aliases.
func pushdownSubqueryAliasFilters(ctx *sql.Context, a *Analyzer, n sql.Node, scope *plan.Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	span, ctx := ctx.Span("pushdown_subquery_alias_filters")
	defer span.End()

	if !canDoPushdown(n) {
		return n, transform.SameTree, nil
	}

	if !hasSubqueryAlias(n) {
		return n, transform.SameTree, nil
	}

	tableAliases, err := getTableAliases(n, scope)
	if err != nil {
		return nil, transform.SameTree, err
	}

	return transformPushdownSubqueryAliasFilters(ctx, a, n, scope, tableAliases)
}

func hasSubqueryAlias(n sql.Node) bool {
	return transform.InspectUp(n, func(n sql.Node) bool {
		_, isSubq := n.(*plan.SubqueryAlias)
		return isSubq
	})
}

// canDoPushdown returns whether the node given can safely be analyzed for pushdown
func canDoPushdown(n sql.Node) bool {
	if !n.Resolved() {
		return false
	}

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

// Pushing down a filter is incompatible with the secondary table in a Left
// or Right join. If we push a predicate on the secondary table below the
// join, we end up not evaluating it in all cases (since the secondary table
// result is sometimes null in these types of joins). It must be evaluated
// only after the join result is computed.
func filterPushdownChildSelector(c transform.Context) bool {
	switch c.Node.(type) {
	case *plan.Limit:
		return false
	}

	switch n := c.Parent.(type) {
	case *plan.TableAlias:
		return false
	case *plan.Window:
		// Windows operate across the rows they see and cannot have
		// filters pushed below them. Instead, the step will be run
		// again by the Transform function, starting at this node.
		return false
	case *plan.JoinNode:
		switch {
		case n.Op.IsMerge():
			return false
		case n.Op.IsLookup():
			if n.JoinType().IsLeftOuter() {
				return c.ChildNum == 0
			}
			return true
		case n.Op.IsLeftOuter():
			return c.ChildNum == 0
		default:
		}
	default:
	}
	return true
}

func transformPushdownSubqueryAliasFilters(ctx *sql.Context, a *Analyzer, n sql.Node, scope *plan.Scope, tableAliases TableAliases) (sql.Node, transform.TreeIdentity, error) {
	var filters *filterSet

	transformFilterNode := func(n *plan.Filter) (sql.Node, transform.TreeIdentity, error) {
		return transform.NodeWithCtx(n, filterPushdownChildSelector, func(c transform.Context) (sql.Node, transform.TreeIdentity, error) {
			switch node := c.Node.(type) {
			case *plan.Filter:
				newF := removePushedDownPredicates(ctx, a, node, filters)
				if newF == nil {
					return node, transform.SameTree, nil
				}
				return newF, transform.NewTree, nil
			case *plan.SubqueryAlias:
				return pushdownFiltersUnderSubqueryAlias(ctx, a, node, filters)
			default:
				return node, transform.SameTree, nil
			}
		})
	}

	// For each filter node, we want to push its predicates as low as possible.
	return transform.Node(n, func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		switch n := n.(type) {
		case *plan.Filter:
			// First step is to find all col exprs and group them by the table they mention.
			filtersByTable := getFiltersByTable(n)
			filters = newFilterSet(n.Expression, filtersByTable, tableAliases)
			return transformFilterNode(n)
		default:
			return n, transform.SameTree, nil
		}
	})
}

// pushdownFiltersToAboveTable introduces a filter node with the given predicate
func pushdownFiltersToAboveTable(
	ctx *sql.Context,
	a *Analyzer,
	tableNode sql.NameableNode,
	scope *plan.Scope,
	filters *filterSet,
) (sql.Node, transform.TreeIdentity, error) {
	table := getTable(tableNode)
	if table == nil || plan.IsDualTable(table) {
		return tableNode, transform.SameTree, nil
	}

	// Move any remaining filters for the table directly above the table itself
	var pushedDownFilterExpression sql.Expression
	if tableFilters := filters.availableFiltersForTable(ctx, tableNode.Name()); len(tableFilters) > 0 {
		filters.markFiltersHandled(tableFilters...)
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
			return plan.NewFilter(pushedDownFilterExpression, tableNode), transform.NewTree, nil
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
	handled := filters.availableFiltersForTable(ctx, sa.Name())
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
		expressionsForChild[i], _, err = transform.Expr(h, func(e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
			if gt, ok := e.(*expression.GetField); ok {
				gf, ok := sa.ScopeMapping[gt.Id()]
				if !ok {
					return e, transform.SameTree, fmt.Errorf("unable to find child with id: %d", gt.Index())
				}
				return gf, transform.NewTree, nil
			}
			return e, transform.SameTree, nil
		})
		if err != nil {
			return sa, transform.SameTree, err
		}
	}

	n, err := sa.WithChildren(plan.NewFilter(expression.JoinAnd(expressionsForChild...), sa.Child))
	if err != nil {
		return nil, transform.SameTree, err
	}
	return n, transform.NewTree, nil
}

// removePushedDownPredicates removes all handled filter predicates from the filter given and returns. If all
// predicates have been handled, it replaces the filter with its child.
func removePushedDownPredicates(ctx *sql.Context, a *Analyzer, node *plan.Filter, filters *filterSet) sql.Node {
	if filters.handledCount() == 0 {
		a.Log("no handled filters, leaving filter untouched")
		return nil
	}

	// figure out if the filter's filters were all handled
	filterExpressions := expression.SplitConjunction(node.Expression)
	unhandled := subtractExprSet(filterExpressions, filters.handledFilters)
	if len(unhandled) == 0 {
		a.Log("filter node has no unhandled filters, so it will be removed")
		return node.Child
	}

	if len(unhandled) == len(filterExpressions) {
		a.Log("no filters removed from filter node")
		return nil
	}

	a.Log(
		"filters removed from filter node: %s\nfilter has now %d filters: %s",
		filters.handledFilters,
		len(unhandled),
		unhandled,
	)

	return plan.NewFilter(expression.JoinAnd(unhandled...), node.Child)
}

// getIndexesByTable returns applicable index lookups for each table named in the query node given
func getIndexesByTable(ctx *sql.Context, a *Analyzer, node sql.Node, scope *plan.Scope) (indexLookupsByTable, error) {
	indexSpan, ctx := ctx.Span("getIndexesByTable")
	defer indexSpan.End()

	tableAliases, err := getTableAliases(node, scope)
	if err != nil {
		return nil, err
	}

	var indexes indexLookupsByTable
	cont := true
	var errInAnalysis error
	transform.Inspect(node, func(node sql.Node) bool {
		if !cont || errInAnalysis != nil {
			return false
		}

		filter, ok := node.(*plan.Filter)
		if !ok {
			return true
		}

		indexAnalyzer, err := newIndexAnalyzerForNode(ctx, filter.Child)
		if err != nil {
			errInAnalysis = err
			return false
		}
		defer indexAnalyzer.releaseUsedIndexes()

		var result indexLookupsByTable
		filterExpression := convertIsNullForIndexes(ctx, filter.Expression)
		result, err = getIndexes(ctx, indexAnalyzer, filterExpression, tableAliases)
		if err != nil {
			errInAnalysis = err
			return false
		}

		if !canMergeIndexLookups(indexes, result) {
			indexes = nil
			cont = false
			return false
		}

		indexes, err = indexesIntersection(ctx, indexes, result)
		if err != nil {
			errInAnalysis = err
			return false
		}
		return true
	})

	if errInAnalysis != nil {
		return nil, errInAnalysis
	}

	return indexes, nil
}

// convertIsNullForIndexes converts all nested IsNull(col) expressions to Equals(col, nil) expressions, as they are
// equivalent as far as the index interfaces are concerned.
func convertIsNullForIndexes(ctx *sql.Context, e sql.Expression) sql.Expression {
	expr, _, _ := transform.Expr(e, func(e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
		isNull, ok := e.(*expression.IsNull)
		if !ok {
			return e, transform.SameTree, nil
		}
		return expression.NewNullSafeEquals(isNull.Child, expression.NewLiteral(nil, types.Null)), transform.NewTree, nil
	})
	return expr
}
