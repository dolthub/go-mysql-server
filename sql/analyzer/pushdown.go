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

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// filterHasBindVar looks for any BindVars found in filter nodes
func filterHasBindVar(filter sql.Node) bool {
	var hasBindVar bool
	transform.Inspect(filter, func(node sql.Node) bool {
		if fn, ok := node.(*plan.Filter); ok {
			for _, expr := range fn.Expressions() {
				if exprHasBindVar(expr) {
					hasBindVar = true
					return false
				}
			}
		}
		return !hasBindVar // stop recursing if bindvar already found
	})
	return hasBindVar
}

// exprHasBindVar looks for any BindVars found in expressions
func exprHasBindVar(expr sql.Expression) bool {
	var hasBindVar bool
	transform.InspectExpr(expr, func(e sql.Expression) bool {
		if _, ok := e.(*expression.BindVar); ok {
			hasBindVar = true
			return true
		}
		return false
	})
	return hasBindVar
}

// pushdownFilters attempts to push conditions in filters down to individual tables. Tables that implement
// sql.FilteredTable will get such conditions applied to them. For conditions that have an index, tables that implement
// sql.IndexAddressableTable will get an appropriate index lookup applied.
// TODO(max): filter pushdown should happen as part of join reordering.
// A memo should be built before reorder with filter exprGroups, with
// bitmaps tracking table dependencies. The same processes here should
// be applied there: 1) filter pushdown to table, 2) filter pushdown to
// join node, 3) range scan indexes applied to non-lookup tables.
func pushdownFilters(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	span, ctx := ctx.Span("pushdown_filters")
	defer span.End()

	if !canDoPushdown(n) {
		return n, transform.SameTree, nil
	}

	node, same, err := pushdownFiltersAtNode(ctx, a, n, scope, sel)
	if err != nil {
		return nil, transform.SameTree, err
	}

	if !filterHasBindVar(n) {
		return node, same, err
	}

	// Wrap with DeferredFilteredTable if there are bindvars
	return transform.NodeWithOpaque(node, func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		if rt, ok := n.(*plan.ResolvedTable); ok {
			if _, ok := rt.Table.(sql.FilteredTable); ok {
				return plan.NewDeferredFilteredTable(rt), transform.NewTree, nil
			}
		}
		return n, transform.SameTree, nil
	})
}

func pushdownFiltersAtNode(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	indexes, err := getIndexesByTable(ctx, a, n, scope)
	if err != nil {
		return nil, transform.SameTree, err
	}

	tableAliases, err := getTableAliases(n, scope)
	if err != nil {
		return nil, transform.SameTree, err
	}

	n, sameI, err := convertFiltersToIndexedAccess(ctx, a, n, scope, indexes)
	if err != nil {
		return nil, transform.SameTree, err
	}

	n, sameF, err := transformPushdownFilters(ctx, a, n, scope, tableAliases, indexes, sel)
	if err != nil {
		return nil, transform.SameTree, err
	}
	return n, sameF && sameI, nil
}

// pushdownSubqueryAliasFilters attempts to push conditions in filters down to
// individual subquery aliases.
func pushdownSubqueryAliasFilters(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	span, ctx := ctx.Span("pushdown_subquery_alias_filters")
	defer span.End()

	if !canDoPushdown(n) {
		return n, transform.SameTree, nil
	}

	tableAliases, err := getTableAliases(n, scope)
	if err != nil {
		return nil, transform.SameTree, err
	}

	return transformPushdownSubqueryAliasFilters(ctx, a, n, scope, tableAliases)
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

// Pushing down a filter is incompatible with the secondary table in a Left or Right join. If we push a predicate on
// the secondary table below the join, we end up not evaluating it in all cases (since the secondary table result is
// sometimes null in these types of joins). It must be evaluated only after the join result is computed.
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

// Like filterPushdownChildSelector, but for pushing filters down via the introduction of additional Filter nodes
// (for tables that can't treat the filter as an index lookup or accept it directly). In this case, we want to avoid
// introducing additional Filter nodes unnecessarily. This means only introducing new filter nodes when they are being
// pushed below a join or other structure.
func filterPushdownAboveTablesChildSelector(c transform.Context) bool {
	// All the same restrictions that apply to pushing filters down in general apply here as well
	if !filterPushdownChildSelector(c) {
		return false
	}
	switch c.Parent.(type) {
	case *plan.Filter:
		switch c.Node.(type) {
		// Don't bother pushing filters down above tables if the direct child node is a table. At best this
		// just splits the predicates into multiple filter nodes, and at worst it breaks other parts of the
		// analyzer that don't expect this structure in the tree.
		case *plan.TableAlias, *plan.ResolvedTable, *plan.IndexedTableAccess, *plan.ValueDerivedTable:
			return false
		}
	}

	return true
}

func transformPushdownFilters(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope, tableAliases TableAliases, indexes indexLookupsByTable, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	applyFilteredTables := func(n *plan.Filter, filters *filterSet) (sql.Node, transform.TreeIdentity, error) {
		return transform.NodeWithCtx(n, filterPushdownChildSelector, func(c transform.Context) (sql.Node, transform.TreeIdentity, error) {
			switch node := c.Node.(type) {
			case *plan.Filter:
				n, samePred, err := removePushedDownPredicates(ctx, a, node, filters)
				if err != nil {
					return nil, transform.SameTree, err
				}
				n, sameFix, err := pushdownFixIndices(a, n, scope)
				if err != nil {
					return nil, transform.SameTree, err
				}
				return n, samePred && sameFix, nil
			case *plan.IndexedTableAccess:
				if jn, ok := c.Parent.(*plan.JoinNode); ok {
					if jn.Op.IsMerge() {
						return n, transform.SameTree, nil
					}
				}
				if plan.GetIndexLookup(node).IsEmpty() {
					// Index without lookup has no filters to mark/push.
					// Relevant for IndexJoin, which has more restrictive
					// rules for lookup expressions.
					return node, transform.SameTree, nil
				}
				lookup, ok := indexes[node.Name()]
				if !ok || lookup.expr == nil {
					return node, transform.SameTree, nil
				}
				handled, err := getPredicateExprsHandledByLookup(ctx, a, node, lookup, tableAliases)
				if err != nil {
					return nil, transform.SameTree, err
				}
				filters.markFiltersHandled(handled...)
				ret, err := plan.NewStaticIndexedAccessForResolvedTable(node.ResolvedTable, lookup.lookup)
				if err != nil {
					return node, transform.SameTree, err
				}
				return ret, len(handled) == 0, nil
			case *plan.TableAlias, *plan.ResolvedTable, *plan.ValueDerivedTable:
				n, samePred, err := pushdownFiltersToTable(ctx, a, node.(sql.NameableNode), scope, filters, tableAliases)
				if plan.ErrInvalidLookupForIndexedTable.Is(err) {
					return node, transform.SameTree, nil
				} else if err != nil {
					return nil, transform.SameTree, err
				}
				n, sameFix, err := pushdownFixIndices(a, n, scope)
				if err != nil {
					return nil, transform.SameTree, err
				}
				return n, samePred && sameFix, nil
			default:
				return pushdownFixIndices(a, node, scope)
			}
		})
	}

	pushdownAboveTables := func(n sql.Node, filters *filterSet) (sql.Node, transform.TreeIdentity, error) {
		return transform.NodeWithCtx(n, filterPushdownAboveTablesChildSelector, func(c transform.Context) (sql.Node, transform.TreeIdentity, error) {
			switch node := c.Node.(type) {
			case *plan.Filter:
				n, same, err := removePushedDownPredicates(ctx, a, node, filters)
				if err != nil {
					return nil, transform.SameTree, err
				}
				if same {
					return n, transform.SameTree, nil
				}
				n, _, err = pushdownFixIndices(a, n, scope)
				if err != nil {
					return nil, transform.SameTree, err
				}
				return n, transform.NewTree, nil
			case *plan.TableAlias, *plan.ResolvedTable, *plan.IndexedTableAccess, *plan.ValueDerivedTable:
				table, same, err := pushdownFiltersToAboveTable(ctx, a, node.(sql.NameableNode), scope, filters)
				if err != nil {
					return nil, transform.SameTree, err
				}
				if same {
					return node, transform.SameTree, nil
				}
				node, _, err = pushdownFixIndices(a, table, scope)
				if err != nil {
					return nil, transform.SameTree, err
				}
				return node, transform.NewTree, nil
			default:
				return pushdownFixIndices(a, node, scope)
			}
		})
	}

	// For each filter node, we want to push its predicates as low as possible.
	return transform.Node(n, func(node sql.Node) (sql.Node, transform.TreeIdentity, error) {
		switch n := node.(type) {
		case *plan.Filter:
			// Find all col exprs and group them by the table they mention so that we can keep track of which ones
			// have been pushed down and need to be removed from the parent filter
			filtersByTable := getFiltersByTable(n)
			filters := newFilterSet(n.Expression, filtersByTable, tableAliases)

			// Two passes: first push filters to any tables that implement sql.Filtered table directly
			node, sameA, err := applyFilteredTables(n, filters)
			if err != nil {
				return nil, transform.SameTree, err
			}

			// Then move filter predicates directly above their respective tables in joins
			node, sameB, err := pushdownAboveTables(node, filters)
			if err != nil {
				return nil, transform.SameTree, err
			}
			return node, sameA && sameB, nil
		case *plan.Window:
			// Analyze below the Window in isolation to push down
			// any relevant indexes, for example.
			child, same, err := pushdownFiltersAtNode(ctx, a, n.Child, scope, sel)
			if err != nil {
				return nil, transform.SameTree, err
			}
			if same {
				return n, transform.SameTree, nil
			}
			node, err = n.WithChildren(child)
			if err != nil {
				return nil, transform.SameTree, err
			}
			return node, transform.NewTree, nil
		default:
			return n, transform.SameTree, nil
		}
	})
}

func transformPushdownSubqueryAliasFilters(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope, tableAliases TableAliases) (sql.Node, transform.TreeIdentity, error) {
	var filters *filterSet

	transformFilterNode := func(n *plan.Filter) (sql.Node, transform.TreeIdentity, error) {
		return transform.NodeWithCtx(n, filterPushdownChildSelector, func(c transform.Context) (sql.Node, transform.TreeIdentity, error) {
			switch node := c.Node.(type) {
			case *plan.Filter:
				return removePushedDownPredicates(ctx, a, node, filters)
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

// convertFiltersToIndexedAccess attempts to replace filter predicates with indexed accesses where possible
// TODO: this function doesn't actually remove filters that have been converted to index lookups,
// that optimization is handled in transformPushdownFilters.
func convertFiltersToIndexedAccess(
	ctx *sql.Context,
	a *Analyzer,
	n sql.Node,
	scope *Scope,
	indexes indexLookupsByTable,
) (sql.Node, transform.TreeIdentity, error) {
	childSelector := func(c transform.Context) bool {
		switch n := c.Node.(type) {
		// We can't push any indexes down to a table has already had an index pushed down it
		case *plan.IndexedTableAccess:
			return false
		case *plan.RecursiveCte:
			// TODO: fix memory IndexLookup bugs that are not reproduceable in Dolt
			// this probably fails for *plan.Union also, we just don't have tests for it
			return false
		case *plan.JoinNode:
			// avoid changing anti and semi join condition indexes
			return !n.Op.IsPartial() && !n.Op.IsFullOuter()
		}

		switch n := c.Parent.(type) {
		// For IndexedJoins, if we are already using indexed access during query execution for the secondary table,
		// replacing the secondary table with an indexed lookup will have no effect on the result of the join, but
		// *will* inappropriately remove the filter from the predicate.
		// TODO: the analyzer should combine these indexed lookups better
		case *plan.JoinNode:
			if n.Op.IsMerge() {
				return false
			} else if n.Op.IsLookup() || n.Op.IsLeftOuter() {
				return c.ChildNum == 0
			}
		case *plan.TableAlias:
			// For a TableAlias, we apply this pushdown to the
			// TableAlias, but not to the resolved table directly
			// beneath it.
			return false
		case *plan.Window:
			// Windows operate across the rows they see and cannot
			// have filters pushed below them. If there is an index
			// pushdown, it will get picked up in the isolated pass
			// run by the filters pushdown transform.
			return false
		case *plan.Filter:
			// Can't push Filter Nodes below Limit Nodes
			if _, ok := c.Node.(*plan.Limit); ok {
				return false
			}
			if p, ok := c.Node.(*plan.Project); ok {
				if _, ok := p.Child.(*plan.Limit); ok {
					return false
				}
			}
		}
		return true
	}

	return transform.NodeWithCtx(n, childSelector, func(c transform.Context) (sql.Node, transform.TreeIdentity, error) {
		switch node := c.Node.(type) {
		case *plan.TableAlias:
			table, same, err := pushdownIndexesToTable(a, node, indexes)
			if err != nil {
				return nil, transform.SameTree, err
			}
			if same {
				return c.Node, transform.SameTree, nil
			}
			n, _, err := transform.Node(table, func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
				ita, ok := n.(*plan.IndexedTableAccess)
				if !ok {
					return n, transform.SameTree, nil
				}

				newExprs, same, err := FixFieldIndexesOnExpressions(scope, a, table.Schema(), ita.Expressions()...)
				if err != nil {
					return nil, transform.SameTree, err
				}
				if same {
					return n, transform.SameTree, nil
				}
				n, err = ita.WithExpressions(newExprs...)
				if err != nil {
					return nil, transform.SameTree, err
				}
				return n, transform.NewTree, nil
			})
			return n, transform.NewTree, err
		case *plan.ResolvedTable:
			table, sameTab, err := pushdownIndexesToTable(a, node, indexes)
			if err != nil {
				return nil, transform.SameTree, err
			}

			// We can't use pushdownFixIndexes() here, because it uses the schema of children, and
			// ResolvedTable doesn't have any.
			if sameTab {
				return c.Node, transform.SameTree, nil
			}
			n, _, err := FixFieldIndexesForTableNode(ctx, a, table, scope)
			if err != nil {
				return nil, transform.SameTree, err
			}
			return n, transform.NewTree, nil
		default:
			return pushdownFixIndices(a, node, scope)
		}
	})
}

// pushdownFiltersToTable attempts to push down filters to indexes that can accept them.
func getPredicateExprsHandledByLookup(ctx *sql.Context, a *Analyzer, idxTable *plan.IndexedTableAccess, lookup *indexLookup, tableAliases TableAliases) ([]sql.Expression, error) {
	filteredIdx, ok := idxTable.Index().(sql.FilteredIndex)
	if !ok {
		return nil, nil
	}
	// Spatial Indexes are lossy, so do not remove filter node above the lookup
	if filteredIdx.IsSpatial() {
		return nil, nil
	}

	idxFilters := splitConjunction(lookup.expr)
	if len(idxFilters) == 0 {
		return nil, nil
	}
	idxFilters = normalizeExpressions(tableAliases, idxFilters...)

	handled := filteredIdx.HandledFilters(idxFilters)
	if len(handled) == 0 {
		return nil, nil
	}

	a.Log(
		"table %q transformed with pushdown of filters to index %s, %d filters handled of %d",
		idxTable.Name(),
		idxTable.Index().ID(),
		len(handled),
		len(idxFilters),
	)
	return handled, nil
}

// pushdownFiltersToTable attempts to push filters to tables that can accept them
func pushdownFiltersToTable(
	ctx *sql.Context,
	a *Analyzer,
	tableNode sql.NameableNode,
	scope *Scope,
	filters *filterSet,
	tableAliases TableAliases,
) (sql.Node, transform.TreeIdentity, error) {
	switch tableNode.(type) {
	case *plan.ResolvedTable, *plan.TableAlias, *plan.IndexedTableAccess, *plan.ValueDerivedTable:
		// only subset of nodes can be sql.FilteredTables
	default:
		return nil, transform.SameTree, ErrInvalidNodeType.New("pushdownFiltersToTable", tableNode)
	}

	table := getTable(tableNode)
	if table == nil {
		return tableNode, transform.SameTree, nil
	}

	ft, ok := table.(sql.FilteredTable)
	if !ok {
		return tableNode, transform.SameTree, nil
	}

	// push filters for this table onto the table itself
	tableFilters := filters.availableFiltersForTable(ctx, tableNode.Name())
	if len(tableFilters) == 0 {
		return tableNode, transform.SameTree, nil
	}
	handledFilters := getHandledFilters(ctx, tableNode.Name(), ft, tableAliases, filters)
	filters.markFiltersHandled(handledFilters...)

	handledFilters, _, err := FixFieldIndexesOnExpressions(scope, a, tableNode.Schema(), handledFilters...)
	if err != nil {
		return nil, transform.SameTree, err
	}

	table = ft.WithFilters(ctx, handledFilters)
	a.Log(
		"table %q transformed with pushdown of filters, %d filters handled of %d",
		tableNode.Name(),
		len(handledFilters),
		len(tableFilters),
	)
	return withTable(tableNode, table)
}

// getHandledFilters returns the filter expressions that the specified table can handle. This
// function takes care of normalizing the available filter expressions, which is required for
// FilteredTable.HandledFilters to correctly identify what filter expressions it can handle.
// The returned filter expressions are the denormalized expressions as expected in other parts
// of the analyzer code (e.g. FixFieldIndexes).
func getHandledFilters(ctx *sql.Context, tableNameOrAlias string, ft sql.FilteredTable, tableAliases TableAliases, filters *filterSet) []sql.Expression {
	tableFilters := filters.availableFiltersForTable(ctx, tableNameOrAlias)

	normalizedFilters := normalizeExpressions(tableAliases, tableFilters...)
	normalizedToDenormalizedFilterMap := make(map[sql.Expression]sql.Expression)
	for i, normalizedFilter := range normalizedFilters {
		normalizedToDenormalizedFilterMap[normalizedFilter] = tableFilters[i]
	}

	handledNormalizedFilters := ft.HandledFilters(normalizedFilters)
	handledDenormalizedFilters := make([]sql.Expression, len(handledNormalizedFilters))
	for i, handledFilter := range handledNormalizedFilters {
		if val, ok := normalizedToDenormalizedFilterMap[handledFilter]; ok {
			handledDenormalizedFilters[i] = val
		} else {
			handledDenormalizedFilters[i] = handledFilter
		}
	}

	return handledDenormalizedFilters
}

// pushdownFiltersToAboveTable introduces a filter node with the given predicate
func pushdownFiltersToAboveTable(
	ctx *sql.Context,
	a *Analyzer,
	tableNode sql.NameableNode,
	scope *Scope,
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

		handled, _, err := FixFieldIndexesOnExpressions(scope, a, tableNode.Schema(), tableFilters...)
		if err != nil {
			return nil, transform.SameTree, err
		}

		pushedDownFilterExpression = expression.JoinAnd(handled...)

		a.Log(
			"pushed down filters %s above table %q, %d filters handled of %d",
			handled,
			tableNode.Name(),
			len(handled),
			len(tableFilters),
		)
	}

	switch tableNode.(type) {
	case *plan.ResolvedTable, *plan.TableAlias, *plan.IndexedTableAccess, *plan.ValueDerivedTable:
		node, _, err := withTable(tableNode, table)
		if plan.ErrInvalidLookupForIndexedTable.Is(err) {
			node = tableNode
		} else if err != nil {
			return nil, transform.SameTree, err
		}

		if pushedDownFilterExpression != nil {
			return plan.NewFilter(pushedDownFilterExpression, node), transform.NewTree, nil
		}

		return node, transform.NewTree, nil
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
	handled := filters.availableFiltersForTable(ctx, sa.Name())
	if len(handled) == 0 {
		return sa, transform.SameTree, nil
	}
	filters.markFiltersHandled(handled...)
	schema := sa.Schema()
	handled, _, err := FixFieldIndexesOnExpressions(nil, a, schema, handled...)
	if err != nil {
		return nil, transform.SameTree, err
	}

	// |handled| is in terms of the parent schema, and in particular the
	// |Source| is the alias name. Rewrite it to refer to the |sa.Child|
	// schema instead.
	childSchema := sa.Child.Schema()
	expressionsForChild := make([]sql.Expression, len(handled))
	for i, h := range handled {
		expressionsForChild[i], _, err = transform.Expr(h, func(e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
			if gt, ok := e.(*expression.GetField); ok {
				col := childSchema[gt.Index()]
				return gt.WithTable(col.Source).WithName(col.Name), transform.NewTree, nil
			}
			return e, transform.SameTree, nil
		})
	}

	n, err := sa.WithChildren(plan.NewFilter(expression.JoinAnd(expressionsForChild...), sa.Child))
	if err != nil {
		return nil, transform.SameTree, err
	}
	return n, transform.NewTree, nil
}

// pushdownIndexesToTable attempts to convert filter predicates to indexes on tables that implement
// sql.IndexAddressableTable
func pushdownIndexesToTable(a *Analyzer, tableNode sql.NameableNode, indexes map[string]*indexLookup) (sql.Node, transform.TreeIdentity, error) {
	return transform.Node(tableNode, func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		switch n := n.(type) {
		case *plan.ResolvedTable:
			table := getTable(tableNode)
			if table == nil {
				return n, transform.SameTree, nil
			}
			if tw, ok := table.(sql.TableWrapper); ok {
				table = tw.Underlying()
			}
			if _, ok := table.(sql.IndexAddressableTable); ok {
				indexLookup, ok := indexes[tableNode.Name()]
				if ok && indexLookup.lookup.Index.CanSupport(indexLookup.lookup.Ranges...) {
					a.Log("table %q transformed with pushdown of index", tableNode.Name())
					ret, err := plan.NewStaticIndexedAccessForResolvedTable(n, indexLookup.lookup)
					if plan.ErrInvalidLookupForIndexedTable.Is(err) {
						return n, transform.SameTree, nil
					}
					if err != nil {
						return nil, transform.SameTree, err
					}
					return ret, transform.NewTree, nil
				}
			}
		}
		return n, transform.SameTree, nil
	})
}

// removePushedDownPredicates removes all handled filter predicates from the filter given and returns. If all
// predicates have been handled, it replaces the filter with its child.
func removePushedDownPredicates(ctx *sql.Context, a *Analyzer, node *plan.Filter, filters *filterSet) (sql.Node, transform.TreeIdentity, error) {
	if filters.handledCount() == 0 {
		a.Log("no handled filters, leaving filter untouched")
		return node, transform.SameTree, nil
	}

	// figure out if the filter's filters were all handled
	filterExpressions := splitConjunction(node.Expression)
	unhandled := subtractExprSet(filterExpressions, filters.handledFilters)
	if len(unhandled) == 0 {
		a.Log("filter node has no unhandled filters, so it will be removed")
		return node.Child, transform.NewTree, nil
	}

	if len(unhandled) == len(filterExpressions) {
		a.Log("no filters removed from filter node")
		return node, transform.SameTree, nil
	}

	a.Log(
		"filters removed from filter node: %s\nfilter has now %d filters: %s",
		filters.handledFilters,
		len(unhandled),
		unhandled,
	)

	return plan.NewFilter(expression.JoinAnd(unhandled...), node.Child), transform.NewTree, nil
}

// getIndexesByTable returns applicable index lookups for each table named in the query node given
func getIndexesByTable(ctx *sql.Context, a *Analyzer, node sql.Node, scope *Scope) (indexLookupsByTable, error) {
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

		indexAnalyzer, err := newIndexAnalyzerForNode(ctx, node)
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

func replacePkSortHelper(ctx *sql.Context, scope *Scope, node sql.Node, sortNode *plan.Sort) (sql.Node, transform.TreeIdentity, error) {
	switch n := node.(type) {
	case *plan.Sort:
		sortNode = n // TODO: this only preserves the most recent Sort node
	// TODO: make this work with IndexedTableAccess, if we are statically sorting by the same col
	case *plan.ResolvedTable:
		// No sort node above this, so do nothing
		if sortNode == nil {
			return n, transform.SameTree, nil
		}
		tableAliases, err := getTableAliases(sortNode, scope)
		if err != nil {
			return n, transform.SameTree, nil
		}
		sfExprs := normalizeExpressions(tableAliases, sortNode.SortFields.ToExpressions()...)
		sfAliases := aliasedExpressionsInNode(sortNode)
		table := n.Table
		if w, ok := table.(sql.TableWrapper); ok {
			table = w.Underlying()
		}
		idxTbl, ok := table.(sql.IndexAddressableTable)
		if !ok {
			return n, transform.SameTree, nil
		}
		idxs, err := idxTbl.GetIndexes(ctx)
		if err != nil {
			return nil, transform.SameTree, err
		}

		var pkIndex sql.Index // TODO: support secondary indexes
		for _, idx := range idxs {
			if idx.ID() == "PRIMARY" {
				pkIndex = idx
				break
			}
		}
		if pkIndex == nil {
			return n, transform.SameTree, nil
		}

		pkColNames := pkIndex.Expressions()
		if len(sfExprs) > len(pkColNames) {
			return n, transform.SameTree, nil
		}
		for i, fieldExpr := range sfExprs {
			// TODO: could generalize this to more monotonic expressions.
			// For example, order by x+1 is ok, but order by mod(x) is not
			if sortNode.SortFields[0].Order != sortNode.SortFields[i].Order {
				return n, transform.SameTree, nil
			}
			fieldName := fieldExpr.String()
			if alias, ok := sfAliases[strings.ToLower(pkColNames[i])]; ok && alias == fieldName {
				continue
			}
			if pkColNames[i] != fieldExpr.String() {
				return n, transform.SameTree, nil
			}
		}

		// Create lookup based off of PrimaryKey
		indexBuilder := sql.NewIndexBuilder(pkIndex)
		lookup, err := indexBuilder.Build(ctx)
		if err != nil {
			return nil, transform.SameTree, err
		}
		lookup.IsReverse = sortNode.SortFields[0].Order == sql.Descending
		// Some Primary Keys (like doltHistoryTable) are not in order
		if oi, ok := pkIndex.(sql.OrderedIndex); ok && ((lookup.IsReverse && !oi.Reversible()) || oi.Order() == sql.IndexOrderNone) {
			return n, transform.SameTree, nil
		}
		if !pkIndex.CanSupport(lookup.Ranges...) {
			return n, transform.SameTree, nil
		}
		nn, err := plan.NewStaticIndexedAccessForResolvedTable(n, lookup)
		if err != nil {
			return nil, transform.SameTree, err
		}

		return nn, transform.NewTree, err
	}

	allSame := transform.SameTree
	newChildren := make([]sql.Node, len(node.Children()))
	for i, child := range node.Children() {
		var err error
		same := transform.SameTree
		switch c := child.(type) {
		case *plan.Project, *plan.TableAlias, *plan.ResolvedTable, *plan.Filter, *plan.Limit, *plan.Sort:
			newChildren[i], same, err = replacePkSortHelper(ctx, scope, child, sortNode)
		default:
			newChildren[i] = c
		}
		if err != nil {
			return nil, transform.SameTree, err
		}
		allSame = allSame && same
	}

	if allSame {
		return node, transform.SameTree, nil
	}

	// if sort node was replaced with indexed access, drop sort node
	if node == sortNode {
		return newChildren[0], transform.NewTree, nil
	}

	newNode, err := node.WithChildren(newChildren...)
	if err != nil {
		return nil, transform.SameTree, err
	}
	return newNode, transform.NewTree, nil
}

// replacePkSort applies an IndexAccess when there is an `OrderBy` over a prefix of any `PrimaryKey`s
func replacePkSort(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	return replacePkSortHelper(ctx, scope, n, nil)
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

// pushdownFixIndices fixes field indices for non-join expressions (replanJoin
// is responsible for join filters and conditions.)
func pushdownFixIndices(a *Analyzer, n sql.Node, scope *Scope) (sql.Node, transform.TreeIdentity, error) {
	switch n := n.(type) {
	case *plan.JoinNode, *plan.HashLookup:
		return n, transform.SameTree, nil
	}
	return FixFieldIndexesForExpressions(a, n, scope)
}
