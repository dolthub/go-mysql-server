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
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/information_schema"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
)

// pushdownFilters attempts to push conditions in filters down to individual tables. Tables that implement
// sql.FilteredTable will get such conditions applied to them. For conditions that have an index, tables that implement
// sql.IndexAddressableTable will get an appropriate index lookup applied.
func pushdownFilters(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	span, ctx := ctx.Span("pushdown_filters")
	defer span.Finish()

	if !canDoPushdown(n) {
		return n, transform.SameTree, nil
	}

	return pushdownFiltersAtNode(ctx, a, n, scope, sel)
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
	defer span.Finish()

	if !canDoPushdown(n) {
		return n, transform.SameTree, nil
	}

	tableAliases, err := getTableAliases(n, scope)
	if err != nil {
		return nil, transform.SameTree, err
	}

	return transformPushdownSubqueryAliasFilters(ctx, a, n, scope, tableAliases)
}

// pushdownProjections attempts to push projections down to individual tables that implement sql.ProjectTable
func pushdownProjections(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	span, ctx := ctx.Span("pushdown_projections")
	defer span.Finish()

	if !canDoPushdown(n) {
		return n, transform.SameTree, nil
	}
	if !canProject(n, a) {
		return n, transform.SameTree, nil
	}

	return transformPushdownProjections(ctx, a, n, scope)
}

func canProject(n sql.Node, a *Analyzer) bool {
	switch n.(type) {
	case *plan.Update, *plan.RowUpdateAccumulator, *plan.DeleteFrom, *plan.Block, *plan.BeginEndBlock, *plan.TriggerBeginEndBlock:
		return false
	}

	// Pushdown of projections interferes with subqueries on the same table: the table gets two different sets of
	// projected columns pushed down, once for its alias in the subquery and once for its alias outside. For that reason,
	// skip pushdown for any query with a subquery in it.
	// TODO: fix this
	containsSubquery := false
	transform.InspectExpressions(n, func(e sql.Expression) bool {
		if _, ok := e.(*plan.Subquery); ok {
			containsSubquery = true
			return false
		}
		return true
	})

	if containsSubquery {
		a.Log("skipping pushdown of projection for query with subquery")
		return false
	}

	containsIndexedJoin := false
	transform.Inspect(n, func(node sql.Node) bool {
		if _, ok := node.(*plan.IndexedJoin); ok {
			containsIndexedJoin = true
			return false
		}
		return true

	})

	if containsIndexedJoin {
		a.Log("skipping pushdown of projection for query with an indexed join")
		return false
	}

	// Because analysis runs more than once on subquery, it's possible for projection pushdown logic to be applied
	// multiple times. It's totally undefined what happens when you push a projection down to a table that already has
	// one, and shouldn't happen. We don't have the necessary interface to interrogate a projected table about its
	// projection, so we do this for now.
	// TODO: this is a hack, we shouldn't use decorator nodes for logic like this.
	alreadyPushedDown := false
	transform.Inspect(n, func(n sql.Node) bool {
		if n, ok := n.(*plan.DecoratedNode); ok && strings.Contains(n.String(), "Projected table access on") {
			alreadyPushedDown = true
			return false
		}
		return true
	})

	return !alreadyPushedDown
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
	switch n := c.Parent.(type) {
	case *plan.TableAlias:
		return false
	case *plan.Window:
		// Windows operate across the rows they see and cannot have
		// filters pushed below them. Instead, the step will be run
		// again by the Transform function, starting at this node.
		return false
	case *plan.IndexedJoin:
		if n.JoinType() == plan.JoinTypeLeft || n.JoinType() == plan.JoinTypeRight {
			return c.ChildNum == 0
		}
		return true
	case *plan.LeftJoin:
		return c.ChildNum == 0
	case *plan.RightJoin:
		return c.ChildNum == 1
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
		case *plan.TableAlias, *plan.ResolvedTable, *plan.IndexedTableAccess, *plan.ValueDerivedTable, *information_schema.ColumnsTable:
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
				n, sameFix, err := FixFieldIndexesForExpressions(ctx, a, n, scope)
				if err != nil {
					return nil, transform.SameTree, err
				}
				return n, samePred && sameFix, nil
			case *plan.IndexedTableAccess:
				if plan.GetIndexLookup(node) == nil {
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
				return node, len(handled) == 0, nil
			case *plan.TableAlias, *plan.ResolvedTable, *plan.ValueDerivedTable, *information_schema.ColumnsTable:
				n, samePred, err := pushdownFiltersToTable(ctx, a, node.(NameableNode), scope, filters, tableAliases)
				if err != nil {
					return nil, transform.SameTree, err
				}
				n, sameFix, err := FixFieldIndexesForExpressions(ctx, a, n, scope)
				if err != nil {
					return nil, transform.SameTree, err
				}
				return n, samePred && sameFix, nil
			default:
				return FixFieldIndexesForExpressions(ctx, a, node, scope)
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
				n, _, err = FixFieldIndexesForExpressions(ctx, a, n, scope)
				if err != nil {
					return nil, transform.SameTree, err
				}
				return n, transform.NewTree, nil
			case *plan.TableAlias, *plan.ResolvedTable, *plan.IndexedTableAccess, *plan.ValueDerivedTable, *information_schema.ColumnsTable:
				table, same, err := pushdownFiltersToAboveTable(ctx, a, node.(NameableNode), scope, filters)
				if err != nil {
					return nil, transform.SameTree, err
				}
				if same {
					return node, transform.SameTree, nil
				}
				node, _, err = FixFieldIndexesForExpressions(ctx, a, table, scope)
				if err != nil {
					return nil, transform.SameTree, err
				}
				return node, transform.NewTree, nil
			default:
				return FixFieldIndexesForExpressions(ctx, a, node, scope)
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
//   that optimization is handled in transformPushdownFilters.
func convertFiltersToIndexedAccess(
	ctx *sql.Context,
	a *Analyzer,
	n sql.Node,
	scope *Scope,
	indexes indexLookupsByTable,
) (sql.Node, transform.TreeIdentity, error) {
	childSelector := func(c transform.Context) bool {
		switch c.Node.(type) {
		// We can't push any indexes down to a table has already had an index pushed down it
		case *plan.IndexedTableAccess:
			return false
		case *plan.RecursiveCte:
			// TODO: fix memory IndexLookup bugs that are not reproduceable in Dolt
			// this probably fails for *plan.Union also, we just don't have tests for it
			return false
		}

		switch c.Parent.(type) {
		// For IndexedJoins, if we are already using indexed access during query execution for the secondary table,
		// replacing the secondary table with an indexed lookup will have no effect on the result of the join, but
		// *will* inappropriately remove the filter from the predicate.
		// TODO: the analyzer should combine these indexed lookups better
		case *plan.IndexedJoin:
			// Left and right joins can push down indexes for the primary table, but not the secondary. See comment
			// on transformPushdownFilters
			return c.ChildNum == 0
		case *plan.LeftJoin:
			return c.ChildNum == 0
		case *plan.RightJoin:
			return c.ChildNum == 1
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
		}
		return true
	}

	return transform.NodeWithCtx(n, childSelector, func(c transform.Context) (sql.Node, transform.TreeIdentity, error) {
		switch node := c.Node.(type) {
		// TODO: some indexes, once pushed down, can be safely removed from the filter. But not all of them, as currently
		//  implemented -- some indexes return more values than strictly match.
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

				newExprs, same, err := FixFieldIndexesOnExpressions(ctx, scope, a, table.Schema(), ita.Expressions()...)
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

			// We can't use FixFieldIndexesForExpressions here, because it uses the schema of children, and
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
			return FixFieldIndexesForExpressions(ctx, a, node, scope)
		}
	})
}

// pushdownFiltersToTable attempts to push down filters to indexes that can accept them.
func getPredicateExprsHandledByLookup(ctx *sql.Context, a *Analyzer, idxTable *plan.IndexedTableAccess, lookup *indexLookup, tableAliases TableAliases) ([]sql.Expression, error) {
	filteredIdx, ok := idxTable.Index().(sql.FilteredIndex)
	if !ok {
		return nil, nil
	}

	idxFilters := splitConjunction(lookup.expr)
	if len(idxFilters) == 0 {
		return nil, nil
	}
	idxFilters = normalizeExpressions(ctx, tableAliases, idxFilters...)

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
	tableNode NameableNode,
	scope *Scope,
	filters *filterSet,
	tableAliases TableAliases,
) (sql.Node, transform.TreeIdentity, error) {
	switch tableNode.(type) {
	case *plan.ResolvedTable, *plan.TableAlias, *plan.IndexedTableAccess, *plan.ValueDerivedTable, *information_schema.ColumnsTable:
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

	handledFilters, _, err := FixFieldIndexesOnExpressions(ctx, scope, a, tableNode.Schema(), handledFilters...)
	if err != nil {
		return nil, transform.SameTree, err
	}

	table = ft.WithFilters(ctx, handledFilters)
	newTableNode := plan.NewDecoratedNode(
		fmt.Sprintf("Filtered table access on %v", handledFilters),
		tableNode)

	a.Log(
		"table %q transformed with pushdown of filters, %d filters handled of %d",
		tableNode.Name(),
		len(handledFilters),
		len(tableFilters),
	)
	return withTable(newTableNode, table)
}

// getHandledFilters returns the filter expressions that the specified table can handle. This
// function takes care of normalizing the available filter expressions, which is required for
// FilteredTable.HandledFilters to correctly identify what filter expressions it can handle.
// The returned filter expressions are the denormalized expressions as expected in other parts
// of the analyzer code (e.g. FixFieldIndexes).
func getHandledFilters(ctx *sql.Context, tableNameOrAlias string, ft sql.FilteredTable, tableAliases TableAliases, filters *filterSet) []sql.Expression {
	tableFilters := filters.availableFiltersForTable(ctx, tableNameOrAlias)

	normalizedFilters := normalizeExpressions(ctx, tableAliases, tableFilters...)
	normalizedToDenormalizedFilterMap := make(map[sql.Expression]sql.Expression)
	for i, normalizedFilter := range normalizedFilters {
		normalizedToDenormalizedFilterMap[normalizedFilter] = tableFilters[i]
	}

	handledNormalizedFilters := ft.HandledFilters(normalizedFilters)
	handledDenormalizedFilters := make([]sql.Expression, len(handledNormalizedFilters))
	for i, handledFilter := range handledNormalizedFilters {
		handledDenormalizedFilters[i] = normalizedToDenormalizedFilterMap[handledFilter]
	}

	return handledDenormalizedFilters
}

// pushdownFiltersToAboveTable introduces a filter node with the given predicate
func pushdownFiltersToAboveTable(
	ctx *sql.Context,
	a *Analyzer,
	tableNode NameableNode,
	scope *Scope,
	filters *filterSet,
) (sql.Node, transform.TreeIdentity, error) {
	table := getTable(tableNode)
	if table == nil {
		return tableNode, transform.SameTree, nil
	}

	// Move any remaining filters for the table directly above the table itself
	var pushedDownFilterExpression sql.Expression
	if tableFilters := filters.availableFiltersForTable(ctx, tableNode.Name()); len(tableFilters) > 0 {
		filters.markFiltersHandled(tableFilters...)

		handled, _, err := FixFieldIndexesOnExpressions(ctx, scope, a, tableNode.Schema(), tableFilters...)
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
	case *plan.ResolvedTable, *plan.TableAlias, *plan.IndexedTableAccess, *plan.ValueDerivedTable, *information_schema.ColumnsTable:
		node, _, err := withTable(tableNode, table)
		if err != nil {
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
	handled, _, err := FixFieldIndexesOnExpressions(ctx, nil, a, schema, handled...)
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
func pushdownIndexesToTable(a *Analyzer, tableNode NameableNode, indexes map[string]*indexLookup) (sql.Node, transform.TreeIdentity, error) {
	return transform.Node(tableNode, func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		switch n := n.(type) {
		case *plan.ResolvedTable:
			table := getTable(tableNode)
			if table == nil {
				return n, transform.SameTree, nil
			}
			if _, ok := table.(sql.IndexAddressableTable); ok {
				indexLookup, ok := indexes[tableNode.Name()]
				if ok {
					a.Log("table %q transformed with pushdown of index", tableNode.Name())
					return plan.NewStaticIndexedTableAccess(n, indexLookup.lookup), transform.NewTree, nil
				}
			}
		}
		return n, transform.SameTree, nil
	})
}

func formatIndexDecoratorString(indexes ...sql.Index) []string {
	var indexStrs []string
	for _, idx := range indexes {
		var expStrs []string
		for _, e := range idx.Expressions() {
			expStrs = append(expStrs, e)
		}
		indexStrs = append(indexStrs, fmt.Sprintf("[%s]", strings.Join(expStrs, ",")))
	}
	return indexStrs
}

// pushdownProjectionsToTable attempts to push projected columns down to tables that implement sql.ProjectedTable.
func pushdownProjectionsToTable(
	a *Analyzer,
	tableNode NameableNode,
	fieldsByTable fieldsByTable,
	usedProjections fieldsByTable,
) (sql.Node, transform.TreeIdentity, error) {

	table := getTable(tableNode)
	if table == nil {
		return tableNode, transform.SameTree, nil
	}

	var newTableNode sql.Node = tableNode

	replacedTable := false
	if pt, ok := table.(sql.ProjectedTable); ok && len(fieldsByTable[tableNode.Name()]) > 0 {
		if usedProjections[tableNode.Name()] == nil {
			projectedFields := fieldsByTable[tableNode.Name()]
			table = pt.WithProjections(projectedFields)
			usedProjections[tableNode.Name()] = projectedFields
		}

		newTableNode = plan.NewDecoratedNode(
			fmt.Sprintf("Projected table access on %v",
				fieldsByTable[tableNode.Name()]), newTableNode)
		a.Log("table %q transformed with pushdown of projection", tableNode.Name())

		replacedTable = true
	}

	if !replacedTable {
		return tableNode, transform.SameTree, nil
	}

	switch tableNode.(type) {
	case *plan.ResolvedTable, *plan.TableAlias, *plan.IndexedTableAccess:
		node, _, err := withTable(newTableNode, table)
		if err != nil {
			return nil, transform.SameTree, err
		}

		return node, transform.NewTree, nil
	default:
		return nil, transform.SameTree, ErrInvalidNodeType.New("pushdown", tableNode)
	}
}

func transformPushdownProjections(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, transform.TreeIdentity, error) {
	usedFieldsByTable := make(fieldsByTable)
	fieldsByTable := getFieldsByTable(ctx, n)

	selector := func(c transform.Context) bool {
		switch c.Parent.(type) {
		case *plan.TableAlias:
			// When we hit a table alias, we don't want to descend farther into the tree for expression matches, which
			// would give us the original (unaliased) names of columns
			return false
		default:
			return true
		}
	}

	return transform.NodeWithCtx(n, selector, func(c transform.Context) (sql.Node, transform.TreeIdentity, error) {
		var nameable NameableNode

		switch c.Node.(type) {
		case *plan.TableAlias, *plan.ResolvedTable, *plan.IndexedTableAccess:
			nameable = c.Node.(NameableNode)
		}

		if nameable != nil {
			table, same, err := pushdownProjectionsToTable(a, nameable, fieldsByTable, usedFieldsByTable)
			if err != nil {
				return nil, transform.SameTree, err
			}
			if !same {
				n, _, err := FixFieldIndexesForExpressions(ctx, a, table, scope)
				if err != nil {
					return nil, transform.SameTree, err
				}
				return n, transform.NewTree, nil
			}
		}
		return FixFieldIndexesForExpressions(ctx, a, c.Node, scope)
	})
}

// removePushedDownPredicates removes all handled filter predicates from the filter given and returns. If all
// predicates have been handled, it replaces the filter with its child.
func removePushedDownPredicates(ctx *sql.Context, a *Analyzer, node *plan.Filter, filters *filterSet) (sql.Node, transform.TreeIdentity, error) {
	if filters.handledCount() == 0 {
		a.Log("no handled filters, leaving filter untouched")
		return node, transform.SameTree, nil
	}

	unhandled := filters.unhandledPredicates(ctx)
	if len(unhandled) == 0 {
		a.Log("filter node has no unhandled filters, so it will be removed")
		return node.Child, transform.NewTree, nil
	}

	a.Log(
		"filters removed from filter node: %s\nfilter has now %d filters",
		filters.handledFilters,
		len(unhandled),
	)

	return plan.NewFilter(expression.JoinAnd(unhandled...), node.Child), transform.NewTree, nil
}

// getIndexesByTable returns applicable index lookups for each table named in the query node given
func getIndexesByTable(ctx *sql.Context, a *Analyzer, node sql.Node, scope *Scope) (indexLookupsByTable, error) {
	indexSpan, _ := ctx.Span("getIndexesByTable")
	defer indexSpan.Finish()

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

// stripDecorations removes *plan.DecoratedNode that wrap plan.ResolvedTable instances.
// Without this step, some prepared statement reanalysis rules fail to identify
// filter-table relationships.
func stripDecorations(ctx *sql.Context, a *Analyzer, node sql.Node, scope *Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	return transform.Node(node, func(node sql.Node) (sql.Node, transform.TreeIdentity, error) {
		switch n := node.(type) {
		case *plan.DecoratedNode:
			return n.Child, transform.NewTree, nil
		default:
			return node, transform.SameTree, nil
		}
	})
}

func replacePkSort(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	return transform.NodeWithCtx(n, nil, func(tc transform.Context) (sql.Node, transform.TreeIdentity, error) {
		n := tc.Node

		// Find order by nodes
		s, ok := n.(*plan.Sort)
		if !ok {
			return n, transform.SameTree, nil
		}

		// Must be sorting by ascending
		for _, field := range s.SortFields {
			if field.Order != sql.Ascending {
				return n, transform.SameTree, nil
			}
		}

		// Check for any alias projections
		var rs *plan.ResolvedTable
		aliasMap := make(map[string]string)
		pj, ok := s.UnaryNode.Child.(*plan.Project)
		if ok {
			// If there is a projection, its immediate child must be ResolvedTable
			if rs, ok = pj.UnaryNode.Child.(*plan.ResolvedTable); !ok {
				return n, transform.SameTree, nil
			}
			// Extract aliases
			for _, expr := range pj.Expressions() {
				if alias, ok := expr.(*expression.Alias); ok {
					aliasMap[alias.Name()] = alias.UnaryExpression.Child.String()
				}
			}
		} else {
			// Otherwise, sorts immediate child must be ResolvedTable
			if rs, ok = s.UnaryNode.Child.(*plan.ResolvedTable); !ok {
				return n, transform.SameTree, nil
			}
		}

		// Extract primary key columns from index to maintain order
		idxTbl, ok := rs.Table.(sql.IndexedTable)
		if !ok {
			return n, transform.SameTree, nil
		}
		idxs, err := idxTbl.GetIndexes(ctx)
		if err != nil {
			return nil, transform.SameTree, err
		}

		// Extract primary index
		var pkIndex sql.Index
		for _, idx := range idxs {
			oi, ok := idx.(sql.OrderedIndex)
			if !ok || oi.Order() != sql.IndexOrderAsc {
				continue
			}
			if idx.ID() == "PRIMARY" {
				pkIndex = idx
				break
			}
		}
		if pkIndex == nil {
			return n, transform.SameTree, nil
		}

		// Get primary key column names; these are qualified
		pkColNames := pkIndex.Expressions()

		// Extract SortField Column Names
		var sfColNames []string
		for _, field := range s.SortFields {
			gf, ok := field.Column.(*expression.GetField)
			if !ok {
				return n, transform.SameTree, nil
			}
			// Resolve aliases; aliases should have empty table in GetField
			if name, ok := aliasMap[gf.String()]; ok {
				sfColNames = append(sfColNames, name)
			} else {
				sfColNames = append(sfColNames, gf.String())
			}
		}

		// SortField is definitely not a prefix to PrimaryKey
		if len(sfColNames) > len(pkColNames) {
			return n, transform.SameTree, nil
		}

		// Check if SortField is a prefix to PrimaryKey
		for i := 0; i < len(sfColNames); i++ {
			// Stop when column names stop matching
			if sfColNames[i] != pkColNames[i] {
				return n, transform.SameTree, nil
			}
		}

		// Create lookup based off of PrimaryKey
		indexBuilder := sql.NewIndexBuilder(ctx, pkIndex)
		lookup, err := indexBuilder.Build(ctx)
		if err != nil {
			return nil, transform.SameTree, err
		}
		newNode := plan.NewStaticIndexedTableAccess(rs, lookup)

		// Don't forget aliases
		if pj != nil {
			resNode, err := pj.WithChildren(newNode)
			if err != nil {
				return nil, transform.SameTree, err
			}
			return resNode, transform.NewTree, nil
		}

		return newNode, transform.NewTree, nil
	})
}

// convertIsNullForIndexes converts all nested IsNull(col) expressions to Equals(col, nil) expressions, as they are
// equivalent as far as the index interfaces are concerned.
func convertIsNullForIndexes(ctx *sql.Context, e sql.Expression) sql.Expression {
	expr, _, _ := transform.Expr(e, func(e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
		isNull, ok := e.(*expression.IsNull)
		if !ok {
			return e, transform.SameTree, nil
		}
		return expression.NewNullSafeEquals(isNull.Child, expression.NewLiteral(nil, sql.Null)), transform.NewTree, nil
	})
	return expr
}
