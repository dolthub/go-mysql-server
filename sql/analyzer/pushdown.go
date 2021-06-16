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
	"github.com/dolthub/go-mysql-server/sql/plan"
)

// pushdownFilters attempts to push conditions in filters down to individual tables. Tables that implement
// sql.FilteredTable will get such conditions applied to them. For conditions that have an index, tables that implement
// sql.IndexAddressableTable will get an appropriate index lookup applied.
func pushdownFilters(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	span, ctx := ctx.Span("pushdown_filters")
	defer span.Finish()

	if !canDoPushdown(n) {
		return n, nil
	}

	indexes, err := getIndexesByTable(ctx, a, n, scope)
	if err != nil {
		return nil, err
	}

	tableAliases, err := getTableAliases(n, scope)
	if err != nil {
		return nil, err
	}

	n, err = convertFiltersToIndexedAccess(ctx, a, n, scope, indexes)
	if err != nil {
		return nil, err
	}

	return transformPushdownFilters(ctx, a, n, scope, tableAliases)
}

// pushdownSubqueryAliasFilters attempts to push conditions in filters down to
// individual subquery aliases.
func pushdownSubqueryAliasFilters(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	span, ctx := ctx.Span("pushdown_subquery_alias_filters")
	defer span.Finish()

	if !canDoPushdown(n) {
		return n, nil
	}

	tableAliases, err := getTableAliases(n, scope)
	if err != nil {
		return nil, err
	}

	return transformPushdownSubqueryAliasFilters(ctx, a, n, scope, tableAliases)
}

// pushdownProjections attempts to push projections down to individual tables that implement sql.ProjectTable
func pushdownProjections(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	span, ctx := ctx.Span("pushdown_projections")
	defer span.Finish()

	if !canDoPushdown(n) {
		return n, nil
	}
	if !canProject(n, a) {
		return n, nil
	}

	return transformPushdownProjections(ctx, a, n, scope)
}

func canProject(n sql.Node, a *Analyzer) bool {
	switch n.(type) {
	case *plan.Update, *plan.RowUpdateAccumulator, *plan.DeleteFrom:
		return false
	}

	// Pushdown of projections interferes with subqueries on the same table: the table gets two different sets of
	// projected columns pushed down, once for its alias in the subquery and once for its alias outside. For that reason,
	// skip pushdown for any query with a subquery in it.
	// TODO: fix this
	containsSubquery := false
	plan.InspectExpressions(n, func(e sql.Expression) bool {
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
	plan.Inspect(n, func(node sql.Node) bool {
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
	plan.Inspect(n, func(n sql.Node) bool {
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
func filterPushdownChildSelector(parent sql.Node, child sql.Node, childNum int) bool {
	switch n := parent.(type) {
	case *plan.TableAlias:
		return false
	case *plan.IndexedJoin:
		if n.JoinType() == plan.JoinTypeLeft || n.JoinType() == plan.JoinTypeRight {
			return childNum == 0
		}
		return true
	case *plan.LeftJoin:
		return childNum == 0
	case *plan.RightJoin:
		return childNum == 1
	}
	return true
}

// Like filterPushdownChildSelector, but for pushing filters down via the introduction of additional Filter nodes
// (for tables that can't treat the filter as an index lookup or accept it directly). In this case, we want to avoid
// introducing additional Filter nodes unnecessarily. This means only introducing new filter nodes when they are being
// pushed below a join or other structure.
func filterPushdownAboveTablesChildSelector(parent sql.Node, child sql.Node, childNum int) bool {
	// All the same restrictions that apply to pushing filters down in general apply here as well
	if !filterPushdownChildSelector(parent, child, childNum) {
		return false
	}
	switch parent.(type) {
	case *plan.Filter:
		switch child.(type) {
		// Don't bother pushing filters down above tables if the direct child node is a table. At best this
		// just splits the predicates into multiple filter nodes, and at worst it breaks other parts of the
		// analyzer that don't expect this structure in the tree.
		case *plan.TableAlias, *plan.ResolvedTable, *plan.IndexedTableAccess, *plan.ValueDerivedTable:
			return false
		}
	}

	return true
}

func transformPushdownFilters(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope, tableAliases TableAliases) (sql.Node, error) {
	applyFilteredTables := func(n *plan.Filter, filters *filterSet) (sql.Node, error) {
		return plan.TransformUpWithSelector(n, filterPushdownChildSelector, func(node sql.Node) (sql.Node, error) {
			switch node := node.(type) {
			case *plan.Filter:
				n, err := removePushedDownPredicates(ctx, a, node, filters)
				if err != nil {
					return nil, err
				}
				return FixFieldIndexesForExpressions(ctx, a, n, scope)
			case *plan.TableAlias, *plan.ResolvedTable, *plan.IndexedTableAccess, *plan.ValueDerivedTable:
				table, err := pushdownFiltersToTable(ctx, a, node.(NameableNode), scope, filters, tableAliases)
				if err != nil {
					return nil, err
				}
				return FixFieldIndexesForExpressions(ctx, a, table, scope)
			default:
				return FixFieldIndexesForExpressions(ctx, a, node, scope)
			}
		})
	}

	pushdownAboveTables := func(n sql.Node, filters *filterSet) (sql.Node, error) {
		return plan.TransformUpWithSelector(n, filterPushdownAboveTablesChildSelector, func(node sql.Node) (sql.Node, error) {
			switch node := node.(type) {
			case *plan.Filter:
				n, err := removePushedDownPredicates(ctx, a, node, filters)
				if err != nil {
					return nil, err
				}
				return FixFieldIndexesForExpressions(ctx, a, n, scope)
			case *plan.TableAlias, *plan.ResolvedTable, *plan.IndexedTableAccess, *plan.ValueDerivedTable:
				table, err := pushdownFiltersToAboveTable(ctx, a, node.(NameableNode), scope, filters)
				if err != nil {
					return nil, err
				}
				return FixFieldIndexesForExpressions(ctx, a, table, scope)
			default:
				return FixFieldIndexesForExpressions(ctx, a, node, scope)
			}
		})
	}

	// For each filter node, we want to push its predicates as low as possible.
	return plan.TransformUp(n, func(n sql.Node) (sql.Node, error) {
		switch n := n.(type) {
		case *plan.Filter:
			// Find all col exprs and group them by the table they mention so that we can keep track of which ones
			// have been pushed down and need to be removed from the parent filter
			filtersByTable := getFiltersByTable(n)
			filters := newFilterSet(n.Expression, filtersByTable, tableAliases)

			// Two passes: first push filters to any tables that implement sql.Filtered table directly
			node, err := applyFilteredTables(n, filters)
			if err != nil {
				return nil, err
			}

			// Then move filter predicates directly above their respective tables in joins
			return pushdownAboveTables(node, filters)
		default:
			return n, nil
		}
	})
}

func transformPushdownSubqueryAliasFilters(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope, tableAliases TableAliases) (sql.Node, error) {
	var filters *filterSet

	transformFilterNode := func(n *plan.Filter) (sql.Node, error) {
		return plan.TransformUpWithSelector(n, filterPushdownChildSelector, func(node sql.Node) (sql.Node, error) {
			switch node := node.(type) {
			case *plan.Filter:
				return removePushedDownPredicates(ctx, a, node, filters)
			case *plan.SubqueryAlias:
				return pushdownFiltersUnderSubqueryAlias(ctx, a, node, filters)
			default:
				return node, nil
			}
		})
	}

	// For each filter node, we want to push its predicates as low as possible.
	return plan.TransformUp(n, func(n sql.Node) (sql.Node, error) {
		switch n := n.(type) {
		case *plan.Filter:
			// First step is to find all col exprs and group them by the table they mention.
			filtersByTable := getFiltersByTable(n)
			filters = newFilterSet(n.Expression, filtersByTable, tableAliases)
			return transformFilterNode(n)
		default:
			return n, nil
		}
	})
}

// convertFiltersToIndexedAccess attempts to replace filter predicates with indexed accesses where possible
func convertFiltersToIndexedAccess(
	ctx *sql.Context,
	a *Analyzer,
	n sql.Node,
	scope *Scope,
	indexes indexLookupsByTable,
) (sql.Node, error) {
	childSelector := func(parent sql.Node, child sql.Node, childNum int) bool {
		switch child.(type) {
		// We can't push any indexes down to a table has already had an index pushed down it
		case *plan.IndexedTableAccess:
			return false
		}

		switch parent.(type) {
		// For IndexedJoins, if we are already using indexed access during query execution for the secondary table,
		// replacing the secondary table with an indexed lookup will have no effect on the result of the join, but
		// *will* inappropriately remove the filter from the predicate.
		// TODO: the analyzer should combine these indexed lookups better
		case *plan.IndexedJoin:
			// Left and right joins can push down indexes for the primary table, but not the secondary. See comment
			// on transformPushdownFilters
			return childNum == 0
		case *plan.LeftJoin:
			return childNum == 0
		case *plan.RightJoin:
			return childNum == 1
		case *plan.TableAlias:
			// For a TableAlias, we apply this pushdown to the
			// TableAlias, but not to the resolved table directly
			// beneath it.
			return false
		}
		return true
	}

	node, err := plan.TransformUpWithSelector(n, childSelector, func(node sql.Node) (sql.Node, error) {
		switch node := node.(type) {
		// TODO: some indexes, once pushed down, can be safely removed from the filter. But not all of them, as currently
		//  implemented -- some indexes return more values than strictly match.
		case *plan.TableAlias:
			table, err := pushdownIndexesToTable(a, node, indexes)
			if err != nil {
				return nil, err
			}

			return plan.TransformUp(table, func(n sql.Node) (sql.Node, error) {
				ita, ok := n.(*plan.IndexedTableAccess)
				if !ok {
					return n, nil
				}

				newExprs, err := FixFieldIndexesOnExpressions(ctx, scope, a, table.Schema(), ita.Expressions()...)
				if err != nil {
					return nil, err
				}

				return ita.WithExpressions(newExprs...)
			})
		case *plan.ResolvedTable:
			table, err := pushdownIndexesToTable(a, node, indexes)
			if err != nil {
				return nil, err
			}

			// We can't use FixFieldIndexesForExpressions here, because it uses the schema of children, and
			// ResolvedTable doesn't have any.
			return FixFieldIndexesForTableNode(ctx, a, table, scope)
		default:
			return FixFieldIndexesForExpressions(ctx, a, node, scope)
		}
	})

	if err != nil {
		return nil, err
	}

	return node, nil
}

// pushdownFiltersToTable attempts to push filters to tables that can accept them
func pushdownFiltersToTable(
	ctx *sql.Context,
	a *Analyzer,
	tableNode NameableNode,
	scope *Scope,
	filters *filterSet,
	tableAliases TableAliases,
) (sql.Node, error) {
	table := getTable(tableNode)
	if table == nil {
		return tableNode, nil
	}

	var newTableNode sql.Node = tableNode

	// Push any filters for this table onto the table itself if it's a sql.FilteredTable
	if ft, ok := table.(sql.FilteredTable); ok && len(filters.availableFiltersForTable(ctx, tableNode.Name())) > 0 {
		tableFilters := filters.availableFiltersForTable(ctx, tableNode.Name())
		handled := ft.HandledFilters(normalizeExpressions(ctx, tableAliases, tableFilters...))
		filters.markFiltersHandled(handled...)

		handled, err := FixFieldIndexesOnExpressions(ctx, scope, a, tableNode.Schema(), handled...)
		if err != nil {
			return nil, err
		}

		table = ft.WithFilters(ctx, handled)
		newTableNode = plan.NewDecoratedNode(
			fmt.Sprintf("Filtered table access on %v", handled),
			newTableNode)

		a.Log(
			"table %q transformed with pushdown of filters, %d filters handled of %d",
			tableNode.Name(),
			len(handled),
			len(tableFilters),
		)
	}

	switch tableNode.(type) {
	case *plan.ResolvedTable, *plan.TableAlias, *plan.IndexedTableAccess, *plan.ValueDerivedTable:
		return withTable(newTableNode, table)
	default:
		return nil, ErrInvalidNodeType.New("pushdownFiltersToTable", tableNode)
	}
}

// pushdownFiltersToAboveTable introduces a filter node with the given predicate
func pushdownFiltersToAboveTable(
	ctx *sql.Context,
	a *Analyzer,
	tableNode NameableNode,
	scope *Scope,
	filters *filterSet,
) (sql.Node, error) {
	table := getTable(tableNode)
	if table == nil {
		return tableNode, nil
	}

	// Move any remaining filters for the table directly above the table itself
	var pushedDownFilterExpression sql.Expression
	if tableFilters := filters.availableFiltersForTable(ctx, tableNode.Name()); len(tableFilters) > 0 {
		filters.markFiltersHandled(tableFilters...)

		handled, err := FixFieldIndexesOnExpressions(ctx, scope, a, tableNode.Schema(), tableFilters...)
		if err != nil {
			return nil, err
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
		node, err := withTable(tableNode, table)
		if err != nil {
			return nil, err
		}

		if pushedDownFilterExpression != nil {
			return plan.NewFilter(pushedDownFilterExpression, node), nil
		}

		return node, nil
	default:
		return nil, ErrInvalidNodeType.New("pushdownFiltersToAboveTable", tableNode)
	}
}

// pushdownFiltersUnderSubqueryAlias takes |filters| applying to the subquery
// alias a moves them under the subquery alias. Because the subquery alias is
// Opaque, it behaves a little bit like a FilteredTable, and pushing the
// filters down below it can help find index usage opportunities later in the
// analysis phase.
func pushdownFiltersUnderSubqueryAlias(ctx *sql.Context, a *Analyzer, sa *plan.SubqueryAlias, filters *filterSet) (sql.Node, error) {
	handled := filters.availableFiltersForTable(ctx, sa.Name())
	if len(handled) == 0 {
		return sa, nil
	}
	filters.markFiltersHandled(handled...)
	schema := sa.Schema()
	handled, err := FixFieldIndexesOnExpressions(ctx, nil, a, schema, handled...)
	if err != nil {
		return nil, err
	}

	// |handled| is in terms of the parent schema, and in particular the
	// |Source| is the alias name. Rewrite it to refer to the |sa.Child|
	// schema instead.
	childSchema := sa.Child.Schema()
	expressionsForChild := make([]sql.Expression, len(handled))
	for i, h := range handled {
		expressionsForChild[i], err = expression.TransformUp(ctx, h, func(e sql.Expression) (sql.Expression, error) {
			if gt, ok := e.(*expression.GetField); ok {
				col := childSchema[gt.Index()]
				return gt.WithTable(col.Source).WithName(col.Name), nil
			}
			return e, nil
		})
	}

	return sa.WithChildren(plan.NewFilter(expression.JoinAnd(expressionsForChild...), sa.Child))
}

// pushdownIndexesToTable attempts to convert filter predicates to indexes on tables that implement
// sql.IndexAddressableTable
func pushdownIndexesToTable(a *Analyzer, tableNode NameableNode, indexes map[string]*indexLookup) (sql.Node, error) {
	return plan.TransformUp(tableNode, func(n sql.Node) (sql.Node, error) {
		switch n := n.(type) {
		case *plan.ResolvedTable:
			table := getTable(tableNode)
			if table == nil {
				return n, nil
			}
			if _, ok := table.(sql.IndexAddressableTable); ok {
				indexLookup, ok := indexes[tableNode.Name()]
				if ok {
					a.Log("table %q transformed with pushdown of index", tableNode.Name())
					return plan.NewStaticIndexedTableAccess(n, indexLookup.lookup, indexLookup.indexes[0], indexLookup.exprs), nil
				}
			}
		}
		return n, nil
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
) (sql.Node, error) {

	table := getTable(tableNode)
	if table == nil {
		return tableNode, nil
	}

	var newTableNode sql.Node = tableNode

	replacedTable := false
	if pt, ok := table.(sql.ProjectedTable); ok && len(fieldsByTable[tableNode.Name()]) > 0 {
		if usedProjections[tableNode.Name()] == nil {
			projectedFields := fieldsByTable[tableNode.Name()]
			table = pt.WithProjection(projectedFields)
			usedProjections[tableNode.Name()] = projectedFields
		}

		newTableNode = plan.NewDecoratedNode(
			fmt.Sprintf("Projected table access on %v",
				fieldsByTable[tableNode.Name()]), newTableNode)
		a.Log("table %q transformed with pushdown of projection", tableNode.Name())

		replacedTable = true
	}

	if !replacedTable {
		return tableNode, nil
	}

	switch tableNode.(type) {
	case *plan.ResolvedTable, *plan.TableAlias, *plan.IndexedTableAccess:
		node, err := withTable(newTableNode, table)
		if err != nil {
			return nil, err
		}

		return node, nil
	default:
		return nil, ErrInvalidNodeType.New("pushdown", tableNode)
	}
}

func transformPushdownProjections(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	usedFieldsByTable := make(fieldsByTable)
	fieldsByTable := getFieldsByTable(ctx, n)

	selector := func(parent sql.Node, child sql.Node, childNum int) bool {
		switch parent.(type) {
		case *plan.TableAlias:
			// When we hit a table alias, we don't want to descend farther into the tree for expression matches, which
			// would give us the original (unaliased) names of columns
			return false
		default:
			return true
		}
	}

	node, err := plan.TransformUpWithSelector(n, selector, func(node sql.Node) (sql.Node, error) {
		var nameable NameableNode

		switch node.(type) {
		case *plan.TableAlias, *plan.ResolvedTable, *plan.IndexedTableAccess:
			nameable = node.(NameableNode)
		}

		if nameable != nil {
			table, err := pushdownProjectionsToTable(a, nameable, fieldsByTable, usedFieldsByTable)
			if err != nil {
				return nil, err
			}
			return FixFieldIndexesForExpressions(ctx, a, table, scope)
		} else {
			return FixFieldIndexesForExpressions(ctx, a, node, scope)
		}
	})

	if err != nil {
		return nil, err
	}

	return node, nil
}

// removePushedDownPredicates removes all handled filter predicates from the filter given and returns. If all
// predicates have been handled, it replaces the filter with its child.
func removePushedDownPredicates(ctx *sql.Context, a *Analyzer, node *plan.Filter, filters *filterSet) (sql.Node, error) {
	if filters.handledCount() == 0 {
		a.Log("no handled filters, leaving filter untouched")
		return node, nil
	}

	unhandled := filters.unhandledPredicates(ctx)
	if len(unhandled) == 0 {
		a.Log("filter node has no unhandled filters, so it will be removed")
		return node.Child, nil
	}

	a.Log(
		"filters removed from filter node: %s\nfilter has now %d filters",
		filters.handledFilters,
		len(unhandled),
	)

	return plan.NewFilter(expression.JoinAnd(unhandled...), node.Child), nil
}

type exprSlice []sql.Expression

func (es exprSlice) String() string {
	var sb strings.Builder
	for i, e := range es {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(e.String())
	}
	return sb.String()
}
