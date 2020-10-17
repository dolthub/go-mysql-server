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

	if !canDoPushdown(n, scope, a) {
		return n, nil
	}

	// First step is to find all col exprs and group them by the table they mention.
	// Even if they appear multiple times, only the first one will be used.
	filtersByTable, err := getFiltersByTable(ctx, n)

	// An error returned by getFiltersByTable means that we can't cleanly separate all the filters into tables.
	// In that case, skip pushing down the filters.
	// TODO: we could also handle this by keeping track of the filters we can't handle and re-applying them at the end
	if err != nil {
		return n, nil
	}

	indexes, err := getIndexesByTable(ctx, a, n)
	if err != nil {
		return nil, err
	}

	exprAliases := getExpressionAliases(n)
	tableAliases, err := getTableAliases(n)
	if err != nil {
		return nil, err
	}

	filters := newFilterSet(filtersByTable, exprAliases, tableAliases)

	n, err = convertFiltersToIndexedAccess(a, n, filters, indexes, exprAliases, tableAliases)
	if err != nil {
		return nil, err
	}

	return transformPushdownFilters(a, n, filters, indexes, exprAliases, tableAliases)
}

// pushdownProjections attempts to push projections down to individual tables that implement sql.ProjectTable
func pushdownProjections(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	span, ctx := ctx.Span("pushdown_projections")
	defer span.Finish()

	if !canDoPushdown(n, scope, a) {
		return n, nil
	}

	return transformPushdownProjections(ctx, a, n)
}

// canDoPushdown returns whether the node given can safely be analyzed for pushdown
func canDoPushdown(n sql.Node, scope *Scope, a *Analyzer) bool {
	if !n.Resolved() {
		return false
	}

	// don't do pushdown on certain queries
	switch n.(type) {
	case *plan.RowUpdateAccumulator, *plan.InsertInto, *plan.DeleteFrom, *plan.Update, *plan.CreateIndex, *plan.CreateTrigger:
		return false
	}

	if len(scope.Schema()) > 0 {
		// TODO: field index rewriting is broken for subqueries, skip it for now
		return false
	}

	// Pushdown interferes with left and right joins (some where clauses must only be evaluated on the result of the join,
	// not pushed down to the tables), so skip them.
	// TODO: only some join queries are incompatible with pushdown semantics, and we could be more judicious with this
	//  pruning. The issue is that for left and right joins, some where clauses must be evaluated on the result set after
	//  joining, and cannot be pushed down to the individual tables. For example, filtering on whether a field in the
	//  secondary table is NULL must happen after the join, not before, to give correct results.
	incompatibleJoin := false
	plan.Inspect(n, func(node sql.Node) bool {
		switch node.(type) {
		case *plan.LeftJoin, *plan.RightJoin:
			incompatibleJoin = true
		// case *plan.IndexedJoin:
		// 	incompatibleJoin = n.JoinType() == plan.JoinTypeLeft || n.JoinType() == plan.JoinTypeRight
		}
		return true
	})
	if incompatibleJoin {
		a.Log("skipping pushdown for incompatible join")
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
		a.Log("skipping pushdown for query with subquery")
		return false
	}

	return true
}

func transformPushdownFilters(a *Analyzer, n sql.Node, filters *filterSet, indexes indexLookupsByTable, exprAliases ExprAliases, tableAliases TableAliases) (sql.Node, error) {
	node, err := plan.TransformUp(n, func(node sql.Node) (sql.Node, error) {
		switch node := node.(type) {
		case *plan.Filter:
			n, err := removePushedDownPredicates(a, node, filters, exprAliases, tableAliases)
			if err != nil {
				return nil, err
			}
			return FixFieldIndexesForExpressions(n)
		case *plan.TableAlias:
			table, err := pushdownFiltersToTable(a, node, filters, exprAliases, tableAliases)
			if err != nil {
				return nil, err
			}
			return FixFieldIndexesForExpressions(table)
		case *plan.ResolvedTable:
			table, err := pushdownFiltersToTable(a, node, filters, exprAliases, tableAliases)
			if err != nil {
				return nil, err
			}
			return FixFieldIndexesForExpressions(table)
		case *plan.IndexedTableAccess:
			table, err := pushdownFiltersToTable(a, node, filters, exprAliases, tableAliases)
			if err != nil {
				return nil, err
			}
			return FixFieldIndexesForExpressions(table)
		default:
			return FixFieldIndexesForExpressions(node)
		}
	})

	if err != nil {
		return nil, err
	}

	return node, nil
}

func convertFiltersToIndexedAccess(a *Analyzer, n sql.Node, filters *filterSet, indexes indexLookupsByTable, exprAliases ExprAliases, tableAliases TableAliases) (sql.Node, error) {
	selector := func(parent sql.Node, child sql.Node, childNum int) bool {
		switch parent.(type) {
		// For IndexedJoins, we already are using indexed access during query execution for the secondary table, so
		// replacing the secondary table with an indexed lookup will have no effect on the result of the join, but *will*
		// inappropriately remove the filter from the predicate.
		case *plan.IndexedJoin:
			return childNum == 0
		}
		return true
	}

	node, err := plan.TransformUpWithSelector(n, selector, func(node sql.Node) (sql.Node, error) {
		switch node := node.(type) {
		case *plan.Filter:
			n, err := removePushedDownPredicates(a, node, filters, exprAliases, tableAliases)
			if err != nil {
				return nil, err
			}
			return FixFieldIndexesForExpressions(n)
		case *plan.TableAlias:
			table, err := pushdownIndexesToTable(a, node, filters, indexes)
			if err != nil {
				return nil, err
			}
			return FixFieldIndexesForExpressions(table)
		case *plan.ResolvedTable:
			table, err := pushdownIndexesToTable(a, node, filters, indexes)
			if err != nil {
				return nil, err
			}
			return FixFieldIndexesForExpressions(table)
		default:
			return FixFieldIndexesForExpressions(node)
		}
	})

	if err != nil {
		return nil, err
	}

	return node, nil
}

func transformPushdownProjections(ctx *sql.Context, a *Analyzer, n sql.Node) (sql.Node, error) {
	usedFieldsByTable := make(fieldsByTable)
	fieldsByTable := getFieldsByTable(ctx, n)

	node, err := plan.TransformUp(n, func(node sql.Node) (sql.Node, error) {
		switch node := node.(type) {
		case *plan.TableAlias:
			table, err := pushdownProjectionsToTable(a, node, fieldsByTable, usedFieldsByTable)
			if err != nil {
				return nil, err
			}
			return FixFieldIndexesForExpressions(table)
		case *plan.ResolvedTable:
			table, err := pushdownProjectionsToTable(a, node, fieldsByTable, usedFieldsByTable)
			if err != nil {
				return nil, err
			}
			return FixFieldIndexesForExpressions(table)
		default:
			return FixFieldIndexesForExpressions(node)
		}
	})

	if err != nil {
		return nil, err
	}

	return node, nil
}

type NameableNode interface {
	sql.Nameable
	sql.Node
}

// pushdownFiltersToTable attempts to push filters to tables that can accept them
func pushdownFiltersToTable(
		a *Analyzer,
		tableNode NameableNode,
		filters *filterSet,
		exprAliases ExprAliases,
		tableAliases TableAliases,
) (sql.Node, error) {

	table := getTable(tableNode)
	if table == nil {
		return tableNode, nil
	}

	var newTableNode sql.Node = tableNode

	// First push remaining filters onto the table itself if it's a sql.FilteredTable
	if ft, ok := table.(sql.FilteredTable); ok && len(filters.availableFiltersForTable(tableNode.Name())) > 0 {
		tableFilters := filters.availableFiltersForTable(tableNode.Name())
		handled := ft.HandledFilters(normalizeExpressions(exprAliases, tableAliases, tableFilters...))
		filters.markFiltersHandled(handled...)
		schema := table.Schema()

		handled, err := FixFieldIndexesOnExpressions(schema, handled...)
		if err != nil {
			return nil, err
		}

		table = ft.WithFilters(handled)
		newTableNode = plan.NewDecoratedNode(fmt.Sprintf("Filtered table access on %v", handled), newTableNode)

		a.Log(
			"table %q transformed with pushdown of filters, %d filters handled of %d",
			tableNode.Name(),
			len(handled),
			len(tableFilters),
		)
	}

	// Then move any remaining filters for the table directly above the table itself
	var pushedDownFilterExpression sql.Expression
	if tableFilters := filters.availableFiltersForTable(tableNode.Name()); len(tableFilters) > 0 {
		filters.markFiltersHandled(tableFilters...)

		schema := tableNode.Schema()
		handled, err := FixFieldIndexesOnExpressions(schema, tableFilters...)
		if err != nil {
			return nil, err
		}

		pushedDownFilterExpression = expression.JoinAnd(handled...)

		a.Log(
			"pushed down filters above table %q, %d filters handled of %d",
			tableNode.Name(),
			len(handled),
			len(tableFilters),
		)
	}

	switch tableNode.(type) {
	case *plan.ResolvedTable, *plan.TableAlias, *plan.IndexedTableAccess:
		node, err := withTable(newTableNode, table)
		if err != nil {
			return nil, err
		}

		if pushedDownFilterExpression != nil {
			return plan.NewFilter(pushedDownFilterExpression, node), nil
		}

		return node, nil
	default:
		return nil, ErrInvalidNodeType.New("pushdown", tableNode)
	}
}

// pushdownIndexesToTable attempts to convert filters to indexes on tables that can accept them
func pushdownIndexesToTable(
		a *Analyzer,
		tableNode NameableNode,
		filters *filterSet,
		indexes map[string]*indexLookup,
) (sql.Node, error) {

	table := getTable(tableNode)
	if table == nil {
		return tableNode, nil
	}

	var newTableNode sql.Node = tableNode

	// First attempt to apply any possible indexes to the table
	if it, ok := table.(sql.IndexAddressableTable); ok {
		indexLookup, ok := indexes[tableNode.Name()]
		if ok {
			table = it.WithIndexLookup(indexLookup.lookup)
			indexStrs := formatIndexDecoratorString(indexLookup)

			indexNoun := "index"
			if len(indexStrs) > 1 {
				indexNoun = "indexes"
			}
			newTableNode = plan.NewDecoratedNode(
				fmt.Sprintf("Indexed table access on %s %s", indexNoun, strings.Join(indexStrs, ", ")),
				newTableNode)
			a.Log("table %q transformed with pushdown of index", tableNode.Name())

			filters.markIndexesHandled(indexLookup.indexes)
		}
	}

	switch tableNode.(type) {
	case *plan.ResolvedTable, *plan.TableAlias:
		node, err := withTable(newTableNode, table)
		if err != nil {
			return nil, err
		}

		return node, nil
	default:
		return nil, ErrInvalidNodeType.New("pushdown", tableNode)
	}
}


func formatIndexDecoratorString(indexLookup *indexLookup) []string {
	var indexStrs []string
	for _, idx := range indexLookup.indexes {
		var expStrs []string
		for _, e := range idx.Expressions() {
			expStrs = append(expStrs, e)
		}
		indexStrs = append(indexStrs, fmt.Sprintf("[%s]", strings.Join(expStrs, ",")))
	}
	return indexStrs
}

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
	}

	switch tableNode.(type) {
	case *plan.ResolvedTable, *plan.TableAlias:
		node, err := withTable(newTableNode, table)
		if err != nil {
			return nil, err
		}

		return node, nil
	default:
		return nil, ErrInvalidNodeType.New("pushdown", tableNode)
	}
}

// removePushedDownPredicates removes all handled filter predicates from the filter given and returns. If all
// predicates have been handled, it replaces the filter with its child.
func removePushedDownPredicates(a *Analyzer, node *plan.Filter, filters *filterSet, exprAliases ExprAliases, tableAliases TableAliases) (sql.Node, error) {
	if filters.handledCount() == 0 {
		a.Log("no handled filters, leaving filter untouched")
		return node, nil
	}

	unhandled := filters.availableFilters()
	if len(unhandled) == 0 {
		a.Log("filter node has no unhandled filters, so it will be removed")
		return node.Child, nil
	}

	a.Log(
		"%d handled filters removed from filter node, filter has now %d filters",
		len(filters.handledFilters),
		len(unhandled),
	)

	return plan.NewFilter(expression.JoinAnd(unhandled...), node.Child), nil
}
