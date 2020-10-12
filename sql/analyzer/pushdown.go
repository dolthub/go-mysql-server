package analyzer

import (
	"fmt"
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
	filters := getFiltersByTable(ctx, n)
	indexes, err := getIndexesByTable(ctx, a, n)
	if err != nil {
		return nil, err
	}

	exprAliases := getExpressionAliases(n)
	tableAliases, err := getTableAliases(n)
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

	return transformPushdownProjections(ctx , a, n)
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

func transformPushdownFilters(
		a *Analyzer,
		n sql.Node,
		filters filtersByTable,
		indexes indexLookupsByTable,
		exprAliases ExprAliases,
		tableAliases TableAliases,
) (sql.Node, error) {
	var handledFilters []sql.Expression

	node, err := plan.TransformUp(n, func(node sql.Node) (sql.Node, error) {
		switch node := node.(type) {
		case *plan.Filter:
			n, err := removePushedDownPredicates(a, node, handledFilters, exprAliases, tableAliases)
			if err != nil {
				return nil, err
			}
			return FixFieldIndexesForExpressions(n)
		case *plan.TableAlias:
			table, err := pushdownFiltersToTable(a, node, filters, &handledFilters, indexes, exprAliases, tableAliases)
			if err != nil {
				return nil, err
			}
			return FixFieldIndexesForExpressions(table)
		case *plan.ResolvedTable:
			table, err := pushdownFiltersToTable(a, node, filters, &handledFilters, indexes, exprAliases, tableAliases)
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

// pushdownFiltersToTable attempts to push filters, projections, and indexes to tables that can accept them
func pushdownFiltersToTable(
		a *Analyzer,
		tableNode NameableNode,
		filters filtersByTable,
		handledFilters *[]sql.Expression,
		indexes map[string]*indexLookup,
		exprAliases ExprAliases,
		tableAliases TableAliases,
) (sql.Node, error) {

	table := getTable(tableNode)
	if table == nil {
		return tableNode, nil
	}

	var newTableNode sql.Node = tableNode

	if it, ok := table.(sql.IndexAddressableTable); ok {
		indexLookup, ok := indexes[tableNode.Name()]
		if ok {
			table = it.WithIndexLookup(indexLookup.lookup)
			newTableNode = plan.NewDecoratedNode(fmt.Sprintf("Indexed table access on %v", indexLookup.lookup), newTableNode)
			a.Log("table %q transformed with pushdown of index", tableNode.Name())
		}
	}

	if ft, ok := table.(sql.FilteredTable); ok && len(filters[tableNode.Name()]) > 0 {
		tableFilters := filters[tableNode.Name()]
		handled := ft.HandledFilters(normalizeExpressions(exprAliases, tableAliases, subtractExprSet(tableFilters, *handledFilters)...))
		*handledFilters = append(*handledFilters, handled...)
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

	var pushedDownFilterExpression sql.Expression
	if len(filters[tableNode.Name()]) > 0 {
		tableFilters := filters[tableNode.Name()]
		leftToHandle := subtractExprSet(tableFilters, *handledFilters)
		*handledFilters = append(*handledFilters, leftToHandle...)
		schema := tableNode.Schema()
		handled, err := FixFieldIndexesOnExpressions(schema, leftToHandle...)
		if err != nil {
			return nil, err
		}

		pushedDownFilterExpression = expression.JoinAnd(handled...)

		a.Log(
			"pushed down filters to table %q, %d filters handled of %d",
			tableNode.Name(),
			len(handled),
			len(tableFilters),
		)
	}

	switch tableNode.(type) {
	case *plan.ResolvedTable, *plan.TableAlias:
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

type fieldsByTable map[string][]string

// add adds the table and field given if not already present
func (f fieldsByTable) add(table, field string) {
	if !stringContains(f[table], field) {
		f[table] = append(f[table], field)
	}
}

// addAll adds the tables and fields given if not already present
func (f fieldsByTable) addAll(f2 fieldsByTable) {
	for table, fields := range f2 {
		for _, field := range fields {
			f.add(table, field)
		}
	}
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

		newTableNode = plan.NewDecoratedNode(fmt.Sprintf("Projected table access on %v", fieldsByTable[tableNode.Name()]), newTableNode)
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
func removePushedDownPredicates(
	a *Analyzer,
	node *plan.Filter,
	handledFilters []sql.Expression,
	exprAliases ExprAliases,
	tableAliases TableAliases,
) (sql.Node, error) {

	if len(handledFilters) == 0 {
		a.Log("no handled filters, leaving filter untouched")
		return node, nil
	}

	unhandled := subtractExprSet(
		normalizeExpressions(exprAliases, tableAliases, splitConjunction(node.Expression)...),
		handledFilters,
	)

	if len(unhandled) == 0 {
		a.Log("filter node has no unhandled filters, so it will be removed")
		return node.Child, nil
	}

	a.Log(
		"%d handled filters removed from filter node, filter has now %d filters",
		len(handledFilters),
		len(unhandled),
	)

	return plan.NewFilter(expression.JoinAnd(unhandled...), node.Child), nil
}
