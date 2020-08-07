package analyzer

import (
	"github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/liquidata-inc/go-mysql-server/sql/expression"
	"github.com/liquidata-inc/go-mysql-server/sql/plan"
)

// pushdownFilters attempts to push conditions in filters down to individual tables. Tables that implement
// sql.FilteredTable will get such conditions applied to them. For conditions that have an index, tables that implement
// sql.IndexAddressableTable will get an appropriate index lookup applied. Additionally, projections are pushed down
// onto tables that implement sql.ProjectedTable.
func pushdownFilters(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	span, ctx := ctx.Span("pushdown_filters")
	defer span.Finish()

	if !n.Resolved() {
		return n, nil
	}

	// don't do pushdown on certain queries
	switch n.(type) {
	case *plan.InsertInto, *plan.DeleteFrom, *plan.Update, *plan.CreateIndex:
		return n, nil
	}

	if len(scope.Schema()) > 0 {
		// TODO: field index rewriting is broken for subqueries, skip it for now
		return n, nil
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
		return n, nil
	}

	// First step is to find all col exprs and group them by the table they mention.
	// Even if they appear multiple times, only the first one will be used.
	fieldsByTable := getFieldsByTable(ctx, n)
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

	return transformPushdown(ctx, a, n, filters, indexes, fieldsByTable, exprAliases, tableAliases)
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

// getFieldsByTable returns a map of table name to set of field names in the node provided
func getFieldsByTable(ctx *sql.Context, n sql.Node) fieldsByTable {
	colSpan, _ := ctx.Span("getFieldsByTable")
	defer colSpan.Finish()

	var fieldsByTable = make(fieldsByTable)
	plan.InspectExpressionsWithNode(n, func(n sql.Node, e sql.Expression) bool {
		if gf, ok := e.(*expression.GetField); ok {
			fieldsByTable.add(gf.Table(), gf.Name())
		}
		if s, ok := e.(*plan.Subquery); ok {
			fieldsByTable.addAll(getFieldsByTable(ctx, s.Query))
		}
		return true
	})
	return fieldsByTable
}

func transformPushdown(
	ctx *sql.Context,
	a *Analyzer,
	n sql.Node,
	filters filtersByTable,
	indexes indexLookupsByTable,
	fieldNamesByTable fieldsByTable,
	exprAliases ExprAliases,
	tableAliases TableAliases,
) (sql.Node, error) {
	var handledFilters []sql.Expression
	usedFieldsByTable := make(fieldsByTable)

	node, err := plan.TransformUp(n, func(node sql.Node) (sql.Node, error) {
		switch node := node.(type) {
		case *plan.Filter:
			n, err := removePushedDownPredicates(a, node, handledFilters, exprAliases, tableAliases)
			if err != nil {
				return nil, err
			}
			return FixFieldIndexesForExpressions(n)
		case *plan.TableAlias:
			table, err := pushdownToTable(
				a,
				node,
				filters,
				&handledFilters,
				fieldNamesByTable,
				usedFieldsByTable,
				indexes,
				exprAliases,
				tableAliases,
			)
			if err != nil {
				return nil, err
			}
			return FixFieldIndexesForExpressions(table)
		case *plan.ResolvedTable:
			table, err := pushdownToTable(
				a,
				node,
				filters,
				&handledFilters,
				fieldNamesByTable,
				usedFieldsByTable,
				indexes,
				exprAliases,
				tableAliases,
			)
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

// pushdownToTable attempts to push filters, projections, and indexes to tables that can accept them
// TODO: this should also push predicates down to individual tables via wrapping them with a Filter node, not just via
//  the sql.FilteredTable interface.
func pushdownToTable(
	a *Analyzer,
	tableNode NameableNode,
	filters filtersByTable,
	handledFilters *[]sql.Expression,
	fieldsByTable fieldsByTable,
	usedProjections fieldsByTable,
	indexes map[string]*indexLookup,
	exprAliases ExprAliases,
	tableAliases TableAliases,
) (sql.Node, error) {

	table := getTable(tableNode)
	if table == nil {
		return tableNode, nil
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
		a.Log(
			"table %q transformed with pushdown of filters, %d filters handled of %d",
			tableNode.Name(),
			len(handled),
			len(tableFilters),
		)
	}

	if pt, ok := table.(sql.ProjectedTable); ok && len(fieldsByTable[tableNode.Name()]) > 0 {
		if usedProjections[tableNode.Name()] == nil {
			projectedFields := fieldsByTable[tableNode.Name()]
			table = pt.WithProjection(projectedFields)
			usedProjections[tableNode.Name()] = projectedFields
		}
		a.Log("table %q transformed with pushdown of projection", tableNode.Name())
	}

	if it, ok := table.(sql.IndexAddressableTable); ok {
		indexLookup, ok := indexes[tableNode.Name()]
		if ok {
			table = it.WithIndexLookup(indexLookup.lookup)
			a.Log("table %q transformed with pushdown of index", tableNode.Name())
		}
	}

	switch tableNode.(type) {
	case *plan.ResolvedTable:
		return plan.NewResolvedTable(table), nil
	case *plan.TableAlias:
		return withTable(tableNode, table)
	default:
		return nil, ErrInvalidNodeType.New("pushdown", tableNode)
	}
}

// Transforms the node given bottom up by setting resolve tables to reference the table given. Returns an error if more
// than one table was set in this way.
func withTable(node NameableNode, table sql.Table) (sql.Node, error) {
	foundTable := false
	return plan.TransformUp(node, func(n sql.Node) (sql.Node, error) {
		switch n := n.(type) {
		case *plan.ResolvedTable:
			if foundTable {
				return nil, ErrInAnalysis.New("attempted to set more than one table in withTable()")
			}
			foundTable = true
			return plan.NewResolvedTable(table), nil
		default:
			return n, nil
		}
	})
}

// Finds first table node that is a descendant of the node given
func getTable(node sql.Node) sql.Table {
	var table sql.Table
	plan.Inspect(node, func(node sql.Node) bool {
		switch n := node.(type) {
		case *plan.ResolvedTable:
			table = n.Table
		}
		return true
	})
	return table
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
