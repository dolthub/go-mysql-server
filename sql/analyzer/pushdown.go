package analyzer

import (
	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/expression"
	"github.com/src-d/go-mysql-server/sql/plan"
)

func pushdown(ctx *sql.Context, a *Analyzer, n sql.Node) (sql.Node, error) {
	span, ctx := ctx.Span("pushdown")
	defer span.Finish()

	if !n.Resolved() {
		return n, nil
	}

	// don't do pushdown on certain queries
	switch n.(type) {
	case *plan.InsertInto, *plan.DeleteFrom, *plan.Update, *plan.CreateIndex:
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
	a.Log("finding used columns in node")
	fieldsByTable := getFieldsByTable(ctx, n)

	a.Log("finding filters in node")
	filters := getFiltersByTable(ctx, n)

	indexes, err := getIndexesByTable(ctx, a, n)
	if err != nil {
		return nil, err
	}

	exprAliases := getExpressionAliases(n)
	tableAliases := getTableAliases(n)

	a.Log("transforming nodes with pushdown of filters, projections and indexes")
	return transformPushdown(ctx, a, n, filters, indexes, fieldsByTable, exprAliases, tableAliases)
}

// getFieldsByTable returns a map of table name to set of field names in the node provided
func getFieldsByTable(ctx *sql.Context, n sql.Node) map[string][]string {
	colSpan, _ := ctx.Span("getFieldsByTable")
	defer colSpan.Finish()

	var fieldsByTable = make(map[string][]string)
	plan.InspectExpressions(n, func(e sql.Expression) bool {
		if gf, ok := e.(*expression.GetField); ok {
			if !stringContains(fieldsByTable[gf.Table()], gf.Name()) {
				fieldsByTable[gf.Table()] = append(fieldsByTable[gf.Table()], gf.Name())
			}
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
		indexes map[string]*indexLookup,
		fieldsByTable map[string][]string,
		aliases ExprAliases,
		tableAliases TableAliases,
) (sql.Node, error) {
	var handledFilters []sql.Expression
	var usedIndexes []sql.Index

	node, err := plan.TransformUp(n, func(node sql.Node) (sql.Node, error) {
		switch node := node.(type) {
		case *plan.Filter:
			n, err := removePushedDownPredicates(a, node, handledFilters)
			if err != nil {
				return nil, err
			}
			return fixFieldIndexesForExpressions(n)
		case *plan.ResolvedTable:
			table, err := pushdownToTable(
				a,
				node,
				filters,
				handledFilters,
				usedIndexes,
				fieldsByTable,
				indexes,
			)
			if err != nil {
				return nil, err
			}
			return fixFieldIndexesForExpressions(table)
		default:
			return fixFieldIndexesForExpressions(node)
		}
	})

	release := func() {
		for _, idx := range usedIndexes {
			ctx.ReleaseIndex(idx)
		}
	}

	if err != nil {
		release()
		return nil, err
	}

	if len(usedIndexes) > 0 {
		return &Releaser{node, release}, nil
	}

	return node, nil
}

// pushdownToTable attempts to push filters, projections, and indexes to tables that can accept them
// TODO: this should also push predicates down to individual tables via wrapping them with a Filter node, not just via
//  the sql.FilteredTable interface.
func pushdownToTable(
		a *Analyzer,
		rt *plan.ResolvedTable,
		filters filtersByTable,
		handledFilters []sql.Expression,
		usedIndexes []sql.Index,
		fieldsByTable map[string][]string,
		indexes map[string]*indexLookup,
) (sql.Node, error) {
	var table = rt.Table

	if ft, ok := table.(sql.FilteredTable); ok {
		tableFilters := filters[rt.Name()]
		handled := ft.HandledFilters(tableFilters)
		handledFilters = append(handledFilters, handled...)
		schema := rt.Schema()
		handled, err := fixFieldIndexesOnExpressions(schema, handled...)
		if err != nil {
			return nil, err
		}

		table = ft.WithFilters(handled)
		a.Log(
			"table %q transformed with pushdown of filters, %d filters handled of %d",
			rt.Name(),
			len(handled),
			len(tableFilters),
		)
	}

	if pt, ok := table.(sql.ProjectedTable); ok {
		table = pt.WithProjection(fieldsByTable[rt.Name()])
		a.Log("table %q transformed with pushdown of projection", rt.Name())
	}

	if it, ok := table.(sql.IndexableTable); ok {
		indexLookup, ok := indexes[rt.Name()]
		if ok {
			usedIndexes = append(usedIndexes, indexLookup.indexes...)
			table = it.WithIndexLookup(indexLookup.lookup)
			a.Log("table %q transformed with pushdown of index", rt.Name())
		}
	}

	return plan.NewResolvedTable(table), nil
}

// removePushedDownPredicates removes all handled filter predicates from the filter given and returns. If all
// predicates have been handled, it replaces the filter with its child.
func removePushedDownPredicates(
		a *Analyzer,
		node *plan.Filter,
		handledFilters []sql.Expression,
) (sql.Node, error) {

	if len(handledFilters) == 0 {
		a.Log("no handled filters, leaving filter untouched")
		return node, nil
	}

	unhandled := subtractExprSet(
		splitConjunction(node.Expression),
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