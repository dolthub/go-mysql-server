package analyzer

import (
	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/expression"
	"github.com/src-d/go-mysql-server/sql/plan"
)

func pushdown(ctx *sql.Context, a *Analyzer, n sql.Node) (sql.Node, error) {
	span, ctx := ctx.Span("pushdown")
	defer span.Finish()

	a.Log("pushdown, node of type: %T", n)
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
	fieldsByTable := findFieldsByTable(ctx, n)

	a.Log("finding filters in node")
	filters := findFilters(ctx, n)

	indexes, err := assignIndexes(ctx, a, n)
	if err != nil {
		return nil, err
	}

	a.Log("transforming nodes with pushdown of filters, projections and indexes")
	return transformPushdown(ctx, a, n, filters, indexes, fieldsByTable)
}

func findFieldsByTable(ctx *sql.Context, n sql.Node) map[string][]string {
	colSpan, _ := ctx.Span("find_field_by_table")
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

func findFilters(ctx *sql.Context, n sql.Node) filters {
	span, _ := ctx.Span("find_pushdown_filters")
	defer span.Finish()

	// Find all filters, also by table. Note that filters that mention
	// more than one table will not be passed to neither.
	filters := make(filters)
	plan.Inspect(n, func(node sql.Node) bool {
		switch node := node.(type) {
		case *plan.Filter:
			fs := exprToTableFilters(node.Expression)
			filters.merge(fs)
		}
		return true
	})

	return filters
}

func transformPushdown(
	ctx *sql.Context,
	a *Analyzer,
	n sql.Node,
	filters filters,
	indexes map[string]*indexLookup,
	fieldsByTable map[string][]string,
) (sql.Node, error) {
	// Now all nodes can be transformed. Since traversal of the tree is done
	// from inner to outer the filters have to be processed first so they get
	// to the tables.
	var handledFilters []sql.Expression
	var queryIndexes []sql.Index

	node, err := plan.TransformUp(n, func(node sql.Node) (sql.Node, error) {
		switch node := node.(type) {
		case *plan.Filter:
			n, err := pushdownFilter(a, node, handledFilters)
			if err != nil {
				return nil, err
			}
			// After pushing down the filter, we need to fix field indexes as well
			return fixFieldIndexesForExpressions(n)
		case *plan.ResolvedTable:
			table, err := pushdownTable(
				a,
				node,
				filters,
				&handledFilters,
				&queryIndexes,
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
		for _, idx := range queryIndexes {
			ctx.ReleaseIndex(idx)
		}
	}

	if err != nil {
		release()
		return nil, err
	}

	if len(queryIndexes) > 0 {
		return &Releaser{node, release}, nil
	}

	return node, nil
}

func pushdownTable(
	a *Analyzer,
	node *plan.ResolvedTable,
	filters filters,
	handledFilters *[]sql.Expression,
	queryIndexes *[]sql.Index,
	fieldsByTable map[string][]string,
	indexes map[string]*indexLookup,
) (sql.Node, error) {
	var table = node.Table

	if ft, ok := table.(sql.FilteredTable); ok {
		tableFilters := filters[node.Name()]
		handled := ft.HandledFilters(tableFilters)
		*handledFilters = append(*handledFilters, handled...)
		schema := node.Schema()
		handled, err := fixFieldIndexesOnExpressions(schema, handled...)
		if err != nil {
			return nil, err
		}

		table = ft.WithFilters(handled)
		a.Log(
			"table %q transformed with pushdown of filters, %d filters handled of %d",
			node.Name(),
			len(handled),
			len(tableFilters),
		)
	}

	if pt, ok := table.(sql.ProjectedTable); ok {
		table = pt.WithProjection(fieldsByTable[node.Name()])
		a.Log("table %q transformed with pushdown of projection", node.Name())
	}

	if it, ok := table.(sql.IndexableTable); ok {
		indexLookup, ok := indexes[node.Name()]
		if ok {
			*queryIndexes = append(*queryIndexes, indexLookup.indexes...)
			table = it.WithIndexLookup(indexLookup.lookup)
			a.Log("table %q transformed with pushdown of index", node.Name())
		}
	}

	return plan.NewResolvedTable(table), nil
}

func pushdownFilter(
	a *Analyzer,
	node *plan.Filter,
	handledFilters []sql.Expression,
) (sql.Node, error) {
	if len(handledFilters) == 0 {
		a.Log("no handled filters, leaving filter untouched")
		return node, nil
	}

	unhandled := getUnhandledFilters(
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