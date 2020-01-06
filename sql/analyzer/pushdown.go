package analyzer

import (
	"reflect"
	"sync"

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

	// Pushdown interferes with evaluating per-row index lookups for indexed joins, so skip them
	foundIndexedJoin := false
	plan.Inspect(n, func(node sql.Node) bool {
		if _, ok:= node.(*plan.IndexedJoin); ok {
			foundIndexedJoin = true
			return false
		}
		return true
	})
	if foundIndexedJoin {
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
	return transformPushdown(a, n, filters, indexes, fieldsByTable)
}

// fixFieldIndexesOnExpressions executes fixFieldIndexes on a list of exprs.
func fixFieldIndexesOnExpressions(schema sql.Schema, expressions ...sql.Expression) ([]sql.Expression, error) {
	var result = make([]sql.Expression, len(expressions))
	for i, e := range expressions {
		var err error
		result[i], err = fixFieldIndexes(schema, e)
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

// fixFieldIndexes transforms the given expression setting correct indexes
// for GetField expressions according to the schema of the row in the table
// and not the one where the filter came from.
func fixFieldIndexes(schema sql.Schema, exp sql.Expression) (sql.Expression, error) {
	return expression.TransformUp(exp, func(e sql.Expression) (sql.Expression, error) {
		switch e := e.(type) {
		case *expression.GetField:
			// we need to rewrite the indexes for the table row
			for i, col := range schema {
				if e.Name() == col.Name && e.Table() == col.Source {
					return expression.NewGetFieldWithTable(
						i,
						e.Type(),
						e.Table(),
						e.Name(),
						e.IsNullable(),
					), nil
				}
			}

			return nil, ErrFieldMissing.New(e.Name())
		}

		return e, nil
	})
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
		a.Log("transforming node of type: %T", node)
		switch node := node.(type) {
		case *plan.Filter:
			n, err := pushdownFilter(a, node, handledFilters)
			if err != nil {
				return nil, err
			}
			// After pushing down the filter, we need to fix field indexes as well
			return transformExpressioners(n)
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
			return transformExpressioners(table)
		default:
			return transformExpressioners(node)
		}
	})

	release := func() {
		for _, idx := range queryIndexes {
			a.Catalog.ReleaseIndex(idx)
		}
	}

	if err != nil {
		release()
		return nil, err
	}

	if len(queryIndexes) > 0 {
		return &releaser{node, release}, nil
	}

	return node, nil
}

func transformExpressioners(node sql.Node) (sql.Node, error) {
	if _, ok := node.(sql.Expressioner); !ok {
		return node, nil
	}

	var schemas []sql.Schema
	for _, child := range node.Children() {
		schemas = append(schemas, child.Schema())
	}

	if len(schemas) < 1 {
		return node, nil
	}

	n, err := plan.TransformExpressions(node, func(e sql.Expression) (sql.Expression, error) {
		for _, schema := range schemas {
			fixed, err := fixFieldIndexes(schema, e)
			if err == nil {
				return fixed, nil
			}

			if ErrFieldMissing.Is(err) {
				continue
			}

			return nil, err
		}

		return e, nil
	})

	if err != nil {
		return nil, err
	}

	switch j := n.(type) {
	case *plan.InnerJoin:
		cond, err := fixFieldIndexes(j.Schema(), j.Cond)
		if err != nil {
			return nil, err
		}

		n = plan.NewInnerJoin(j.Left, j.Right, cond)
	case *plan.RightJoin:
		cond, err := fixFieldIndexes(j.Schema(), j.Cond)
		if err != nil {
			return nil, err
		}

		n = plan.NewRightJoin(j.Left, j.Right, cond)
	case *plan.LeftJoin:
		cond, err := fixFieldIndexes(j.Schema(), j.Cond)
		if err != nil {
			return nil, err
		}

		n = plan.NewLeftJoin(j.Left, j.Right, cond)
	}

	return n, nil
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
		splitExpression(node.Expression),
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

type releaser struct {
	Child   sql.Node
	Release func()
}

func (r *releaser) Resolved() bool {
	return r.Child.Resolved()
}

func (r *releaser) Children() []sql.Node {
	return []sql.Node{r.Child}
}

func (r *releaser) RowIter(ctx *sql.Context) (sql.RowIter, error) {
	iter, err := r.Child.RowIter(ctx)
	if err != nil {
		r.Release()
		return nil, err
	}

	return &releaseIter{child: iter, release: r.Release}, nil
}

func (r *releaser) Schema() sql.Schema {
	return r.Child.Schema()
}

func (r *releaser) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(r, len(children), 1)
	}
	return &releaser{children[0], r.Release}, nil
}

func (r *releaser) String() string {
	return r.Child.String()
}

func (r *releaser) Equal(n sql.Node) bool {
	if r2, ok := n.(*releaser); ok {
		return reflect.DeepEqual(r.Child, r2.Child)
	}
	return false
}

type releaseIter struct {
	child   sql.RowIter
	release func()
	once    sync.Once
}

func (i *releaseIter) Next() (sql.Row, error) {
	row, err := i.child.Next()
	if err != nil {
		_ = i.Close()
		return nil, err
	}
	return row, nil
}

func (i *releaseIter) Close() (err error) {
	i.once.Do(i.release)
	if i.child != nil {
		err = i.child.Close()
	}
	return err
}
