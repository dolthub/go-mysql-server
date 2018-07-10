package analyzer

import (
	"reflect"
	"sync"

	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
	"gopkg.in/src-d/go-mysql-server.v0/sql/plan"
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
	case *plan.InsertInto, *plan.CreateIndex:
		return n, nil
	}

	var fieldsByTable = make(map[string][]string)
	var exprsByTable = make(map[string][]sql.Expression)
	type tableField struct {
		table string
		field string
	}
	var tableFields = make(map[tableField]struct{})

	a.Log("finding used columns in node")

	colSpan, _ := ctx.Span("find_pushdown_columns")

	// First step is to find all col exprs and group them by the table they mention.
	// Even if they appear multiple times, only the first one will be used.
	plan.InspectExpressions(n, func(e sql.Expression) bool {
		if e, ok := e.(*expression.GetField); ok {
			tf := tableField{e.Table(), e.Name()}
			if _, ok := tableFields[tf]; !ok {
				a.Log("found used column %s.%s", e.Table(), e.Name())
				tableFields[tf] = struct{}{}
				fieldsByTable[e.Table()] = append(fieldsByTable[e.Table()], e.Name())
				exprsByTable[e.Table()] = append(exprsByTable[e.Table()], e)
			}
		}
		return true
	})

	colSpan.Finish()

	a.Log("finding filters in node")

	filterSpan, _ := ctx.Span("find_pushdown_filters")

	// then find all filters, also by table. Note that filters that mention
	// more than one table will not be passed to neither.
	filters := make(filters)
	plan.Inspect(n, func(node sql.Node) bool {
		a.Log("inspecting node of type: %T", node)
		switch node := node.(type) {
		case *plan.Filter:
			fs := exprToTableFilters(node.Expression)
			a.Log("found filters for %d tables %s", len(fs), node.Expression)
			filters.merge(fs)
		}
		return true
	})

	filterSpan.Finish()

	a.Log("transforming nodes with pushdown of filters and projections")

	// Now all nodes can be transformed. Since traversal of the tree is done
	// from inner to outer the filters have to be processed first so they get
	// to the tables.
	var handledFilters []sql.Expression
	var queryIndexes []sql.Index
	node, err := n.TransformUp(func(node sql.Node) (sql.Node, error) {
		a.Log("transforming node of type: %T", node)
		switch node := node.(type) {
		case *plan.Filter:
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
		case *plan.PushdownProjectionAndFiltersTable,
			*plan.PushdownProjectionTable,
			*plan.IndexableTable:
			// they also implement the interfaces for pushdown, so we better return
			// or there will be a very nice infinite loop
			return node, nil
		case sql.PushdownProjectionAndFiltersTable:
			cols := exprsByTable[node.Name()]
			tableFilters := filters[node.Name()]
			handled := node.HandledFilters(tableFilters)
			handledFilters = append(handledFilters, handled...)

			schema := node.Schema()
			cols, err := fixFieldIndexesOnExpressions(schema, cols...)
			if err != nil {
				return nil, err
			}

			handled, err = fixFieldIndexesOnExpressions(schema, handled...)
			if err != nil {
				return nil, err
			}

			indexable, ok := node.(*indexable)
			if !ok {
				a.Log(
					"table %q transformed with pushdown of projection and filters, %d filters handled of %d",
					node.Name(),
					len(handled),
					len(tableFilters),
				)

				return plan.NewPushdownProjectionAndFiltersTable(
					cols,
					handled,
					node,
				), nil
			}

			a.Log(
				"table %q transformed with pushdown of projection, filters and index, %d filters handled of %d",
				node.Name(),
				len(handled),
				len(tableFilters),
			)

			queryIndexes = append(queryIndexes, indexable.index.indexes...)
			return plan.NewIndexableTable(
				cols,
				handled,
				indexable.index.lookup,
				indexable.Indexable,
			), nil
		case sql.PushdownProjectionTable:
			cols := fieldsByTable[node.Name()]
			a.Log("table %q transformed with pushdown of projection", node.Name())
			return plan.NewPushdownProjectionTable(cols, node), nil
		}
		return node, nil
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
	return exp.TransformUp(func(e sql.Expression) (sql.Expression, error) {
		switch e := e.(type) {
		case *expression.GetField:
			// we need to rewrite the indexes for the table row
			for i, col := range schema {
				if e.Name() == col.Name {
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

type releaser struct {
	Child   sql.Node
	Release func()
}

var _ sql.Node = (*releaser)(nil)

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

func (r *releaser) TransformUp(f sql.TransformNodeFunc) (sql.Node, error) {
	child, err := r.Child.TransformUp(f)
	if err != nil {
		return nil, err
	}
	return f(&releaser{child, r.Release})
}

func (r *releaser) TransformExpressionsUp(f sql.TransformExprFunc) (sql.Node, error) {
	child, err := r.Child.TransformExpressionsUp(f)
	if err != nil {
		return nil, err
	}
	return &releaser{child, r.Release}, nil
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

func (i *releaseIter) Close() error {
	i.once.Do(i.release)
	return nil
}
