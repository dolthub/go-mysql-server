package plan

import (
	"fmt"
	"strings"

	opentracing "github.com/opentracing/opentracing-go"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

// PushdownProjectionTable is a node wrapping a table implementing the
// sql.PushdownProjectionTable interface so it returns a RowIter with
// custom logic given the set of used columns that need to be projected.
// PushdownProjectionTable nodes don't propagate transformations.
type PushdownProjectionTable struct {
	sql.PushdownProjectionTable
	Columns []string
}

// NewPushdownProjectionTable creates a new PushdownProjectionTable node.
func NewPushdownProjectionTable(
	columns []string,
	table sql.PushdownProjectionTable,
) *PushdownProjectionTable {
	return &PushdownProjectionTable{table, columns}
}

// TransformUp implements the Node interface.
func (t *PushdownProjectionTable) TransformUp(f sql.TransformNodeFunc) (sql.Node, error) {
	node, err := t.PushdownProjectionTable.TransformUp(f)
	if err != nil {
		return nil, err
	}

	table, ok := node.(sql.PushdownProjectionTable)
	if !ok {
		return node, nil
	}

	return f(NewPushdownProjectionTable(t.Columns, table))
}

// TransformExpressionsUp implements the Node interface.
func (t *PushdownProjectionTable) TransformExpressionsUp(
	f sql.TransformExprFunc,
) (sql.Node, error) {
	node, err := t.PushdownProjectionTable.TransformExpressionsUp(f)
	if err != nil {
		return nil, err
	}

	table, ok := node.(sql.PushdownProjectionTable)
	if !ok {
		return node, nil
	}

	return NewPushdownProjectionTable(t.Columns, table), nil
}

// RowIter implements the Node interface.
func (t *PushdownProjectionTable) RowIter(ctx *sql.Context) (sql.RowIter, error) {
	span, ctx := ctx.Span("plan.PushdownProjectionTable", opentracing.Tags{
		"columns": len(t.Columns),
		"table":   t.Name(),
	})

	iter, err := t.WithProject(ctx, t.Columns)
	if err != nil {
		span.Finish()
		return nil, err
	}

	return sql.NewSpanIter(span, iter), nil
}

func (t PushdownProjectionTable) String() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("PushdownProjectionTable(%s)", strings.Join(t.Columns, ", "))
	_ = pr.WriteChildren(t.PushdownProjectionTable.String())
	return pr.String()
}

// PushdownProjectionAndFiltersTable is a node wrapping a table implementing
// the sql.PushdownProjectionAndFiltersTable interface so it returns a RowIter
// with custom logic given the set of used columns that need to be projected
// and the filters that apply to that table.
// PushdownProjectionAndFiltersTable nodes don't propagate transformations.
type PushdownProjectionAndFiltersTable struct {
	sql.PushdownProjectionAndFiltersTable
	Columns []sql.Expression
	Filters []sql.Expression
}

// NewPushdownProjectionAndFiltersTable creates a new
// PushdownProjectionAndFiltersTable node.
func NewPushdownProjectionAndFiltersTable(
	columns []sql.Expression,
	filters []sql.Expression,
	table sql.PushdownProjectionAndFiltersTable,
) *PushdownProjectionAndFiltersTable {
	return &PushdownProjectionAndFiltersTable{table, columns, filters}
}

// TransformUp implements the Node interface.
func (t *PushdownProjectionAndFiltersTable) TransformUp(
	f sql.TransformNodeFunc,
) (sql.Node, error) {
	return f(t)
}

// TransformExpressionsUp implements the Node interface.
func (t *PushdownProjectionAndFiltersTable) TransformExpressionsUp(
	f sql.TransformExprFunc,
) (sql.Node, error) {
	filters, err := transformExpressionsUp(f, t.Filters)
	if err != nil {
		return nil, err
	}

	return NewPushdownProjectionAndFiltersTable(t.Columns, filters, t.PushdownProjectionAndFiltersTable), nil
}

// RowIter implements the Node interface.
func (t *PushdownProjectionAndFiltersTable) RowIter(ctx *sql.Context) (sql.RowIter, error) {
	span, ctx := ctx.Span("plan.PushdownProjectionAndFiltersTable", opentracing.Tags{
		"columns": len(t.Columns),
		"filters": len(t.Filters),
		"table":   t.Name(),
	})

	iter, err := t.WithProjectAndFilters(ctx, t.Columns, t.Filters)
	if err != nil {
		span.Finish()
		return nil, err
	}

	return sql.NewSpanIter(span, iter), nil
}

func (t *PushdownProjectionAndFiltersTable) String() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("PushdownProjectionAndFiltersTable")

	var columns = make([]string, len(t.Columns))
	for i, col := range t.Columns {
		columns[i] = col.String()
	}

	var filters = make([]string, len(t.Filters))
	for i, f := range t.Filters {
		filters[i] = f.String()
	}

	_ = pr.WriteChildren(
		fmt.Sprintf("Columns(%s)", strings.Join(columns, ", ")),
		fmt.Sprintf("Filters(%s)", strings.Join(filters, ", ")),
		t.PushdownProjectionAndFiltersTable.String(),
	)

	return pr.String()
}

// Expressions implements the Expressioner interface.
func (t *PushdownProjectionAndFiltersTable) Expressions() []sql.Expression {
	var exprs []sql.Expression
	exprs = append(exprs, t.Columns...)
	exprs = append(exprs, t.Filters...)
	return exprs
}

// TransformExpressions implements the Expressioner interface.
func (t *PushdownProjectionAndFiltersTable) TransformExpressions(f sql.TransformExprFunc) (sql.Node, error) {
	cols, err := transformExpressionsUp(f, t.Columns)
	filters, err := transformExpressionsUp(f, t.Filters)
	if err != nil {
		return nil, err
	}

	return NewPushdownProjectionAndFiltersTable(
		cols,
		filters,
		t.PushdownProjectionAndFiltersTable,
	), nil
}

// IndexableTable is a node wrapping a table implementing the sql.Inedxable
// interface so it returns a RowIter with custom logic given the set of used
// columns that need to be projected, the filtes that apply to that table and
// the indexes to use.
// IndexableTable nodes don't propagate transformations to the underlying
// table.
type IndexableTable struct {
	sql.Indexable
	Columns []sql.Expression
	Filters []sql.Expression
	Index   sql.IndexLookup
}

// NewIndexableTable creates a new IndexableTable node.
func NewIndexableTable(
	columns []sql.Expression,
	filters []sql.Expression,
	index sql.IndexLookup,
	table sql.Indexable,
) *IndexableTable {
	return &IndexableTable{table, columns, filters, index}
}

// TransformUp implements the Node interface.
func (t *IndexableTable) TransformUp(
	f sql.TransformNodeFunc,
) (sql.Node, error) {
	return f(t)
}

// TransformExpressionsUp implements the Node interface.
func (t *IndexableTable) TransformExpressionsUp(
	f sql.TransformExprFunc,
) (sql.Node, error) {
	filters, err := transformExpressionsUp(f, t.Filters)
	if err != nil {
		return nil, err
	}

	return NewIndexableTable(t.Columns, filters, t.Index, t.Indexable), nil
}

// RowIter implements the Node interface.
func (t *IndexableTable) RowIter(ctx *sql.Context) (sql.RowIter, error) {
	span, ctx := ctx.Span("plan.IndexableTable", opentracing.Tags{
		"columns": len(t.Columns),
		"filters": len(t.Filters),
		"table":   t.Name(),
	})

	values, err := t.Index.Values()
	if err != nil {
		return nil, err
	}

	iter, err := t.WithProjectFiltersAndIndex(ctx, t.Columns, t.Filters, values)
	if err != nil {
		span.Finish()
		return nil, err
	}

	return sql.NewSpanIter(span, iter), nil
}

func (t IndexableTable) String() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("IndexableTable")

	var columns = make([]string, len(t.Columns))
	for i, col := range t.Columns {
		columns[i] = col.String()
	}

	var filters = make([]string, len(t.Filters))
	for i, f := range t.Filters {
		filters[i] = f.String()
	}

	_ = pr.WriteChildren(
		fmt.Sprintf("Columns(%s)", strings.Join(columns, ", ")),
		fmt.Sprintf("Filters(%s)", strings.Join(filters, ", ")),
		t.Indexable.String(),
	)

	return pr.String()
}

// Expressions implements the Expressioner interface.
func (t IndexableTable) Expressions() []sql.Expression {
	var exprs []sql.Expression
	exprs = append(exprs, t.Columns...)
	exprs = append(exprs, t.Filters...)
	return exprs
}

// Children implements the Node interface.
func (t IndexableTable) Children() []sql.Node { return nil }
