package plan

import (
	"fmt"
	"hash/crc64"
	"io"
	"strings"

	opentracing "github.com/opentracing/opentracing-go"
	errors "gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

// ErrGroupBy is returned when the aggregation is not supported.
var ErrGroupBy = errors.NewKind("group by aggregation '%v' not supported")

// GroupBy groups the rows by some expressions.
type GroupBy struct {
	UnaryNode
	SelectedExprs []sql.Expression
	GroupByExprs  []sql.Expression
}

// NewGroupBy creates a new GroupBy node. Like Project, GroupBy is a top-level node, and contains all the fields that
// will appear in the output of the query. Some of these fields may be aggregate functions, some may be columns or
// other expressions. Unlike a project, the GroupBy also has a list of group-by expressions, which usually also appear
// in the list of selected expressions.
func NewGroupBy(selectedExprs, groupByExprs []sql.Expression, child sql.Node) *GroupBy {
	return &GroupBy{
		UnaryNode:     UnaryNode{Child: child},
		SelectedExprs: selectedExprs,
		GroupByExprs:  groupByExprs,
	}
}

// Resolved implements the Resolvable interface.
func (g *GroupBy) Resolved() bool {
	return g.UnaryNode.Child.Resolved() &&
		expressionsResolved(g.SelectedExprs...) &&
		expressionsResolved(g.GroupByExprs...)
}

// Schema implements the Node interface.
func (g *GroupBy) Schema() sql.Schema {
	var s = make(sql.Schema, len(g.SelectedExprs))
	for i, e := range g.SelectedExprs {
		var name string
		if n, ok := e.(sql.Nameable); ok {
			name = n.Name()
		} else {
			name = e.String()
		}

		var table string
		if t, ok := e.(sql.Tableable); ok {
			table = t.Table()
		}

		s[i] = &sql.Column{
			Name:     name,
			Type:     e.Type(),
			Nullable: e.IsNullable(),
			Source:   table,
		}
	}

	return s
}

// RowIter implements the Node interface.
func (g *GroupBy) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	span, ctx := ctx.Span("plan.GroupBy", opentracing.Tags{
		"groupings":  len(g.GroupByExprs),
		"aggregates": len(g.SelectedExprs),
	})

	i, err := g.Child.RowIter(ctx, row)
	if err != nil {
		span.Finish()
		return nil, err
	}

	var iter sql.RowIter
	if len(g.GroupByExprs) == 0 {
		iter = newGroupByIter(ctx, g.SelectedExprs, i)
	} else {
		iter = newGroupByGroupingIter(ctx, g.SelectedExprs, g.GroupByExprs, i)
	}

	return sql.NewSpanIter(span, iter), nil
}

// WithChildren implements the Node interface.
func (g *GroupBy) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(g, len(children), 1)
	}

	return NewGroupBy(g.SelectedExprs, g.GroupByExprs, children[0]), nil
}

// WithExpressions implements the Node interface.
func (g *GroupBy) WithExpressions(exprs ...sql.Expression) (sql.Node, error) {
	expected := len(g.SelectedExprs) + len(g.GroupByExprs)
	if len(exprs) != expected {
		return nil, sql.ErrInvalidChildrenNumber.New(g, len(exprs), expected)
	}

	var agg = make([]sql.Expression, len(g.SelectedExprs))
	for i := 0; i < len(g.SelectedExprs); i++ {
		agg[i] = exprs[i]
	}

	var grouping = make([]sql.Expression, len(g.GroupByExprs))
	offset := len(g.SelectedExprs)
	for i := 0; i < len(g.GroupByExprs); i++ {
		grouping[i] = exprs[i+offset]
	}

	return NewGroupBy(agg, grouping, g.Child), nil
}

func (g *GroupBy) String() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("GroupBy")

	var selectedExprs = make([]string, len(g.SelectedExprs))
	for i, e := range g.SelectedExprs {
		selectedExprs[i] = e.String()
	}

	var grouping = make([]string, len(g.GroupByExprs))
	for i, g := range g.GroupByExprs {
		grouping[i] = g.String()
	}

	_ = pr.WriteChildren(
		fmt.Sprintf("SelectedExprs(%s)", strings.Join(selectedExprs, ", ")),
		fmt.Sprintf("Grouping(%s)", strings.Join(grouping, ", ")),
		g.Child.String(),
	)
	return pr.String()
}

func (g *GroupBy) DebugString() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("GroupBy")

	var selectedExprs = make([]string, len(g.SelectedExprs))
	for i, e := range g.SelectedExprs {
		selectedExprs[i] = sql.DebugString(e)
	}

	var grouping = make([]string, len(g.GroupByExprs))
	for i, g := range g.GroupByExprs {
		grouping[i] = sql.DebugString(g)
	}

	_ = pr.WriteChildren(
		fmt.Sprintf("SelectedExprs(%s)", strings.Join(selectedExprs, ", ")),
		fmt.Sprintf("Grouping(%s)", strings.Join(grouping, ", ")),
		sql.DebugString(g.Child),
	)
	return pr.String()
}

// Expressions implements the Expressioner interface.
func (g *GroupBy) Expressions() []sql.Expression {
	var exprs []sql.Expression
	exprs = append(exprs, g.SelectedExprs...)
	exprs = append(exprs, g.GroupByExprs...)
	return exprs
}

type groupByIter struct {
	selectedExprs []sql.Expression
	child         sql.RowIter
	ctx           *sql.Context
	buf           []sql.Row
	done          bool
}

func newGroupByIter(ctx *sql.Context, selectedExprs []sql.Expression, child sql.RowIter) *groupByIter {
	return &groupByIter{
		selectedExprs: selectedExprs,
		child:         child,
		ctx:           ctx,
		buf:           make([]sql.Row, len(selectedExprs)),
	}
}

func (i *groupByIter) Next() (sql.Row, error) {
	if i.done {
		return nil, io.EOF
	}

	i.done = true

	for j, a := range i.selectedExprs {
		i.buf[j] = fillBuffer(a)
	}

	for {
		row, err := i.child.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		if err := updateBuffers(i.ctx, i.buf, i.selectedExprs, row); err != nil {
			return nil, err
		}
	}

	return evalBuffers(i.ctx, i.buf, i.selectedExprs)
}

func (i *groupByIter) Close() error {
	i.buf = nil
	return i.child.Close()
}

type groupByGroupingIter struct {
	selectedExprs []sql.Expression
	groupByExprs  []sql.Expression
	aggregations  sql.KeyValueCache
	keys          []uint64
	pos           int
	child         sql.RowIter
	ctx           *sql.Context
	dispose       sql.DisposeFunc
}

func newGroupByGroupingIter(
	ctx *sql.Context,
	selectedExprs, groupByExprs []sql.Expression,
	child sql.RowIter,
) *groupByGroupingIter {
	return &groupByGroupingIter{
		selectedExprs: selectedExprs,
		groupByExprs:  groupByExprs,
		child:         child,
		ctx:           ctx,
	}
}

func (i *groupByGroupingIter) Next() (sql.Row, error) {
	if i.aggregations == nil {
		i.aggregations, i.dispose = i.ctx.Memory.NewHistoryCache()
		if err := i.compute(); err != nil {
			return nil, err
		}
	}

	if i.pos >= len(i.keys) {
		return nil, io.EOF
	}

	buffers, err := i.aggregations.Get(i.keys[i.pos])
	if err != nil {
		return nil, err
	}
	i.pos++
	return evalBuffers(i.ctx, buffers.([]sql.Row), i.selectedExprs)
}

func (i *groupByGroupingIter) compute() error {
	for {
		row, err := i.child.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		key, err := groupingKey(i.ctx, i.groupByExprs, row)
		if err != nil {
			return err
		}

		if _, err := i.aggregations.Get(key); err != nil {
			var buf = make([]sql.Row, len(i.selectedExprs))
			for j, a := range i.selectedExprs {
				buf[j] = fillBuffer(a)
			}

			if err := i.aggregations.Put(key, buf); err != nil {
				return err
			}

			i.keys = append(i.keys, key)
		}

		b, err := i.aggregations.Get(key)
		if err != nil {
			return err
		}

		err = updateBuffers(i.ctx, b.([]sql.Row), i.selectedExprs, row)
		if err != nil {
			return err
		}
	}

	return nil
}

func (i *groupByGroupingIter) Close() error {
	i.aggregations = nil
	return i.child.Close()
}

var table = crc64.MakeTable(crc64.ISO)

func groupingKey(
	ctx *sql.Context,
	exprs []sql.Expression,
	row sql.Row,
) (uint64, error) {
	vals := make([]string, 0, len(exprs))

	for _, expr := range exprs {
		v, err := expr.Eval(ctx, row)
		if err != nil {
			return 0, err
		}
		vals = append(vals, fmt.Sprintf("%#v", v))
	}

	// TODO: use a faster hash func
	return crc64.Checksum([]byte(strings.Join(vals, ",")), table), nil
}

func fillBuffer(expr sql.Expression) sql.Row {
	switch n := expr.(type) {
	case sql.Aggregation:
		return n.NewBuffer()
	case *expression.Alias:
		return fillBuffer(n.Child)
	default:
		return nil
	}
}

func updateBuffers(
	ctx *sql.Context,
	buffers []sql.Row,
	aggregates []sql.Expression,
	row sql.Row,
) error {
	for i, a := range aggregates {
		if err := updateBuffer(ctx, buffers, i, a, row); err != nil {
			return err
		}
	}

	return nil
}

func updateBuffer(
	ctx *sql.Context,
	buffers []sql.Row,
	idx int,
	expr sql.Expression,
	row sql.Row,
) error {
	switch n := expr.(type) {
	case sql.Aggregation:
		return n.Update(ctx, buffers[idx], row)
	case *expression.Alias:
		return updateBuffer(ctx, buffers, idx, n.Child, row)
	default:
		val, err := expr.Eval(ctx, row)
		if err != nil {
			return err
		}
		buffers[idx] = sql.NewRow(val)
		return nil
	}
}

func evalBuffers(
	ctx *sql.Context,
	buffers []sql.Row,
	aggregates []sql.Expression,
) (sql.Row, error) {
	var row = make(sql.Row, len(aggregates))

	for i, agg := range aggregates {
		val, err := evalBuffer(ctx, agg, buffers[i])
		if err != nil {
			return nil, err
		}
		row[i] = val
	}

	return row, nil
}

func evalBuffer(
	ctx *sql.Context,
	aggregation sql.Expression,
	buffer sql.Row,
) (interface{}, error) {
	switch n := aggregation.(type) {
	case sql.Aggregation:
		return n.Eval(ctx, buffer)
	case *expression.Alias:
		return evalBuffer(ctx, n.Child, buffer)
	default:
		if len(buffer) > 0 {
			return buffer[0], nil
		}
		return nil, nil
	}
}
