package plan

import (
	"fmt"
	"hash/crc64"
	"io"
	"strings"

	opentracing "github.com/opentracing/opentracing-go"
	errors "gopkg.in/src-d/go-errors.v1"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
)

// ErrGroupBy is returned when the aggregation is not supported.
var ErrGroupBy = errors.NewKind("group by aggregation '%v' not supported")

// GroupBy groups the rows by some expressions.
type GroupBy struct {
	UnaryNode
	Aggregate []sql.Expression
	Grouping  []sql.Expression
}

// NewGroupBy creates a new GroupBy node.
func NewGroupBy(
	aggregate []sql.Expression,
	grouping []sql.Expression,
	child sql.Node,
) *GroupBy {

	return &GroupBy{
		UnaryNode: UnaryNode{Child: child},
		Aggregate: aggregate,
		Grouping:  grouping,
	}
}

// Resolved implements the Resolvable interface.
func (p *GroupBy) Resolved() bool {
	return p.UnaryNode.Child.Resolved() &&
		expressionsResolved(p.Aggregate...) &&
		expressionsResolved(p.Grouping...)
}

// Schema implements the Node interface.
func (p *GroupBy) Schema() sql.Schema {
	var s = make(sql.Schema, len(p.Aggregate))
	for i, e := range p.Aggregate {
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
func (p *GroupBy) RowIter(ctx *sql.Context) (sql.RowIter, error) {
	span, ctx := ctx.Span("plan.GroupBy", opentracing.Tags{
		"groupings":  len(p.Grouping),
		"aggregates": len(p.Aggregate),
	})

	i, err := p.Child.RowIter(ctx)
	if err != nil {
		span.Finish()
		return nil, err
	}

	var iter sql.RowIter
	if len(p.Grouping) == 0 {
		iter = newGroupByIter(ctx, p.Aggregate, i)
	} else {
		iter = newGroupByGroupingIter(ctx, p.Aggregate, p.Grouping, i)
	}

	return sql.NewSpanIter(span, iter), nil
}

// TransformUp implements the Transformable interface.
func (p *GroupBy) TransformUp(f sql.TransformNodeFunc) (sql.Node, error) {
	child, err := p.Child.TransformUp(f)
	if err != nil {
		return nil, err
	}
	return f(NewGroupBy(p.Aggregate, p.Grouping, child))
}

// TransformExpressionsUp implements the Transformable interface.
func (p *GroupBy) TransformExpressionsUp(f sql.TransformExprFunc) (sql.Node, error) {
	aggregate, err := transformExpressionsUp(f, p.Aggregate)
	if err != nil {
		return nil, err
	}

	grouping, err := transformExpressionsUp(f, p.Grouping)
	if err != nil {
		return nil, err
	}

	child, err := p.Child.TransformExpressionsUp(f)
	if err != nil {
		return nil, err
	}

	return NewGroupBy(aggregate, grouping, child), nil
}

func (p *GroupBy) String() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("GroupBy")

	var aggregate = make([]string, len(p.Aggregate))
	for i, agg := range p.Aggregate {
		aggregate[i] = agg.String()
	}

	var grouping = make([]string, len(p.Grouping))
	for i, g := range p.Grouping {
		grouping[i] = g.String()
	}

	_ = pr.WriteChildren(
		fmt.Sprintf("Aggregate(%s)", strings.Join(aggregate, ", ")),
		fmt.Sprintf("Grouping(%s)", strings.Join(grouping, ", ")),
		p.Child.String(),
	)
	return pr.String()
}

// Expressions implements the Expressioner interface.
func (p *GroupBy) Expressions() []sql.Expression {
	var exprs []sql.Expression
	exprs = append(exprs, p.Aggregate...)
	exprs = append(exprs, p.Grouping...)
	return exprs
}

// TransformExpressions implements the Expressioner interface.
func (p *GroupBy) TransformExpressions(f sql.TransformExprFunc) (sql.Node, error) {
	agg, err := transformExpressionsUp(f, p.Aggregate)
	if err != nil {
		return nil, err
	}

	group, err := transformExpressionsUp(f, p.Grouping)
	if err != nil {
		return nil, err
	}

	return NewGroupBy(agg, group, p.Child), nil
}

type groupByIter struct {
	aggregate []sql.Expression
	child     sql.RowIter
	ctx       *sql.Context
	buf       []sql.Row
	done      bool
}

func newGroupByIter(ctx *sql.Context, aggregate []sql.Expression, child sql.RowIter) *groupByIter {
	return &groupByIter{
		aggregate: aggregate,
		child:     child,
		ctx:       ctx,
		buf:       make([]sql.Row, len(aggregate)),
	}
}

func (i *groupByIter) Next() (sql.Row, error) {
	if i.done {
		return nil, io.EOF
	}

	i.done = true

	for j, a := range i.aggregate {
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

		if err := updateBuffers(i.ctx, i.buf, i.aggregate, row); err != nil {
			return nil, err
		}
	}

	return evalBuffers(i.ctx, i.buf, i.aggregate)
}

func (i *groupByIter) Close() error {
	i.buf = nil
	return i.child.Close()
}

type groupByGroupingIter struct {
	aggregate   []sql.Expression
	grouping    []sql.Expression
	aggregation map[uint64][]sql.Row
	keys        []uint64
	pos         int
	child       sql.RowIter
	ctx         *sql.Context
}

func newGroupByGroupingIter(
	ctx *sql.Context,
	aggregate, grouping []sql.Expression,
	child sql.RowIter,
) *groupByGroupingIter {
	return &groupByGroupingIter{
		aggregate: aggregate,
		grouping:  grouping,
		child:     child,
		ctx:       ctx,
	}
}

func (i *groupByGroupingIter) Next() (sql.Row, error) {
	if i.aggregation == nil {
		i.aggregation = make(map[uint64][]sql.Row)
		if err := i.compute(); err != nil {
			return nil, err
		}
	}

	if i.pos >= len(i.keys) {
		return nil, io.EOF
	}

	buffers := i.aggregation[i.keys[i.pos]]
	i.pos++
	return evalBuffers(i.ctx, buffers, i.aggregate)
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

		key, err := groupingKey(i.ctx, i.grouping, row)
		if err != nil {
			return err
		}

		if _, ok := i.aggregation[key]; !ok {
			var buf = make([]sql.Row, len(i.aggregate))
			for j, a := range i.aggregate {
				buf[j] = fillBuffer(a)
			}
			i.aggregation[key] = buf
			i.keys = append(i.keys, key)
		}

		err = updateBuffers(i.ctx, i.aggregation[key], i.aggregate, row)
		if err != nil {
			return err
		}
	}

	return nil
}

func (i *groupByGroupingIter) Close() error {
	i.aggregation = nil
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
	aggregate []sql.Expression,
	row sql.Row,
) error {
	for i, a := range aggregate {
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
	case *expression.GetField:
		val, err := expr.Eval(ctx, row)
		if err != nil {
			return err
		}
		buffers[idx] = sql.NewRow(val)
		return nil
	default:
		return ErrGroupBy.New(n.String())
	}
}

func evalBuffers(
	ctx *sql.Context,
	buffers []sql.Row,
	aggregate []sql.Expression,
) (sql.Row, error) {
	var row = make(sql.Row, len(aggregate))

	for i, agg := range aggregate {
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
	case *expression.GetField:
		return buffer[0], nil
	default:
		return nil, ErrGroupBy.New(n.String())
	}
}
