package plan

import (
	"fmt"
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
	return sql.NewSpanIter(span, newGroupByIter(ctx, p, i)), nil
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
	p         *GroupBy
	childIter sql.RowIter
	rows      []sql.Row
	idx       int
	ctx       *sql.Context
}

func newGroupByIter(s *sql.Context, p *GroupBy, child sql.RowIter) *groupByIter {
	return &groupByIter{
		p:         p,
		childIter: child,
		rows:      nil,
		idx:       -1,
		ctx:       s,
	}
}

func (i *groupByIter) Next() (sql.Row, error) {
	if i.idx == -1 {
		err := i.computeRows()
		if err != nil {
			return nil, err
		}
		i.idx = 0
	}
	if i.idx >= len(i.rows) {
		return nil, io.EOF
	}
	row := i.rows[i.idx]
	i.idx++
	return row, nil
}

func (i *groupByIter) Close() error {
	i.rows = nil
	return i.childIter.Close()
}

func (i *groupByIter) computeRows() error {
	rows := []sql.Row{}
	for {
		childRow, err := i.childIter.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		rows = append(rows, childRow)
	}

	rows, err := groupBy(i.ctx, rows, i.p.Aggregate, i.p.Grouping)
	if err != nil {
		return err
	}

	i.rows = rows
	return nil
}

func groupBy(
	ctx *sql.Context,
	rows []sql.Row,
	aggExpr []sql.Expression,
	groupExpr []sql.Expression,
) ([]sql.Row, error) {
	//TODO: currently, we first group all rows, and then
	//      compute aggregations in a separate stage. We should
	//      compute aggregations incrementally instead.

	hrows := map[interface{}][]sql.Row{}
	for _, row := range rows {
		key, err := groupingKey(ctx, groupExpr, row)
		if err != nil {
			return nil, err
		}
		hrows[key] = append(hrows[key], row)
	}

	result := make([]sql.Row, 0, len(hrows))
	for _, rows := range hrows {
		row, err := aggregate(ctx, aggExpr, rows)
		if err != nil {
			return nil, err
		}
		result = append(result, row)
	}

	return result, nil
}

func groupingKey(
	ctx *sql.Context,
	exprs []sql.Expression,
	row sql.Row,
) (interface{}, error) {
	//TODO: use a more robust/efficient way of calculating grouping keys.
	vals := make([]string, 0, len(exprs))
	for _, expr := range exprs {
		v, err := expr.Eval(ctx, row)
		if err != nil {
			return nil, err
		}
		vals = append(vals, fmt.Sprintf("%#v", v))
	}

	return strings.Join(vals, ","), nil
}

func aggregate(
	ctx *sql.Context,
	exprs []sql.Expression,
	rows []sql.Row,
) (sql.Row, error) {
	buffers := make([]sql.Row, len(exprs))
	for i, expr := range exprs {
		buffers[i] = fillBuffer(expr)
	}

	for _, row := range rows {
		for i, expr := range exprs {
			if err := updateBuffer(ctx, buffers, i, expr, row); err != nil {
				return nil, err
			}
		}
	}

	fields := make([]interface{}, 0, len(exprs))
	for i, expr := range exprs {
		field, err := expr.Eval(ctx, buffers[i])
		if err != nil {
			return nil, err
		}

		fields = append(fields, field)
	}

	return sql.NewRow(fields...), nil
}

func fillBuffer(expr sql.Expression) sql.Row {
	switch n := expr.(type) {
	case sql.Aggregation:
		return n.NewBuffer()
	case *expression.Alias:
		return fillBuffer(n.Child)
	default:
		return sql.NewRow(nil)
	}
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
		n.Update(ctx, buffers[idx], row)
		return nil
	case *expression.Alias:
		return updateBuffer(ctx, buffers, idx, n.Child, row)
	case *expression.GetField:
		buffers[idx] = row
		return nil
	default:
		return ErrGroupBy.New(n.String())
	}
}
