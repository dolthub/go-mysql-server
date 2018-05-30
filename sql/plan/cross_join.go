package plan

import (
	"io"
	"reflect"

	opentracing "github.com/opentracing/opentracing-go"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

// CrossJoin is a cross join between two tables.
type CrossJoin struct {
	BinaryNode
}

// NewCrossJoin creates a new cross join node from two tables.
func NewCrossJoin(left sql.Node, right sql.Node) *CrossJoin {
	return &CrossJoin{
		BinaryNode: BinaryNode{
			Left:  left,
			Right: right,
		},
	}
}

// Schema implements the Node interface.
func (p *CrossJoin) Schema() sql.Schema {
	return append(p.Left.Schema(), p.Right.Schema()...)
}

// Resolved implements the Resolvable interface.
func (p *CrossJoin) Resolved() bool {
	return p.Left.Resolved() && p.Right.Resolved()
}

// RowIter implements the Node interface.
func (p *CrossJoin) RowIter(ctx *sql.Context) (sql.RowIter, error) {
	var left, right string
	if leftTable, ok := p.Left.(sql.Nameable); ok {
		left = leftTable.Name()
	} else {
		left = reflect.TypeOf(p.Left).String()
	}

	if rightTable, ok := p.Right.(sql.Nameable); ok {
		right = rightTable.Name()
	} else {
		right = reflect.TypeOf(p.Right).String()
	}

	span, ctx := ctx.Span("plan.CrossJoin", opentracing.Tags{
		"left":  left,
		"right": right,
	})

	li, err := p.Left.RowIter(ctx)
	if err != nil {
		span.Finish()
		return nil, err
	}

	return sql.NewSpanIter(span, &crossJoinIterator{
		l:  li,
		rp: p.Right,
		s:  ctx,
	}), nil
}

// TransformUp implements the Transformable interface.
func (p *CrossJoin) TransformUp(f sql.TransformNodeFunc) (sql.Node, error) {
	left, err := p.Left.TransformUp(f)
	if err != nil {
		return nil, err
	}

	right, err := p.Right.TransformUp(f)
	if err != nil {
		return nil, err
	}

	return f(NewCrossJoin(left, right))
}

// TransformExpressionsUp implements the Transformable interface.
func (p *CrossJoin) TransformExpressionsUp(f sql.TransformExprFunc) (sql.Node, error) {
	left, err := p.Left.TransformExpressionsUp(f)
	if err != nil {
		return nil, err
	}

	right, err := p.Right.TransformExpressionsUp(f)
	if err != nil {
		return nil, err
	}

	return NewCrossJoin(left, right), nil
}

func (p *CrossJoin) String() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("CrossJoin")
	_ = pr.WriteChildren(p.Left.String(), p.Right.String())
	return pr.String()
}

type rowIterProvider interface {
	RowIter(*sql.Context) (sql.RowIter, error)
}

type crossJoinIterator struct {
	l  sql.RowIter
	rp rowIterProvider
	r  sql.RowIter
	s  *sql.Context

	leftRow sql.Row
}

func (i *crossJoinIterator) Next() (sql.Row, error) {
	for {
		if i.leftRow == nil {
			r, err := i.l.Next()
			if err != nil {
				return nil, err
			}

			i.leftRow = r
		}

		if i.r == nil {
			iter, err := i.rp.RowIter(i.s)
			if err != nil {
				return nil, err
			}

			i.r = iter
		}

		rightRow, err := i.r.Next()
		if err == io.EOF {
			i.r = nil
			i.leftRow = nil
			continue
		}

		if err != nil {
			return nil, err
		}

		var row sql.Row
		row = append(row, i.leftRow...)
		row = append(row, rightRow...)

		return row, nil
	}
}

func (i *crossJoinIterator) Close() error {
	if err := i.l.Close(); err != nil {
		if i.r != nil {
			_ = i.r.Close()
		}
		return err
	}

	if i.r != nil {
		return i.r.Close()
	}

	return nil
}
