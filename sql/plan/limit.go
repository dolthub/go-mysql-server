package plan

import (
	"io"

	opentracing "github.com/opentracing/opentracing-go"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

var _ sql.Node = &Limit{}

// Limit is a node that only allows up to N rows to be retrieved.
type Limit struct {
	UnaryNode
	size int64
}

// NewLimit creates a new Limit node with the given size.
func NewLimit(size int64, child sql.Node) *Limit {
	return &Limit{
		UnaryNode: UnaryNode{Child: child},
		size:      size,
	}
}

// Resolved implements the Resolvable interface.
func (l *Limit) Resolved() bool {
	return l.UnaryNode.Child.Resolved()
}

// RowIter implements the Node interface.
func (l *Limit) RowIter(ctx *sql.Context) (sql.RowIter, error) {
	span, ctx := ctx.Span("plan.Limit", opentracing.Tag{Key: "limit", Value: l.size})

	li, err := l.Child.RowIter(ctx)
	if err != nil {
		span.Finish()
		return nil, err
	}
	return sql.NewSpanIter(span, &limitIter{l, 0, li}), nil
}

// TransformUp implements the Transformable interface.
func (l *Limit) TransformUp(f sql.TransformNodeFunc) (sql.Node, error) {
	child, err := l.Child.TransformUp(f)
	if err != nil {
		return nil, err
	}
	return f(NewLimit(l.size, child))
}

// TransformExpressionsUp implements the Transformable interface.
func (l *Limit) TransformExpressionsUp(f sql.TransformExprFunc) (sql.Node, error) {
	child, err := l.Child.TransformExpressionsUp(f)
	if err != nil {
		return nil, err
	}
	return NewLimit(l.size, child), nil
}

func (l Limit) String() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("Limit(%d)", l.size)
	_ = pr.WriteChildren(l.Child.String())
	return pr.String()
}

type limitIter struct {
	l          *Limit
	currentPos int64
	childIter  sql.RowIter
}

func (li *limitIter) Next() (sql.Row, error) {
	for {
		if li.currentPos >= li.l.size {
			return nil, io.EOF
		}
		childRow, err := li.childIter.Next()
		li.currentPos++
		if err != nil {
			return nil, err
		}

		return childRow, nil
	}
}

func (li *limitIter) Close() error {
	return li.childIter.Close()
}
