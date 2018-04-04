package plan

import (
	opentracing "github.com/opentracing/opentracing-go"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

// Offset is a node that skips the first N rows.
type Offset struct {
	UnaryNode
	n int64
}

// NewOffset creates a new Offset node.
func NewOffset(n int64, child sql.Node) *Offset {
	return &Offset{
		UnaryNode: UnaryNode{Child: child},
		n:         n,
	}
}

// Resolved implements the Resolvable interface.
func (o *Offset) Resolved() bool {
	return o.Child.Resolved()
}

// RowIter implements the Node interface.
func (o *Offset) RowIter(ctx *sql.Context) (sql.RowIter, error) {
	span, ctx := ctx.Span("plan.Offset", opentracing.Tag{Key: "offset", Value: o.n})

	it, err := o.Child.RowIter(ctx)
	if err != nil {
		span.Finish()
		return nil, err
	}
	return sql.NewSpanIter(span, &offsetIter{o.n, it}), nil
}

// TransformUp implements the Transformable interface.
func (o *Offset) TransformUp(f sql.TransformNodeFunc) (sql.Node, error) {
	child, err := o.Child.TransformUp(f)
	if err != nil {
		return nil, err
	}
	return f(NewOffset(o.n, child))
}

// TransformExpressionsUp implements the Transformable interface.
func (o *Offset) TransformExpressionsUp(f sql.TransformExprFunc) (sql.Node, error) {
	child, err := o.Child.TransformExpressionsUp(f)
	if err != nil {
		return nil, err
	}
	return NewOffset(o.n, child), nil
}

func (o Offset) String() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("Offset(%d)", o.n)
	_ = pr.WriteChildren(o.Child.String())
	return pr.String()
}

type offsetIter struct {
	skip      int64
	childIter sql.RowIter
}

func (i *offsetIter) Next() (sql.Row, error) {
	if i.skip > 0 {
		for i.skip > 0 {
			_, err := i.childIter.Next()
			if err != nil {
				return nil, err
			}
			i.skip--
		}
	}

	row, err := i.childIter.Next()
	if err != nil {
		return nil, err
	}

	return row, nil
}

func (i *offsetIter) Close() error {
	return i.childIter.Close()
}
