package plan

import (
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/src-d/go-mysql-server/sql"
)

// Offset is a node that skips the first N rows.
type Offset struct {
	UnaryNode
	Offset int64
}

// NewOffset creates a new Offset node.
func NewOffset(n int64, child sql.Node) *Offset {
	return &Offset{
		UnaryNode: UnaryNode{Child: child},
		Offset:    n,
	}
}

// Resolved implements the Resolvable interface.
func (o *Offset) Resolved() bool {
	return o.Child.Resolved()
}

// RowIter implements the Node interface.
func (o *Offset) RowIter(ctx *sql.Context) (sql.RowIter, error) {
	span, ctx := ctx.Span("plan.Offset", opentracing.Tag{Key: "offset", Value: o.Offset})

	it, err := o.Child.RowIter(ctx)
	if err != nil {
		span.Finish()
		return nil, err
	}
	return sql.NewSpanIter(span, &offsetIter{o.Offset, it}), nil
}

// WithChildren implements the Node interface.
func (o *Offset) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(o, len(children), 1)
	}
	return NewOffset(o.Offset, children[0]), nil
}

func (o Offset) String() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("Offset(%d)", o.Offset)
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
