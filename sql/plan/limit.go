package plan

import (
	"io"

	opentracing "github.com/opentracing/opentracing-go"

	"github.com/liquidata-inc/go-mysql-server/sql"
)

// Limit is a node that only allows up to N rows to be retrieved.
type Limit struct {
	UnaryNode
	Limit int64
}

// NewLimit creates a new Limit node with the given size.
func NewLimit(size int64, child sql.Node) *Limit {
	return &Limit{
		UnaryNode: UnaryNode{Child: child},
		Limit:     size,
	}
}

// Resolved implements the Resolvable interface.
func (l *Limit) Resolved() bool {
	return l.UnaryNode.Child.Resolved()
}

// RowIter implements the Node interface.
func (l *Limit) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	span, ctx := ctx.Span("plan.Limit", opentracing.Tag{Key: "limit", Value: l.Limit})

	li, err := l.Child.RowIter(ctx, row)
	if err != nil {
		span.Finish()
		return nil, err
	}
	return sql.NewSpanIter(span, &limitIter{l, 0, li}), nil
}

// WithChildren implements the Node interface.
func (l *Limit) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(l, len(children), 1)
	}
	return NewLimit(l.Limit, children[0]), nil
}

func (l Limit) String() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("Limit(%d)", l.Limit)
	_ = pr.WriteChildren(l.Child.String())
	return pr.String()
}

func (l Limit) DebugString() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("Limit(%d)", l.Limit)
	_ = pr.WriteChildren(sql.DebugString(l.Child))
	return pr.String()
}

type limitIter struct {
	l          *Limit
	currentPos int64
	childIter  sql.RowIter
}

func (li *limitIter) Next() (sql.Row, error) {
	if li.currentPos >= li.l.Limit {
		return nil, io.EOF
	}

	childRow, err := li.childIter.Next()
	li.currentPos++
	if err != nil {
		return nil, err
	}

	return childRow, nil
}

func (li *limitIter) Close() error {
	return li.childIter.Close()
}
