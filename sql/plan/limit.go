package plan

import (
	"io"

	"github.com/mvader/gitql/sql"
)

type Limit struct {
	UnaryNode
	size int64
}

func NewLimit(size int64, child sql.Node) *Limit {
	return &Limit{
		UnaryNode: UnaryNode{Child: child},
		size:      size,
	}
}

func (l *Limit) Schema() sql.Schema {
	return l.UnaryNode.Child.Schema()
}

func (p *Limit) Resolved() bool {
	return p.UnaryNode.Child.Resolved()
}

func (l *Limit) RowIter() (sql.RowIter, error) {
	li, err := l.Child.RowIter()
	if err != nil {
		return nil, err
	}
	return &limitIter{l, 0, li}, nil
}

func (l *Limit) TransformUp(f func(sql.Node) sql.Node) sql.Node {
	c := l.UnaryNode.Child.TransformUp(f)
	n := NewLimit(l.size, c)

	return f(n)
}

func (l *Limit) TransformExpressionsUp(f func(sql.Expression) sql.Expression) sql.Node {
	c := l.UnaryNode.Child.TransformExpressionsUp(f)
	n := NewLimit(l.size, c)

	return n
}

type limitIter struct {
	l          *Limit
	currentPos int64
	childIter  sql.RowIter
}

func (li *limitIter) Next() (sql.Row, error) {
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
