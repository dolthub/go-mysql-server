package plan

import (
	"io"

	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

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
func (l *Limit) RowIter() (sql.RowIter, error) {
	li, err := l.Child.RowIter()
	if err != nil {
		return nil, err
	}
	return &limitIter{l, 0, li}, nil
}

// TransformUp implements the Transformable interface.
func (l *Limit) TransformUp(f func(sql.Node) sql.Node) sql.Node {
	c := l.Child.TransformUp(f)
	return f(NewLimit(l.size, c))
}

// TransformExpressionsUp implements the Transformable interface.
func (l *Limit) TransformExpressionsUp(f func(sql.Expression) sql.Expression) sql.Node {
	c := l.Child.TransformExpressionsUp(f)
	return NewLimit(l.size, c)
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
