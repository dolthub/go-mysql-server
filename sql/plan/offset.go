package plan

import "gopkg.in/src-d/go-mysql-server.v0/sql"

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
func (o *Offset) RowIter() (sql.RowIter, error) {
	it, err := o.Child.RowIter()
	if err != nil {
		return nil, err
	}
	return &offsetIter{o.n, it}, nil
}

// TransformUp implements the Transformable interface.
func (o *Offset) TransformUp(f func(sql.Node) sql.Node) sql.Node {
	return f(NewOffset(o.n, o.Child.TransformUp(f)))
}

// TransformExpressionsUp implements the Transformable interface.
func (o *Offset) TransformExpressionsUp(f func(sql.Expression) sql.Expression) sql.Node {
	return NewOffset(o.n, o.Child.TransformExpressionsUp(f))
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
