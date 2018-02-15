package plan

import "gopkg.in/src-d/go-mysql-server.v0/sql"

type Offset struct {
	UnaryNode
	n int64
}

func NewOffset(n int64, child sql.Node) *Offset {
	return &Offset{
		UnaryNode: UnaryNode{Child: child},
		n:         n,
	}
}

func (o *Offset) Resolved() bool {
	return o.Child.Resolved()
}

func (o *Offset) RowIter() (sql.RowIter, error) {
	it, err := o.Child.RowIter()
	if err != nil {
		return nil, err
	}
	return &offsetIter{o.n, it}, nil
}

func (o *Offset) TransformUp(f func(sql.Node) sql.Node) sql.Node {
	c := o.Child.TransformUp(f)
	return f(NewOffset(o.n, c))
}

func (o *Offset) TransformExpressionsUp(f func(sql.Expression) sql.Expression) sql.Node {
	c := o.Child.TransformExpressionsUp(f)
	return NewOffset(o.n, c)
}

type offsetIter struct {
	skip      int64
	childIter sql.RowIter
}

func (i *offsetIter) Next() (sql.Row, error) {
	if i.skip > 0 {
		for j := int64(0); j < i.skip; j++ {
			_, err := i.childIter.Next()
			if err != nil {
				return nil, err
			}
		}
		i.skip = 0
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
