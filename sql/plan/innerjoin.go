package plan

import (
	"io"

	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

// InnerJoin is an inner join between two tables.
type InnerJoin struct {
	BinaryNode
	Cond sql.Expression
}

// NewInnerJoin creates a new inner join node from two tables.
func NewInnerJoin(left, right sql.Node, cond sql.Expression) *InnerJoin {
	return &InnerJoin{
		BinaryNode: BinaryNode{
			Left:  left,
			Right: right,
		},
		Cond: cond,
	}
}

// Schema implements the Node interface.
func (j *InnerJoin) Schema() sql.Schema {
	return append(j.Left.Schema(), j.Right.Schema()...)
}

// Resolved implements the Resolvable interface.
func (j *InnerJoin) Resolved() bool {
	return j.Left.Resolved() && j.Right.Resolved()
}

// RowIter implements the Node interface.
func (j *InnerJoin) RowIter() (sql.RowIter, error) {
	l, err := j.Left.RowIter()
	if err != nil {
		return nil, err
	}

	return &innerJoinIterator{
		l:    l,
		rp:   j.Right,
		cond: j.Cond,
	}, nil
}

// TransformUp implements the Transformable interface.
func (j *InnerJoin) TransformUp(f func(sql.Node) sql.Node) sql.Node {
	return f(NewInnerJoin(j.Left.TransformUp(f), j.Right.TransformUp(f), j.Cond))
}

// TransformExpressionsUp implements the Transformable interface.
func (j *InnerJoin) TransformExpressionsUp(f func(sql.Expression) sql.Expression) sql.Node {
	return NewInnerJoin(
		j.Left.TransformExpressionsUp(f),
		j.Right.TransformExpressionsUp(f),
		j.Cond.TransformUp(f),
	)
}

type rowIterProvider interface {
	RowIter() (sql.RowIter, error)
}

type innerJoinIterator struct {
	l    sql.RowIter
	rp   rowIterProvider
	r    sql.RowIter
	cond sql.Expression

	leftRow sql.Row
}

func (i *innerJoinIterator) Next() (sql.Row, error) {
	for {
		if i.leftRow == nil {
			r, err := i.l.Next()
			if err != nil {
				return nil, err
			}

			i.leftRow = r
		}

		if i.r == nil {
			iter, err := i.rp.RowIter()
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

		row := append(i.leftRow, rightRow...)
		result, err := i.cond.Eval(row)
		if err != nil {
			return nil, err
		}

		if result == true {
			return row, nil
		}
	}
}

func (i *innerJoinIterator) Close() error {
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
