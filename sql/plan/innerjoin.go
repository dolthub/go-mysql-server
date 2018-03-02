package plan

import (
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
func (j *InnerJoin) RowIter(session sql.Session) (sql.RowIter, error) {
	l, err := j.Left.RowIter(session)
	if err != nil {
		return nil, err
	}

	return &filterIter{
		childIter: &crossJoinIterator{
			l:  l,
			rp: j.Right,
		},
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
