package plan

import (
	"io"

	"github.com/gitql/gitql/sql"
)

type CrossJoin struct {
	BinaryNode
}

func NewCrossJoin(left sql.Node, right sql.Node) *CrossJoin {
	return &CrossJoin{
		BinaryNode: BinaryNode{
			Left:  left,
			Right: right,
		},
	}
}

func (p *CrossJoin) Schema() sql.Schema {
	return append(p.Left.Schema(), p.Right.Schema()...)
}

func (p *CrossJoin) Resolved() bool {
	return p.Left.Resolved() && p.Right.Resolved()
}

func (p *CrossJoin) RowIter() (sql.RowIter, error) {
	li, err := p.Left.RowIter()
	if err != nil {
		return nil, err
	}

	ri, err := p.Right.RowIter()
	if err != nil {
		return nil, err
	}

	return &crossJoinIterator{
		li: li,
		ri: ri,
	}, nil
}

func (p *CrossJoin) TransformUp(f func(sql.Node) sql.Node) sql.Node {
	ln := p.BinaryNode.Left.TransformUp(f)
	rn := p.BinaryNode.Right.TransformUp(f)

	n := NewCrossJoin(ln, rn)

	return f(n)
}

func (p *CrossJoin) TransformExpressionsUp(f func(sql.Expression) sql.Expression) sql.Node {
	ln := p.BinaryNode.Left.TransformExpressionsUp(f)
	rn := p.BinaryNode.Right.TransformExpressionsUp(f)

	return NewCrossJoin(ln, rn)
}

type crossJoinIterator struct {
	li sql.RowIter
	ri sql.RowIter

	// TODO use a method to reset right iterator in order to not duplicate rows into memory
	rightRows []sql.Row
	index     int
	leftRow   sql.Row
}

func (i *crossJoinIterator) Next() (sql.Row, error) {
	if len(i.rightRows) == 0 {
		if err := i.fillRows(); err != io.EOF {
			return nil, err
		}

		if len(i.rightRows) == 0 {
			return nil, io.EOF
		}
	}

	if i.leftRow == nil {
		lr, err := i.li.Next()
		if err != nil {
			return nil, err
		}

		i.index = 0
		i.leftRow = lr
	}

	row := append(i.leftRow, i.rightRows[i.index]...)
	i.index++
	if i.index >= len(i.rightRows) {
		i.index = 0
		i.leftRow = nil
	}

	return row, nil
}

func (i *crossJoinIterator) fillRows() error {
	for {
		rr, err := i.ri.Next()
		if err != nil {
			return err
		}

		i.rightRows = append(i.rightRows, rr)
	}
}
