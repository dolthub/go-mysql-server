package plan

import (
	"errors"
	"io"

	"gopkg.in/sqle/sqle.v0/sql"
	"gopkg.in/sqle/sqle.v0/sql/expression"
)

type InsertInto struct {
	BinaryNode
	Columns []string
}

func NewInsertInto(dst, src sql.Node, cols []string) *InsertInto {
	return &InsertInto{
		BinaryNode: BinaryNode{Left: dst, Right: src},
		Columns:    cols,
	}
}

func (p *InsertInto) Schema() sql.Schema {
	return sql.Schema{{
		Name:     "updated",
		Type:     sql.BigInteger,
		Default:  int64(0),
		Nullable: false,
	}}
}

func (p *InsertInto) Execute() (int, error) {
	insertable, ok := p.Left.(sql.Inserter)
	if !ok {
		return 0, errors.New("destination table does not support INSERT TO")
	}

	dstSchema := p.Left.Schema()
	projExprs := make([]sql.Expression, len(dstSchema))
	for i, f := range dstSchema {
		found := false
		for j, col := range p.Columns {
			if f.Name == col {
				projExprs[i] = expression.NewGetField(j, f.Type, f.Name, f.Nullable)
				found = true
				break
			}
		}

		if !found {
			projExprs[i] = expression.NewLiteral(f.Type.Default(), f.Type)
		}
	}

	proj := NewProject(projExprs, p.Right)

	iter, err := proj.RowIter()
	if err != nil {
		return 0, err
	}

	i := 0
	for {
		row, err := iter.Next()
		if err == io.EOF {
			break
		}

		if err != nil {
			_ = iter.Close()
			return i, err
		}

		if err := insertable.Insert(row); err != nil {
			_ = iter.Close()
			return i, err
		}

		i++
	}

	return i, nil
}

func (p *InsertInto) RowIter() (sql.RowIter, error) {
	n, err := p.Execute()
	if err != nil {
		return nil, err
	}

	return sql.RowsToRowIter(sql.NewRow(int64(n))), nil
}

func (p *InsertInto) TransformUp(f func(sql.Node) sql.Node) sql.Node {
	ln := p.BinaryNode.Left.TransformUp(f)
	rn := p.BinaryNode.Right.TransformUp(f)

	n := NewInsertInto(ln, rn, p.Columns)
	return f(n)
}

func (p *InsertInto) TransformExpressionsUp(f func(sql.Expression) sql.Expression) sql.Node {
	ln := p.BinaryNode.Left.TransformExpressionsUp(f)
	rn := p.BinaryNode.Right.TransformExpressionsUp(f)

	return NewInsertInto(ln, rn, p.Columns)
}
