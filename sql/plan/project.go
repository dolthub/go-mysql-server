package plan

import (
	"gopkg.in/sqle/sqle.v0/sql"
)

type Project struct {
	UnaryNode
	Expressions []sql.Expression
}

func NewProject(expressions []sql.Expression, child sql.Node) *Project {
	return &Project{
		UnaryNode:   UnaryNode{child},
		Expressions: expressions,
	}
}

func (p *Project) Schema() sql.Schema {
	var s sql.Schema
	for _, e := range p.Expressions {
		f := sql.Column{
			Name: e.Name(),
			Type: e.Type(),
		}
		s = append(s, f)
	}
	return s
}

func (p *Project) Resolved() bool {
	return p.UnaryNode.Child.Resolved() &&
		expressionsResolved(p.Expressions...)
}

func (p *Project) RowIter() (sql.RowIter, error) {
	i, err := p.Child.RowIter()
	if err != nil {
		return nil, err
	}
	return &iter{p, i}, nil
}

func (p *Project) TransformUp(f func(sql.Node) sql.Node) sql.Node {
	c := p.UnaryNode.Child.TransformUp(f)
	n := NewProject(p.Expressions, c)

	return f(n)
}

func (p *Project) TransformExpressionsUp(f func(sql.Expression) sql.Expression) sql.Node {
	c := p.UnaryNode.Child.TransformExpressionsUp(f)
	es := transformExpressionsUp(f, p.Expressions)
	n := NewProject(es, c)

	return n
}

type iter struct {
	p         *Project
	childIter sql.RowIter
}

func (i *iter) Next() (sql.Row, error) {
	childRow, err := i.childIter.Next()
	if err != nil {
		return nil, err
	}
	return filterRow(i.p.Expressions, childRow), nil
}

func (i *iter) Close() error {
	return i.childIter.Close()
}

func filterRow(expressions []sql.Expression, row sql.Row) sql.Row {
	fields := []interface{}{}
	for _, expr := range expressions {
		fields = append(fields, expr.Eval(row))
	}
	return sql.NewRow(fields...)
}
