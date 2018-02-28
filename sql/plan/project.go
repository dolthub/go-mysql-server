package plan

import (
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

// Project is a projection of certain expression from the children node.
type Project struct {
	UnaryNode
	// Expression projected.
	Expressions []sql.Expression
}

// NewProject creates a new projection.
func NewProject(expressions []sql.Expression, child sql.Node) *Project {
	return &Project{
		UnaryNode:   UnaryNode{child},
		Expressions: expressions,
	}
}

// Schema implements the Node interface.
func (p *Project) Schema() sql.Schema {
	var s sql.Schema
	for _, e := range p.Expressions {
		f := &sql.Column{
			Name:     e.Name(),
			Type:     e.Type(),
			Nullable: e.IsNullable(),
		}
		s = append(s, f)
	}
	return s
}

// Resolved implements the Resolvable interface.
func (p *Project) Resolved() bool {
	return p.UnaryNode.Child.Resolved() &&
		expressionsResolved(p.Expressions...)
}

// RowIter implements the Node interface.
func (p *Project) RowIter() (sql.RowIter, error) {
	i, err := p.Child.RowIter()
	if err != nil {
		return nil, err
	}
	return &iter{p, i}, nil
}

// TransformUp implements the Transformable interface.
func (p *Project) TransformUp(f func(sql.Node) sql.Node) sql.Node {
	return f(NewProject(p.Expressions, p.Child.TransformUp(f)))
}

// TransformExpressionsUp implements the Transformable interface.
func (p *Project) TransformExpressionsUp(f func(sql.Expression) sql.Expression) sql.Node {
	return NewProject(
		transformExpressionsUp(f, p.Expressions),
		p.Child.TransformExpressionsUp(f),
	)
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
	return filterRow(i.p.Expressions, childRow)
}

func (i *iter) Close() error {
	return i.childIter.Close()
}

func filterRow(expressions []sql.Expression, row sql.Row) (sql.Row, error) {
	var fields []interface{}
	for _, expr := range expressions {
		f, err := expr.Eval(row)
		if err != nil {
			return nil, err
		}
		fields = append(fields, f)
	}
	return sql.NewRow(fields...), nil
}
