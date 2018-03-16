package plan

import (
	"strings"

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
	var s = make(sql.Schema, len(p.Expressions))
	for i, e := range p.Expressions {
		var name string
		if n, ok := e.(sql.Nameable); ok {
			name = n.Name()
		} else {
			name = e.String()
		}
		s[i] = &sql.Column{
			Name:     name,
			Type:     e.Type(),
			Nullable: e.IsNullable(),
		}
	}
	return s
}

// Resolved implements the Resolvable interface.
func (p *Project) Resolved() bool {
	return p.UnaryNode.Child.Resolved() &&
		expressionsResolved(p.Expressions...)
}

// RowIter implements the Node interface.
func (p *Project) RowIter(session sql.Session) (sql.RowIter, error) {
	i, err := p.Child.RowIter(session)
	if err != nil {
		return nil, err
	}
	return &iter{p, i, session}, nil
}

// TransformUp implements the Transformable interface.
func (p *Project) TransformUp(f func(sql.Node) (sql.Node, error)) (sql.Node, error) {
	child, err := p.Child.TransformUp(f)
	if err != nil {
		return nil, err
	}
	return f(NewProject(p.Expressions, child))
}

// TransformExpressionsUp implements the Transformable interface.
func (p *Project) TransformExpressionsUp(f func(sql.Expression) (sql.Expression, error)) (sql.Node, error) {
	exprs, err := transformExpressionsUp(f, p.Expressions)
	if err != nil {
		return nil, err
	}

	child, err := p.Child.TransformExpressionsUp(f)
	if err != nil {
		return nil, err
	}

	return NewProject(exprs, child), nil
}

func (p Project) String() string {
	pr := sql.NewTreePrinter()
	var exprs = make([]string, len(p.Expressions))
	for i, expr := range p.Expressions {
		exprs[i] = expr.String()
	}
	_ = pr.WriteNode("Project(%s)", strings.Join(exprs, ", "))
	_ = pr.WriteChildren(p.Child.String())
	return pr.String()
}

type iter struct {
	p         *Project
	childIter sql.RowIter
	session   sql.Session
}

func (i *iter) Next() (sql.Row, error) {
	childRow, err := i.childIter.Next()
	if err != nil {
		return nil, err
	}
	return filterRow(i.session, i.p.Expressions, childRow)
}

func (i *iter) Close() error {
	return i.childIter.Close()
}

func filterRow(
	s sql.Session,
	expressions []sql.Expression,
	row sql.Row,
) (sql.Row, error) {
	var fields []interface{}
	for _, expr := range expressions {
		f, err := expr.Eval(s, row)
		if err != nil {
			return nil, err
		}
		fields = append(fields, f)
	}
	return sql.NewRow(fields...), nil
}
