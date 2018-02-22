package expression

import "gopkg.in/src-d/go-mysql-server.v0/sql"

// Alias is a node that gives a name to an expression.
type Alias struct {
	UnaryExpression
	name string
}

// NewAlias returns a new Alias node.
func NewAlias(child sql.Expression, name string) *Alias {
	return &Alias{UnaryExpression{child}, name}
}

// Type returns the type of the expression.
func (e *Alias) Type() sql.Type {
	return e.Child.Type()
}

// Eval implements the Expression interface.
func (e *Alias) Eval(row sql.Row) (interface{}, error) {
	return e.Child.Eval(row)
}

// Name implements the Expression interface.
func (e *Alias) Name() string {
	return e.name
}

// TransformUp implements the Expression interface.
func (e *Alias) TransformUp(f func(sql.Expression) sql.Expression) sql.Expression {
	c := e.Child.TransformUp(f)
	n := NewAlias(c, e.name)

	return f(n)
}
