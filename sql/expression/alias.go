package expression

import "gopkg.in/sqle/sqle.v0/sql"

type Alias struct {
	UnaryExpression
	name string
}

func NewAlias(child sql.Expression, name string) *Alias {
	return &Alias{UnaryExpression{child}, name}
}

func (e *Alias) Type() sql.Type {
	return e.Child.Type()
}

func (e *Alias) Eval(row sql.Row) interface{} {
	return e.Child.Eval(row)
}

func (e *Alias) Name() string {
	return e.name
}

func (e *Alias) TransformUp(f func(sql.Expression) sql.Expression) sql.Expression {
	c := e.Child.TransformUp(f)
	n := NewAlias(c, e.name)

	return f(n)
}
