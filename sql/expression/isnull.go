package expression

import "gopkg.in/sqle/sqle.v0/sql"

type IsNull struct {
	UnaryExpression
}

func NewIsNull(child sql.Expression) *IsNull {
	return &IsNull{UnaryExpression{child}}
}

func (e *IsNull) Type() sql.Type {
	return sql.Boolean
}

func (e *IsNull) IsNullable() bool {
	return false
}

func (e *IsNull) Eval(row sql.Row) interface{} {
	return e.Child.Eval(row) == nil
}

func (e *IsNull) Name() string {
	return "IsNull(" + e.Child.Name() + ")"
}

func (e *IsNull) TransformUp(f func(sql.Expression) sql.Expression) sql.Expression {
	c := e.UnaryExpression.Child.TransformUp(f)
	n := &IsNull{UnaryExpression{c}}

	return f(n)
}
