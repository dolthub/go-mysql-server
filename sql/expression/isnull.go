package expression

import "gopkg.in/src-d/go-mysql-server.v0/sql"

// IsNull is an expression that checks if an expression is null.
type IsNull struct {
	UnaryExpression
}

// NewIsNull creates a new IsNull expression.
func NewIsNull(child sql.Expression) *IsNull {
	return &IsNull{UnaryExpression{child}}
}

// Type implements the Expression interface.
func (e *IsNull) Type() sql.Type {
	return sql.Boolean
}

// IsNullable implements the Expression interface.
func (e *IsNull) IsNullable() bool {
	return false
}

// Eval implements the Expression interface.
func (e *IsNull) Eval(row sql.Row) interface{} {
	return e.Child.Eval(row) == nil
}

// Name implements the Expression interface.
func (e *IsNull) Name() string {
	return "IsNull(" + e.Child.Name() + ")"
}

// TransformUp implements the Expression interface.
func (e *IsNull) TransformUp(f func(sql.Expression) sql.Expression) sql.Expression {
	c := e.UnaryExpression.Child.TransformUp(f)
	n := &IsNull{UnaryExpression{c}}

	return f(n)
}
