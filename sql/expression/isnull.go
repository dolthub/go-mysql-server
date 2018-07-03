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
func (e *IsNull) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	v, err := e.Child.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	return v == nil, nil
}

func (e IsNull) String() string {
	return e.Child.String() + " IS NULL"
}

// TransformUp implements the Expression interface.
func (e *IsNull) TransformUp(f sql.TransformExprFunc) (sql.Expression, error) {
	child, err := e.Child.TransformUp(f)
	if err != nil {
		return nil, err
	}
	return f(NewIsNull(child))
}
