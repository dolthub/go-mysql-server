package function

import (
	"fmt"

	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
)

// NullIf function compares two expressions and returns NULL if they are equal. Otherwise, the first expression is returned.
type NullIf struct {
	expression.BinaryExpression
}

// NewNullIf returns a new NULLIF UDF
func NewNullIf(ex1, ex2 sql.Expression) sql.Expression {
	return &NullIf{
		expression.BinaryExpression{
			Left:  ex1,
			Right: ex2,
		},
	}
}

// Eval implements the Expression interface.
func (f *NullIf) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	if sql.IsNull(f.Left) && sql.IsNull(f.Right) {
		return sql.Null, nil
	}

	val, err := expression.NewEquals(f.Left, f.Right).Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	if b, ok := val.(bool); ok && b {
		return sql.Null, nil
	}

	return f.Left.Eval(ctx, row)
}

// Type implements the Expression interface.
func (f *NullIf) Type() sql.Type {
	if sql.IsNull(f.Left) {
		return sql.Null
	}

	return f.Left.Type()
}

// IsNullable implements the Expression interface.
func (f *NullIf) IsNullable() bool {
	return true
}

func (f *NullIf) String() string {
	return fmt.Sprintf("nullif(%s, %s)", f.Left, f.Right)
}

// TransformUp implements the Expression interface.
func (f *NullIf) TransformUp(fn sql.TransformExprFunc) (sql.Expression, error) {
	left, err := f.Left.TransformUp(fn)
	if err != nil {
		return nil, err
	}

	right, err := f.Right.TransformUp(fn)
	if err != nil {
		return nil, err
	}

	return fn(NewNullIf(left, right))
}
