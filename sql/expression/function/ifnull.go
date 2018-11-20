package function

import (
	"fmt"

	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
)

// IfNull function returns the specified value IF the expression is NULL, otherwise return the expression.
type IfNull struct {
	expression.BinaryExpression
}

// NewIfNull returns a new IFNULL UDF
func NewIfNull(ex, value sql.Expression) sql.Expression {
	return &IfNull{
		expression.BinaryExpression{
			Left:  ex,
			Right: value,
		},
	}
}

// Eval implements the Expression interface.
func (f *IfNull) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	left, err := f.Left.Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	if left != nil {
		return left, nil
	}

	right, err := f.Right.Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	return right, nil
}

// Type implements the Expression interface.
func (f *IfNull) Type() sql.Type {
	if sql.IsNull(f.Left) {
		if sql.IsNull(f.Right) {
			return sql.Null
		}
		return f.Right.Type()
	}
	return f.Left.Type()
}

// IsNullable implements the Expression interface.
func (f *IfNull) IsNullable() bool {
	if sql.IsNull(f.Left) {
		if sql.IsNull(f.Right) {
			return true
		}
		return f.Right.IsNullable()
	}
	return f.Left.IsNullable()
}

func (f *IfNull) String() string {
	return fmt.Sprintf("ifnull(%s, %s)", f.Left, f.Right)
}

// TransformUp implements the Expression interface.
func (f *IfNull) TransformUp(fn sql.TransformExprFunc) (sql.Expression, error) {
	left, err := f.Left.TransformUp(fn)
	if err != nil {
		return nil, err
	}

	right, err := f.Right.TransformUp(fn)
	if err != nil {
		return nil, err
	}

	return fn(NewIfNull(left, right))
}
