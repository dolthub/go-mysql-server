package expression

import (
	"fmt"

	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

// And checks whether two expressions are true.
type And struct {
	BinaryExpression
}

// NewAnd creates a new And expression.
func NewAnd(left, right sql.Expression) sql.Expression {
	return &And{BinaryExpression{Left: left, Right: right}}
}

// JoinAnd joins several expressions with And.
func JoinAnd(exprs ...sql.Expression) sql.Expression {
	switch len(exprs) {
	case 0:
		return nil
	case 1:
		return exprs[0]
	default:
		result := NewAnd(exprs[0], exprs[1])
		for _, e := range exprs[2:] {
			result = NewAnd(result, e)
		}
		return result
	}
}

func (a *And) String() string {
	return fmt.Sprintf("%s AND %s", a.Left, a.Right)
}

// Type implements the Expression interface.
func (*And) Type() sql.Type {
	return sql.Boolean
}

// Eval implements the Expression interface.
func (a *And) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	lval, err := a.Left.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if lval == false {
		return false, nil
	}

	rval, err := a.Right.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if rval == false {
		return false, nil
	}

	if lval == nil || rval == nil {
		return nil, nil
	}

	return true, nil
}

// TransformUp implements the Expression interface.
func (a *And) TransformUp(f sql.TransformExprFunc) (sql.Expression, error) {
	left, err := a.Left.TransformUp(f)
	if err != nil {
		return nil, err
	}

	right, err := a.Right.TransformUp(f)
	if err != nil {
		return nil, err
	}

	return f(NewAnd(left, right))
}

// Or checks whether one of the two given expressions is true.
type Or struct {
	BinaryExpression
}

// NewOr creates a new Or expression.
func NewOr(left, right sql.Expression) sql.Expression {
	return &Or{BinaryExpression{Left: left, Right: right}}
}

func (o *Or) String() string {
	return fmt.Sprintf("%s OR %s", o.Left, o.Right)
}

// Type implements the Expression interface.
func (*Or) Type() sql.Type {
	return sql.Boolean
}

// Eval implements the Expression interface.
func (o *Or) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	lval, err := o.Left.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if lval == true {
		return true, nil
	}

	rval, err := o.Right.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if lval == nil && rval == nil {
		return nil, nil
	}

	return rval == true, nil
}

// TransformUp implements the Expression interface.
func (o *Or) TransformUp(f sql.TransformExprFunc) (sql.Expression, error) {
	left, err := o.Left.TransformUp(f)
	if err != nil {
		return nil, err
	}

	right, err := o.Right.TransformUp(f)
	if err != nil {
		return nil, err
	}

	return f(NewOr(left, right))
}
