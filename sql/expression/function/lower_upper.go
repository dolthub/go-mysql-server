package function

import (
	"fmt"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
	"strings"
)

// Lower is a function that returns the lowercase of the text provided.
type Lower struct {
	expression.UnaryExpression
}

// NewLower creates a new Lower expression.
func NewLower(e sql.Expression) sql.Expression {
	return &Lower{expression.UnaryExpression{Child: e}}
}

// Eval implements the Expression interface.
func (l *Lower) Eval(
	ctx *sql.Context,
	row sql.Row,
) (interface{}, error) {
	v, err := l.Child.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if v == nil {
		return nil, nil
	}

	v, err = sql.Text.Convert(v)
	if err != nil {
		return nil, err
	}

	return strings.ToLower(v.(string)), nil
}

func (l *Lower) String() string {
	return fmt.Sprintf("LOWER(%s)", l.Child)
}

// TransformUp implements the Expression interface.
func (l *Lower) TransformUp(f sql.TransformExprFunc) (sql.Expression, error) {
	child, err := l.Child.TransformUp(f)
	if err != nil {
		return nil, err
	}
	return f(NewLower(child))
}

// Type implements the Expression interface.
func (l *Lower) Type() sql.Type {
	return l.Child.Type()
}

// Upper is a function that returns the UPPERCASE of the text provided.
type Upper struct {
	expression.UnaryExpression
}

// NewUpper creates a new Lower expression.
func NewUpper(e sql.Expression) sql.Expression {
	return &Upper{expression.UnaryExpression{Child: e}}
}

// Eval implements the Expression interface.
func (u *Upper) Eval(
	ctx *sql.Context,
	row sql.Row,
) (interface{}, error) {
	v, err := u.Child.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if v == nil {
		return nil, nil
	}

	v, err = sql.Text.Convert(v)
	if err != nil {
		return nil, err
	}

	return strings.ToUpper(v.(string)), nil
}

func (u *Upper) String() string {
	return fmt.Sprintf("UPPER(%s)", u.Child)
}

// TransformUp implements the Expression interface.
func (u *Upper) TransformUp(f sql.TransformExprFunc) (sql.Expression, error) {
	child, err := u.Child.TransformUp(f)
	if err != nil {
		return nil, err
	}
	return f(NewUpper(child))
}

// Type implements the Expression interface.
func (u *Upper) Type() sql.Type {
	return u.Child.Type()
}
