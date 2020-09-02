package function

import (
	"fmt"
	"strings"

	"github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/liquidata-inc/go-mysql-server/sql/expression"
)

// Lower is a function that returns the lowercase of the text provided.
type Lower struct {
	expression.UnaryExpression
}

var _ sql.FunctionExpression = (*Lower)(nil)

// NewLower creates a new Lower expression.
func NewLower(e sql.Expression) sql.Expression {
	return &Lower{expression.UnaryExpression{Child: e}}
}

// FunctionName implements sql.FunctionExpression
func (l *Lower) FunctionName() string {
	return "lower"
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

	v, err = sql.LongText.Convert(v)
	if err != nil {
		return nil, err
	}

	return strings.ToLower(v.(string)), nil
}

func (l *Lower) String() string {
	return fmt.Sprintf("LOWER(%s)", l.Child)
}

// WithChildren implements the Expression interface.
func (l *Lower) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(l, len(children), 1)
	}
	return NewLower(children[0]), nil
}

// Type implements the Expression interface.
func (l *Lower) Type() sql.Type {
	return l.Child.Type()
}

// Upper is a function that returns the UPPERCASE of the text provided.
type Upper struct {
	expression.UnaryExpression
}

var _ sql.FunctionExpression = (*Upper)(nil)

// FunctionName implements sql.FunctionExpression
func (u *Upper) FunctionName() string {
	return "upper"
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

	v, err = sql.LongText.Convert(v)
	if err != nil {
		return nil, err
	}

	return strings.ToUpper(v.(string)), nil
}

func (u *Upper) String() string {
	return fmt.Sprintf("UPPER(%s)", u.Child)
}

// WithChildren implements the Expression interface.
func (u *Upper) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(u, len(children), 1)
	}
	return NewUpper(children[0]), nil
}

// Type implements the Expression interface.
func (u *Upper) Type() sql.Type {
	return u.Child.Type()
}
