package function

import (
	"fmt"
	"reflect"
	"strings"
	"unicode"

	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

type trimType rune
const (
	lTrimType trimType = 'l'
	rTrimType trimType = 'r'
	bTrimType trimType = 'b'
)

// NewTrimFunc returns a Trim creator function with a specific trimType.
func NewTrimFunc(tType trimType) func(e sql.Expression) sql.Expression {
	return func(e sql.Expression) sql.Expression {
		return NewTrim(tType, e)
	}
}

// NewTrim creates a new Trim expression.
func NewTrim(tType trimType, str sql.Expression) sql.Expression {
	return &Trim{expression.UnaryExpression{Child: str}, tType}
}

// Trim is a function that returns the string with prefix or suffix spaces removed based on the trimType
type Trim struct {
	expression.UnaryExpression
	trimType
}

// Type implements the Expression interface.
func (t *Trim) Type() sql.Type { return sql.Text }

func (t *Trim) String() string {
	switch t.trimType {
	case lTrimType:
		return fmt.Sprintf("ltrim(%s)", t.Child)
	case rTrimType:
		return fmt.Sprintf("rtrim(%s)", t.Child)
	default:
		return fmt.Sprintf("trim(%s)", t.Child)
	}
}

// IsNullable implements the Expression interface.
func (t *Trim) IsNullable() bool {
	return t.Child.IsNullable()
}

// TransformUp implements the Expression interface.
func (t *Trim) TransformUp(f sql.TransformExprFunc) (sql.Expression, error) {
	str, err := t.Child.TransformUp(f)
	if err != nil {
		return nil, err
	}

	return f(NewTrim(t.trimType, str))
}

// Eval implements the Expression interface.
func (t *Trim) Eval(
	ctx *sql.Context,
	row sql.Row,
) (interface{}, error) {
	str, err := t.Child.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if str == nil {
		return nil, nil
	}

	str, err = sql.Text.Convert(str)
	if err != nil {
		return nil, sql.ErrInvalidType.New(reflect.TypeOf(str))
	}

	switch t.trimType {
	case lTrimType:
		return strings.TrimLeftFunc(str.(string), unicode.IsSpace), nil
	case rTrimType:
		return strings.TrimRightFunc(str.(string), unicode.IsSpace), nil
	default:
		return strings.TrimFunc(str.(string), unicode.IsSpace), nil
	}
}
