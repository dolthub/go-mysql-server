package function

import (
	"fmt"
	"reflect"
	"strings"

	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

type padType int

const (
	leftPadType  = iota
	rightPadType
)

// MakePadder returns a Pad creator functions with a specific padType.
func MakePadder(pType padType) func(e ...sql.Expression) (sql.Expression, error) {
	return func(e ...sql.Expression) (sql.Expression, error) {
		return NewPad(pType, e...)
	}
}

// NewLogBase creates a new LogBase expression.
func NewPad(pType padType, args ...sql.Expression) (sql.Expression, error) {
	argLen := len(args)
	if argLen != 3 {
		return nil, sql.ErrInvalidArgumentNumber.New("3", argLen)
	}

	return &Pad{args[0], args[1], args[2], pType}, nil
}

// Pad is a function that pads a string with another string.
type Pad struct {
	str     sql.Expression
	len     sql.Expression
	padStr  sql.Expression
	padType padType
}

// Children implements the Expression interface.
func (p *Pad) Children() []sql.Expression {
	return []sql.Expression{p.str, p.len, p.padStr}
}

// Resolved implements the Expression interface.
func (p *Pad) Resolved() bool {
	return p.str.Resolved() && p.len.Resolved() && (p.padStr.Resolved())
}

// IsNullable implements the Expression interface.
func (p *Pad) IsNullable() bool {
	return p.str.IsNullable() || p.len.IsNullable() || p.padStr.IsNullable()
}

// Type implements the Expression interface.
func (p *Pad) Type() sql.Type { return sql.Text }

func (p *Pad) String() string {
	if p.padType == leftPadType {
		return fmt.Sprintf("lpad(%s, %s, %s)", p.str, p.len, p.padStr)
	}
	return fmt.Sprintf("rpad(%s, %s, %s)", p.str, p.len, p.padStr)
}

// TransformUp implements the Expression interface.
func (p *Pad) TransformUp(f sql.TransformExprFunc) (sql.Expression, error) {
	str, err := p.str.TransformUp(f)
	if err != nil {
		return nil, err
	}

	len, err := p.len.TransformUp(f)
	if err != nil {
		return nil, err
	}

	padStr, err := p.padStr.TransformUp(f)
	if err != nil {
		return nil, err
	}
	padded, _ := NewPad(p.padType, str, len, padStr)
	return f(padded)
}

// Eval implements the Expression interface.
func (p *Pad) Eval(
	ctx *sql.Context,
	row sql.Row,
) (interface{}, error) {
	str, err := p.str.Eval(ctx, row)
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

	length, err := p.len.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if length == nil {
		return nil, nil
	}

	length, err = sql.Int64.Convert(length)
	if err != nil {
		return nil, err
	}

	padStr, err := p.padStr.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if padStr == nil {
		return nil, nil
	}

	padStr, err = sql.Text.Convert(padStr)
	if err != nil {
		return nil, err
	}

	return padString(str.(string), length.(int64), padStr.(string), p.padType)
}

func padString(str string, length int64, padStr string, padType padType) (string, error) {
	if length <= 0 {
		return "", nil
	}
	if int64(len(str)) >= length {
		return str[:length], nil
	}
	if len(padStr) == 0 {
		return "", nil
	}

	padLen := int(length - int64(len(str)))
	quo, rem := divmod(int64(padLen), int64(len(padStr)))

	if padType == leftPadType {
		result := strings.Repeat(padStr, int(quo)) + padStr[:rem] + str
		return result[:length], nil
	} else {
		result := str + strings.Repeat(padStr, int(quo)) + padStr[:rem]
		return result[(int64(len(result)) - length):], nil
	}
}

func divmod(a, b int64) (quotient, remainder int64) {
	quotient = a / b
	remainder = a % b
	return
}
