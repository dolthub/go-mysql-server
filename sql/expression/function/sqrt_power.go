package function

import (
	"fmt"
	"math"

	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

// Sqrt is a function that returns the square value of the number provided.
type Sqrt struct {
	expression.UnaryExpression
}

// NewSqrt creates a new Sqrt expression.
func NewSqrt(e sql.Expression) sql.Expression {
	return &Sqrt{expression.UnaryExpression{Child: e}}
}

func (s *Sqrt) String() string {
	return fmt.Sprintf("sqrt(%s)", s.Child.String())
}

// Type implements the Expression interface.
func (s *Sqrt) Type() sql.Type {
	return sql.Float64
}

// IsNullable implements the Expression interface.
func (s *Sqrt) IsNullable() bool {
	return s.Child.IsNullable()
}

// TransformUp implements the Expression interface.
func (s *Sqrt) TransformUp(fn sql.TransformExprFunc) (sql.Expression, error) {
	child, err := s.Child.TransformUp(fn)
	if err != nil {
		return nil, err
	}
	return fn(NewSqrt(child))
}

// Eval implements the Expression interface.
func (s *Sqrt) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	child, err := s.Child.Eval(ctx, row)

	if err != nil {
		return nil, err
	}

	if child == nil {
		return nil, nil
	}

	child, err = sql.Float64.Convert(child)
	if err != nil {
		return nil, nil
	}

	return computeSqrt(child.(float64))
}

func computeSqrt(x float64) (float64, error) {
	return math.Sqrt(x), nil
}

type powerFuncName int
const (
	funcNamePow   powerFuncName = iota
	funcNamePower
)

// Power is a function that returns value of X raised to the power of Y.
type Power struct {
	expression.BinaryExpression
	powerFuncName
}

// NewPowerFunc returns a NewPower creator function with a specific powerFuncName.
func NewPowerFunc(fName powerFuncName) func(e1, e2 sql.Expression) sql.Expression {
	return func(e1, e2 sql.Expression) sql.Expression {
		return NewPower(fName, e1, e2)
	}
}

// NewPad creates a new Power expression.
func NewPower(fName powerFuncName, e1, e2 sql.Expression) sql.Expression {
	return &Power{
		expression.BinaryExpression{
			Left:  e1,
			Right: e2,
		},
		fName,
	}
}

// Type implements the Expression interface.
func (p *Power) Type() sql.Type { return sql.Float64 }

// IsNullable implements the Expression interface.
func (p *Power) IsNullable() bool { return p.Left.IsNullable() || p.Right.IsNullable() }

func (p *Power) String() string {
	if p.powerFuncName == funcNamePow {
		return fmt.Sprintf("pow(%s, %s)", p.Left, p.Right)
	}

	return fmt.Sprintf("power(%s, %s)", p.Left, p.Right)
}

// TransformUp implements the Expression interface.
func (p *Power) TransformUp(fn sql.TransformExprFunc) (sql.Expression, error) {
	left, err := p.Left.TransformUp(fn)
	if err != nil {
		return nil, err
	}

	right, err := p.Right.TransformUp(fn)
	if err != nil {
		return nil, err
	}

	return fn(NewPower(p.powerFuncName, left, right))
}

// Eval implements the Expression interface.
func (p *Power) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	left, err := p.Left.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if left == nil {
		return nil, nil
	}

	left, err = sql.Float64.Convert(left)
	if err != nil {
		return nil, err
	}

	right, err := p.Right.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if right == nil {
		return nil, nil
	}

	right, err = sql.Float64.Convert(right)
	if err != nil {
		return nil, err
	}

	return computePower(left.(float64), right.(float64))
}

func computePower(a, b float64) (float64, error) {
	return math.Pow(a, b), nil
}
