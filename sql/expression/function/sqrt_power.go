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
		return nil, err
	}

	return math.Sqrt(child.(float64)), nil
}

// Power is a function that returns value of X raised to the power of Y.
type Power struct {
	expression.BinaryExpression
}

// NewPower creates a new Power expression.
func NewPower(e1, e2 sql.Expression) sql.Expression {
	return &Power{
		expression.BinaryExpression{
			Left:  e1,
			Right: e2,
		},
	}
}

// Type implements the Expression interface.
func (p *Power) Type() sql.Type { return sql.Float64 }

// IsNullable implements the Expression interface.
func (p *Power) IsNullable() bool { return p.Left.IsNullable() || p.Right.IsNullable() }

func (p *Power) String() string {
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

	return fn(NewPower(left, right))
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

	return math.Pow(left.(float64), right.(float64)), nil
}
