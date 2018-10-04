package function

import (
	"fmt"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
	"math"
	"reflect"
)

// Ceil returns the smallest integer value not less than X.
type Ceil struct {
	expression.UnaryExpression
}

func (c *Ceil) Resolved() bool {
	return c.Child.Resolved()
}

func (c *Ceil) IsNullable() bool {
	return c.Child.IsNullable()
}

func (c *Ceil) Children() []sql.Expression {
	return c.Child.Children()
}

func NewCeil(array sql.Expression) sql.Expression {
	return &Ceil{expression.UnaryExpression{Child: array}}
}

func (c *Ceil) Type() sql.Type {
	return c.Child.Type()
}

func (c *Ceil) String() string {
	return fmt.Sprintf("CEIL(%s)", c.Child)
}

// TransformUp implements the Expression interface.
func (c *Ceil) TransformUp(fn sql.TransformExprFunc) (sql.Expression, error) {
	child, err := c.Child.TransformUp(fn)
	if err != nil {
		return nil, err
	}
	return fn(NewCeil(child))
}

// Eval implements the Expression interface.
func (c *Ceil) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	if !sql.IsNumber(c.Child.Type()) {
		return nil, sql.ErrInvalidType.New(c.Child.Type().Type().String())
	}

	child, err := c.Child.Eval(ctx, row)

	if !sql.IsDecimal(c.Child.Type()) {
		return child, err
	}

	if err != nil {
		return nil, err
	}

	if child == nil {
		return nil, nil
	}

	switch num := child.(type) {
	case float64:
		return math.Ceil(num), nil
	case float32:
		return float32(math.Ceil(float64(num))), nil
	default:
		return nil, sql.ErrInvalidType.New(reflect.TypeOf(num))
	}
}

// Floor returns the biggest integer value not less than X.
type Floor struct {
	expression.UnaryExpression
}

func (f *Floor) Resolved() bool {
	return f.Child.Resolved()
}

func (f *Floor) IsNullable() bool {
	return f.Child.IsNullable()
}

func (f *Floor) Children() []sql.Expression {
	return f.Child.Children()
}

func NewFloor(array sql.Expression) sql.Expression {
	return &Floor{expression.UnaryExpression{Child: array}}
}

func (f *Floor) Type() sql.Type {
	return f.Child.Type()
}

func (f *Floor) String() string {
	return fmt.Sprintf("FLOOR(%s)", f.Child)
}

// TransformUp implements the Expression interface.
func (f *Floor) TransformUp(fn sql.TransformExprFunc) (sql.Expression, error) {
	child, err := f.Child.TransformUp(fn)
	if err != nil {
		return nil, err
	}
	return fn(NewFloor(child))
}

// Eval implements the Expression interface.
func (f *Floor) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	if !sql.IsNumber(f.Child.Type()) {
		return nil, sql.ErrInvalidType.New(f.Child.Type().Type().String())
	}

	child, err := f.Child.Eval(ctx, row)

	if !sql.IsDecimal(f.Child.Type()) {
		return child, err
	}

	if err != nil {
		return nil, err
	}

	if child == nil {
		return nil, nil
	}

	switch num := child.(type) {
	case float64:
		return math.Floor(num), nil
	case float32:
		return float32(math.Floor(float64(num))), nil
	default:
		return nil, sql.ErrInvalidType.New(reflect.TypeOf(num))
	}
}

type Round struct {
	expression.BinaryExpression
}

func NewRound(x, d sql.Expression) sql.Expression {
	return &Round{expression.BinaryExpression{Left: x, Right: d}}
}

// Children implements the Expression interface.
func (r *Round) Children() []sql.Expression {
	if r.Right == nil {
		return []sql.Expression{r.Left}
	}

	return r.Children()
}

// Eval implements the Expression interface.
func (r *Round) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	if !sql.IsNumber(r.Left.Type()) {
		return nil, sql.ErrInvalidType.New(r.Left.Type().Type().String())
	}

	xVal, err := r.Left.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if xVal == nil {
		return nil, nil
	}

	dVal := float64(0)

	if r.Right != nil {
		dTemp, err := r.Right.Eval(ctx, row)
		if err != nil {
			return nil, err
		}

		if !sql.IsInteger(r.Right.Type()) {
			return nil, sql.ErrInvalidType.New(r.Right.Type().Type().String())
		}

		if dTemp != nil {
			switch dNum := dTemp.(type) {
			case int64:
				dVal = float64(dNum)
			case int32:
				dVal = float64(dNum)
			case int:
				dVal = float64(dNum)
			default:
				return nil, sql.ErrInvalidType.New(r.Right.Type().Type().String())
			}
		}
	}

	switch xNum := xVal.(type) {
	case float64:
		return math.Round(xNum*math.Pow(10.0, dVal)) / math.Pow(10.0, dVal), nil
	case float32:
		return float32(math.Round(float64(xNum)*math.Pow(10.0, dVal)) / math.Pow(10.0, dVal)), nil
	case int64:
		return int64(math.Round(float64(xNum)*math.Pow(10.0, dVal)) / math.Pow(10.0, dVal)), nil
	case int32:
		return int32(math.Round(float64(xNum)*math.Pow(10.0, dVal)) / math.Pow(10.0, dVal)), nil
	case int:
		return int(math.Round(float64(xNum)*math.Pow(10.0, dVal)) / math.Pow(10.0, dVal)), nil
	default:
		return nil, sql.ErrInvalidType.New(r.Right.Type().Type().String())
	}
}

// IsNullable implements the Expression interface.
func (r *Round) IsNullable() bool {
	return r.Left.IsNullable()
}

func (r *Round) String() string {
	if r.Right == nil {
		return fmt.Sprintf("ROUND(%s, 0)", r.Right.String())
	}

	return fmt.Sprintf("ROUND(%s, %s)", r.Right.String(), r.Left.String())
}

// Resolved implements the Expression interface.
func (r *Round) Resolved() bool {
	return r.Left.Resolved() && (r.Right == nil || r.Right.Resolved())
}

// Type implements the Expression interface.
func (r *Round) Type() sql.Type { return r.Left.Type() }

// TransformUp implements the Expression interface.
func (r *Round) TransformUp(f sql.TransformExprFunc) (sql.Expression, error) {
	l, err := r.Left.TransformUp(f)
	if err != nil {
		return nil, err
	}

	if r.Right != nil {
		ri, err := r.Right.TransformUp(f)
		if err != nil {
			return nil, err
		}
		return f(NewRound(l, ri))
	}

	return f(NewRound(l, nil))
}
