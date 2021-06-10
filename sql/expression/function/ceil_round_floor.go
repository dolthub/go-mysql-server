// Copyright 2020-2021 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package function

import (
	"fmt"
	"math"
	"reflect"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

// Ceil returns the smallest integer value not less than X.
type Ceil struct {
	expression.UnaryExpression
}

var _ sql.FunctionExpression = (*Ceil)(nil)

// NewCeil creates a new Ceil expression.
func NewCeil(ctx *sql.Context, num sql.Expression) sql.Expression {
	return &Ceil{expression.UnaryExpression{Child: num}}
}

// FunctionName implements sql.FunctionExpression
func (c *Ceil) FunctionName() string {
	return "ceil"
}

// Type implements the Expression interface.
func (c *Ceil) Type() sql.Type {
	childType := c.Child.Type()
	if sql.IsNumber(childType) {
		return childType
	}
	return sql.Int32
}

func (c *Ceil) String() string {
	return fmt.Sprintf("CEIL(%s)", c.Child)
}

// WithChildren implements the Expression interface.
func (c *Ceil) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(c, len(children), 1)
	}
	return NewCeil(ctx, children[0]), nil
}

// Eval implements the Expression interface.
func (c *Ceil) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	child, err := c.Child.Eval(ctx, row)

	if err != nil {
		return nil, err
	}

	if child == nil {
		return nil, nil
	}

	if !sql.IsNumber(c.Child.Type()) {
		child, err = sql.Float64.Convert(child)
		if err != nil {
			return int32(0), nil
		}

		return int32(math.Ceil(child.(float64))), nil
	}

	if !sql.IsFloat(c.Child.Type()) {
		return child, err
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

var _ sql.FunctionExpression = (*Floor)(nil)

// NewFloor returns a new Floor expression.
func NewFloor(ctx *sql.Context, num sql.Expression) sql.Expression {
	return &Floor{expression.UnaryExpression{Child: num}}
}

// FunctionName implements sql.FunctionExpression
func (f *Floor) FunctionName() string {
	return "floor"
}

// Type implements the Expression interface.
func (f *Floor) Type() sql.Type {
	childType := f.Child.Type()
	if sql.IsNumber(childType) {
		return childType
	}
	return sql.Int32
}

func (f *Floor) String() string {
	return fmt.Sprintf("FLOOR(%s)", f.Child)
}

// WithChildren implements the Expression interface.
func (f *Floor) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(f, len(children), 1)
	}
	return NewFloor(ctx, children[0]), nil
}

// Eval implements the Expression interface.
func (f *Floor) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	child, err := f.Child.Eval(ctx, row)

	if err != nil {
		return nil, err
	}

	if child == nil {
		return nil, nil
	}

	if !sql.IsNumber(f.Child.Type()) {
		child, err = sql.Float64.Convert(child)
		if err != nil {
			return int32(0), nil
		}

		return int32(math.Floor(child.(float64))), nil
	}

	if !sql.IsFloat(f.Child.Type()) {
		return child, err
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

// Round returns the number (x) with (d) requested decimal places.
// If d is negative, the number is returned with the (abs(d)) least significant
// digits of it's integer part set to 0. If d is not specified or nil/null
// it defaults to 0.
type Round struct {
	expression.BinaryExpression
}

var _ sql.FunctionExpression = (*Round)(nil)

// NewRound returns a new Round expression.
func NewRound(ctx *sql.Context, args ...sql.Expression) (sql.Expression, error) {
	argLen := len(args)
	if argLen == 0 || argLen > 2 {
		return nil, sql.ErrInvalidArgumentNumber.New("ROUND", "1 or 2", argLen)
	}

	var right sql.Expression
	if len(args) == 2 {
		right = args[1]
	}

	return &Round{expression.BinaryExpression{Left: args[0], Right: right}}, nil
}

// FunctionName implements sql.FunctionExpression
func (r *Round) FunctionName() string {
	return "round"
}

// Children implements the Expression interface.
func (r *Round) Children() []sql.Expression {
	if r.Right == nil {
		return []sql.Expression{r.Left}
	}

	return r.BinaryExpression.Children()
}

// Eval implements the Expression interface.
func (r *Round) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	xVal, err := r.Left.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if xVal == nil {
		return nil, nil
	}

	dVal := float64(0)

	if r.Right != nil {
		var dTemp interface{}
		dTemp, err = r.Right.Eval(ctx, row)
		if err != nil {
			return nil, err
		}

		if dTemp != nil {
			switch dNum := dTemp.(type) {
			case float64:
				dVal = float64(int64(dNum))
			case float32:
				dVal = float64(int64(dNum))
			case int64:
				dVal = float64(dNum)
			case int32:
				dVal = float64(dNum)
			case int16:
				dVal = float64(dNum)
			case int8:
				dVal = float64(dNum)
			case uint64:
				dVal = float64(dNum)
			case uint32:
				dVal = float64(dNum)
			case uint16:
				dVal = float64(dNum)
			case uint8:
				dVal = float64(dNum)
			case int:
				dVal = float64(dNum)
			default:
				dTemp, err = sql.Float64.Convert(dTemp)
				if err == nil {
					dVal = dTemp.(float64)
				}
			}
		}
	}

	if !sql.IsNumber(r.Left.Type()) {
		xVal, err = sql.Float64.Convert(xVal)
		if err != nil {
			return int32(0), nil
		}

		xNum := xVal.(float64)
		return int32(math.Round(xNum*math.Pow(10.0, dVal)) / math.Pow(10.0, dVal)), nil
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
	case int16:
		return int16(math.Round(float64(xNum)*math.Pow(10.0, dVal)) / math.Pow(10.0, dVal)), nil
	case int8:
		return int8(math.Round(float64(xNum)*math.Pow(10.0, dVal)) / math.Pow(10.0, dVal)), nil
	case uint64:
		return uint64(math.Round(float64(xNum)*math.Pow(10.0, dVal)) / math.Pow(10.0, dVal)), nil
	case uint32:
		return uint32(math.Round(float64(xNum)*math.Pow(10.0, dVal)) / math.Pow(10.0, dVal)), nil
	case uint16:
		return uint16(math.Round(float64(xNum)*math.Pow(10.0, dVal)) / math.Pow(10.0, dVal)), nil
	case uint8:
		return uint8(math.Round(float64(xNum)*math.Pow(10.0, dVal)) / math.Pow(10.0, dVal)), nil
	case int:
		return int(math.Round(float64(xNum)*math.Pow(10.0, dVal)) / math.Pow(10.0, dVal)), nil
	default:
		return nil, sql.ErrInvalidType.New(r.Left.Type().String())
	}
}

// IsNullable implements the Expression interface.
func (r *Round) IsNullable() bool {
	return r.Left.IsNullable()
}

func (r *Round) String() string {
	if r.Right == nil {
		return fmt.Sprintf("ROUND(%s, 0)", r.Left.String())
	}

	return fmt.Sprintf("ROUND(%s, %s)", r.Left.String(), r.Right.String())
}

// Resolved implements the Expression interface.
func (r *Round) Resolved() bool {
	return r.Left.Resolved() && (r.Right == nil || r.Right.Resolved())
}

// Type implements the Expression interface.
func (r *Round) Type() sql.Type {
	leftChildType := r.Left.Type()
	if sql.IsNumber(leftChildType) {
		return leftChildType
	}
	return sql.Int32
}

// WithChildren implements the Expression interface.
func (r *Round) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	return NewRound(ctx, children...)
}
