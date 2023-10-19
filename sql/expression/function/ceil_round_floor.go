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
	"encoding/hex"
	"fmt"
	"math"
	"strconv"

	"github.com/shopspring/decimal"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// Ceil returns the smallest integer value not less than X.
type Ceil struct {
	expression.UnaryExpression
}

var _ sql.FunctionExpression = (*Ceil)(nil)
var _ sql.CollationCoercible = (*Ceil)(nil)

// NewCeil creates a new Ceil expression.
func NewCeil(num sql.Expression) sql.Expression {
	return &Ceil{expression.UnaryExpression{Child: num}}
}

// FunctionName implements sql.FunctionExpression
func (c *Ceil) FunctionName() string {
	return "ceil"
}

// Description implements sql.FunctionExpression
func (c *Ceil) Description() string {
	return "returns the smallest integer value that is greater than or equal to number."
}

// Type implements the Expression interface.
func (c *Ceil) Type() sql.Type {
	childType := c.Child.Type()
	if types.IsInteger(childType) {
		return childType
	}
	return types.Int32
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*Ceil) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 5
}

func (c *Ceil) String() string {
	return fmt.Sprintf("%s(%s)", c.FunctionName(), c.Child)
}

// WithChildren implements the Expression interface.
func (c *Ceil) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(c, len(children), 1)
	}
	return NewCeil(children[0]), nil
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

	// non number type will be caught here
	if !types.IsNumber(c.Child.Type()) {
		child, _, err = types.Float64.Convert(child)
		if err != nil {
			return int32(0), nil
		}

		return int32(math.Ceil(child.(float64))), nil
	}

	// if it's number type and not float value, it does not need ceil-ing
	switch num := child.(type) {
	case float64:
		return math.Ceil(num), nil
	case float32:
		return float32(math.Ceil(float64(num))), nil
	case decimal.Decimal:
		return num.Ceil(), nil
	default:
		return child, nil
	}
}

// Floor returns the biggest integer value not less than X.
type Floor struct {
	expression.UnaryExpression
}

var _ sql.FunctionExpression = (*Floor)(nil)
var _ sql.CollationCoercible = (*Floor)(nil)

// NewFloor returns a new Floor expression.
func NewFloor(num sql.Expression) sql.Expression {
	return &Floor{expression.UnaryExpression{Child: num}}
}

// FunctionName implements sql.FunctionExpression
func (f *Floor) FunctionName() string {
	return "floor"
}

// Description implements sql.FunctionExpression
func (f *Floor) Description() string {
	return "returns the largest integer value that is less than or equal to number."
}

// Type implements the Expression interface.
func (f *Floor) Type() sql.Type {
	childType := f.Child.Type()
	if types.IsInteger(childType) {
		return childType
	}
	return types.Int32
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*Floor) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 5
}

func (f *Floor) String() string {
	return fmt.Sprintf("%s(%s)", f.FunctionName(), f.Child)
}

// WithChildren implements the Expression interface.
func (f *Floor) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(f, len(children), 1)
	}
	return NewFloor(children[0]), nil
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

	// non number type will be caught here
	if !types.IsNumber(f.Child.Type()) {
		child, _, err = types.Float64.Convert(child)
		if err != nil {
			return int32(0), nil
		}

		return int32(math.Floor(child.(float64))), nil
	}

	// if it's number type and not float value, it does not need floor-ing
	switch num := child.(type) {
	case float64:
		return math.Floor(num), nil
	case float32:
		return float32(math.Floor(float64(num))), nil
	case decimal.Decimal:
		return num.Floor(), nil
	default:
		return child, nil
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
var _ sql.CollationCoercible = (*Round)(nil)

// NewRound returns a new Round expression.
func NewRound(args ...sql.Expression) (sql.Expression, error) {
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

// Description implements sql.FunctionExpression
func (r *Round) Description() string {
	return "rounds the number to decimals decimal places."
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
			case []byte:
				val, err := strconv.ParseUint(hex.EncodeToString(dNum), 16, 64)
				if err != nil {
					return nil, err
				}
				dVal = float64(val)
			default:
				dTemp, _, err = types.Float64.Convert(dTemp)
				if err == nil {
					dVal = dTemp.(float64)
				}
			}
			if dVal > 30 { // MySQL cuts off at 30 for larger values
				dVal = 30
			}
		}
	}

	if types.IsText(r.Left.Type()) {
		xVal, _, err = types.Float64.Convert(xVal)
		if err != nil {
			return int32(0), nil
		}
	} else if !types.IsNumber(r.Left.Type()) {
		xVal, _, err = types.Float64.Convert(xVal)
		if err != nil {
			return int32(0), nil
		}

		xNum := xVal.(float64)
		return int32(math.Round(xNum*math.Pow(10.0, dVal)) / math.Pow(10.0, dVal)), nil
	}

	// One way to round to a decimal place is to shift the number up by the desired decimal position, round to the
	// nearest integer, and then shift back down.
	// For example, we have 5.855 and want to round to 2 decimal places.
	// In this case, xNum = 5.855 and dVal = 2
	// round(xNum * 10^dVal) / 10^dVal
	// round(5.855 * 10^2) / 10^2
	// round(5.855 * 100) / 100
	// round(585.5) / 100
	// 586 / 100
	// 5.86
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
	case decimal.Decimal:
		return xNum.Round(int32(dVal)), nil
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
		return fmt.Sprintf("%s(%s,0)", r.FunctionName(), r.Left.String())
	}

	return fmt.Sprintf("%s(%s,%s)", r.FunctionName(), r.Left.String(), r.Right.String())
}

// Resolved implements the Expression interface.
func (r *Round) Resolved() bool {
	return r.Left.Resolved() && (r.Right == nil || r.Right.Resolved())
}

// Type implements the Expression interface.
func (r *Round) Type() sql.Type {
	leftChildType := r.Left.Type()
	if types.IsNumber(leftChildType) {
		return leftChildType
	}
	return types.Int32
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*Round) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 5
}

// WithChildren implements the Expression interface.
func (r *Round) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	return NewRound(children...)
}
