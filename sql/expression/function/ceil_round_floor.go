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

	"github.com/cockroachdb/apd/v3"
	"github.com/dolthub/vitess/go/mysql"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// numericRetType returns the appropriate return type for numeric functions
// like ROUND() and TRUNCATE() according to MySQL specification:
// Integer types return BIGINT
// Floating-point types or non-numeric types return DOUBLE
// DECIMAL values return DECIMAL
func numericRetType(inputType sql.Type) sql.Type {
	if types.IsSigned(inputType) || types.IsUnsigned(inputType) {
		return types.Int64
	} else if types.IsFloat(inputType) {
		return types.Float64
	} else if types.IsDecimal(inputType) {
		return inputType
	} else if types.IsTextBlob(inputType) {
		return types.Float64 // DOUBLE for non-numeric types
	}

	return types.Float64
}

// Ceil returns the smallest integer value not less than X.
type Ceil struct {
	expression.UnaryExpressionStub
}

var _ sql.FunctionExpression = (*Ceil)(nil)
var _ sql.CollationCoercible = (*Ceil)(nil)

// NewCeil creates a new Ceil expression.
func NewCeil(ctx *sql.Context, num sql.Expression) sql.Expression {
	return &Ceil{expression.UnaryExpressionStub{Child: num}}
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
func (c *Ceil) Type(ctx *sql.Context) sql.Type {
	childType := c.Child.Type(ctx)
	if types.IsUnsigned(childType) {
		return types.Uint64
	}
	if types.IsNumber(childType) {
		return types.Int64
	}
	return types.Float64
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*Ceil) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 5
}

func (c *Ceil) String() string {
	return fmt.Sprintf("%s(%s)", c.FunctionName(), c.Child)
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
	if !types.IsNumber(c.Child.Type(ctx)) {
		child, _, err = types.Float64.Convert(ctx, child)
		if err != nil {
			if !sql.ErrTruncatedIncorrect.Is(err) {
				return nil, err
			}
			ctx.Warn(mysql.ERTruncatedWrongValue, "%s", err.Error())
		}
	}
	// if it's number type and not float value, it does not need ceil-ing
	switch num := child.(type) {
	case float32:
		child = math.Ceil(float64(num))
	case float64:
		child = math.Ceil(num)
	case apd.Decimal:
		_, err = sql.DecimalCtx.Ceil(&num, &num)
		if err != nil {
			return nil, err
		}
		child = num
	}
	child, _, _ = c.Type(ctx).Convert(ctx, child)
	return child, nil
}

// Floor returns the biggest integer value not less than X.
type Floor struct {
	expression.UnaryExpressionStub
}

var _ sql.FunctionExpression = (*Floor)(nil)
var _ sql.CollationCoercible = (*Floor)(nil)

// NewFloor returns a new Floor expression.
func NewFloor(ctx *sql.Context, num sql.Expression) sql.Expression {
	return &Floor{expression.UnaryExpressionStub{Child: num}}
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
func (f *Floor) Type(ctx *sql.Context) sql.Type {
	childType := f.Child.Type(ctx)
	if types.IsUnsigned(childType) {
		return types.Uint64
	}
	if types.IsNumber(childType) {
		return types.Int64
	}
	return types.Float64
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*Floor) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 5
}

func (f *Floor) String() string {
	return fmt.Sprintf("%s(%s)", f.FunctionName(), f.Child)
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
	if !types.IsNumber(f.Child.Type(ctx)) {
		child, _, err = types.Float64.Convert(ctx, child)
		if err != nil {
			if !sql.ErrTruncatedIncorrect.Is(err) {
				return nil, err
			}
			ctx.Warn(mysql.ERTruncatedWrongValue, "%s", err.Error())
		}
	}
	// if it's number type and not float value, it does not need ceil-ing
	switch num := child.(type) {
	case float32:
		child = math.Floor(float64(num))
	case float64:
		child = math.Floor(num)
	case apd.Decimal:
		_, err = sql.DecimalCtx.Floor(&num, &num)
		if err != nil {
			return nil, err
		}
		child = num
	}
	child, _, _ = f.Type(ctx).Convert(ctx, child)
	return child, nil
}

// Round returns the number (x) with (d) requested decimal places.
// If d is negative, the number is returned with the (abs(d)) least significant
// digits of it's integer part set to 0. If d is not specified or nil/null
// it defaults to 0.
type Round struct {
	Num sql.Expression
	Dec sql.Expression
}

var _ sql.FunctionExpression = (*Round)(nil)
var _ sql.CollationCoercible = (*Round)(nil)

// NewRound returns a new Round expression.
func NewRound(ctx *sql.Context, args ...sql.Expression) (sql.Expression, error) {
	argLen := len(args)
	switch argLen {
	case 1:
		return &Round{
			Num: args[0],
		}, nil
	case 2:
		return &Round{
			Num: args[0],
			Dec: args[1],
		}, nil
	default:
		return nil, sql.ErrInvalidArgumentNumber.New("ROUND", "1 or 2", argLen)
	}
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
	if r.Dec == nil {
		return []sql.Expression{r.Num}
	}
	return []sql.Expression{r.Num, r.Dec}
}

// Eval implements the Expression interface.
func (r *Round) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	val, err := r.Num.Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	if val == nil {
		return nil, nil
	}

	val, _, err = types.InternalDecimalType.Convert(ctx, val)
	if err != nil && sql.ErrTruncatedIncorrect.Is(err) {
		ctx.Warn(mysql.ERTruncatedWrongValue, "%s", err.Error())
	}

	prec := int32(0)
	if r.Dec != nil {
		var tmp any
		tmp, err = r.Dec.Eval(ctx, row)
		if err != nil {
			return nil, err
		}
		if tmp == nil {
			return nil, nil
		}
		tmp, _, err = types.Int32.Convert(ctx, tmp)
		if err != nil {
			if !sql.ErrTruncatedIncorrect.Is(err) {
				return nil, err
			}
			ctx.Warn(mysql.ERTruncatedWrongValue, "%s", err.Error())
		}
		prec = tmp.(int32)
		// MySQL cuts off at 30 for larger values
		// TODO: these limits are fine only because we can't handle decimals larger than this
		if prec > types.DecimalTypeMaxPrecision {
			prec = types.DecimalTypeMaxPrecision
		}
		if prec < -types.DecimalTypeMaxScale {
			prec = -types.DecimalTypeMaxScale
		}
	}

	var res interface{}
	tmp, err := types.DecimalRound(val.(apd.Decimal), prec)
	if err != nil {
		return nil, err
	}

	lType := r.Num.Type(ctx)
	if types.IsSigned(lType) {
		res, _, err = types.Int64.Convert(ctx, tmp)
	} else if types.IsUnsigned(lType) {
		res, _, err = types.Uint64.Convert(ctx, tmp)
	} else if types.IsFloat(lType) {
		res, _, err = types.Float64.Convert(ctx, tmp)
	} else if types.IsDecimal(lType) {
		res = tmp
	} else if types.IsTextBlob(lType) {
		res, _, err = types.Float64.Convert(ctx, tmp)
	}
	if err != nil && sql.ErrTruncatedIncorrect.Is(err) {
		ctx.Warn(mysql.ERTruncatedWrongValue, "%s", err.Error())
		err = nil
	}
	return res, err
}

// IsNullable implements the Expression interface.
func (r *Round) IsNullable(ctx *sql.Context) bool {
	return r.Num.IsNullable(ctx) || (r.Dec != nil && r.Dec.IsNullable(ctx))
}

func (r *Round) String() string {
	if r.Dec == nil {
		return fmt.Sprintf("%s(%s,0)", r.FunctionName(), r.Num.String())
	}
	return fmt.Sprintf("%s(%s,%s)", r.FunctionName(), r.Num.String(), r.Dec.String())
}

// Resolved implements the Expression interface.
func (r *Round) Resolved() bool {
	return r.Num.Resolved() && (r.Dec == nil || r.Dec.Resolved())
}

// Type implements the Expression interface.
func (r *Round) Type(ctx *sql.Context) sql.Type {
	return numericRetType(r.Num.Type(ctx))
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*Round) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 5
}

// WithChildren implements the Expression interface.
func (r *Round) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	return NewRound(ctx, children...)
}
