// Copyright 2025 Dolthub, Inc.
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

	"github.com/dolthub/vitess/go/mysql"
	"github.com/shopspring/decimal"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// Truncate truncates a number to a specified number of decimal places.
// If D is 0, the result has no decimal point or fractional part.
// D can be negative to cause D digits left of the decimal point of the value X to become zero.
// If X or D is NULL, the function returns NULL.
// All numbers are rounded toward zero.
type Truncate struct {
	expression.BinaryExpressionStub
}

var _ sql.FunctionExpression = (*Truncate)(nil)
var _ sql.CollationCoercible = (*Truncate)(nil)

// NewTruncate returns a new Truncate expression.
func NewTruncate(args ...sql.Expression) (sql.Expression, error) {
	argLen := len(args)
	if argLen != 2 {
		return nil, sql.ErrInvalidArgumentNumber.New("TRUNCATE", "2", argLen)
	}

	return &Truncate{expression.BinaryExpressionStub{LeftChild: args[0], RightChild: args[1]}}, nil
}

// FunctionName implements sql.FunctionExpression
func (t *Truncate) FunctionName() string {
	return "truncate"
}

// Description implements sql.FunctionExpression
func (t *Truncate) Description() string {
	return "truncates the number to decimals decimal places."
}

// Children implements the Expression interface.
func (t *Truncate) Children() []sql.Expression {
	return t.BinaryExpressionStub.Children()
}

// Eval implements the Expression interface.
func (t *Truncate) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	val, err := t.LeftChild.Eval(ctx, row)
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

	prec, err := t.RightChild.Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	if prec == nil {
		return nil, nil
	}
	prec, _, err = types.Int32.Convert(ctx, prec)
	if err != nil {
		if !sql.ErrTruncatedIncorrect.Is(err) {
			return nil, err
		}
		ctx.Warn(mysql.ERTruncatedWrongValue, "%s", err.Error())
	}
	precision := prec.(int32)
	
	// MySQL cuts off at 30 for larger values
	// TODO: these limits are fine only because we can't handle decimals larger than this
	if precision > types.DecimalTypeMaxPrecision {
		precision = types.DecimalTypeMaxPrecision
	}
	if precision < -types.DecimalTypeMaxScale {
		precision = -types.DecimalTypeMaxScale
	}

	var res interface{}
	var tmp decimal.Decimal
	
	if precision < 0 {
		// For negative precision, we need to truncate digits to the left of decimal point
		// This is different from the decimal library's Truncate method
		// We need to divide by 10^|precision|, truncate, then multiply back
		multiplier := decimal.NewFromInt(1)
		for i := int32(0); i < -precision; i++ {
			multiplier = multiplier.Mul(decimal.NewFromInt(10))
		}
		tmp = val.(decimal.Decimal).Div(multiplier).Truncate(0).Mul(multiplier)
	} else {
		// For positive precision, use the standard Truncate method
		tmp = val.(decimal.Decimal).Truncate(precision)
	}
	
	lType := t.LeftChild.Type()
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
	} else {
		// Default to Float64 for unknown types
		res, _, err = types.Float64.Convert(ctx, tmp)
	}
	if err != nil && sql.ErrTruncatedIncorrect.Is(err) {
		ctx.Warn(mysql.ERTruncatedWrongValue, "%s", err.Error())
		err = nil
	}
	return res, err
}

// IsNullable implements the Expression interface.
func (t *Truncate) IsNullable() bool {
	return t.LeftChild.IsNullable() || t.RightChild.IsNullable()
}

func (t *Truncate) String() string {
	return fmt.Sprintf("%s(%s,%s)", t.FunctionName(), t.LeftChild.String(), t.RightChild.String())
}

// Resolved implements the Expression interface.
func (t *Truncate) Resolved() bool {
	return t.LeftChild.Resolved() && t.RightChild.Resolved()
}

// Type implements the Expression interface.
func (t *Truncate) Type() sql.Type {
	return numericRetType(t.LeftChild.Type())
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*Truncate) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 5
}

// WithChildren implements the Expression interface.
func (t *Truncate) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	return NewTruncate(children...)
}
