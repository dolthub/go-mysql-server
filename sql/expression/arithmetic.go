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

package expression

import (
	"fmt"
	"math"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/apd/v3"
	"github.com/dolthub/vitess/go/mysql"
	"github.com/dolthub/vitess/go/vt/sqlparser"
	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

var (
	// errUnableToCast means that we could not find common type for two arithemtic objects
	errUnableToCast = errors.NewKind("Unable to cast between types: %T, %T")

	// errUnableToEval means that we could not evaluate an expression
	errUnableToEval = errors.NewKind("Unable to evaluate an expression: %v %s %v")

	timeTypeRegex = regexp.MustCompile("[0-9]+")
)

func arithmeticWarning(ctx *sql.Context, errCode int, errMsg string) {
	if ctx != nil && ctx.Session != nil {
		ctx.Session.Warn(&sql.Warning{
			Level:   "Warning",
			Code:    errCode,
			Message: errMsg,
		})
	}
}

// ArithmeticOp implements an arithmetic expression. Since we had separate expressions
// for division and mod operation, we need to group all arithmetic together. Use this
// expression to define any arithmetic operation that is separately implemented from
// Arithmetic expression in the future.
type ArithmeticOp interface {
	sql.Expression
	BinaryExpression
	SetOpCount(int32)
	Operator() string
}

var _ ArithmeticOp = (*Arithmetic)(nil)
var _ sql.CollationCoercible = (*Arithmetic)(nil)

// Arithmetic expressions include plus, minus and multiplication (+, -, *) operations.
type Arithmetic struct {
	BinaryExpressionStub
	Op  string
	ops int32
	typ sql.Type
}

// NewArithmetic creates a new Arithmetic sql.Expression.
func NewArithmetic(left, right sql.Expression, op string) *Arithmetic {
	a := &Arithmetic{
		BinaryExpressionStub: BinaryExpressionStub{
			LeftChild:  left,
			RightChild: right,
		},
		Op: op,
	}
	ops := countArithmeticOps(a)
	setArithmeticOps(a, ops)
	return a
}

// NewPlus creates a new Arithmetic + sql.Expression.
func NewPlus(left, right sql.Expression) *Arithmetic {
	return NewArithmetic(left, right, sqlparser.PlusStr)
}

// NewMinus creates a new Arithmetic - sql.Expression.
func NewMinus(left, right sql.Expression) *Arithmetic {
	return NewArithmetic(left, right, sqlparser.MinusStr)
}

// NewMult creates a new Arithmetic * sql.Expression.
func NewMult(left, right sql.Expression) *Arithmetic {
	return NewArithmetic(left, right, sqlparser.MultStr)
}

func (a *Arithmetic) Operator() string {
	return a.Op
}

func (a *Arithmetic) SetOpCount(i int32) {
	a.ops = i
}

func (a *Arithmetic) String() string {
	return fmt.Sprintf("(%s %s %s)", a.LeftChild.String(), a.Op, a.RightChild.String())
}

func (a *Arithmetic) DebugString(ctx *sql.Context) string {
	return fmt.Sprintf("(%s %s %s)", sql.DebugString(ctx, a.LeftChild), a.Op, sql.DebugString(ctx, a.RightChild))
}

// IsNullable implements the sql.Expression interface.
func (a *Arithmetic) IsNullable(ctx *sql.Context) bool {
	typ := a.Type(ctx)
	if types.IsDatetimeType(typ) || types.IsTimestampType(typ) {
		return true
	}

	return a.BinaryExpressionStub.IsNullable(ctx)
}

// getReturnType returns the greatest type for given operation.
func (a *Arithmetic) getReturnType(ctx *sql.Context) sql.Type {
	// TODO: what if both BindVars? should be constant folded
	rTyp := a.RightChild.Type(ctx)
	if types.IsDeferredType(rTyp) {
		return rTyp
	}
	lTyp := a.LeftChild.Type(ctx)
	if types.IsDeferredType(lTyp) {
		return lTyp
	}

	// applies for + and - ops
	if isInterval(a.LeftChild) || isInterval(a.RightChild) {
		// TODO: need to use the precision stored in datetimeType; something like
		//   return types.MustCreateDatetimeType(sqltypes.Datetime, 0)
		return types.Datetime
	}

	if types.IsText(lTyp) || types.IsText(rTyp) {
		return types.Float64
	}

	if types.IsJSON(lTyp) || types.IsJSON(rTyp) {
		return types.Float64
	}

	if types.IsFloat(lTyp) || types.IsFloat(rTyp) {
		return types.Float64
	}

	if types.IsYear(lTyp) && types.IsYear(rTyp) {
		// MySQL just returns the largest int that fits
		return types.Uint64
	}

	// Bit types are integers
	if types.IsBit(lTyp) {
		lTyp = types.Int64
	}
	if types.IsBit(rTyp) {
		rTyp = types.Int64
	}

	// Dates are Integers
	if types.IsDateType(lTyp) {
		lTyp = types.Int64
	}
	if types.IsDateType(rTyp) {
		rTyp = types.Int64
	}

	// Datetime(0) is treated as Int64, otherwise as Decimal
	if types.IsDatetimeType(lTyp) {
		if dtType, ok := lTyp.(sql.DatetimeType); ok {
			scale := uint8(dtType.Precision())
			if scale == 0 {
				lTyp = types.Int64
			} else {
				lTyp = types.MustCreateDecimalType(types.DecimalTypeMaxPrecision, scale)
			}
		}
	}
	if types.IsDatetimeType(rTyp) {
		if dtType, ok := rTyp.(sql.DatetimeType); ok {
			scale := uint8(dtType.Precision())
			if scale == 0 {
				rTyp = types.Int64
			} else {
				rTyp = types.MustCreateDecimalType(types.DecimalTypeMaxPrecision, scale)
			}
		}
	}

	if types.IsUnsigned(lTyp) && types.IsUnsigned(rTyp) {
		return types.Uint64
	}

	if types.IsInteger(lTyp) && types.IsInteger(rTyp) {
		return types.Int64
	}

	if types.IsDecimal(lTyp) && !types.IsDecimal(rTyp) {
		return lTyp
	}

	if types.IsDecimal(rTyp) && !types.IsDecimal(lTyp) {
		return rTyp
	}

	if types.IsDecimal(lTyp) && types.IsDecimal(rTyp) {
		lPrec := lTyp.(sql.DecimalType).Precision()
		lScale := lTyp.(sql.DecimalType).Scale()
		rPrec := rTyp.(sql.DecimalType).Precision()
		rScale := rTyp.(sql.DecimalType).Scale()

		var prec, scale uint8
		if lPrec > rPrec {
			prec = lPrec
		} else {
			prec = rPrec
		}

		switch a.Op {
		case sqlparser.PlusStr, sqlparser.MinusStr:
			if lScale > rScale {
				scale = lScale
			} else {
				scale = rScale
			}
			prec = prec + scale
		case sqlparser.MultStr:
			scale = lScale + rScale
			prec = prec + scale
		}

		if prec > types.DecimalTypeMaxPrecision {
			prec = types.DecimalTypeMaxPrecision
		}
		if scale > types.DecimalTypeMaxScale {
			scale = types.DecimalTypeMaxScale
		}

		return types.MustCreateDecimalType(prec, scale)
	}

	// When in doubt return float64
	return types.Float64
}

// Type implements the Expression interface
func (a *Arithmetic) Type(ctx *sql.Context) sql.Type {
	// Cache the return type for Arithmetic functions for performance.
	// We this here instead of NewArithmeticExpression because of placeholder expressions.
	if a.typ == nil {
		a.typ = a.getReturnType(ctx)
	}
	return a.typ
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*Arithmetic) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 5
}

// WithChildren implements the Expression interface.
func (a *Arithmetic) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 2 {
		return nil, sql.ErrInvalidChildrenNumber.New(a, len(children), 2)
	}
	// sanity check
	switch strings.ToLower(a.Op) {
	case sqlparser.DivStr:
		return NewDiv(children[0], children[1]), nil
	case sqlparser.ModStr:
		return NewMod(children[0], children[1]), nil
	}
	return NewArithmetic(children[0], children[1], a.Op), nil
}

// Eval implements the Expression interface.
func (a *Arithmetic) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	lVal, rVal, err := a.evalLeftRight(ctx, row)
	if err != nil {
		return nil, err
	}

	if lVal == nil || rVal == nil {
		return nil, nil
	}

	lVal, rVal, err = a.convertLeftRight(ctx, lVal, rVal)
	if err != nil {
		return nil, err
	}

	var result interface{}
	switch strings.ToLower(a.Op) {
	case sqlparser.PlusStr:
		result, err = plus(lVal, rVal)
	case sqlparser.MinusStr:
		result, err = minus(lVal, rVal)
	case sqlparser.MultStr:
		result, err = mult(lVal, rVal)
	}

	if err != nil {
		return nil, err
	}

	// Decimals must be rounded
	if res, ok := result.(*apd.Decimal); ok {
		if isOutermostArithmeticOp(a, a.ops) {
			finalScale, hasDiv := getFinalScale(ctx, row, a, 0)
			if hasDiv {
				// TODO: should always round regardless; we have bad Decimal defaults
				return sql.DecimalRound(res, finalScale)
			}
		}
	}

	return result, nil
}

func (a *Arithmetic) evalLeftRight(ctx *sql.Context, row sql.Row) (interface{}, interface{}, error) {
	var lval, rval interface{}
	var err error

	if i, ok := a.LeftChild.(TimeDeltaExpression); ok {
		lval, err = i.EvalDelta(ctx, row)
		if err != nil {
			return nil, nil, err
		}
	} else {
		lval, err = a.LeftChild.Eval(ctx, row)
		if err != nil {
			return nil, nil, err
		}
	}

	if i, ok := a.RightChild.(TimeDeltaExpression); ok {
		rval, err = i.EvalDelta(ctx, row)
		if err != nil {
			return nil, nil, err
		}
	} else {
		rval, err = a.RightChild.Eval(ctx, row)
		if err != nil {
			return nil, nil, err
		}
	}

	return lval, rval, nil
}

func (a *Arithmetic) convertLeftRight(ctx *sql.Context, lVal, rVal any) (any, any, error) {
	typ, lTyp, rTyp := a.Type(ctx), a.LeftChild.Type(ctx), a.RightChild.Type(ctx)
	if i, ok := lVal.(*TimeDelta); ok {
		lVal = i
	} else {
		// these are the types we specifically want to capture from we get from Type()
		if types.IsInteger(typ) || types.IsFloat(typ) || types.IsTime(typ) {
			lVal = convertValueToType(ctx, lTyp, typ, lVal)
		} else {
			lVal = convertToDecimalValue(ctx, lTyp, typ, lVal)
		}
	}

	if i, ok := rVal.(*TimeDelta); ok {
		rVal = i
	} else {
		// these are the types we specifically want to capture from we get from Type()
		if types.IsInteger(typ) || types.IsFloat(typ) || types.IsTime(typ) {
			rVal = convertValueToType(ctx, rTyp, typ, rVal)
		} else {
			rVal = convertToDecimalValue(ctx, rTyp, typ, rVal)
		}
	}

	return lVal, rVal, nil
}

func isInterval(expr sql.Expression) bool {
	_, ok := expr.(TimeDeltaExpression)
	return ok
}

// countArithmeticOps returns the number of arithmetic operators under the current node.
// This lets us count how many arithmetic operators used one after the other
func countArithmeticOps(e sql.Expression) int32 {
	if e == nil {
		return 0
	}

	if a, ok := e.(ArithmeticOp); ok {
		return countArithmeticOps(a.Left()) + countArithmeticOps(a.Right()) + 1
	}

	return 0
}

// setArithmeticOps will set ops number with number counted by countArithmeticOps. This allows
// us to keep track of whether the expression is the last arithmetic operation.
func setArithmeticOps(e sql.Expression, ops int32) {
	if e == nil {
		return
	}

	if a, ok := e.(ArithmeticOp); ok {
		a.SetOpCount(ops)
		setArithmeticOps(a.Left(), ops)
		setArithmeticOps(a.Right(), ops)
	}

	if tup, ok := e.(Tuple); ok {
		for _, expr := range tup {
			setArithmeticOps(expr, ops)
		}
	}

	return
}

// isOutermostArithmeticOp return whether the expression we're currently on is
// the last arithmetic operation of all continuous arithmetic operations.
func isOutermostArithmeticOp(e sql.Expression, opScale int32) bool {
	return opScale == countArithmeticOps(e)
}

// convertValueToType returns |val| converted into type |typ|. If the value is
// invalid and cannot be converted to the given type, it returns nil, and it should be
// interpreted as value of 0. For time types, all the numbers are parsed up to seconds only.
// E.g: `2022-11-10 12:14:36` is parsed into `20221110121436` and `2022-03-24` is parsed into `20220324`.
func convertValueToType(ctx *sql.Context, origType, typ sql.Type, val any) (res any) {
	// TODO: update type aware implementation for datetime types
	//  This is a placeholder implementation for existing tests
	if dtTyp, ok := origType.(sql.DatetimeType); ok && !types.IsTime(typ) {
		var err error
		val, err = DateTimeToNumericString(ctx, dtTyp, val)
		if err != nil {
			ctx.Warn(mysql.ERTruncatedWrongValue, "%s", sql.ErrTruncatedIncorrect.New(dtTyp.String(), val).Error())
		}
	}

	var cVal any
	var err error
	switch t := typ.(type) {
	case sql.DatetimeType:
		cVal, _, err = t.Convert(ctx, val)
		if err == nil {
			if timeVal, ok := cVal.(time.Time); ok && types.ZeroTime.Equal(timeVal) {
				ctx.Warn(mysql.ERTruncatedWrongValue, "%s", sql.ErrTruncatedIncorrect.New(typ.String(), val).Error())
				return nil
			}
		}
	default:
		cVal, _, err = typ.Convert(ctx, val)
	}

	if err != nil {
		// the value is interpreted as 0, but we need to match the type of the other valid value
		// to avoid additional conversion, the nil value is handled in each operation
		ctx.Warn(mysql.ERTruncatedWrongValue, "%s", sql.ErrTruncatedIncorrect.New(typ.String(), val).Error())
	}
	return cVal
}

// DateTimeToNumericString converts a time.Time object into a "numeric" string (datetime with all delimiters removed).
// `2022-11-10 12:14:36` is parsed into `20221110121436`
// `2022-03-24` is parsed into `20220324`.
// TODO: this should just be on the dateTimeType itself
func DateTimeToNumericString(ctx *sql.Context, typ sql.DatetimeType, val any) (string, error) {
	// Convert to MySQL formatted DateTime string
	sqlVal, err := typ.SQL(ctx, nil, val)
	if err != nil {
		return "", err
	}
	// Drop all non-digits and concat
	nums := timeTypeRegex.FindAllString(sqlVal.ToString(), -1)
	res := strings.Join(nums, "")
	return res, nil
}

func plus(lval, rval interface{}) (interface{}, error) {
	if lval == nil || rval == nil {
		return nil, nil
	}
	switch l := lval.(type) {
	case uint8:
		switch r := rval.(type) {
		case uint8:
			return l + r, nil
		}
	case int8:
		switch r := rval.(type) {
		case int8:
			return l + r, nil
		}
	case uint16:
		switch r := rval.(type) {
		case uint16:
			return l + r, nil
		}
	case int16:
		switch r := rval.(type) {
		case int16:
			return l + r, nil
		}
	case uint32:
		switch r := rval.(type) {
		case uint32:
			return l + r, nil
		}
	case int32:
		switch r := rval.(type) {
		case int32:
			return l + r, nil
		}
	case uint64:
		switch r := rval.(type) {
		case uint64:
			return l + r, nil
		}
	case int64:
		switch r := rval.(type) {
		case int64:
			return l + r, nil
		}
	case float32:
		switch r := rval.(type) {
		case float32:
			return l + r, nil
		}
	case float64:
		switch r := rval.(type) {
		case float64:
			return l + r, nil
		}
	case *apd.Decimal:
		switch r := rval.(type) {
		case *apd.Decimal:
			res := new(apd.Decimal)
			_, err := sql.DecimalCtx.Add(res, l, r)
			return res, err
		}
	case time.Time:
		switch r := rval.(type) {
		case *TimeDelta:
			return types.ValidateTime(r.Add(l)), nil
		case time.Time:
			return l.Unix() + r.Unix(), nil
		}
	case *TimeDelta:
		switch r := rval.(type) {
		case time.Time:
			return types.ValidateTime(l.Add(r)), nil
		}
	}

	return nil, errUnableToCast.New(lval, rval)
}

func minus(lval, rval interface{}) (interface{}, error) {
	if lval == nil || rval == nil {
		return nil, nil
	}
	switch l := lval.(type) {
	case uint8:
		switch r := rval.(type) {
		case uint8:
			return l - r, nil
		}
	case int8:
		switch r := rval.(type) {
		case int8:
			return l - r, nil
		}
	case uint16:
		switch r := rval.(type) {
		case uint16:
			return l - r, nil
		}
	case int16:
		switch r := rval.(type) {
		case int16:
			return l - r, nil
		}
	case uint32:
		switch r := rval.(type) {
		case uint32:
			return l - r, nil
		}
	case int32:
		switch r := rval.(type) {
		case int32:
			return l - r, nil
		}
	case uint64:
		switch r := rval.(type) {
		case uint64:
			return l - r, nil
		}
	case int64:
		switch r := rval.(type) {
		case int64:
			return l - r, nil
		}
	case float32:
		switch r := rval.(type) {
		case float32:
			return l - r, nil
		}
	case float64:
		switch r := rval.(type) {
		case float64:
			return l - r, nil
		}
	case *apd.Decimal:
		switch r := rval.(type) {
		case *apd.Decimal:
			res := new(apd.Decimal)
			_, err := sql.DecimalCtx.Sub(res, l, r)
			return res, err
		}
	case time.Time:
		switch r := rval.(type) {
		case *TimeDelta:
			return types.ValidateTime(r.Sub(l)), nil
		case time.Time:
			return l.Unix() - r.Unix(), nil
		}
	}

	return nil, errUnableToCast.New(lval, rval)
}

func mult(lval, rval interface{}) (interface{}, error) {
	switch l := lval.(type) {
	case uint8:
		switch r := rval.(type) {
		case uint8:
			return l * r, nil
		}
	case int8:
		switch r := rval.(type) {
		case int8:
			return l * r, nil
		}
	case uint16:
		switch r := rval.(type) {
		case uint16:
			return l * r, nil
		}
	case int16:
		switch r := rval.(type) {
		case int16:
			return l * r, nil
		}
	case uint32:
		switch r := rval.(type) {
		case uint32:
			return l * r, nil
		}
	case int32:
		switch r := rval.(type) {
		case int32:
			return l * r, nil
		}
	case uint64:
		switch r := rval.(type) {
		case uint64:
			return l * r, nil
		}
	case int64:
		switch r := rval.(type) {
		case int64:
			return l * r, nil
		}
	case float32:
		switch r := rval.(type) {
		case float32:
			return l * r, nil
		}
	case float64:
		switch r := rval.(type) {
		case float64:
			return l * r, nil
		}
	case *apd.Decimal:
		switch r := rval.(type) {
		case *apd.Decimal:
			res := new(apd.Decimal)
			_, err := sql.DecimalCtx.Mul(res, l, r)
			return res, err
		}
	}

	return nil, errUnableToCast.New(lval, rval)
}

// UnaryMinus is an unary minus operator.
type UnaryMinus struct {
	UnaryExpressionStub
}

var _ sql.Expression = (*UnaryMinus)(nil)
var _ sql.CollationCoercible = (*UnaryMinus)(nil)

// NewUnaryMinus creates a new UnaryMinus expression node.
func NewUnaryMinus(child sql.Expression) *UnaryMinus {
	return &UnaryMinus{UnaryExpressionStub{Child: child}}
}

// Eval implements the sql.Expression interface.
func (e *UnaryMinus) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	child, err := e.Child.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if child == nil {
		return nil, nil
	}

	if !types.IsNumber(e.Child.Type(ctx)) {
		child, _, err = types.InternalDecimalType.Convert(ctx, child)
		if err != nil {
			if !sql.ErrTruncatedIncorrect.Is(err) {
				child = 0.0
			}
			ctx.Warn(mysql.ERTruncatedWrongValue, "%s", err.Error())
		}
	}

	switch n := child.(type) {
	case float64:
		return -n, nil
	case float32:
		return -n, nil
	case int8:
		return -int64(n), nil
	case int16:
		return -int64(n), nil
	case int32:
		return -int64(n), nil
	case int64:
		if n == math.MinInt64 {
			if _, ok := e.Child.(*Literal); ok {
				dec := types.DecimalFromInt64(n)
				return dec.Neg(dec), nil
			}
			return nil, sql.ErrValueOutOfRange.New("BIGINT", fmt.Sprintf("%d", n))
		}
		return -n, nil
	case uint:
		return -int(n), nil
	case uint8:
		return -int8(n), nil
	case uint16:
		return -int16(n), nil
	case uint32:
		return -int32(n), nil
	case uint64:
		return -int64(n), nil
	case *apd.Decimal:
		res := new(apd.Decimal)
		return res.Neg(n), nil
	case string:
		// try getting int out of string value
		i, iErr := strconv.ParseInt(n, 10, 64)
		if iErr != nil {
			return nil, sql.ErrInvalidType.New(reflect.TypeOf(n))
		}
		return -i, nil
	case bool:
		if n {
			return -1, nil
		} else {
			return 0, nil
		}
	default:
		return nil, sql.ErrInvalidType.New(reflect.TypeOf(n))
	}
}

// Type implements the sql.Expression interface.
func (e *UnaryMinus) Type(ctx *sql.Context) sql.Type {
	typ := e.Child.Type(ctx)
	switch typ {
	case types.Int8, types.Int16, types.Int32:
		typ = types.Int64
	case types.Int64:
		if lit, ok := e.Child.(*Literal); ok {
			// lit.Value() can be nil
			if v, ok := lit.Value().(int64); ok && v == math.MinInt64 {
				return types.InternalDecimalType
			}
		}
	}

	if !types.IsNumber(typ) {
		return types.Float64
	}

	if typ == types.Uint32 {
		return types.Int32
	}

	if typ == types.Uint64 {
		return types.Int64
	}

	return typ
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*UnaryMinus) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 5
}

func (e *UnaryMinus) String() string {
	return fmt.Sprintf("-%s", e.Child)
}

// WithChildren implements the Expression interface.
func (e *UnaryMinus) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(e, len(children), 1)
	}
	return NewUnaryMinus(children[0]), nil
}
