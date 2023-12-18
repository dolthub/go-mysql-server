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
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/dolthub/vitess/go/mysql"
	"github.com/dolthub/vitess/go/vt/sqlparser"
	"github.com/shopspring/decimal"
	errors "gopkg.in/src-d/go-errors.v1"

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
	ctx.Session.Warn(&sql.Warning{
		Level:   "Warning",
		Code:    errCode,
		Message: errMsg,
	})
}

// ArithmeticOp implements an arithmetic expression. Since we had separate expressions
// for division and mod operation, we need to group all arithmetic together. Use this
// expression to define any arithmetic operation that is separately implemented from
// Arithmetic expression in the future.
type ArithmeticOp interface {
	sql.Expression
	LeftChild() sql.Expression
	RightChild() sql.Expression
	SetOpCount(int32)
	Operator() string
}

var _ ArithmeticOp = (*Arithmetic)(nil)
var _ sql.CollationCoercible = (*Arithmetic)(nil)

// Arithmetic expressions include plus, minus and multiplication (+, -, *) operations.
type Arithmetic struct {
	BinaryExpression
	Op  string
	ops int32
}

// NewArithmetic creates a new Arithmetic sql.Expression.
func NewArithmetic(left, right sql.Expression, op string) *Arithmetic {
	a := &Arithmetic{BinaryExpression{Left: left, Right: right}, op, 0}
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

func (a *Arithmetic) LeftChild() sql.Expression {
	return a.Left
}

func (a *Arithmetic) RightChild() sql.Expression {
	return a.Right
}

func (a *Arithmetic) Operator() string {
	return a.Op
}

func (a *Arithmetic) SetOpCount(i int32) {
	a.ops = i
}

func (a *Arithmetic) String() string {
	return fmt.Sprintf("(%s %s %s)", a.Left, a.Op, a.Right)
}

func (a *Arithmetic) DebugString() string {
	return fmt.Sprintf("(%s %s %s)", sql.DebugString(a.Left), a.Op, sql.DebugString(a.Right))
}

// IsNullable implements the sql.Expression interface.
func (a *Arithmetic) IsNullable() bool {
	if types.IsDatetimeType(a.Type()) || types.IsTimestampType(a.Type()) {
		return true
	}

	return a.BinaryExpression.IsNullable()
}

// Type returns the greatest type for given operation.
func (a *Arithmetic) Type() sql.Type {
	//TODO: what if both BindVars? should be constant folded
	rTyp := a.Right.Type()
	if types.IsDeferredType(rTyp) {
		return rTyp
	}
	lTyp := a.Left.Type()
	if types.IsDeferredType(lTyp) {
		return lTyp
	}

	// applies for + and - ops
	if isInterval(a.Left) || isInterval(a.Right) {
		// TODO: we might need to truncate precision here
		return types.DatetimeMaxPrecision
	}

	if types.IsTime(lTyp) && types.IsTime(rTyp) {
		return types.Int64
	}

	if !types.IsNumber(lTyp) || !types.IsNumber(rTyp) {
		return types.Float64
	}

	if types.IsUnsigned(lTyp) && types.IsUnsigned(rTyp) {
		return types.Uint64
	} else if types.IsSigned(lTyp) && types.IsSigned(rTyp) {
		return types.Int64
	}

	// if one is uint and the other is int of any size, then use int64
	if types.IsInteger(lTyp) && types.IsInteger(rTyp) {
		return types.Int64
	}

	return getFloatOrMaxDecimalType(a, false)
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*Arithmetic) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 5
}

// WithChildren implements the Expression interface.
func (a *Arithmetic) WithChildren(children ...sql.Expression) (sql.Expression, error) {
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
	lval, rval, err := a.evalLeftRight(ctx, row)
	if err != nil {
		return nil, err
	}

	if lval == nil || rval == nil {
		return nil, nil
	}

	lval, rval, err = a.convertLeftRight(ctx, lval, rval)
	if err != nil {
		return nil, err
	}

	switch strings.ToLower(a.Op) {
	case sqlparser.PlusStr:
		return plus(lval, rval)
	case sqlparser.MinusStr:
		return minus(lval, rval)
	case sqlparser.MultStr:
		return mult(lval, rval)
	}

	return nil, errUnableToEval.New(lval, a.Op, rval)
}

func (a *Arithmetic) evalLeftRight(ctx *sql.Context, row sql.Row) (interface{}, interface{}, error) {
	var lval, rval interface{}
	var err error

	if i, ok := a.Left.(*Interval); ok {
		lval, err = i.EvalDelta(ctx, row)
		if err != nil {
			return nil, nil, err
		}
	} else {
		lval, err = a.Left.Eval(ctx, row)
		if err != nil {
			return nil, nil, err
		}
	}

	if i, ok := a.Right.(*Interval); ok {
		rval, err = i.EvalDelta(ctx, row)
		if err != nil {
			return nil, nil, err
		}
	} else {
		rval, err = a.Right.Eval(ctx, row)
		if err != nil {
			return nil, nil, err
		}
	}

	return lval, rval, nil
}

func (a *Arithmetic) convertLeftRight(ctx *sql.Context, left interface{}, right interface{}) (interface{}, interface{}, error) {
	typ := a.Type()

	lIsTimeType := types.IsTime(a.Left.Type())
	rIsTimeType := types.IsTime(a.Right.Type())

	if i, ok := left.(*TimeDelta); ok {
		left = i
	} else {
		// these are the types we specifically want to capture from we get from Type()
		if types.IsInteger(typ) || types.IsFloat(typ) || types.IsTime(typ) {
			left = convertValueToType(ctx, typ, left, lIsTimeType)
		} else {
			left = convertToDecimalValue(left, lIsTimeType)
		}
	}

	if i, ok := right.(*TimeDelta); ok {
		right = i
	} else {
		// these are the types we specifically want to capture from we get from Type()
		if types.IsInteger(typ) || types.IsFloat(typ) || types.IsTime(typ) {
			right = convertValueToType(ctx, typ, right, rIsTimeType)
		} else {
			right = convertToDecimalValue(right, rIsTimeType)
		}
	}

	return left, right, nil
}

func isInterval(expr sql.Expression) bool {
	_, ok := expr.(*Interval)
	return ok
}

// countArithmeticOps returns the number of arithmetic operators in order on the left child node of the current node.
// This lets us count how many arithmetic operators used one after the other
func countArithmeticOps(e sql.Expression) int32 {
	if e == nil {
		return 0
	}

	if a, ok := e.(ArithmeticOp); ok {
		return countDivs(a.LeftChild()) + 1
	}

	return 0
}

// setArithmeticOps will set ops number with number counted by countArithmeticOps. This allows
// us to keep track of whether the expression is the last arithmetic operation.
func setArithmeticOps(e sql.Expression, opScale int32) {
	if e == nil {
		return
	}

	if a, ok := e.(ArithmeticOp); ok {
		a.SetOpCount(opScale)
		setDivs(a.LeftChild(), opScale)
		setDivs(a.RightChild(), opScale)
	}

	return
}

// isOutermostArithmeticOp return whether the expression we're currently on is
// the last arithmetic operation of all continuous arithmetic operations.
func isOutermostArithmeticOp(e sql.Expression, d, dScale int32) bool {
	if e == nil {
		return false
	}

	if a, ok := e.(ArithmeticOp); ok {
		d = d + 1
		if d == dScale {
			return true
		} else {
			return isOutermostDiv(a.LeftChild(), d, dScale)
		}
	}

	return false
}

// convertValueToType returns |val| converted into type |typ|. If the value is
// invalid and cannot be converted to the given type, it returns nil, and it should be
// interpreted as value of 0. For time types, all the numbers are parsed up to seconds only.
// E.g: `2022-11-10 12:14:36` is parsed into `20221110121436` and `2022-03-24` is parsed into `20220324`.
func convertValueToType(ctx *sql.Context, typ sql.Type, val interface{}, isTimeType bool) interface{} {
	var cval interface{}
	if isTimeType {
		val = convertTimeTypeToString(val)
	}

	cval, _, err := typ.Convert(val)
	if err != nil {
		arithmeticWarning(ctx, mysql.ERTruncatedWrongValue, fmt.Sprintf("Truncated incorrect %s value: '%v'", typ.String(), val))
		// the value is interpreted as 0, but we need to match the type of the other valid value
		// to avoid additional conversion, the nil value is handled in each operation
	}
	return cval
}

// convertTimeTypeToString returns string value parsed from either time.Time or string
// representation. all the numbers are parsed up to seconds only. The location can be
// different between two time.Time values, so we set it to default UTC location before
// parsing. E.g:
// `2022-11-10 12:14:36` is parsed into `20221110121436`
// `2022-03-24` is parsed into `20220324`.
func convertTimeTypeToString(val interface{}) interface{} {
	if t, ok := val.(time.Time); ok {
		val = t.In(time.UTC).Format("2006-01-02 15:04:05")
	}
	if t, ok := val.(string); ok {
		nums := timeTypeRegex.FindAllString(t, -1)
		val = strings.Join(nums, "")
	}

	return val
}

func plus(lval, rval interface{}) (interface{}, error) {
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
	case decimal.Decimal:
		switch r := rval.(type) {
		case decimal.Decimal:
			return l.Add(r), nil
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
	case decimal.Decimal:
		switch r := rval.(type) {
		case decimal.Decimal:
			return l.Sub(r), nil
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

// UnaryMinus is an unary minus operator.
type UnaryMinus struct {
	UnaryExpression
}

var _ sql.Expression = (*UnaryMinus)(nil)
var _ sql.CollationCoercible = (*UnaryMinus)(nil)

// NewUnaryMinus creates a new UnaryMinus expression node.
func NewUnaryMinus(child sql.Expression) *UnaryMinus {
	return &UnaryMinus{UnaryExpression{Child: child}}
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

	if !types.IsNumber(e.Child.Type()) {
		child, err = decimal.NewFromString(fmt.Sprintf("%v", child))
		if err != nil {
			child = 0.0
		}
	}

	switch n := child.(type) {
	case float64:
		return -n, nil
	case float32:
		return -n, nil
	case int:
		return -n, nil
	case int8:
		return -n, nil
	case int16:
		return -n, nil
	case int32:
		return -n, nil
	case int64:
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
	case decimal.Decimal:
		return n.Neg(), err
	case string:
		// try getting int out of string value
		i, iErr := strconv.ParseInt(n, 10, 64)
		if iErr != nil {
			return nil, sql.ErrInvalidType.New(reflect.TypeOf(n))
		}
		return -i, nil
	default:
		return nil, sql.ErrInvalidType.New(reflect.TypeOf(n))
	}
}

// Type implements the sql.Expression interface.
func (e *UnaryMinus) Type() sql.Type {
	typ := e.Child.Type()
	if !types.IsNumber(typ) {
		return types.Float64
	}

	if typ == types.Uint32 {
		return types.Int32
	}

	if typ == types.Uint64 {
		return types.Int64
	}

	return e.Child.Type()
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*UnaryMinus) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 5
}

func (e *UnaryMinus) String() string {
	return fmt.Sprintf("-%s", e.Child)
}

// WithChildren implements the Expression interface.
func (e *UnaryMinus) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(e, len(children), 1)
	}
	return NewUnaryMinus(children[0]), nil
}
