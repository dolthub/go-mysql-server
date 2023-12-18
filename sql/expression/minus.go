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

var _ ArithmeticOp = (*Minus)(nil)
var _ sql.CollationCoercible = (*Minus)(nil)

// Minus expressions include plus, minus and multiplication (+, -, *) operations.
type Minus struct {
	BinaryExpression
	ops int32
}

// NewMinus creates a new Minus sql.Expression.
func NewMinus(left, right sql.Expression) *Minus {
	a := &Minus{BinaryExpression{Left: left, Right: right}, 0}
	ops := countArithmeticOps(a)
	setArithmeticOps(a, ops)
	return a
}

func (m *Minus) LeftChild() sql.Expression {
	return m.Left
}

func (m *Minus) RightChild() sql.Expression {
	return m.Right
}

func (m *Minus) Operator() string {
	return sqlparser.MinusStr
}

func (m *Minus) SetOpCount(i int32) {
	m.ops = i
}

func (m *Minus) String() string {
	return fmt.Sprintf("(%s - %s)", m.Left, m.Right)
}

func (m *Minus) DebugString() string {
	return fmt.Sprintf("(%s - %s)", sql.DebugString(m.Left), sql.DebugString(m.Right))
}

// IsNullable implements the sql.Expression interface.
func (m *Minus) IsNullable() bool {
	if types.IsDatetimeType(m.Type()) || types.IsTimestampType(m.Type()) {
		return true
	}

	return m.BinaryExpression.IsNullable()
}

// Type returns the greatest type for given operation.
func (m *Minus) Type() sql.Type {
	//TODO: what if both BindVars? should be constant folded
	rTyp := m.Right.Type()
	if types.IsDeferredType(rTyp) {
		return rTyp
	}
	lTyp := m.Left.Type()
	if types.IsDeferredType(lTyp) {
		return lTyp
	}

	// applies for + and - ops
	if isInterval(m.Left) || isInterval(m.Right) {
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

	return getFloatOrMaxDecimalType(m, false)
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*Minus) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 5
}

// WithChildren implements the Expression interface.
func (m *Minus) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 2 {
		return nil, sql.ErrInvalidChildrenNumber.New(m, len(children), 2)
	}
	return NewMinus(children[0], children[1]), nil
}

// Eval implements the Expression interface.
func (m *Minus) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	lval, rval, err := m.evalLeftRight(ctx, row)
	if err != nil {
		return nil, err
	}

	if lval == nil || rval == nil {
		return nil, nil
	}

	lval, rval, err = m.convertLeftRight(ctx, lval, rval)
	if err != nil {
		return nil, err
	}

	return minus(lval, rval)
}

func (m *Minus) evalLeftRight(ctx *sql.Context, row sql.Row) (interface{}, interface{}, error) {
	var lval, rval interface{}
	var err error

	if i, ok := m.Left.(*Interval); ok {
		lval, err = i.EvalDelta(ctx, row)
		if err != nil {
			return nil, nil, err
		}
	} else {
		lval, err = m.Left.Eval(ctx, row)
		if err != nil {
			return nil, nil, err
		}
	}

	if i, ok := m.Right.(*Interval); ok {
		rval, err = i.EvalDelta(ctx, row)
		if err != nil {
			return nil, nil, err
		}
	} else {
		rval, err = m.Right.Eval(ctx, row)
		if err != nil {
			return nil, nil, err
		}
	}

	return lval, rval, nil
}

func (m *Minus) convertLeftRight(ctx *sql.Context, left interface{}, right interface{}) (interface{}, interface{}, error) {
	typ := m.Type()

	lIsTimeType := types.IsTime(m.Left.Type())
	rIsTimeType := types.IsTime(m.Right.Type())

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

// UnaryMinus is a unary minus operator.
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
