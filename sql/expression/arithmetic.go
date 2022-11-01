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
	"strings"
	"time"

	"github.com/dolthub/vitess/go/vt/sqlparser"
	"github.com/shopspring/decimal"
	errors "gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/sql"
)

var (
	// errUnableToCast means that we could not find common type for two arithemtic objects
	errUnableToCast = errors.NewKind("Unable to cast between types: %T, %T")

	// errUnableToEval means that we could not evaluate an expression
	errUnableToEval = errors.NewKind("Unable to evaluate an expression: %v %s %v")
)

// Arithmetic expressions (+, -, *, /, ...)
type Arithmetic struct {
	BinaryExpression
	Op string

	DivScale int32

	lval interface{}
	rval interface{}
}

// NewArithmetic creates a new Arithmetic sql.Expression.
func NewArithmetic(left, right sql.Expression, op string) *Arithmetic {
	a := &Arithmetic{BinaryExpression{Left: left, Right: right}, op, 0, nil, nil}
	divs := countDivs(a)
	setDivs(a, 0, divs)
	return a
}

// NewPlus creates a new Arithmetic + sql.Expression.
func NewPlus(left, right sql.Expression) *Arithmetic {
	return NewArithmetic(left, right, sqlparser.PlusStr)
}

func NewIncrement(left sql.Expression) *Arithmetic {
	one := NewLiteral(sql.NumericUnaryValue(left.Type()), left.Type())
	return NewArithmetic(left, one, sqlparser.PlusStr)
}

// NewMinus creates a new Arithmetic - sql.Expression.
func NewMinus(left, right sql.Expression) *Arithmetic {
	return NewArithmetic(left, right, sqlparser.MinusStr)
}

// NewMult creates a new Arithmetic * sql.Expression.
func NewMult(left, right sql.Expression) *Arithmetic {
	return NewArithmetic(left, right, sqlparser.MultStr)
}

// NewDiv creates a new Arithmetic / sql.Expression.
func NewDiv(left, right sql.Expression) *Arithmetic {
	return NewArithmetic(left, right, sqlparser.DivStr)
}

// NewShiftLeft creates a new Arithmetic << sql.Expression.
func NewShiftLeft(left, right sql.Expression) *Arithmetic {
	return NewArithmetic(left, right, sqlparser.ShiftLeftStr)
}

// NewShiftRight creates a new Arithmetic >> sql.Expression.
func NewShiftRight(left, right sql.Expression) *Arithmetic {
	return NewArithmetic(left, right, sqlparser.ShiftRightStr)
}

// NewBitAnd creates a new Arithmetic & sql.Expression.
func NewBitAnd(left, right sql.Expression) *Arithmetic {
	return NewArithmetic(left, right, sqlparser.BitAndStr)
}

// NewBitOr creates a new Arithmetic | sql.Expression.
func NewBitOr(left, right sql.Expression) *Arithmetic {
	return NewArithmetic(left, right, sqlparser.BitOrStr)
}

// NewBitXor creates a new Arithmetic ^ sql.Expression.
func NewBitXor(left, right sql.Expression) *Arithmetic {
	return NewArithmetic(left, right, sqlparser.BitXorStr)
}

// NewIntDiv creates a new Arithmetic div sql.Expression.
func NewIntDiv(left, right sql.Expression) *Arithmetic {
	return NewArithmetic(left, right, sqlparser.IntDivStr)
}

// NewMod creates a new Arithmetic % sql.Expression.
func NewMod(left, right sql.Expression) *Arithmetic {
	return NewArithmetic(left, right, sqlparser.ModStr)
}

func setDivs(e sql.Expression, d int32, divScale int32) {
	if e == nil {
		return
	}

	if a, ok := e.(*Arithmetic); ok && a.Op == sqlparser.DivStr {
		d = d + 1
		if d == divScale {
			a.DivScale = divScale
		} else {
			a.DivScale = 0
		}
		setDivs(a.Left, d, divScale)
	}

	return
}

func countDivs(e sql.Expression) int32 {
	if e == nil {
		return 0
	}

	if a, ok := e.(*Arithmetic); ok && a.Op == sqlparser.DivStr {
		return countDivs(a.Left) + 1
	}

	return 0
}

func (a *Arithmetic) String() string {
	return fmt.Sprintf("(%s %s %s)", a.Left, a.Op, a.Right)
}

func (a *Arithmetic) DebugString() string {
	return fmt.Sprintf("(%s %s %s)", sql.DebugString(a.Left), a.Op, sql.DebugString(a.Right))
}

// IsNullable implements the sql.Expression interface.
func (a *Arithmetic) IsNullable() bool {
	if a.Type() == sql.Timestamp || a.Type() == sql.Datetime {
		return true
	}

	return a.BinaryExpression.IsNullable()
}

// Type returns the greatest type for given operation.
func (a *Arithmetic) Type() sql.Type {
	//TODO: what if both BindVars? should be constant folded
	rTyp := a.Right.Type()
	if sql.IsDeferredType(rTyp) {
		return rTyp
	}
	lTyp := a.Left.Type()
	if sql.IsDeferredType(lTyp) {
		return lTyp
	}

	switch strings.ToLower(a.Op) {
	case sqlparser.PlusStr, sqlparser.MinusStr, sqlparser.MultStr:
		if isInterval(a.Left) || isInterval(a.Right) {
			return sql.Datetime
		}

		if sql.IsTime(lTyp) && sql.IsTime(rTyp) {
			return sql.Int64
		}

		if sql.IsInteger(lTyp) && sql.IsInteger(rTyp) {
			if sql.IsUnsigned(lTyp) && sql.IsUnsigned(rTyp) {
				return sql.Uint64
			}
			return sql.Int64
		}

		return a.getArithmeticTypeFromExpr(lTyp, rTyp)

	case sqlparser.DivStr:
		if isInterval(a.Left) || isInterval(a.Right) {
			return sql.Datetime
		}

		if sql.IsTime(lTyp) && sql.IsTime(rTyp) {
			return sql.Int64
		}
		return a.floatOrDecimal(lTyp, rTyp)

	case sqlparser.ShiftLeftStr, sqlparser.ShiftRightStr:
		return sql.Uint64

	case sqlparser.BitAndStr, sqlparser.BitOrStr, sqlparser.BitXorStr, sqlparser.IntDivStr, sqlparser.ModStr:
		if sql.IsUnsigned(lTyp) && sql.IsUnsigned(rTyp) {
			return sql.Uint64
		}
		return sql.Int64
	}

	return a.getArithmeticTypeFromExpr(lTyp, rTyp)
}

func (a *Arithmetic) floatOrDecimal(lType, rType sql.Type) sql.Type {
	var resType sql.Type
	sql.Inspect(a, func(expr sql.Expression) bool {
		switch c := expr.(type) {
		case *GetField:
			if sql.IsFloat(c.Type()) {
				resType = sql.Float64
				return false
			}
		}
		return true
	})

	if resType == sql.Float64 {
		return resType
	}

	defType, derr := sql.CreateDecimalType(65, uint8(a.DivScale*4))
	if derr != nil {
		return sql.Float64
	}

	if a.lval != nil && a.rval != nil {
		lp, ls := getPrecisionAndScale(a.lval)
		rp, rs := getPrecisionAndScale(a.rval)
		maxp := uint8(math.Max(float64(lp), float64(rp)))
		maxs := uint8(math.Max(float64(ls), float64(rs)))
		r, err := sql.CreateDecimalType(maxp+maxs, maxs)
		if err == nil {
			return r
		}
	} else if a.lval == nil {
		p, s := getPrecisionAndScale(a.rval)
		r, err := sql.CreateDecimalType(uint8(p), uint8(s))
		if err == nil {
			return r
		}
	} else if a.rval == nil {
		p, s := getPrecisionAndScale(a.lval)
		r, err := sql.CreateDecimalType(uint8(p), uint8(s))
		if err == nil {
			return r
		}
	}

	if sql.IsDecimal(lType) && sql.IsDecimal(rType) {
		lp := lType.(sql.DecimalType).Precision()
		ls := lType.(sql.DecimalType).Scale()
		rp := rType.(sql.DecimalType).Precision()
		rs := lType.(sql.DecimalType).Scale()
		maxp := uint8(math.Max(float64(lp), float64(rp)))
		maxs := uint8(math.Max(float64(ls), float64(rs)))
		r, err := sql.CreateDecimalType(maxp+maxs, maxs)
		if err == nil {
			return r
		}
	} else if sql.IsDecimal(lType) {
		return lType
	} else if sql.IsDecimal(rType) {
		return rType
	}

	return defType
}

func getPrecisionAndScale(val interface{}) (int, int) {
	var str string
	switch v := val.(type) {
	case decimal.Decimal:
		str = v.StringFixed(v.Exponent() * -1)
	case float32:
		d := decimal.NewFromFloat32(v)
		str = d.StringFixed(d.Exponent() * -1)
	case float64:
		d := decimal.NewFromFloat(v)
		str = d.StringFixed(d.Exponent() * -1)
	default:
		str = fmt.Sprintf("%v", val)
	}
	ps := strings.Split(str, ".")
	p := len(ps[0])
	s := 0
	if len(ps) == 2 {
		s = len(ps[1])
	}
	return p, s
}

// getArithmeticTypeFromExpr returns a type that left and right values to be converted into.
// If there is system variable, return type should be the type of that system variable.
// For any non-DECIMAL column type, it will use default sql.Float64 type.
// For DECIMAL column type, or any Literal values, the return type will the DECIMAL type with
// the highest precision and scale calculated out of all Literals and DECIMAL column type definition.
func (a *Arithmetic) getArithmeticTypeFromExpr(lTyp, rTyp sql.Type) sql.Type {
	var resType sql.Type
	var precision uint8
	var scale uint8
	sql.Inspect(a, func(expr sql.Expression) bool {
		switch c := expr.(type) {
		case *SystemVar:
			resType = c.Type()
			return false
		case *GetField:
			if sql.IsDecimal(resType) {
				resType = c.Type()
				dt, _ := resType.(sql.DecimalType)
				if dt.Precision() > (precision) {
					precision = dt.Precision()
				}
				if dt.Scale() > scale {
					scale = dt.Precision()
				}
			} else {
				resType = sql.Float64
			}
		case *Literal:
			val, err := c.Eval(nil, nil)
			if err != nil {
				return false
			}
			var v string
			switch val.(type) {
			case float64:
				v = fmt.Sprintf("%f", val)
			default:
				v = fmt.Sprintf("%v", val)
			}
			p, s := GetDecimalPrecisionAndScale(v)
			if p > precision {
				precision = p
			}
			if s > scale {
				scale = s
			}
		}
		return true
	})

	if sql.IsDecimal(resType) {
		r, err := sql.CreateDecimalType(precision, scale)
		if err == nil {
			resType = r
		}
	} else if resType == nil {
		return a.floatOrDecimal(lTyp, rTyp)
	}

	return resType
}

// GetDecimalPrecisionAndScale returns precision and scale for given string formatted float/double number.
func GetDecimalPrecisionAndScale(val string) (uint8, uint8) {
	scale := 0
	precScale := strings.Split(strings.TrimPrefix(val, "-"), ".")
	if len(precScale) != 1 {
		scale = len(precScale[1])
	}
	precision := len((precScale)[0]) + scale
	return uint8(precision), uint8(scale)
}

func isInterval(expr sql.Expression) bool {
	_, ok := expr.(*Interval)
	return ok
}

// WithChildren implements the Expression interface.
func (a *Arithmetic) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 2 {
		return nil, sql.ErrInvalidChildrenNumber.New(a, len(children), 2)
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

	a.lval = lval
	a.rval = rval

	lval, rval, err = a.convertLeftRight(lval, rval)
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
	case sqlparser.DivStr:
		return div(lval, rval, a.DivScale)
	case sqlparser.BitAndStr:
		return bitAnd(lval, rval)
	case sqlparser.BitOrStr:
		return bitOr(lval, rval)
	case sqlparser.BitXorStr:
		return bitXor(lval, rval)
	case sqlparser.ShiftLeftStr:
		return shiftLeft(lval, rval)
	case sqlparser.ShiftRightStr:
		return shiftRight(lval, rval)
	case sqlparser.IntDivStr:
		return intDiv(lval, rval)
	case sqlparser.ModStr:
		return mod(lval, rval)
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

func (a *Arithmetic) convertLeftRight(left interface{}, right interface{}) (interface{}, interface{}, error) {
	var err error

	// type needs to be found in better way...
	typ := a.Type()

	if i, ok := left.(*TimeDelta); ok {
		left = i
	} else {
		left, err = typ.Convert(left)
		if err != nil {
			return nil, nil, err
		}
	}

	if i, ok := right.(*TimeDelta); ok {
		right = i
	} else {
		right, err = typ.Convert(right)
		if err != nil {
			return nil, nil, err
		}
	}

	return left, right, nil
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
			return sql.ValidateTime(r.Add(l)), nil
		case time.Time:
			return l.Unix() + r.Unix(), nil
		}
	case *TimeDelta:
		switch r := rval.(type) {
		case time.Time:
			return sql.ValidateTime(l.Add(r)), nil
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
			return sql.ValidateTime(r.Sub(l)), nil
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
	case decimal.Decimal:
		switch r := rval.(type) {
		case decimal.Decimal:
			return l.Mul(r), nil
		}
	}

	return nil, errUnableToCast.New(lval, rval)
}

func div(lval, rval interface{}, divScale int32) (interface{}, error) {
	switch l := lval.(type) {
	case uint8:
		switch r := rval.(type) {
		case uint8:
			if r == 0 {
				return nil, nil
			}
			return l / r, nil
		}
	case int8:
		switch r := rval.(type) {
		case int8:
			if r == 0 {
				return nil, nil
			}
			return l / r, nil
		}
	case uint16:
		switch r := rval.(type) {
		case uint16:
			if r == 0 {
				return nil, nil
			}
			return l / r, nil
		}
	case int16:
		switch r := rval.(type) {
		case int16:
			if r == 0 {
				return nil, nil
			}
			return l / r, nil
		}
	case uint32:
		switch r := rval.(type) {
		case uint32:
			if r == 0 {
				return nil, nil
			}
			return l / r, nil
		}
	case int32:
		switch r := rval.(type) {
		case int32:
			if r == 0 {
				return nil, nil
			}
			return l / r, nil
		}
	case uint64:
		switch r := rval.(type) {
		case uint64:
			if r == 0 {
				return nil, nil
			}
			return l / r, nil
		}
	case int64:
		switch r := rval.(type) {
		case int64:
			if r == 0 {
				return nil, nil
			}
			return l / r, nil
		}
	case float32:
		switch r := rval.(type) {
		case float32:
			if r == 0 {
				return nil, nil
			}
			return l / r, nil
		}
	case float64:
		switch r := rval.(type) {
		case float64:
			if r == 0 {
				return nil, nil
			}
			return l / r, nil
		}
	case decimal.Decimal:
		switch r := rval.(type) {
		case decimal.Decimal:
			if r.String() == "0" {
				return nil, nil
			}
			exp := (l.Exponent() * -1) + divScale*4
			return l.DivRound(r, exp), nil
		}
	}

	return nil, errUnableToCast.New(lval, rval)
}

func bitAnd(lval, rval interface{}) (interface{}, error) {
	switch l := lval.(type) {
	case uint64:
		switch r := rval.(type) {
		case uint64:
			return l & r, nil
		}

	case int64:
		switch r := rval.(type) {
		case int64:
			return l & r, nil
		}
	}

	return nil, errUnableToCast.New(lval, rval)
}

func bitOr(lval, rval interface{}) (interface{}, error) {
	switch l := lval.(type) {
	case uint64:
		switch r := rval.(type) {
		case uint64:
			return l | r, nil
		}

	case int64:
		switch r := rval.(type) {
		case int64:
			return l | r, nil
		}
	}

	return nil, errUnableToCast.New(lval, rval)
}

func bitXor(lval, rval interface{}) (interface{}, error) {
	switch l := lval.(type) {
	case uint64:
		switch r := rval.(type) {
		case uint64:
			return l ^ r, nil
		}

	case int64:
		switch r := rval.(type) {
		case int64:
			return l ^ r, nil
		}
	}

	return nil, errUnableToCast.New(lval, rval)
}

func shiftLeft(lval, rval interface{}) (interface{}, error) {
	switch l := lval.(type) {
	case uint64:
		switch r := rval.(type) {
		case uint64:
			return l << r, nil
		}
	}

	return nil, errUnableToCast.New(lval, rval)
}

func shiftRight(lval, rval interface{}) (interface{}, error) {
	switch l := lval.(type) {
	case uint64:
		switch r := rval.(type) {
		case uint64:
			return l >> r, nil
		}
	}

	return nil, errUnableToCast.New(lval, rval)
}

func intDiv(lval, rval interface{}) (interface{}, error) {
	switch l := lval.(type) {
	case uint64:
		switch r := rval.(type) {
		case uint64:
			if r == 0 {
				return nil, nil
			}
			return uint64(l / r), nil
		}

	case int64:
		switch r := rval.(type) {
		case int64:
			if r == 0 {
				return nil, nil
			}
			return int64(l / r), nil
		}
	}

	return nil, errUnableToCast.New(lval, rval)
}

func mod(lval, rval interface{}) (interface{}, error) {
	switch l := lval.(type) {
	case uint64:
		switch r := rval.(type) {
		case uint64:
			if r == 0 {
				return nil, nil
			}
			return l % r, nil
		}

	case int64:
		switch r := rval.(type) {
		case int64:
			if r == 0 {
				return nil, nil
			}
			return l % r, nil
		}
	}

	return nil, errUnableToCast.New(lval, rval)
}

// UnaryMinus is an unary minus operator.
type UnaryMinus struct {
	UnaryExpression
}

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

	if !sql.IsNumber(e.Child.Type()) {
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
	default:
		return nil, sql.ErrInvalidType.New(reflect.TypeOf(n))
	}
}

// Type implements the sql.Expression interface.
func (e *UnaryMinus) Type() sql.Type {
	typ := e.Child.Type()
	if !sql.IsNumber(typ) {
		return sql.Float64
	}

	if typ == sql.Uint32 {
		return sql.Int32
	}

	if typ == sql.Uint64 {
		return sql.Int64
	}

	return e.Child.Type()
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
