// Copyright 2022 Dolthub, Inc.
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
	"strconv"
	"strings"
	"time"

	"github.com/dolthub/vitess/go/vt/sqlparser"
	"github.com/shopspring/decimal"
	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

var ErrIntDivDataOutOfRange = errors.NewKind("BIGINT value is out of range (%s DIV %s)")

// '4 scales' are added to scale of the number on the left side of division operator at every division operation.
// The default value is 4, and it can be set using sysvar https://dev.mysql.com/doc/refman/8.0/en/server-system-variables.html#sysvar_div_precision_increment
const divPrecisionIncrement = 4

// '9 scales' are added for every non-integer divider(right side).
const divIntermediatePrecisionInc = 9

const ERDivisionByZero = 1365

var _ ArithmeticOp = (*Div)(nil)
var _ sql.CollationCoercible = (*Div)(nil)

// Div expression represents "/" arithmetic operation
type Div struct {
	BinaryExpression
	ops int32
	// divScale is number of continuous division operations; this value will be available of all layers
	divScale int32
	// leftmostScale is a length of scale of the leftmost value in continuous division operation
	leftmostScale               int32
	curIntermediatePrecisionInc int
}

// NewDiv creates a new Div / sql.Expression.
func NewDiv(left, right sql.Expression) *Div {
	a := &Div{BinaryExpression{Left: left, Right: right}, 0, 0, 0, 0}
	divs := countDivs(a)
	setDivs(a, divs)
	ops := countArithmeticOps(a)
	setArithmeticOps(a, ops)
	return a
}

func (d *Div) LeftChild() sql.Expression {
	return d.Left
}

func (d *Div) RightChild() sql.Expression {
	return d.Right
}

func (d *Div) Operator() string {
	return sqlparser.DivStr
}

func (d *Div) SetOpCount(i int32) {
	d.ops = i
}

func (d *Div) String() string {
	return fmt.Sprintf("(%s / %s)", d.Left, d.Right)
}

func (d *Div) DebugString() string {
	return fmt.Sprintf("(%s / %s)", sql.DebugString(d.Left), sql.DebugString(d.Right))
}

// IsNullable implements the sql.Expression interface.
func (d *Div) IsNullable() bool {
	return d.BinaryExpression.IsNullable()
}

// Type returns the greatest type for given operation.
func (d *Div) Type() sql.Type {
	//TODO: what if both BindVars? should be constant folded
	rTyp := d.Right.Type()
	if types.IsDeferredType(rTyp) {
		return rTyp
	}
	lTyp := d.Left.Type()
	if types.IsDeferredType(lTyp) {
		return lTyp
	}

	if types.IsText(lTyp) || types.IsText(rTyp) {
		return types.Float64
	}

	// For division operations, the result type is always either a float or decimal.Decimal. When working with
	// integers, we prefer float types internally, since the performance is orders of magnitude faster to divide
	// floats than to divide Decimals, but if this is the outermost division operation, we need to
	// return a decimal in order to match MySQL's results exactly.
	if isOutermostDiv(d, 0, d.divScale) {
		return floatOrDecimalType(d, false)
	} else {
		return floatOrDecimalType(d, true)
	}
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*Div) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 5
}

// WithChildren implements the Expression interface.
func (d *Div) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 2 {
		return nil, sql.ErrInvalidChildrenNumber.New(d, len(children), 2)
	}
	return NewDiv(children[0], children[1]), nil
}

// Eval implements the Expression interface.
func (d *Div) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	// we need to get the scale of the leftmost value of all continuous division
	// for the final result rounding precision. This only is able to happens in the
	// outermost layer, which is where we use this value to round the final result.
	// we do not round the value until it's the last division operation.
	if isOutermostDiv(d, 0, d.divScale) {
		d.leftmostScale = getScaleOfLeftmostValue(ctx, row, d, 0, d.divScale)
	}

	lval, rval, err := d.evalLeftRight(ctx, row)
	if err != nil {
		return nil, err
	}

	if lval == nil || rval == nil {
		return nil, nil
	}

	lval, rval = d.convertLeftRight(ctx, lval, rval)

	result, err := d.div(ctx, lval, rval)
	if err != nil {
		return nil, err
	}

	// we do not round the value until it's the last division operation.
	if isOutermostDiv(d, 0, d.divScale) {
		// We prefer using floats internally for division operations, but if this expressions output type
		// is a Decimal, make sure we convert the result and return it as a decimal.
		if types.IsDecimal(d.Type()) {
			result = convertValueToType(ctx, types.InternalDecimalType, result, false)
		}

		if res, ok := result.(decimal.Decimal); ok {
			finalScale := d.divScale*int32(divPrecisionIncrement) + d.leftmostScale
			if finalScale > types.DecimalTypeMaxScale {
				finalScale = types.DecimalTypeMaxScale
			}
			if isOutermostArithmeticOp(d, 0, d.ops) {
				return res.Round(finalScale), nil
			}
			// TODO : need to pass finalScale if this div is the last div but not the last arithmetic op
		}
	}

	return result, nil
}

func (d *Div) evalLeftRight(ctx *sql.Context, row sql.Row) (interface{}, interface{}, error) {
	var lval, rval interface{}
	var err error

	// division used with Interval error is caught at parsing the query
	lval, err = d.Left.Eval(ctx, row)
	if err != nil {
		return nil, nil, err
	}

	// this operation is only done on the left value as the scale/fraction part of the leftmost value
	// is used to calculate the scale of the final result. If the value is GetField of decimal type column
	// the decimal value evaluated does not always match the scale of column type definition
	if dt, ok := d.Left.Type().(sql.DecimalType); ok {
		if dVal, ok := lval.(decimal.Decimal); ok {
			ts := int32(dt.Scale())
			if ts > dVal.Exponent()*-1 {
				lval, err = decimal.NewFromString(dVal.StringFixed(ts))
				if err != nil {
					return nil, nil, err
				}
			}
		}
	}

	rval, err = d.Right.Eval(ctx, row)
	if err != nil {
		return nil, nil, err
	}

	return lval, rval, nil
}

// convertLeftRight returns the most appropriate type for left and right evaluated values,
// which may or may not be converted from its original type.
// It checks for float type column reference, then the both values converted to the same float types.
// If there is no float type column reference, both values should be handled as decimal type
// The decimal types of left and right value does NOT need to be the same. Both the types
// should be preserved.
func (d *Div) convertLeftRight(ctx *sql.Context, left interface{}, right interface{}) (interface{}, interface{}) {
	typ := d.Type()
	lIsTimeType := types.IsTime(d.Left.Type())
	rIsTimeType := types.IsTime(d.Right.Type())

	if types.IsFloat(typ) {
		left = convertValueToType(ctx, typ, left, lIsTimeType)
	} else {
		left = convertToDecimalValue(left, lIsTimeType)
	}

	if types.IsFloat(typ) {
		right = convertValueToType(ctx, typ, right, rIsTimeType)
	} else {
		right = convertToDecimalValue(right, rIsTimeType)
	}

	return left, right
}

func (d *Div) div(ctx *sql.Context, lval, rval interface{}) (interface{}, error) {
	switch l := lval.(type) {
	case float32:
		switch r := rval.(type) {
		case float32:
			if r == 0 {
				arithmeticWarning(ctx, ERDivisionByZero, fmt.Sprintf("Division by 0"))
				return nil, nil
			}
			return l / r, nil
		}
	case float64:
		switch r := rval.(type) {
		case float64:
			if r == 0 {
				arithmeticWarning(ctx, ERDivisionByZero, fmt.Sprintf("Division by 0"))
				return nil, nil
			}
			return l / r, nil
		}
	case decimal.Decimal:
		switch r := rval.(type) {
		case decimal.Decimal:
			if r.Equal(decimal.NewFromInt(0)) {
				arithmeticWarning(ctx, ERDivisionByZero, fmt.Sprintf("Division by 0"))
				return nil, nil
			}

			if d.curIntermediatePrecisionInc == 0 {
				d.curIntermediatePrecisionInc = getPrecInc(d, 0)
				// if the first dividend / the leftmost value is non int value,
				// then curIntermediatePrecisionInc gets additional increment per every 9 scales
				if d.curIntermediatePrecisionInc == 0 {
					if !isIntOr1(l) {
						d.curIntermediatePrecisionInc = int(math.Ceil(float64(l.Exponent()*-1) / float64(divIntermediatePrecisionInc)))
					}
				}
			}

			// for every divider we increment the curIntermediatePrecisionInc per every 9 scales
			// for 0 scaled number, we increment 1
			if r.Exponent() == 0 {
				d.curIntermediatePrecisionInc += 1
			} else {
				d.curIntermediatePrecisionInc += int(math.Ceil(float64(r.Exponent()*-1) / float64(divIntermediatePrecisionInc)))
			}

			storedScale := d.leftmostScale + int32(d.curIntermediatePrecisionInc*divIntermediatePrecisionInc)
			l = l.Truncate(storedScale)
			r = r.Truncate(storedScale)

			// give it buffer of 2 additional scale to avoid the result to be rounded
			divRes := l.DivRound(r, storedScale+2)
			return divRes.Truncate(storedScale), nil
		}
	}

	return nil, errUnableToCast.New(lval, rval)
}

// floatOrDecimalType returns either Float64 or Decimal type depending on column reference,
// left and right expression types and left and right evaluated types.
// If there is float type column reference, the result type is always float
// regardless of the column reference on the left or right side of division operation.
// If |treatIntsAsFloats| is true, then integers are treated as floats instead of Decimals. This
// is a performance optimization for division operations, since float division can be several orders
// of magnitude faster than division with Decimals.
// Otherwise, the return type is always decimal. The expression and evaluated types
// are used to determine appropriate Decimal type to return that will not result in
// precision loss.
func floatOrDecimalType(e sql.Expression, treatIntsAsFloats bool) sql.Type {
	var resType sql.Type
	var decType sql.Type
	var maxWhole, maxFrac uint8
	sql.Inspect(e, func(expr sql.Expression) bool {
		switch c := expr.(type) {
		case *GetField:
			if treatIntsAsFloats && types.IsInteger(c.Type()) {
				resType = types.Float64
				return false
			}
			if types.IsFloat(c.Type()) {
				resType = types.Float64
				return false
			}
			if types.IsDecimal(c.Type()) {
				decType = c.Type()
			}
		case *Literal:
			if types.IsNumber(c.Type()) {
				l, err := c.Eval(nil, nil)
				if err == nil {
					p, s := GetPrecisionAndScale(l)
					if cw := p - s; cw > maxWhole {
						maxWhole = cw
					}
					if s > maxFrac {
						maxFrac = s
					}
				}
			}
		}
		return true
	})

	if resType == types.Float64 {
		return resType
	}

	if decType != nil {
		return decType
	}

	// defType is defined by evaluating all number literals available
	defType, derr := types.CreateDecimalType(maxWhole+maxFrac, maxFrac)
	if derr != nil {
		return types.MustCreateDecimalType(65, 10)
	}

	return defType
}

// convertToDecimalValue returns value converted to decimaltype.
// If the value is invalid, it returns decimal 0. This function
// is used for 'div' or 'mod' arithmetic operation, which requires
// the result value to have precise precision and scale.
func convertToDecimalValue(val interface{}, isTimeType bool) interface{} {
	if isTimeType {
		val = convertTimeTypeToString(val)
	}

	if _, ok := val.(decimal.Decimal); !ok {
		p, s := GetPrecisionAndScale(val)
		dtyp, err := types.CreateDecimalType(p, s)
		if err != nil {
			val = decimal.Zero
		}
		val, _, err = dtyp.Convert(val)
		if err != nil {
			val = decimal.Zero
		}
	}

	return val
}

// countDivs returns the number of division operators in order on the left child node of the current node.
// This lets us count how many division operator used one after the other. E.g. 24/3/2/1 will have this structure:
//
//		     'div'
//		     /   \
//		   'div'  1
//		   /   \
//		 'div'  2
//		 /   \
//	    24    3
func countDivs(e sql.Expression) int32 {
	if e == nil {
		return 0
	}

	if a, ok := e.(*Div); ok {
		return countDivs(a.Left) + 1
	}

	if a, ok := e.(ArithmeticOp); ok {
		return countDivs(a.LeftChild())
	}

	return 0
}

// setDivs will set each node's DivScale to the number counted by countDivs. This allows us to
// keep track of whether the current Div expression is the last Div operation, so the result is
// rounded appropriately.
func setDivs(e sql.Expression, dScale int32) {
	if e == nil {
		return
	}

	if a, ok := e.(*Div); ok {
		a.divScale = dScale
		setDivs(a.Left, dScale)
		setDivs(a.Right, dScale)
	}

	if a, ok := e.(ArithmeticOp); ok {
		setDivs(a.LeftChild(), dScale)
		setDivs(a.RightChild(), dScale)
	}

	return
}

// getScaleOfLeftmostValue find the leftmost/first value of all continuous divisions.
// E.g. 24/50/3.2/2/1 will return 2 for len('50') of number '24.50'.
func getScaleOfLeftmostValue(ctx *sql.Context, row sql.Row, e sql.Expression, d, dScale int32) int32 {
	if e == nil {
		return 0
	}

	if a, ok := e.(*Div); ok {
		d = d + 1
		if d == dScale {
			lval, err := a.Left.Eval(ctx, row)
			if err != nil {
				return 0
			}
			_, s := GetPrecisionAndScale(lval)
			// the leftmost value can be row value of decimal type column
			// the evaluated value does not always match the scale of column type definition
			typ := a.Left.Type()
			if dt, dok := typ.(sql.DecimalType); dok {
				ts := dt.Scale()
				if ts > s {
					s = ts
				}
			}
			return int32(s)
		} else {
			return getScaleOfLeftmostValue(ctx, row, a.Left, d, dScale)
		}
	}

	return 0
}

// isOutermostDiv returns whether the expression we're currently evaluating is
// the last division operation of all continuous divisions.
// E.g. the top 'div' (divided by 1) is the outermost/last division that is calculated:
//
//		     'div'
//		     /   \
//		   'div'  1
//		   /   \
//		 'div'  2
//		 /   \
//	    24    3
func isOutermostDiv(e sql.Expression, d, dScale int32) bool {
	if e == nil {
		return false
	}

	if a, ok := e.(*Div); ok {
		d = d + 1
		if d == dScale {
			return true
		} else {
			return isOutermostDiv(a.Left, d, dScale)
		}
	} else if a, ok := e.(ArithmeticOp); ok {
		return isOutermostDiv(a.LeftChild(), d, dScale)
	}

	return false
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

// GetPrecisionAndScale converts the value to string format and parses it to get the precision and scale.
func GetPrecisionAndScale(val interface{}) (uint8, uint8) {
	var str string
	switch v := val.(type) {
	case time.Time:
		str = fmt.Sprintf("%v", v.In(time.UTC).Format("2006-01-02 15:04:05"))
	case decimal.Decimal:
		str = v.StringFixed(v.Exponent() * -1)
	case float32:
		d := decimal.NewFromFloat32(v)
		str = d.StringFixed(d.Exponent() * -1)
	case float64:
		d := decimal.NewFromFloat(v)
		str = d.StringFixed(d.Exponent() * -1)
	default:
		str = fmt.Sprintf("%v", v)
	}
	return GetDecimalPrecisionAndScale(str)
}

// isIntOr1 checks whether the decimal number is equal to 1
// or it is an integer value even though the value can be
// given as decimal. This function returns true if val is
// 1 or 1.000 or 2.00 or 13. These all are int numbers.
func isIntOr1(val decimal.Decimal) bool {
	if val.Equal(decimal.NewFromInt(1)) {
		return true
	}
	if val.Equal(decimal.NewFromInt(-1)) {
		return true
	}
	if val.Equal(decimal.NewFromInt(val.IntPart())) {
		return true
	}
	return false
}

// getPrecInc returns the max curIntermediatePrecisionInc by searching the children
// of the expression given. This allows us to keep track of the appropriate value
// of curIntermediatePrecisionInc that is used to storing scale number for the decimal value.
func getPrecInc(e sql.Expression, cur int) int {
	if e == nil {
		return 0
	}

	if d, ok := e.(*Div); ok {
		if d.curIntermediatePrecisionInc > cur {
			return d.curIntermediatePrecisionInc
		}
		l := getPrecInc(d.Left, cur)
		if l > cur {
			cur = l
		}
		r := getPrecInc(d.Right, cur)
		if r > cur {
			cur = r
		}
		return cur
	} else if d, ok := e.(ArithmeticOp); ok {
		l := getPrecInc(d.LeftChild(), cur)
		if l > cur {
			cur = l
		}
		r := getPrecInc(d.RightChild(), cur)
		if r > cur {
			cur = r
		}
		return cur
	} else {
		return cur
	}
}

var _ ArithmeticOp = (*IntDiv)(nil)
var _ sql.CollationCoercible = (*IntDiv)(nil)

// IntDiv expression represents integer "div" arithmetic operation
type IntDiv struct {
	BinaryExpression
	ops int32
}

// NewIntDiv creates a new IntDiv 'div' sql.Expression.
func NewIntDiv(left, right sql.Expression) *IntDiv {
	a := &IntDiv{BinaryExpression{Left: left, Right: right}, 0}
	ops := countArithmeticOps(a)
	setArithmeticOps(a, ops)
	return a
}

func (i *IntDiv) LeftChild() sql.Expression {
	return i.Left
}

func (i *IntDiv) RightChild() sql.Expression {
	return i.Right
}

func (i *IntDiv) Operator() string {
	return sqlparser.IntDivStr
}

func (i *IntDiv) SetOpCount(i2 int32) {
	i.ops = i2
}

func (i *IntDiv) String() string {
	return fmt.Sprintf("(%s div %s)", i.Left, i.Right)
}

func (i *IntDiv) DebugString() string {
	return fmt.Sprintf("(%s div %s)", sql.DebugString(i.Left), sql.DebugString(i.Right))
}

// IsNullable implements the sql.Expression interface.
func (i *IntDiv) IsNullable() bool {
	return i.BinaryExpression.IsNullable()
}

// Type returns the greatest type for given operation.
func (i *IntDiv) Type() sql.Type {
	//TODO: what if both BindVars? should be constant folded
	rTyp := i.Right.Type()
	if types.IsDeferredType(rTyp) {
		return rTyp
	}
	lTyp := i.Left.Type()
	if types.IsDeferredType(lTyp) {
		return lTyp
	}

	if types.IsTime(lTyp) && types.IsTime(rTyp) {
		return types.Int64
	}

	if types.IsText(lTyp) || types.IsText(rTyp) {
		return types.Float64
	}

	if types.IsUnsigned(lTyp) && types.IsUnsigned(rTyp) {
		return types.Uint64
	} else if types.IsSigned(lTyp) && types.IsSigned(rTyp) {
		return types.Int64
	}

	// using max precision which is 65.
	defType := types.MustCreateDecimalType(65, 0)
	return defType
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*IntDiv) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 5
}

// WithChildren implements the Expression interface.
func (i *IntDiv) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 2 {
		return nil, sql.ErrInvalidChildrenNumber.New(i, len(children), 2)
	}
	return NewIntDiv(children[0], children[1]), nil
}

// Eval implements the Expression interface.
func (i *IntDiv) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	lval, rval, err := i.evalLeftRight(ctx, row)
	if err != nil {
		return nil, err
	}

	if lval == nil || rval == nil {
		return nil, nil
	}

	lval, rval = i.convertLeftRight(ctx, lval, rval)

	return intDiv(ctx, lval, rval)
}

func (i *IntDiv) evalLeftRight(ctx *sql.Context, row sql.Row) (interface{}, interface{}, error) {
	var lval, rval interface{}
	var err error

	// int division used with Interval error is caught at parsing the query
	lval, err = i.Left.Eval(ctx, row)
	if err != nil {
		return nil, nil, err
	}

	rval, err = i.Right.Eval(ctx, row)
	if err != nil {
		return nil, nil, err
	}

	return lval, rval, nil
}

// convertLeftRight return most appropriate value for left and right from evaluated value,
// which can might or might not be converted from its original value.
// It checks for float type column reference, then the both values converted to the same float types.
// If there is no float type column reference, both values should be handled as decimal type
// The decimal types of left and right value does NOT need to be the same. Both the types
// should be preserved.
func (i *IntDiv) convertLeftRight(ctx *sql.Context, left interface{}, right interface{}) (interface{}, interface{}) {
	typ := i.Type()
	lIsTimeType := types.IsTime(i.Left.Type())
	rIsTimeType := types.IsTime(i.Right.Type())

	if types.IsInteger(typ) || types.IsFloat(typ) {
		left = convertValueToType(ctx, typ, left, lIsTimeType)
	} else {
		left = convertToDecimalValue(left, lIsTimeType)
	}

	if types.IsInteger(typ) || types.IsFloat(typ) {
		right = convertValueToType(ctx, typ, right, rIsTimeType)
	} else {
		right = convertToDecimalValue(right, rIsTimeType)
	}

	return left, right
}

func intDiv(ctx *sql.Context, lval, rval interface{}) (interface{}, error) {
	switch l := lval.(type) {
	case uint64:
		switch r := rval.(type) {
		case uint64:
			if r == 0 {
				arithmeticWarning(ctx, ERDivisionByZero, fmt.Sprintf("Division by 0"))
				return nil, nil
			}
			return l / r, nil
		}
	case int64:
		switch r := rval.(type) {
		case int64:
			if r == 0 {
				arithmeticWarning(ctx, ERDivisionByZero, fmt.Sprintf("Division by 0"))
				return nil, nil
			}
			return l / r, nil
		}
	case float64:
		switch r := rval.(type) {
		case float64:
			if r == 0 {
				arithmeticWarning(ctx, ERDivisionByZero, fmt.Sprintf("Division by 0"))
				return nil, nil
			}
			res := l / r
			return int64(math.Floor(res)), nil
		}
	case decimal.Decimal:
		switch r := rval.(type) {
		case decimal.Decimal:
			if r.Equal(decimal.NewFromInt(0)) {
				arithmeticWarning(ctx, ERDivisionByZero, fmt.Sprintf("Division by 0"))
				return nil, nil
			}

			// intDiv operation gets the integer part of the divided value without rounding the result with 0 precision
			// We get division result with non-zero precision and then truncate it to get integer part without it being rounded
			divRes := l.DivRound(r, 2).Truncate(0)

			// cannot use IntPart() function of decimal.Decimal package as it returns 0 as undefined value for out of range value
			// it causes valid result value of 0 to be the same as invalid out of range value of 0. The fraction part
			// should not be rounded, so truncate the result wih 0 precision.
			intPart, err := strconv.ParseInt(divRes.String(), 10, 64)
			if err != nil {
				return nil, ErrIntDivDataOutOfRange.New(l.StringFixed(l.Exponent()), r.StringFixed(r.Exponent()))
			}

			return intPart, nil
		}
	}

	return nil, errUnableToCast.New(lval, rval)
}
