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
	"strings"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/shopspring/decimal"
)

// '4 scales' are added to scale of the number on the left side of division operator at every division operation.
// The default value is 4, and it can be set using sysvar https://dev.mysql.com/doc/refman/8.0/en/server-system-variables.html#sysvar_div_precision_increment
const divPrecisionIncrement = 4

// '9 scales' are added for every non-integer divider(right side).
const divIntermediatePrecisionInc = 9

// Div expression (/)
type Div struct {
	BinaryExpression

	// divScale is number of continuous division operations; this value will be available of all layers
	divScale int32
	// leftmostScale is a length of scale of the leftmost value in continuous division operation
	leftmostScale               int32
	curIntermediatePrecisionInc int
}

// NewDiv creates a new Div / sql.Expression.
func NewDiv(left, right sql.Expression) *Div {
	a := &Div{BinaryExpression{Left: left, Right: right}, 0, 0, 0}
	divs := countDivs(a)
	setDivs(a, divs)
	return a
}

func (d *Div) String() string {
	return fmt.Sprintf("(%s / %s)", d.Left, d.Right)
}

func (d *Div) DebugString() string {
	return fmt.Sprintf("(%s / %s)", sql.DebugString(d.Left), sql.DebugString(d.Right))
}

// IsNullable implements the sql.Expression interface.
func (d *Div) IsNullable() bool {
	if d.Type() == sql.Timestamp || d.Type() == sql.Datetime {
		return true
	}

	return d.BinaryExpression.IsNullable()
}

// Type returns the greatest type for given operation.
func (d *Div) Type() sql.Type {
	//TODO: what if both BindVars? should be constant folded
	rTyp := d.Right.Type()
	if sql.IsDeferredType(rTyp) {
		return rTyp
	}
	lTyp := d.Left.Type()
	if sql.IsDeferredType(lTyp) {
		return lTyp
	}

	if isInterval(d.Left) || isInterval(d.Right) {
		return sql.Datetime
	}

	if sql.IsTime(lTyp) && sql.IsTime(rTyp) {
		return sql.Int64
	}

	// for division operation, it's either float or decimal.Decimal type
	// except invalid value will result it either 0 or nil
	return d.floatOrDecimal()
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

	lval, rval, err = d.convertLeftRight(lval, rval)
	if err != nil {
		return nil, err
	}

	result, err := d.div(lval, rval)
	if err != nil {
		return nil, err
	}

	// we do not round the value until it's the last division operation.
	if isOutermostDiv(d, 0, d.divScale) {
		if res, ok := result.(decimal.Decimal); ok {
			finalScale := d.divScale*int32(divPrecisionIncrement) + d.leftmostScale
			if finalScale > sql.DecimalTypeMaxScale {
				finalScale = sql.DecimalTypeMaxScale
			}
			return res.Round(finalScale), nil
		}
	}

	return result, nil

}

func (d *Div) evalLeftRight(ctx *sql.Context, row sql.Row) (interface{}, interface{}, error) {
	var lval, rval interface{}
	var err error

	if i, ok := d.Left.(*Interval); ok {
		lval, err = i.EvalDelta(ctx, row)
		if err != nil {
			return nil, nil, err
		}
	} else {
		lval, err = d.Left.Eval(ctx, row)
		if err != nil {
			return nil, nil, err
		}
	}

	if i, ok := d.Right.(*Interval); ok {
		rval, err = i.EvalDelta(ctx, row)
		if err != nil {
			return nil, nil, err
		}
	} else {
		rval, err = d.Right.Eval(ctx, row)
		if err != nil {
			return nil, nil, err
		}
	}

	return lval, rval, nil
}

// convertLeftRight return most appropriate value for left and right from evaluated value,
// which can might or might not be converted from its original value.
// It checks for float type column reference, then the both values converted to the same float types.
// If there is no float type column reference, both values should be handled as decimal type
// The decimal types of left and right value does NOT need to be the same. Both the types
// should be preserved.
func (d *Div) convertLeftRight(left interface{}, right interface{}) (interface{}, interface{}, error) {
	var err error

	typ := d.Type()

	if i, ok := left.(*TimeDelta); ok {
		left = i
	} else {
		if sql.IsFloat(typ) {
			left, err = typ.Convert(left)
			if err != nil {
				// TODO : any error here from converting should be added as warning
				// the value is interpreted as 0, but we need to match the type of the other valid value
				// to avoid additional conversion, the nil value is handled in each operation
				left = nil
			}
		} else {
			if _, ok := left.(decimal.Decimal); !ok {
				p, s := getPrecisionAndScale(left)
				ltyp, err := sql.CreateDecimalType(p, s)
				if err != nil {
					left = nil
				}
				left, err = ltyp.Convert(left)
				if err != nil {
					left = nil
				}
			}
		}
	}

	if i, ok := right.(*TimeDelta); ok {
		right = i
	} else {
		if sql.IsFloat(typ) {
			right, err = typ.Convert(right)
			if err != nil {
				// TODO : any error here from converting should be added as warning
				// the value is interpreted as 0, but we need to match the type of the other valid value
				// to avoid additional conversion, the nil value is handled in each operation
				right = nil
			}
		} else {
			if _, ok := right.(decimal.Decimal); !ok {
				p, s := getPrecisionAndScale(right)
				rtyp, err := sql.CreateDecimalType(p, s)
				if err != nil {
					right = nil
				}
				right, err = rtyp.Convert(right)
				if err != nil {
					right = nil
				}
			}
		}
	}

	return left, right, nil
}

func (d *Div) div(lval, rval interface{}) (interface{}, error) {
	if rval == nil {
		return nil, nil
	}
	if lval == nil {
		return 0, nil
	}

	switch l := lval.(type) {
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

			storedScale := int32(d.curIntermediatePrecisionInc * divIntermediatePrecisionInc)
			l = l.Truncate(storedScale)
			r = r.Truncate(storedScale)

			// give it buffer of 2 additional scale to avoid the result to be rounded
			divRes := l.DivRound(r, storedScale+2)
			return divRes.Truncate(storedScale), nil
		}
	}

	return nil, errUnableToCast.New(lval, rval)
}

// floatOrDecimal returns either Float64 or decimaltype depending on column reference,
// left and right expressions types and left and right evaluated types.
// If there is float type column reference, the result type is always float
// regardless of the column reference on the left or right side of division operation.
// Otherwise, the return type is always decimal. The expression and evaluated types
// are used to determine appropriate decimaltype to return that will not result in
// precision loss.
func (d *Div) floatOrDecimal() sql.Type {
	var resType sql.Type
	sql.Inspect(d, func(expr sql.Expression) bool {
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

	// using max precision which is 65 and DivScale for scale number.
	// DivScale will be non-zero number if it is the innermost division operation.
	defType, derr := sql.CreateDecimalType(65, 30)
	if derr != nil {
		return sql.Float64
	}

	return defType
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

	return 0
}

// setDivs will set the innermost node's DivScale to the number counted by countDivs, and the rest of it
// to 0. This allows us to calculate the first division with the exact precision of the end result. Otherwise,
// we lose precision at each division since we only add 4 scales at every division operation.
func setDivs(e sql.Expression, dScale int32) {
	if e == nil {
		return
	}

	if a, ok := e.(*Div); ok {
		a.divScale = dScale
		setDivs(a.Left, dScale)
		setDivs(a.Right, dScale)
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
			_, s := getPrecisionAndScale(lval)
			return int32(s)
		} else {
			return getScaleOfLeftmostValue(ctx, row, a.Left, d, dScale)
		}
	}

	return 0
}

// isOutermostDiv return whether the expression we're currently on is
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

// getPrecisionAndScale converts the value to string format and parses it to get the precision and scale.
func getPrecisionAndScale(val interface{}) (uint8, uint8) {
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
	} else {
		return cur
	}
}
