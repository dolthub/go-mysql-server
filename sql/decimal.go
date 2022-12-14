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

package sql

import (
	"fmt"
	"math/big"
	"reflect"
	"strconv"

	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/dolthub/vitess/go/vt/proto/query"
	"github.com/shopspring/decimal"
	"gopkg.in/src-d/go-errors.v1"
)

const (
	// DecimalTypeMaxPrecision returns the maximum precision allowed for the Decimal type.
	DecimalTypeMaxPrecision = 65
	// DecimalTypeMaxScale returns the maximum scale allowed for the Decimal type, assuming the
	// maximum precision is used. For a maximum scale that is relative to the precision of a given
	// decimal type, use its MaximumScale function.
	DecimalTypeMaxScale = 30
)

var (
	ErrConvertingToDecimal   = errors.NewKind("value %v is not a valid Decimal")
	ErrConvertToDecimalLimit = errors.NewKind("Out of range value for column of Decimal type ")
	ErrMarshalNullDecimal    = errors.NewKind("Decimal cannot marshal a null value")

	decimalValueType = reflect.TypeOf(decimal.Decimal{})
)

// DecimalType represents the DECIMAL type.
// https://dev.mysql.com/doc/refman/8.0/en/fixed-point-types.html
// The type of the returned value is decimal.Decimal.
type DecimalType interface {
	Type
	// ConvertToNullDecimal converts the given value to a decimal.NullDecimal if it has a compatible type. It is worth
	// noting that Convert() returns a nil value for nil inputs, and also returns decimal.Decimal rather than
	// decimal.NullDecimal.
	ConvertToNullDecimal(v interface{}) (decimal.NullDecimal, error)
	//ConvertNoBoundsCheck normalizes an interface{} to a decimal type without performing expensive bound checks
	ConvertNoBoundsCheck(v interface{}) (decimal.Decimal, error)
	// BoundsCheck rounds and validates a decimal
	BoundsCheck(v decimal.Decimal) (decimal.Decimal, error)
	// ExclusiveUpperBound returns the exclusive upper bound for this Decimal.
	// For example, DECIMAL(5,2) would return 1000, as 999.99 is the max represented.
	ExclusiveUpperBound() decimal.Decimal
	// MaximumScale returns the maximum scale allowed for the current precision.
	MaximumScale() uint8
	// Precision returns the base-10 precision of the type, which is the total number of digits. For example, a
	// precision of 3 means that 999, 99.9, 9.99, and .999 are all valid maximums (depending on the scale).
	Precision() uint8
	// Scale returns the scale, or number of digits after the decimal, that may be held.
	// This will always be less than or equal to the precision.
	Scale() uint8
}

type decimalType struct {
	exclusiveUpperBound decimal.Decimal
	definesColumn       bool
	precision           uint8
	scale               uint8
}

// InternalDecimalType is a special DecimalType that is used internally for Decimal comparisons. Not intended for usage
// from integrators.
var InternalDecimalType DecimalType = decimalType{
	exclusiveUpperBound: decimal.New(1, int32(65)),
	definesColumn:       false,
	precision:           95,
	scale:               30,
}

// CreateDecimalType creates a DecimalType for NON-TABLE-COLUMN.
func CreateDecimalType(precision uint8, scale uint8) (DecimalType, error) {
	return createDecimalType(precision, scale, false)
}

// CreateColumnDecimalType creates a DecimalType for VALID-TABLE-COLUMN. Creating a decimal type for a column ensures that
// when operating on instances of this type, the result will be restricted to the defined precision and scale.
func CreateColumnDecimalType(precision uint8, scale uint8) (DecimalType, error) {
	return createDecimalType(precision, scale, true)
}

// createDecimalType creates a DecimalType using given precision, scale
// and whether this type defines a valid table column.
func createDecimalType(precision uint8, scale uint8, definesColumn bool) (DecimalType, error) {
	if scale > DecimalTypeMaxScale {
		return nil, fmt.Errorf("Too big scale %v specified. Maximum is %v.", scale, DecimalTypeMaxScale)
	}
	if precision > DecimalTypeMaxPrecision {
		return nil, fmt.Errorf("Too big precision %v specified. Maximum is %v.", precision, DecimalTypeMaxPrecision)
	}
	if scale > precision {
		return nil, fmt.Errorf("Scale %v cannot be larger than the precision %v", scale, precision)
	}

	if precision == 0 {
		precision = 10
	}
	return decimalType{
		exclusiveUpperBound: decimal.New(1, int32(precision-scale)),
		definesColumn:       definesColumn,
		precision:           precision,
		scale:               scale,
	}, nil
}

// MustCreateDecimalType is the same as CreateDecimalType except it panics on errors and for NON-TABLE-COLUMN.
func MustCreateDecimalType(precision uint8, scale uint8) DecimalType {
	dt, err := CreateDecimalType(precision, scale)
	if err != nil {
		panic(err)
	}
	return dt
}

// MustCreateColumnDecimalType is the same as CreateDecimalType except it panics on errors and for VALID-TABLE-COLUMN.
func MustCreateColumnDecimalType(precision uint8, scale uint8) DecimalType {
	dt, err := CreateColumnDecimalType(precision, scale)
	if err != nil {
		panic(err)
	}
	return dt
}

// Type implements Type interface.
func (t decimalType) Type() query.Type {
	return sqltypes.Decimal
}

// Compare implements Type interface.
func (t decimalType) Compare(a interface{}, b interface{}) (int, error) {
	if hasNulls, res := compareNulls(a, b); hasNulls {
		return res, nil
	}

	af, err := t.ConvertToNullDecimal(a)
	if err != nil {
		return 0, err
	}
	bf, err := t.ConvertToNullDecimal(b)
	if err != nil {
		return 0, err
	}

	return af.Decimal.Cmp(bf.Decimal), nil
}

// Convert implements Type interface.
func (t decimalType) Convert(v interface{}) (interface{}, error) {
	dec, err := t.ConvertToNullDecimal(v)
	if err != nil {
		return nil, err
	}
	if !dec.Valid {
		return nil, nil
	}
	return t.BoundsCheck(dec.Decimal)
}

func (t decimalType) ConvertNoBoundsCheck(v interface{}) (decimal.Decimal, error) {
	dec, err := t.ConvertToNullDecimal(v)
	if err != nil {
		return decimal.Decimal{}, err
	}
	if !dec.Valid {
		return decimal.Decimal{}, nil
	}
	return dec.Decimal, nil
}

// ConvertToNullDecimal implements DecimalType interface.
func (t decimalType) ConvertToNullDecimal(v interface{}) (decimal.NullDecimal, error) {
	if v == nil {
		return decimal.NullDecimal{}, nil
	}

	var res decimal.Decimal

	switch value := v.(type) {
	case int:
		return t.ConvertToNullDecimal(int64(value))
	case uint:
		return t.ConvertToNullDecimal(uint64(value))
	case int8:
		return t.ConvertToNullDecimal(int64(value))
	case uint8:
		return t.ConvertToNullDecimal(uint64(value))
	case int16:
		return t.ConvertToNullDecimal(int64(value))
	case uint16:
		return t.ConvertToNullDecimal(uint64(value))
	case int32:
		res = decimal.NewFromInt32(value)
	case uint32:
		return t.ConvertToNullDecimal(uint64(value))
	case int64:
		res = decimal.NewFromInt(value)
	case uint64:
		res = decimal.NewFromBigInt(new(big.Int).SetUint64(value), 0)
	case float32:
		res = decimal.NewFromFloat32(value)
	case float64:
		res = decimal.NewFromFloat(value)
	case string:
		var err error
		res, err = decimal.NewFromString(value)
		if err != nil {
			// The decimal library cannot handle all of the different formats
			bf, _, err := new(big.Float).SetPrec(217).Parse(value, 0)
			if err != nil {
				return decimal.NullDecimal{}, err
			}
			res, err = decimal.NewFromString(bf.Text('f', -1))
			if err != nil {
				return decimal.NullDecimal{}, err
			}
		}
	case *big.Float:
		return t.ConvertToNullDecimal(value.Text('f', -1))
	case *big.Int:
		return t.ConvertToNullDecimal(value.Text(10))
	case *big.Rat:
		return t.ConvertToNullDecimal(new(big.Float).SetRat(value))
	case decimal.Decimal:
		res = value
	case []uint8:
		val, err := strconv.ParseFloat(string(value[:]), 64)
		if err != nil {
			return decimal.NullDecimal{}, err
		}
		res = decimal.NewFromFloat(val)
	case decimal.NullDecimal:
		// This is the equivalent of passing in a nil
		if !value.Valid {
			return decimal.NullDecimal{}, nil
		}
		res = value.Decimal
	case JSONDocument:
		return t.ConvertToNullDecimal(value.Val)
	default:
		return decimal.NullDecimal{}, ErrConvertingToDecimal.New(v)
	}

	return decimal.NullDecimal{Decimal: res, Valid: true}, nil
}

func (t decimalType) BoundsCheck(v decimal.Decimal) (decimal.Decimal, error) {
	if -v.Exponent() > int32(t.scale) {
		// TODO : add 'Data truncated' warning
		v = v.Round(int32(t.scale))
	}
	// TODO add shortcut for common case
	// ex: certain num of bits fast tracks OK
	if !v.Abs().LessThan(t.exclusiveUpperBound) {
		return decimal.Decimal{}, ErrConvertToDecimalLimit.New()
	}
	return v, nil
}

// MustConvert implements the Type interface.
func (t decimalType) MustConvert(v interface{}) interface{} {
	value, err := t.Convert(v)
	if err != nil {
		panic(err)
	}
	return value
}

// Equals implements the Type interface.
func (t decimalType) Equals(otherType Type) bool {
	if ot, ok := otherType.(decimalType); ok {
		return t.precision == ot.precision && t.scale == ot.scale
	}
	return false
}

// MaxTextResponseByteLength implements the Type interface
func (t decimalType) MaxTextResponseByteLength() uint32 {
	if t.scale == 0 {
		// if no digits are reserved for the right-hand side of the decimal point,
		// just return precision plus one byte for sign
		return uint32(t.precision + 1)
	} else {
		// otherwise return precision plus one byte for sign plus one byte for the decimal point
		return uint32(t.precision + 2)
	}
}

// Promote implements the Type interface.
func (t decimalType) Promote() Type {
	if t.definesColumn {
		return MustCreateColumnDecimalType(DecimalTypeMaxPrecision, t.scale)
	}
	return MustCreateDecimalType(DecimalTypeMaxPrecision, t.scale)
}

// SQL implements Type interface.
func (t decimalType) SQL(ctx *Context, dest []byte, v interface{}) (sqltypes.Value, error) {
	if v == nil {
		return sqltypes.NULL, nil
	}
	value, err := t.ConvertToNullDecimal(v)
	if err != nil {
		return sqltypes.Value{}, err
	}

	// decimal type value for valid table column should use scale defined by the column.
	// if the value is not part of valid table column, the result value should used its
	// own precision and scale.
	var val []byte
	if t.definesColumn {
		val = appendAndSliceString(dest, value.Decimal.StringFixed(int32(t.scale)))
	} else {
		decStr := value.Decimal.StringFixed(value.Decimal.Exponent() * -1)
		val = appendAndSliceString(dest, decStr)
	}

	return sqltypes.MakeTrusted(sqltypes.Decimal, val), nil
}

// String implements Type interface.
func (t decimalType) String() string {
	return fmt.Sprintf("decimal(%v,%v)", t.precision, t.scale)
}

// ValueType implements Type interface.
func (t decimalType) ValueType() reflect.Type {
	return decimalValueType
}

// Zero implements Type interface.
func (t decimalType) Zero() interface{} {
	return decimal.NewFromInt(0)
}

// ExclusiveUpperBound implements DecimalType interface.
func (t decimalType) ExclusiveUpperBound() decimal.Decimal {
	return t.exclusiveUpperBound
}

// MaximumScale implements DecimalType interface.
func (t decimalType) MaximumScale() uint8 {
	if t.precision >= DecimalTypeMaxScale {
		return DecimalTypeMaxScale
	}
	return t.precision
}

// Precision implements DecimalType interface.
func (t decimalType) Precision() uint8 {
	return t.precision
}

// Scale implements DecimalType interface.
func (t decimalType) Scale() uint8 {
	return t.scale
}
