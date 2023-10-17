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

package types

import (
	"fmt"
	"math/big"
	"reflect"
	"strconv"

	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/dolthub/vitess/go/vt/proto/query"
	"github.com/shopspring/decimal"
	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/sql"
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

type DecimalType_ struct {
	exclusiveUpperBound decimal.Decimal
	definesColumn       bool
	precision           uint8
	scale               uint8
}

// InternalDecimalType is a special DecimalType that is used internally for Decimal comparisons. Not intended for usage
// from integrators.
var InternalDecimalType sql.DecimalType = DecimalType_{
	exclusiveUpperBound: decimal.New(1, int32(65)),
	definesColumn:       false,
	precision:           65,
	scale:               30,
}

// CreateDecimalType creates a DecimalType for NON-TABLE-COLUMN.
func CreateDecimalType(precision uint8, scale uint8) (sql.DecimalType, error) {
	return createDecimalType(precision, scale, false)
}

// CreateColumnDecimalType creates a DecimalType for VALID-TABLE-COLUMN. Creating a decimal type for a column ensures that
// when operating on instances of this type, the result will be restricted to the defined precision and scale.
func CreateColumnDecimalType(precision uint8, scale uint8) (sql.DecimalType, error) {
	return createDecimalType(precision, scale, true)
}

// createDecimalType creates a DecimalType using given precision, scale
// and whether this type defines a valid table column.
func createDecimalType(precision uint8, scale uint8, definesColumn bool) (sql.DecimalType, error) {
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
	return DecimalType_{
		exclusiveUpperBound: decimal.New(1, int32(precision-scale)),
		definesColumn:       definesColumn,
		precision:           precision,
		scale:               scale,
	}, nil
}

// MustCreateDecimalType is the same as CreateDecimalType except it panics on errors and for NON-TABLE-COLUMN.
func MustCreateDecimalType(precision uint8, scale uint8) sql.DecimalType {
	dt, err := CreateDecimalType(precision, scale)
	if err != nil {
		panic(err)
	}
	return dt
}

// MustCreateColumnDecimalType is the same as CreateDecimalType except it panics on errors and for VALID-TABLE-COLUMN.
func MustCreateColumnDecimalType(precision uint8, scale uint8) sql.DecimalType {
	dt, err := CreateColumnDecimalType(precision, scale)
	if err != nil {
		panic(err)
	}
	return dt
}

// Type implements Type interface.
func (t DecimalType_) Type() query.Type {
	return sqltypes.Decimal
}

// Compare implements Type interface.
func (t DecimalType_) Compare(a interface{}, b interface{}) (int, error) {
	if hasNulls, res := CompareNulls(a, b); hasNulls {
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
func (t DecimalType_) Convert(v interface{}) (interface{}, sql.ConvertInRange, error) {
	dec, err := t.ConvertToNullDecimal(v)
	if err != nil {
		return nil, sql.OutOfRange, err
	}
	if !dec.Valid {
		return nil, sql.InRange, nil
	}
	return t.BoundsCheck(dec.Decimal)
}

func (t DecimalType_) ConvertNoBoundsCheck(v interface{}) (decimal.Decimal, error) {
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
func (t DecimalType_) ConvertToNullDecimal(v interface{}) (decimal.NullDecimal, error) {
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
		val, err := strconv.ParseFloat(string(value), 64)
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

func (t DecimalType_) BoundsCheck(v decimal.Decimal) (decimal.Decimal, sql.ConvertInRange, error) {
	if -v.Exponent() > int32(t.scale) {
		// TODO : add 'Data truncated' warning
		v = v.Round(int32(t.scale))
	}
	// TODO add shortcut for common case
	// ex: certain num of bits fast tracks OK
	if !v.Abs().LessThan(t.exclusiveUpperBound) {
		return decimal.Decimal{}, sql.InRange, ErrConvertToDecimalLimit.New()
	}
	return v, sql.InRange, nil
}

// MustConvert implements the Type interface.
func (t DecimalType_) MustConvert(v interface{}) interface{} {
	value, _, err := t.Convert(v)
	if err != nil {
		panic(err)
	}
	return value
}

// Equals implements the Type interface.
func (t DecimalType_) Equals(otherType sql.Type) bool {
	if ot, ok := otherType.(DecimalType_); ok {
		return t.precision == ot.precision && t.scale == ot.scale
	}
	return false
}

// MaxTextResponseByteLength implements the Type interface
func (t DecimalType_) MaxTextResponseByteLength(_ *sql.Context) uint32 {
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
func (t DecimalType_) Promote() sql.Type {
	if t.definesColumn {
		return MustCreateColumnDecimalType(DecimalTypeMaxPrecision, t.scale)
	}
	return MustCreateDecimalType(DecimalTypeMaxPrecision, t.scale)
}

// SQL implements Type interface.
func (t DecimalType_) SQL(ctx *sql.Context, dest []byte, v interface{}) (sqltypes.Value, error) {
	if v == nil {
		return sqltypes.NULL, nil
	}
	value, err := t.ConvertToNullDecimal(v)
	if err != nil {
		return sqltypes.Value{}, err
	}

	// decimal type value for valid table column should use scale defined by the column.
	// if the value is not part of valid table column, the result value should use its
	// own precision and scale.
	var val []byte
	if t.definesColumn {
		val = AppendAndSliceString(dest, value.Decimal.StringFixed(int32(t.scale)))
	} else {
		decStr := value.Decimal.StringFixed(value.Decimal.Exponent() * -1)
		val = AppendAndSliceString(dest, decStr)
	}

	return sqltypes.MakeTrusted(sqltypes.Decimal, val), nil
}

// String implements Type interface.
func (t DecimalType_) String() string {
	return fmt.Sprintf("decimal(%v,%v)", t.precision, t.scale)
}

// ValueType implements Type interface.
func (t DecimalType_) ValueType() reflect.Type {
	return decimalValueType
}

// Zero implements Type interface.
func (t DecimalType_) Zero() interface{} {
	return decimal.NewFromInt(0)
}

// CollationCoercibility implements sql.CollationCoercible interface.
func (DecimalType_) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 5
}

// ExclusiveUpperBound implements DecimalType interface.
func (t DecimalType_) ExclusiveUpperBound() decimal.Decimal {
	return t.exclusiveUpperBound
}

// MaximumScale implements DecimalType interface.
func (t DecimalType_) MaximumScale() uint8 {
	if t.precision >= DecimalTypeMaxScale {
		return DecimalTypeMaxScale
	}
	return t.precision
}

// Precision implements DecimalType interface.
func (t DecimalType_) Precision() uint8 {
	return t.precision
}

// Scale implements DecimalType interface.
func (t DecimalType_) Scale() uint8 {
	return t.scale
}
