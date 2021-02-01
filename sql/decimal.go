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
	ErrConvertToDecimalLimit = errors.NewKind("value of Decimal is too large for type")
	ErrMarshalNullDecimal    = errors.NewKind("Decimal cannot marshal a null value")
)

type DecimalType interface {
	Type
	ConvertToDecimal(v interface{}) (decimal.NullDecimal, error)
	ExclusiveUpperBound() decimal.Decimal
	MaximumScale() uint8
	Precision() uint8
	Scale() uint8
}

type decimalType struct {
	exclusiveUpperBound decimal.Decimal
	precision           uint8
	scale               uint8
}

// CreateDecimalType creates a DecimalType.
func CreateDecimalType(precision uint8, scale uint8) (DecimalType, error) {
	if precision > DecimalTypeMaxPrecision {
		return nil, fmt.Errorf("%v is beyond the max precision", precision)
	}
	if scale > precision {
		return nil, fmt.Errorf("%v cannot be larger than the precision %v", scale, precision)
	}
	if scale > DecimalTypeMaxScale {
		return nil, fmt.Errorf("%v is beyond the max scale", scale)
	}
	if precision == 0 {
		precision = 10
	}
	return decimalType{
		exclusiveUpperBound: decimal.New(1, int32(precision-scale)),
		precision:           precision,
		scale:               scale,
	}, nil
}

// MustCreateDecimalType is the same as CreateDecimalType except it panics on errors.
func MustCreateDecimalType(precision uint8, scale uint8) DecimalType {
	dt, err := CreateDecimalType(precision, scale)
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

	af, err := t.ConvertToDecimal(a)
	if err != nil {
		return 0, err
	}
	bf, err := t.ConvertToDecimal(b)
	if err != nil {
		return 0, err
	}

	return af.Decimal.Cmp(bf.Decimal), nil
}

// Convert implements Type interface.
func (t decimalType) Convert(v interface{}) (interface{}, error) {
	dec, err := t.ConvertToDecimal(v)
	if err != nil {
		return nil, err
	}
	if !dec.Valid {
		return nil, nil
	}
	return dec.Decimal.StringFixed(int32(t.scale)), nil
}

// Precision returns the precision, or total number of digits, that may be held.
func (t decimalType) ConvertToDecimal(v interface{}) (decimal.NullDecimal, error) {
	if v == nil {
		return decimal.NullDecimal{}, nil
	}

	var res decimal.Decimal

	switch value := v.(type) {
	case int:
		return t.ConvertToDecimal(int64(value))
	case uint:
		return t.ConvertToDecimal(uint64(value))
	case int8:
		return t.ConvertToDecimal(int64(value))
	case uint8:
		return t.ConvertToDecimal(uint64(value))
	case int16:
		return t.ConvertToDecimal(int64(value))
	case uint16:
		return t.ConvertToDecimal(uint64(value))
	case int32:
		res = decimal.NewFromInt32(value)
	case uint32:
		return t.ConvertToDecimal(uint64(value))
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
		return t.ConvertToDecimal(value.Text('f', -1))
	case *big.Int:
		return t.ConvertToDecimal(value.Text(10))
	case *big.Rat:
		return t.ConvertToDecimal(new(big.Float).SetRat(value))
	case decimal.Decimal:
		res = value
	case decimal.NullDecimal:
		// This is the equivalent of passing in a nil
		if !value.Valid {
			return decimal.NullDecimal{}, nil
		}
		res = value.Decimal
	default:
		return decimal.NullDecimal{}, ErrConvertingToDecimal.New(v)
	}

	res = res.Round(int32(t.scale))
	if !res.Abs().LessThan(t.exclusiveUpperBound) {
		return decimal.NullDecimal{}, ErrConvertToDecimalLimit.New()
	}

	return decimal.NullDecimal{Decimal: res, Valid: true}, nil
}

// MustConvert implements the Type interface.
func (t decimalType) MustConvert(v interface{}) interface{} {
	value, err := t.Convert(v)
	if err != nil {
		panic(err)
	}
	return value
}

// Promote implements the Type interface.
func (t decimalType) Promote() Type {
	return MustCreateDecimalType(DecimalTypeMaxPrecision, t.scale)
}

// SQL implements Type interface.
func (t decimalType) SQL(v interface{}) (sqltypes.Value, error) {
	if v == nil {
		return sqltypes.NULL, nil
	}
	value, err := t.Convert(v)
	if err != nil {
		return sqltypes.Value{}, err
	}
	return sqltypes.MakeTrusted(sqltypes.Decimal, []byte(value.(string))), nil
}

// String implements Type interface.
func (t decimalType) String() string {
	return fmt.Sprintf("DECIMAL(%v,%v)", t.precision, t.scale)
}

// Zero implements Type interface. Returns a uint64 value.
func (t decimalType) Zero() interface{} {
	return decimal.NewFromInt(0).StringFixed(int32(t.scale))
}

// ExclusiveUpperBound returns the exclusive upper bound for this Decimal.
// For example, DECIMAL(5,2) would return 1000, as 999.99 is the max represented.
func (t decimalType) ExclusiveUpperBound() decimal.Decimal {
	return t.exclusiveUpperBound
}

// MaximumScale returns the maximum scale allowed for the current precision.
func (t decimalType) MaximumScale() uint8 {
	if t.precision >= DecimalTypeMaxScale {
		return DecimalTypeMaxScale
	}
	return t.precision
}

// Precision returns the base-10 precision of the type, which is the total number of digits.
// For example, a precision of 3 means that 999, 99.9, 9.99, and .999 are all valid maximums (depending on the scale).
func (t decimalType) Precision() uint8 {
	return t.precision
}

// Scale returns the scale, or number of digits after the decimal, that may be held.
// This will always be less than or equal to the precision.
func (t decimalType) Scale() uint8 {
	return t.scale
}
