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
	"math"
	"strconv"
	"time"

	"github.com/shopspring/decimal"
	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/dolthub/vitess/go/vt/proto/query"
	"github.com/spf13/cast"
)

const (
	// Numeric representation of False as defined by MySQL.
	False = int8(0)
	// Numeric representation of True as defined by MySQL.
	True = int8(1)
)

var (
	ErrOutOfRange = errors.NewKind("%v out of range for %v")

	// Boolean is a synonym for TINYINT
	Boolean = Int8
	// Int8 is an integer of 8 bits
	Int8 = MustCreateNumberType(sqltypes.Int8)
	// Uint8 is an unsigned integer of 8 bits
	Uint8 = MustCreateNumberType(sqltypes.Uint8)
	// Int16 is an integer of 16 bits
	Int16 = MustCreateNumberType(sqltypes.Int16)
	// Uint16 is an unsigned integer of 16 bits
	Uint16 = MustCreateNumberType(sqltypes.Uint16)
	// Int24 is an integer of 24 bits.
	Int24 = MustCreateNumberType(sqltypes.Int24)
	// Uint24 is an unsigned integer of 24 bits.
	Uint24 = MustCreateNumberType(sqltypes.Uint24)
	// Int32 is an integer of 32 bits.
	Int32 = MustCreateNumberType(sqltypes.Int32)
	// Uint32 is an unsigned integer of 32 bits.
	Uint32 = MustCreateNumberType(sqltypes.Uint32)
	// Int64 is an integer of 64 bytes.
	Int64 = MustCreateNumberType(sqltypes.Int64)
	// Uint64 is an unsigned integer of 64 bits.
	Uint64 = MustCreateNumberType(sqltypes.Uint64)
	// Float32 is a floating point number of 32 bits.
	Float32 = MustCreateNumberType(sqltypes.Float32)
	// Float64 is a floating point number of 64 bits.
	Float64 = MustCreateNumberType(sqltypes.Float64)

	// decimal that represents the max value an uint64 can hold
	dec_uint64_max = decimal.NewFromInt(math.MaxInt64).Mul(decimal.NewFromInt(2).Add(decimal.NewFromInt(1)))
	// decimal that represents the max value an int64 can hold
	dec_int64_max = decimal.NewFromInt(math.MaxInt64)
	// decimal that represents the min value an int64 can hold
	dec_int64_min = decimal.NewFromInt(math.MinInt64)
	// decimal that represents the zero value
	dec_zero = decimal.NewFromInt(0)
)

// Represents all integer and floating point types.
// https://dev.mysql.com/doc/refman/8.0/en/integer-types.html
// https://dev.mysql.com/doc/refman/8.0/en/floating-point-types.html
type NumberType interface {
	Type
	IsSigned() bool
	IsFloat() bool
}

type numberTypeImpl struct {
	baseType query.Type
}

// CreateNumberType creates a NumberType.
func CreateNumberType(baseType query.Type) (NumberType, error) {
	switch baseType {
	case sqltypes.Int8, sqltypes.Uint8, sqltypes.Int16, sqltypes.Uint16, sqltypes.Int24, sqltypes.Uint24,
		sqltypes.Int32, sqltypes.Uint32, sqltypes.Int64, sqltypes.Uint64, sqltypes.Float32, sqltypes.Float64:
		return numberTypeImpl{
			baseType: baseType,
		}, nil
	}
	return nil, fmt.Errorf("%v is not a valid number base type", baseType.String())
}

// MustCreateNumberType is the same as CreateNumberType except it panics on errors.
func MustCreateNumberType(baseType query.Type) NumberType {
	nt, err := CreateNumberType(baseType)
	if err != nil {
		panic(err)
	}
	return nt
}

func NumericUnaryValue(t Type) interface{} {
	nt := t.(numberTypeImpl)
	switch nt.baseType {
	case sqltypes.Int8:
		return int8(1)
	case sqltypes.Uint8:
		return uint8(1)
	case sqltypes.Int16:
		return int16(1)
	case sqltypes.Uint16:
		return uint16(1)
	case sqltypes.Int24:
		return int32(1)
	case sqltypes.Uint24:
		return uint32(1)
	case sqltypes.Int32:
		return int32(1)
	case sqltypes.Uint32:
		return uint32(1)
	case sqltypes.Int64:
		return int64(1)
	case sqltypes.Uint64:
		return uint64(1)
	case sqltypes.Float32:
		return float32(1)
	case sqltypes.Float64:
		return float64(1)
	default:
		panic(fmt.Sprintf("%v is not a valid number base type", nt.baseType.String()))
	}
}

// Compare implements Type interface.
func (t numberTypeImpl) Compare(a interface{}, b interface{}) (int, error) {
	switch t.baseType {
	case sqltypes.Uint8, sqltypes.Uint16, sqltypes.Uint24, sqltypes.Uint32, sqltypes.Uint64:
		return compareUnsignedInts(a, b)
	case sqltypes.Float32, sqltypes.Float64:
		return compareFloats(a, b)
	default:
		return compareSignedInts(a, b)
	}
}

// Convert implements Type interface.
func (t numberTypeImpl) Convert(v interface{}) (interface{}, error) {
	if v == nil {
		return nil, nil
	}

	if ti, ok := v.(time.Time); ok {
		v = ti.UTC().Unix()
	}

	if jv, ok := v.(JSONValue); ok {
		jd, err := jv.Unmarshall(nil)
		if err != nil {
			return nil, err
		}
		v = jd.Val
	}

	switch t.baseType {
	case sqltypes.Int8:
		if dec, ok := v.(decimal.Decimal); ok {
			v = dec.IntPart()
		}
		num, err := cast.ToInt64E(v)
		if err != nil {
			return nil, err
		}
		if num > math.MaxInt8 || num < math.MinInt8 {
			return nil, ErrOutOfRange.New(num, t)
		}
		return int8(num), nil
	case sqltypes.Uint8:
		if dec, ok := v.(decimal.Decimal); ok {
			v = dec.IntPart()
		}
		num, err := cast.ToUint64E(v)
		if err != nil {
			return nil, err
		}
		if num > math.MaxUint8 {
			return nil, ErrOutOfRange.New(num, t)
		}
		return uint8(num), nil
	case sqltypes.Int16:
		if dec, ok := v.(decimal.Decimal); ok {
			v = dec.IntPart()
		}
		num, err := cast.ToInt64E(v)
		if err != nil {
			return nil, err
		}
		if num > math.MaxInt16 || num < math.MinInt16 {
			return nil, ErrOutOfRange.New(num, t)
		}
		return int16(num), nil
	case sqltypes.Uint16:
		if dec, ok := v.(decimal.Decimal); ok {
			v = dec.IntPart()
		}
		num, err := cast.ToUint64E(v)
		if err != nil {
			return nil, err
		}
		if num > math.MaxUint16 {
			return nil, ErrOutOfRange.New(num, t)
		}
		return uint16(num), nil
	case sqltypes.Int24:
		if dec, ok := v.(decimal.Decimal); ok {
			v = dec.IntPart()
		}
		num, err := cast.ToInt64E(v)
		if err != nil {
			return nil, err
		}
		if num > (1<<23-1) || num < (-1<<23) {
			return nil, ErrOutOfRange.New(num, t)
		}
		return int32(num), nil
	case sqltypes.Uint24:
		if dec, ok := v.(decimal.Decimal); ok {
			v = dec.IntPart()
		}
		num, err := cast.ToUint64E(v)
		if err != nil {
			return nil, err
		}
		if num > (1<<24 - 1) {
			return nil, ErrOutOfRange.New(num, t)
		}
		return uint32(num), nil
	case sqltypes.Int32:
		if dec, ok := v.(decimal.Decimal); ok {
			v = dec.IntPart()
		}
		num, err := cast.ToInt64E(v)
		if err != nil {
			return nil, err
		}
		if num > math.MaxInt32 || num < math.MinInt32 {
			return nil, ErrOutOfRange.New(num, t)
		}
		return int32(num), nil
	case sqltypes.Uint32:
		if dec, ok := v.(decimal.Decimal); ok {
			v = dec.IntPart()
		}
		num, err := cast.ToUint64E(v)
		if err != nil {
			return nil, err
		}
		if num > math.MaxUint32 {
			return nil, ErrOutOfRange.New(num, t)
		}
		return uint32(num), nil
	case sqltypes.Int64:
		if u, ok := v.(uint64); ok {
			if u > math.MaxInt64 {
				return nil, ErrOutOfRange.New(u, t)
			}
		}
		if dec, ok := v.(decimal.Decimal); ok {
			if dec.GreaterThan(dec_int64_max) || dec.LessThan(dec_int64_min) {
				return nil, ErrOutOfRange.New(dec.String(), t)
			}
			return dec.IntPart(), nil
		}
		num, err := cast.ToInt64E(v)
		if err != nil {
			return nil, err
		}
		return num, err
	case sqltypes.Uint64:
		if dec, ok := v.(decimal.Decimal); ok {
			if dec.GreaterThan(dec_uint64_max) || dec.LessThan(dec_zero) {
				return nil, ErrOutOfRange.New(dec.String(), t)
			}
			v = dec.IntPart()
		}
		num, err := cast.ToUint64E(v)
		if err != nil {
			return nil, err
		}
		return num, nil
	case sqltypes.Float32:
		if dec, ok := v.(decimal.Decimal); ok {
			f, _ := dec.Float64()
			return float32(f), nil
		}
		num, err := cast.ToFloat64E(v)
		if err != nil {
			return nil, err
		}
		if num > math.MaxFloat32 || num < -math.MaxFloat32 {
			return nil, ErrOutOfRange.New(num, t)
		}
		return float32(num), nil
	case sqltypes.Float64:
		if dec, ok := v.(decimal.Decimal); ok {
			f, _ := dec.Float64()
			return f, nil
		}

		num, err := cast.ToFloat64E(v)
		if err != nil {
			return nil, err
		}
		return num, nil
	default:
		return nil, ErrInvalidType.New(t.baseType.String())
	}
}

// MustConvert implements the Type interface.
func (t numberTypeImpl) MustConvert(v interface{}) interface{} {
	value, err := t.Convert(v)
	if err != nil {
		panic(err)
	}
	return value
}

// Promote implements the Type interface.
func (t numberTypeImpl) Promote() Type {
	switch t.baseType {
	case sqltypes.Int8, sqltypes.Int16, sqltypes.Int24, sqltypes.Int32, sqltypes.Int64:
		return Int64
	case sqltypes.Uint8, sqltypes.Uint16, sqltypes.Uint24, sqltypes.Uint32, sqltypes.Uint64:
		return Uint64
	case sqltypes.Float32, sqltypes.Float64:
		return Float64
	default:
		panic(ErrInvalidBaseType.New(t.baseType.String(), "number"))
	}
}

// SQL implements Type interface.
func (t numberTypeImpl) SQL(v interface{}) (sqltypes.Value, error) {
	if v == nil {
		return sqltypes.NULL, nil
	}

	switch t.baseType {
	case sqltypes.Int8:
		return sqltypes.MakeTrusted(sqltypes.Int8, strconv.AppendInt(nil, cast.ToInt64(v), 10)), nil
	case sqltypes.Uint8:
		return sqltypes.MakeTrusted(sqltypes.Uint8, strconv.AppendUint(nil, cast.ToUint64(v), 10)), nil
	case sqltypes.Int16:
		return sqltypes.MakeTrusted(sqltypes.Int16, strconv.AppendInt(nil, cast.ToInt64(v), 10)), nil
	case sqltypes.Uint16:
		return sqltypes.MakeTrusted(sqltypes.Uint16, strconv.AppendUint(nil, cast.ToUint64(v), 10)), nil
	case sqltypes.Int24:
		return sqltypes.MakeTrusted(sqltypes.Int24, strconv.AppendInt(nil, cast.ToInt64(v), 10)), nil
	case sqltypes.Uint24:
		return sqltypes.MakeTrusted(sqltypes.Uint24, strconv.AppendUint(nil, cast.ToUint64(v), 10)), nil
	case sqltypes.Int32:
		return sqltypes.MakeTrusted(sqltypes.Int32, strconv.AppendInt(nil, cast.ToInt64(v), 10)), nil
	case sqltypes.Uint32:
		return sqltypes.MakeTrusted(sqltypes.Uint32, strconv.AppendUint(nil, cast.ToUint64(v), 10)), nil
	case sqltypes.Int64:
		return sqltypes.MakeTrusted(sqltypes.Int64, strconv.AppendInt(nil, cast.ToInt64(v), 10)), nil
	case sqltypes.Uint64:
		return sqltypes.MakeTrusted(sqltypes.Uint64, strconv.AppendUint(nil, cast.ToUint64(v), 10)), nil
	case sqltypes.Float32:
		return sqltypes.MakeTrusted(sqltypes.Float32, strconv.AppendFloat(nil, cast.ToFloat64(v), 'f', -1, 64)), nil
	case sqltypes.Float64:
		return sqltypes.MakeTrusted(sqltypes.Float64, strconv.AppendFloat(nil, cast.ToFloat64(v), 'f', -1, 64)), nil
	default:
		panic(ErrInvalidBaseType.New(t.baseType.String(), "number"))
	}
}

// String implements Type interface.
func (t numberTypeImpl) String() string {
	switch t.baseType {
	case sqltypes.Int8:
		return "TINYINT"
	case sqltypes.Uint8:
		return "TINYINT UNSIGNED"
	case sqltypes.Int16:
		return "SMALLINT"
	case sqltypes.Uint16:
		return "SMALLINT UNSIGNED"
	case sqltypes.Int24:
		return "MEDIUMINT"
	case sqltypes.Uint24:
		return "MEDIUMINT UNSIGNED"
	case sqltypes.Int32:
		return "INT"
	case sqltypes.Uint32:
		return "INT UNSIGNED"
	case sqltypes.Int64:
		return "BIGINT"
	case sqltypes.Uint64:
		return "BIGINT UNSIGNED"
	case sqltypes.Float32:
		return "FLOAT"
	case sqltypes.Float64:
		return "DOUBLE"
	default:
		panic(fmt.Sprintf("%v is not a valid number base type", t.baseType.String()))
	}
}

// Type implements Type interface.
func (t numberTypeImpl) Type() query.Type {
	return t.baseType
}

// Zero implements Type interface.
func (t numberTypeImpl) Zero() interface{} {
	switch t.baseType {
	case sqltypes.Int8:
		return int8(0)
	case sqltypes.Uint8:
		return uint8(0)
	case sqltypes.Int16:
		return int16(0)
	case sqltypes.Uint16:
		return uint16(0)
	case sqltypes.Int24:
		return int32(0)
	case sqltypes.Uint24:
		return uint32(0)
	case sqltypes.Int32:
		return int32(0)
	case sqltypes.Uint32:
		return uint32(0)
	case sqltypes.Int64:
		return int64(0)
	case sqltypes.Uint64:
		return uint64(0)
	case sqltypes.Float32:
		return float32(0)
	case sqltypes.Float64:
		return float64(0)
	default:
		panic(fmt.Sprintf("%v is not a valid number base type", t.baseType.String()))
	}
}

// IsFloat implements NumberType interface.
func (t numberTypeImpl) IsFloat() bool {
	switch t.baseType {
	case sqltypes.Float32, sqltypes.Float64:
		return true
	}
	return false
}

// IsSigned implements NumberType interface.
func (t numberTypeImpl) IsSigned() bool {
	switch t.baseType {
	case sqltypes.Int8, sqltypes.Int16, sqltypes.Int24, sqltypes.Int32, sqltypes.Int64, sqltypes.Float32, sqltypes.Float64:
		return true
	}
	return false
}

func compareFloats(a interface{}, b interface{}) (int, error) {
	if hasNulls, res := compareNulls(a, b); hasNulls {
		return res, nil
	}

	ca, err := cast.ToFloat64E(a)
	if err != nil {
		return 0, err
	}
	cb, err := cast.ToFloat64E(b)
	if err != nil {
		return 0, err
	}

	if ca == cb {
		return 0, nil
	}

	if ca < cb {
		return -1, nil
	}

	return +1, nil
}

func compareSignedInts(a interface{}, b interface{}) (int, error) {
	if hasNulls, res := compareNulls(a, b); hasNulls {
		return res, nil
	}

	ca, err := cast.ToInt64E(a)
	if err != nil {
		return 0, err
	}
	cb, err := cast.ToInt64E(b)
	if err != nil {
		return 0, err
	}

	if ca == cb {
		return 0, nil
	}

	if ca < cb {
		return -1, nil
	}

	return +1, nil
}

func compareUnsignedInts(a interface{}, b interface{}) (int, error) {
	if hasNulls, res := compareNulls(a, b); hasNulls {
		return res, nil
	}

	ca, err := cast.ToUint64E(a)
	if err != nil {
		return 0, err
	}
	cb, err := cast.ToUint64E(b)
	if err != nil {
		return 0, err
	}

	if ca == cb {
		return 0, nil
	}

	if ca < cb {
		return -1, nil
	}

	return +1, nil
}
