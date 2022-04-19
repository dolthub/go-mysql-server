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
	"encoding/hex"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/dolthub/vitess/go/vt/proto/query"
	"github.com/shopspring/decimal"
	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/sql/values"
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

var _ Type = numberTypeImpl{}
var _ Type2 = numberTypeImpl{}

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
	if hasNulls, res := compareNulls(a, b); hasNulls {
		return res, nil
	}

	switch t.baseType {
	case sqltypes.Uint8, sqltypes.Uint16, sqltypes.Uint24, sqltypes.Uint32, sqltypes.Uint64:
		ca, err := convertToUint64(t, a)
		if err != nil {
			return 0, err
		}
		cb, err := convertToUint64(t, b)
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
	case sqltypes.Float32, sqltypes.Float64:
		ca, err := convertToFloat64(t, a)
		if err != nil {
			return 0, err
		}
		cb, err := convertToFloat64(t, b)
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
	default:
		ca, err := convertToInt64(t, a)
		if err != nil {
			return 0, err
		}
		cb, err := convertToInt64(t, b)
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
		num, err := convertToInt64(t, v)
		if err != nil {
			return nil, err
		}
		if num > math.MaxInt8 || num < math.MinInt8 {
			return nil, ErrOutOfRange.New(num, t)
		}
		return int8(num), nil
	case sqltypes.Uint8:
		num, err := convertToUint64(t, v)
		if err != nil {
			return nil, err
		}
		if num > math.MaxUint8 {
			return nil, ErrOutOfRange.New(num, t)
		}
		return uint8(num), nil
	case sqltypes.Int16:
		num, err := convertToInt64(t, v)
		if err != nil {
			return nil, err
		}
		if num > math.MaxInt16 || num < math.MinInt16 {
			return nil, ErrOutOfRange.New(num, t)
		}
		return int16(num), nil
	case sqltypes.Uint16:
		num, err := convertToUint64(t, v)
		if err != nil {
			return nil, err
		}
		if num > math.MaxUint16 {
			return nil, ErrOutOfRange.New(num, t)
		}
		return uint16(num), nil
	case sqltypes.Int24:
		num, err := convertToInt64(t, v)
		if err != nil {
			return nil, err
		}
		if num > (1<<23-1) || num < (-1<<23) {
			return nil, ErrOutOfRange.New(num, t)
		}
		return int32(num), nil
	case sqltypes.Uint24:
		num, err := convertToUint64(t, v)
		if err != nil {
			return nil, err
		}
		if num > (1<<24 - 1) {
			return nil, ErrOutOfRange.New(num, t)
		}
		return uint32(num), nil
	case sqltypes.Int32:
		num, err := convertToInt64(t, v)
		if err != nil {
			return nil, err
		}
		if num > math.MaxInt32 || num < math.MinInt32 {
			return nil, ErrOutOfRange.New(num, t)
		}
		return int32(num), nil
	case sqltypes.Uint32:
		num, err := convertToUint64(t, v)
		if err != nil {
			return nil, err
		}
		if num > math.MaxUint32 {
			return nil, ErrOutOfRange.New(num, t)
		}
		return uint32(num), nil
	case sqltypes.Int64:
		return convertToInt64(t, v)
	case sqltypes.Uint64:
		return convertToUint64(t, v)
	case sqltypes.Float32:
		num, err := convertToFloat64(t, v)
		if err != nil {
			return nil, err
		}
		if num > math.MaxFloat32 || num < -math.MaxFloat32 {
			return nil, ErrOutOfRange.New(num, t)
		}
		return float32(num), nil
	case sqltypes.Float64:
		return convertToFloat64(t, v)
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

// Equals implements the Type interface.
func (t numberTypeImpl) Equals(otherType Type) bool {
	return t.baseType == otherType.Type()
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
func (t numberTypeImpl) SQL(dest []byte, v interface{}) (sqltypes.Value, error) {
	if v == nil {
		return sqltypes.NULL, nil
	}

	stop := len(dest)
	switch t.baseType {
	case sqltypes.Int8, sqltypes.Int16, sqltypes.Int24, sqltypes.Int32, sqltypes.Int64:
		dest = strconv.AppendInt(dest, mustInt64(v), 10)
	case sqltypes.Uint8, sqltypes.Uint16, sqltypes.Uint24, sqltypes.Uint32, sqltypes.Uint64:
		dest = strconv.AppendUint(dest, mustUint64(v), 10)
	case sqltypes.Float32:
		dest = strconv.AppendFloat(dest, float64(v.(float32)), 'f', -1, 32)
	case sqltypes.Float64:
		dest = strconv.AppendFloat(dest, v.(float64), 'f', -1, 64)
	default:
		panic(ErrInvalidBaseType.New(t.baseType.String(), "number"))
	}

	val := dest[stop:]

	return sqltypes.MakeTrusted(t.baseType, val), nil
}

func (t numberTypeImpl) Compare2(a Value, b Value) (int, error) {
	switch t.baseType {
	case sqltypes.Uint8, sqltypes.Uint16, sqltypes.Uint24, sqltypes.Uint32, sqltypes.Uint64:
		ca, err := convertValueToUint64(t, a)
		if err != nil {
			return 0, err
		}
		cb, err := convertValueToUint64(t, b)
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
	case sqltypes.Float32, sqltypes.Float64:
		ca, err := convertValueToFloat64(t, a)
		if err != nil {
			return 0, err
		}
		cb, err := convertValueToFloat64(t, b)
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
	default:
		ca, err := convertValueToInt64(t, a)
		if err != nil {
			return 0, err
		}
		cb, err := convertValueToInt64(t, b)
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
}

func (t numberTypeImpl) Convert2(value Value) (Value, error) {
	panic("implement me")
}

func (t numberTypeImpl) Zero2() Value {
	switch t.baseType {
	case sqltypes.Int8:
		x := values.WriteInt8(make([]byte, values.Int8Size), 0)
		return Value{
			Typ: query.Type_INT8,
			Val: x,
		}
	case sqltypes.Int16:
		x := values.WriteInt16(make([]byte, values.Int16Size), 0)
		return Value{
			Typ: query.Type_INT16,
			Val: x,
		}
	case sqltypes.Int24:
		x := values.WriteInt24(make([]byte, values.Int24Size), 0)
		return Value{
			Typ: query.Type_INT24,
			Val: x,
		}
	case sqltypes.Int32:
		x := values.WriteInt32(make([]byte, values.Int32Size), 0)
		return Value{
			Typ: query.Type_INT32,
			Val: x,
		}
	case sqltypes.Int64:
		x := values.WriteInt64(make([]byte, values.Int64Size), 0)
		return Value{
			Typ: query.Type_INT64,
			Val: x,
		}
	case sqltypes.Uint8:
		x := values.WriteUint8(make([]byte, values.Uint8Size), 0)
		return Value{
			Typ: query.Type_UINT8,
			Val: x,
		}
	case sqltypes.Uint16:
		x := values.WriteUint16(make([]byte, values.Uint16Size), 0)
		return Value{
			Typ: query.Type_UINT16,
			Val: x,
		}
	case sqltypes.Uint24:
		x := values.WriteUint24(make([]byte, values.Uint24Size), 0)
		return Value{
			Typ: query.Type_UINT24,
			Val: x,
		}
	case sqltypes.Uint32:
		x := values.WriteUint32(make([]byte, values.Uint32Size), 0)
		return Value{
			Typ: query.Type_UINT32,
			Val: x,
		}
	case sqltypes.Uint64:
		x := values.WriteUint64(make([]byte, values.Uint64Size), 0)
		return Value{
			Typ: query.Type_UINT64,
			Val: x,
		}
	case sqltypes.Float32:
		x := values.WriteFloat32(make([]byte, values.Float32Size), 0)
		return Value{
			Typ: query.Type_FLOAT32,
			Val: x,
		}
	case sqltypes.Float64:
		x := values.WriteUint64(make([]byte, values.Uint64Size), 0)
		return Value{
			Typ: query.Type_UINT64,
			Val: x,
		}
	default:
		panic(ErrInvalidBaseType.New(t.baseType.String(), "number"))
	}
}

// SQL2 implements Type2 interface.
func (t numberTypeImpl) SQL2(v Value) (sqltypes.Value, error) {
	if v.IsNull() {
		return sqltypes.NULL, nil
	}

	var val []byte
	switch t.baseType {
	case sqltypes.Int8:
		x := values.ReadInt8(v.Val)
		val = []byte(strconv.FormatInt(int64(x), 10))
	case sqltypes.Int16:
		x := values.ReadInt16(v.Val)
		val = []byte(strconv.FormatInt(int64(x), 10))
	case sqltypes.Int24:
		x := values.ReadInt24(v.Val)
		val = []byte(strconv.FormatInt(int64(x), 10))
	case sqltypes.Int32:
		x := values.ReadInt32(v.Val)
		val = []byte(strconv.FormatInt(int64(x), 10))
	case sqltypes.Int64:
		x := values.ReadInt64(v.Val)
		val = []byte(strconv.FormatInt(x, 10))
	case sqltypes.Uint8:
		x := values.ReadUint8(v.Val)
		val = []byte(strconv.FormatUint(uint64(x), 10))
	case sqltypes.Uint16:
		x := values.ReadUint16(v.Val)
		val = []byte(strconv.FormatUint(uint64(x), 10))
	case sqltypes.Uint24:
		x := values.ReadUint24(v.Val)
		val = []byte(strconv.FormatUint(uint64(x), 10))
	case sqltypes.Uint32:
		x := values.ReadUint32(v.Val)
		val = []byte(strconv.FormatUint(uint64(x), 10))
	case sqltypes.Uint64:
		x := values.ReadUint64(v.Val)
		val = []byte(strconv.FormatUint(x, 10))
	case sqltypes.Float32:
		x := values.ReadFloat32(v.Val)
		val = []byte(strconv.FormatFloat(float64(x), 'f', -1, 32))
	case sqltypes.Float64:
		x := values.ReadFloat64(v.Val)
		val = []byte(strconv.FormatFloat(x, 'f', -1, 64))
	default:
		panic(ErrInvalidBaseType.New(t.baseType.String(), "number"))
	}

	return sqltypes.MakeTrusted(t.baseType, val), nil
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

func convertToInt64(t numberTypeImpl, v interface{}) (int64, error) {
	switch v := v.(type) {
	case int:
		return int64(v), nil
	case int8:
		return int64(v), nil
	case int16:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case int64:
		return v, nil
	case uint:
		return int64(v), nil
	case uint8:
		return int64(v), nil
	case uint16:
		return int64(v), nil
	case uint32:
		return int64(v), nil
	case uint64:
		if v > math.MaxInt64 {
			return 0, ErrOutOfRange.New(v, t)
		}
		return int64(v), nil
	case float32:
		if float32(math.MaxInt64) >= v && v >= float32(math.MinInt64) {
			return int64(v), nil
		}
		return 0, ErrOutOfRange.New(v, t)
	case float64:
		if float64(math.MaxInt64) >= v && v >= float64(math.MinInt64) {
			return int64(v), nil
		}
		return 0, ErrOutOfRange.New(v, t)
	case decimal.Decimal:
		if v.GreaterThan(dec_int64_max) || v.LessThan(dec_int64_min) {
			return 0, ErrOutOfRange.New(v.String(), t)
		}
		return v.IntPart(), nil
	case []byte:
		i, err := strconv.ParseInt(hex.EncodeToString(v), 16, 64)
		if err != nil {
			return 0, ErrInvalidValue.New(v, t.String())
		}
		return i, nil
	case string:
		// Parse first an integer, which allows for more values than float64
		i, err := strconv.ParseInt(v, 10, 64)
		if err == nil {
			return i, nil
		}
		// If that fails, try as a float and truncate it to integral
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return 0, ErrInvalidValue.New(v, t.String())
		}
		return int64(f), nil
	case bool:
		if v {
			return 1, nil
		}
		return 0, nil
	case nil:
		return 0, nil
	default:
		return 0, ErrInvalidValueType.New(v, t.String())
	}
}

func convertValueToInt64(t numberTypeImpl, v Value) (int64, error) {
	switch v.Typ {
	case query.Type_INT8:
		return int64(values.ReadInt8(v.Val)), nil
	case query.Type_INT16:
		return int64(values.ReadInt16(v.Val)), nil
	case query.Type_INT24:
		return int64(values.ReadInt24(v.Val)), nil
	case query.Type_INT32:
		return int64(values.ReadInt32(v.Val)), nil
	case query.Type_INT64:
		return values.ReadInt64(v.Val), nil
	case query.Type_UINT8:
		return int64(values.ReadUint8(v.Val)), nil
	case query.Type_UINT16:
		return int64(values.ReadUint16(v.Val)), nil
	case query.Type_UINT24:
		return int64(values.ReadUint24(v.Val)), nil
	case query.Type_UINT32:
		return int64(values.ReadUint32(v.Val)), nil
	case query.Type_UINT64:
		v := values.ReadUint64(v.Val)
		if v > math.MaxInt64 {
			return 0, ErrOutOfRange.New(v, t)
		}
		return int64(v), nil
	case query.Type_FLOAT32:
		v := values.ReadFloat32(v.Val)
		if float32(math.MaxInt64) >= v && v >= float32(math.MinInt64) {
			return int64(v), nil
		}
		return 0, ErrOutOfRange.New(v, t)
	case query.Type_FLOAT64:
		v := values.ReadFloat64(v.Val)
		if float64(math.MaxInt64) >= v && v >= float64(math.MinInt64) {
			return int64(v), nil
		}
		return 0, ErrOutOfRange.New(v, t)
		// TODO: add more conversions
	default:
		panic(ErrInvalidBaseType.New(t.baseType.String(), "number"))
	}
}

func convertValueToUint64(t numberTypeImpl, v Value) (uint64, error) {
	switch v.Typ {
	case query.Type_INT8:
		return uint64(values.ReadInt8(v.Val)), nil
	case query.Type_INT16:
		return uint64(values.ReadInt16(v.Val)), nil
	case query.Type_INT24:
		return uint64(values.ReadInt24(v.Val)), nil
	case query.Type_INT32:
		return uint64(values.ReadInt32(v.Val)), nil
	case query.Type_INT64:
		return uint64(values.ReadInt64(v.Val)), nil
	case query.Type_UINT8:
		return uint64(values.ReadUint8(v.Val)), nil
	case query.Type_UINT16:
		return uint64(values.ReadUint16(v.Val)), nil
	case query.Type_UINT24:
		return uint64(values.ReadUint24(v.Val)), nil
	case query.Type_UINT32:
		return uint64(values.ReadUint32(v.Val)), nil
	case query.Type_UINT64:
		return values.ReadUint64(v.Val), nil
	case query.Type_FLOAT32:
		v := values.ReadFloat32(v.Val)
		if float32(math.MaxUint64) >= v {
			return uint64(v), nil
		}
		return 0, ErrOutOfRange.New(v, t)
	case query.Type_FLOAT64:
		v := values.ReadFloat64(v.Val)
		if float64(math.MaxUint64) >= v {
			return uint64(v), nil
		}
		return 0, ErrOutOfRange.New(v, t)
		// TODO: add more conversions
	default:
		panic(ErrInvalidBaseType.New(t.baseType.String(), "number"))
	}
}

func convertToUint64(t numberTypeImpl, v interface{}) (uint64, error) {
	switch v := v.(type) {
	case int:
		if v < 0 {
			return 0, ErrOutOfRange.New(v, t)
		}
		return uint64(v), nil
	case int8:
		if v < 0 {
			return 0, ErrOutOfRange.New(v, t)
		}
		return uint64(v), nil
	case int16:
		if v < 0 {
			return 0, ErrOutOfRange.New(v, t)
		}
		return uint64(v), nil
	case int32:
		if v < 0 {
			return 0, ErrOutOfRange.New(v, t)
		}
		return uint64(v), nil
	case int64:
		if v < 0 {
			return 0, ErrOutOfRange.New(v, t)
		}
		return uint64(v), nil
	case uint:
		return uint64(v), nil
	case uint8:
		return uint64(v), nil
	case uint16:
		return uint64(v), nil
	case uint32:
		return uint64(v), nil
	case uint64:
		return v, nil
	case float32:
		if float32(math.MaxUint64) >= v && v >= 0 {
			return uint64(v), nil
		}
		return 0, ErrOutOfRange.New(v, t)
	case float64:
		if float64(math.MaxUint64) >= v && v >= 0 {
			return uint64(v), nil
		}
		return 0, ErrOutOfRange.New(v, t)
	case decimal.Decimal:
		if v.GreaterThan(dec_uint64_max) || v.LessThan(dec_zero) {
			return 0, ErrOutOfRange.New(v.String(), t)
		}
		// TODO: If we ever internally switch to using Decimal for large numbers, this will need to be updated
		f, _ := v.Float64()
		return uint64(f), nil
	case []byte:
		i, err := strconv.ParseUint(hex.EncodeToString(v), 16, 64)
		if err != nil {
			return 0, ErrInvalidValue.New(v, t.String())
		}
		return i, nil
	case string:
		i, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			return 0, ErrInvalidValue.New(v, t.String())
		}
		return i, nil
	case bool:
		if v {
			return 1, nil
		}
		return 0, nil
	case nil:
		return 0, nil
	default:
		return 0, ErrInvalidValueType.New(v, t.String())
	}
}

func convertToFloat64(t numberTypeImpl, v interface{}) (float64, error) {
	switch v := v.(type) {
	case int:
		return float64(v), nil
	case int8:
		return float64(v), nil
	case int16:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case uint:
		return float64(v), nil
	case uint8:
		return float64(v), nil
	case uint16:
		return float64(v), nil
	case uint32:
		return float64(v), nil
	case uint64:
		return float64(v), nil
	case float32:
		return float64(v), nil
	case float64:
		return v, nil
	case decimal.Decimal:
		f, _ := v.Float64()
		return f, nil
	case []byte:
		i, err := strconv.ParseUint(hex.EncodeToString(v), 16, 64)
		if err != nil {
			return 0, ErrInvalidValue.New(v, t.String())
		}
		return float64(i), nil
	case string:
		i, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return 0, ErrInvalidValue.New(v, t.String())
		}
		return i, nil
	case bool:
		if v {
			return 1, nil
		}
		return 0, nil
	case nil:
		return 0, nil
	default:
		return 0, ErrInvalidValueType.New(v, t.String())
	}
}

func convertValueToFloat64(t numberTypeImpl, v Value) (float64, error) {
	switch v.Typ {
	case query.Type_INT8:
		return float64(values.ReadInt8(v.Val)), nil
	case query.Type_INT16:
		return float64(values.ReadInt16(v.Val)), nil
	case query.Type_INT24:
		return float64(values.ReadInt24(v.Val)), nil
	case query.Type_INT32:
		return float64(values.ReadInt32(v.Val)), nil
	case query.Type_INT64:
		return float64(values.ReadInt64(v.Val)), nil
	case query.Type_UINT8:
		return float64(values.ReadUint8(v.Val)), nil
	case query.Type_UINT16:
		return float64(values.ReadUint16(v.Val)), nil
	case query.Type_UINT24:
		return float64(values.ReadUint24(v.Val)), nil
	case query.Type_UINT32:
		return float64(values.ReadUint32(v.Val)), nil
	case query.Type_UINT64:
		return float64(values.ReadUint64(v.Val)), nil
	case query.Type_FLOAT32:
		return float64(values.ReadFloat32(v.Val)), nil
	case query.Type_FLOAT64:
		return values.ReadFloat64(v.Val), nil
	default:
		panic(ErrInvalidBaseType.New(t.baseType.String(), "number"))
	}
}

func mustInt64(v interface{}) int64 {
	switch tv := v.(type) {
	case int:
		return int64(tv)
	case int8:
		return int64(tv)
	case int16:
		return int64(tv)
	case int32:
		return int64(tv)
	case int64:
		return int64(tv)
	case bool:
		if tv {
			return int64(1)
		}
		return int64(0)
	default:
		panic("unexpected type")
	}
}

func mustUint64(v interface{}) uint64 {
	switch tv := v.(type) {
	case uint:
		return uint64(tv)
	case uint8:
		return uint64(tv)
	case uint16:
		return uint64(tv)
	case uint32:
		return uint64(tv)
	case uint64:
		return uint64(tv)
	case bool:
		if tv {
			return uint64(1)
		}
		return uint64(0)
	case int:
		return uint64(tv)
	default:
		panic("unexpected type")
	}
}
