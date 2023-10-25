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
	"encoding/hex"
	"fmt"
	"math"
	"reflect"
	"regexp"
	"strconv"
	"time"

	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/dolthub/vitess/go/vt/proto/query"
	"github.com/shopspring/decimal"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/values"
)

var (
	// Boolean is a synonym for TINYINT(1)
	Boolean = MustCreateNumberTypeWithDisplayWidth(sqltypes.Int8, 1)
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
	dec_uint32_max = decimal.NewFromInt(math.MaxInt32).Mul(decimal.NewFromInt(2).Add(decimal.NewFromInt(1)))
	dec_uint16_max = decimal.NewFromInt(math.MaxInt16).Mul(decimal.NewFromInt(2).Add(decimal.NewFromInt(1)))
	dec_uint8_max  = decimal.NewFromInt(math.MaxInt8).Mul(decimal.NewFromInt(2).Add(decimal.NewFromInt(1)))
	// decimal that represents the max value an int64 can hold
	dec_int64_max = decimal.NewFromInt(math.MaxInt64)
	// decimal that represents the min value an int64 can hold
	dec_int64_min = decimal.NewFromInt(math.MinInt64)
	// decimal that represents the zero value
	dec_zero = decimal.NewFromInt(0)

	numberInt8ValueType    = reflect.TypeOf(int8(0))
	numberInt16ValueType   = reflect.TypeOf(int16(0))
	numberInt32ValueType   = reflect.TypeOf(int32(0))
	numberInt64ValueType   = reflect.TypeOf(int64(0))
	numberUint8ValueType   = reflect.TypeOf(uint8(0))
	numberUint16ValueType  = reflect.TypeOf(uint16(0))
	numberUint32ValueType  = reflect.TypeOf(uint32(0))
	numberUint64ValueType  = reflect.TypeOf(uint64(0))
	numberFloat32ValueType = reflect.TypeOf(float32(0))
	numberFloat64ValueType = reflect.TypeOf(float64(0))

	numre = regexp.MustCompile(`^[ ]*[0-9]*\.?[0-9]+`)
)

type NumberTypeImpl_ struct {
	baseType     query.Type
	displayWidth int
}

var _ sql.Type = NumberTypeImpl_{}
var _ sql.Type2 = NumberTypeImpl_{}
var _ sql.CollationCoercible = NumberTypeImpl_{}
var _ sql.NumberType = NumberTypeImpl_{}

// CreateNumberType creates a NumberType.
func CreateNumberType(baseType query.Type) (sql.NumberType, error) {
	return CreateNumberTypeWithDisplayWidth(baseType, 0)
}

// CreateNumberTypeWithDisplayWidth creates a NumberType that includes optional |displayWidth| metadata. Note that
// MySQL only allows a |displayWidth| of 1 for Int8 (i.e. TINYINT(1)); any other combination of |displayWidth| and
// |baseType| is not supported and will cause this function to return an error.
func CreateNumberTypeWithDisplayWidth(baseType query.Type, displayWidth int) (sql.NumberType, error) {
	switch baseType {
	case sqltypes.Int8, sqltypes.Uint8, sqltypes.Int16, sqltypes.Uint16, sqltypes.Int24, sqltypes.Uint24,
		sqltypes.Int32, sqltypes.Uint32, sqltypes.Int64, sqltypes.Uint64, sqltypes.Float32, sqltypes.Float64:

		// displayWidth of 0 is valid for all types, displayWidth of 1 is only valid for Int8
		if displayWidth == 0 || (displayWidth == 1 && baseType == sqltypes.Int8) {
			return NumberTypeImpl_{
				baseType:     baseType,
				displayWidth: displayWidth,
			}, nil
		}
		return nil, fmt.Errorf("display width of %d is not valid for type %s", displayWidth, baseType.String())
	}
	return nil, fmt.Errorf("%v is not a valid number base type", baseType.String())
}

// MustCreateNumberType is the same as CreateNumberType except it panics on errors.
func MustCreateNumberType(baseType query.Type) sql.NumberType {
	nt, err := CreateNumberType(baseType)
	if err != nil {
		panic(err)
	}
	return nt
}

// MustCreateNumberTypeWithDisplayWidth is the same as CreateNumberTypeWithDisplayWidth except it panics on errors.
func MustCreateNumberTypeWithDisplayWidth(baseType query.Type, displayWidth int) sql.NumberType {
	nt, err := CreateNumberTypeWithDisplayWidth(baseType, displayWidth)
	if err != nil {
		panic(err)
	}
	return nt
}

func NumericUnaryValue(t sql.Type) interface{} {
	nt := t.(NumberTypeImpl_)
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
func (t NumberTypeImpl_) Compare(a interface{}, b interface{}) (int, error) {
	if hasNulls, res := CompareNulls(a, b); hasNulls {
		return res, nil
	}

	switch t.baseType {
	case sqltypes.Uint8, sqltypes.Uint16, sqltypes.Uint24, sqltypes.Uint32, sqltypes.Uint64:
		ca, _, err := convertToUint64(t, a)
		if err != nil {
			return 0, err
		}
		cb, _, err := convertToUint64(t, b)
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
		ca, _, err := convertToInt64(t, a)
		if err != nil {
			ca = 0
		}
		cb, _, err := convertToInt64(t, b)
		if err != nil {
			cb = 0
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
func (t NumberTypeImpl_) Convert(v interface{}) (interface{}, sql.ConvertInRange, error) {
	if v == nil {
		return nil, sql.InRange, nil
	}

	if ti, ok := v.(time.Time); ok {
		v = ti.UTC().Unix()
	}

	if jv, ok := v.(sql.JSONWrapper); ok {
		v = jv.ToInterface()
	}

	switch t.baseType {
	case sqltypes.Int8:
		num, _, err := convertToInt64(t, v)
		if err != nil {
			return nil, sql.OutOfRange, err
		}
		if num > math.MaxInt8 {
			return int8(math.MaxInt8), sql.OutOfRange, nil
		} else if num < math.MinInt8 {
			return int8(math.MinInt8), sql.OutOfRange, nil
		}
		return int8(num), sql.InRange, nil
	case sqltypes.Uint8:
		return convertToUint8(t, v)
	case sqltypes.Int16:
		num, _, err := convertToInt64(t, v)
		if err != nil {
			return nil, sql.OutOfRange, err
		}
		if num > math.MaxInt16 {
			return int16(math.MaxInt16), sql.OutOfRange, nil
		} else if num < math.MinInt16 {
			return int16(math.MinInt16), sql.OutOfRange, nil
		}
		return int16(num), sql.InRange, nil
	case sqltypes.Uint16:
		return convertToUint16(t, v)
	case sqltypes.Int24:
		num, _, err := convertToInt64(t, v)
		if err != nil {
			return nil, sql.OutOfRange, err
		}
		if num > (1<<23 - 1) {
			return int32(1<<23 - 1), sql.OutOfRange, nil
		} else if num < (-1 << 23) {
			return int32(-1 << 23), sql.OutOfRange, nil
		}
		return int32(num), sql.InRange, nil
	case sqltypes.Uint24:
		num, _, err := convertToInt64(t, v)
		if err != nil {
			return nil, sql.OutOfRange, err
		}
		if num >= (1 << 24) {
			return uint32(1<<24 - 1), sql.OutOfRange, nil
		} else if num < 0 {
			return uint32(1<<24 - int32(-num)), sql.OutOfRange, nil
		}
		return uint32(num), sql.InRange, nil
	case sqltypes.Int32:
		num, _, err := convertToInt64(t, v)
		if err != nil {
			return nil, sql.OutOfRange, err
		}
		if num > math.MaxInt32 {
			return int32(math.MaxInt32), sql.OutOfRange, nil
		} else if num < math.MinInt32 {
			return int32(math.MinInt32), sql.OutOfRange, nil
		}
		return int32(num), sql.InRange, nil
	case sqltypes.Uint32:
		return convertToUint32(t, v)
	case sqltypes.Int64:
		return convertToInt64(t, v)
	case sqltypes.Uint64:
		return convertToUint64(t, v)
	case sqltypes.Float32:
		num, err := convertToFloat64(t, v)
		if err != nil {
			return nil, sql.OutOfRange, err
		}
		if num > math.MaxFloat32 {
			return float32(math.MaxFloat32), sql.OutOfRange, nil
		} else if num < -math.MaxFloat32 {
			return float32(-math.MaxFloat32), sql.OutOfRange, nil
		}
		return float32(num), sql.InRange, nil
	case sqltypes.Float64:
		ret, err := convertToFloat64(t, v)
		return ret, sql.InRange, err
	default:
		return nil, sql.OutOfRange, sql.ErrInvalidType.New(t.baseType.String())
	}
}

// MaxTextResponseByteLength implements the Type interface
func (t NumberTypeImpl_) MaxTextResponseByteLength(_ *sql.Context) uint32 {
	// MySQL integer type limits: https://dev.mysql.com/doc/refman/8.0/en/integer-types.html
	// This is for a text response format, NOT a binary encoding
	switch t.baseType {
	case sqltypes.Uint8:
		return 3
	case sqltypes.Int8:
		return 4
	case sqltypes.Uint16:
		return 5
	case sqltypes.Int16:
		return 6
	case sqltypes.Uint24:
		return 8
	case sqltypes.Int24:
		return 9
	case sqltypes.Uint32:
		return 10
	case sqltypes.Int32:
		return 11
	case sqltypes.Uint64:
		return 20
	case sqltypes.Int64:
		return 20
	case sqltypes.Float32:
		return 12
	case sqltypes.Float64:
		return 22
	default:
		panic(fmt.Sprintf("%v is not a valid number base type", t.baseType.String()))
	}
}

// MustConvert implements the Type interface.
func (t NumberTypeImpl_) MustConvert(v interface{}) interface{} {
	value, _, err := t.Convert(v)
	if err != nil {
		panic(err)
	}
	return value
}

// Equals implements the Type interface.
func (t NumberTypeImpl_) Equals(otherType sql.Type) bool {
	return t.baseType == otherType.Type()
}

// Promote implements the Type interface.
func (t NumberTypeImpl_) Promote() sql.Type {
	switch t.baseType {
	case sqltypes.Int8, sqltypes.Int16, sqltypes.Int24, sqltypes.Int32, sqltypes.Int64:
		return Int64
	case sqltypes.Uint8, sqltypes.Uint16, sqltypes.Uint24, sqltypes.Uint32, sqltypes.Uint64:
		return Uint64
	case sqltypes.Float32, sqltypes.Float64:
		return Float64
	default:
		panic(sql.ErrInvalidBaseType.New(t.baseType.String(), "number"))
	}
}

// SQL implements Type interface.
func (t NumberTypeImpl_) SQL(ctx *sql.Context, dest []byte, v interface{}) (sqltypes.Value, error) {
	if v == nil {
		return sqltypes.NULL, nil
	}

	stop := len(dest)
	if vt, _, err := t.Convert(v); err == nil {
		switch t.baseType {
		case sqltypes.Int8, sqltypes.Int16, sqltypes.Int24, sqltypes.Int32, sqltypes.Int64:
			dest = strconv.AppendInt(dest, mustInt64(vt), 10)
		case sqltypes.Uint8, sqltypes.Uint16, sqltypes.Uint24, sqltypes.Uint32, sqltypes.Uint64:
			dest = strconv.AppendUint(dest, mustUint64(vt), 10)
		case sqltypes.Float32:
			dest = strconv.AppendFloat(dest, mustFloat64(vt), 'g', -1, 32)
		case sqltypes.Float64:
			dest = strconv.AppendFloat(dest, mustFloat64(vt), 'g', -1, 64)
		default:
			panic(sql.ErrInvalidBaseType.New(t.baseType.String(), "number"))
		}
	} else if sql.ErrInvalidValue.Is(err) {
		switch str := v.(type) {
		case []byte:
			dest = str
		case string:
			dest = []byte(str)
		default:
			return sqltypes.Value{}, err
		}
	} else {
		return sqltypes.Value{}, err
	}

	val := dest[stop:]

	return sqltypes.MakeTrusted(t.baseType, val), nil
}

func (t NumberTypeImpl_) Compare2(a sql.Value, b sql.Value) (int, error) {
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

func (t NumberTypeImpl_) Convert2(value sql.Value) (sql.Value, error) {
	panic("implement me")
}

func (t NumberTypeImpl_) Zero2() sql.Value {
	switch t.baseType {
	case sqltypes.Int8:
		x := values.WriteInt8(make([]byte, values.Int8Size), 0)
		return sql.Value{
			Typ: query.Type_INT8,
			Val: x,
		}
	case sqltypes.Int16:
		x := values.WriteInt16(make([]byte, values.Int16Size), 0)
		return sql.Value{
			Typ: query.Type_INT16,
			Val: x,
		}
	case sqltypes.Int24:
		x := values.WriteInt24(make([]byte, values.Int24Size), 0)
		return sql.Value{
			Typ: query.Type_INT24,
			Val: x,
		}
	case sqltypes.Int32:
		x := values.WriteInt32(make([]byte, values.Int32Size), 0)
		return sql.Value{
			Typ: query.Type_INT32,
			Val: x,
		}
	case sqltypes.Int64:
		x := values.WriteInt64(make([]byte, values.Int64Size), 0)
		return sql.Value{
			Typ: query.Type_INT64,
			Val: x,
		}
	case sqltypes.Uint8:
		x := values.WriteUint8(make([]byte, values.Uint8Size), 0)
		return sql.Value{
			Typ: query.Type_UINT8,
			Val: x,
		}
	case sqltypes.Uint16:
		x := values.WriteUint16(make([]byte, values.Uint16Size), 0)
		return sql.Value{
			Typ: query.Type_UINT16,
			Val: x,
		}
	case sqltypes.Uint24:
		x := values.WriteUint24(make([]byte, values.Uint24Size), 0)
		return sql.Value{
			Typ: query.Type_UINT24,
			Val: x,
		}
	case sqltypes.Uint32:
		x := values.WriteUint32(make([]byte, values.Uint32Size), 0)
		return sql.Value{
			Typ: query.Type_UINT32,
			Val: x,
		}
	case sqltypes.Uint64:
		x := values.WriteUint64(make([]byte, values.Uint64Size), 0)
		return sql.Value{
			Typ: query.Type_UINT64,
			Val: x,
		}
	case sqltypes.Float32:
		x := values.WriteFloat32(make([]byte, values.Float32Size), 0)
		return sql.Value{
			Typ: query.Type_FLOAT32,
			Val: x,
		}
	case sqltypes.Float64:
		x := values.WriteUint64(make([]byte, values.Uint64Size), 0)
		return sql.Value{
			Typ: query.Type_UINT64,
			Val: x,
		}
	default:
		panic(sql.ErrInvalidBaseType.New(t.baseType.String(), "number"))
	}
}

// SQL2 implements Type2 interface.
func (t NumberTypeImpl_) SQL2(v sql.Value) (sqltypes.Value, error) {
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
		panic(sql.ErrInvalidBaseType.New(t.baseType.String(), "number"))
	}

	return sqltypes.MakeTrusted(t.baseType, val), nil
}

// String implements Type interface.
func (t NumberTypeImpl_) String() string {
	switch t.baseType {
	case sqltypes.Int8:
		// MySQL 8.1.0 only honors display width for signed TINYINT fields
		if t.displayWidth != 0 {
			return fmt.Sprintf("tinyint(%d)", t.displayWidth)
		}
		return "tinyint"
	case sqltypes.Uint8:
		return "tinyint unsigned"
	case sqltypes.Int16:
		return "smallint"
	case sqltypes.Uint16:
		return "smallint unsigned"
	case sqltypes.Int24:
		return "mediumint"
	case sqltypes.Uint24:
		return "mediumint unsigned"
	case sqltypes.Int32:
		return "int"
	case sqltypes.Uint32:
		return "int unsigned"
	case sqltypes.Int64:
		return "bigint"
	case sqltypes.Uint64:
		return "bigint unsigned"
	case sqltypes.Float32:
		return "float"
	case sqltypes.Float64:
		return "double"
	default:
		panic(fmt.Sprintf("%v is not a valid number base type", t.baseType.String()))
	}
}

// Type implements Type interface.
func (t NumberTypeImpl_) Type() query.Type {
	return t.baseType
}

// ValueType implements Type interface.
func (t NumberTypeImpl_) ValueType() reflect.Type {
	switch t.baseType {
	case sqltypes.Int8:
		return numberInt8ValueType
	case sqltypes.Uint8:
		return numberUint8ValueType
	case sqltypes.Int16:
		return numberInt16ValueType
	case sqltypes.Uint16:
		return numberUint16ValueType
	case sqltypes.Int24:
		return numberInt32ValueType
	case sqltypes.Uint24:
		return numberUint32ValueType
	case sqltypes.Int32:
		return numberInt32ValueType
	case sqltypes.Uint32:
		return numberUint32ValueType
	case sqltypes.Int64:
		return numberInt64ValueType
	case sqltypes.Uint64:
		return numberUint64ValueType
	case sqltypes.Float32:
		return numberFloat32ValueType
	case sqltypes.Float64:
		return numberFloat64ValueType
	default:
		panic(fmt.Sprintf("%v is not a valid number base type", t.baseType.String()))
	}
}

// Zero implements Type interface.
func (t NumberTypeImpl_) Zero() interface{} {
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

// CollationCoercibility implements sql.CollationCoercible interface.
func (NumberTypeImpl_) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 5
}

// IsFloat implements NumberType interface.
func (t NumberTypeImpl_) IsFloat() bool {
	switch t.baseType {
	case sqltypes.Float32, sqltypes.Float64:
		return true
	}
	return false
}

// IsSigned implements NumberType interface.
func (t NumberTypeImpl_) IsSigned() bool {
	switch t.baseType {
	case sqltypes.Int8, sqltypes.Int16, sqltypes.Int24, sqltypes.Int32, sqltypes.Int64, sqltypes.Float32, sqltypes.Float64:
		return true
	}
	return false
}

// DisplayWidth() implements NumberType inteface.
func (t NumberTypeImpl_) DisplayWidth() int {
	return t.displayWidth
}

func convertToInt64(t NumberTypeImpl_, v interface{}) (int64, sql.ConvertInRange, error) {
	switch v := v.(type) {
	case int:
		return int64(v), sql.InRange, nil
	case int8:
		return int64(v), sql.InRange, nil
	case int16:
		return int64(v), sql.InRange, nil
	case int32:
		return int64(v), sql.InRange, nil
	case int64:
		return v, sql.InRange, nil
	case uint:
		return int64(v), sql.InRange, nil
	case uint8:
		return int64(v), sql.InRange, nil
	case uint16:
		return int64(v), sql.InRange, nil
	case uint32:
		return int64(v), sql.InRange, nil
	case uint64:
		if v > math.MaxInt64 {
			return math.MaxInt64, sql.OutOfRange, nil
		}
		return int64(v), sql.InRange, nil
	case float32:
		if v > float32(math.MaxInt64) {
			return math.MaxInt64, sql.OutOfRange, nil
		} else if v < float32(math.MinInt64) {
			return math.MinInt64, sql.OutOfRange, nil
		}
		return int64(math.Round(float64(v))), sql.OutOfRange, nil
	case float64:
		if v > float64(math.MaxInt64) {
			return math.MaxInt64, sql.OutOfRange, nil
		} else if v < float64(math.MinInt64) {
			return math.MinInt64, sql.OutOfRange, nil
		}
		return int64(math.Round(v)), sql.InRange, nil
	case decimal.Decimal:
		if v.GreaterThan(dec_int64_max) {
			return dec_int64_max.IntPart(), sql.OutOfRange, nil
		} else if v.LessThan(dec_int64_min) {
			return dec_int64_min.IntPart(), sql.OutOfRange, nil
		}
		return v.Round(0).IntPart(), sql.InRange, nil
	case []byte:
		i, err := strconv.ParseInt(hex.EncodeToString(v), 16, 64)
		if err != nil {
			return 0, sql.OutOfRange, sql.ErrInvalidValue.New(v, t.String())
		}
		return i, sql.InRange, nil
	case string:
		// Parse first an integer, which allows for more values than float64
		i, err := strconv.ParseInt(v, 10, 64)
		if err == nil {
			return i, sql.InRange, nil
		}
		// If that fails, try as a float and truncate it to integral
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return 0, sql.OutOfRange, sql.ErrInvalidValue.New(v, t.String())
		}
		return int64(f), sql.InRange, nil
	case bool:
		if v {
			return 1, sql.InRange, nil
		}
		return 0, sql.InRange, nil
	case nil:
		return 0, sql.InRange, nil
	default:
		return 0, sql.OutOfRange, sql.ErrInvalidValueType.New(v, t.String())
	}
}

func convertValueToInt64(t NumberTypeImpl_, v sql.Value) (int64, error) {
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
			return math.MaxInt64, nil
		}
		return int64(v), nil
	case query.Type_FLOAT32:
		v := values.ReadFloat32(v.Val)
		if v > float32(math.MaxInt64) {
			return math.MaxInt64, nil
		} else if v < float32(math.MinInt64) {
			return math.MinInt64, nil
		}
		return int64(math.Round(float64(v))), nil
	case query.Type_FLOAT64:
		v := values.ReadFloat64(v.Val)
		if v > float64(math.MaxInt64) {
			return math.MaxInt64, nil
		} else if v < float64(math.MinInt64) {
			return math.MinInt64, nil
		}
		return int64(math.Round(v)), nil
		// TODO: add more conversions
	default:
		panic(sql.ErrInvalidBaseType.New(t.baseType.String(), "number"))
	}
}

func convertValueToUint64(t NumberTypeImpl_, v sql.Value) (uint64, error) {
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
		if v >= float32(math.MaxUint64) {
			return math.MaxUint64, nil
		}
		return uint64(math.Round(float64(v))), nil
	case query.Type_FLOAT64:
		v := values.ReadFloat64(v.Val)
		if v >= float64(math.MaxUint64) {
			return math.MaxUint64, nil
		}
		return uint64(math.Round(v)), nil
		// TODO: add more conversions
	default:
		panic(sql.ErrInvalidBaseType.New(t.baseType.String(), "number"))
	}
}

func convertToUint64(t NumberTypeImpl_, v interface{}) (uint64, sql.ConvertInRange, error) {
	switch v := v.(type) {
	case int:
		if v < 0 {
			return uint64(math.MaxUint64 - uint(-v-1)), sql.OutOfRange, nil
		}
		return uint64(v), sql.InRange, nil
	case int8:
		if v < 0 {
			return uint64(math.MaxUint64 - uint(-v-1)), sql.OutOfRange, nil
		}
		return uint64(v), sql.InRange, nil
	case int16:
		if v < 0 {
			return uint64(math.MaxUint64 - uint(-v-1)), sql.OutOfRange, nil
		}
		return uint64(v), sql.InRange, nil
	case int32:
		if v < 0 {
			return uint64(math.MaxUint64 - uint(-v-1)), sql.OutOfRange, nil
		}
		return uint64(v), sql.InRange, nil
	case int64:
		if v < 0 {
			return uint64(math.MaxUint64 - uint(-v-1)), sql.OutOfRange, nil
		}
		return uint64(v), sql.InRange, nil
	case uint:
		return uint64(v), sql.InRange, nil
	case uint8:
		return uint64(v), sql.InRange, nil
	case uint16:
		return uint64(v), sql.InRange, nil
	case uint32:
		return uint64(v), sql.InRange, nil
	case uint64:
		return v, sql.InRange, nil
	case float32:
		if v > float32(math.MaxInt64) {
			return math.MaxUint64, sql.OutOfRange, nil
		} else if v < 0 {
			return uint64(math.MaxUint64 - v), sql.OutOfRange, nil
		}
		return uint64(math.Round(float64(v))), sql.InRange, nil
	case float64:
		if v >= float64(math.MaxUint64) {
			return math.MaxUint64, sql.OutOfRange, nil
		} else if v <= 0 {
			return uint64(math.MaxUint64 - v), sql.OutOfRange, nil
		}
		return uint64(math.Round(v)), sql.InRange, nil
	case decimal.Decimal:
		if v.GreaterThan(dec_uint64_max) {
			return math.MaxUint64, sql.InRange, nil
		} else if v.LessThan(dec_zero) {
			ret, _ := dec_uint64_max.Sub(v).Float64()
			return uint64(math.Round(ret)), sql.OutOfRange, nil
		}
		// TODO: If we ever internally switch to using Decimal for large numbers, this will need to be updated
		f, _ := v.Float64()
		return uint64(math.Round(f)), sql.InRange, nil
	case []byte:
		i, err := strconv.ParseUint(hex.EncodeToString(v), 16, 64)
		if err != nil {
			return 0, sql.OutOfRange, sql.ErrInvalidValue.New(v, t.String())
		}
		return i, sql.InRange, nil
	case string:
		i, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			return 0, sql.OutOfRange, sql.ErrInvalidValue.New(v, t.String())
		}
		return i, sql.InRange, nil
	case bool:
		if v {
			return 1, sql.InRange, nil
		}
		return 0, sql.InRange, nil
	case nil:
		return 0, sql.InRange, nil
	default:
		return 0, sql.OutOfRange, sql.ErrInvalidValueType.New(v, t.String())
	}
}

func convertToUint32(t NumberTypeImpl_, v interface{}) (uint32, sql.ConvertInRange, error) {
	switch v := v.(type) {
	case int:
		if v < 0 {
			return uint32(math.MaxUint32 - uint(-v-1)), sql.OutOfRange, nil
		} else if v > math.MaxUint32 {
			return uint32(math.MaxUint32), sql.OutOfRange, nil
		}
		return uint32(v), sql.InRange, nil
	case int8:
		if v < 0 {
			return uint32(math.MaxUint32 - uint(-v-1)), sql.OutOfRange, nil
		} else if int(v) > math.MaxUint32 {
			return uint32(math.MaxUint32), sql.OutOfRange, nil
		}
		return uint32(v), sql.InRange, nil
	case int16:
		if v < 0 {
			return uint32(math.MaxUint32 - uint(-v-1)), sql.OutOfRange, nil
		} else if int(v) > math.MaxUint32 {
			return uint32(math.MaxUint32), sql.OutOfRange, nil
		}
		return uint32(v), sql.InRange, nil
	case int32:
		if v < 0 {
			return uint32(math.MaxUint32 + uint(v)), sql.OutOfRange, nil
		} else if int(v) > math.MaxUint32 {
			return uint32(math.MaxUint32), sql.OutOfRange, nil
		}
		return uint32(v), sql.InRange, nil
	case int64:
		if v < 0 {
			return uint32(math.MaxUint32 + uint(v)), sql.OutOfRange, nil
		} else if v > math.MaxUint32 {
			return uint32(math.MaxUint32), sql.OutOfRange, nil
		}
		return uint32(v), sql.InRange, nil
	case uint:
		return uint32(v), sql.InRange, nil
	case uint8:
		return uint32(v), sql.InRange, nil
	case uint16:
		return uint32(v), sql.InRange, nil
	case uint32:
		return v, sql.InRange, nil
	case uint64:
		return uint32(v), sql.InRange, nil
	case float64:
		if float32(v) > float32(math.MaxInt32) {
			return math.MaxUint32, sql.OutOfRange, nil
		} else if v < 0 {
			return uint32(math.MaxUint32 - v), sql.OutOfRange, nil
		}
		return uint32(math.Round(float64(v))), sql.InRange, nil
	case float32:
		if v >= float32(math.MaxUint32) {
			return math.MaxUint32, sql.OutOfRange, nil
		} else if v <= 0 {
			return uint32(math.MaxUint32 - v), sql.OutOfRange, nil
		}
		return uint32(math.Round(float64(v))), sql.InRange, nil
	case decimal.Decimal:
		if v.GreaterThan(dec_uint32_max) {
			return math.MaxUint32, sql.InRange, nil
		} else if v.LessThan(dec_zero) {
			ret, _ := dec_uint32_max.Sub(v).Float64()
			return uint32(math.Round(ret)), sql.OutOfRange, nil
		}
		// TODO: If we ever internally switch to using Decimal for large numbers, this will need to be updated
		f, _ := v.Float64()
		return uint32(math.Round(f)), sql.InRange, nil
	case []byte:
		i, err := strconv.ParseUint(hex.EncodeToString(v), 16, 32)
		if err != nil {
			return 0, sql.OutOfRange, sql.ErrInvalidValue.New(v, t.String())
		}
		return uint32(i), sql.InRange, nil
	case string:
		i, err := strconv.ParseUint(v, 10, 32)
		if err != nil {
			return 0, sql.OutOfRange, sql.ErrInvalidValue.New(v, t.String())
		}
		return uint32(i), sql.InRange, nil
	case bool:
		if v {
			return 1, sql.InRange, nil
		}
		return 0, sql.InRange, nil
	case nil:
		return 0, sql.InRange, nil
	default:
		return 0, sql.OutOfRange, sql.ErrInvalidValueType.New(v, t.String())
	}
}

func convertToUint16(t NumberTypeImpl_, v interface{}) (uint16, sql.ConvertInRange, error) {
	switch v := v.(type) {
	case int:
		if v < 0 {
			return uint16(math.MaxUint16 - uint(-v-1)), sql.OutOfRange, nil
		} else if v > math.MaxUint16 {
			return uint16(math.MaxUint16), sql.OutOfRange, nil
		}
		return uint16(v), sql.InRange, nil
	case int8:
		if v < 0 {
			return uint16(math.MaxUint16 - uint(-v-1)), sql.OutOfRange, nil
		}
		return uint16(v), sql.InRange, nil
	case int16:
		if v < 0 {
			return uint16(math.MaxUint16 - uint(-v-1)), sql.OutOfRange, nil
		}
		return uint16(v), sql.InRange, nil
	case int32:
		if v < 0 {
			return uint16(math.MaxUint16 - uint(-v-1)), sql.OutOfRange, nil
		} else if v > math.MaxUint16 {
			return uint16(math.MaxUint16), sql.OutOfRange, nil
		}
		return uint16(v), sql.InRange, nil
	case int64:
		if v < 0 {
			return uint16(math.MaxUint16 - uint(-v-1)), sql.OutOfRange, nil
		} else if v > math.MaxUint16 {
			return uint16(math.MaxUint16), sql.OutOfRange, nil
		}
		return uint16(v), sql.InRange, nil
	case uint:
		return uint16(v), sql.InRange, nil
	case uint8:
		return uint16(v), sql.InRange, nil
	case uint64:
		return uint16(v), sql.InRange, nil
	case uint32:
		return uint16(v), sql.InRange, nil
	case uint16:
		return v, sql.InRange, nil
	case float32:
		if v > float32(math.MaxInt16) {
			return math.MaxUint16, sql.OutOfRange, nil
		} else if v < 0 {
			return uint16(math.MaxUint16 - v), sql.OutOfRange, nil
		}
		return uint16(math.Round(float64(v))), sql.InRange, nil
	case float64:
		if v >= float64(math.MaxUint16) {
			return math.MaxUint16, sql.OutOfRange, nil
		} else if v <= 0 {
			return uint16(math.MaxUint16 - v), sql.OutOfRange, nil
		}
		return uint16(math.Round(v)), sql.InRange, nil
	case decimal.Decimal:
		if v.GreaterThan(dec_uint16_max) {
			return math.MaxUint16, sql.InRange, nil
		} else if v.LessThan(dec_zero) {
			ret, _ := dec_uint16_max.Sub(v).Float64()
			return uint16(math.Round(ret)), sql.OutOfRange, nil
		}
		// TODO: If we ever internally switch to using Decimal for large numbers, this will need to be updated
		f, _ := v.Float64()
		return uint16(math.Round(f)), sql.InRange, nil
	case []byte:
		i, err := strconv.ParseUint(hex.EncodeToString(v), 16, 16)
		if err != nil {
			return 0, sql.OutOfRange, sql.ErrInvalidValue.New(v, t.String())
		}
		return uint16(i), sql.InRange, nil
	case string:
		i, err := strconv.ParseUint(v, 10, 16)
		if err != nil {
			return 0, sql.OutOfRange, sql.ErrInvalidValue.New(v, t.String())
		}
		return uint16(i), sql.InRange, nil
	case bool:
		if v {
			return 1, sql.InRange, nil
		}
		return 0, sql.InRange, nil
	case nil:
		return 0, sql.InRange, nil
	default:
		return 0, sql.OutOfRange, sql.ErrInvalidValueType.New(v, t.String())
	}
}

func convertToUint8(t NumberTypeImpl_, v interface{}) (uint8, sql.ConvertInRange, error) {
	switch v := v.(type) {
	case int:
		if v < 0 {
			return uint8(math.MaxUint8 - uint(-v-1)), sql.OutOfRange, nil
		} else if v > math.MaxUint8 {
			return uint8(math.MaxUint8), sql.OutOfRange, nil
		}
		return uint8(v), sql.InRange, nil
	case int16:
		if v < 0 {
			return uint8(math.MaxUint8 - uint(-v-1)), sql.OutOfRange, nil
		} else if v > math.MaxUint8 {
			return uint8(math.MaxUint8), sql.OutOfRange, nil
		}
		return uint8(v), sql.InRange, nil
	case int8:
		if v < 0 {
			return uint8(math.MaxUint8 - uint(-v-1)), sql.OutOfRange, nil
		} else if int(v) > math.MaxUint8 {
			return uint8(math.MaxUint8), sql.OutOfRange, nil
		}
		return uint8(v), sql.InRange, nil
	case int32:
		if v < 0 {
			return uint8(math.MaxUint8 - uint(-v-1)), sql.OutOfRange, nil
		} else if v > math.MaxUint8 {
			return uint8(math.MaxUint8), sql.OutOfRange, nil
		}
		return uint8(v), sql.InRange, nil
	case int64:
		if v < 0 {
			return uint8(math.MaxUint8 - uint(-v-1)), sql.OutOfRange, nil
		} else if v > math.MaxUint8 {
			return uint8(math.MaxUint8), sql.OutOfRange, nil
		}
		return uint8(v), sql.InRange, nil
	case uint:
		return uint8(v), sql.InRange, nil
	case uint16:
		return uint8(v), sql.InRange, nil
	case uint64:
		return uint8(v), sql.InRange, nil
	case uint32:
		return uint8(v), sql.InRange, nil
	case uint8:
		return v, sql.InRange, nil
	case float32:
		if v > float32(math.MaxInt8) {
			return math.MaxUint8, sql.OutOfRange, nil
		} else if v < 0 {
			return uint8(math.MaxUint8 - v), sql.OutOfRange, nil
		}
		return uint8(math.Round(float64(v))), sql.InRange, nil
	case float64:
		if v >= float64(math.MaxUint8) {
			return math.MaxUint8, sql.OutOfRange, nil
		} else if v <= 0 {
			return uint8(math.MaxUint8 - v), sql.OutOfRange, nil
		}
		return uint8(math.Round(v)), sql.InRange, nil
	case decimal.Decimal:
		if v.GreaterThan(dec_uint8_max) {
			return math.MaxUint8, sql.InRange, nil
		} else if v.LessThan(dec_zero) {
			ret, _ := dec_uint8_max.Sub(v).Float64()
			return uint8(math.Round(ret)), sql.OutOfRange, nil
		}
		// TODO: If we ever internally switch to using Decimal for large numbers, this will need to be updated
		f, _ := v.Float64()
		return uint8(math.Round(f)), sql.InRange, nil
	case []byte:
		i, err := strconv.ParseUint(hex.EncodeToString(v), 8, 8)
		if err != nil {
			return 0, sql.OutOfRange, sql.ErrInvalidValue.New(v, t.String())
		}
		return uint8(i), sql.InRange, nil
	case string:
		i, err := strconv.ParseUint(v, 10, 8)
		if err != nil {
			return 0, sql.OutOfRange, sql.ErrInvalidValue.New(v, t.String())
		}
		return uint8(i), sql.InRange, nil
	case bool:
		if v {
			return 1, sql.InRange, nil
		}
		return 0, sql.InRange, nil
	case nil:
		return 0, sql.InRange, nil
	default:
		return 0, sql.OutOfRange, sql.ErrInvalidValueType.New(v, t.String())
	}
}

func convertToFloat64(t NumberTypeImpl_, v interface{}) (float64, error) {
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
			return 0, sql.ErrInvalidValue.New(v, t.String())
		}
		return float64(i), nil
	case string:
		i, err := strconv.ParseFloat(v, 64)
		if err != nil {
			// parse the first longest valid numbers
			s := numre.FindString(v)
			i, _ = strconv.ParseFloat(s, 64)
			return i, sql.ErrInvalidValue.New(v, t.String())
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
		return 0, sql.ErrInvalidValueType.New(v, t.String())
	}
}

func convertValueToFloat64(t NumberTypeImpl_, v sql.Value) (float64, error) {
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
		panic(sql.ErrInvalidBaseType.New(t.baseType.String(), "number"))
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
		return tv
	case uint:
		return int64(tv)
	case uint8:
		return int64(tv)
	case uint16:
		return int64(tv)
	case uint32:
		return int64(tv)
	case uint64:
		return int64(tv)
	case bool:
		if tv {
			return int64(1)
		}
		return int64(0)
	case float32:
		return int64(tv)
	case float64:
		return int64(tv)
	default:
		panic(fmt.Sprintf("unexpected type %v", v))
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
		return tv
	case int:
		return uint64(tv)
	case int8:
		return uint64(tv)
	case int16:
		return uint64(tv)
	case int32:
		return uint64(tv)
	case int64:
		return uint64(tv)
	case bool:
		if tv {
			return uint64(1)
		}
		return uint64(0)
	case float32:
		return uint64(tv)
	case float64:
		return uint64(tv)
	default:
		panic(fmt.Sprintf("unexpected type %v", v))
	}
}

func mustFloat64(v interface{}) float64 {
	switch tv := v.(type) {
	case uint:
		return float64(tv)
	case uint8:
		return float64(tv)
	case uint16:
		return float64(tv)
	case uint32:
		return float64(tv)
	case uint64:
		return float64(tv)
	case int:
		return float64(tv)
	case int8:
		return float64(tv)
	case int16:
		return float64(tv)
	case int32:
		return float64(tv)
	case int64:
		return float64(tv)
	case bool:
		if tv {
			return float64(1)
		}
		return float64(0)
	case float32:
		return float64(tv)
	case float64:
		return tv
	default:
		panic(fmt.Sprintf("unexpected type %v", v))
	}
}

func isString(v interface{}) bool {
	switch v.(type) {
	case []byte, string:
		return true
	default:
		return false
	}
}
