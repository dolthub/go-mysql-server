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
	"encoding/binary"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/dolthub/vitess/go/vt/proto/query"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
)

func TestNumberCompare(t *testing.T) {
	ctx := sql.NewEmptyContext()
	tests := []struct {
		typ         sql.Type
		val1        interface{}
		val2        interface{}
		expectedCmp int
	}{
		{Int8, nil, 0, 1},
		{Uint24, 0, nil, -1},
		{Float64, nil, nil, 0},

		{Boolean, 0, 1, -1},
		{Int8, -1, 2, -1},
		{Int16, -2, 3, -1},
		{Int24, -3, 4, -1},
		{Int32, -4, 5, -1},
		{Int64, -5, 6, -1},
		{Uint8, 6, 7, -1},
		{Uint16, 7, 8, -1},
		{Uint24, 8, 9, -1},
		{Uint32, 9, 10, -1},
		{Uint64, 10, 11, -1},
		{Float32, -11.1, 12.2, -1},
		{Float64, -12.2, 13.3, -1},
		{Boolean, 0, 0, 0},
		{Int8, 1, 1, 0},
		{Int16, 2, 2, 0},
		{Int24, 3, 3, 0},
		{Int32, 4, 4, 0},
		{Int64, 5, 5, 0},
		{Uint8, 6, 6, 0},
		{Uint16, 7, 7, 0},
		{Uint24, 8, 8, 0},
		{Uint32, 9, 9, 0},
		{Uint64, 10, 10, 0},
		{Float32, 11.1, 11.1, 0},
		{Float64, 12.2, 12.2, 0},
		{Boolean, 1, 0, 1},
		{Int8, 2, -1, 1},
		{Int16, 3, -2, 1},
		{Int24, 4, -3, 1},
		{Int32, 5, -4, 1},
		{Int64, 6, -5, 1},
		{Uint8, 7, 6, 1},
		{Uint16, 8, 7, 1},
		{Uint24, 9, 8, 1},
		{Uint32, 10, 9, 1},
		{Uint64, 11, 10, 1},
		{Float32, 12.2, -11.1, 1},
		{Float64, 13.3, -12.2, 1},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%v %v %v", test.typ, test.val1, test.val2), func(t *testing.T) {
			cmp, err := test.typ.Compare(ctx, test.val1, test.val2)
			require.NoError(t, err)
			assert.Equal(t, test.expectedCmp, cmp)
		})
	}
}

func TestNumberCreate(t *testing.T) {
	tests := []struct {
		baseType     query.Type
		expectedType NumberTypeImpl_
		expectedErr  bool
	}{
		{sqltypes.Int8, NumberTypeImpl_{sqltypes.Int8, 0}, false},
		{sqltypes.Int16, NumberTypeImpl_{sqltypes.Int16, 0}, false},
		{sqltypes.Int24, NumberTypeImpl_{sqltypes.Int24, 0}, false},
		{sqltypes.Int32, NumberTypeImpl_{sqltypes.Int32, 0}, false},
		{sqltypes.Int64, NumberTypeImpl_{sqltypes.Int64, 0}, false},
		{sqltypes.Uint8, NumberTypeImpl_{sqltypes.Uint8, 0}, false},
		{sqltypes.Uint16, NumberTypeImpl_{sqltypes.Uint16, 0}, false},
		{sqltypes.Uint24, NumberTypeImpl_{sqltypes.Uint24, 0}, false},
		{sqltypes.Uint32, NumberTypeImpl_{sqltypes.Uint32, 0}, false},
		{sqltypes.Uint64, NumberTypeImpl_{sqltypes.Uint64, 0}, false},
		{sqltypes.Float32, NumberTypeImpl_{sqltypes.Float32, 0}, false},
		{sqltypes.Float64, NumberTypeImpl_{sqltypes.Float64, 0}, false},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%v", test.baseType), func(t *testing.T) {
			typ, err := CreateNumberType(test.baseType)
			if test.expectedErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, test.expectedType, typ)
			}
		})
	}
}

func TestNumberCreateInvalidBaseTypes(t *testing.T) {
	tests := []struct {
		baseType     query.Type
		expectedType NumberTypeImpl_
		expectedErr  bool
	}{
		{sqltypes.Binary, NumberTypeImpl_{}, true},
		{sqltypes.Bit, NumberTypeImpl_{}, true},
		{sqltypes.Blob, NumberTypeImpl_{}, true},
		{sqltypes.Char, NumberTypeImpl_{}, true},
		{sqltypes.Date, NumberTypeImpl_{}, true},
		{sqltypes.Datetime, NumberTypeImpl_{}, true},
		{sqltypes.Decimal, NumberTypeImpl_{}, true},
		{sqltypes.Enum, NumberTypeImpl_{}, true},
		{sqltypes.Expression, NumberTypeImpl_{}, true},
		{sqltypes.Geometry, NumberTypeImpl_{}, true},
		{sqltypes.Null, NumberTypeImpl_{}, true},
		{sqltypes.Set, NumberTypeImpl_{}, true},
		{sqltypes.Text, NumberTypeImpl_{}, true},
		{sqltypes.Time, NumberTypeImpl_{}, true},
		{sqltypes.Timestamp, NumberTypeImpl_{}, true},
		{sqltypes.TypeJSON, NumberTypeImpl_{}, true},
		{sqltypes.VarBinary, NumberTypeImpl_{}, true},
		{sqltypes.VarChar, NumberTypeImpl_{}, true},
		{sqltypes.Year, NumberTypeImpl_{}, true},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%v", test.baseType), func(t *testing.T) {
			typ, err := CreateNumberType(test.baseType)
			if test.expectedErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, test.expectedType, typ)
			}
		})
	}
}

func TestNumberConvert(t *testing.T) {
	ctx := sql.NewEmptyContext()
	tests := []struct {
		typ     sql.Type
		inp     interface{}
		exp     interface{}
		err     bool
		inRange sql.ConvertInRange
	}{
		{typ: Boolean, inp: true, exp: int8(1), err: false, inRange: sql.InRange},
		{typ: Int8, inp: int32(0), exp: int8(0), err: false, inRange: sql.InRange},
		{typ: Int16, inp: uint16(1), exp: int16(1), err: false, inRange: sql.InRange},
		{typ: Int24, inp: false, exp: int32(0), err: false, inRange: sql.InRange},
		{typ: Int32, inp: nil, exp: nil, err: false, inRange: sql.InRange},
		{typ: Int32, inp: 2147483647, exp: int32(2147483647), err: false, inRange: sql.InRange},
		{typ: Int64, inp: "33", exp: int64(33), err: false, inRange: sql.InRange},
		{typ: Int64, inp: "33.0", exp: int64(33), err: false, inRange: sql.InRange},
		{typ: Int64, inp: "33.1", exp: int64(33), err: false, inRange: sql.InRange},
		{typ: Int64, inp: strconv.FormatInt(math.MaxInt64, 10), exp: int64(math.MaxInt64), err: false, inRange: sql.InRange},
		{typ: Int64, inp: true, exp: int64(1), err: false, inRange: sql.InRange},
		{typ: Int64, inp: false, exp: int64(0), err: false, inRange: sql.InRange},
		{typ: Uint8, inp: int64(34), exp: uint8(34), err: false, inRange: sql.InRange},
		{typ: Uint16, inp: int16(35), exp: uint16(35), err: false, inRange: sql.InRange},
		{typ: Uint24, inp: 36.756, exp: uint32(37), err: false, inRange: sql.InRange},
		{typ: Uint32, inp: uint8(37), exp: uint32(37), err: false, inRange: sql.InRange},
		{typ: Uint64, inp: time.Date(2009, 1, 2, 3, 4, 5, 0, time.UTC), exp: uint64(time.Date(2009, 1, 2, 3, 4, 5, 0, time.UTC).Unix()), err: false, inRange: sql.InRange},
		{typ: Uint64, inp: "01000", exp: uint64(1000), err: false, inRange: sql.InRange},
		{typ: Uint64, inp: true, exp: uint64(1), err: false, inRange: sql.InRange},
		{typ: Uint64, inp: false, exp: uint64(0), err: false, inRange: sql.InRange},
		{typ: Uint64, inp: "123.9abc", exp: uint64(123), err: false, inRange: sql.InRange},
		{typ: Uint64, inp: "+123.9abc", exp: uint64(123), err: false, inRange: sql.InRange},
		{typ: Float32, inp: "22.25", exp: float32(22.25), err: false, inRange: sql.InRange},
		{typ: Float32, inp: []byte{90, 140, 228, 206, 116}, exp: float32(388910861940), err: false, inRange: sql.InRange},
		{typ: Float64, inp: float32(893.875), exp: float64(893.875), err: false, inRange: sql.InRange},
		{typ: Boolean, inp: math.MaxInt8 + 1, exp: int8(math.MaxInt8), err: false, inRange: sql.Overflow},
		{typ: Int8, inp: math.MaxInt8 + 1, exp: int8(math.MaxInt8), err: false, inRange: sql.Overflow},
		{typ: Int8, inp: math.MinInt8 - 1, exp: int8(math.MinInt8), err: false, inRange: sql.Underflow},
		{typ: Int16, inp: math.MaxInt16 + 1, exp: int16(math.MaxInt16), err: false, inRange: sql.Overflow},
		{typ: Int16, inp: math.MinInt16 - 1, exp: int16(math.MinInt16), err: false, inRange: sql.Underflow},
		{typ: Int24, inp: 1 << 24, exp: int32(1<<23 - 1), err: false, inRange: sql.Overflow},
		{typ: Int24, inp: -1 << 24, exp: int32(-1 << 23), err: false, inRange: sql.Underflow},
		{typ: Int32, inp: math.MaxInt32 + 1, exp: int32(math.MaxInt32), err: false, inRange: sql.Overflow},
		{typ: Int32, inp: math.MinInt32 - 1, exp: int32(math.MinInt32), err: false, inRange: sql.Underflow},
		{typ: Int64, inp: uint64(math.MaxInt64 + 1), exp: int64(math.MaxInt64), err: false, inRange: sql.Overflow},
		{typ: Uint8, inp: math.MaxUint8 + 1, exp: uint8(math.MaxUint8), err: false, inRange: sql.Overflow},
		{typ: Uint8, inp: -1, exp: uint8(math.MaxUint8), err: false, inRange: sql.Underflow},
		{typ: Uint16, inp: math.MaxUint16 + 1, exp: uint16(math.MaxUint16), err: false, inRange: sql.Overflow},
		{typ: Uint16, inp: -1, exp: uint16(math.MaxUint16), err: false, inRange: sql.Underflow},
		{typ: Uint24, inp: 1 << 24, exp: uint32(1<<24 - 1), err: false, inRange: sql.Overflow},
		{typ: Uint24, inp: -1, exp: uint32(1<<24 - 1), err: false, inRange: sql.Underflow},
		{typ: Uint32, inp: math.MaxUint32 + 1, exp: uint32(math.MaxUint32), err: false, inRange: sql.Overflow},
		{typ: Uint32, inp: -1, exp: uint32(math.MaxUint32), err: false, inRange: sql.Underflow},
		{typ: Uint64, inp: -1, exp: uint64(math.MaxUint64), err: false, inRange: sql.Underflow},
		{typ: Uint64, inp: "-1", exp: uint64(math.MaxUint64), err: false, inRange: sql.Underflow},
		{typ: Float32, inp: math.MaxFloat32 * 2, exp: float32(math.MaxFloat32), err: false, inRange: sql.Overflow},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%v %v %v", test.typ, test.inp, test.exp), func(t *testing.T) {
			val, inRange, err := test.typ.Convert(ctx, test.inp)
			if test.err {
				assert.Error(t, err)
			} else {
				if sql.ErrTruncatedIncorrect.Is(err) {
					err = nil
				}
				require.NoError(t, err)
				assert.Equal(t, test.exp, val)
				assert.Equal(t, test.inRange, inRange)
				if val != nil {
					assert.Equal(t, test.typ.ValueType(), reflect.TypeOf(val))
				}
			}
		})
	}
}

func TestNumberConvertRound(t *testing.T) {
	ctx := sql.NewEmptyContext()
	tests := []struct {
		typ     sql.Type
		inp     interface{}
		exp     interface{}
		err     bool
		inRange sql.ConvertInRange
	}{
		// Boolean, Int8, Uint8, ... all use convertToInt64
		{
			typ:     Int64,
			inp:     "",
			exp:     int64(0),
			err:     false,
			inRange: sql.InRange,
		},
		{
			typ:     Int64,
			inp:     " \t",
			exp:     int64(0),
			err:     false,
			inRange: sql.InRange,
		},
		{
			typ:     Int64,
			inp:     "!@#$%^&*()",
			exp:     int64(0),
			err:     true,
			inRange: sql.InRange,
		},
		{
			typ:     Int64,
			inp:     "1.1",
			exp:     int64(1),
			err:     false,
			inRange: sql.InRange,
		},
		{
			typ:     Int64,
			inp:     "1.9",
			exp:     int64(2),
			err:     false,
			inRange: sql.InRange,
		},
		{
			typ:     Int64,
			inp:     "100.1ABC",
			exp:     int64(100),
			err:     true,
			inRange: sql.InRange,
		},
		{
			typ:     Int64,
			inp:     "100.9ABC",
			exp:     int64(101),
			err:     true,
			inRange: sql.InRange,
		},
		{
			typ:     Int64,
			inp:     ".123ABC",
			exp:     int64(0),
			err:     true,
			inRange: sql.InRange,
		},
		{
			typ:     Int64,
			inp:     "1.1.1",
			exp:     int64(1),
			err:     true,
			inRange: sql.InRange,
		},
		{
			typ:     Int64,
			inp:     "+1",
			exp:     int64(1),
			err:     false,
			inRange: sql.InRange,
		},
		{
			typ:     Int64,
			inp:     "-1",
			exp:     int64(-1),
			err:     false,
			inRange: sql.InRange,
		},
		{
			typ:     Int64,
			inp:     "+ 1",
			exp:     int64(0),
			err:     true,
			inRange: sql.InRange,
		},
		{
			typ:     Int64,
			inp:     "- 1",
			exp:     int64(0),
			err:     true,
			inRange: sql.InRange,
		},
		{
			typ:     Int64,
			inp:     "+-+-1",
			exp:     int64(0),
			err:     false,
			inRange: sql.InRange,
		},

		{
			typ:     Uint64,
			inp:     "",
			exp:     uint64(0),
			err:     false,
			inRange: sql.InRange,
		},
		{
			typ:     Uint64,
			inp:     " \t",
			exp:     uint64(0),
			err:     false,
			inRange: sql.InRange,
		},
		{
			typ:     Uint64,
			inp:     "!@#$%^&*()",
			exp:     uint64(0),
			err:     true,
			inRange: sql.InRange,
		},
		{
			typ:     Uint64,
			inp:     "1.1",
			exp:     uint64(1),
			err:     false,
			inRange: sql.InRange,
		},
		{
			typ:     Uint64,
			inp:     "1.9",
			exp:     uint64(2),
			err:     false,
			inRange: sql.InRange,
		},
		{
			typ:     Uint64,
			inp:     "100.1ABC",
			exp:     uint64(100),
			err:     true,
			inRange: sql.InRange,
		},
		{
			typ:     Uint64,
			inp:     "100.9ABC",
			exp:     uint64(101),
			err:     true,
			inRange: sql.InRange,
		},
		{
			typ:     Uint64,
			inp:     ".123ABC",
			exp:     uint64(0),
			err:     true,
			inRange: sql.InRange,
		},
		{
			typ:     Uint64,
			inp:     "1.1.1",
			exp:     uint64(1),
			err:     true,
			inRange: sql.InRange,
		},
		{
			typ:     Uint64,
			inp:     "+1",
			exp:     uint64(1),
			err:     false,
			inRange: sql.InRange,
		},
		{
			typ:     Uint64,
			inp:     "-1",
			exp:     uint64(math.MaxUint64),
			err:     false,
			inRange: sql.Underflow,
		},
		{
			typ:     Uint64,
			inp:     "+ 1",
			exp:     uint64(0),
			err:     true,
			inRange: sql.InRange,
		},
		{
			typ:     Uint64,
			inp:     "- 1",
			exp:     uint64(0),
			err:     true,
			inRange: sql.InRange,
		},
		{
			typ:     Uint64,
			inp:     "+-+-1",
			exp:     uint64(0),
			err:     false,
			inRange: sql.InRange,
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%v %v %v", test.typ, test.inp, test.exp), func(t *testing.T) {
			val, inRange, err := test.typ.(sql.RoundingNumberType).ConvertRound(ctx, test.inp)
			if test.err {
				assert.True(t, sql.ErrTruncatedIncorrect.Is(err))
			}
			assert.Equal(t, test.exp, val)
			assert.Equal(t, test.inRange, inRange)
		})
	}
}

func TestNumberSQL_BooleanFromBoolean(t *testing.T) {
	val, err := Boolean.SQL(sql.NewEmptyContext(), nil, true)
	require.NoError(t, err)
	assert.Equal(t, "INT8(1)", val.String())

	val, err = Boolean.SQL(sql.NewEmptyContext(), nil, false)
	require.NoError(t, err)
	assert.Equal(t, "INT8(0)", val.String())
}

func TestNumberSQL_NumberFromString(t *testing.T) {
	val, err := Int64.SQL(sql.NewEmptyContext(), nil, "not a number")
	require.NoError(t, err)
	assert.Equal(t, "not a number", val.ToString())

	val, err = Float64.SQL(sql.NewEmptyContext(), nil, "also not a number")
	require.NoError(t, err)
	assert.Equal(t, "0", val.ToString())
}

func TestNumberString(t *testing.T) {
	tests := []struct {
		typ         sql.Type
		expectedStr string
	}{
		{Boolean, "tinyint(1)"},
		{Int8, "tinyint"},
		{Int16, "smallint"},
		{Int24, "mediumint"},
		{Int32, "int"},
		{Int64, "bigint"},
		{Uint8, "tinyint unsigned"},
		{Uint16, "smallint unsigned"},
		{Uint24, "mediumint unsigned"},
		{Uint32, "int unsigned"},
		{Uint64, "bigint unsigned"},
		{Float32, "float"},
		{Float64, "double"},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%v %v", test.typ, test.expectedStr), func(t *testing.T) {
			str := test.typ.String()
			assert.Equal(t, test.expectedStr, str)
		})
	}
}

func TestTruncateStringToInt(t *testing.T) {
	tests := []struct {
		input    string
		exp      string
		expTrunc bool
	}{
		{
			input:    "1",
			exp:      "1",
			expTrunc: false,
		},
		{
			// Whitespace does not count as truncation
			input:    " \t   1 \t  ",
			exp:      "1",
			expTrunc: false,
		},
		{
			// Newlines do count as part of truncation
			input:    " \t\n1",
			exp:      "0",
			expTrunc: true,
		},
		{
			input:    "123abc",
			exp:      "123",
			expTrunc: true,
		},
		{
			input:    "abc",
			exp:      "0",
			expTrunc: true,
		},
		{
			// Leading sign is fine
			input:    "+123",
			exp:      "+123",
			expTrunc: false,
		},
		{
			// Leading sign is fine
			input:    "-123",
			exp:      "-123",
			expTrunc: false,
		},
		{
			// Repeated signs
			input:    "+-+-+-123",
			exp:      "0",
			expTrunc: true,
		},
		{
			// Space after sign
			input:    "+ 123",
			exp:      "0",
			expTrunc: true,
		},
		{
			// Valid float strings are not valid ints
			input:    "1.23",
			exp:      "1",
			expTrunc: true,
		},
		{
			// Scientific float notation is not valid
			input:    "123.456e10",
			exp:      "123",
			expTrunc: true,
		},
		{
			// Scientific notation is not valid
			input:    "123e10",
			exp:      "123",
			expTrunc: true,
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%v", test.input), func(t *testing.T) {
			truncStr, didTrunc := TruncateStringToInt(test.input)
			assert.Equal(t, test.exp, truncStr)
			assert.Equal(t, test.expTrunc, didTrunc)
		})
	}
}

func TestTruncateStringToDouble(t *testing.T) {
	tests := []struct {
		input    string
		exp      string
		expTrunc bool
	}{
		{
			input:    "1",
			exp:      "1",
			expTrunc: false,
		},
		{
			// Whitespace does not count as truncation
			input:    " \t\n 1 \t\n ",
			exp:      "1",
			expTrunc: false,
		},
		{
			input:    "123abc",
			exp:      "123",
			expTrunc: true,
		},
		{
			input:    "abc",
			exp:      "0",
			expTrunc: true,
		},
		{
			// Leading sign is fine
			input:    "+123",
			exp:      "+123",
			expTrunc: false,
		},
		{
			// Leading sign is fine
			input:    "-123",
			exp:      "-123",
			expTrunc: false,
		},
		{
			// Repeated signs
			input:    "+-+-+-123",
			exp:      "0",
			expTrunc: true,
		},
		{
			// Space after sign
			input:    "+ 123",
			exp:      "0",
			expTrunc: true,
		},
		{
			// Valid float strings are not valid ints
			input:    "1.23",
			exp:      "1.23",
			expTrunc: false,
		},
		{
			// Scientific notation
			input:    "123.456e10",
			exp:      "123.456e10",
			expTrunc: false,
		},
		{
			// Scientific notation
			input:    "123e10",
			exp:      "123e10",
			expTrunc: false,
		},
		{
			// Scientific notation
			input:    "+123.456e-10",
			exp:      "+123.456e-10",
			expTrunc: false,
		},
		{
			// Scientific notation truncates
			input:    "+123.456e-10notaumber",
			exp:      "+123.456e-10",
			expTrunc: true,
		},
		{
			// Invalid Scientific notation
			input:    "e123",
			exp:      "0",
			expTrunc: true,
		},
		{
			// Invalid Scientific notation
			input:    ".e1",
			exp:      "0",
			expTrunc: true,
		},
		{
			// Invalid Scientific notation
			input:    "1e2e3",
			exp:      "1e2",
			expTrunc: true,
		},
		{
			input:    ".0e123",
			exp:      ".0e123",
			expTrunc: false,
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%v", test.input), func(t *testing.T) {
			truncStr, didTrunc := TruncateStringToDouble(test.input)
			assert.Equal(t, test.exp, truncStr)
			assert.Equal(t, test.expTrunc, didTrunc)
		})
	}
}

func serializeDecimal(dec decimal.Decimal) []byte {
	var res []byte
	coef := dec.Coefficient()
	res = binary.LittleEndian.AppendUint32(res, uint32(dec.Exponent()))
	res = append(res, byte(coef.Sign()))
	res = append(res, coef.Bytes()...)
	return res
}

func TestConvertValueToInt64(t *testing.T) {
	ctx := sql.NewEmptyContext()

	zeroDec := serializeDecimal(decimal.Zero)
	testDec := serializeDecimal(decimal.NewFromFloat(123.456))
	minInt64Dec := serializeDecimal(decimal.NewFromInt(math.MinInt64))
	maxInt64Dec := serializeDecimal(decimal.NewFromInt(math.MaxInt64))

	tests := []struct {
		val sql.Value
		exp int64
		rng sql.ConvertInRange
		err bool
	}{
		// Int8 -> Int64
		{
			val: sql.Value{
				Val: []byte{0},
				Typ: sqltypes.Int8,
			},
			exp: 0,
			rng: sql.InRange,
		},
		{
			val: sql.Value{
				Val: []byte{67},
				Typ: sqltypes.Int8,
			},
			exp: 67,
			rng: sql.InRange,
		},
		{
			val: sql.Value{
				Val: []byte{128},
				Typ: sqltypes.Int8,
			},
			exp: -128,
			rng: sql.InRange,
		},
		{
			val: sql.Value{
				Val: []byte{255},
				Typ: sqltypes.Int8,
			},
			exp: -1,
			rng: sql.InRange,
		},

		// Int16 -> Int64
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint16(nil, uint16(0)),
				Typ: sqltypes.Int16,
			},
			exp: 0,
			rng: sql.InRange,
		},
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint16(nil, uint16(67)),
				Typ: sqltypes.Int16,
			},
			exp: 67,
			rng: sql.InRange,
		},
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint16(nil, math.MaxInt16+1),
				Typ: sqltypes.Int16,
			},
			exp: math.MinInt16,
			rng: sql.InRange,
		},
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint16(nil, math.MaxInt16),
				Typ: sqltypes.Int16,
			},
			exp: math.MaxInt16,
			rng: sql.InRange,
		},
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint16(nil, math.MaxUint16),
				Typ: sqltypes.Int16,
			},
			exp: -1,
			rng: sql.InRange,
		},

		// Int32 -> Int64
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint32(nil, uint32(0)),
				Typ: sqltypes.Int32,
			},
			exp: 0,
			rng: sql.InRange,
		},
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint32(nil, uint32(67)),
				Typ: sqltypes.Int32,
			},
			exp: 67,
			rng: sql.InRange,
		},
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint32(nil, math.MaxInt32+1),
				Typ: sqltypes.Int32,
			},
			exp: math.MinInt32,
			rng: sql.InRange,
		},
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint32(nil, math.MaxInt32),
				Typ: sqltypes.Int32,
			},
			exp: math.MaxInt32,
			rng: sql.InRange,
		},
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint32(nil, math.MaxUint32),
				Typ: sqltypes.Int32,
			},
			exp: -1,
			rng: sql.InRange,
		},

		// Int64 -> Int64
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint64(nil, uint64(0)),
				Typ: sqltypes.Int64,
			},
			exp: 0,
			rng: sql.InRange,
		},
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint64(nil, uint64(67)),
				Typ: sqltypes.Int64,
			},
			exp: 67,
			rng: sql.InRange,
		},
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint64(nil, math.MaxInt64+1),
				Typ: sqltypes.Int64,
			},
			exp: math.MinInt64,
			rng: sql.InRange,
		},
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint64(nil, math.MaxInt64),
				Typ: sqltypes.Int64,
			},
			exp: math.MaxInt64,
			rng: sql.InRange,
		},
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint64(nil, math.MaxUint64),
				Typ: sqltypes.Int64,
			},
			exp: -1,
			rng: sql.InRange,
		},

		// Uint8 -> Int64
		{
			val: sql.Value{
				Val: []byte{0},
				Typ: sqltypes.Uint8,
			},
			exp: 0,
			rng: sql.InRange,
		},
		{
			val: sql.Value{
				Val: []byte{67},
				Typ: sqltypes.Uint8,
			},
			exp: 67,
			rng: sql.InRange,
		},
		{
			val: sql.Value{
				Val: []byte{128},
				Typ: sqltypes.Uint8,
			},
			exp: 128,
			rng: sql.InRange,
		},
		{
			val: sql.Value{
				Val: []byte{255},
				Typ: sqltypes.Uint8,
			},
			exp: 255,
			rng: sql.InRange,
		},

		// Uint16 -> Int64
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint16(nil, uint16(0)),
				Typ: sqltypes.Uint16,
			},
			exp: 0,
			rng: sql.InRange,
		},
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint16(nil, uint16(67)),
				Typ: sqltypes.Uint16,
			},
			exp: 67,
			rng: sql.InRange,
		},
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint16(nil, math.MaxInt16),
				Typ: sqltypes.Uint16,
			},
			exp: math.MaxInt16,
			rng: sql.InRange,
		},
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint16(nil, math.MaxUint16),
				Typ: sqltypes.Uint16,
			},
			exp: math.MaxUint16,
			rng: sql.InRange,
		},

		// Uint32 -> Int64
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint32(nil, uint32(0)),
				Typ: sqltypes.Uint32,
			},
			exp: 0,
			rng: sql.InRange,
		},
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint32(nil, uint32(67)),
				Typ: sqltypes.Uint32,
			},
			exp: 67,
			rng: sql.InRange,
		},
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint32(nil, math.MaxInt32),
				Typ: sqltypes.Uint32,
			},
			exp: math.MaxInt32,
			rng: sql.InRange,
		},
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint32(nil, math.MaxUint32),
				Typ: sqltypes.Uint32,
			},
			exp: math.MaxUint32,
			rng: sql.InRange,
		},

		// Uint64 -> Int64
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint64(nil, uint64(0)),
				Typ: sqltypes.Uint64,
			},
			exp: 0,
			rng: sql.InRange,
		},
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint64(nil, uint64(67)),
				Typ: sqltypes.Uint64,
			},
			exp: 67,
			rng: sql.InRange,
		},
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint64(nil, math.MaxInt64),
				Typ: sqltypes.Uint64,
			},
			exp: math.MaxInt64,
			rng: sql.InRange,
		},
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint64(nil, math.MaxUint64),
				Typ: sqltypes.Uint64,
			},
			exp: math.MaxInt64,
			rng: sql.Overflow,
		},

		// Float32 -> Int64
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint32(nil, math.Float32bits(0)),
				Typ: sqltypes.Float32,
			},
			exp: 0,
			rng: sql.InRange,
		},
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint32(nil, math.Float32bits(123.456)),
				Typ: sqltypes.Float32,
			},
			exp: 123,
			rng: sql.InRange,
		},
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint32(nil, math.Float32bits(-math.MaxFloat32)),
				Typ: sqltypes.Float32,
			},
			exp: math.MinInt64,
			rng: sql.Underflow,
		},
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint32(nil, math.Float32bits(math.MaxFloat32)),
				Typ: sqltypes.Float32,
			},
			exp: math.MaxInt64,
			rng: sql.Overflow,
		},

		// Float64 -> Int64
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint64(nil, math.Float64bits(0)),
				Typ: sqltypes.Float64,
			},
			exp: 0,
			rng: sql.InRange,
		},
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint64(nil, math.Float64bits(123.456)),
				Typ: sqltypes.Float64,
			},
			exp: 123,
			rng: sql.InRange,
		},
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint64(nil, math.Float64bits(-math.MaxFloat32)),
				Typ: sqltypes.Float64,
			},
			exp: math.MinInt64,
			rng: sql.Underflow,
		},
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint64(nil, math.Float64bits(math.MaxFloat32)),
				Typ: sqltypes.Float64,
			},
			exp: math.MaxInt64,
			rng: sql.Overflow,
		},
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint64(nil, math.Float64bits(-math.MaxFloat64)),
				Typ: sqltypes.Float64,
			},
			exp: math.MinInt64,
			rng: sql.Underflow,
		},
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint64(nil, math.Float64bits(math.MaxFloat64)),
				Typ: sqltypes.Float64,
			},
			exp: math.MaxInt64,
			rng: sql.Overflow,
		},

		// Decimal -> Int64
		{
			val: sql.Value{
				Val: zeroDec,
				Typ: sqltypes.Decimal,
			},
			exp: 0,
			rng: sql.InRange,
		},
		{
			val: sql.Value{
				Val: testDec,
				Typ: sqltypes.Decimal,
			},
			exp: 123,
			rng: sql.InRange,
		},
		{
			val: sql.Value{
				Val: minInt64Dec,
				Typ: sqltypes.Decimal,
			},
			exp: math.MinInt64,
			rng: sql.InRange,
		},
		{
			val: sql.Value{
				Val: maxInt64Dec,
				Typ: sqltypes.Decimal,
			},
			exp: math.MaxInt64,
			rng: sql.InRange,
		},

		// Bit -> Int64
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint64(nil, uint64(0)),
				Typ: sqltypes.Bit,
			},
			exp: 0,
			rng: sql.InRange,
		},
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint64(nil, uint64(67)),
				Typ: sqltypes.Bit,
			},
			exp: 67,
			rng: sql.InRange,
		},
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint64(nil, math.MaxInt64),
				Typ: sqltypes.Bit,
			},
			exp: math.MaxInt64,
			rng: sql.InRange,
		},
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint64(nil, math.MaxUint64),
				Typ: sqltypes.Bit,
			},
			exp: math.MaxInt64,
			rng: sql.Overflow,
		},

		// Year -> Int64
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint16(nil, uint16(0)),
				Typ: sqltypes.Year,
			},
			exp: 0,
			rng: sql.InRange,
		},
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint16(nil, uint16(1967)),
				Typ: sqltypes.Year,
			},
			exp: 1967,
			rng: sql.InRange,
		},
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint16(nil, uint16(1901)),
				Typ: sqltypes.Year,
			},
			exp: 1901,
			rng: sql.InRange,
		},
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint16(nil, uint16(2155)),
				Typ: sqltypes.Year,
			},
			exp: 2155,
			rng: sql.InRange,
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%v", test.val), func(t *testing.T) {
			res, rng, err := convertValueToInt64(ctx, test.val)
			if test.err {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, test.exp, res)
			require.Equal(t, test.rng, rng)
		})
	}
}

func TestConvertValueToUint64(t *testing.T) {
	ctx := sql.NewEmptyContext()

	zeroDec := serializeDecimal(decimal.Zero)
	testDec := serializeDecimal(decimal.NewFromFloat(123.456))
	maxInt64Dec := serializeDecimal(decimal.NewFromInt(math.MaxInt64))

	tests := []struct {
		val sql.Value
		exp uint64
		rng sql.ConvertInRange
		err bool
	}{
		// Int8 -> Uint64
		{
			val: sql.Value{
				Val: []byte{0},
				Typ: sqltypes.Int8,
			},
			exp: 0,
			rng: sql.InRange,
		},
		{
			val: sql.Value{
				Val: []byte{67},
				Typ: sqltypes.Int8,
			},
			exp: 67,
			rng: sql.InRange,
		},
		{
			val: sql.Value{
				Val: []byte{127},
				Typ: sqltypes.Int8,
			},
			exp: 127,
			rng: sql.InRange,
		},
		{
			val: sql.Value{
				Val: []byte{255},
				Typ: sqltypes.Int8,
			},
			exp: math.MaxUint64,
			rng: sql.InRange,
		},

		// Int16 -> Uint64
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint16(nil, uint16(0)),
				Typ: sqltypes.Int16,
			},
			exp: 0,
			rng: sql.InRange,
		},
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint16(nil, uint16(67)),
				Typ: sqltypes.Int16,
			},
			exp: 67,
			rng: sql.InRange,
		},
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint16(nil, math.MaxInt16),
				Typ: sqltypes.Int16,
			},
			exp: math.MaxInt16,
			rng: sql.InRange,
		},
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint16(nil, math.MaxInt16+1),
				Typ: sqltypes.Int16,
			},
			exp: math.MaxUint64 - math.MaxInt16,
			rng: sql.InRange,
		},
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint16(nil, math.MaxUint16),
				Typ: sqltypes.Int16,
			},
			exp: math.MaxUint64,
			rng: sql.InRange,
		},

		// Int32 -> Uint64
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint32(nil, uint32(0)),
				Typ: sqltypes.Int32,
			},
			exp: 0,
			rng: sql.InRange,
		},
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint32(nil, uint32(67)),
				Typ: sqltypes.Int32,
			},
			exp: 67,
			rng: sql.InRange,
		},
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint32(nil, math.MaxInt32),
				Typ: sqltypes.Int32,
			},
			exp: math.MaxInt32,
			rng: sql.InRange,
		},
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint32(nil, math.MaxInt32+1),
				Typ: sqltypes.Int32,
			},
			exp: math.MaxUint64 - math.MaxInt32,
			rng: sql.InRange,
		},
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint32(nil, math.MaxUint32),
				Typ: sqltypes.Int32,
			},
			exp: math.MaxUint64,
			rng: sql.InRange,
		},

		// Int64 -> Uint64
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint64(nil, uint64(0)),
				Typ: sqltypes.Int64,
			},
			exp: 0,
			rng: sql.InRange,
		},
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint64(nil, uint64(67)),
				Typ: sqltypes.Int64,
			},
			exp: 67,
			rng: sql.InRange,
		},
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint64(nil, math.MaxInt64),
				Typ: sqltypes.Int64,
			},
			exp: math.MaxInt64,
			rng: sql.InRange,
		},
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint64(nil, math.MaxInt64+1),
				Typ: sqltypes.Int64,
			},
			exp: math.MaxInt64 + 1,
			rng: sql.InRange,
		},
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint64(nil, math.MaxUint64),
				Typ: sqltypes.Int64,
			},
			exp: math.MaxUint64,
			rng: sql.InRange,
		},

		// Uint8 -> Uint64
		{
			val: sql.Value{
				Val: []byte{0},
				Typ: sqltypes.Uint8,
			},
			exp: 0,
			rng: sql.InRange,
		},
		{
			val: sql.Value{
				Val: []byte{67},
				Typ: sqltypes.Uint8,
			},
			exp: 67,
			rng: sql.InRange,
		},
		{
			val: sql.Value{
				Val: []byte{128},
				Typ: sqltypes.Uint8,
			},
			exp: 128,
			rng: sql.InRange,
		},
		{
			val: sql.Value{
				Val: []byte{255},
				Typ: sqltypes.Uint8,
			},
			exp: 255,
			rng: sql.InRange,
		},

		// Uint16 -> Uint64
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint16(nil, uint16(0)),
				Typ: sqltypes.Uint16,
			},
			exp: 0,
			rng: sql.InRange,
		},
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint16(nil, uint16(67)),
				Typ: sqltypes.Uint16,
			},
			exp: 67,
			rng: sql.InRange,
		},
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint16(nil, math.MaxInt16),
				Typ: sqltypes.Uint16,
			},
			exp: math.MaxInt16,
			rng: sql.InRange,
		},
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint16(nil, math.MaxUint16),
				Typ: sqltypes.Uint16,
			},
			exp: math.MaxUint16,
			rng: sql.InRange,
		},

		// Uint32 -> Uint64
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint32(nil, uint32(0)),
				Typ: sqltypes.Uint32,
			},
			exp: 0,
			rng: sql.InRange,
		},
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint32(nil, uint32(67)),
				Typ: sqltypes.Uint32,
			},
			exp: 67,
			rng: sql.InRange,
		},
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint32(nil, math.MaxInt32),
				Typ: sqltypes.Uint32,
			},
			exp: math.MaxInt32,
			rng: sql.InRange,
		},
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint32(nil, math.MaxUint32),
				Typ: sqltypes.Uint32,
			},
			exp: math.MaxUint32,
			rng: sql.InRange,
		},

		// Uint64 -> Uint64
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint64(nil, uint64(0)),
				Typ: sqltypes.Uint64,
			},
			exp: 0,
			rng: sql.InRange,
		},
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint64(nil, uint64(67)),
				Typ: sqltypes.Uint64,
			},
			exp: 67,
			rng: sql.InRange,
		},
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint64(nil, math.MaxInt64),
				Typ: sqltypes.Uint64,
			},
			exp: math.MaxInt64,
			rng: sql.InRange,
		},
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint64(nil, math.MaxUint64),
				Typ: sqltypes.Uint64,
			},
			exp: math.MaxUint64,
			rng: sql.InRange,
		},

		// Float32 -> Uint64
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint32(nil, math.Float32bits(0)),
				Typ: sqltypes.Float32,
			},
			exp: 0,
			rng: sql.InRange,
		},
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint32(nil, math.Float32bits(123.456)),
				Typ: sqltypes.Float32,
			},
			exp: 123,
			rng: sql.InRange,
		},
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint32(nil, math.Float32bits(math.MaxFloat32)),
				Typ: sqltypes.Float32,
			},
			exp: math.MaxUint64,
			rng: sql.Overflow,
		},

		// Float64 -> Uint64
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint64(nil, math.Float64bits(0)),
				Typ: sqltypes.Float64,
			},
			exp: 0,
			rng: sql.InRange,
		},
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint64(nil, math.Float64bits(123.456)),
				Typ: sqltypes.Float64,
			},
			exp: 123,
			rng: sql.InRange,
		},
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint64(nil, math.Float64bits(math.MaxFloat32)),
				Typ: sqltypes.Float64,
			},
			exp: math.MaxUint64,
			rng: sql.Overflow,
		},
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint64(nil, math.Float64bits(math.MaxFloat64)),
				Typ: sqltypes.Float64,
			},
			exp: math.MaxUint64,
			rng: sql.Overflow,
		},

		// Decimal -> Uint64
		{
			val: sql.Value{
				Val: zeroDec,
				Typ: sqltypes.Decimal,
			},
			exp: 0,
			rng: sql.InRange,
		},
		{
			val: sql.Value{
				Val: testDec,
				Typ: sqltypes.Decimal,
			},
			exp: 123,
			rng: sql.InRange,
		},
		{
			val: sql.Value{
				Val: maxInt64Dec,
				Typ: sqltypes.Decimal,
			},
			exp: math.MaxInt64,
			rng: sql.InRange,
		},

		// Bit -> Uint64
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint64(nil, uint64(0)),
				Typ: sqltypes.Bit,
			},
			exp: 0,
			rng: sql.InRange,
		},
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint64(nil, uint64(67)),
				Typ: sqltypes.Bit,
			},
			exp: 67,
			rng: sql.InRange,
		},
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint64(nil, math.MaxInt64),
				Typ: sqltypes.Bit,
			},
			exp: math.MaxInt64,
			rng: sql.InRange,
		},
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint64(nil, math.MaxUint64),
				Typ: sqltypes.Bit,
			},
			exp: math.MaxUint64,
			rng: sql.InRange,
		},

		// Year -> Uint64
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint16(nil, uint16(0)),
				Typ: sqltypes.Year,
			},
			exp: 0,
			rng: sql.InRange,
		},
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint16(nil, uint16(1967)),
				Typ: sqltypes.Year,
			},
			exp: 1967,
			rng: sql.InRange,
		},
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint16(nil, uint16(1901)),
				Typ: sqltypes.Year,
			},
			exp: 1901,
			rng: sql.InRange,
		},
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint16(nil, uint16(2155)),
				Typ: sqltypes.Year,
			},
			exp: 2155,
			rng: sql.InRange,
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("Val: %v Typ: %v to UINT64", test.val.Val, test.val.Typ), func(t *testing.T) {
			res, rng, err := convertValueToUint64(ctx, test.val)
			if test.err {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, test.exp, res)
			require.Equal(t, test.rng, rng)
		})
	}
}

func TestConvertValueToFloat64(t *testing.T) {
	ctx := sql.NewEmptyContext()

	zeroDec := serializeDecimal(decimal.Zero)
	testDec := serializeDecimal(decimal.NewFromFloat(123.456))
	minInt64Dec := serializeDecimal(decimal.NewFromInt(math.MinInt64))
	maxInt64Dec := serializeDecimal(decimal.NewFromInt(math.MaxInt64))

	tests := []struct {
		val sql.Value
		exp float64
		err bool
	}{
		// Int8 -> Float64
		{
			val: sql.Value{
				Val: []byte{0},
				Typ: sqltypes.Int8,
			},
			exp: 0,
		},
		{
			val: sql.Value{
				Val: []byte{67},
				Typ: sqltypes.Int8,
			},
			exp: 67,
		},
		{
			val: sql.Value{
				Val: []byte{127},
				Typ: sqltypes.Int8,
			},
			exp: 127,
		},
		{
			val: sql.Value{
				Val: []byte{255},
				Typ: sqltypes.Int8,
			},
			exp: -1,
		},

		// Int16 -> Float64
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint16(nil, uint16(0)),
				Typ: sqltypes.Int16,
			},
			exp: 0,
		},
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint16(nil, uint16(67)),
				Typ: sqltypes.Int16,
			},
			exp: 67,
		},
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint16(nil, math.MaxInt16),
				Typ: sqltypes.Int16,
			},
			exp: math.MaxInt16,
		},
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint16(nil, math.MaxInt16+1),
				Typ: sqltypes.Int16,
			},
			exp: math.MinInt16,
		},
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint16(nil, math.MaxUint16),
				Typ: sqltypes.Int16,
			},
			exp: -1,
		},

		// Int32 -> Float64
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint32(nil, uint32(0)),
				Typ: sqltypes.Int32,
			},
			exp: 0,
		},
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint32(nil, uint32(67)),
				Typ: sqltypes.Int32,
			},
			exp: 67,
		},
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint32(nil, math.MaxInt32),
				Typ: sqltypes.Int32,
			},
			exp: math.MaxInt32,
		},
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint32(nil, math.MaxInt32+1),
				Typ: sqltypes.Int32,
			},
			exp: math.MinInt32,
		},
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint32(nil, math.MaxUint32),
				Typ: sqltypes.Int32,
			},
			exp: -1,
		},

		// Int64 -> Float64
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint64(nil, uint64(0)),
				Typ: sqltypes.Int64,
			},
			exp: 0,
		},
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint64(nil, uint64(67)),
				Typ: sqltypes.Int64,
			},
			exp: 67,
		},
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint64(nil, math.MaxInt64),
				Typ: sqltypes.Int64,
			},
			exp: math.MaxInt64,
		},
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint64(nil, math.MaxInt64+1),
				Typ: sqltypes.Int64,
			},
			exp: math.MinInt64,
		},
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint64(nil, math.MaxUint64),
				Typ: sqltypes.Int64,
			},
			exp: -1,
		},

		// Uint8 -> Float64
		{
			val: sql.Value{
				Val: []byte{0},
				Typ: sqltypes.Uint8,
			},
			exp: 0,
		},
		{
			val: sql.Value{
				Val: []byte{67},
				Typ: sqltypes.Uint8,
			},
			exp: 67,
		},
		{
			val: sql.Value{
				Val: []byte{128},
				Typ: sqltypes.Uint8,
			},
			exp: 128,
		},
		{
			val: sql.Value{
				Val: []byte{255},
				Typ: sqltypes.Uint8,
			},
			exp: 255,
		},

		// Uint16 -> Float64
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint16(nil, uint16(0)),
				Typ: sqltypes.Uint16,
			},
			exp: 0,
		},
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint16(nil, uint16(67)),
				Typ: sqltypes.Uint16,
			},
			exp: 67,
		},
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint16(nil, math.MaxInt16),
				Typ: sqltypes.Uint16,
			},
			exp: math.MaxInt16,
		},
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint16(nil, math.MaxUint16),
				Typ: sqltypes.Uint16,
			},
			exp: math.MaxUint16,
		},

		// Uint32 -> Float64
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint32(nil, uint32(0)),
				Typ: sqltypes.Uint32,
			},
			exp: 0,
		},
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint32(nil, uint32(67)),
				Typ: sqltypes.Uint32,
			},
			exp: 67,
		},
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint32(nil, math.MaxInt32),
				Typ: sqltypes.Uint32,
			},
			exp: math.MaxInt32,
		},
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint32(nil, math.MaxUint32),
				Typ: sqltypes.Uint32,
			},
			exp: math.MaxUint32,
		},

		// Uint64 -> Float64
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint64(nil, uint64(0)),
				Typ: sqltypes.Uint64,
			},
			exp: 0,
		},
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint64(nil, uint64(67)),
				Typ: sqltypes.Uint64,
			},
			exp: 67,
		},
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint64(nil, math.MaxInt64),
				Typ: sqltypes.Uint64,
			},
			exp: math.MaxInt64,
		},
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint64(nil, math.MaxUint64),
				Typ: sqltypes.Uint64,
			},
			exp: math.MaxUint64,
		},

		// Float32 -> Float64
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint32(nil, math.Float32bits(0)),
				Typ: sqltypes.Float32,
			},
			exp: 0,
		},
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint32(nil, math.Float32bits(123.456)),
				Typ: sqltypes.Float32,
			},
			exp: 123.456,
		},
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint32(nil, math.Float32bits(-math.MaxFloat32)),
				Typ: sqltypes.Float32,
			},
			exp: -math.MaxFloat32,
		},
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint32(nil, math.Float32bits(math.MaxFloat32)),
				Typ: sqltypes.Float32,
			},
			exp: math.MaxFloat32,
		},

		// Float64 -> Float64
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint64(nil, math.Float64bits(0)),
				Typ: sqltypes.Float64,
			},
			exp: 0,
		},
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint64(nil, math.Float64bits(123.456)),
				Typ: sqltypes.Float64,
			},
			exp: 123,
		},
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint64(nil, math.Float64bits(-math.MaxFloat32)),
				Typ: sqltypes.Float64,
			},
			exp: -math.MaxFloat32,
		},
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint64(nil, math.Float64bits(math.MaxFloat32)),
				Typ: sqltypes.Float64,
			},
			exp: math.MaxFloat32,
		},
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint64(nil, math.Float64bits(-math.MaxFloat64)),
				Typ: sqltypes.Float64,
			},
			exp: -math.MaxFloat64,
		},
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint64(nil, math.Float64bits(math.MaxFloat64)),
				Typ: sqltypes.Float64,
			},
			exp: math.MaxFloat64,
		},

		// Decimal -> Float64
		{
			val: sql.Value{
				Val: zeroDec,
				Typ: sqltypes.Decimal,
			},
			exp: 0,
		},
		{
			val: sql.Value{
				Val: testDec,
				Typ: sqltypes.Decimal,
			},
			exp: 123.456,
		},
		{
			val: sql.Value{
				Val: minInt64Dec,
				Typ: sqltypes.Decimal,
			},
			exp: math.MinInt64,
		},
		{
			val: sql.Value{
				Val: maxInt64Dec,
				Typ: sqltypes.Decimal,
			},
			exp: math.MaxInt64,
		},

		// Bit -> Float64
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint64(nil, uint64(0)),
				Typ: sqltypes.Bit,
			},
			exp: 0,
		},
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint64(nil, uint64(67)),
				Typ: sqltypes.Bit,
			},
			exp: 67,
		},
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint64(nil, math.MaxInt64),
				Typ: sqltypes.Bit,
			},
			exp: math.MaxInt64,
		},
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint64(nil, math.MaxUint64),
				Typ: sqltypes.Bit,
			},
			exp: math.MaxUint64,
		},

		// Year -> Float64
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint16(nil, uint16(0)),
				Typ: sqltypes.Year,
			},
			exp: 0,
		},
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint16(nil, uint16(1967)),
				Typ: sqltypes.Year,
			},
			exp: 1967,
		},
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint16(nil, uint16(1901)),
				Typ: sqltypes.Year,
			},
			exp: 1901,
		},
		{
			val: sql.Value{
				Val: binary.LittleEndian.AppendUint16(nil, uint16(2155)),
				Typ: sqltypes.Year,
			},
			exp: 2155,
		},
	}

	epsilon := 0.01
	for _, test := range tests {
		t.Run(fmt.Sprintf("%v", test.val), func(t *testing.T) {
			res, err := convertValueToFloat64(ctx, test.val)
			if test.err {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			if test.exp == 0 {
				require.Zero(t, res)
				return
			}
			require.InEpsilonf(t, test.exp, res, epsilon, fmt.Sprintf("Actual is: %v", res))
		})
	}
}
