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
	"math"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/dolthub/vitess/go/vt/proto/query"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
)

func TestNumberCompare(t *testing.T) {
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
			cmp, err := test.typ.Compare(test.val1, test.val2)
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
		{sqltypes.Int8, NumberTypeImpl_{sqltypes.Int8}, false},
		{sqltypes.Int16, NumberTypeImpl_{sqltypes.Int16}, false},
		{sqltypes.Int24, NumberTypeImpl_{sqltypes.Int24}, false},
		{sqltypes.Int32, NumberTypeImpl_{sqltypes.Int32}, false},
		{sqltypes.Int64, NumberTypeImpl_{sqltypes.Int64}, false},
		{sqltypes.Uint8, NumberTypeImpl_{sqltypes.Uint8}, false},
		{sqltypes.Uint16, NumberTypeImpl_{sqltypes.Uint16}, false},
		{sqltypes.Uint24, NumberTypeImpl_{sqltypes.Uint24}, false},
		{sqltypes.Uint32, NumberTypeImpl_{sqltypes.Uint32}, false},
		{sqltypes.Uint64, NumberTypeImpl_{sqltypes.Uint64}, false},
		{sqltypes.Float32, NumberTypeImpl_{sqltypes.Float32}, false},
		{sqltypes.Float64, NumberTypeImpl_{sqltypes.Float64}, false},
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
	tests := []struct {
		typ         sql.Type
		val         interface{}
		expectedVal interface{}
		expectedErr bool
	}{
		{Boolean, true, int8(1), false},
		{Int8, int32(0), int8(0), false},
		{Int16, uint16(1), int16(1), false},
		{Int24, false, int32(0), false},
		{Int32, nil, nil, false},
		{Int64, "33", int64(33), false},
		{Int64, "33.0", int64(33), false},
		{Int64, "33.1", int64(33), false},
		{Int64, strconv.FormatInt(math.MaxInt64, 10), int64(math.MaxInt64), false},
		{Int64, true, int64(1), false},
		{Int64, false, int64(0), false},
		{Uint8, int64(34), uint8(34), false},
		{Uint16, int16(35), uint16(35), false},
		{Uint24, 36.756, uint32(37), false},
		{Uint32, uint8(37), uint32(37), false},
		{Uint64, time.Date(2009, 1, 2, 3, 4, 5, 0, time.UTC), uint64(time.Date(2009, 1, 2, 3, 4, 5, 0, time.UTC).Unix()), false},
		{Uint64, "01000", uint64(1000), false},
		{Uint64, true, uint64(1), false},
		{Uint64, false, uint64(0), false},
		{Float32, "22.25", float32(22.25), false},
		{Float32, []byte{90, 140, 228, 206, 116}, float32(388910861940), false},
		{Float64, float32(893.875), float64(893.875), false},

		{Boolean, math.MaxInt8 + 1, nil, true},
		{Int8, math.MaxInt8 + 1, nil, true},
		{Int8, math.MinInt8 - 1, nil, true},
		{Int16, math.MaxInt16 + 1, nil, true},
		{Int16, math.MinInt16 - 1, nil, true},
		{Int24, 1 << 23, nil, true},
		{Int24, -1<<23 - 1, nil, true},
		{Int32, math.MaxInt32 + 1, nil, true},
		{Int32, math.MinInt32 - 1, nil, true},
		{Int64, uint64(math.MaxInt64 + 1), nil, true},
		{Uint8, math.MaxUint8 + 1, nil, true},
		{Uint8, -1, nil, true},
		{Uint16, math.MaxUint16 + 1, nil, true},
		{Uint16, -1, nil, true},
		{Uint24, 1 << 24, nil, true},
		{Uint24, -1, nil, true},
		{Uint32, math.MaxUint32 + 1, nil, true},
		{Uint32, -1, nil, true},
		{Uint64, -1, nil, true},
		{Float32, math.MaxFloat32 * 2, nil, true},
		{Uint8, -1, nil, true},
		{Uint16, -1, nil, true},
		{Uint24, -1, nil, true},
		{Uint32, -1, nil, true},
		{Uint64, -1, nil, true},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%v %v %v", test.typ, test.val, test.expectedVal), func(t *testing.T) {
			val, err := test.typ.Convert(test.val)
			if test.expectedErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, test.expectedVal, val)
				if val != nil {
					assert.Equal(t, test.typ.ValueType(), reflect.TypeOf(val))
				}
			}
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
	assert.Equal(t, "also not a number", val.ToString())
}

func TestNumberString(t *testing.T) {
	tests := []struct {
		typ         sql.Type
		expectedStr string
	}{
		{Boolean, "tinyint"},
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
