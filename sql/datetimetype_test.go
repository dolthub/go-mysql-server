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
	"reflect"
	"testing"
	"time"

	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/dolthub/vitess/go/vt/proto/query"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDatetimeCompare(t *testing.T) {
	tests := []struct {
		typ         Type
		val1        interface{}
		val2        interface{}
		expectedCmp int
	}{
		{types.Date, nil, 0, 1},
		{types.Datetime, 0, nil, -1},
		{types.Timestamp, nil, nil, 0},

		{types.Date, time.Date(2012, 12, 12, 12, 12, 12, 12, time.UTC),
			time.Date(2012, 12, 12, 12, 24, 24, 24, time.UTC), 0},
		{types.Date, time.Date(2012, 12, 12, 12, 12, 12, 12, time.UTC),
			"2010-06-03", 1},
		{types.Date, "2010-06-03 06:03:11",
			time.Date(2012, 12, 12, 12, 12, 12, 12, time.UTC), -1},
		{types.Datetime, time.Date(2012, 12, 12, 12, 12, 12, 12, time.UTC),
			time.Date(2012, 12, 12, 12, 24, 24, 24, time.UTC), -1},
		{types.Datetime, time.Date(2012, 12, 12, 12, 12, 12, 12, time.UTC),
			"2010-06-03", 1},
		{types.Datetime, "2010-06-03 06:03:11",
			time.Date(2010, 6, 3, 6, 3, 11, 0, time.UTC), 0},
		{types.Timestamp, time.Date(2012, 12, 12, 12, 12, 12, 12, time.UTC),
			time.Date(2012, 12, 12, 12, 24, 24, 24, time.UTC), -1},
		{types.Timestamp, time.Date(2012, 12, 12, 12, 12, 12, 12, time.UTC),
			"2010-06-03", 1},
		{types.Timestamp, "2010-06-03 06:03:11",
			time.Date(2010, 6, 3, 6, 3, 11, 0, time.UTC), 0},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%v %v", test.val1, test.val2), func(t *testing.T) {
			cmp, err := test.typ.Compare(test.val1, test.val2)
			require.NoError(t, err)
			assert.Equal(t, test.expectedCmp, cmp)
		})
	}
}

func TestDatetimeCreate(t *testing.T) {
	tests := []struct {
		baseType     query.Type
		expectedType types.datetimeType
		expectedErr  bool
	}{
		{sqltypes.Date, types.datetimeType{sqltypes.Date}, false},
		{sqltypes.Datetime, types.datetimeType{sqltypes.Datetime}, false},
		{sqltypes.Timestamp, types.datetimeType{sqltypes.Timestamp}, false},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%v", test.baseType), func(t *testing.T) {
			typ, err := types.CreateDatetimeType(test.baseType)
			if test.expectedErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, test.expectedType, typ)
			}
		})
	}
}

func TestDatetimeCreateInvalidBaseTypes(t *testing.T) {
	tests := []struct {
		baseType     query.Type
		expectedType types.datetimeType
		expectedErr  bool
	}{
		{sqltypes.Binary, types.datetimeType{}, true},
		{sqltypes.Bit, types.datetimeType{}, true},
		{sqltypes.Blob, types.datetimeType{}, true},
		{sqltypes.Char, types.datetimeType{}, true},
		{sqltypes.Decimal, types.datetimeType{}, true},
		{sqltypes.Enum, types.datetimeType{}, true},
		{sqltypes.Expression, types.datetimeType{}, true},
		{sqltypes.Float32, types.datetimeType{}, true},
		{sqltypes.Float64, types.datetimeType{}, true},
		{sqltypes.Geometry, types.datetimeType{}, true},
		{sqltypes.Int16, types.datetimeType{}, true},
		{sqltypes.Int24, types.datetimeType{}, true},
		{sqltypes.Int32, types.datetimeType{}, true},
		{sqltypes.Int64, types.datetimeType{}, true},
		{sqltypes.Int8, types.datetimeType{}, true},
		{sqltypes.Null, types.datetimeType{}, true},
		{sqltypes.Set, types.datetimeType{}, true},
		{sqltypes.Text, types.datetimeType{}, true},
		{sqltypes.Time, types.datetimeType{}, true},
		{sqltypes.TypeJSON, types.datetimeType{}, true},
		{sqltypes.Uint16, types.datetimeType{}, true},
		{sqltypes.Uint24, types.datetimeType{}, true},
		{sqltypes.Uint32, types.datetimeType{}, true},
		{sqltypes.Uint64, types.datetimeType{}, true},
		{sqltypes.Uint8, types.datetimeType{}, true},
		{sqltypes.VarBinary, types.datetimeType{}, true},
		{sqltypes.VarChar, types.datetimeType{}, true},
		{sqltypes.Year, types.datetimeType{}, true},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%v", test.baseType), func(t *testing.T) {
			typ, err := types.CreateDatetimeType(test.baseType)
			if test.expectedErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, test.expectedType, typ)
			}
		})
	}
}

func TestDatetimeConvert(t *testing.T) {
	tests := []struct {
		typ         Type
		val         interface{}
		expectedVal interface{}
		expectedErr bool
	}{
		{types.Date, nil, nil, false},
		{types.Date, time.Date(2012, 12, 12, 12, 12, 12, 12, time.UTC),
			time.Date(2012, 12, 12, 0, 0, 0, 0, time.UTC), false},
		{types.Date, "2010-06-03", time.Date(2010, 6, 3, 0, 0, 0, 0, time.UTC), false},
		{types.Date, "2010-6-3", time.Date(2010, 6, 3, 0, 0, 0, 0, time.UTC), false},
		{types.Date, "2010-6-03", time.Date(2010, 6, 3, 0, 0, 0, 0, time.UTC), false},
		{types.Date, "2010-06-3", time.Date(2010, 6, 3, 0, 0, 0, 0, time.UTC), false},
		{types.Date, "2010-06-03 12:12:12", time.Date(2010, 6, 3, 0, 0, 0, 0, time.UTC), false},
		{types.Date, "2010-06-03 12:12:12.000012", time.Date(2010, 6, 3, 0, 0, 0, 0, time.UTC), false},
		{types.Date, "2010-06-03T12:12:12Z", time.Date(2010, 6, 3, 0, 0, 0, 0, time.UTC), false},
		{types.Date, "2010-06-03T12:12:12.000012Z", time.Date(2010, 6, 3, 0, 0, 0, 0, time.UTC), false},
		{types.Date, "20100603", time.Date(2010, 6, 3, 0, 0, 0, 0, time.UTC), false},
		{types.Date, "20100603121212", time.Date(2010, 6, 3, 0, 0, 0, 0, time.UTC), false},

		{types.Datetime, nil, nil, false},
		{types.Datetime, time.Date(2012, 12, 12, 12, 12, 12, 12, time.UTC),
			time.Date(2012, 12, 12, 12, 12, 12, 12, time.UTC), false},
		{types.Datetime, "2010-06-03", time.Date(2010, 6, 3, 0, 0, 0, 0, time.UTC), false},
		{types.Datetime, "2010-6-3", time.Date(2010, 6, 3, 0, 0, 0, 0, time.UTC), false},
		{types.Datetime, "2010-06-3", time.Date(2010, 6, 3, 0, 0, 0, 0, time.UTC), false},
		{types.Datetime, "2010-6-03", time.Date(2010, 6, 3, 0, 0, 0, 0, time.UTC), false},
		{types.Datetime, "2010-06-03 12:12:12", time.Date(2010, 6, 3, 12, 12, 12, 0, time.UTC), false},
		{types.Datetime, "2010-06-03 12:12:12.000012", time.Date(2010, 6, 3, 12, 12, 12, 12000, time.UTC), false},
		{types.Datetime, "2010-06-03T12:12:12Z", time.Date(2010, 6, 3, 12, 12, 12, 0, time.UTC), false},
		{types.Datetime, "2010-06-03T12:12:12.000012Z", time.Date(2010, 6, 3, 12, 12, 12, 12000, time.UTC), false},
		{types.Datetime, "20100603", time.Date(2010, 6, 3, 0, 0, 0, 0, time.UTC), false},
		{types.Datetime, "20100603121212", time.Date(2010, 6, 3, 12, 12, 12, 0, time.UTC), false},
		{types.Datetime, "2010-6-3 12:12:12", time.Date(2010, 6, 3, 12, 12, 12, 0, time.UTC), false},
		{types.Datetime, "2010-6-13 12:12:12", time.Date(2010, 6, 13, 12, 12, 12, 0, time.UTC), false},
		{types.Datetime, "2010-10-3 12:12:12", time.Date(2010, 10, 3, 12, 12, 12, 0, time.UTC), false},
		{types.Datetime, "2010-10-3 12:12:2", time.Date(2010, 10, 3, 12, 12, 2, 0, time.UTC), false},
		{types.Datetime, "2010-10-3 12:2:2", time.Date(2010, 10, 3, 12, 2, 2, 0, time.UTC), false},

		{types.Datetime, "2010-06-03 12:3", time.Date(2010, 6, 3, 12, 3, 0, 0, time.UTC), false},
		{types.Datetime, "2010-06-03 12:34", time.Date(2010, 6, 3, 12, 34, 0, 0, time.UTC), false},
		{types.Datetime, "2010-06-03 12:34:", time.Date(2010, 6, 3, 12, 34, 0, 0, time.UTC), false},
		{types.Datetime, "2010-06-03 12:34:.", time.Date(2010, 6, 3, 12, 34, 0, 0, time.UTC), false},
		{types.Datetime, "2010-06-03 12:34:5", time.Date(2010, 6, 3, 12, 34, 5, 0, time.UTC), false},
		{types.Datetime, "2010-06-03 12:34:56", time.Date(2010, 6, 3, 12, 34, 56, 0, time.UTC), false},
		{types.Datetime, "2010-06-03 12:34:56.", time.Date(2010, 6, 3, 12, 34, 56, 0, time.UTC), false},
		{types.Datetime, "2010-06-03 12:34:56.7", time.Date(2010, 6, 3, 12, 34, 56, 700000000, time.UTC), false},
		{types.Datetime, "2010-06-03 12:34:56.78", time.Date(2010, 6, 3, 12, 34, 56, 780000000, time.UTC), false},
		{types.Datetime, "2010-06-03 12:34:56.789", time.Date(2010, 6, 3, 12, 34, 56, 789000000, time.UTC), false},

		{types.Timestamp, nil, nil, false},
		{types.Timestamp, time.Date(2012, 12, 12, 12, 12, 12, 12, time.UTC),
			time.Date(2012, 12, 12, 12, 12, 12, 12, time.UTC), false},
		{types.Timestamp, "2010-06-03", time.Date(2010, 6, 3, 0, 0, 0, 0, time.UTC), false},
		{types.Timestamp, "2010-6-3", time.Date(2010, 6, 3, 0, 0, 0, 0, time.UTC), false},
		{types.Timestamp, "2010-6-03", time.Date(2010, 6, 3, 0, 0, 0, 0, time.UTC), false},
		{types.Timestamp, "2010-06-3", time.Date(2010, 6, 3, 0, 0, 0, 0, time.UTC), false},
		{types.Timestamp, "2010-06-03 12:12:12", time.Date(2010, 6, 3, 12, 12, 12, 0, time.UTC), false},
		{types.Timestamp, "2010-06-03 12:12:12.000012", time.Date(2010, 6, 3, 12, 12, 12, 12000, time.UTC), false},
		{types.Timestamp, "2010-06-03T12:12:12Z", time.Date(2010, 6, 3, 12, 12, 12, 0, time.UTC), false},
		{types.Timestamp, "2010-06-03T12:12:12.000012Z", time.Date(2010, 6, 3, 12, 12, 12, 12000, time.UTC), false},
		{types.Timestamp, "20100603", time.Date(2010, 6, 3, 0, 0, 0, 0, time.UTC), false},
		{types.Timestamp, "20100603121212", time.Date(2010, 6, 3, 12, 12, 12, 0, time.UTC), false},
		{types.Timestamp, time.Date(2012, 12, 12, 12, 12, 12, 12, time.UTC).UTC().String(), time.Date(2012, 12, 12, 12, 12, 12, 12, time.UTC), false},

		{types.Date, "0000-01-01 00:00:00", time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC), false},
		{types.Date, "0500-01-01 00:00:00", time.Date(500, 1, 1, 0, 0, 0, 0, time.UTC), false},
		{types.Date, time.Date(10000, 1, 1, 1, 1, 1, 1, time.UTC), nil, true},
		{types.Date, "", nil, true},
		{types.Date, "0500-01-01", time.Date(500, 1, 1, 0, 0, 0, 0, time.UTC), false},
		{types.Date, "10000-01-01", nil, true},
		{types.Date, int(0), time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC), false},
		{types.Date, int8(0), time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC), false},
		{types.Date, int16(0), time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC), false},
		{types.Date, int32(0), time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC), false},
		{types.Date, int64(0), time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC), false},
		{types.Date, uint(0), time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC), false},
		{types.Date, uint8(0), time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC), false},
		{types.Date, uint16(0), time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC), false},
		{types.Date, uint32(0), time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC), false},
		{types.Date, uint64(0), time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC), false},
		{types.Date, float32(0), time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC), false},
		{types.Date, float64(0), time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC), false},
		{types.Date, []byte{0}, nil, true},

		{types.Datetime, "0500-01-01 01:01:01", time.Date(500, 1, 1, 1, 1, 1, 0, time.UTC), false},
		{types.Datetime, "0000-01-01 00:00:00", time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC), false},
		{types.Datetime, time.Date(10000, 1, 1, 1, 1, 1, 1, time.UTC), nil, true},
		{types.Datetime, int(0), time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC), false},
		{types.Datetime, int8(0), time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC), false},
		{types.Datetime, int16(0), time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC), false},
		{types.Datetime, int32(0), time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC), false},
		{types.Datetime, int64(0), time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC), false},
		{types.Datetime, uint(0), time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC), false},
		{types.Datetime, uint8(0), time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC), false},
		{types.Datetime, uint16(0), time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC), false},
		{types.Datetime, uint32(0), time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC), false},
		{types.Datetime, uint64(0), time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC), false},
		{types.Datetime, float32(0), time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC), false},
		{types.Datetime, float64(0), time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC), false},
		{types.Datetime, []byte{0}, nil, true},

		{types.Timestamp, time.Date(1960, 1, 1, 1, 1, 1, 1, time.UTC), nil, true},
		{types.Timestamp, "1970-01-01 00:00:00", nil, true},
		{types.Timestamp, "1970-01-01 00:00:01", time.Date(1970, 1, 1, 0, 0, 1, 0, time.UTC), false},
		{types.Timestamp, time.Date(2040, 1, 1, 1, 1, 1, 1, time.UTC), nil, true},
		{types.Timestamp, int(0), time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC), false},
		{types.Timestamp, int8(0), time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC), false},
		{types.Timestamp, int16(0), time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC), false},
		{types.Timestamp, int32(0), time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC), false},
		{types.Timestamp, int64(0), time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC), false},
		{types.Timestamp, uint(0), time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC), false},
		{types.Timestamp, uint8(0), time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC), false},
		{types.Timestamp, uint16(0), time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC), false},
		{types.Timestamp, uint32(0), time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC), false},
		{types.Timestamp, uint64(0), time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC), false},
		{types.Timestamp, float32(0), time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC), false},
		{types.Timestamp, float64(0), time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC), false},
		{types.Timestamp, []byte{0}, nil, true},

		{types.Date, int(1), nil, true},
		{types.Date, int8(1), nil, true},
		{types.Date, int16(1), nil, true},
		{types.Date, int32(1), nil, true},
		{types.Date, int64(1), nil, true},
		{types.Date, uint(1), nil, true},
		{types.Date, uint8(1), nil, true},
		{types.Date, uint16(1), nil, true},
		{types.Date, uint32(1), nil, true},
		{types.Date, uint64(1), nil, true},
		{types.Date, float32(1), nil, true},
		{types.Date, float64(1), nil, true},

		{types.Datetime, int(1), nil, true},
		{types.Datetime, int8(1), nil, true},
		{types.Datetime, int16(1), nil, true},
		{types.Datetime, int32(1), nil, true},
		{types.Datetime, int64(1), nil, true},
		{types.Datetime, uint(1), nil, true},
		{types.Datetime, uint8(1), nil, true},
		{types.Datetime, uint16(1), nil, true},
		{types.Datetime, uint32(1), nil, true},
		{types.Datetime, uint64(1), nil, true},
		{types.Datetime, float32(1), nil, true},
		{types.Datetime, float64(1), nil, true},

		{types.Timestamp, int(1), nil, true},
		{types.Timestamp, int8(1), nil, true},
		{types.Timestamp, int16(1), nil, true},
		{types.Timestamp, int32(1), nil, true},
		{types.Timestamp, int64(1), nil, true},
		{types.Timestamp, uint(1), nil, true},
		{types.Timestamp, uint8(1), nil, true},
		{types.Timestamp, uint16(1), nil, true},
		{types.Timestamp, uint32(1), nil, true},
		{types.Timestamp, uint64(1), nil, true},
		{types.Timestamp, float32(1), nil, true},
		{types.Timestamp, float64(1), nil, true},
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

func TestDatetimeString(t *testing.T) {
	tests := []struct {
		typ         Type
		expectedStr string
	}{
		{types.MustCreateDatetimeType(sqltypes.Date), "date"},
		{types.MustCreateDatetimeType(sqltypes.Datetime), "datetime"},
		{types.MustCreateDatetimeType(sqltypes.Timestamp), "timestamp"},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%v %v", test.typ, test.expectedStr), func(t *testing.T) {
			str := test.typ.String()
			assert.Equal(t, test.expectedStr, str)
		})
	}
}

func TestDatetimeZero(t *testing.T) {
	_, ok := types.MustCreateDatetimeType(sqltypes.Date).Zero().(time.Time)
	require.True(t, ok)
	_, ok = types.MustCreateDatetimeType(sqltypes.Datetime).Zero().(time.Time)
	require.True(t, ok)
	_, ok = types.MustCreateDatetimeType(sqltypes.Timestamp).Zero().(time.Time)
	require.True(t, ok)
}
