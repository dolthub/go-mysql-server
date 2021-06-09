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
	"testing"
	"time"

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
		{Date, nil, 0, -1},
		{Datetime, 0, nil, 1},
		{Timestamp, nil, nil, 0},

		{Date, time.Date(2012, 12, 12, 12, 12, 12, 12, time.UTC),
			time.Date(2012, 12, 12, 12, 24, 24, 24, time.UTC), 0},
		{Date, time.Date(2012, 12, 12, 12, 12, 12, 12, time.UTC),
			"2010-06-03", 1},
		{Date, "2010-06-03 06:03:11",
			time.Date(2012, 12, 12, 12, 12, 12, 12, time.UTC), -1},
		{Datetime, time.Date(2012, 12, 12, 12, 12, 12, 12, time.UTC),
			time.Date(2012, 12, 12, 12, 24, 24, 24, time.UTC), -1},
		{Datetime, time.Date(2012, 12, 12, 12, 12, 12, 12, time.UTC),
			"2010-06-03", 1},
		{Datetime, "2010-06-03 06:03:11",
			time.Date(2010, 6, 3, 6, 3, 11, 0, time.UTC), 0},
		{Timestamp, time.Date(2012, 12, 12, 12, 12, 12, 12, time.UTC),
			time.Date(2012, 12, 12, 12, 24, 24, 24, time.UTC), -1},
		{Timestamp, time.Date(2012, 12, 12, 12, 12, 12, 12, time.UTC),
			"2010-06-03", 1},
		{Timestamp, "2010-06-03 06:03:11",
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
		expectedType datetimeType
		expectedErr  bool
	}{
		{sqltypes.Date, datetimeType{sqltypes.Date}, false},
		{sqltypes.Datetime, datetimeType{sqltypes.Datetime}, false},
		{sqltypes.Timestamp, datetimeType{sqltypes.Timestamp}, false},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%v", test.baseType), func(t *testing.T) {
			typ, err := CreateDatetimeType(test.baseType)
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
		expectedType datetimeType
		expectedErr  bool
	}{
		{sqltypes.Binary, datetimeType{}, true},
		{sqltypes.Bit, datetimeType{}, true},
		{sqltypes.Blob, datetimeType{}, true},
		{sqltypes.Char, datetimeType{}, true},
		{sqltypes.Decimal, datetimeType{}, true},
		{sqltypes.Enum, datetimeType{}, true},
		{sqltypes.Expression, datetimeType{}, true},
		{sqltypes.Float32, datetimeType{}, true},
		{sqltypes.Float64, datetimeType{}, true},
		{sqltypes.Geometry, datetimeType{}, true},
		{sqltypes.Int16, datetimeType{}, true},
		{sqltypes.Int24, datetimeType{}, true},
		{sqltypes.Int32, datetimeType{}, true},
		{sqltypes.Int64, datetimeType{}, true},
		{sqltypes.Int8, datetimeType{}, true},
		{sqltypes.Null, datetimeType{}, true},
		{sqltypes.Set, datetimeType{}, true},
		{sqltypes.Text, datetimeType{}, true},
		{sqltypes.Time, datetimeType{}, true},
		{sqltypes.TypeJSON, datetimeType{}, true},
		{sqltypes.Uint16, datetimeType{}, true},
		{sqltypes.Uint24, datetimeType{}, true},
		{sqltypes.Uint32, datetimeType{}, true},
		{sqltypes.Uint64, datetimeType{}, true},
		{sqltypes.Uint8, datetimeType{}, true},
		{sqltypes.VarBinary, datetimeType{}, true},
		{sqltypes.VarChar, datetimeType{}, true},
		{sqltypes.Year, datetimeType{}, true},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%v", test.baseType), func(t *testing.T) {
			typ, err := CreateDatetimeType(test.baseType)
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
		{Date, nil, nil, false},
		{Date, time.Date(2012, 12, 12, 12, 12, 12, 12, time.UTC),
			time.Date(2012, 12, 12, 0, 0, 0, 0, time.UTC), false},
		{Date, "2010-06-03", time.Date(2010, 6, 3, 0, 0, 0, 0, time.UTC), false},
		{Date, "2010-06-03 12:12:12", time.Date(2010, 6, 3, 0, 0, 0, 0, time.UTC), false},
		{Date, "2010-06-03 12:12:12.000012", time.Date(2010, 6, 3, 0, 0, 0, 0, time.UTC), false},
		{Date, "2010-06-03T12:12:12Z", time.Date(2010, 6, 3, 0, 0, 0, 0, time.UTC), false},
		{Date, "2010-06-03T12:12:12.000012Z", time.Date(2010, 6, 3, 0, 0, 0, 0, time.UTC), false},
		{Date, "20100603", time.Date(2010, 6, 3, 0, 0, 0, 0, time.UTC), false},
		{Date, "20100603121212", time.Date(2010, 6, 3, 0, 0, 0, 0, time.UTC), false},

		{Datetime, nil, nil, false},
		{Datetime, time.Date(2012, 12, 12, 12, 12, 12, 12, time.UTC),
			time.Date(2012, 12, 12, 12, 12, 12, 12, time.UTC), false},
		{Datetime, "2010-06-03", time.Date(2010, 6, 3, 0, 0, 0, 0, time.UTC), false},
		{Datetime, "2010-06-03 12:12:12", time.Date(2010, 6, 3, 12, 12, 12, 0, time.UTC), false},
		{Datetime, "2010-06-03 12:12:12.000012", time.Date(2010, 6, 3, 12, 12, 12, 12000, time.UTC), false},
		{Datetime, "2010-06-03T12:12:12Z", time.Date(2010, 6, 3, 12, 12, 12, 0, time.UTC), false},
		{Datetime, "2010-06-03T12:12:12.000012Z", time.Date(2010, 6, 3, 12, 12, 12, 12000, time.UTC), false},
		{Datetime, "20100603", time.Date(2010, 6, 3, 0, 0, 0, 0, time.UTC), false},
		{Datetime, "20100603121212", time.Date(2010, 6, 3, 12, 12, 12, 0, time.UTC), false},
		{Datetime, "2010-6-3 12:12:12", time.Date(2010, 6, 3, 12, 12, 12, 0, time.UTC), false},
		{Datetime, "2010-6-13 12:12:12", time.Date(2010, 6, 13, 12, 12, 12, 0, time.UTC), false},
		{Datetime, "2010-10-3 12:12:12", time.Date(2010, 10, 3, 12, 12, 12, 0, time.UTC), false},
		{Datetime, "2010-10-3 12:12:2", time.Date(2010, 10, 3, 12, 12, 2, 0, time.UTC), false},
		{Datetime, "2010-10-3 12:2:2", time.Date(2010, 10, 3, 12, 2, 2, 0, time.UTC), false},

		{Timestamp, nil, nil, false},
		{Timestamp, time.Date(2012, 12, 12, 12, 12, 12, 12, time.UTC),
			time.Date(2012, 12, 12, 12, 12, 12, 12, time.UTC), false},
		{Timestamp, "2010-06-03", time.Date(2010, 6, 3, 0, 0, 0, 0, time.UTC), false},
		{Timestamp, "2010-06-03 12:12:12", time.Date(2010, 6, 3, 12, 12, 12, 0, time.UTC), false},
		{Timestamp, "2010-06-03 12:12:12.000012", time.Date(2010, 6, 3, 12, 12, 12, 12000, time.UTC), false},
		{Timestamp, "2010-06-03T12:12:12Z", time.Date(2010, 6, 3, 12, 12, 12, 0, time.UTC), false},
		{Timestamp, "2010-06-03T12:12:12.000012Z", time.Date(2010, 6, 3, 12, 12, 12, 12000, time.UTC), false},
		{Timestamp, "20100603", time.Date(2010, 6, 3, 0, 0, 0, 0, time.UTC), false},
		{Timestamp, "20100603121212", time.Date(2010, 6, 3, 12, 12, 12, 0, time.UTC), false},

		{Date, time.Date(500, 1, 1, 1, 1, 1, 1, time.UTC), nil, true},
		{Date, time.Date(10000, 1, 1, 1, 1, 1, 1, time.UTC), nil, true},
		{Date, "", nil, true},
		{Date, "500-01-01", nil, true},
		{Date, "10000-01-01", nil, true},
		{Date, int(0), nil, true},
		{Date, int8(0), nil, true},
		{Date, int16(0), nil, true},
		{Date, int32(0), nil, true},
		{Date, int64(0), nil, true},
		{Date, uint(0), nil, true},
		{Date, uint8(0), nil, true},
		{Date, uint16(0), nil, true},
		{Date, uint32(0), nil, true},
		{Date, uint64(0), nil, true},
		{Date, float32(0), nil, true},
		{Date, float64(0), nil, true},
		{Date, []byte{0}, nil, true},

		{Datetime, time.Date(500, 1, 1, 1, 1, 1, 1, time.UTC), nil, true},
		{Datetime, time.Date(10000, 1, 1, 1, 1, 1, 1, time.UTC), nil, true},
		{Datetime, int(0), nil, true},
		{Datetime, int8(0), nil, true},
		{Datetime, int16(0), nil, true},
		{Datetime, int32(0), nil, true},
		{Datetime, int64(0), nil, true},
		{Datetime, uint(0), nil, true},
		{Datetime, uint8(0), nil, true},
		{Datetime, uint16(0), nil, true},
		{Datetime, uint32(0), nil, true},
		{Datetime, uint64(0), nil, true},
		{Datetime, float32(0), nil, true},
		{Datetime, float64(0), nil, true},
		{Datetime, []byte{0}, nil, true},

		{Timestamp, time.Date(1960, 1, 1, 1, 1, 1, 1, time.UTC), nil, true},
		{Timestamp, time.Date(2040, 1, 1, 1, 1, 1, 1, time.UTC), nil, true},
		{Timestamp, int(0), nil, true},
		{Timestamp, int8(0), nil, true},
		{Timestamp, int16(0), nil, true},
		{Timestamp, int32(0), nil, true},
		{Timestamp, int64(0), nil, true},
		{Timestamp, uint(0), nil, true},
		{Timestamp, uint8(0), nil, true},
		{Timestamp, uint16(0), nil, true},
		{Timestamp, uint32(0), nil, true},
		{Timestamp, uint64(0), nil, true},
		{Timestamp, float32(0), nil, true},
		{Timestamp, float64(0), nil, true},
		{Timestamp, []byte{0}, nil, true},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%v %v %v", test.typ, test.val, test.expectedVal), func(t *testing.T) {
			val, err := test.typ.Convert(test.val)
			if test.expectedErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, test.expectedVal, val)
			}
		})
	}
}

func TestDatetimeString(t *testing.T) {
	tests := []struct {
		typ         Type
		expectedStr string
	}{
		{MustCreateDatetimeType(sqltypes.Date), "DATE"},
		{MustCreateDatetimeType(sqltypes.Datetime), "DATETIME"},
		{MustCreateDatetimeType(sqltypes.Timestamp), "TIMESTAMP"},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%v %v", test.typ, test.expectedStr), func(t *testing.T) {
			str := test.typ.String()
			assert.Equal(t, test.expectedStr, str)
		})
	}
}
