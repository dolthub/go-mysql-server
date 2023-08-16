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
	"reflect"
	"testing"
	"time"

	sqltypes "github.com/dolthub/vitess/go/sqltypes"
	"github.com/dolthub/vitess/go/vt/proto/query"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
)

func TestDatetimeCompare(t *testing.T) {
	tests := []struct {
		typ         sql.Type
		val1        interface{}
		val2        interface{}
		expectedCmp int
	}{
		{Date, nil, 0, 1},
		{Datetime, 0, nil, -1},
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
		{baseType: sqltypes.Date, expectedType: datetimeType{baseType: sqltypes.Date}},
		{baseType: sqltypes.Datetime, expectedType: datetimeType{baseType: sqltypes.Datetime}},
		{baseType: sqltypes.Timestamp, expectedType: datetimeType{baseType: sqltypes.Timestamp}},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%v", test.baseType), func(t *testing.T) {
			typ, err := CreateDatetimeType(test.baseType, 0)
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
			typ, err := CreateDatetimeType(test.baseType, 0)
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
	type testcase struct {
		typ         sql.Type
		val         interface{}
		expectedVal interface{}
		expectedErr bool
	}
	tests := []testcase {
		{Date, nil, nil, false},
		{Date, time.Date(2012, 12, 12, 12, 12, 12, 12, time.UTC),
			time.Date(2012, 12, 12, 0, 0, 0, 0, time.UTC), false},
		{Date, "2010-06-03", time.Date(2010, 6, 3, 0, 0, 0, 0, time.UTC), false},
		{Date, "2010-6-3", time.Date(2010, 6, 3, 0, 0, 0, 0, time.UTC), false},
		{Date, "2010-6-03", time.Date(2010, 6, 3, 0, 0, 0, 0, time.UTC), false},
		{Date, "2010-06-3", time.Date(2010, 6, 3, 0, 0, 0, 0, time.UTC), false},
		{Date, "2010-06-03 12:12:12", time.Date(2010, 6, 3, 0, 0, 0, 0, time.UTC), false},
		{Date, "2010-06-03 12:12:12.000012", time.Date(2010, 6, 3, 0, 0, 0, 0, time.UTC), false},
		{Date, "2010-06-03T12:12:12Z", time.Date(2010, 6, 3, 0, 0, 0, 0, time.UTC), false},
		{Date, "2010-06-03T12:12:12.000012Z", time.Date(2010, 6, 3, 0, 0, 0, 0, time.UTC), false},
		{Date, "20100603", time.Date(2010, 6, 3, 0, 0, 0, 0, time.UTC), false},
		{Date, "20100603121212", time.Date(2010, 6, 3, 0, 0, 0, 0, time.UTC), false},

		{DatetimeMaxPrecision, nil, nil, false},
		{DatetimeMaxPrecision, time.Date(2012, 12, 12, 12, 12, 12, 12, time.UTC),
			time.Date(2012, 12, 12, 12, 12, 12, 0, time.UTC), false},
		{DatetimeMaxPrecision, time.Date(2012, 12, 12, 12, 12, 12, 12345, time.UTC),
			time.Date(2012, 12, 12, 12, 12, 12, 12000, time.UTC), false},
		{DatetimeMaxPrecision, "2010-06-03", time.Date(2010, 6, 3, 0, 0, 0, 0, time.UTC), false},
		{DatetimeMaxPrecision, "2010-6-3", time.Date(2010, 6, 3, 0, 0, 0, 0, time.UTC), false},
		{DatetimeMaxPrecision, "2010-06-3", time.Date(2010, 6, 3, 0, 0, 0, 0, time.UTC), false},
		{DatetimeMaxPrecision, "2010-6-03", time.Date(2010, 6, 3, 0, 0, 0, 0, time.UTC), false},
		{DatetimeMaxPrecision, "2010-06-03 12:12:12", time.Date(2010, 6, 3, 12, 12, 12, 0, time.UTC), false},
		{DatetimeMaxPrecision, "2010-06-03 12:12:12.000012", time.Date(2010, 6, 3, 12, 12, 12, 12000, time.UTC), false},
		{DatetimeMaxPrecision, "2010-06-03T12:12:12Z", time.Date(2010, 6, 3, 12, 12, 12, 0, time.UTC), false},
		{DatetimeMaxPrecision, "2010-06-03T12:12:12.000012Z", time.Date(2010, 6, 3, 12, 12, 12, 12000, time.UTC), false},
		{DatetimeMaxPrecision, "20100603", time.Date(2010, 6, 3, 0, 0, 0, 0, time.UTC), false},
		{DatetimeMaxPrecision, "20100603121212", time.Date(2010, 6, 3, 12, 12, 12, 0, time.UTC), false},
		{DatetimeMaxPrecision, "2010-6-3 12:12:12", time.Date(2010, 6, 3, 12, 12, 12, 0, time.UTC), false},
		{DatetimeMaxPrecision, "2010-6-13 12:12:12", time.Date(2010, 6, 13, 12, 12, 12, 0, time.UTC), false},
		{DatetimeMaxPrecision, "2010-10-3 12:12:12", time.Date(2010, 10, 3, 12, 12, 12, 0, time.UTC), false},
		{DatetimeMaxPrecision, "2010-10-3 12:12:2", time.Date(2010, 10, 3, 12, 12, 2, 0, time.UTC), false},
		{DatetimeMaxPrecision, "2010-10-3 12:2:2", time.Date(2010, 10, 3, 12, 2, 2, 0, time.UTC), false},
		{DatetimeMaxPrecision, "2010-06-03 12:3", time.Date(2010, 6, 3, 12, 3, 0, 0, time.UTC), false},
		{DatetimeMaxPrecision, "2010-06-03 12:34", time.Date(2010, 6, 3, 12, 34, 0, 0, time.UTC), false},
		{DatetimeMaxPrecision, "2010-06-03 12:34:", time.Date(2010, 6, 3, 12, 34, 0, 0, time.UTC), false},
		{DatetimeMaxPrecision, "2010-06-03 12:34:.", time.Date(2010, 6, 3, 12, 34, 0, 0, time.UTC), false},
		{DatetimeMaxPrecision, "2010-06-03 12:34:5", time.Date(2010, 6, 3, 12, 34, 5, 0, time.UTC), false},
		{DatetimeMaxPrecision, "2010-06-03 12:34:56", time.Date(2010, 6, 3, 12, 34, 56, 0, time.UTC), false},
		{DatetimeMaxPrecision, "2010-06-03 12:34:56.", time.Date(2010, 6, 3, 12, 34, 56, 0, time.UTC), false},
		{DatetimeMaxPrecision, "2010-06-03 12:34:56.7", time.Date(2010, 6, 3, 12, 34, 56, 700000000, time.UTC), false},
		{DatetimeMaxPrecision, "2010-06-03 12:34:56.78", time.Date(2010, 6, 3, 12, 34, 56, 780000000, time.UTC), false},
		{DatetimeMaxPrecision, "2010-06-03 12:34:56.789", time.Date(2010, 6, 3, 12, 34, 56, 789000000, time.UTC), false},

		{MustCreateDatetimeType(sqltypes.Datetime, 3), nil, nil, false},
		{MustCreateDatetimeType(sqltypes.Datetime, 3), time.Date(2012, 12, 12, 12, 12, 12, 12345678, time.UTC),
			time.Date(2012, 12, 12, 12, 12, 12, 12000000, time.UTC), false},
		{MustCreateDatetimeType(sqltypes.Datetime, 3), time.Date(2012, 12, 12, 12, 12, 12, 12345678, time.UTC),
			time.Date(2012, 12, 12, 12, 12, 12, 12000000, time.UTC), false},
		{MustCreateDatetimeType(sqltypes.Datetime, 3), "2010-06-03 12:12:12.123456", time.Date(2010, 6, 3, 12, 12, 12, 123000000, time.UTC), false},
		{MustCreateDatetimeType(sqltypes.Datetime, 3), "2010-06-03T12:12:12.123456Z", time.Date(2010, 6, 3, 12, 12, 12, 123000000, time.UTC), false},
		{MustCreateDatetimeType(sqltypes.Datetime, 3), "2010-06-03 12:34:56.7", time.Date(2010, 6, 3, 12, 34, 56, 700000000, time.UTC), false},
		{MustCreateDatetimeType(sqltypes.Datetime, 3), "2010-06-03 12:34:56.78", time.Date(2010, 6, 3, 12, 34, 56, 780000000, time.UTC), false},
		{MustCreateDatetimeType(sqltypes.Datetime, 3), "2010-06-03 12:34:56.789", time.Date(2010, 6, 3, 12, 34, 56, 789000000, time.UTC), false},

		{Datetime, nil, nil, false},
		{Datetime, time.Date(2012, 12, 12, 12, 12, 12, 12345678, time.UTC),
			time.Date(2012, 12, 12, 12, 12, 12, 0, time.UTC), false},
		{Datetime, time.Date(2012, 12, 12, 12, 12, 12, 12345678, time.UTC),
			time.Date(2012, 12, 12, 12, 12, 12, 0, time.UTC), false},
		{Datetime, "2010-06-03 12:12:12.123456", time.Date(2010, 6, 3, 12, 12, 12, 0, time.UTC), false},
		{Datetime, "2010-06-03T12:12:12.123456Z", time.Date(2010, 6, 3, 12, 12, 12, 0, time.UTC), false},
		{Datetime, "2010-06-03 12:34:56.7", time.Date(2010, 6, 3, 12, 34, 56, 0, time.UTC), false},
		{Datetime, "2010-06-03 12:34:56.78", time.Date(2010, 6, 3, 12, 34, 56, 0, time.UTC), false},
		{Datetime, "2010-06-03 12:34:56.789", time.Date(2010, 6, 3, 12, 34, 56, 0, time.UTC), false},

		{TimestampMaxPrecision, nil, nil, false},
		{TimestampMaxPrecision, time.Date(2012, 12, 12, 12, 12, 12, 12, time.UTC),
			time.Date(2012, 12, 12, 12, 12, 12, 0, time.UTC), false},
		{TimestampMaxPrecision, time.Date(2012, 12, 12, 12, 12, 12, 12345, time.UTC),
			time.Date(2012, 12, 12, 12, 12, 12, 12000, time.UTC), false},
		{TimestampMaxPrecision, "2010-06-03", time.Date(2010, 6, 3, 0, 0, 0, 0, time.UTC), false},
		{TimestampMaxPrecision, "2010-6-3", time.Date(2010, 6, 3, 0, 0, 0, 0, time.UTC), false},
		{TimestampMaxPrecision, "2010-6-03", time.Date(2010, 6, 3, 0, 0, 0, 0, time.UTC), false},
		{TimestampMaxPrecision, "2010-06-3", time.Date(2010, 6, 3, 0, 0, 0, 0, time.UTC), false},
		{TimestampMaxPrecision, "2010-06-03 12:12:12", time.Date(2010, 6, 3, 12, 12, 12, 0, time.UTC), false},
		{TimestampMaxPrecision, "2010-06-03 12:12:12.000012", time.Date(2010, 6, 3, 12, 12, 12, 12000, time.UTC), false},
		{TimestampMaxPrecision, "2010-06-03T12:12:12Z", time.Date(2010, 6, 3, 12, 12, 12, 0, time.UTC), false},
		{TimestampMaxPrecision, "2010-06-03T12:12:12.000012Z", time.Date(2010, 6, 3, 12, 12, 12, 12000, time.UTC), false},
		{TimestampMaxPrecision, "20100603", time.Date(2010, 6, 3, 0, 0, 0, 0, time.UTC), false},
		{TimestampMaxPrecision, "20100603121212", time.Date(2010, 6, 3, 12, 12, 12, 0, time.UTC), false},
		{TimestampMaxPrecision, time.Date(2012, 12, 12, 12, 12, 12, 12345, time.UTC).UTC().String(), time.Date(2012, 12, 12, 12, 12, 12, 12000, time.UTC), false},

		{MustCreateDatetimeType(sqltypes.Timestamp, 3), nil, nil, false},
		{MustCreateDatetimeType(sqltypes.Timestamp, 3), time.Date(2012, 12, 12, 12, 12, 12, 12345678, time.UTC),
			time.Date(2012, 12, 12, 12, 12, 12, 12000000, time.UTC), false},
		{MustCreateDatetimeType(sqltypes.Timestamp, 3), time.Date(2012, 12, 12, 12, 12, 12, 12345678, time.UTC),
			time.Date(2012, 12, 12, 12, 12, 12, 12000000, time.UTC), false},
		{MustCreateDatetimeType(sqltypes.Timestamp, 3), "2010-06-03 12:12:12.123456", time.Date(2010, 6, 3, 12, 12, 12, 123000000, time.UTC), false},
		{MustCreateDatetimeType(sqltypes.Timestamp, 3), "2010-06-03T12:12:12.123456Z", time.Date(2010, 6, 3, 12, 12, 12, 123000000, time.UTC), false},
		{MustCreateDatetimeType(sqltypes.Timestamp, 3), "2010-06-03 12:34:56.7", time.Date(2010, 6, 3, 12, 34, 56, 700000000, time.UTC), false},
		{MustCreateDatetimeType(sqltypes.Timestamp, 3), "2010-06-03 12:34:56.78", time.Date(2010, 6, 3, 12, 34, 56, 780000000, time.UTC), false},
		{MustCreateDatetimeType(sqltypes.Timestamp, 3), "2010-06-03 12:34:56.789", time.Date(2010, 6, 3, 12, 34, 56, 789000000, time.UTC), false},

		{Timestamp, nil, nil, false},
		{Timestamp, time.Date(2012, 12, 12, 12, 12, 12, 12345678, time.UTC),
			time.Date(2012, 12, 12, 12, 12, 12, 0, time.UTC), false},
		{Timestamp, time.Date(2012, 12, 12, 12, 12, 12, 12345678, time.UTC),
			time.Date(2012, 12, 12, 12, 12, 12, 0, time.UTC), false},
		{Timestamp, "2010-06-03 12:12:12.123456", time.Date(2010, 6, 3, 12, 12, 12, 0, time.UTC), false},
		{Timestamp, "2010-06-03T12:12:12.123456Z", time.Date(2010, 6, 3, 12, 12, 12, 0, time.UTC), false},
		{Timestamp, "2010-06-03 12:34:56.7", time.Date(2010, 6, 3, 12, 34, 56, 0, time.UTC), false},
		{Timestamp, "2010-06-03 12:34:56.78", time.Date(2010, 6, 3, 12, 34, 56, 0, time.UTC), false},
		{Timestamp, "2010-06-03 12:34:56.789", time.Date(2010, 6, 3, 12, 34, 56, 0, time.UTC), false},
		
		{Date, "0000-01-01 00:00:00", time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC), false},
		{Date, "0500-01-01 00:00:00", time.Date(500, 1, 1, 0, 0, 0, 0, time.UTC), false},
		{Date, time.Date(10000, 1, 1, 1, 1, 1, 1, time.UTC), nil, true},
		{Date, "", nil, true},
		{Date, "0500-01-01", time.Date(500, 1, 1, 0, 0, 0, 0, time.UTC), false},
		{Date, "10000-01-01", nil, true},
		{Date, int(0), time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC), false},
		{Date, int8(0), time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC), false},
		{Date, int16(0), time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC), false},
		{Date, int32(0), time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC), false},
		{Date, int64(0), time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC), false},
		{Date, uint(0), time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC), false},
		{Date, uint8(0), time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC), false},
		{Date, uint16(0), time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC), false},
		{Date, uint32(0), time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC), false},
		{Date, uint64(0), time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC), false},
		{Date, float32(0), time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC), false},
		{Date, float64(0), time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC), false},
		{Date, []byte{0}, nil, true},

		{DatetimeMaxPrecision, "0500-01-01 01:01:01", time.Date(500, 1, 1, 1, 1, 1, 0, time.UTC), false},
		{DatetimeMaxPrecision, "0000-01-01 00:00:00", time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC), false},
		{DatetimeMaxPrecision, time.Date(10000, 1, 1, 1, 1, 1, 1, time.UTC), nil, true},
		{DatetimeMaxPrecision, int(0), time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC), false},
		{DatetimeMaxPrecision, int8(0), time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC), false},
		{DatetimeMaxPrecision, int16(0), time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC), false},
		{DatetimeMaxPrecision, int32(0), time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC), false},
		{DatetimeMaxPrecision, int64(0), time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC), false},
		{DatetimeMaxPrecision, uint(0), time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC), false},
		{DatetimeMaxPrecision, uint8(0), time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC), false},
		{DatetimeMaxPrecision, uint16(0), time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC), false},
		{DatetimeMaxPrecision, uint32(0), time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC), false},
		{DatetimeMaxPrecision, uint64(0), time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC), false},
		{DatetimeMaxPrecision, float32(0), time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC), false},
		{DatetimeMaxPrecision, float64(0), time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC), false},
		{DatetimeMaxPrecision, []byte{0}, nil, true},

		{TimestampMaxPrecision, time.Date(1960, 1, 1, 1, 1, 1, 1, time.UTC), nil, true},
		{TimestampMaxPrecision, "1970-01-01 00:00:00", nil, true},
		{TimestampMaxPrecision, "1970-01-01 00:00:01", time.Date(1970, 1, 1, 0, 0, 1, 0, time.UTC), false},
		{TimestampMaxPrecision, time.Date(2040, 1, 1, 1, 1, 1, 1, time.UTC), nil, true},
		{TimestampMaxPrecision, int(0), time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC), false},
		{TimestampMaxPrecision, int8(0), time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC), false},
		{TimestampMaxPrecision, int16(0), time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC), false},
		{TimestampMaxPrecision, int32(0), time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC), false},
		{TimestampMaxPrecision, int64(0), time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC), false},
		{TimestampMaxPrecision, uint(0), time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC), false},
		{TimestampMaxPrecision, uint8(0), time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC), false},
		{TimestampMaxPrecision, uint16(0), time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC), false},
		{TimestampMaxPrecision, uint32(0), time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC), false},
		{TimestampMaxPrecision, uint64(0), time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC), false},
		{TimestampMaxPrecision, float32(0), time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC), false},
		{TimestampMaxPrecision, float64(0), time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC), false},
		{TimestampMaxPrecision, []byte{0}, nil, true},

		{Date, int(1), nil, true},
		{Date, int8(1), nil, true},
		{Date, int16(1), nil, true},
		{Date, int32(1), nil, true},
		{Date, int64(1), nil, true},
		{Date, uint(1), nil, true},
		{Date, uint8(1), nil, true},
		{Date, uint16(1), nil, true},
		{Date, uint32(1), nil, true},
		{Date, uint64(1), nil, true},
		{Date, float32(1), nil, true},
		{Date, float64(1), nil, true},

		{DatetimeMaxPrecision, int(1), nil, true},
		{DatetimeMaxPrecision, int8(1), nil, true},
		{DatetimeMaxPrecision, int16(1), nil, true},
		{DatetimeMaxPrecision, int32(1), nil, true},
		{DatetimeMaxPrecision, int64(1), nil, true},
		{DatetimeMaxPrecision, uint(1), nil, true},
		{DatetimeMaxPrecision, uint8(1), nil, true},
		{DatetimeMaxPrecision, uint16(1), nil, true},
		{DatetimeMaxPrecision, uint32(1), nil, true},
		{DatetimeMaxPrecision, uint64(1), nil, true},
		{DatetimeMaxPrecision, float32(1), nil, true},
		{DatetimeMaxPrecision, float64(1), nil, true},

		{TimestampMaxPrecision, int(1), nil, true},
		{TimestampMaxPrecision, int8(1), nil, true},
		{TimestampMaxPrecision, int16(1), nil, true},
		{TimestampMaxPrecision, int32(1), nil, true},
		{TimestampMaxPrecision, int64(1), nil, true},
		{TimestampMaxPrecision, uint(1), nil, true},
		{TimestampMaxPrecision, uint8(1), nil, true},
		{TimestampMaxPrecision, uint16(1), nil, true},
		{TimestampMaxPrecision, uint32(1), nil, true},
		{TimestampMaxPrecision, uint64(1), nil, true},
		{TimestampMaxPrecision, float32(1), nil, true},
		{TimestampMaxPrecision, float64(1), nil, true},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%v %v %v", test.typ, test.val, test.expectedVal), func(t *testing.T) {
			val, _, err := test.typ.Convert(test.val)
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
		typ         sql.Type
		expectedStr string
	}{
		{MustCreateDatetimeType(sqltypes.Date, 0), "date"},
		{MustCreateDatetimeType(sqltypes.Datetime, 0), "datetime"},
		{datetimeType{baseType: sqltypes.Datetime, precision: 3}, "datetime(3)"},
		{datetimeType{baseType: sqltypes.Datetime, precision: 6}, "datetime(6)"},
		{MustCreateDatetimeType(sqltypes.Timestamp, 0), "timestamp"},
		{MustCreateDatetimeType(sqltypes.Timestamp, 3), "timestamp(3)"},
		{MustCreateDatetimeType(sqltypes.Timestamp, 6), "timestamp(6)"},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%v %v", test.typ, test.expectedStr), func(t *testing.T) {
			str := test.typ.String()
			assert.Equal(t, test.expectedStr, str)
		})
	}
}

func TestDatetimeZero(t *testing.T) {
	_, ok := MustCreateDatetimeType(sqltypes.Date, 0).Zero().(time.Time)
	require.True(t, ok)
	_, ok = MustCreateDatetimeType(sqltypes.Datetime, 0).Zero().(time.Time)
	require.True(t, ok)
	_, ok = MustCreateDatetimeType(sqltypes.Timestamp, 0).Zero().(time.Time)
	require.True(t, ok)
}
