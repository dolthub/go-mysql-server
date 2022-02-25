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

package server

import (
	"math"
	"testing"

	"github.com/dolthub/vitess/go/vt/proto/query"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
)

func TestCapRequirements(t *testing.T) {
	tests := []struct {
		name string
		req  capRequirement
		typ  sql.Type
		vals []interface{}
	}{
		{
			name: "int8",
			req:  int8CapRequirement,
			typ:  mustNumberType(query.Type_INT8),
			vals: []interface{}{
				math.MaxInt8,
				math.MinInt8,
			},
		},
		{
			name: "uint8",
			req:  uint8CapRequirement,
			typ:  mustNumberType(query.Type_UINT8),
			vals: []interface{}{
				math.MaxUint8,
			},
		},
		{
			name: "int16",
			req:  int16CapRequirement,
			typ:  mustNumberType(query.Type_INT16),
			vals: []interface{}{
				math.MaxInt16,
				math.MinInt16,
			},
		},
		{
			name: "uint16",
			req:  uint16CapRequirement,
			typ:  mustNumberType(query.Type_UINT16),
			vals: []interface{}{
				math.MaxUint16,
			},
		},
		{
			name: "int32",
			req:  int32CapRequirement,
			typ:  mustNumberType(query.Type_INT32),
			vals: []interface{}{
				math.MaxInt32,
				math.MinInt32,
			},
		},
		{
			name: "uint32",
			req:  uint32CapRequirement,
			typ:  mustNumberType(query.Type_UINT32),
			vals: []interface{}{
				math.MaxUint32,
			},
		},
		{
			name: "int64",
			req:  int64CapRequirement,
			typ:  mustNumberType(query.Type_INT64),
			vals: []interface{}{
				math.MaxInt64,
				math.MinInt64,
			},
		},
		{
			name: "uint64",
			req:  uint64CapRequirement,
			typ:  mustNumberType(query.Type_UINT64),
			vals: []interface{}{
				uint64(math.MaxUint64),
			},
		},
		{
			name: "float32",
			req:  float32CapRequirement,
			typ:  mustNumberType(query.Type_FLOAT32),
			vals: []interface{}{
				float32(math.MaxFloat32),
				float32(-math.MaxFloat32),
				float32(math.SmallestNonzeroFloat32),
				float32(-math.SmallestNonzeroFloat32),
			},
		},
		{
			name: "float64",
			req:  float64CapRequirement,
			typ:  mustNumberType(query.Type_FLOAT64),
			vals: []interface{}{
				float64(math.MaxFloat64),
				float64(-math.MaxFloat64),
				float64(math.SmallestNonzeroFloat64),
				float64(-math.SmallestNonzeroFloat64),
			},
		},
		{
			name: "timestampCapRequirement",
			req:  timestampCapRequirement,
			typ:  sql.Timestamp,
			vals: []interface{}{
				"2010-06-03",
				"2010-6-3",
				"2010-6-03",
				"2010-06-3",
				"2010-06-03 12:12:12",
				"2010-06-03 12:12:12.000012",
				"2010-06-03T12:12:12Z",
				"2010-06-03T12:12:12.000012Z",
				"20100603",
				"20100603121212",
			},
		},
		{
			name: "dateCapRequirement",
			req:  dateCapRequirement,
			typ:  sql.Date,
			vals: []interface{}{
				"2010-06-03",
				"2010-6-3",
				"2010-6-03",
				"2010-06-3",
				"2010-06-03 12:12:12",
				"2010-06-03 12:12:12.000012",
				"2010-06-03T12:12:12Z",
				"2010-06-03T12:12:12.000012Z",
				"20100603",
				"20100603121212",
			},
		},
		{
			name: "timeCapRequirement",
			req:  timeCapRequirement,
			typ:  sql.Time,
			vals: []interface{}{
				"40.134608",
				"401122.134612",
				"401122.134612585",
				"595959.99999951",
				"585859.999999514",
				"40:11:22.134612585",
				"59:59:59.9999995",
				"58:59:59.99999951",
				"58:58:59.999999514",
				"11:12",
				"-850:00:00",
				"850:00:00",
				"-838:59:59.1",
				"838:59:59.1",
			},
		},
		{
			name: "datetimeCapRequirement",
			req:  datetimeCapRequirement,
			typ:  sql.Datetime,
			vals: []interface{}{
				"2010-06-03",
				"2010-6-3",
				"2010-06-3",
				"2010-6-03",
				"2010-06-03 12:12:12",
				"2010-06-03 12:12:12.000012",
				"2010-06-03T12:12:12Z",
				"2010-06-03T12:12:12.000012Z",
				"20100603",
				"20100603121212",
				"2010-6-3 12:12:12",
				"2010-6-13 12:12:12",
				"2010-10-3 12:12:12",
				"2010-10-3 12:12:2",
				"2010-10-3 12:2:2",
			},
		},
		{
			name: "yearCapRequirement",
			req:  yearCapRequirement,
			typ:  sql.Year,
			vals: []interface{}{
				"0",
				"1",
				"31",
				"32",
				"69",
				"70",
				"99",
				"1901",
				"2000",
				"2100",
				"2155",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.NotEmpty(t, test.vals)
			for _, v := range test.vals {
				val, err := test.typ.SQL(nil, v)
				require.NoError(t, err)
				assert.GreaterOrEqual(t, test.req, uint(val.Len()))
			}
		})
	}
}

func mustNumberType(t query.Type) (typ sql.Type) {
	var err error
	typ, err = sql.CreateNumberType(t)
	if err != nil {
		panic(err)
	}
	return
}
