// Copyright 2020-2026 Dolthub, Inc.
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

package function

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestTime(t *testing.T) {
	ctx := sql.NewEmptyContext()
	f := NewTime(ctx, expression.NewGetField(0, types.LongText, "foo", false))

	testCases := []struct {
		name     string
		row      sql.Row
		expected any
		err      bool
	}{
		{"null date", sql.NewRow(nil), nil, false},
		{"invalid type", sql.NewRow([]byte{0, 1, 2}), nil, false},
		{"time as string", sql.NewRow(stringDate), "14:15:16", false},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			val, err := f.Eval(ctx, tt.row)
			if tt.err {
				require.Error(err)
			} else {
				require.NoError(err)
				if v, ok := val.(types.Timespan); ok {
					require.Equal(tt.expected, v.String())
				} else {
					require.Equal(tt.expected, val)
				}
			}
		})
	}
}

func TestTimePrecision(t *testing.T) {
	ctx := sql.NewEmptyContext()

	testCases := []struct {
		name              string
		child             sql.Expression
		expectedPrecision int
	}{
		{
			name:              "string literal max precision (6)",
			child:             expression.NewLiteral("12:34:56.123456", types.LongText),
			expectedPrecision: 6,
		},
		{
			name:              "string literal 4 precision",
			child:             expression.NewLiteral("12:34:56.1234", types.LongText),
			expectedPrecision: 4,
		},
		{
			name:              "string literal 0 precision",
			child:             expression.NewLiteral("12:34:56", types.LongText),
			expectedPrecision: 0,
		},
		{
			name:              "string literal capped at 6 precision",
			child:             expression.NewLiteral("12:34:56.123456789", types.LongText),
			expectedPrecision: 6,
		},
		{
			name:              "datetime literal with fractional seconds",
			child:             expression.NewLiteral("2024-03-02 12:34:56.123", types.LongText),
			expectedPrecision: 3,
		},
		{
			name:              "time column with precision 5",
			child:             expression.NewGetField(0, types.MustCreateTimespanType(5), "col", false),
			expectedPrecision: 5,
		},
		{
			name:              "datetime column with precision 3",
			child:             expression.NewGetField(0, types.Datetime3, "col", false),
			expectedPrecision: 3,
		},
		{
			name:              "date column (precision 0)",
			child:             expression.NewGetField(0, types.Date, "col", false),
			expectedPrecision: 0,
		},
		{
			name:              "decimal column with scale 2",
			child:             expression.NewGetField(0, types.MustCreateDecimalType(10, 2), "col", false),
			expectedPrecision: 2,
		},
		{
			name:              "integer column (precision 0)",
			child:             expression.NewGetField(0, types.Int64, "col", false),
			expectedPrecision: 0,
		},
		{
			name:              "untyped string column defaults to max precision",
			child:             expression.NewGetField(0, types.LongText, "col", false),
			expectedPrecision: 6,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			f := NewTime(ctx, tt.child)
			timeType, ok := f.Type(ctx).(types.TimeType)
			require.True(t, ok)
			require.Equal(t, tt.expectedPrecision, timeType.Precision())
		})
	}
}

