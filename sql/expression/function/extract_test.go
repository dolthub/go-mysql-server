// Copyright 2023-2024 Dolthub, Inc.
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
	"time"


	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestExtract(t *testing.T) {
	testCases := []struct {
		name     string
		unit     string
		dateTime string
		expected interface{}
		skip     bool
	}{
		{
			name:     "get year",
			unit:     "YEAR",
			dateTime: "2023-11-12 11:22:33.445566",
			expected: 2023,
		},
		{
			name:     "get quarter 1",
			unit:     "QUARTER",
			dateTime: "2023-3-12 11:22:33.445566",
			expected: 1,
		},
		{
			name:     "get quarter 2",
			unit:     "QUARTER",
			dateTime: "2023-6-12 11:22:33.445566",
			expected: 2,
		},
		{
			name:     "get quarter 3",
			unit:     "QUARTER",
			dateTime: "2023-9-12 11:22:33.445566",
			expected: 3,
		},
		{
			name:     "get quarter 4",
			unit:     "QUARTER",
			dateTime: "2023-11-12 11:22:33.445566",
			expected: 4,
		},
		{
			name:     "get month",
			unit:     "MONTH",
			dateTime: "2023-11-12 11:22:33.445566",
			expected: 11,
		},
		{
			name:     "get week",
			unit:     "WEEK",
			dateTime: "2023-11-12 11:22:33.445566",
			expected: 46,
		},
		{
			name:     "get day",
			unit:     "DAY",
			dateTime: "2023-11-12 11:22:33.445566",
			expected: 12,
		},
		{
			name:     "get hour",
			unit:     "HOUR",
			dateTime: "2023-11-12 11:22:33.445566",
			expected: 11,
		},
		{
			name:     "get minute",
			unit:     "MINUTE",
			dateTime: "2023-11-12 11:22:33.445566",
			expected: 22,
		},
		{
			name:     "get second",
			unit:     "SECOND",
			dateTime: "2023-11-12 11:22:33.445566",
			expected: 33,
		},
		{
			name:     "get microsecond",
			unit:     "MICROSECOND",
			dateTime: "2023-11-12 11:22:33.445566",
			expected: 445566,
		},
		{
			name:     "get year_month",
			unit:     "YEAR_MONTH",
			dateTime: "2023-11-12 11:22:33.445566",
			expected: 202311,
		},
		{
			name:     "get day_hour",
			unit:     "DAY_HOUR",
			dateTime: "2023-11-12 11:22:33.445566",
			expected: 1211,
		},
		{
			name:     "get day_minute",
			unit:     "DAY_MINUTE",
			dateTime: "2023-11-12 11:22:33.445566",
			expected: 121122,
		},
		{
			name:     "get day_second",
			unit:     "DAY_SECOND",
			dateTime: "2023-11-12 11:22:33.445566",
			expected: 12112233,
		},
		{
			name:     "get day_microsecond",
			unit:     "DAY_MICROSECOND",
			dateTime: "2023-11-12 11:22:33.445566",
			expected: 12112233445566,
		},
		{
			name:     "get hour_minute",
			unit:     "HOUR_MINUTE",
			dateTime: "2023-11-12 11:22:33.445566",
			expected: 1122,
		},
		{
			name:     "get hour_second",
			unit:     "HOUR_SECOND",
			dateTime: "2023-11-12 11:22:33.445566",
			expected: 112233,
		},
		{
			name:     "get hour_microsecond",
			unit:     "HOUR_MICROSECOND",
			dateTime: "2023-11-12 11:22:33.445566",
			expected: 112233445566,
		},
		{
			name:     "get minute_second",
			unit:     "MINUTE_SECOND",
			dateTime: "2023-11-12 11:22:33.445566",
			expected: 2233,
		},
		{
			name:     "get minute_microsecond",
			unit:     "MINUTE_MICROSECOND",
			dateTime: "2023-11-12 11:22:33.445566",
			expected: 2233445566,
		},
		{
			name:     "get second_microsecond",
			unit:     "SECOND_MICROSECOND",
			dateTime: "2023-11-12 11:22:33.445566",
			expected: 33445566,
		},
		{
			name:     "get month 0",
			unit:     "MONTH",
			dateTime: "2023-00-12 11:22:33.445566",
			expected: 0,
			skip:     true,
		},
		{
			name:     "get quarter 0",
			unit:     "QUARTER",
			dateTime: "2023-00-12 11:22:33.445566",
			expected: 0,
			skip:     true,
		},
		{
			name:     "get day 0",
			unit:     "DAY",
			dateTime: "2023-01-00 11:22:33.445566",
			expected: 0,
			skip:     true,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			if tt.skip {
				t.Skip()
			}
			require := require.New(t)
			f := NewExtract(expression.NewLiteral(tt.unit, types.LongText), expression.NewLiteral(tt.dateTime, types.LongText))
			v, err := f.Eval(sql.NewEmptyContext(), nil)
			require.NoError(err)
			require.Equal(tt.expected, v)
		})
	}

	t.Run("test extract null datetime", func(t *testing.T) {
		require := require.New(t)
		f := NewExtract(expression.NewLiteral("DAY", types.LongText), expression.NewLiteral(nil, types.Null))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(nil, v)
	})

	t.Run("test extract null units", func(t *testing.T) {
		require := require.New(t)
		f := NewExtract(expression.NewLiteral(nil, types.Null), expression.NewLiteral("2023-11-12 11:22:33.445566", types.LongText))
		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(nil, v)
	})

	t.Run("test bad date", func(t *testing.T) {
		require := require.New(t)
		f := NewExtract(expression.NewLiteral("DAY", types.LongText), expression.NewLiteral("thisisnotadate", types.LongText))
		ctx := sql.NewEmptyContext()
		_, err := f.Eval(ctx, nil)
		require.NoError(err)
		require.Equal(1, len(ctx.Warnings()))
		require.Equal(1292, ctx.Warnings()[0].Code)
	})
}

func TestExtractWithTime(t *testing.T) {
	now := time.Date(2001, 2, 3, 4, 5, 6, 7, time.UTC)
	data := "12:34:56.778899"
	testCases := []struct {
		name     string
		unit     string
		expected interface{}
		skip     bool
	}{
		{
			name:     "get year",
			unit:     "YEAR",
			expected:  now.Year(),
		},
		{
			name:     "get quarter",
			unit:     "QUARTER",
			expected: 1,
		},
		{
			name:     "get month",
			unit:     "MONTH",
			expected: 2,
		},
		{
			name:     "get week",
			unit:     "WEEK",
			expected: 5,
		},
		{
			name:     "get day",
			unit:     "DAY",
			expected: 3,
		},
		{
			name:     "get hour",
			unit:     "HOUR",
			expected: 12,
		},
		{
			name:     "get minute",
			unit:     "MINUTE",
			expected: 34,
		},
		{
			name:     "get second",
			unit:     "SECOND",
			expected: 56,
		},
		{
			name:     "get microsecond",
			unit:     "MICROSECOND",
			expected: 778899,
		},
		{
			name:     "get year_month",
			unit:     "YEAR_MONTH",
			expected: now.Year() * 100 + int(now.Month()),
		},
		{
			name:     "get day_hour",
			unit:     "DAY_HOUR",
			expected: now.Day() * 100 + 12,
		},
		{
			name:     "get day_minute",
			unit:     "DAY_MINUTE",
			expected: now.Day() * 10000 + 1234,
		},
		{
			name:     "get day_second",
			unit:     "DAY_SECOND",
			expected: now.Day() * 1_000_000 + 123456,
		},
		{
			name:     "get day_microsecond",
			unit:     "DAY_MICROSECOND",
			expected: now.Day() * 1_000_000_000_000 + 123456778899,
		},
		{
			name:     "get hour_minute",
			unit:     "HOUR_MINUTE",
			expected: 1234,
		},
		{
			name:     "get hour_second",
			unit:     "HOUR_SECOND",
			expected: 123456,
		},
		{
			name:     "get hour_microsecond",
			unit:     "HOUR_MICROSECOND",
			expected: 123456778899,
		},
		{
			name:     "get minute_second",
			unit:     "MINUTE_SECOND",
			expected: 3456,
		},
		{
			name:     "get minute_microsecond",
			unit:     "MINUTE_MICROSECOND",
			expected: 3456778899,
		},
		{
			name:     "get second_microsecond",
			unit:     "SECOND_MICROSECOND",
			expected: 56778899,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			if tt.skip {
				t.Skip()
			}
			require := require.New(t)
			f := NewExtract(expression.NewLiteral(tt.unit, types.LongText), expression.NewLiteral(data, types.LongText))
			ctx := sql.NewEmptyContext()
			ctx.SetQueryTime(now)
			v, err := f.Eval(ctx, nil)
			require.NoError(err)
			require.Equal(tt.expected, v)
		})
	}
}
