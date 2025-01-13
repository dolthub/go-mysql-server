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

package expression

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestTimeDelta(t *testing.T) {
	leapYear := date(2004, time.February, 29, 0, 0, 0, 0)
	testCases := []struct {
		name   string
		delta  TimeDelta
		date   time.Time
		output time.Time
	}{
		{
			"leap year minus one year",
			TimeDelta{Years: -1},
			leapYear,
			date(2003, time.February, 28, 0, 0, 0, 0),
		},
		{
			"leap year plus one year",
			TimeDelta{Years: 1},
			leapYear,
			date(2005, time.February, 28, 0, 0, 0, 0),
		},
		{
			"plus overflowing months",
			TimeDelta{Months: 13},
			leapYear,
			date(2005, time.March, 29, 0, 0, 0, 0),
		},
		{
			"plus overflowing until december",
			TimeDelta{Months: 22},
			leapYear,
			date(2006, time.December, 29, 0, 0, 0, 0),
		},
		{
			"minus overflowing months",
			TimeDelta{Months: -13},
			leapYear,
			date(2003, time.January, 29, 0, 0, 0, 0),
		},
		{
			"minus overflowing until december",
			TimeDelta{Months: -14},
			leapYear,
			date(2002, time.December, 29, 0, 0, 0, 0),
		},
		{
			"minus months",
			TimeDelta{Months: -1},
			leapYear,
			date(2004, time.January, 29, 0, 0, 0, 0),
		},
		{
			"plus months",
			TimeDelta{Months: 1},
			leapYear,
			date(2004, time.March, 29, 0, 0, 0, 0),
		},
		{
			"minus days",
			TimeDelta{Days: -2},
			leapYear,
			date(2004, time.February, 27, 0, 0, 0, 0),
		},
		{
			"plus days",
			TimeDelta{Days: 1},
			leapYear,
			date(2004, time.March, 1, 0, 0, 0, 0),
		},
		{
			"minus hours",
			TimeDelta{Hours: -2},
			leapYear,
			date(2004, time.February, 28, 22, 0, 0, 0),
		},
		{
			"plus hours",
			TimeDelta{Hours: 26},
			leapYear,
			date(2004, time.March, 1, 2, 0, 0, 0),
		},
		{
			"minus minutes",
			TimeDelta{Minutes: -2},
			leapYear,
			date(2004, time.February, 28, 23, 58, 0, 0),
		},
		{
			"plus minutes",
			TimeDelta{Minutes: 26},
			leapYear,
			date(2004, time.February, 29, 0, 26, 0, 0),
		},
		{
			"minus seconds",
			TimeDelta{Seconds: -2},
			leapYear,
			date(2004, time.February, 28, 23, 59, 58, 0),
		},
		{
			"plus seconds",
			TimeDelta{Seconds: 26},
			leapYear,
			date(2004, time.February, 29, 0, 0, 26, 0),
		},
		{
			"minus microseconds",
			TimeDelta{Microseconds: -2},
			leapYear,
			date(2004, time.February, 28, 23, 59, 59, 999998),
		},
		{
			"plus microseconds",
			TimeDelta{Microseconds: 26},
			leapYear,
			date(2004, time.February, 29, 0, 0, 0, 26),
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.delta.Add(tt.date)
			require.Equal(t, tt.output, result)
		})
	}
}

func TestIntervalEvalDelta(t *testing.T) {
	testCases := []struct {
		expr     sql.Expression
		unit     string
		row      sql.Row
		expected TimeDelta
	}{
		{
			NewGetField(0, types.Int64, "foo", false),
			"DAY",
			sql.UntypedSqlRow{int64(2)},
			TimeDelta{Days: 2},
		},
		{
			NewLiteral(int64(2), types.Int64),
			"DAY",
			nil,
			TimeDelta{Days: 2},
		},
		{
			NewLiteral(int64(2), types.Int64),
			"MONTH",
			nil,
			TimeDelta{Months: 2},
		},
		{
			NewLiteral(int64(2), types.Int64),
			"YEAR",
			nil,
			TimeDelta{Years: 2},
		},
		{
			NewLiteral(int64(2), types.Int64),
			"QUARTER",
			nil,
			TimeDelta{Months: 6},
		},
		{
			NewLiteral(int64(2), types.Int64),
			"WEEK",
			nil,
			TimeDelta{Days: 14},
		},
		{
			NewLiteral(int64(2), types.Int64),
			"HOUR",
			nil,
			TimeDelta{Hours: 2},
		},
		{
			NewLiteral(int64(2), types.Int64),
			"MINUTE",
			nil,
			TimeDelta{Minutes: 2},
		},
		{
			NewLiteral(int64(2), types.Int64),
			"SECOND",
			nil,
			TimeDelta{Seconds: 2},
		},
		{
			NewLiteral(int64(2), types.Int64),
			"MICROSECOND",
			nil,
			TimeDelta{Microseconds: 2},
		},
		{
			NewLiteral("2 3", types.LongText),
			"DAY_HOUR",
			nil,
			TimeDelta{Days: 2, Hours: 3},
		},
		{
			NewLiteral("2 3:04:05.06", types.LongText),
			"DAY_MICROSECOND",
			nil,
			TimeDelta{Days: 2, Hours: 3, Minutes: 4, Seconds: 5, Microseconds: 6},
		},
		{
			NewLiteral("2 3:04:05", types.LongText),
			"DAY_SECOND",
			nil,
			TimeDelta{Days: 2, Hours: 3, Minutes: 4, Seconds: 5},
		},
		{
			NewLiteral("2 3:04", types.LongText),
			"DAY_MINUTE",
			nil,
			TimeDelta{Days: 2, Hours: 3, Minutes: 4},
		},
		{
			NewLiteral("3:04:05.06", types.LongText),
			"HOUR_MICROSECOND",
			nil,
			TimeDelta{Hours: 3, Minutes: 4, Seconds: 5, Microseconds: 6},
		},
		{
			NewLiteral("3:04:05", types.LongText),
			"HOUR_SECOND",
			nil,
			TimeDelta{Hours: 3, Minutes: 4, Seconds: 5},
		},
		{
			NewLiteral("3:04", types.LongText),
			"HOUR_MINUTE",
			nil,
			TimeDelta{Hours: 3, Minutes: 4},
		},
		{
			NewLiteral("04:05.06", types.LongText),
			"MINUTE_MICROSECOND",
			nil,
			TimeDelta{Minutes: 4, Seconds: 5, Microseconds: 6},
		},
		{
			NewLiteral("04:05", types.LongText),
			"MINUTE_SECOND",
			nil,
			TimeDelta{Minutes: 4, Seconds: 5},
		},
		{
			NewLiteral("04.05", types.LongText),
			"SECOND_MICROSECOND",
			nil,
			TimeDelta{Seconds: 4, Microseconds: 5},
		},
		{
			NewLiteral("1-5", types.LongText),
			"YEAR_MONTH",
			nil,
			TimeDelta{Years: 1, Months: 5},
		},
	}

	for _, tt := range testCases {
		interval := NewInterval(tt.expr, tt.unit)
		t.Run(interval.String(), func(t *testing.T) {
			require := require.New(t)
			result, err := interval.EvalDelta(sql.NewEmptyContext(), tt.row)
			require.NoError(err)
			require.Equal(tt.expected, *result)
		})
	}
}

func date(year int, month time.Month, day, hour, min, sec, micro int) time.Time {
	return time.Date(year, month, day, hour, min, sec, micro*int(time.Microsecond), time.Local)
}
