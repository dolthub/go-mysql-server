// Copyright 2021 Dolthub, Inc.
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
	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

func TestTimeDiff(t *testing.T) {
	toTimespan := func(str string) sql.Timespan {
		res, err := sql.Time.ConvertToTimespan(str)
		if err != nil {
			t.Fatal(err)
		}
		return res
	}

	ctx := sql.NewEmptyContext()
	testCases := []struct {
		name     string
		from     sql.Expression
		to       sql.Expression
		expected sql.Timespan
		err      bool
	}{
		{
			"invalid type text",
			expression.NewLiteral("hello there", sql.Text),
			expression.NewConvert(expression.NewLiteral("01:00:00", sql.Text), expression.ConvertToTime),
			toTimespan(""),
			true,
		},
		//TODO: handle Date properly
		/*{
			"invalid type date",
			expression.NewConvert(expression.NewLiteral("2020-01-03", sql.Text), expression.ConvertToDate),
			expression.NewConvert(expression.NewLiteral("2020-01-04", sql.Text), expression.ConvertToDate),
			"",
			true,
		},*/
		{
			"type mismatch 1",
			expression.NewLiteral(time.Date(2008, time.December, 29, 1, 1, 1, 2, time.Local), sql.Timestamp),
			expression.NewConvert(expression.NewLiteral("01:00:00", sql.Text), expression.ConvertToTime),
			toTimespan(""),
			true,
		},
		{
			"type mismatch 2",
			expression.NewLiteral("00:00:00.2", sql.Text),
			expression.NewLiteral("2020-10-10 10:10:10", sql.Text),
			toTimespan(""),
			true,
		},
		{
			"valid mismatch",
			expression.NewLiteral(time.Date(2008, time.December, 29, 1, 1, 1, 2, time.Local), sql.Timestamp),
			expression.NewLiteral(time.Date(2008, time.December, 30, 1, 1, 1, 2, time.Local), sql.Datetime),
			toTimespan("-24:00:00"),
			false,
		},
		{
			"timestamp types 1",
			expression.NewLiteral(time.Date(2018, time.May, 2, 0, 0, 0, 0, time.Local), sql.Timestamp),
			expression.NewLiteral(time.Date(2018, time.May, 2, 0, 0, 1, 0, time.Local), sql.Timestamp),
			toTimespan("-00:00:01"),
			false,
		},
		{
			"timestamp types 2",
			expression.NewLiteral(time.Date(2008, time.December, 31, 23, 59, 59, 1, time.Local), sql.Timestamp),
			expression.NewLiteral(time.Date(2008, time.December, 30, 1, 1, 1, 2, time.Local), sql.Timestamp),
			toTimespan("46:58:57.999999"),
			false,
		},
		{
			"time types 1",
			expression.NewConvert(expression.NewLiteral("00:00:00.1", sql.Text), expression.ConvertToTime),
			expression.NewConvert(expression.NewLiteral("00:00:00.2", sql.Text), expression.ConvertToTime),
			toTimespan("-00:00:00.100000"),
			false,
		},
		{
			"time types 2",
			expression.NewLiteral("00:00:00.2", sql.Text),
			expression.NewLiteral("00:00:00.4", sql.Text),
			toTimespan("-00:00:00.200000"),
			false,
		},
		{
			"datetime types",
			expression.NewLiteral(time.Date(2008, time.December, 29, 0, 0, 0, 0, time.Local), sql.Datetime),
			expression.NewLiteral(time.Date(2008, time.December, 30, 0, 0, 0, 0, time.Local), sql.Datetime),
			toTimespan("-24:00:00"),
			false,
		},
		{
			"datetime string types",
			expression.NewLiteral("2008-12-29 00:00:00", sql.Text),
			expression.NewLiteral("2008-12-30 00:00:00", sql.Text),
			toTimespan("-24:00:00"),
			false,
		},
		{
			"datetime string mix types",
			expression.NewLiteral(time.Date(2008, time.December, 29, 0, 0, 0, 0, time.UTC), sql.Datetime),
			expression.NewLiteral("2008-12-30 00:00:00", sql.Text),
			toTimespan("-24:00:00"),
			false,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			diff := NewTimeDiff(tt.from, tt.to)
			result, err := diff.Eval(ctx, nil)
			if tt.err {
				require.Error(err)
			} else {
				require.NoError(err)
				require.Equal(tt.expected, result)
			}
		})
	}
}

func TestDateDiff(t *testing.T) {
	dt, _ := time.Parse("2006-Jan-02", "2019-Dec-31")
	testCases := []struct {
		name     string
		e1Type   sql.Type
		e2Type   sql.Type
		row      sql.Row
		expected interface{}
		err      *errors.Kind
	}{
		{"time and text types, ", sql.Datetime, sql.Text, sql.NewRow(dt, "2019-12-28"), int64(3), nil},
		{"text types, diff day, less than 24 hours time diff", sql.Text, sql.Text, sql.NewRow("2007-12-31 23:58:59", "2007-12-30 23:59:59"), int64(1), nil},
		{"text types, same day, 23:59:59 time diff", sql.Text, sql.Text, sql.NewRow("2007-12-30 23:59:59", "2007-12-30 00:00:00"), int64(0), nil},
		{"text types, diff day, 1 min time diff", sql.Text, sql.Text, sql.NewRow("2007-12-31 00:00:59", "2007-12-30 23:59:59"), int64(1), nil},
		{"text types, negative result", sql.Text, sql.Text, sql.NewRow("2010-11-30 22:59:59", "2010-12-31 23:59:59"), int64(-31), nil},
		{"text types, positive result", sql.Text, sql.Text, sql.NewRow("2007-12-31 23:59:59", "2007-12-30"), int64(1), nil},
		{"text types, negative result", sql.Text, sql.Text, sql.NewRow("2010-11-30 23:59:59", "2010-12-31"), int64(-31), nil},
		{"text types, day difference result", sql.Text, sql.Text, sql.NewRow("2017-06-25", "2017-06-15"), int64(10), nil},
		{"text types, year difference result", sql.Text, sql.Text, sql.NewRow("2017-06-25", "2016-06-15"), int64(375), nil},
		{"text types, format with /", sql.Text, sql.Text, sql.NewRow("2007/12/22", "2007/12/20"), int64(2), nil},
		{"text types, positive result", sql.Text, sql.Text, sql.NewRow("2007-12-31", "2007-12-29 23:59:59"), int64(2), nil},
		{"text types, negative result", sql.Text, sql.Text, sql.NewRow("2010-11-02", "2010-11-30 23:59:59"), int64(-28), nil},
	}

	for _, tt := range testCases {
		args0 := expression.NewGetField(0, tt.e1Type, "", false)
		args1 := expression.NewGetField(1, tt.e2Type, "", false)
		f := NewDateDiff(args0, args1)

		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)

			result, err := f.Eval(sql.NewEmptyContext(), tt.row)
			if tt.err != nil {
				require.Error(err)
				require.True(tt.err.Is(err))
			} else {
				require.NoError(err)
				require.Equal(tt.expected, result)
			}
		})
	}
}

func TestTimestampDiff(t *testing.T) {
	testCases := []struct {
		name     string
		unit     sql.Type
		e1Type   sql.Type
		e2Type   sql.Type
		row      sql.Row
		expected interface{}
		err      bool
	}{
		{"invalid unit", sql.Text, sql.Text, sql.Text, sql.NewRow("MILLISECOND", "2007-12-30 23:59:59", "2007-12-31 00:00:00"), nil, true},
		{"microsecond", sql.Text, sql.Text, sql.Text, sql.NewRow("MICROSECOND", "2007-12-30 23:59:59", "2007-12-31 00:00:00"), int64(1000000), false},
		{"microsecond - small number", sql.Text, sql.Datetime, sql.Datetime, sql.NewRow("MICROSECOND",
			time.Date(2017, 11, 12, 16, 16, 25, 2*int(time.Microsecond), time.Local),
			time.Date(2017, 11, 12, 16, 16, 25, 333*int(time.Microsecond), time.Local)), int64(331), false},
		{"microsecond - negative", sql.Text, sql.Text, sql.Text, sql.NewRow("SQL_TSI_MICROSECOND", "2017-11-12 16:16:25.000022 +0000 UTC", "2017-11-12 16:16:25.000000 +0000 UTC"), int64(-22), false},
		{"second", sql.Text, sql.Text, sql.Text, sql.NewRow("SECOND", "2007-12-30 23:59:58", "2007-12-31 00:00:00"), int64(2), false},
		{"second", sql.Text, sql.Text, sql.Text, sql.NewRow("SQL_TSI_SECOND", "2017-11-12 16:16:25.000022 +0000 UTC", "2017-11-12 16:16:25.000000 +0000 UTC"), int64(0), false},
		{"minute - less than minute", sql.Text, sql.Text, sql.Text, sql.NewRow("MINUTE", "2007-12-30 23:59:59", "2007-12-31 00:00:00"), int64(0), false},
		{"minute - exactly one minute", sql.Text, sql.Text, sql.Text, sql.NewRow("SQL_TSI_MINUTE", "2007-12-30 23:59:00", "2007-12-31 00:00:00"), int64(1), false},
		{"hour - less", sql.Text, sql.Text, sql.Text, sql.NewRow("SQL_TSI_HOUR", "2007-12-30 22:29:00", "2007-12-31 00:00:00"), int64(1), false},
		{"hour", sql.Text, sql.Text, sql.Text, sql.NewRow("HOUR", "2007-12-29 22:29:00", "2007-12-31 00:00:00"), int64(25), false},
		{"hour - negative", sql.Text, sql.Text, sql.Text, sql.NewRow("HOUR", "2007-12-31 22:29:00", "2007-12-31 00:00:00"), int64(-22), false},
		{"day - less", sql.Text, sql.Text, sql.Text, sql.NewRow("DAY", "2007-12-30 22:29:00", "2007-12-31 00:00:00"), int64(0), false},
		{"day", sql.Text, sql.Text, sql.Text, sql.NewRow("SQL_TSI_DAY", "2007-12-01 22:29:00", "2007-12-31 00:00:00"), int64(29), false},
		{"day - negative", sql.Text, sql.Text, sql.Text, sql.NewRow("DAY", "2007-12-31 22:29:00", "2007-12-30 00:00:00"), int64(-1), false},
		{"week - less", sql.Text, sql.Text, sql.Text, sql.NewRow("WEEK", "2007-12-31 00:00:00", "2007-12-24 00:00:01"), int64(0), false},
		{"week", sql.Text, sql.Text, sql.Text, sql.NewRow("WEEK", "2007-10-30 00:00:00", "2007-12-24 00:00:01"), int64(7), false},
		{"week - negative", sql.Text, sql.Text, sql.Text, sql.NewRow("SQL_TSI_WEEK", "2007-12-31 00:00:00", "2007-12-24 00:00:00"), int64(-1), false},
		{"month - second less than a month", sql.Text, sql.Text, sql.Text, sql.NewRow("SQL_TSI_MONTH", "2007-11-30 00:00:00", "2007-12-29 23:59:59"), int64(0), false},
		{"month", sql.Text, sql.Text, sql.Text, sql.NewRow("MONTH", "2007-01-31 00:00:00", "2007-12-30 00:00:00"), int64(10), false},
		{"month - negative", sql.Text, sql.Text, sql.Text, sql.NewRow("MONTH", "2008-01-31 00:00:01", "2007-12-30 00:00:00"), int64(-1), false},
		{"quarter - exactly a quarter", sql.Text, sql.Text, sql.Text, sql.NewRow("QUARTER", "2007-08-30 00:00:00", "2007-11-30 00:00:00"), int64(1), false},
		{"quarter - second less than a quarter", sql.Text, sql.Text, sql.Text, sql.NewRow("SQL_TSI_QUARTER", "2007-08-30 00:00:01", "2007-11-30 00:00:00"), int64(0), false},
		{"quarter", sql.Text, sql.Text, sql.Text, sql.NewRow("QUARTER", "2006-08-30 00:00:00", "2007-11-30 00:00:00"), int64(5), false},
		{"quarter - negative", sql.Text, sql.Text, sql.Text, sql.NewRow("QUARTER", "2006-08-30 00:00:00", "2002-11-30 00:00:00"), int64(-15), false},
		{"year - second less than a month", sql.Text, sql.Text, sql.Text, sql.NewRow("YEAR", "2019-01-01 00:00:00", "2019-12-31 23:59:59"), int64(0), false},
		{"year", sql.Text, sql.Text, sql.Text, sql.NewRow("YEAR", "2016-09-04 00:00:01", "2021-09-04 00:00:00"), int64(4), false},
		{"year - ", sql.Text, sql.Text, sql.Text, sql.NewRow("YEAR", "2016-09-04 01:00:01", "2021-09-04 02:00:02"), int64(5), false},
		{"year - negative", sql.Text, sql.Text, sql.Text, sql.NewRow("SQL_TSI_YEAR", "2016-09-05 00:00:00", "2006-09-04 23:59:59"), int64(-10), false},
	}

	for _, tt := range testCases {
		args0 := expression.NewGetField(0, tt.unit, "", false)
		args1 := expression.NewGetField(1, tt.e1Type, "", false)
		args2 := expression.NewGetField(2, tt.e2Type, "", false)
		f := NewTimestampDiff(args0, args1, args2)

		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)

			result, err := f.Eval(sql.NewEmptyContext(), tt.row)
			if tt.err {
				require.Error(err)
			} else {
				require.NoError(err)
				require.Equal(tt.expected, result)
			}
		})
	}
}
