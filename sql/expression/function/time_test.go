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

package function

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

const (
	tsDate     = 1258882545 // Sunday, November 22, 2009 10:35:45 PM GMT+01:00
	stringDate = "2007-01-02 14:15:16"
)

//TODO: look over all of the "invalid type" tests later, ignoring them for now since they're unlikely to be hit
func TestTime_Year(t *testing.T) {
	ctx := sql.NewEmptyContext()
	f := NewYear(ctx, expression.NewGetField(0, sql.LongText, "foo", false))

	testCases := []struct {
		name     string
		row      sql.Row
		expected interface{}
		err      bool
	}{
		{"invalid type", sql.NewRow([]byte{0, 1, 2}), int32(0), false},
		{"date as string", sql.NewRow(stringDate), int32(2007), false},
		{"date as time", sql.NewRow(time.Now()), int32(time.Now().UTC().Year()), false},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			val, err := f.Eval(ctx, tt.row)
			if tt.err {
				require.Error(err)
			} else {
				require.NoError(err)
				require.Equal(tt.expected, val)
			}
		})
	}
}

func TestTime_Month(t *testing.T) {
	ctx := sql.NewEmptyContext()
	f := NewMonth(ctx, expression.NewGetField(0, sql.LongText, "foo", false))

	testCases := []struct {
		name     string
		row      sql.Row
		expected interface{}
		err      bool
	}{
		{"null date", sql.NewRow(nil), nil, false},
		{"invalid type", sql.NewRow([]byte{0, 1, 2}), int32(1), false},
		{"date as string", sql.NewRow(stringDate), int32(1), false},
		{"date as time", sql.NewRow(time.Now()), int32(time.Now().UTC().Month()), false},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			val, err := f.Eval(ctx, tt.row)
			if tt.err {
				require.Error(err)
			} else {
				require.NoError(err)
				require.Equal(tt.expected, val)
			}
		})
	}
}

func TestTime_Day(t *testing.T) {
	ctx := sql.NewEmptyContext()
	f := NewDay(ctx, expression.NewGetField(0, sql.LongText, "foo", false))

	testCases := []struct {
		name     string
		row      sql.Row
		expected interface{}
		err      bool
	}{
		{"null date", sql.NewRow(nil), nil, false},
		{"invalid type", sql.NewRow([]byte{0, 1, 2}), int32(1), false},
		{"date as string", sql.NewRow(stringDate), int32(2), false},
		{"date as time", sql.NewRow(time.Now()), int32(time.Now().UTC().Day()), false},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			val, err := f.Eval(ctx, tt.row)
			if tt.err {
				require.Error(err)
			} else {
				require.NoError(err)
				require.Equal(tt.expected, val)
			}
		})
	}
}

func TestTime_Weekday(t *testing.T) {
	ctx := sql.NewEmptyContext()
	f := NewWeekday(ctx, expression.NewGetField(0, sql.LongText, "foo", false))

	testCases := []struct {
		name     string
		row      sql.Row
		expected interface{}
		err      bool
	}{
		{"null date", sql.NewRow(nil), nil, false},
		{"invalid type", sql.NewRow([]byte{0, 1, 2}), int32(5), false},
		{"date as string", sql.NewRow(stringDate), int32(1), false},
		{"date as time", sql.NewRow(time.Now()), int32(time.Now().UTC().Weekday()+6) % 7, false},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			val, err := f.Eval(ctx, tt.row)
			if tt.err {
				require.Error(err)
			} else {
				require.NoError(err)
				require.Equal(tt.expected, val)
			}
		})
	}
}

func TestTime_Hour(t *testing.T) {
	ctx := sql.NewEmptyContext()
	f := NewHour(ctx, expression.NewGetField(0, sql.LongText, "foo", false))

	testCases := []struct {
		name     string
		row      sql.Row
		expected interface{}
		err      bool
	}{
		{"null date", sql.NewRow(nil), nil, false},
		{"invalid type", sql.NewRow([]byte{0, 1, 2}), int32(0), false},
		{"date as string", sql.NewRow(stringDate), int32(14), false},
		{"date as time", sql.NewRow(time.Now()), int32(time.Now().UTC().Hour()), false},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			val, err := f.Eval(ctx, tt.row)
			if tt.err {
				require.Error(err)
			} else {
				require.NoError(err)
				require.Equal(tt.expected, val)
			}
		})
	}
}

func TestTime_Minute(t *testing.T) {
	ctx := sql.NewEmptyContext()
	f := NewMinute(ctx, expression.NewGetField(0, sql.LongText, "foo", false))

	testCases := []struct {
		name     string
		row      sql.Row
		expected interface{}
		err      bool
	}{
		{"null date", sql.NewRow(nil), nil, false},
		{"invalid type", sql.NewRow([]byte{0, 1, 2}), int32(0), false},
		{"date as string", sql.NewRow(stringDate), int32(15), false},
		{"date as time", sql.NewRow(time.Now()), int32(time.Now().UTC().Minute()), false},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			val, err := f.Eval(ctx, tt.row)
			if tt.err {
				require.Error(err)
			} else {
				require.NoError(err)
				require.Equal(tt.expected, val)
			}
		})
	}
}

func TestTime_Second(t *testing.T) {
	ctx := sql.NewEmptyContext()
	f := NewSecond(ctx, expression.NewGetField(0, sql.LongText, "foo", false))

	testCases := []struct {
		name     string
		row      sql.Row
		expected interface{}
		err      bool
	}{
		{"null date", sql.NewRow(nil), nil, false},
		{"invalid type", sql.NewRow([]byte{0, 1, 2}), int32(0), false},
		{"date as string", sql.NewRow(stringDate), int32(16), false},
		{"date as time", sql.NewRow(time.Now()), int32(time.Now().UTC().Second()), false},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			val, err := f.Eval(ctx, tt.row)
			if tt.err {
				require.Error(err)
			} else {
				require.NoError(err)
				require.Equal(tt.expected, val)
			}
		})
	}
}

func TestTime_DayOfWeek(t *testing.T) {
	ctx := sql.NewEmptyContext()
	f := NewDayOfWeek(ctx, expression.NewGetField(0, sql.LongText, "foo", false))

	testCases := []struct {
		name     string
		row      sql.Row
		expected interface{}
		err      bool
	}{
		{"null date", sql.NewRow(nil), nil, false},
		{"invalid type", sql.NewRow([]byte{0, 1, 2}), int32(7), false},
		{"date as string", sql.NewRow(stringDate), int32(3), false},
		{"date as time", sql.NewRow(time.Now()), int32(time.Now().UTC().Weekday() + 1), false},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			val, err := f.Eval(ctx, tt.row)
			if tt.err {
				require.Error(err)
			} else {
				require.NoError(err)
				require.Equal(tt.expected, val)
			}
		})
	}
}

func TestTime_DayOfYear(t *testing.T) {
	ctx := sql.NewEmptyContext()
	f := NewDayOfYear(ctx, expression.NewGetField(0, sql.LongText, "foo", false))

	testCases := []struct {
		name     string
		row      sql.Row
		expected interface{}
		err      bool
	}{
		{"null date", sql.NewRow(nil), nil, false},
		{"invalid type", sql.NewRow([]byte{0, 1, 2}), int32(1), false},
		{"date as string", sql.NewRow(stringDate), int32(2), false},
		{"date as time", sql.NewRow(time.Now()), int32(time.Now().UTC().YearDay()), false},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			val, err := f.Eval(ctx, tt.row)
			if tt.err {
				require.Error(err)
			} else {
				require.NoError(err)
				require.Equal(tt.expected, val)
			}
		})
	}
}

func TestYearWeek(t *testing.T) {
	ctx := sql.NewEmptyContext()
	f, err := NewYearWeek(ctx, expression.NewGetField(0, sql.LongText, "foo", false))
	require.NoError(t, err)

	testCases := []struct {
		name     string
		row      sql.Row
		expected interface{}
		err      bool
	}{
		{"null date", sql.NewRow(nil), nil, true},
		{"invalid type", sql.NewRow([]byte{0, 1, 2}), int32(1), false},
		{"date as string", sql.NewRow(stringDate), int32(200653), false},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			val, err := f.Eval(ctx, tt.row)
			if tt.err {
				require.Error(err)
			} else {
				require.NoError(err)
				require.Equal(tt.expected, val)
			}
		})
	}
}

func TestCalcDaynr(t *testing.T) {
	require.EqualValues(t, calcDaynr(0, 0, 0), 0)
	require.EqualValues(t, calcDaynr(9999, 12, 31), 3652424)
	require.EqualValues(t, calcDaynr(1970, 1, 1), 719528)
	require.EqualValues(t, calcDaynr(2006, 12, 16), 733026)
	require.EqualValues(t, calcDaynr(10, 1, 2), 3654)
	require.EqualValues(t, calcDaynr(2008, 2, 20), 733457)
}

func TestCalcWeek(t *testing.T) {
	_, w := calcWeek(2008, 2, 20, weekMode(0))

	_, w = calcWeek(2008, 2, 20, weekMode(1))
	require.EqualValues(t, w, 8)

	_, w = calcWeek(2008, 12, 31, weekMode(1))
	require.EqualValues(t, w, 53)
}

func TestNow(t *testing.T) {
	date := time.Date(2018, time.December, 2, 16, 25, 0, 0, time.Local)
	testNowFunc := func() time.Time {
		return date
	}

	var ctx *sql.Context
	err := sql.RunWithNowFunc(testNowFunc, func() error {
		ctx = sql.NewEmptyContext()
		return nil
	})
	require.NoError(t, err)

	tests := []struct {
		args      []sql.Expression
		result    time.Time
		expectErr bool
	}{
		{
			args:      nil,
			result:    date,
			expectErr: false,
		},
		{
			args:      []sql.Expression{expression.NewLiteral(0, sql.Int8)},
			result:    date,
			expectErr: false,
		},
		{
			args:      []sql.Expression{expression.NewLiteral(0, sql.Int64)},
			result:    date,
			expectErr: false,
		},
		{
			args:      []sql.Expression{expression.NewLiteral(6, sql.Uint8)},
			result:    date,
			expectErr: false,
		},
		{
			args:      []sql.Expression{expression.NewLiteral(7, sql.Int8)},
			result:    time.Time{},
			expectErr: true,
		},
		{
			args:      []sql.Expression{expression.NewLiteral(-1, sql.Int8)},
			result:    time.Time{},
			expectErr: true,
		},
		{
			args:      []sql.Expression{expression.NewConvert(expression.NewLiteral("2020-10-10 01:02:03", sql.Text), expression.ConvertToDatetime)},
			result:    time.Time{},
			expectErr: true,
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprint(test.args), func(t *testing.T) {
			ut, err := NewNow(ctx, test.args...)
			if !test.expectErr {
				require.NoError(t, err)
				val, err := ut.Eval(ctx, nil)
				require.NoError(t, err)
				assert.Equal(t, test.result, val)
			} else {
				assert.Error(t, err)
			}
		})
	}
}

func TestUTCTimestamp(t *testing.T) {
	date := time.Date(2018, time.December, 2, 16, 25, 0, 0, time.Local)
	testNowFunc := func() time.Time {
		return date
	}

	var ctx *sql.Context
	err := sql.RunWithNowFunc(testNowFunc, func() error {
		ctx = sql.NewEmptyContext()
		return nil
	})
	require.NoError(t, err)

	tests := []struct {
		args      []sql.Expression
		result    time.Time
		expectErr bool
	}{
		{
			args:      nil,
			result:    date,
			expectErr: false,
		},
		{
			args:      []sql.Expression{expression.NewLiteral(0, sql.Int8)},
			result:    date,
			expectErr: false,
		},
		{
			args:      []sql.Expression{expression.NewLiteral(0, sql.Int64)},
			result:    date,
			expectErr: false,
		},
		{
			args:      []sql.Expression{expression.NewLiteral(6, sql.Uint8)},
			result:    date,
			expectErr: false,
		},
		{
			args:      []sql.Expression{expression.NewLiteral(7, sql.Int8)},
			result:    time.Time{},
			expectErr: true,
		},
		{
			args:      []sql.Expression{expression.NewLiteral(-1, sql.Int8)},
			result:    time.Time{},
			expectErr: true,
		},
		{
			args:      []sql.Expression{expression.NewConvert(expression.NewLiteral("2020-10-10 01:02:03", sql.Text), expression.ConvertToDatetime)},
			result:    time.Time{},
			expectErr: true,
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprint(test.args), func(t *testing.T) {
			ut, err := NewUTCTimestamp(ctx, test.args...)
			if !test.expectErr {
				require.NoError(t, err)
				val, err := ut.Eval(ctx, nil)
				require.NoError(t, err)
				assert.Equal(t, test.result.UTC(), val)
			} else {
				assert.Error(t, err)
			}
		})
	}
}

func TestDate(t *testing.T) {
	ctx := sql.NewEmptyContext()
	f := NewDate(ctx, expression.NewGetField(0, sql.LongText, "foo", false))

	testCases := []struct {
		name     string
		row      sql.Row
		expected interface{}
		err      bool
	}{
		{"null date", sql.NewRow(nil), nil, false},
		{"invalid type", sql.NewRow([]byte{0, 1, 2}), sql.Date.Zero().(time.Time).Format("2006-01-02"), false},
		{"date as string", sql.NewRow(stringDate), "2007-01-02", false},
		{"date as time", sql.NewRow(time.Now().UTC()), time.Now().UTC().Format("2006-01-02"), false},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			val, err := f.Eval(ctx, tt.row)
			if tt.err {
				require.Error(err)
			} else {
				require.NoError(err)
				require.Equal(tt.expected, val)
			}
		})
	}
}

func TestTimeDiff(t *testing.T) {
	ctx := sql.NewEmptyContext()
	testCases := []struct {
		name     string
		from     sql.Expression
		to       sql.Expression
		expected string
		err      bool
	}{
		{
			"invalid type text",
			expression.NewLiteral("hello there", sql.Text),
			expression.NewConvert(expression.NewLiteral("01:00:00", sql.Text), expression.ConvertToTime),
			"",
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
			"",
			true,
		},
		{
			"type mismatch 2",
			expression.NewLiteral("00:00:00.2", sql.Text),
			expression.NewLiteral("2020-10-10 10:10:10", sql.Text),
			"",
			true,
		},
		{
			"valid mismatch",
			expression.NewLiteral(time.Date(2008, time.December, 29, 1, 1, 1, 2, time.Local), sql.Timestamp),
			expression.NewLiteral(time.Date(2008, time.December, 30, 1, 1, 1, 2, time.Local), sql.Datetime),
			"-24:00:00",
			false,
		},
		{
			"timestamp types 1",
			expression.NewLiteral(time.Date(2018, time.May, 2, 0, 0, 0, 0, time.Local), sql.Timestamp),
			expression.NewLiteral(time.Date(2018, time.May, 2, 0, 0, 1, 0, time.Local), sql.Timestamp),
			"-00:00:01",
			false,
		},
		{
			"timestamp types 2",
			expression.NewLiteral(time.Date(2008, time.December, 31, 23, 59, 59, 1, time.Local), sql.Timestamp),
			expression.NewLiteral(time.Date(2008, time.December, 30, 1, 1, 1, 2, time.Local), sql.Timestamp),
			"46:58:57.999999",
			false,
		},
		{
			"time types 1",
			expression.NewConvert(expression.NewLiteral("00:00:00.1", sql.Text), expression.ConvertToTime),
			expression.NewConvert(expression.NewLiteral("00:00:00.2", sql.Text), expression.ConvertToTime),
			"-00:00:00.100000",
			false,
		},
		{
			"time types 2",
			expression.NewLiteral("00:00:00.2", sql.Text),
			expression.NewLiteral("00:00:00.4", sql.Text),
			"-00:00:00.200000",
			false,
		},
		{
			"datetime types",
			expression.NewLiteral(time.Date(2008, time.December, 29, 0, 0, 0, 0, time.Local), sql.Datetime),
			expression.NewLiteral(time.Date(2008, time.December, 30, 0, 0, 0, 0, time.Local), sql.Datetime),
			"-24:00:00",
			false,
		},
		{
			"datetime string types",
			expression.NewLiteral("2008-12-29 00:00:00", sql.Text),
			expression.NewLiteral("2008-12-30 00:00:00", sql.Text),
			"-24:00:00",
			false,
		},
		{
			"datetime string mix types",
			expression.NewLiteral(time.Date(2008, time.December, 29, 0, 0, 0, 0, time.UTC), sql.Datetime),
			expression.NewLiteral("2008-12-30 00:00:00", sql.Text),
			"-24:00:00",
			false,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			diff := NewTimeDiff(ctx, tt.from, tt.to)
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
