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
	f := NewYear(expression.NewGetField(0, sql.LongText, "foo", false))

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
	f := NewMonth(expression.NewGetField(0, sql.LongText, "foo", false))

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
	f := NewDay(expression.NewGetField(0, sql.LongText, "foo", false))

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
	f := NewWeekday(expression.NewGetField(0, sql.LongText, "foo", false))

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
	f := NewHour(expression.NewGetField(0, sql.LongText, "foo", false))

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
	f := NewMinute(expression.NewGetField(0, sql.LongText, "foo", false))

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
	f := NewSecond(expression.NewGetField(0, sql.LongText, "foo", false))

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
	f := NewDayOfWeek(expression.NewGetField(0, sql.LongText, "foo", false))

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
	f := NewDayOfYear(expression.NewGetField(0, sql.LongText, "foo", false))

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
	f, err := NewYearWeek(expression.NewGetField(0, sql.LongText, "foo", false))
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
			ut, err := NewNow(test.args...)
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
			ut, err := NewUTCTimestamp(test.args...)
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
	f := NewDate(expression.NewGetField(0, sql.LongText, "foo", false))

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

func TestCurrentTimestamp(t *testing.T) {
	f, _ := NewCurrTimestamp(expression.NewGetField(0, sql.LongText, "foo", false))
	date := time.Date(
		2021,     // year
		1,        // month
		1,        // day
		8,        // hour
		30,       // min
		15,       // sec
		12345678, // nsec
		time.UTC, // location (UTC)
	)

	testCases := []struct {
		name     string
		row      sql.Row
		expected interface{}
		err      bool
	}{
		{"null date", sql.NewRow(nil), nil, true},
		{"different int type", sql.NewRow(int8(0)), time.Date(2021, 1, 1, 8, 30, 15, 0, time.UTC), false},
		{"precision of -1", sql.NewRow(-1), nil, true},
		{"precision of 0", sql.NewRow(0), time.Date(2021, 1, 1, 8, 30, 15, 0, time.UTC), false},
		{"precision of 1 trailing 0s are trimmed", sql.NewRow(1), time.Date(2021, 1, 1, 8, 30, 15, 0, time.UTC), false},
		{"precision of 2", sql.NewRow(2), time.Date(2021, 1, 1, 8, 30, 15, 10000000, time.UTC), false},
		{"precision of 3", sql.NewRow(3), time.Date(2021, 1, 1, 8, 30, 15, 12000000, time.UTC), false},
		{"precision of 4", sql.NewRow(4), time.Date(2021, 1, 1, 8, 30, 15, 12300000, time.UTC), false},
		{"precision of 5", sql.NewRow(5), time.Date(2021, 1, 1, 8, 30, 15, 12340000, time.UTC), false},
		{"precision of 6", sql.NewRow(6), time.Date(2021, 1, 1, 8, 30, 15, 12345000, time.UTC), false},
		{"precision of 7 which is too high", sql.NewRow(7), nil, true},
		{"incorrect type", sql.NewRow("notanint"), nil, true},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			sql.RunWithNowFunc(func() time.Time {
				return date
			}, func() error {
				ctx := sql.NewEmptyContext()
				require := require.New(t)
				val, err := f.Eval(ctx, tt.row)
				if tt.err {
					require.Error(err)
				} else {
					require.NoError(err)
					require.Equal(tt.expected, val)
				}
				return nil
			})
		})
	}
}
