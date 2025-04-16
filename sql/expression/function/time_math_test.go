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
	"github.com/dolthub/go-mysql-server/sql/types"
)

// MySQL's ADDDATE function is just syntactic sugar on top of DATE_ADD. The first param is the date, and the
// second is the value to add. If the second param is an interval type, it gets passed to DATE_ADD as-is. If
// it is not an explicit interval, the interval period is assumed to be "DAY".
func TestAddDate(t *testing.T) {
	require := require.New(t)
	ctx := sql.NewEmptyContext()

	// Not enough params
	_, err := NewAddDate()
	require.Error(err)

	// Not enough params
	_, err = NewAddDate(expression.NewLiteral("2018-05-02", types.LongText))
	require.Error(err)

	var expected, result interface{}
	var f sql.Expression

	f, err = NewAddDate(
		expression.NewLiteral(time.Date(2018, 5, 2, 12, 34, 56, 123456000, time.UTC), types.Date),
		expression.NewLiteral(int64(1), types.Int64))
	require.NoError(err)
	expected = time.Date(2018, 5, 3, 0, 0, 0, 0, time.UTC)
	result, err = f.Eval(ctx, sql.Row{})
	require.NoError(err)
	require.Equal(expected, result)

	f, err = NewAddDate(
		expression.NewLiteral(time.Date(2018, 5, 2, 12, 34, 56, 0, time.UTC), types.Datetime),
		expression.NewLiteral(int64(1), types.Int64))
	require.NoError(err)
	expected = time.Date(2018, 5, 3, 12, 34, 56, 0, time.UTC)
	result, err = f.Eval(ctx, sql.Row{})
	require.NoError(err)
	require.Equal(expected, result)

	f, err = NewAddDate(
		expression.NewLiteral(time.Date(2018, 5, 2, 12, 34, 56, 123456000, time.UTC), types.DatetimeMaxPrecision),
		expression.NewLiteral(int64(1), types.Int64))
	require.NoError(err)
	expected = time.Date(2018, 5, 3, 12, 34, 56, 123456000, time.UTC)
	result, err = f.Eval(ctx, sql.Row{})
	require.NoError(err)
	require.Equal(expected, result)

	// If the second argument is NOT an interval, then ADDDATE works exactly like DATE_ADD
	f, err = NewAddDate(
		expression.NewLiteral("2018-05-02", types.LongText),
		expression.NewLiteral(int64(1), types.Int64))
	require.NoError(err)
	expected = "2018-05-03"
	result, err = f.Eval(ctx, sql.Row{})
	require.NoError(err)
	require.Equal(expected, result)

	// If the second argument is an interval, then ADDDATE works exactly like DATE_ADD
	f, err = NewAddDate(
		expression.NewGetField(0, types.Text, "foo", false),
		expression.NewInterval(expression.NewLiteral(int64(1), types.Int64), "DAY"))
	require.NoError(err)
	result, err = f.Eval(ctx, sql.Row{"2018-05-02"})
	require.NoError(err)
	require.Equal(expected, result)

	f, err = NewAddDate(
		expression.NewLiteral("2018-05-02", types.LongText),
		expression.NewInterval(expression.NewLiteral(int64(1), types.Int64), "DAY"))
	require.NoError(err)
	expected = "2018-05-03"
	result, err = f.Eval(ctx, sql.Row{})
	require.NoError(err)
	require.Equal(expected, result)

	f, err = NewAddDate(
		expression.NewLiteral("2018-05-02 12:34:56", types.LongText),
		expression.NewInterval(expression.NewLiteral(int64(1), types.Int64), "DAY"))
	require.NoError(err)
	expected = "2018-05-03 12:34:56"
	result, err = f.Eval(ctx, sql.Row{})
	require.NoError(err)
	require.Equal(expected, result)

	f, err = NewAddDate(
		expression.NewLiteral("2018-05-02 12:34:56.123", types.LongText),
		expression.NewInterval(expression.NewLiteral(int64(1), types.Int64), "DAY"))
	require.NoError(err)
	expected = "2018-05-03 12:34:56.123000"
	result, err = f.Eval(ctx, sql.Row{})
	require.NoError(err)
	require.Equal(expected, result)

	f, err = NewAddDate(
		expression.NewLiteral("2018-05-02 12:34:56.123456", types.LongText),
		expression.NewInterval(expression.NewLiteral(int64(1), types.Int64), "DAY"))
	require.NoError(err)
	expected = "2018-05-03 12:34:56.123456"
	result, err = f.Eval(ctx, sql.Row{})
	require.NoError(err)
	require.Equal(expected, result)

	f, err = NewAddDate(
		expression.NewLiteral("2018-05-02", types.LongText),
		expression.NewInterval(expression.NewLiteral(int64(1), types.Int64), "SECOND"))
	require.NoError(err)
	expected = "2018-05-02 00:00:01"
	result, err = f.Eval(ctx, sql.Row{})
	require.NoError(err)
	require.Equal(expected, result)

	f, err = NewAddDate(
		expression.NewLiteral("2018-05-02", types.LongText),
		expression.NewInterval(expression.NewLiteral(int64(10), types.Int64), "MICROSECOND"))
	require.NoError(err)
	expected = "2018-05-02 00:00:00.000010"
	result, err = f.Eval(ctx, sql.Row{})
	require.NoError(err)
	require.Equal(expected, result)

	f, err = NewAddDate(
		expression.NewLiteral("2018-05-02", types.LongText),
		expression.NewInterval(expression.NewLiteral(int64(1), types.Int64), "MICROSECOND"))
	require.NoError(err)
	expected = "2018-05-02 00:00:00.000001"
	result, err = f.Eval(ctx, sql.Row{})
	require.NoError(err)
	require.Equal(expected, result)

	// If the interval param is NULL, then NULL is returned
	f2, err := NewAddDate(
		expression.NewLiteral("2018-05-02", types.LongText),
		expression.NewGetField(0, types.Int64, "foo", true))
	result, err = f2.Eval(ctx, sql.Row{nil})
	require.NoError(err)
	require.Nil(result)

	f, err = NewAddDate(
		expression.NewGetField(0, types.Int64, "foo", true),
		expression.NewLiteral(int64(1), types.Int64))

	// If the date param is NULL, then NULL is returned
	require.NoError(err)
	result, err = f.Eval(ctx, sql.Row{nil})
	require.NoError(err)
	require.Nil(result)

	// If a time is passed (and no date) then NULL is returned
	result, err = f.Eval(ctx, sql.Row{"12:00:56"})
	require.NoError(err)
	require.Nil(result)

	// If an invalid date is passed, then NULL is returned
	result, err = f.Eval(ctx, sql.Row{"asdasdasd"})
	require.NoError(err)
	require.Nil(result)

	// If the second argument is NOT an interval, then it's assumed to be a day interval
	t.Skip("Interval does not handle overflows correctly")
	f, err = NewAddDate(
		expression.NewLiteral("2018-05-02", types.Text),
		expression.NewLiteral(int64(1_000_000), types.Int64))
	require.NoError(err)
	expected = "4756-03-29"
	result, err = f.Eval(ctx, sql.Row{})
	require.NoError(err)
	require.Equal(expected, result)
}

func TestDateAdd(t *testing.T) {
	require := require.New(t)
	ctx := sql.NewEmptyContext()

	_, err := NewDateAdd()
	require.Error(err)

	_, err = NewDateAdd(expression.NewLiteral("2018-05-02", types.LongText))
	require.Error(err)

	_, err = NewDateAdd(expression.NewLiteral("2018-05-02", types.LongText),
		expression.NewLiteral(int64(1), types.Int64),
	)
	require.Error(err)

	f, err := NewDateAdd(expression.NewGetField(0, types.Text, "foo", false),
		expression.NewInterval(
			expression.NewLiteral(int64(1), types.Int64),
			"DAY",
		),
	)
	require.NoError(err)

	expected := "2018-05-03"
	result, err := f.Eval(ctx, sql.Row{"2018-05-02"})
	require.NoError(err)
	require.Equal(expected, result)

	result, err = f.Eval(ctx, sql.Row{"12:34:56"})
	require.NoError(err)
	require.Nil(result)

	result, err = f.Eval(ctx, sql.Row{nil})
	require.NoError(err)
	require.Nil(result)

	result, err = f.Eval(ctx, sql.Row{"asdasdasd"})
	require.NoError(err)
	require.Nil(result)
}

// MySQL's SUBDATE function is just syntactic sugar on top of DATE_SUB. The first param is the date, and the
// second is the value to subtract. If the second param is an interval type, it gets passed to DATE_SUB as-is. If
// it is not an explicit interval, the interval period is assumed to be "DAY".
func TestSubDate(t *testing.T) {
	require := require.New(t)
	ctx := sql.NewEmptyContext()

	// Not enough params
	_, err := NewSubDate()
	require.Error(err)

	// Not enough params
	_, err = NewSubDate(expression.NewLiteral("2018-05-02", types.LongText))
	require.Error(err)

	var expected, result interface{}
	var f sql.Expression

	f, err = NewSubDate(
		expression.NewLiteral(time.Date(2018, 5, 2, 12, 34, 56, 123456000, time.UTC), types.Date),
		expression.NewLiteral(int64(1), types.Int64))
	require.NoError(err)
	expected = time.Date(2018, 5, 1, 0, 0, 0, 0, time.UTC)
	result, err = f.Eval(ctx, sql.Row{})
	require.NoError(err)
	require.Equal(expected, result)

	f, err = NewSubDate(
		expression.NewLiteral(time.Date(2018, 5, 2, 12, 34, 56, 0, time.UTC), types.Datetime),
		expression.NewLiteral(int64(1), types.Int64))
	require.NoError(err)
	expected = time.Date(2018, 5, 1, 12, 34, 56, 0, time.UTC)
	result, err = f.Eval(ctx, sql.Row{})
	require.NoError(err)
	require.Equal(expected, result)

	f, err = NewSubDate(
		expression.NewLiteral(time.Date(2018, 5, 2, 12, 34, 56, 123456000, time.UTC), types.DatetimeMaxPrecision),
		expression.NewLiteral(int64(1), types.Int64))
	require.NoError(err)
	expected = time.Date(2018, 5, 1, 12, 34, 56, 123456000, time.UTC)
	result, err = f.Eval(ctx, sql.Row{})
	require.NoError(err)
	require.Equal(expected, result)

	// If the second argument is NOT an interval, then it's assumed to be a day interval
	f, err = NewSubDate(
		expression.NewLiteral("2018-05-02", types.LongText),
		expression.NewLiteral(int64(1), types.Int64))
	require.NoError(err)
	expected = "2018-05-01"
	result, err = f.Eval(ctx, sql.Row{})
	require.NoError(err)
	require.Equal(expected, result)

	// If the second argument is an interval, then SUBDATE works exactly like DATE_SUB
	f, err = NewSubDate(
		expression.NewGetField(0, types.Text, "foo", false),
		expression.NewInterval(expression.NewLiteral(int64(1), types.Int64), "DAY"))
	require.NoError(err)
	result, err = f.Eval(ctx, sql.Row{"2018-05-02"})
	require.NoError(err)
	require.Equal(expected, result)

	f, err = NewSubDate(
		expression.NewLiteral("2018-05-02", types.LongText),
		expression.NewInterval(expression.NewLiteral(int64(1), types.Int64), "DAY"))
	require.NoError(err)
	expected = "2018-05-01"
	result, err = f.Eval(ctx, sql.Row{})
	require.NoError(err)
	require.Equal(expected, result)

	f, err = NewSubDate(
		expression.NewLiteral("2018-05-02 12:34:56", types.LongText),
		expression.NewInterval(expression.NewLiteral(int64(1), types.Int64), "DAY"))
	require.NoError(err)
	expected = "2018-05-01 12:34:56"
	result, err = f.Eval(ctx, sql.Row{})
	require.NoError(err)
	require.Equal(expected, result)

	f, err = NewSubDate(
		expression.NewLiteral("2018-05-02 12:34:56.123", types.LongText),
		expression.NewInterval(expression.NewLiteral(int64(1), types.Int64), "DAY"))
	require.NoError(err)
	expected = "2018-05-01 12:34:56.123000"
	result, err = f.Eval(ctx, sql.Row{})
	require.NoError(err)
	require.Equal(expected, result)

	f, err = NewSubDate(
		expression.NewLiteral("2018-05-02 12:34:56.123456", types.LongText),
		expression.NewInterval(expression.NewLiteral(int64(1), types.Int64), "DAY"))
	require.NoError(err)
	expected = "2018-05-01 12:34:56.123456"
	result, err = f.Eval(ctx, sql.Row{})
	require.NoError(err)
	require.Equal(expected, result)

	f, err = NewSubDate(
		expression.NewLiteral("2018-05-02", types.LongText),
		expression.NewInterval(expression.NewLiteral(int64(1), types.Int64), "SECOND"))
	require.NoError(err)
	expected = "2018-05-01 23:59:59"
	result, err = f.Eval(ctx, sql.Row{})
	require.NoError(err)
	require.Equal(expected, result)

	f, err = NewSubDate(
		expression.NewLiteral("2018-05-02", types.LongText),
		expression.NewInterval(expression.NewLiteral(int64(10), types.Int64), "MICROSECOND"))
	require.NoError(err)
	expected = "2018-05-01 23:59:59.999990"
	result, err = f.Eval(ctx, sql.Row{})
	require.NoError(err)
	require.Equal(expected, result)

	f, err = NewSubDate(
		expression.NewLiteral("2018-05-02", types.LongText),
		expression.NewInterval(expression.NewLiteral(int64(1), types.Int64), "MICROSECOND"))
	require.NoError(err)
	expected = "2018-05-01 23:59:59.999999"
	result, err = f.Eval(ctx, sql.Row{})
	require.NoError(err)
	require.Equal(expected, result)

	// If the interval param is NULL, then NULL is returned
	f2, err := NewSubDate(
		expression.NewLiteral("2018-05-02", types.LongText),
		expression.NewGetField(0, types.Int64, "foo", true))
	result, err = f2.Eval(ctx, sql.Row{nil})
	require.NoError(err)
	require.Nil(result)

	f, err = NewSubDate(
		expression.NewGetField(0, types.Int64, "foo", true),
		expression.NewLiteral(int64(1), types.Int64))

	// If the date param is NULL, then NULL is returned
	result, err = f.Eval(ctx, sql.Row{nil})
	require.NoError(err)
	require.Nil(result)

	// If a time is passed (and no date) then NULL is returned
	result, err = f.Eval(ctx, sql.Row{"12:00:56"})
	require.NoError(err)
	require.Nil(result)

	// If an invalid date is passed, then NULL is returned
	result, err = f.Eval(ctx, sql.Row{"asdasdasd"})
	require.NoError(err)
	require.Nil(result)
}

func TestDateSub(t *testing.T) {
	require := require.New(t)
	ctx := sql.NewEmptyContext()

	_, err := NewDateSub()
	require.Error(err)

	_, err = NewDateSub(expression.NewLiteral("2018-05-02", types.LongText))
	require.Error(err)

	_, err = NewDateSub(expression.NewLiteral("2018-05-02", types.LongText),
		expression.NewLiteral(int64(1), types.Int64),
	)
	require.Error(err)

	f, err := NewDateSub(expression.NewGetField(0, types.Text, "foo", false),
		expression.NewInterval(
			expression.NewLiteral(int64(1), types.Int64),
			"DAY",
		),
	)
	require.NoError(err)

	expected := "2018-05-01"
	result, err := f.Eval(ctx, sql.Row{"2018-05-02"})
	require.NoError(err)
	require.Equal(expected, result)

	result, err = f.Eval(ctx, sql.Row{"12:34:56"})
	require.NoError(err)
	require.Nil(result)

	result, err = f.Eval(ctx, sql.Row{nil})
	require.NoError(err)
	require.Nil(result)

	result, err = f.Eval(ctx, sql.Row{"asdasdasd"})
	require.NoError(err)
	require.Nil(result)
}

func TestTimeDiff(t *testing.T) {
	toTimespan := func(str string) types.Timespan {
		res, err := types.Time.ConvertToTimespan(str)
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
		expected interface{}
		err      bool
	}{
		{
			"invalid type text",
			expression.NewLiteral("hello there", types.Text),
			expression.NewConvert(expression.NewLiteral("01:00:00", types.Text), expression.ConvertToTime),
			nil,
			false,
		},
		{
			"invalid type date",
			expression.NewConvert(expression.NewLiteral("2020-01-03", types.Text), expression.ConvertToDate),
			expression.NewConvert(expression.NewLiteral("2020-01-04", types.Text), expression.ConvertToDate),
			toTimespan("-24:00:00"),
			false,
		},
		{
			"type mismatch 1",
			expression.NewLiteral(time.Date(2008, time.December, 29, 1, 1, 1, 2, time.Local), types.Timestamp),
			expression.NewConvert(expression.NewLiteral("01:00:00", types.Text), expression.ConvertToTime),
			nil,
			false,
		},
		{
			"type mismatch 2",
			expression.NewLiteral("00:00:00.2", types.Text),
			expression.NewLiteral("2020-10-10 10:10:10", types.Text),
			nil,
			false,
		},
		{
			"valid mismatch",
			expression.NewLiteral(time.Date(2008, time.December, 29, 1, 1, 1, 2, time.Local), types.Timestamp),
			expression.NewLiteral(time.Date(2008, time.December, 30, 1, 1, 1, 2, time.Local), types.DatetimeMaxPrecision),
			toTimespan("-24:00:00"),
			false,
		},
		{
			"timestamp types 1",
			expression.NewLiteral(time.Date(2018, time.May, 2, 0, 0, 0, 0, time.Local), types.Timestamp),
			expression.NewLiteral(time.Date(2018, time.May, 2, 0, 0, 1, 0, time.Local), types.Timestamp),
			toTimespan("-00:00:01"),
			false,
		},
		{
			"timestamp types 2",
			expression.NewLiteral(time.Date(2008, time.December, 31, 23, 59, 59, 1, time.Local), types.Timestamp),
			expression.NewLiteral(time.Date(2008, time.December, 30, 1, 1, 1, 2, time.Local), types.Timestamp),
			toTimespan("46:58:57.999999"),
			false,
		},
		{
			"time types 1",
			expression.NewConvert(expression.NewLiteral("00:00:00.1", types.Text), expression.ConvertToTime),
			expression.NewConvert(expression.NewLiteral("00:00:00.2", types.Text), expression.ConvertToTime),
			toTimespan("-00:00:00.100000"),
			false,
		},
		{
			"time types 2",
			expression.NewLiteral("00:00:00.2", types.Text),
			expression.NewLiteral("00:00:00.4", types.Text),
			toTimespan("-00:00:00.200000"),
			false,
		},
		{
			"datetime types",
			expression.NewLiteral(time.Date(2008, time.December, 29, 0, 0, 0, 0, time.Local), types.DatetimeMaxPrecision),
			expression.NewLiteral(time.Date(2008, time.December, 30, 0, 0, 0, 0, time.Local), types.DatetimeMaxPrecision),
			toTimespan("-24:00:00"),
			false,
		},
		{
			"datetime string types",
			expression.NewLiteral("2008-12-29 00:00:00", types.Text),
			expression.NewLiteral("2008-12-30 00:00:00", types.Text),
			toTimespan("-24:00:00"),
			false,
		},
		{
			"datetime string mix types",
			expression.NewLiteral(time.Date(2008, time.December, 29, 0, 0, 0, 0, time.UTC), types.DatetimeMaxPrecision),
			expression.NewLiteral("2008-12-30 00:00:00", types.Text),
			toTimespan("-24:00:00"),
			false,
		},
		{
			"first argument is null",
			nil,
			expression.NewLiteral("2008-12-30 00:00:00", types.Text),
			nil,
			false,
		},
		{
			"second argument is null",
			expression.NewLiteral("2008-12-30 00:00:00", types.Text),
			nil,
			nil,
			false,
		},
		{
			"both arguments are null",
			nil,
			nil,
			nil,
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
		{"time and text types, ", types.DatetimeMaxPrecision, types.Text, sql.NewRow(dt, "2019-12-28"), int64(3), nil},
		{"text types, diff day, less than 24 hours time diff", types.Text, types.Text, sql.NewRow("2007-12-31 23:58:59", "2007-12-30 23:59:59"), int64(1), nil},
		{"text types, same day, 23:59:59 time diff", types.Text, types.Text, sql.NewRow("2007-12-30 23:59:59", "2007-12-30 00:00:00"), int64(0), nil},
		{"text types, diff day, 1 min time diff", types.Text, types.Text, sql.NewRow("2007-12-31 00:00:59", "2007-12-30 23:59:59"), int64(1), nil},
		{"text types, negative result", types.Text, types.Text, sql.NewRow("2010-11-30 22:59:59", "2010-12-31 23:59:59"), int64(-31), nil},
		{"text types, positive result", types.Text, types.Text, sql.NewRow("2007-12-31 23:59:59", "2007-12-30"), int64(1), nil},
		{"text types, negative result", types.Text, types.Text, sql.NewRow("2010-11-30 23:59:59", "2010-12-31"), int64(-31), nil},
		{"text types, day difference result", types.Text, types.Text, sql.NewRow("2017-06-25", "2017-06-15"), int64(10), nil},
		{"text types, year difference result", types.Text, types.Text, sql.NewRow("2017-06-25", "2016-06-15"), int64(375), nil},
		{"text types, format with /", types.Text, types.Text, sql.NewRow("2007/12/22", "2007/12/20"), int64(2), nil},
		{"text types, positive result", types.Text, types.Text, sql.NewRow("2007-12-31", "2007-12-29 23:59:59"), int64(2), nil},
		{"text types, negative result", types.Text, types.Text, sql.NewRow("2010-11-02", "2010-11-30 23:59:59"), int64(-28), nil},
		{"first argument is null", types.Text, types.Text, sql.NewRow(nil, "2010-11-02"), nil, nil},
		{"second argument is null", types.Text, types.Text, sql.NewRow("2010-11-02", nil), nil, nil},
		{"both arguments are null", types.Text, types.Text, sql.NewRow(nil, nil), nil, nil},
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
		{"invalid unit", types.Text, types.Text, types.Text, sql.NewRow("MILLISECOND", "2007-12-30 23:59:59", "2007-12-31 00:00:00"), nil, true},
		{"microsecond", types.Text, types.Text, types.Text, sql.NewRow("MICROSECOND", "2007-12-30 23:59:59", "2007-12-31 00:00:00"), int64(1000000), false},
		{"microsecond - small number", types.Text, types.DatetimeMaxPrecision, types.DatetimeMaxPrecision, sql.NewRow("MICROSECOND",
			time.Date(2017, 11, 12, 16, 16, 25, 2*int(time.Microsecond), time.Local),
			time.Date(2017, 11, 12, 16, 16, 25, 333*int(time.Microsecond), time.Local)), int64(331), false},
		{"microsecond - negative", types.Text, types.Text, types.Text, sql.NewRow("SQL_TSI_MICROSECOND", "2017-11-12 16:16:25.000022 +0000 UTC", "2017-11-12 16:16:25.000000 +0000 UTC"), int64(-22), false},
		{"second", types.Text, types.Text, types.Text, sql.NewRow("SECOND", "2007-12-30 23:59:58", "2007-12-31 00:00:00"), int64(2), false},
		{"second", types.Text, types.Text, types.Text, sql.NewRow("SQL_TSI_SECOND", "2017-11-12 16:16:25.000022 +0000 UTC", "2017-11-12 16:16:25.000000 +0000 UTC"), int64(0), false},
		{"minute - less than minute", types.Text, types.Text, types.Text, sql.NewRow("MINUTE", "2007-12-30 23:59:59", "2007-12-31 00:00:00"), int64(0), false},
		{"minute - exactly one minute", types.Text, types.Text, types.Text, sql.NewRow("SQL_TSI_MINUTE", "2007-12-30 23:59:00", "2007-12-31 00:00:00"), int64(1), false},
		{"hour - less", types.Text, types.Text, types.Text, sql.NewRow("SQL_TSI_HOUR", "2007-12-30 22:29:00", "2007-12-31 00:00:00"), int64(1), false},
		{"hour", types.Text, types.Text, types.Text, sql.NewRow("HOUR", "2007-12-29 22:29:00", "2007-12-31 00:00:00"), int64(25), false},
		{"hour - negative", types.Text, types.Text, types.Text, sql.NewRow("HOUR", "2007-12-31 22:29:00", "2007-12-31 00:00:00"), int64(-22), false},
		{"day - less", types.Text, types.Text, types.Text, sql.NewRow("DAY", "2007-12-30 22:29:00", "2007-12-31 00:00:00"), int64(0), false},
		{"day", types.Text, types.Text, types.Text, sql.NewRow("SQL_TSI_DAY", "2007-12-01 22:29:00", "2007-12-31 00:00:00"), int64(29), false},
		{"day - negative", types.Text, types.Text, types.Text, sql.NewRow("DAY", "2007-12-31 22:29:00", "2007-12-30 00:00:00"), int64(-1), false},
		{"week - less", types.Text, types.Text, types.Text, sql.NewRow("WEEK", "2007-12-31 00:00:00", "2007-12-24 00:00:01"), int64(0), false},
		{"week", types.Text, types.Text, types.Text, sql.NewRow("WEEK", "2007-10-30 00:00:00", "2007-12-24 00:00:01"), int64(7), false},
		{"week - negative", types.Text, types.Text, types.Text, sql.NewRow("SQL_TSI_WEEK", "2007-12-31 00:00:00", "2007-12-24 00:00:00"), int64(-1), false},
		{"month - second less than a month", types.Text, types.Text, types.Text, sql.NewRow("SQL_TSI_MONTH", "2007-11-30 00:00:00", "2007-12-29 23:59:59"), int64(0), false},
		{"month", types.Text, types.Text, types.Text, sql.NewRow("MONTH", "2007-01-31 00:00:00", "2007-12-30 00:00:00"), int64(10), false},
		{"month - negative", types.Text, types.Text, types.Text, sql.NewRow("MONTH", "2008-01-31 00:00:01", "2007-12-30 00:00:00"), int64(-1), false},
		{"quarter - exactly a quarter", types.Text, types.Text, types.Text, sql.NewRow("QUARTER", "2007-08-30 00:00:00", "2007-11-30 00:00:00"), int64(1), false},
		{"quarter - second less than a quarter", types.Text, types.Text, types.Text, sql.NewRow("SQL_TSI_QUARTER", "2007-08-30 00:00:01", "2007-11-30 00:00:00"), int64(0), false},
		{"quarter", types.Text, types.Text, types.Text, sql.NewRow("QUARTER", "2006-08-30 00:00:00", "2007-11-30 00:00:00"), int64(5), false},
		{"quarter - negative", types.Text, types.Text, types.Text, sql.NewRow("QUARTER", "2006-08-30 00:00:00", "2002-11-30 00:00:00"), int64(-15), false},
		{"year - second less than a month", types.Text, types.Text, types.Text, sql.NewRow("YEAR", "2019-01-01 00:00:00", "2019-12-31 23:59:59"), int64(0), false},
		{"year", types.Text, types.Text, types.Text, sql.NewRow("YEAR", "2016-09-04 00:00:01", "2021-09-04 00:00:00"), int64(4), false},
		{"year - ", types.Text, types.Text, types.Text, sql.NewRow("YEAR", "2016-09-04 01:00:01", "2021-09-04 02:00:02"), int64(5), false},
		{"year - negative", types.Text, types.Text, types.Text, sql.NewRow("SQL_TSI_YEAR", "2016-09-05 00:00:00", "2006-09-04 23:59:59"), int64(-10), false},
		{"unit is null", types.Text, types.Text, types.Text, sql.NewRow(nil, "2016-09-05 00:00:00", "2006-09-04 23:59:59"), nil, true},
		{"first timestamp is null", types.Text, types.Text, types.Text, sql.NewRow("YEAR", nil, "2021-09-04 02:00:02"), nil, false},
		{"second timestamp is null", types.Text, types.Text, types.Text, sql.NewRow("YEAR", "2016-09-04 00:00:01", nil), nil, false},
		{"both timestamps are null", types.Text, types.Text, types.Text, sql.NewRow("YEAR", nil, nil), nil, false},
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
