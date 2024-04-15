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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

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

	// If the second argument is NOT an interval, then it's assumed to be a day interval
	f, err := NewAddDate(
		expression.NewLiteral("2018-05-02", types.LongText),
		expression.NewLiteral(int64(1), types.Int64))
	require.NoError(err)
	expected := time.Date(2018, time.May, 3, 0, 0, 0, 0, time.UTC)
	result, err := f.Eval(ctx, sql.Row{})
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

	// If the interval param is NULL, then NULL is returned
	f2, err := NewAddDate(
		expression.NewLiteral("2018-05-02", types.LongText),
		expression.NewGetField(0, types.Int64, "foo", true))
	result, err = f2.Eval(ctx, sql.Row{nil})
	require.NoError(err)
	require.Nil(result)

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

	// If the second argument is NOT an interval, then it's assumed to be a day interval
	t.Skip("Interval does not handle overflows correctly")
	f, err = NewAddDate(
		expression.NewLiteral("2018-05-02", types.Text),
		expression.NewLiteral(int64(1_000_000), types.Int64))
	require.NoError(err)
	expected = time.Date(4756, time.March, 29, 0, 0, 0, 0, time.UTC)
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

	expected := time.Date(2018, time.May, 3, 0, 0, 0, 0, time.UTC)

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

	// If the second argument is NOT an interval, then it's assumed to be a day interval
	f, err := NewSubDate(
		expression.NewLiteral("2018-05-02", types.LongText),
		expression.NewLiteral(int64(1), types.Int64))
	require.NoError(err)
	expected := time.Date(2018, time.May, 1, 0, 0, 0, 0, time.UTC)
	result, err := f.Eval(ctx, sql.Row{})
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

	// If the interval param is NULL, then NULL is returned
	f2, err := NewSubDate(
		expression.NewLiteral("2018-05-02", types.LongText),
		expression.NewGetField(0, types.Int64, "foo", true))
	result, err = f2.Eval(ctx, sql.Row{nil})
	require.NoError(err)
	require.Nil(result)

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

	expected := time.Date(2018, time.May, 1, 0, 0, 0, 0, time.UTC)

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

func TestUnixTimestamp(t *testing.T) {
	require := require.New(t)

	ctx := sql.NewEmptyContext()
	_, err := NewUnixTimestamp()
	require.NoError(err)

	_, err = NewUnixTimestamp(expression.NewLiteral("2018-05-02", types.LongText))
	require.NoError(err)

	_, err = NewUnixTimestamp(expression.NewLiteral("2018-05-02", types.LongText))
	require.NoError(err)

	_, err = NewUnixTimestamp(expression.NewLiteral("2018-05-02", types.LongText), expression.NewLiteral("2018-05-02", types.LongText))
	require.Error(err)

	date := time.Date(2018, time.December, 2, 16, 25, 0, 0, time.Local)
	testNowFunc := func() time.Time {
		return date
	}

	var ctx2 *sql.Context
	err = sql.RunWithNowFunc(testNowFunc, func() error {
		ctx2 = sql.NewEmptyContext()
		return nil
	})
	require.NoError(err)

	var ut sql.Expression
	var expected interface{}
	ut = &UnixTimestamp{nil}
	expected = float64(date.Unix())
	result, err := ut.Eval(ctx2, nil)
	require.NoError(err)
	require.Equal(expected, result)
	require.Equal(uint16(0), ctx.WarningCount())

	ut, err = NewUnixTimestamp(expression.NewLiteral("2018-05-02", types.LongText))
	require.NoError(err)
	expected = float64(time.Date(2018, 5, 2, 0, 0, 0, 0, time.UTC).Unix())
	result, err = ut.Eval(ctx, nil)
	require.NoError(err)
	require.Equal(expected, result)
	require.Equal(uint16(0), ctx.WarningCount())

	ut, err = NewUnixTimestamp(expression.NewLiteral(nil, types.Null))
	require.NoError(err)
	expected = nil
	result, err = ut.Eval(ctx, nil)
	require.NoError(err)
	require.Equal(expected, result)
	require.Equal(uint16(0), ctx.WarningCount())

	// When MySQL can't convert the expression to a date, it always returns 0 and sets a warning
	ut, err = NewUnixTimestamp(expression.NewLiteral(1577995200, types.Int64))
	require.NoError(err)
	result, err = ut.Eval(ctx, nil)
	require.NoError(err)
	require.Equal(0, result)
	require.Equal(uint16(1), ctx.WarningCount())
	require.Equal("Incorrect datetime value: 1577995200", ctx.Warnings()[0].Message)
	require.Equal(1292, ctx.Warnings()[0].Code)

	// When MySQL can't convert the expression to a date, it always returns 0 and sets a warning
	ctx.ClearWarnings()
	// TODO: ClearWarnings has to be called twice to actually clear the warnings because of the way it sets its
	//       warncnt member var. This should be fixed, but existing behavior depends on this behavior currently.
	ctx.ClearWarnings()
	ut, err = NewUnixTimestamp(expression.NewLiteral("d0lthub", types.Text))
	require.NoError(err)
	result, err = ut.Eval(ctx, nil)
	require.NoError(err)
	require.Equal(0, result)
	require.Equal(uint16(1), ctx.WarningCount())
	require.Equal("Incorrect datetime value: 'd0lthub'", ctx.Warnings()[0].Message)
	require.Equal(1292, ctx.Warnings()[0].Code)
}

func TestFromUnixtime(t *testing.T) {
	require := require.New(t)

	_, err := NewUnixTimestamp(expression.NewLiteral(0, types.Int64))
	require.NoError(err)

	_, err = NewUnixTimestamp(expression.NewLiteral(1447430881, types.Int64))
	require.NoError(err)
}
