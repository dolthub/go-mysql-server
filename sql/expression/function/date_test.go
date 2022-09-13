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
)

func TestDateAdd(t *testing.T) {
	require := require.New(t)
	ctx := sql.NewEmptyContext()

	_, err := NewDateAdd()
	require.Error(err)

	_, err = NewDateAdd(expression.NewLiteral("2018-05-02", sql.LongText))
	require.Error(err)

	_, err = NewDateAdd(expression.NewLiteral("2018-05-02", sql.LongText),
		expression.NewLiteral(int64(1), sql.Int64),
	)
	require.Error(err)

	f, err := NewDateAdd(expression.NewGetField(0, sql.Text, "foo", false),
		expression.NewInterval(
			expression.NewLiteral(int64(1), sql.Int64),
			"DAY",
		),
	)
	require.NoError(err)

	expected := time.Date(2018, time.May, 3, 0, 0, 0, 0, time.UTC)

	result, err := f.Eval(ctx, sql.Row{"2018-05-02"})
	require.NoError(err)
	require.Equal(expected, result)

	result, err = f.Eval(ctx, sql.Row{nil})
	require.NoError(err)
	require.Nil(result)

	_, err = f.Eval(ctx, sql.Row{"asdasdasd"})
	require.Error(err)
}

func TestDateSub(t *testing.T) {
	require := require.New(t)
	ctx := sql.NewEmptyContext()

	_, err := NewDateSub()
	require.Error(err)

	_, err = NewDateSub(expression.NewLiteral("2018-05-02", sql.LongText))
	require.Error(err)

	_, err = NewDateSub(expression.NewLiteral("2018-05-02", sql.LongText),
		expression.NewLiteral(int64(1), sql.Int64),
	)
	require.Error(err)

	f, err := NewDateSub(expression.NewGetField(0, sql.Text, "foo", false),
		expression.NewInterval(
			expression.NewLiteral(int64(1), sql.Int64),
			"DAY",
		),
	)
	require.NoError(err)

	expected := time.Date(2018, time.May, 1, 0, 0, 0, 0, time.UTC)

	result, err := f.Eval(ctx, sql.Row{"2018-05-02"})
	require.NoError(err)
	require.Equal(expected, result)

	result, err = f.Eval(ctx, sql.Row{nil})
	require.NoError(err)
	require.Nil(result)

	_, err = f.Eval(ctx, sql.Row{"asdasdasd"})
	require.Error(err)
}

func TestUnixTimestamp(t *testing.T) {
	require := require.New(t)

	ctx := sql.NewEmptyContext()
	_, err := NewUnixTimestamp()
	require.NoError(err)

	_, err = NewUnixTimestamp(expression.NewLiteral("2018-05-02", sql.LongText))
	require.NoError(err)

	_, err = NewUnixTimestamp(expression.NewLiteral("2018-05-02", sql.LongText))
	require.NoError(err)

	_, err = NewUnixTimestamp(expression.NewLiteral("2018-05-02", sql.LongText), expression.NewLiteral("2018-05-02", sql.LongText))
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

	ut, err = NewUnixTimestamp(expression.NewLiteral("2018-05-02", sql.LongText))
	require.NoError(err)
	expected = float64(time.Date(2018, 5, 2, 0, 0, 0, 0, time.UTC).Unix())
	result, err = ut.Eval(ctx, nil)
	require.NoError(err)
	require.Equal(expected, result)
	require.Equal(uint16(0), ctx.WarningCount())

	ut, err = NewUnixTimestamp(expression.NewLiteral(nil, sql.Null))
	require.NoError(err)
	expected = nil
	result, err = ut.Eval(ctx, nil)
	require.NoError(err)
	require.Equal(expected, result)
	require.Equal(uint16(0), ctx.WarningCount())

	// When MySQL can't convert the expression to a date, it always returns 0 and sets a warning
	ut, err = NewUnixTimestamp(expression.NewLiteral(1577995200, sql.Int64))
	require.NoError(err)
	result, err = ut.Eval(ctx, nil)
	require.NoError(err)
	require.Equal(0, result)
	require.Equal(uint16(1), ctx.WarningCount())
	require.Equal("Incorrect datetime value: 1577995200", ctx.Warnings()[0].Message)
	require.Equal(1292, ctx.Warnings()[0].Code)

	// When MySQL can't convert the expression to a date, it always returns 0 and sets a warning
	ctx.ClearWarnings()
	ut, err = NewUnixTimestamp(expression.NewLiteral("d0lthub", sql.Text))
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

	_, err := NewUnixTimestamp(expression.NewLiteral(0, sql.Int64))
	require.NoError(err)

	_, err = NewUnixTimestamp(expression.NewLiteral(1447430881, sql.Int64))
	require.NoError(err)
}
