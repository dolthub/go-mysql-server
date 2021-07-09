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

	_, err := NewDateAdd(ctx)
	require.Error(err)

	_, err = NewDateAdd(ctx, expression.NewLiteral("2018-05-02", sql.LongText))
	require.Error(err)

	_, err = NewDateAdd(ctx,
		expression.NewLiteral("2018-05-02", sql.LongText),
		expression.NewLiteral(int64(1), sql.Int64),
	)
	require.Error(err)

	f, err := NewDateAdd(ctx,
		expression.NewGetField(0, sql.Text, "foo", false),
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

	_, err := NewDateSub(ctx)
	require.Error(err)

	_, err = NewDateSub(ctx, expression.NewLiteral("2018-05-02", sql.LongText))
	require.Error(err)

	_, err = NewDateSub(ctx,
		expression.NewLiteral("2018-05-02", sql.LongText),
		expression.NewLiteral(int64(1), sql.Int64),
	)
	require.Error(err)

	f, err := NewDateSub(ctx,
		expression.NewGetField(0, sql.Text, "foo", false),
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
	_, err := NewUnixTimestamp(ctx)
	require.NoError(err)

	_, err = NewUnixTimestamp(ctx, expression.NewLiteral("2018-05-02", sql.LongText))
	require.NoError(err)

	_, err = NewUnixTimestamp(ctx, expression.NewLiteral("2018-05-02", sql.LongText))
	require.NoError(err)

	_, err = NewUnixTimestamp(ctx, expression.NewLiteral("2018-05-02", sql.LongText), expression.NewLiteral("2018-05-02", sql.LongText))
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

	ut, err = NewUnixTimestamp(ctx, expression.NewLiteral("2018-05-02", sql.LongText))
	require.NoError(err)
	expected = float64(time.Date(2018, 5, 2, 0, 0, 0, 0, time.UTC).Unix())
	result, err = ut.Eval(ctx, nil)
	require.NoError(err)
	require.Equal(expected, result)

	ut, err = NewUnixTimestamp(ctx, expression.NewLiteral(nil, sql.Null))
	require.NoError(err)
	expected = nil
	result, err = ut.Eval(ctx, nil)
	require.NoError(err)
	require.Equal(expected, result)
}

func TestFromUnixtime(t *testing.T) {
	require := require.New(t)

	ctx := sql.NewEmptyContext()
	_, err := NewUnixTimestamp(ctx, expression.NewLiteral(0, sql.Int64))
	require.NoError(err)

	_, err = NewUnixTimestamp(ctx, expression.NewLiteral(1447430881, sql.Int64))
	require.NoError(err)
}
