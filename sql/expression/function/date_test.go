package function

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/liquidata-inc/go-mysql-server/sql/expression"
)

func TestDateAdd(t *testing.T) {
	require := require.New(t)

	_, err := NewDateAdd()
	require.Error(err)

	_, err = NewDateAdd(expression.NewLiteral("2018-05-02", sql.LongText))
	require.Error(err)

	_, err = NewDateAdd(
		expression.NewLiteral("2018-05-02", sql.LongText),
		expression.NewLiteral(int64(1), sql.Int64),
	)
	require.Error(err)

	f, err := NewDateAdd(
		expression.NewGetField(0, sql.Text, "foo", false),
		expression.NewInterval(
			expression.NewLiteral(int64(1), sql.Int64),
			"DAY",
		),
	)
	require.NoError(err)

	ctx := sql.NewEmptyContext()
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

	_, err := NewDateSub()
	require.Error(err)

	_, err = NewDateSub(expression.NewLiteral("2018-05-02", sql.LongText))
	require.Error(err)

	_, err = NewDateSub(
		expression.NewLiteral("2018-05-02", sql.LongText),
		expression.NewLiteral(int64(1), sql.Int64),
	)
	require.Error(err)

	f, err := NewDateSub(
		expression.NewGetField(0, sql.Text, "foo", false),
		expression.NewInterval(
			expression.NewLiteral(int64(1), sql.Int64),
			"DAY",
		),
	)
	require.NoError(err)

	ctx := sql.NewEmptyContext()
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

	var ctx *sql.Context
	err = sql.RunWithNowFunc(testNowFunc, func() error {
		ctx = sql.NewEmptyContext()
		return nil
	})
	require.NoError(err)

	var ut sql.Expression
	var expected interface{}
	ut = &UnixTimestamp{nil}
	expected = float64(date.Unix())
	result, err := ut.Eval(ctx, nil)
	require.NoError(err)
	require.Equal(expected, result)

	ut, err = NewUnixTimestamp(expression.NewLiteral("2018-05-02", sql.LongText))
	require.NoError(err)
	expected = float64(time.Date(2018, 5, 2, 0, 0, 0, 0, time.UTC).Unix())
	result, err = ut.Eval(ctx, nil)
	require.NoError(err)
	require.Equal(expected, result)

	ut, err = NewUnixTimestamp(expression.NewLiteral(nil, sql.Null))
	require.NoError(err)
	expected = nil
	result, err = ut.Eval(ctx, nil)
	require.NoError(err)
	require.Equal(expected, result)
}

func TestUTCTimestamp(t *testing.T) {
	require := require.New(t)

	_, err := NewUTCTimestamp()
	require.NoError(err)

	_, err = NewUTCTimestamp(expression.NewLiteral("2018-05-02", sql.LongText))
	require.NoError(err)

	_, err = NewUTCTimestamp(expression.NewLiteral("2018-05-02", sql.LongText))
	require.NoError(err)

	_, err = NewUTCTimestamp(expression.NewLiteral("2018-05-02", sql.LongText), expression.NewLiteral("2018-05-02", sql.LongText))
	require.Error(err)

	date := time.Date(2018, time.December, 2, 16, 25, 0, 0, time.Local)
	testNowFunc := func() time.Time {
		return date
	}

	var ctx *sql.Context
	err = sql.RunWithNowFunc(testNowFunc, func() error {
		ctx = sql.NewEmptyContext()
		return nil
	})
	require.NoError(err)

	var ut sql.Expression
	var expected interface{}
	ut = &UTCTimestamp{nil}
	expected = date.UTC()
	result, err := ut.Eval(ctx, nil)
	require.NoError(err)
	require.Equal(expected, result)

	ut, err = NewUTCTimestamp(expression.NewLiteral("2018-05-02", sql.LongText))
	require.NoError(err)
	expected = time.Date(2018, 5, 2, 0, 0, 0, 0, time.UTC)
	result, err = ut.Eval(ctx, nil)
	require.NoError(err)
	require.Equal(expected, result)

	ut, err = NewUTCTimestamp(expression.NewLiteral(nil, sql.Null))
	require.NoError(err)
	expected = nil
	result, err = ut.Eval(ctx, nil)
	require.NoError(err)
	require.Equal(expected, result)
}
