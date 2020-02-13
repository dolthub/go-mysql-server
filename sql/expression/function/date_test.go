package function

import (
	"testing"
	"time"

	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/expression"
	"github.com/stretchr/testify/require"
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

	_, err = NewUnixTimestamp(expression.NewLiteral("2018-05-02", sql.LongText), expression.NewLiteral("2018-05-02", sql.LongText))
	require.Error(err)

        date := time.Date(2018, time.December, 2, 16, 25, 0, 0, time.Local)
        clk := clock(func() time.Time {
                return date
        })
	ctx := sql.NewEmptyContext()

	var ut sql.Expression
	var expected interface{}
	ut = &UnixTimestamp{clk, nil}
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
