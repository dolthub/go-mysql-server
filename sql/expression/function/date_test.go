package function

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/expression"
)

func TestDateAdd(t *testing.T) {
	require := require.New(t)

	_, err := NewDateAdd()
	require.Error(err)

	_, err = NewDateAdd(expression.NewLiteral("2018-05-02", sql.Text))
	require.Error(err)

	_, err = NewDateAdd(
		expression.NewLiteral("2018-05-02", sql.Text),
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

	_, err = NewDateSub(expression.NewLiteral("2018-05-02", sql.Text))
	require.Error(err)

	_, err = NewDateSub(
		expression.NewLiteral("2018-05-02", sql.Text),
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
