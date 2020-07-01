package expression

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/liquidata-inc/go-mysql-server/sql"
)

func TestTuple(t *testing.T) {
	require := require.New(t)

	tup := NewTuple(
		NewLiteral(int64(1), sql.Int64),
		NewLiteral(float64(3.14), sql.Float64),
		NewLiteral("foo", sql.LongText),
	)

	ctx := sql.NewEmptyContext()

	require.False(tup.IsNullable())
	require.True(tup.Resolved())
	require.Equal(sql.CreateTuple(sql.Int64, sql.Float64, sql.LongText), tup.Type())

	result, err := tup.Eval(ctx, nil)
	require.NoError(err)
	require.Equal([]interface{}{int64(1), float64(3.14), "foo"}, result)

	tup = NewTuple(
		NewGetField(0, sql.LongText, "text", true),
	)

	require.True(tup.IsNullable())
	require.True(tup.Resolved())
	require.Equal(sql.LongText, tup.Type())

	result, err = tup.Eval(ctx, sql.NewRow("foo"))
	require.NoError(err)
	require.Equal("foo", result)

	tup = NewTuple(
		NewGetField(0, sql.LongText, "text", true),
		NewLiteral("bar", sql.LongText),
	)

	require.False(tup.IsNullable())
	require.True(tup.Resolved())
	require.Equal(sql.CreateTuple(sql.LongText, sql.LongText), tup.Type())

	result, err = tup.Eval(ctx, sql.NewRow("foo"))
	require.NoError(err)
	require.Equal([]interface{}{"foo", "bar"}, result)

	tup = NewTuple(
		NewUnresolvedColumn("bar"),
		NewLiteral("bar", sql.LongText),
	)

	require.False(tup.Resolved())
	require.False(tup.IsNullable())
}
