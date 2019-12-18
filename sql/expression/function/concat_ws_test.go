package function

import (
	"testing"

	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/expression"
	"github.com/stretchr/testify/require"
)

func TestConcatWithSeparator(t *testing.T) {
	t.Run("multiple arguments", func(t *testing.T) {
		require := require.New(t)
		f, err := NewConcatWithSeparator(
			expression.NewLiteral(",", sql.LongText),
			expression.NewLiteral("foo", sql.LongText),
			expression.NewLiteral(5, sql.LongText),
			expression.NewLiteral(true, sql.Boolean),
		)
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal("foo,5,true", v)
	})

	t.Run("some argument is empty", func(t *testing.T) {
		require := require.New(t)
		f, err := NewConcatWithSeparator(
			expression.NewLiteral(",", sql.LongText),
			expression.NewLiteral("foo", sql.LongText),
			expression.NewLiteral("", sql.LongText),
			expression.NewLiteral(true, sql.Boolean),
		)
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal("foo,,true", v)
	})

	t.Run("some argument is nil", func(t *testing.T) {
		require := require.New(t)
		f, err := NewConcatWithSeparator(
			expression.NewLiteral(",", sql.LongText),
			expression.NewLiteral("foo", sql.LongText),
			expression.NewLiteral(nil, sql.LongText),
			expression.NewLiteral(true, sql.Boolean),
		)
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal("foo,true", v)
	})

	t.Run("separator is nil", func(t *testing.T) {
		require := require.New(t)
		f, err := NewConcatWithSeparator(
			expression.NewLiteral(nil, sql.LongText),
			expression.NewLiteral("foo", sql.LongText),
			expression.NewLiteral(5, sql.LongText),
			expression.NewLiteral(true, sql.Boolean),
		)
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(nil, v)
	})

	t.Run("concat_ws array", func(t *testing.T) {
		require := require.New(t)
		f, err := NewConcatWithSeparator(
			expression.NewLiteral([]interface{}{",", 5, "bar", true}, sql.CreateArray(sql.LongText)),
		)
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal("5,bar,true", v)
	})
}

func TestNewConcatWithSeparator(t *testing.T) {
	require := require.New(t)

	_, err := NewConcatWithSeparator(expression.NewLiteral(nil, sql.CreateArray(sql.LongText)))
	require.NoError(err)

	_, err = NewConcatWithSeparator(expression.NewLiteral(nil, sql.CreateArray(sql.LongText)), expression.NewLiteral(nil, sql.Int64))
	require.Error(err)
	require.True(ErrConcatArrayWithOthers.Is(err))

	_, err = NewConcatWithSeparator(expression.NewLiteral(nil, sql.CreateTuple(sql.LongText, sql.LongText)))
	require.Error(err)
	require.True(sql.ErrInvalidType.Is(err))

	_, err = NewConcatWithSeparator(
		expression.NewLiteral(nil, sql.LongText),
		expression.NewLiteral(nil, sql.Boolean),
		expression.NewLiteral(nil, sql.Int64),
		expression.NewLiteral(nil, sql.LongText),
	)
	require.NoError(err)
}
