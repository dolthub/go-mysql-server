package function

import (
	"testing"

	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/expression"
	"github.com/stretchr/testify/require"
)

func TestConcat(t *testing.T) {
	t.Run("concat multiple arguments", func(t *testing.T) {
		require := require.New(t)
		f, err := NewConcat(
			expression.NewLiteral("foo", sql.LongText),
			expression.NewLiteral(5, sql.LongText),
			expression.NewLiteral(true, sql.Boolean),
		)
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal("foo5true", v)
	})

	t.Run("some argument is nil", func(t *testing.T) {
		require := require.New(t)
		f, err := NewConcat(
			expression.NewLiteral("foo", sql.LongText),
			expression.NewLiteral(nil, sql.LongText),
			expression.NewLiteral(true, sql.Boolean),
		)
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(nil, v)
	})

	t.Run("concat array", func(t *testing.T) {
		require := require.New(t)
		f, err := NewConcat(
			expression.NewLiteral([]interface{}{5, "bar", true}, sql.CreateArray(sql.LongText)),
		)
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal("5bartrue", v)
	})
}

func TestNewConcat(t *testing.T) {
	require := require.New(t)

	_, err := NewConcat(expression.NewLiteral(nil, sql.CreateArray(sql.LongText)))
	require.NoError(err)

	_, err = NewConcat(expression.NewLiteral(nil, sql.CreateArray(sql.LongText)), expression.NewLiteral(nil, sql.Int64))
	require.Error(err)
	require.True(ErrConcatArrayWithOthers.Is(err))

	_, err = NewConcat(expression.NewLiteral(nil, sql.CreateTuple(sql.LongText, sql.LongText)))
	require.Error(err)
	require.True(sql.ErrInvalidType.Is(err))

	_, err = NewConcat(
		expression.NewLiteral(nil, sql.LongText),
		expression.NewLiteral(nil, sql.Boolean),
		expression.NewLiteral(nil, sql.Int64),
		expression.NewLiteral(nil, sql.LongText),
	)
	require.NoError(err)
}
