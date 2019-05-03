package function

import (
	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
	"testing"
)

func TestGreatest(t *testing.T) {
	t.Run("null", func(t *testing.T) {
		require := require.New(t)
		f, err := NewGreatest(
			expression.NewLiteral(nil, sql.Null),
			expression.NewLiteral(5, sql.Int64),
			expression.NewLiteral(1, sql.Int64),
		)
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(nil, v)
	})

	t.Run("negative and all ints", func(t *testing.T) {
		require := require.New(t)
		f, err := NewGreatest(
			expression.NewLiteral(int64(-1), sql.Int64),
			expression.NewLiteral(int64(5), sql.Int64),
			expression.NewLiteral(int64(1), sql.Int64),
		)
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.IsType(int64(1), v)
		require.Equal(int64(5), v)
	})

	t.Run("string mixed", func(t *testing.T) {
		require := require.New(t)
		f, err := NewGreatest(
			expression.NewLiteral(string("10"), sql.Int64),
			expression.NewLiteral(int64(5), sql.Int64),
			expression.NewLiteral(int64(1), sql.Int64),
		)
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.IsType(float64(1), v)
		require.Equal(float64(10), v)
	})

	t.Run("unconvertible string mixed ignored", func(t *testing.T) {
		require := require.New(t)
		f, err := NewGreatest(
			expression.NewLiteral(string("10"), sql.Int64),
			expression.NewLiteral(string("foobar"), sql.Int64),
			expression.NewLiteral(int64(5), sql.Int64),
			expression.NewLiteral(int64(1), sql.Int64),
		)
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.IsType(float64(1), v)
		require.Equal(float64(10), v)
	})

	t.Run("float mixed", func(t *testing.T) {
		require := require.New(t)
		f, err := NewGreatest(
			expression.NewLiteral(float64(10.0), sql.Float64),
			expression.NewLiteral(int(5), sql.Int64),
			expression.NewLiteral(int(1), sql.Int64),
		)
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.IsType(float64(1), v)
		require.Equal(float64(10.0), v)
	})

	t.Run("all strings", func(t *testing.T) {
		require := require.New(t)
		f, err := NewGreatest(
			expression.NewLiteral("aaa", sql.Text),
			expression.NewLiteral("bbb", sql.Text),
			expression.NewLiteral("9999", sql.Text),
			expression.NewLiteral("", sql.Text),
		)
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.IsType("", v)
		require.Equal("bbb", v)
	})
}

func TestLeast(t *testing.T) {
	t.Run("null", func(t *testing.T) {
		require := require.New(t)
		f, err := NewLeast(
			expression.NewLiteral(nil, sql.Null),
			expression.NewLiteral(5, sql.Int64),
			expression.NewLiteral(1, sql.Int64),
		)
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.Equal(nil, v)
	})

	t.Run("negative and all ints", func(t *testing.T) {
		require := require.New(t)
		f, err := NewLeast(
			expression.NewLiteral(int64(-1), sql.Int64),
			expression.NewLiteral(int64(5), sql.Int64),
			expression.NewLiteral(int64(1), sql.Int64),
		)
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.IsType(int64(1), v)
		require.Equal(int64(-1), v)
	})

	t.Run("string mixed", func(t *testing.T) {
		require := require.New(t)
		f, err := NewLeast(
			expression.NewLiteral(string("10"), sql.Int64),
			expression.NewLiteral(int64(5), sql.Int64),
			expression.NewLiteral(int64(1), sql.Int64),
		)
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.IsType(float64(1), v)
		require.Equal(float64(1), v)
	})

	t.Run("unconvertible string mixed ignored", func(t *testing.T) {
		require := require.New(t)
		f, err := NewLeast(
			expression.NewLiteral(string("10"), sql.Int64),
			expression.NewLiteral(string("foobar"), sql.Int64),
			expression.NewLiteral(int64(5), sql.Int64),
			expression.NewLiteral(int64(1), sql.Int64),
		)
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.IsType(float64(1), v)
		require.Equal(float64(1), v)
	})

	t.Run("float mixed", func(t *testing.T) {
		require := require.New(t)
		f, err := NewLeast(
			expression.NewLiteral(float64(10.0), sql.Float64),
			expression.NewLiteral(int(5), sql.Int64),
			expression.NewLiteral(int(1), sql.Int64),
		)
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.IsType(float64(1), v)
		require.Equal(float64(1.0), v)
	})

	t.Run("all strings", func(t *testing.T) {
		require := require.New(t)
		f, err := NewLeast(
			expression.NewLiteral("aaa", sql.Text),
			expression.NewLiteral("bbb", sql.Text),
			expression.NewLiteral("9999", sql.Text),
		)
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.IsType("", v)
		require.Equal("9999", v)
	})

	t.Run("all strings and empty", func(t *testing.T) {
		require := require.New(t)
		f, err := NewLeast(
			expression.NewLiteral("aaa", sql.Text),
			expression.NewLiteral("bbb", sql.Text),
			expression.NewLiteral("9999", sql.Text),
			expression.NewLiteral("", sql.Text),
		)
		require.NoError(err)

		v, err := f.Eval(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.IsType("", v)
		require.Equal("", v)
	})
}
