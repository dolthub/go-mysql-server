package plan

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
)

func TestSet(t *testing.T) {
	require := require.New(t)

	ctx := sql.NewContext(context.Background(), sql.WithSession(sql.NewBaseSession()))

	s := NewSet(
		SetVariable{"foo", expression.NewLiteral("bar", sql.Text)},
		SetVariable{"@@baz", expression.NewLiteral(int64(1), sql.Int64)},
	)

	_, err := s.RowIter(ctx)
	require.NoError(err)

	typ, v := ctx.Get("foo")
	require.Equal(sql.Text, typ)
	require.Equal("bar", v)

	typ, v = ctx.Get("baz")
	require.Equal(sql.Int64, typ)
	require.Equal(int64(1), v)
}

func TestSetDesfault(t *testing.T) {
	require := require.New(t)

	ctx := sql.NewContext(context.Background(), sql.WithSession(sql.NewBaseSession()))

	s := NewSet(
		SetVariable{"auto_increment_increment", expression.NewLiteral(int64(123), sql.Int64)},
		SetVariable{"@@sql_select_limit", expression.NewLiteral(int64(1), sql.Int64)},
	)

	_, err := s.RowIter(ctx)
	require.NoError(err)

	typ, v := ctx.Get("auto_increment_increment")
	require.Equal(sql.Int64, typ)
	require.Equal(int64(123), v)

	typ, v = ctx.Get("sql_select_limit")
	require.Equal(sql.Int64, typ)
	require.Equal(int64(1), v)

	s = NewSet(
		SetVariable{"auto_increment_increment", expression.NewDefaultColumn("")},
		SetVariable{"@@sql_select_limit", expression.NewDefaultColumn("")},
	)

	_, err = s.RowIter(ctx)
	require.NoError(err)

	defaults := sql.DefaultSessionConfig()

	typ, v = ctx.Get("auto_increment_increment")
	require.Equal(defaults["auto_increment_increment"].Typ, typ)
	require.Equal(defaults["auto_increment_increment"].Value, v)

	typ, v = ctx.Get("sql_select_limit")
	require.Equal(defaults["sql_select_limit"].Typ, typ)
	require.Equal(defaults["sql_select_limit"].Value, v)

}
