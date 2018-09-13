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
