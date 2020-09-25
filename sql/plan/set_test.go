package plan

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

func TestSet(t *testing.T) {
	require := require.New(t)

	ctx := sql.NewContext(context.Background(), sql.WithSession(sql.NewBaseSession()))

	s := NewSet(
		[]sql.Expression{
			expression.NewSetField(expression.NewSystemVar("foo", sql.LongText), expression.NewLiteral("bar", sql.LongText)),
			expression.NewSetField(expression.NewSystemVar("baz", sql.Int64), expression.NewLiteral(int64(1), sql.Int64)),
		},
	)

	_, err := s.RowIter(ctx, nil)
	require.NoError(err)

	typ, v := ctx.Get("foo")
	require.Equal(sql.LongText, typ)
	require.Equal("bar", v)

	typ, v = ctx.Get("baz")
	require.Equal(sql.Int64, typ)
	require.Equal(int64(1), v)
}
