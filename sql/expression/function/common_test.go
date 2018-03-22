package function

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

func eval(t *testing.T, e sql.Expression, row sql.Row) interface{} {
	ctx := sql.NewContext(context.TODO(), sql.NewBaseSession())

	t.Helper()
	v, err := e.Eval(ctx, row)
	require.NoError(t, err)
	return v
}
