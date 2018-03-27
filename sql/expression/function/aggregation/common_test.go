package aggregation

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

func eval(t *testing.T, e sql.Expression, row sql.Row) interface{} {
	ctx := sql.NewEmptyContext()

	t.Helper()
	v, err := e.Eval(ctx, row)
	require.NoError(t, err)
	return v
}
