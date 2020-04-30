package function

import (
	"testing"

	"github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/stretchr/testify/require"
)

func eval(t *testing.T, e sql.Expression, row sql.Row) interface{} {
	ctx := sql.NewEmptyContext()

	t.Helper()
	v, err := e.Eval(ctx, row)
	require.NoError(t, err)
	return v
}
