package aggregation

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

func eval(t *testing.T, e sql.Expression, row sql.Row) interface{} {
	session := sql.NewBaseSession(context.TODO())

	t.Helper()
	v, err := e.Eval(session, row)
	require.NoError(t, err)
	return v
}
