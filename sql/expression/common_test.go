package expression

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

func eval(t *testing.T, e sql.Expression, row sql.Row) interface{} {
	t.Helper()
	v, err := e.Eval(sql.NewEmptyContext(), row)
	require.NoError(t, err)
	return v
}

func TestIsUnary(t *testing.T) {
	require := require.New(t)
	require.True(IsUnary(NewNot(nil)))
	require.False(IsUnary(NewAnd(nil, nil)))
}

func TestIsBinary(t *testing.T) {
	require := require.New(t)
	require.False(IsBinary(NewNot(nil)))
	require.True(IsBinary(NewAnd(nil, nil)))
}
