package aggregation

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
)

func eval(t *testing.T, e sql.Expression, row sql.Row) interface{} {
	ctx := sql.NewEmptyContext()

	t.Helper()
	v, err := e.Eval(ctx, row)
	require.NoError(t, err)
	return v
}

func aggregate(t *testing.T, agg sql.Aggregation, rows ...sql.Row) interface{} {
	t.Helper()

	ctx := sql.NewEmptyContext()
	buf := agg.NewBuffer()
	for _, row := range rows {
		require.NoError(t, agg.Update(ctx, buf, row))
	}

	v, err := agg.Eval(ctx, buf)
	require.NoError(t, err)
	return v
}
