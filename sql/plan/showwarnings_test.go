package plan

import (
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/liquidata-inc/go-mysql-server/sql"
)

func TestShowWarnings(t *testing.T) {
	require := require.New(t)

	ctx := sql.NewEmptyContext()
	ctx.Session.Warn(&sql.Warning{Level: "l1", Message: "w1", Code: 1})
	ctx.Session.Warn(&sql.Warning{Level: "l2", Message: "w2", Code: 2})
	ctx.Session.Warn(&sql.Warning{Level: "l4", Message: "w3", Code: 3})

	sw := ShowWarnings(ctx.Session.Warnings())
	require.True(sw.Resolved())

	it, err := sw.RowIter(ctx, nil)
	require.NoError(err)

	n := 3
	for row, err := it.Next(); err == nil; row, err = it.Next() {
		level := row[0].(string)
		code := row[1].(int)
		message := row[2].(string)

		t.Logf("level: %s\tcode: %v\tmessage: %s\n", level, code, message)

		require.Equal(n, code)
		n--
	}
	if err != io.EOF {
		require.NoError(err)
	}
	require.NoError(it.Close())
}
