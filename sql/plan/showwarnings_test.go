package plan

import (
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

func TestShowWarnings(t *testing.T) {
	require := require.New(t)

	ctx := sql.NewEmptyContext()
	ctx.Session.Warn(&sql.Warning{"l1", "w1", 1})
	ctx.Session.Warn(&sql.Warning{"l2", "w2", 2})
	ctx.Session.Warn(&sql.Warning{"l4", "w3", 3})

	sw := ShowWarnings(ctx.Session.Warnings())
	require.True(sw.Resolved())

	it, err := sw.RowIter(ctx)
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
