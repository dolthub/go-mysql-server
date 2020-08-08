package plan

import (
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/liquidata-inc/go-mysql-server/memory"
	"github.com/liquidata-inc/go-mysql-server/sql"
)

func TestShowTables(t *testing.T) {
	require := require.New(t)
	ctx := sql.NewEmptyContext()

	unresolvedShowTables := NewShowTables(sql.UnresolvedDatabase(""), false, nil)

	require.False(unresolvedShowTables.Resolved())
	require.Nil(unresolvedShowTables.Children())

	db := memory.NewDatabase("test")
	db.AddTable("test1", memory.NewTable("test1", nil))
	db.AddTable("test2", memory.NewTable("test2", nil))
	db.AddTable("test3", memory.NewTable("test3", nil))

	resolvedShowTables := NewShowTables(db, false, nil)
	require.True(resolvedShowTables.Resolved())
	require.Nil(resolvedShowTables.Children())

	iter, err := resolvedShowTables.RowIter(ctx, nil)
	require.NoError(err)

	res, err := iter.Next()
	require.NoError(err)
	require.Equal("test1", res[0])

	res, err = iter.Next()
	require.NoError(err)
	require.Equal("test2", res[0])

	res, err = iter.Next()
	require.NoError(err)
	require.Equal("test3", res[0])

	_, err = iter.Next()
	require.Equal(io.EOF, err)
}
