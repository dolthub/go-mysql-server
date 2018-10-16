package plan

import (
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/mem"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

func TestShowTables(t *testing.T) {
	require := require.New(t)
	ctx := sql.NewEmptyContext()

	unresolvedShowTables := NewShowTables(sql.UnresolvedDatabase(""))

	require.False(unresolvedShowTables.Resolved())
	require.Nil(unresolvedShowTables.Children())

	db := mem.NewDatabase("test")
	db.AddTable("test1", mem.NewTable("test1", nil))
	db.AddTable("test2", mem.NewTable("test2", nil))
	db.AddTable("test3", mem.NewTable("test3", nil))

	resolvedShowTables := NewShowTables(db)
	require.True(resolvedShowTables.Resolved())
	require.Nil(resolvedShowTables.Children())

	iter, err := resolvedShowTables.RowIter(ctx)
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
