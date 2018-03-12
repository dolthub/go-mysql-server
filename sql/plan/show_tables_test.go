package plan

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/mem"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

func TestShowTables(t *testing.T) {
	require := require.New(t)
	session := sql.NewBaseSession(context.TODO())

	unresolvedShowTables := NewShowTables(&sql.UnresolvedDatabase{})

	require.False(unresolvedShowTables.Resolved())
	require.Nil(unresolvedShowTables.Children())

	db := mem.NewDatabase("test")

	memDb, ok := db.(*mem.Database)

	require.True(ok)

	memDb.AddTable("test1", mem.NewTable("test1", nil))
	memDb.AddTable("test2", mem.NewTable("test2", nil))
	memDb.AddTable("test3", mem.NewTable("test3", nil))

	resolvedShowTables := NewShowTables(db)
	require.True(resolvedShowTables.Resolved())
	require.Nil(resolvedShowTables.Children())

	iter, err := resolvedShowTables.RowIter(session)
	require.Nil(err)

	res, err := iter.Next()
	require.Nil(err)
	require.Equal("test1", res[0])

	res, err = iter.Next()
	require.Nil(err)
	require.Equal("test2", res[0])

	res, err = iter.Next()
	require.Nil(err)
	require.Equal("test3", res[0])

	_, err = iter.Next()
	require.Equal(io.EOF, err)
}
