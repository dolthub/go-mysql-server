package plan

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/mem"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
)

func TestShowIndexes(t *testing.T) {
	var require = require.New(t)

	unresolved := NewShowIndexes(&sql.UnresolvedDatabase{}, "table-test", nil)
	require.False(unresolved.Resolved())
	require.Nil(unresolved.Children())

	db := mem.NewDatabase("test")
	db.AddTable("test1", mem.NewTable("test1", nil))
	db.AddTable("test2", mem.NewTable("test2", nil))
	db.AddTable("test3", mem.NewTable("test3", nil))

	r := sql.NewIndexRegistry()
	for table := range db.Tables() {
		idx := &mockIndex{
			db:    "test",
			table: table,
			id:    "idx_" + table + "_foo",
			exprs: []sql.Expression{
				expression.NewGetFieldWithTable(0, sql.Int32, table, "foo", false),
			},
		}

		created, ready, err := r.AddIndex(idx)
		require.NoError(err)
		close(created)
		<-ready

		showIdxs := NewShowIndexes(db, table, r)

		ctx := sql.NewEmptyContext()
		rowIter, err := showIdxs.RowIter(ctx)
		require.NoError(err)

		rows, err := sql.RowIterToRows(rowIter)
		require.NoError(err)
		require.Len(rows, 1)

		require.Equal(
			sql.NewRow(
				table, int32(1), idx.ID(),
				int32(0), "NULL", "",
				int64(0), int64(0), "",
				"", idx.Driver(), "",
				"", "YES", "NULL",
			),
			rows[0],
		)
	}
}
