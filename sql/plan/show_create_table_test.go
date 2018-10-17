package plan

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/mem"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

func TestShowCreateTable(t *testing.T) {
	var require = require.New(t)

	db := mem.NewDatabase("testdb")

	table := mem.NewTable(
		"test-table",
		sql.Schema{
			&sql.Column{Name: "baz", Type: sql.Text, Default: "", Nullable: false},
			&sql.Column{Name: "zab", Type: sql.Int32, Default: int32(0), Nullable: true},
			&sql.Column{Name: "bza", Type: sql.Int64, Default: int64(0), Nullable: true},
		})

	db.AddTable(table.Name(), table)

	cat := sql.NewCatalog()
	cat.AddDatabase(db)

	showCreateTable := NewShowCreateTable(db.Name(), cat, table.Name())

	ctx := sql.NewEmptyContext()
	rowIter, _ := showCreateTable.RowIter(ctx)

	row, err := rowIter.Next()

	require.Nil(err)

	expected := sql.NewRow(
		table.Name(),
		"CREATE TABLE `test-table` (`baz` TEXT NOT NULL,\n"+
			"`zab` INT32 DEFAULT 0,\n"+
			"`bza` INT64 DEFAULT 0) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
	)

	require.Equal(expected, row)
}
