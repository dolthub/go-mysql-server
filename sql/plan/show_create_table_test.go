package plan

import (
	"testing"
	"vitess.io/vitess/go/sqltypes"

	"github.com/src-d/go-mysql-server/memory"
	"github.com/src-d/go-mysql-server/sql"
	"github.com/stretchr/testify/require"
)

func TestShowCreateTable(t *testing.T) {
	var require = require.New(t)

	db := memory.NewDatabase("testdb")

	table := memory.NewTable(
		"test-table",
		sql.Schema{
			&sql.Column{Name: "baz", Type: sql.Text, Default: "", Nullable: false},
			&sql.Column{Name: "zab", Type: sql.Int32, Default: int32(0), Nullable: true},
			&sql.Column{Name: "bza", Type: sql.Uint64, Default: uint64(0), Nullable: true},
			&sql.Column{Name: "foo", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 123), Default: "", Nullable: true},
			&sql.Column{Name: "pok", Type: sql.MustCreateStringWithDefaults(sqltypes.Char, 123), Default: "", Nullable: true},
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
		"CREATE TABLE `test-table` (\n  `baz` text NOT NULL,\n"+
			"  `zab` int DEFAULT 0,\n"+
			"  `bza` bigint unsigned DEFAULT 0,\n"+
			"  `foo` varchar(123),\n"+
			"  `pok` char(123)\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
	)

	require.Equal(expected, row)
}
