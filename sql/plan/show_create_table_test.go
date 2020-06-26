package plan

import (
	"testing"

	"github.com/stretchr/testify/require"
	"vitess.io/vitess/go/sqltypes"

	"github.com/liquidata-inc/go-mysql-server/memory"
	"github.com/liquidata-inc/go-mysql-server/sql"
)

func TestShowCreateTable(t *testing.T) {
	var require = require.New(t)

	db := memory.NewDatabase("testdb")

	table := memory.NewTable(
		"test-table",
		sql.Schema{
			&sql.Column{Name: "baz", Type: sql.Text, Default: "", Nullable: false, PrimaryKey: true},
			&sql.Column{Name: "zab", Type: sql.Int32, Default: int32(0), Nullable: true, PrimaryKey: true},
			&sql.Column{Name: "bza", Type: sql.Uint64, Default: uint64(0), Nullable: true, Comment: "hello"},
			&sql.Column{Name: "foo", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 123), Default: "", Nullable: true},
			&sql.Column{Name: "pok", Type: sql.MustCreateStringWithDefaults(sqltypes.Char, 123), Default: "", Nullable: true},
		})

	db.AddTable(table.Name(), table)

	cat := sql.NewCatalog()
	cat.AddDatabase(db)

	showCreateTable := NewShowCreateTable(db.Name(), cat, NewResolvedTable(table), false)

	ctx := sql.NewEmptyContext()
	rowIter, _ := showCreateTable.RowIter(ctx)

	row, err := rowIter.Next()

	require.NoError(err)

	expected := sql.NewRow(
		table.Name(),
		"CREATE TABLE `test-table` (\n  `baz` TEXT NOT NULL,\n"+
			"  `zab` INT DEFAULT 0,\n"+
			"  `bza` BIGINT UNSIGNED DEFAULT 0 COMMENT 'hello',\n"+
			"  `foo` VARCHAR(123),\n"+
			"  `pok` CHAR(123),\n"+
			"  PRIMARY KEY (`baz`,`zab`)\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
	)

	require.Equal(expected, row)

	showCreateTable = NewShowCreateTable(db.Name(), cat, NewResolvedTable(table), true)

	ctx = sql.NewEmptyContext()
	rowIter, _ = showCreateTable.RowIter(ctx)

	_, err = rowIter.Next()
	require.Error(err)
	require.True(ErrNotView.Is(err), "wrong error kind")
}

func TestShowCreateView(t *testing.T) {
	var require = require.New(t)

	db := memory.NewDatabase("testdb")

	table := memory.NewTable(
		"test-table",
		sql.Schema{
			&sql.Column{Name: "baz", Type: sql.Text, Default: "", Nullable: false, PrimaryKey: true},
			&sql.Column{Name: "zab", Type: sql.Int32, Default: int32(0), Nullable: true, PrimaryKey: true},
			&sql.Column{Name: "bza", Type: sql.Uint64, Default: uint64(0), Nullable: true, Comment: "hello"},
			&sql.Column{Name: "foo", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 123), Default: "", Nullable: true},
			&sql.Column{Name: "pok", Type: sql.MustCreateStringWithDefaults(sqltypes.Char, 123), Default: "", Nullable: true},
		})

	db.AddTable(table.Name(), table)

	cat := sql.NewCatalog()
	cat.AddDatabase(db)

	showCreateTable := NewShowCreateTable(db.Name(), cat,
		NewSubqueryAlias("myView", "select * from `test-table`", NewResolvedTable(table)),
		true,
	)

	ctx := sql.NewEmptyContext()
	rowIter, _ := showCreateTable.RowIter(ctx)

	row, err := rowIter.Next()

	require.Nil(err)

	expected := sql.NewRow(
		"myView",
		"CREATE VIEW `myView` AS select * from `test-table`",
	)

	require.Equal(expected, row)
}
