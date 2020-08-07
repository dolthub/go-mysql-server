package plan

import (
	"testing"

	"github.com/stretchr/testify/require"
	"vitess.io/vitess/go/sqltypes"

	"github.com/liquidata-inc/go-mysql-server/memory"
	"github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/liquidata-inc/go-mysql-server/sql/expression"
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

	ctx := sql.NewEmptyContext()
	db.AddTable(table.Name(), table)

	cat := sql.NewCatalog()
	cat.AddDatabase(db)

	showCreateTable := NewShowCreateTable(NewResolvedTable(table), false)

	rowIter, _ := showCreateTable.RowIter(ctx, nil)

	row, err := rowIter.Next()

	require.NoError(err)

	expected := sql.NewRow(
		table.Name(),
		"CREATE TABLE `test-table` (\n  `baz` text NOT NULL,\n"+
			"  `zab` int DEFAULT 0,\n"+
			"  `bza` bigint unsigned DEFAULT 0 COMMENT 'hello',\n"+
			"  `foo` varchar(123),\n"+
			"  `pok` char(123),\n"+
			"  PRIMARY KEY (`baz`,`zab`)\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
	)

	require.Equal(expected, row)

	showCreateTable = NewShowCreateTable(NewResolvedTable(table), true)

	ctx = sql.NewEmptyContext()
	rowIter, _ = showCreateTable.RowIter(ctx, nil)

	_, err = rowIter.Next()
	require.Error(err)
	require.True(ErrNotView.Is(err), "wrong error kind")
}

func TestShowCreateTableWithIndexAndForeignKeys(t *testing.T) {
	var require = require.New(t)

	db := memory.NewDatabase("testdb")

	table := memory.NewTable(
		"test-table",
		sql.Schema{
			&sql.Column{Name: "baz", Source: "test-table", Type: sql.Text, Default: "", Nullable: false, PrimaryKey: true},
			&sql.Column{Name: "zab", Source: "test-table", Type: sql.Int32, Default: int32(0), Nullable: true, PrimaryKey: true},
			&sql.Column{Name: "bza", Source: "test-table", Type: sql.Uint64, Default: uint64(0), Nullable: true, Comment: "hello"},
			&sql.Column{Name: "foo", Source: "test-table", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 123), Default: "", Nullable: true},
			&sql.Column{Name: "pok", Source: "test-table", Type: sql.MustCreateStringWithDefaults(sqltypes.Char, 123), Default: "", Nullable: true},
		})

	ctx := sql.NewEmptyContext()
	require.NoError(table.CreateForeignKey(ctx, "fk1", []string{"baz", "zab"}, "otherTable", []string{"a", "b"}, sql.ForeignKeyReferenceOption_DefaultAction, sql.ForeignKeyReferenceOption_Cascade))
	require.NoError(table.CreateForeignKey(ctx, "fk2", []string{"foo"}, "otherTable", []string{"b"}, sql.ForeignKeyReferenceOption_Restrict, sql.ForeignKeyReferenceOption_DefaultAction))
	require.NoError(table.CreateForeignKey(ctx, "fk3", []string{"bza"}, "otherTable", []string{"c"}, sql.ForeignKeyReferenceOption_DefaultAction, sql.ForeignKeyReferenceOption_DefaultAction))

	db.AddTable(table.Name(), table)

	cat := sql.NewCatalog()
	cat.AddDatabase(db)

	showCreateTable := NewShowCreateTable(NewResolvedTable(table), false)
	// This mimics what happens during analysis (indexes get filled in for the table)
	showCreateTable.(*ShowCreateTable).Indexes = []sql.Index{
		&mockIndex{
			db:    "testdb",
			table: "test-table",
			id:    "qux",
			exprs: []sql.Expression{
				expression.NewGetFieldWithTable(3, sql.Int64, "test-table", "foo", true),
			},
			unique: true,
		},
		&mockIndex{
			db:    "testdb",
			table: "test-table",
			id:    "zug",
			exprs: []sql.Expression{
				expression.NewGetFieldWithTable(4, sql.Int64, "test-table", "pok", true),
				expression.NewGetFieldWithTable(3, sql.Int64, "test-table", "foo", true),
			},
		},
	}

	rowIter, _ := showCreateTable.RowIter(ctx, nil)

	row, err := rowIter.Next()

	require.NoError(err)

	expected := sql.NewRow(
		table.Name(),
		"CREATE TABLE `test-table` (\n  `baz` text NOT NULL,\n"+
			"  `zab` int DEFAULT 0,\n"+
			"  `bza` bigint unsigned DEFAULT 0 COMMENT 'hello',\n"+
			"  `foo` varchar(123),\n"+
			"  `pok` char(123),\n"+
			"  PRIMARY KEY (`baz`,`zab`),\n"+
			"  UNIQUE KEY `qux` (`foo`),\n"+
			"  KEY `zug` (`pok`,`foo`),\n"+
			"  CONSTRAINT `fk1` FOREIGN KEY (`baz`,`zab`) REFERENCES `otherTable` (`a`,`b`) ON DELETE CASCADE,\n"+
			"  CONSTRAINT `fk2` FOREIGN KEY (`foo`) REFERENCES `otherTable` (`b`) ON UPDATE RESTRICT,\n"+
			"  CONSTRAINT `fk3` FOREIGN KEY (`bza`) REFERENCES `otherTable` (`c`)\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
	)

	require.Equal(expected, row)
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

	showCreateTable := NewShowCreateTable(
		NewSubqueryAlias("myView", "select * from `test-table`", NewResolvedTable(table)),
		true,
	)

	ctx := sql.NewEmptyContext()
	rowIter, _ := showCreateTable.RowIter(ctx, nil)

	row, err := rowIter.Next()

	require.Nil(err)

	expected := sql.NewRow(
		"myView",
		"CREATE VIEW `myView` AS select * from `test-table`",
	)

	require.Equal(expected, row)
}
