// Copyright 2020-2021 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan_test

import (
	"testing"

	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/parse"
	. "github.com/dolthub/go-mysql-server/sql/plan"
)

func TestShowCreateTable(t *testing.T) {
	var require = require.New(t)
	ctx := sql.NewEmptyContext()

	db := memory.NewDatabase("testdb")

	table := memory.NewTable(
		"test-table",
		sql.Schema{
			&sql.Column{Name: "baz", Type: sql.Text, Default: nil, Nullable: false, PrimaryKey: true},
			&sql.Column{Name: "zab", Type: sql.Int32, Default: parse.MustStringToColumnDefaultValue(ctx, "0", sql.Int32, true), Nullable: true, PrimaryKey: true},
			&sql.Column{Name: "bza", Type: sql.Uint64, Default: parse.MustStringToColumnDefaultValue(ctx, "0", sql.Uint64, true), Nullable: true, Comment: "hello"},
			&sql.Column{Name: "foo", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 123), Default: nil, Nullable: true},
			&sql.Column{Name: "pok", Type: sql.MustCreateStringWithDefaults(sqltypes.Char, 123), Default: nil, Nullable: true},
		})

	db.AddTable(table.Name(), table)

	cat := sql.NewCatalog()
	cat.AddDatabase(db)

	showCreateTable := NewShowCreateTable(NewResolvedTable(table, nil, nil), false)

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

	showCreateTable = NewShowCreateTable(NewResolvedTable(table, nil, nil), true)

	ctx = sql.NewEmptyContext()
	rowIter, _ = showCreateTable.RowIter(ctx, nil)

	_, err = rowIter.Next()
	require.Error(err)
	require.True(ErrNotView.Is(err), "wrong error kind")
}

func TestShowCreateTableWithIndexAndForeignKeysAndChecks(t *testing.T) {
	var require = require.New(t)
	ctx := sql.NewEmptyContext()

	db := memory.NewDatabase("testdb")

	table := memory.NewTable(
		"test-table",
		sql.Schema{
			&sql.Column{Name: "baz", Source: "test-table", Type: sql.Text, Default: nil, Nullable: false, PrimaryKey: true},
			&sql.Column{Name: "zab", Source: "test-table", Type: sql.Int32, Default: parse.MustStringToColumnDefaultValue(ctx, "0", sql.Int32, true), Nullable: true, PrimaryKey: true},
			&sql.Column{Name: "bza", Source: "test-table", Type: sql.Uint64, Default: parse.MustStringToColumnDefaultValue(ctx, "0", sql.Uint64, true), Nullable: true, Comment: "hello"},
			&sql.Column{Name: "foo", Source: "test-table", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 123), Default: nil, Nullable: true},
			&sql.Column{Name: "pok", Source: "test-table", Type: sql.MustCreateStringWithDefaults(sqltypes.Char, 123), Default: nil, Nullable: true},
		})

	require.NoError(table.CreateForeignKey(ctx, "fk1", []string{"baz", "zab"}, "otherTable", []string{"a", "b"}, sql.ForeignKeyReferenceOption_DefaultAction, sql.ForeignKeyReferenceOption_Cascade))
	require.NoError(table.CreateForeignKey(ctx, "fk2", []string{"foo"}, "otherTable", []string{"b"}, sql.ForeignKeyReferenceOption_Restrict, sql.ForeignKeyReferenceOption_DefaultAction))
	require.NoError(table.CreateForeignKey(ctx, "fk3", []string{"bza"}, "otherTable", []string{"c"}, sql.ForeignKeyReferenceOption_DefaultAction, sql.ForeignKeyReferenceOption_DefaultAction))

	db.AddTable(table.Name(), table)

	cat := sql.NewCatalog()
	cat.AddDatabase(db)

	showCreateTable := NewShowCreateTable(NewResolvedTable(table, nil, nil), false)
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
			comment: "test comment",
		},
	}

	showCreateTable.(*ShowCreateTable).Checks = sql.CheckConstraints{
		{
			Name:     "mycheck",
			Expr:     expression.NewGreaterThan(expression.NewUnresolvedColumn("`zab`"), expression.NewLiteral(int8(0), sql.Int8)),
			Enforced: true,
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
			"  KEY `zug` (`pok`,`foo`) COMMENT 'test comment',\n"+
			"  CONSTRAINT `fk1` FOREIGN KEY (`baz`,`zab`) REFERENCES `otherTable` (`a`,`b`) ON DELETE CASCADE,\n"+
			"  CONSTRAINT `fk2` FOREIGN KEY (`foo`) REFERENCES `otherTable` (`b`) ON UPDATE RESTRICT,\n"+
			"  CONSTRAINT `fk3` FOREIGN KEY (`bza`) REFERENCES `otherTable` (`c`),\n"+
			"  CONSTRAINT `mycheck` CHECK (`zab` > 0)\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
	)

	require.Equal(expected, row)
}

func TestShowCreateView(t *testing.T) {
	var require = require.New(t)
	ctx := sql.NewEmptyContext()

	db := memory.NewDatabase("testdb")

	table := memory.NewTable(
		"test-table",
		sql.Schema{
			&sql.Column{Name: "baz", Type: sql.Text, Default: nil, Nullable: false, PrimaryKey: true},
			&sql.Column{Name: "zab", Type: sql.Int32, Default: parse.MustStringToColumnDefaultValue(ctx, "0", sql.Int32, true), Nullable: true, PrimaryKey: true},
			&sql.Column{Name: "bza", Type: sql.Uint64, Default: parse.MustStringToColumnDefaultValue(ctx, "0", sql.Uint64, true), Nullable: true, Comment: "hello"},
			&sql.Column{Name: "foo", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 123), Default: nil, Nullable: true},
			&sql.Column{Name: "pok", Type: sql.MustCreateStringWithDefaults(sqltypes.Char, 123), Default: nil, Nullable: true},
		})

	db.AddTable(table.Name(), table)

	cat := sql.NewCatalog()
	cat.AddDatabase(db)

	showCreateTable := NewShowCreateTable(
		NewSubqueryAlias("myView", "select * from `test-table`", NewResolvedTable(table, nil, nil)),
		true,
	)

	rowIter, _ := showCreateTable.RowIter(ctx, nil)

	row, err := rowIter.Next()

	require.Nil(err)

	expected := sql.NewRow(
		"myView",
		"CREATE VIEW `myView` AS select * from `test-table`",
	)

	require.Equal(expected, row)
}
