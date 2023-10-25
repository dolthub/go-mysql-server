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

package rowexec

import (
	"testing"

	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	. "github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/planbuilder"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestShowCreateTable(t *testing.T) {
	var require = require.New(t)

	db := memory.NewDatabase("test")
	pro := memory.NewDBProvider(db)
	ctx := newContext(pro)

	schema := sql.Schema{
		&sql.Column{Name: "baz", Type: types.Text, Default: nil, Nullable: false, PrimaryKey: true},
		&sql.Column{Name: "z`ab", Type: types.Int32, Default: planbuilder.MustStringToColumnDefaultValue(ctx, "0", types.Int32, true), Nullable: true, PrimaryKey: true},
		&sql.Column{Name: "bza", Type: types.Uint64, Default: planbuilder.MustStringToColumnDefaultValue(ctx, "0", types.Uint64, true), Nullable: true, Comment: "hello"},
		&sql.Column{Name: "foo", Type: types.MustCreateStringWithDefaults(sqltypes.VarChar, 123), Default: nil, Nullable: true},
		&sql.Column{Name: "pok", Type: types.MustCreateStringWithDefaults(sqltypes.Char, 123), Default: nil, Nullable: true},
	}
	table := memory.NewTable(db.BaseDatabase, "test-table", sql.NewPrimaryKeySchema(schema), nil)

	showCreateTable, err := NewShowCreateTable(NewResolvedTable(table, nil, nil), false).WithTargetSchema(schema)
	require.NoError(err)

	rowIter, _ := DefaultBuilder.Build(ctx, showCreateTable, nil)

	row, err := rowIter.Next(ctx)

	require.NoError(err)

	expected := sql.NewRow(
		table.Name(),
		"CREATE TABLE `test-table` (\n  `baz` text NOT NULL,\n"+
			"  `z``ab` int DEFAULT '0',\n"+
			"  `bza` bigint unsigned DEFAULT '0' COMMENT 'hello',\n"+
			"  `foo` varchar(123),\n"+
			"  `pok` char(123),\n"+
			"  PRIMARY KEY (`baz`,`z``ab`)\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin",
	)

	require.Equal(expected, row)

	showCreateTable = NewShowCreateTable(NewResolvedTable(table, nil, nil), true)

	ctx = sql.NewEmptyContext()
	rowIter, _ = DefaultBuilder.Build(ctx, showCreateTable, nil)

	_, err = rowIter.Next(ctx)
	require.Error(err)
	require.True(ErrNotView.Is(err), "wrong error kind")
}

func TestShowCreateTableWithNoPrimaryKey(t *testing.T) {
	var require = require.New(t)

	db := memory.NewDatabase("test")
	pro := memory.NewDBProvider(db)
	ctx := newContext(pro)

	schema := sql.Schema{
		&sql.Column{Name: "baz", Type: types.Text, Default: nil, Nullable: false},
		&sql.Column{Name: "bza", Type: types.Uint64, Default: planbuilder.MustStringToColumnDefaultValue(ctx, "0", types.Uint64, true), Nullable: true, Comment: "hello"},
		&sql.Column{Name: "foo", Type: types.MustCreateStringWithDefaults(sqltypes.VarChar, 123), Default: nil, Nullable: true},
		&sql.Column{Name: "pok", Type: types.MustCreateStringWithDefaults(sqltypes.Char, 123), Default: nil, Nullable: true},
		&sql.Column{Name: "zab", Type: types.Int32, Default: planbuilder.MustStringToColumnDefaultValue(ctx, "0", types.Int32, true), Nullable: true},
	}
	pkSchema := sql.NewPrimaryKeySchema(schema)
	table := memory.NewTable(db.BaseDatabase, "test_table", pkSchema, nil)

	showCreateTable, err := NewShowCreateTable(NewResolvedTable(table, nil, nil), false).WithTargetSchema(schema)
	require.NoError(err)
	showCreateTable, err = showCreateTable.(*ShowCreateTable).WithPrimaryKeySchema(pkSchema)
	require.NoError(err)

	rowIter, _ := DefaultBuilder.Build(ctx, showCreateTable, nil)

	row, err := rowIter.Next(ctx)

	require.NoError(err)

	expected := sql.NewRow(
		table.Name(),
		"CREATE TABLE `test_table` (\n  `baz` text NOT NULL,\n"+
			"  `bza` bigint unsigned DEFAULT '0' COMMENT 'hello',\n"+
			"  `foo` varchar(123),\n"+
			"  `pok` char(123),\n"+
			"  `zab` int DEFAULT '0'\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin",
	)

	require.Equal(expected, row)
}

func TestShowCreateTableWithPrimaryKey(t *testing.T) {
	var require = require.New(t)

	db := memory.NewDatabase("test")
	pro := memory.NewDBProvider(db)
	ctx := newContext(pro)

	schema := sql.Schema{
		&sql.Column{Name: "baz", Type: types.Text, Default: nil, Nullable: false, PrimaryKey: true},
		&sql.Column{Name: "bza", Type: types.Uint64, Default: planbuilder.MustStringToColumnDefaultValue(ctx, "0", types.Uint64, true), Nullable: true, Comment: "hello"},
		&sql.Column{Name: "foo", Type: types.MustCreateStringWithDefaults(sqltypes.VarChar, 123), Default: nil, Nullable: true},
		&sql.Column{Name: "pok", Type: types.MustCreateStringWithDefaults(sqltypes.Char, 123), Default: nil, Nullable: true},
		&sql.Column{Name: "zab", Type: types.Int32, Default: planbuilder.MustStringToColumnDefaultValue(ctx, "0", types.Int32, true), Nullable: true, PrimaryKey: true},
	}
	pkSchema := sql.NewPrimaryKeySchema(schema, 4, 0)
	table := memory.NewTable(db.BaseDatabase, "test-table", pkSchema, nil)

	showCreateTable, err := NewShowCreateTable(NewResolvedTable(table, nil, nil), false).WithTargetSchema(schema)
	require.NoError(err)
	showCreateTable, err = showCreateTable.(*ShowCreateTable).WithPrimaryKeySchema(pkSchema)
	require.NoError(err)

	rowIter, _ := DefaultBuilder.Build(ctx, showCreateTable, nil)

	row, err := rowIter.Next(ctx)

	require.NoError(err)

	expected := sql.NewRow(
		table.Name(),
		"CREATE TABLE `test-table` (\n  `baz` text NOT NULL,\n"+
			"  `bza` bigint unsigned DEFAULT '0' COMMENT 'hello',\n"+
			"  `foo` varchar(123),\n"+
			"  `pok` char(123),\n"+
			"  `zab` int DEFAULT '0',\n"+
			"  PRIMARY KEY (`zab`,`baz`)\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin",
	)

	require.Equal(expected, row)
}

func TestShowCreateTableWithIndexAndForeignKeysAndChecks(t *testing.T) {
	var require = require.New(t)

	db := memory.NewDatabase("test")
	pro := memory.NewDBProvider(db)
	ctx := newContext(pro)

	schema := sql.Schema{
		&sql.Column{Name: "baz", Source: "test-table", Type: types.Text, Default: nil, Nullable: false, PrimaryKey: true},
		&sql.Column{Name: "zab", Source: "test-table", Type: types.Int32, Default: planbuilder.MustStringToColumnDefaultValue(ctx, "0", types.Int32, true), Nullable: true, PrimaryKey: true},
		&sql.Column{Name: "bza", Source: "test-table", Type: types.Uint64, Default: planbuilder.MustStringToColumnDefaultValue(ctx, "0", types.Uint64, true), Nullable: true, Comment: "hello"},
		&sql.Column{Name: "foo", Source: "test-table", Type: types.MustCreateStringWithDefaults(sqltypes.VarChar, 123), Default: nil, Nullable: true},
		&sql.Column{Name: "pok", Source: "test-table", Type: types.MustCreateStringWithDefaults(sqltypes.Char, 123), Default: nil, Nullable: true},
	}
	table := memory.NewTable(db.BaseDatabase, "test-table", sql.NewPrimaryKeySchema(schema), &memory.ForeignKeyCollection{})

	require.NoError(table.AddForeignKey(ctx, sql.ForeignKeyConstraint{
		Name:           "fk1",
		Database:       "testdb",
		Table:          table.Name(),
		Columns:        []string{"baz", "zab"},
		ParentDatabase: "testdb",
		ParentTable:    "otherTable",
		ParentColumns:  []string{"a", "b"},
		OnUpdate:       sql.ForeignKeyReferentialAction_DefaultAction,
		OnDelete:       sql.ForeignKeyReferentialAction_Cascade,
		IsResolved:     true,
	}))
	require.NoError(table.AddForeignKey(ctx, sql.ForeignKeyConstraint{
		Name:           "fk2",
		Database:       "testdb",
		Table:          table.Name(),
		Columns:        []string{"foo"},
		ParentDatabase: "testdb",
		ParentTable:    "otherTable",
		ParentColumns:  []string{"b"},
		OnUpdate:       sql.ForeignKeyReferentialAction_Restrict,
		OnDelete:       sql.ForeignKeyReferentialAction_DefaultAction,
		IsResolved:     true,
	}))
	require.NoError(table.AddForeignKey(ctx, sql.ForeignKeyConstraint{
		Name:           "fk3",
		Database:       "testdb",
		Table:          table.Name(),
		Columns:        []string{"bza"},
		ParentDatabase: "testdb",
		ParentTable:    "otherTable",
		ParentColumns:  []string{"c"},
		OnUpdate:       sql.ForeignKeyReferentialAction_DefaultAction,
		OnDelete:       sql.ForeignKeyReferentialAction_DefaultAction,
		IsResolved:     true,
	}))

	showCreateTable, err := NewShowCreateTable(NewResolvedTable(table, nil, nil), false).WithTargetSchema(schema)
	require.NoError(err)

	// This mimics what happens during analysis (indexes get filled in for the table)
	showCreateTable.(*ShowCreateTable).Indexes = []sql.Index{
		&mockIndex{
			db:    "testdb",
			table: "test-table",
			id:    "`qux`",
			exprs: []sql.Expression{
				expression.NewGetFieldWithTable(3, types.Int64, "db", "test-table", "foo", true),
			},
			unique: true,
		},
		&mockIndex{
			db:    "testdb",
			table: "test-table",
			id:    "zug",
			exprs: []sql.Expression{
				expression.NewGetFieldWithTable(4, types.Int64, "db", "test-table", "pok", true),
				expression.NewGetFieldWithTable(3, types.Int64, "db", "test-table", "foo", true),
			},
			comment: "test comment",
		},
	}

	showCreateTable = showCreateTable.(*ShowCreateTable).WithChecks(sql.CheckConstraints{
		{
			Name:     "mycheck",
			Expr:     expression.NewGreaterThan(expression.NewUnresolvedColumn("`zab`"), expression.NewLiteral(int8(0), types.Int8)),
			Enforced: true,
		},
	})

	rowIter, _ := DefaultBuilder.Build(ctx, showCreateTable, nil)

	row, err := rowIter.Next(ctx)

	require.NoError(err)

	expected := sql.NewRow(
		table.Name(),
		"CREATE TABLE `test-table` (\n  `baz` text NOT NULL,\n"+
			"  `zab` int DEFAULT '0',\n"+
			"  `bza` bigint unsigned DEFAULT '0' COMMENT 'hello',\n"+
			"  `foo` varchar(123),\n"+
			"  `pok` char(123),\n"+
			"  PRIMARY KEY (`baz`,`zab`),\n"+
			"  UNIQUE KEY ```qux``` (`foo`),\n"+
			"  KEY `zug` (`pok`,`foo`) COMMENT 'test comment',\n"+
			"  CONSTRAINT `fk1` FOREIGN KEY (`baz`,`zab`) REFERENCES `otherTable` (`a`,`b`) ON DELETE CASCADE,\n"+
			"  CONSTRAINT `fk2` FOREIGN KEY (`foo`) REFERENCES `otherTable` (`b`) ON UPDATE RESTRICT,\n"+
			"  CONSTRAINT `fk3` FOREIGN KEY (`bza`) REFERENCES `otherTable` (`c`),\n"+
			"  CONSTRAINT `mycheck` CHECK ((`zab` > 0))\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin",
	)

	require.Equal(expected, row)
}

func TestShowCreateView(t *testing.T) {
	var require = require.New(t)

	db := memory.NewDatabase("testdb")
	pro := memory.NewDBProvider(db)
	ctx := newContext(pro)

	table := memory.NewTable(db.BaseDatabase, "test-table", sql.NewPrimaryKeySchema(sql.Schema{
		&sql.Column{Name: "baz", Type: types.Text, Default: nil, Nullable: false, PrimaryKey: true},
		&sql.Column{Name: "zab", Type: types.Int32, Default: planbuilder.MustStringToColumnDefaultValue(ctx, "0", types.Int32, true), Nullable: true, PrimaryKey: true},
		&sql.Column{Name: "bza", Type: types.Uint64, Default: planbuilder.MustStringToColumnDefaultValue(ctx, "0", types.Uint64, true), Nullable: true, Comment: "hello"},
		&sql.Column{Name: "foo", Type: types.MustCreateStringWithDefaults(sqltypes.VarChar, 123), Default: nil, Nullable: true},
		&sql.Column{Name: "pok", Type: types.MustCreateStringWithDefaults(sqltypes.Char, 123), Default: nil, Nullable: true},
	}), nil)

	showCreateTable := NewShowCreateTable(
		NewSubqueryAlias("myView", "select * from `test-table`", NewResolvedTable(table, nil, nil)),
		true,
	)

	rowIter, _ := DefaultBuilder.Build(ctx, showCreateTable, nil)

	row, err := rowIter.Next(ctx)

	require.Nil(err)

	expected := sql.NewRow(
		"myView",
		"CREATE VIEW `myView` AS select * from `test-table`",
		"utf8mb4",
		"utf8mb4_0900_bin",
	)

	require.Equal(expected, row)
}
