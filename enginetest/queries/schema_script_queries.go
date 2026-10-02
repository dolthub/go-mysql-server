// Copyright 2026 Dolthub, Inc.
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

package queries

import (
	"github.com/dolthub/vitess/go/sqltypes"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// SchemaScriptTests contains self-contained schema script tests.
var SchemaScriptTests = []ScriptTest{
	{
		Name: "create table casing",
		SetUpScript: []string{
			"create table t (lower varchar(20) primary key, UPPER varchar(20), MiXeD varchar(20), un_der varchar(20), `da-sh` varchar(20));",
			"insert into t values ('a','b','c','d','e')",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `select * from t`,
				ExpectedColumns: sql.Schema{
					{
						Name: "lower",
						Type: types.MustCreateStringWithDefaults(sqltypes.VarChar, 20),
					},
					{
						Name: "UPPER",
						Type: types.MustCreateStringWithDefaults(sqltypes.VarChar, 20),
					},
					{
						Name: "MiXeD",
						Type: types.MustCreateStringWithDefaults(sqltypes.VarChar, 20),
					},
					{
						Name: "un_der",
						Type: types.MustCreateStringWithDefaults(sqltypes.VarChar, 20),
					},
					{
						Name: "da-sh",
						Type: types.MustCreateStringWithDefaults(sqltypes.VarChar, 20),
					},
				},
				Expected: []sql.Row{{"a", "b", "c", "d", "e"}},
			},
		},
	},
	{
		Name: "CREATE TABLE SELECT Queries",
		SetUpScript: []string{
			`CREATE TABLE t1 (pk int PRIMARY KEY, v1 varchar(10))`,
			`INSERT INTO t1 VALUES (1,"1"), (2,"2"), (3,"3")`,
			`CREATE TABLE t2 AS SELECT * FROM t1`,
			// `CREATE TABLE t3(v0 int) AS SELECT pk FROM t1`, // parser problems
			`CREATE TABLE t3 AS SELECT pk FROM t1`,
			`CREATE TABLE t4 AS SELECT pk, v1 FROM t1`,
			`CREATE TABLE t5 SELECT * FROM t1 ORDER BY pk LIMIT 1`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    `SELECT * FROM t2`,
				Expected: []sql.Row{{1, "1"}, {2, "2"}, {3, "3"}},
			},
			{
				Query:    `SELECT * FROM t3`,
				Expected: []sql.Row{{1}, {2}, {3}},
			},
			{
				Query:    `SELECT * FROM t4`,
				Expected: []sql.Row{{1, "1"}, {2, "2"}, {3, "3"}},
			},
			{
				Query:    `SELECT * FROM t5`,
				Expected: []sql.Row{{1, "1"}},
			},
			{
				Query: `CREATE TABLE test SELECT * FROM t1`,
				Expected: []sql.Row{{types.OkResult{
					RowsAffected: 3,
					InsertID:     0,
					Info:         nil,
				}}},
			},
		},
	},
	{
		Name: "Show create table with various keys and constraints",
		SetUpScript: []string{
			"create table t1(a int primary key, b varchar(10) not null default 'abc')",
			"alter table t1 add constraint ck1 check (b like '%abc%')",
			"create index t1b on t1(b)",
			"create table t2(c int primary key, d varchar(10))",
			"alter table t2 add constraint t2du unique (d)",
			"alter table t2 add constraint fk1 foreign key (d) references t1 (b)",
			"create table t3 (a int, b varchar(100), c datetime(6), primary key (b,a))",
			"create table t4 (a int default (floor(1)), b int default (coalesce(a, 10)))",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "show create table t1",
				Expected: []sql.Row{
					{"t1", "CREATE TABLE `t1` (\n" +
						"  `a` int NOT NULL,\n" +
						"  `b` varchar(10) NOT NULL DEFAULT 'abc',\n" +
						"  PRIMARY KEY (`a`),\n" +
						"  KEY `t1b` (`b`),\n" +
						"  CONSTRAINT `ck1` CHECK (`b` LIKE '%abc%')\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
			{
				Query: "show create table t2",
				Expected: []sql.Row{
					{"t2", "CREATE TABLE `t2` (\n" +
						"  `c` int NOT NULL,\n" +
						"  `d` varchar(10),\n" +
						"  PRIMARY KEY (`c`),\n" +
						"  UNIQUE KEY `t2du` (`d`),\n" +
						"  CONSTRAINT `fk1` FOREIGN KEY (`d`) REFERENCES `t1` (`b`)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
			{
				Query: "show create table t3",
				Expected: []sql.Row{
					{"t3", "CREATE TABLE `t3` (\n" +
						"  `a` int NOT NULL,\n" +
						"  `b` varchar(100) NOT NULL,\n" +
						"  `c` datetime(6),\n" +
						"  PRIMARY KEY (`b`,`a`)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
			{
				Query: "show create table t4",
				Expected: []sql.Row{
					{"t4", "CREATE TABLE `t4` (\n" +
						"  `a` int DEFAULT (floor(1)),\n" +
						"  `b` int DEFAULT (coalesce(`a`,10))\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
		},
	},
	{
		Name:    "describe and show columns with various keys and constraints",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table t1 (i int not null, unique key (i));",
			"create table t2 (i int not null, j int not null, unique key (j), unique key(i));",
			"create table t3 (i int not null, j int, unique key (i, j));",
			"create table t4 (i int not null, j int primary key, unique key (i));",
			"create table t5 (i int not null, j int not null, unique key (j, i), unique key (i));",
			"create table t6 (i int not null, j int not null, unique key (i), unique key (j, i));",
			"create table t7 (pk int primary key, i int, j int not null, unique key (i), unique key (j, i));",
			"create table t8 (pk int primary key, i int, j int, k int, unique key (i, j, k), unique key (i), unique key (j), unique key(k));",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "show create table t1;",
				Expected: []sql.Row{
					{"t1", "CREATE TABLE `t1` (\n" +
						"  `i` int NOT NULL,\n" +
						"  UNIQUE KEY `i` (`i`)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
			{
				Query: "describe t1;",
				Expected: []sql.Row{
					{"i", "int", "NO", "PRI", nil, ""},
				},
			},
			{
				Query: "show columns from t1;",
				Expected: []sql.Row{
					{"i", "int", "NO", "PRI", nil, ""},
				},
			},
			{
				Skip:  true, // supposed to be the first index defined, not in order of columns
				Query: "describe t2;",
				Expected: []sql.Row{
					{"i", "int", "NO", "UNI", nil, ""},
					{"j", "int", "NO", "PRI", nil, ""},
				},
			},
			{
				Query: "describe t3;",
				Expected: []sql.Row{
					{"i", "int", "NO", "MUL", nil, ""},
					{"j", "int", "YES", "", nil, ""},
				},
			},
			{
				Query: "describe t4;",
				Expected: []sql.Row{
					{"i", "int", "NO", "UNI", nil, ""},
					{"j", "int", "NO", "PRI", nil, ""},
				},
			},
			{
				// MySQL reads indexes in the order that they were created, while we sort by idx name
				// https://github.com/dolthub/dolt/issues/2289
				Skip:  true,
				Query: "describe t5;",
				Expected: []sql.Row{
					{"i", "int", "NO", "PRI", nil, ""},
					{"j", "int", "NO", "PRI", nil, ""},
				},
			},
			{
				Query: "describe t6;",
				Expected: []sql.Row{
					{"i", "int", "NO", "PRI", nil, ""},
					{"j", "int", "NO", "MUL", nil, ""},
				},
			},
			{
				Query: "describe t7;",
				Expected: []sql.Row{
					{"pk", "int", "NO", "PRI", nil, ""},
					{"i", "int", "YES", "UNI", nil, ""},
					{"j", "int", "NO", "MUL", nil, ""},
				},
			},
			{
				Skip:  true, // for some reason MUL takes priority over UNI for i
				Query: "describe t8;",
				Expected: []sql.Row{
					{"pk", "int", "NO", "PRI", nil, ""},
					{"i", "int", "YES", "MUL", nil, ""},
					{"j", "int", "YES", "UNI", nil, ""},
					{"k", "int", "YES", "UNI", nil, ""},
				},
			},
		},
	},
	{
		Name:    "Describe with expressions and views work correctly",
		Dialect: "mysql",
		SetUpScript: []string{
			"CREATE TABLE t(pk int primary key, val int DEFAULT (pk * 2))",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "DESCRIBE t",
				Expected: []sql.Row{
					{"pk", "int", "NO", "PRI", nil, ""},
					{"val", "int", "YES", "", "((`pk` * 2))", "DEFAULT_GENERATED"},
				},
			},
		},
	},
	{
		Name: "can't create view with same name as existing table",
		SetUpScript: []string{
			"create table t (i int);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "create view t as select 1",
				ExpectedErr: sql.ErrTableAlreadyExists,
			},
		},
	},
	{
		Name: "can't create table with same name as existing view",
		SetUpScript: []string{
			"create view t as select 1",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "create table t (i int);",
				ExpectedErr: sql.ErrTableAlreadyExists,
			},
		},
	},
	{
		Name:    "drop table if exists on unknown table shows warning",
		Dialect: "mysql",
		Assertions: []ScriptTestAssertion{
			{
				Query:                           "DROP TABLE IF EXISTS non_existent_table;",
				ExpectedWarning:                 1051,
				ExpectedWarningsCount:           1,
				ExpectedWarningMessageSubstring: "Unknown table 'non_existent_table'",
				SkipResultsCheck:                true,
			},
		},
	},
	{
		Name:    "renaming views with RENAME TABLE ... TO .. statement",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table t1 (id int primary key, v1 int);",
			"create view v1 as select * from t1;",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "show tables;",
				Expected: []sql.Row{{"t1"}, {"v1"}},
			},
			{
				Query:    "rename table v1 to view1",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 0}}},
			},
			{
				Query:    "show tables;",
				Expected: []sql.Row{{"t1"}, {"view1"}},
			},
			{
				Query:    "rename table view1 to newViewName, t1 to newTableName",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 0}}},
			},
			{
				Query:    "show tables;",
				Expected: []sql.Row{{"newTableName"}, {"newViewName"}},
			},
		},
	},
	{
		Name:    "renaming views with ALTER TABLE ... RENAME .. statement should fail",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table t1 (id int primary key, v1 int);",
			"create view v1 as select * from t1;",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "show tables;",
				Expected: []sql.Row{{"t1"}, {"v1"}},
			},
			{
				Query:       "alter table v1 rename to view1",
				ExpectedErr: sql.ErrExpectedTableFoundView,
			},
			{
				Query:    "show tables;",
				Expected: []sql.Row{{"t1"}, {"v1"}},
			},
		},
	},

	{
		Name: "Querying existing view that references non-existing table",
		SetUpScript: []string{
			"CREATE TABLE a(id int primary key, col1 int);",
			"CREATE VIEW b AS SELECT * FROM a;",
			"CREATE VIEW f AS SELECT col1 AS npk FROM a;",
			"RENAME TABLE a TO d;",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "CREATE VIEW g AS SELECT * FROM nonexistenttable;",
				ExpectedErr: sql.ErrTableNotFound,
			},
			{
				// TODO: ALTER VIEWs are not supported
				Skip:        true,
				Query:       "ALTER VIEW b AS SELECT * FROM nonexistenttable;",
				ExpectedErr: sql.ErrTableNotFound,
			},
			{
				Query:       "SELECT * FROM b;",
				ExpectedErr: sql.ErrInvalidRefInView,
			},
			{
				Query:    "RENAME TABLE d TO a;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "SELECT * FROM b;",
				Expected: []sql.Row{},
			},
			{
				Query:    "ALTER TABLE a RENAME COLUMN col1 TO newcol;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				// TODO: View definition should have 'SELECT *' be expanded to each column of the referenced table
				Skip:        true,
				Query:       "SELECT * FROM b;",
				ExpectedErr: sql.ErrInvalidRefInView,
			},
			{
				Query:       "SELECT * FROM f;",
				ExpectedErr: sql.ErrInvalidRefInView,
			},
		},
	},
}
