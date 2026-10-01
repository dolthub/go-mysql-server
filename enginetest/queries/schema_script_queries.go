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
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/vitess/go/sqltypes"
)

// SchemaScriptTests contains self-contained script tests for schema.
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
		Name:    "alter table out of range value error of column type change",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table t (i int primary key, i2 int, key(i2));",
			"insert into t values (0,-1)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       `alter table t modify column i2 int unsigned`,
				ExpectedErr: sql.ErrValueOutOfRange,
			},
		},
	},
	{
		Name:    "ALTER TABLE, ALTER COLUMN SET, DROP DEFAULT",
		Dialect: "mysql",
		SetUpScript: []string{
			"CREATE TABLE test (pk BIGINT PRIMARY KEY, v1 BIGINT NOT NULL DEFAULT 88);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "INSERT INTO test (pk) VALUES (1);",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "SELECT * FROM test;",
				Expected: []sql.Row{{1, 88}},
			},
			{
				Query:    "ALTER TABLE test ALTER v1 SET DEFAULT (CONVERT('42', SIGNED));",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "INSERT INTO test (pk) VALUES (2);",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "SELECT * FROM test;",
				Expected: []sql.Row{{1, 88}, {2, 42}},
			},
			{
				Query:       "ALTER TABLE test ALTER v2 SET DEFAULT 1;",
				ExpectedErr: sql.ErrTableColumnNotFound,
			},
			{
				Query:    "ALTER TABLE test ALTER v1 DROP DEFAULT;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:       "INSERT INTO test (pk) VALUES (3);",
				ExpectedErr: sql.ErrFieldNoDefaultValue,
			},
			{
				Query:       "ALTER TABLE test ALTER v2 DROP DEFAULT;",
				ExpectedErr: sql.ErrTableColumnNotFound,
			},
			{ // Just confirms that the last INSERT didn't do anything
				Query:    "SELECT * FROM test;",
				Expected: []sql.Row{{1, 88}, {2, 42}},
			},
			{
				Query:    "ALTER TABLE test ALTER v1 SET DEFAULT 100, alter v1 DROP DEFAULT",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:       "INSERT INTO test (pk) VALUES (2);",
				ExpectedErr: sql.ErrFieldNoDefaultValue,
			},
			{
				Query:    "ALTER TABLE test ALTER v1 SET DEFAULT 100, alter v1 SET DEFAULT 200",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:       "ALTER TABLE test DROP COLUMN v1, alter v1 SET DEFAULT 5000",
				ExpectedErr: sql.ErrTableColumnNotFound,
			},
			{
				Query: "DESCRIBE test",
				Expected: []sql.Row{
					{"pk", "bigint", "NO", "PRI", nil, ""},
					{"v1", "bigint", "NO", "", "200", ""},
				},
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
		Name: "alter json column default; from scorewarrior: https://github.com/dolthub/dolt/issues/4543",
		SetUpScript: []string{
			"CREATE TABLE test (i int default 999, j json);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "alter table test alter column j set default ('[]');",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:   "show create table test",
				Dialect: "mysql",
				Expected: []sql.Row{
					{"test", "CREATE TABLE `test` (\n  `i` int DEFAULT '999',\n  `j` json DEFAULT ('[]')\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
		},
	},
	{
		Name:    "ALTER TABLE MULTI ADD/DROP COLUMN",
		Dialect: "mysql",
		SetUpScript: []string{
			"CREATE TABLE test (pk BIGINT PRIMARY KEY, v1 BIGINT NOT NULL DEFAULT 88);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "INSERT INTO test (pk) VALUES (1);",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "ALTER TABLE test DROP COLUMN v1, ADD COLUMN v2 INT NOT NULL DEFAULT 100",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query: "describe test",
				Expected: []sql.Row{
					{"pk", "bigint", "NO", "PRI", nil, ""},
					{"v2", "int", "NO", "", "100", ""},
				},
			},
			{
				Query:    "ALTER TABLE TEST MODIFY COLUMN pk BIGINT AUTO_INCREMENT, AUTO_INCREMENT = 100",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "INSERT INTO test (v2) values (11)",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1, InsertID: 100}}},
			},
			{
				Query:    "SELECT * from test where pk = 100",
				Expected: []sql.Row{{100, 11}},
			},
			{
				Query:       "ALTER TABLE test DROP COLUMN v2, ADD COLUMN v3 int NOT NULL after v2",
				ExpectedErr: sql.ErrTableColumnNotFound,
			},
			{
				Query: "describe test",
				Expected: []sql.Row{
					{"pk", "bigint", "NO", "PRI", nil, "auto_increment"},
					{"v2", "int", "NO", "", "100", ""},
				},
			},
			{
				Query:       "ALTER TABLE test DROP COLUMN v2, RENAME COLUMN v2 to v3",
				ExpectedErr: sql.ErrTableColumnNotFound,
			},
			{
				Query: "describe test",
				Expected: []sql.Row{
					{"pk", "bigint", "NO", "PRI", nil, "auto_increment"},
					{"v2", "int", "NO", "", "100", ""},
				},
			},
			{
				Query:       "ALTER TABLE test RENAME COLUMN v2 to v3, DROP COLUMN v2",
				ExpectedErr: sql.ErrTableColumnNotFound,
			},
			{
				Query: "describe test",
				Expected: []sql.Row{
					{"pk", "bigint", "NO", "PRI", nil, "auto_increment"},
					{"v2", "int", "NO", "", "100", ""},
				},
			},
			{
				Query:    "ALTER TABLE test ADD COLUMN (v3 int NOT NULL), add column (v4 int), drop column v2, add column (v5 int NOT NULL)",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query: "DESCRIBE test",
				Expected: []sql.Row{
					{"pk", "bigint", "NO", "PRI", nil, "auto_increment"},
					{"v3", "int", "NO", "", nil, ""},
					{"v4", "int", "YES", "", nil, ""},
					{"v5", "int", "NO", "", nil, ""},
				},
			},
			{
				Query:    "ALTER TABLE test ADD COLUMN (v6 int not null), RENAME COLUMN v5 TO mycol, DROP COLUMN v4, ADD COLUMN (v7 int);",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query: "describe test",
				Expected: []sql.Row{
					{"pk", "bigint", "NO", "PRI", nil, "auto_increment"},
					{"v3", "int", "NO", "", nil, ""},
					{"mycol", "int", "NO", "", nil, ""},
					{"v6", "int", "NO", "", nil, ""},
					{"v7", "int", "YES", "", nil, ""},
				},
			},
			// TODO: Does not include tests with column renames and defaults.
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
		Name:    "basic test on tables dual and `dual`",
		Dialect: "mysql",
		SetUpScript: []string{
			"CREATE TABLE `dual` (id int)",
			"INSERT INTO `dual` VALUES (2)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT * from `dual`;",
				Expected: []sql.Row{{2}},
			},
			{
				Query:    "SELECT 3 from dual;",
				Expected: []sql.Row{{3}},
			},
			{
				Dialect:     "mysql",
				Query:       "SELECT * from dual;",
				ExpectedErr: sql.ErrNoTablesUsed,
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
	{
		Name:    "test show create database",
		Dialect: "mysql",
		SetUpScript: []string{
			"create database def_db;",
			"create database latin1_db character set latin1;",
			"create database bin_db charset binary;",
			"create database mb3_db collate utf8mb3_general_ci;",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "show create database def_db",
				Expected: []sql.Row{
					{"def_db", "CREATE DATABASE `def_db` /*!40100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin */"},
				},
			},
			{
				Query: "show create database latin1_db",
				Expected: []sql.Row{
					{"latin1_db", "CREATE DATABASE `latin1_db` /*!40100 DEFAULT CHARACTER SET latin1 COLLATE latin1_swedish_ci */"},
				},
			},
			{
				Query: "show create database bin_db",
				Expected: []sql.Row{
					{"bin_db", "CREATE DATABASE `bin_db` /*!40100 DEFAULT CHARACTER SET binary COLLATE binary */"},
				},
			},
			{
				Query: "show create database mb3_db",
				Expected: []sql.Row{
					{"mb3_db", "CREATE DATABASE `mb3_db` /*!40100 DEFAULT CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci */"},
				},
			},
		},
	},
	{
		Name:    "test create database with modified server variables",
		Dialect: "mysql",
		SetUpScript: []string{
			"set @@session.character_set_server = 'latin1';",
			"create database latin1_db;",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select @@global.character_set_server, @@global.collation_server;",
				Expected: []sql.Row{
					{"utf8mb4", "utf8mb4_0900_bin"},
				},
			},
			{
				Query: "select @@session.character_set_server, @@session.collation_server;",
				Expected: []sql.Row{
					{"latin1", "latin1_swedish_ci"},
				},
			},
			{
				// Interestingly, session actually takes priority over global
				Query: "show create database latin1_db",
				Expected: []sql.Row{
					{"latin1_db", "CREATE DATABASE `latin1_db` /*!40100 DEFAULT CHARACTER SET latin1 COLLATE latin1_swedish_ci */"},
				},
			},
		},
	},
	{
		Name:    "test parenthesized tables",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table t1 (i int);",
			"insert into t1 values (1), (2), (3);",
			"create table t2 (j int);",
			"insert into t2 values (1), (3);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select * from (t1)",
				Expected: []sql.Row{
					{1},
					{2},
					{3},
				},
			},
			{
				Query: "select * from (((((t1)))))",
				Expected: []sql.Row{
					{1},
					{2},
					{3},
				},
			},
			{
				Query: "select * from (((((t1 as t11)))))",
				Expected: []sql.Row{
					{1},
					{2},
					{3},
				},
			},
			{
				Query: "select * from (t1) join t2 where t1.i = t2.j",
				Expected: []sql.Row{
					{1, 1},
					{3, 3},
				},
			},
			{
				Query: "select * from t1 join (t2) where t1.i = t2.j",
				Expected: []sql.Row{
					{1, 1},
					{3, 3},
				},
			},
			{
				Query: "select * from (t1) join (t2) where t1.i = t2.j",
				Expected: []sql.Row{
					{1, 1},
					{3, 3},
				},
			},
			{
				Query: "select * from ((((t1)))) join ((((t2)))) where t1.i = t2.j",
				Expected: []sql.Row{
					{1, 1},
					{3, 3},
				},
			},
			{
				Query: "select * from (t1 as t11) join (t2 as t22) where t11.i = t22.j",
				Expected: []sql.Row{
					{1, 1},
					{3, 3},
				},
			},
		},
	},
}
