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
)

// PrimaryKeysScriptTests contains self-contained primary keys script tests.
var PrimaryKeysScriptTests = []ScriptTest{
	{
		Name: "recreate primary key rebuilds secondary indexes",
		SetUpScript: []string{
			"create table a (x int, y int, z int, primary key (x,y,z), index idx1 (y))",
			"insert into a values (1,2,3), (4,5,6), (7,8,9)",
			"alter table a drop primary key",
			"alter table a add primary key (y,z,x)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "delete from a where y = 2",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "delete from a where y = 2",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "select * from a where y = 2",
				Expected: []sql.Row{},
			},
			{
				Query:    "select * from a where y = 5",
				Expected: []sql.Row{{4, 5, 6}},
			},
		},
	},
	{
		Name:    "Multialter DDL with ADD/DROP Primary Key",
		Dialect: "mysql",
		SetUpScript: []string{
			"CREATE TABLE t(pk int primary key, v1 int)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "ALTER TABLE t ADD COLUMN (v2 int), drop primary key, add primary key (v2)",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query: "DESCRIBE t",
				Expected: []sql.Row{
					{"pk", "int", "NO", "", nil, ""},
					{"v1", "int", "YES", "", nil, ""},
					{"v2", "int", "NO", "PRI", nil, ""},
				},
			},
			{
				Query:       "ALTER TABLE t ADD COLUMN (v3 int), drop primary key, add primary key (notacolumn)",
				ExpectedErr: sql.ErrKeyColumnDoesNotExist,
			},
			{
				Query: "DESCRIBE t",
				Expected: []sql.Row{
					{"pk", "int", "NO", "", nil, ""},
					{"v1", "int", "YES", "", nil, ""},
					{"v2", "int", "NO", "PRI", nil, ""},
				},
			},
		},
	},
	{
		Name: "primary key order",
		SetUpScript: []string{
			"create table t1 (a varchar(5), b varchar(10), primary key(a, b));",
			"create table t2 (a varchar(5), b varchar(10), primary key(b, a));",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:          "insert into t1 (a, b) values ('1234567890', '12345')",
				ExpectedErrStr: "string '1234567890' is too large for column 'a'",
			},
			{
				Query: "insert into t1 (b, a) values ('1234567890', '12345')",
				Expected: []sql.Row{
					{types.OkResult{RowsAffected: 1}},
				},
			},
			{
				Query: "select a, b from t1",
				Expected: []sql.Row{
					{"12345", "1234567890"},
				},
			},
			{
				Query:          "insert into t2 (a, b) values ('1234567890', '12345')",
				ExpectedErrStr: "string '1234567890' is too large for column 'a'",
			},
			{
				Query: "insert into t2 (b, a) values ('1234567890', '12345')",
				Expected: []sql.Row{
					{types.OkResult{RowsAffected: 1}},
				},
			},
			{
				Query: "select a, b from t2",
				Expected: []sql.Row{
					{"12345", "1234567890"},
				},
			},
		},
	},
}

var AddDropPrimaryKeyScripts = []ScriptTest{
	{
		Name: "Add primary key",
		SetUpScript: []string{
			"create table t1 (i int, j int)",
			"insert into t1 values (1,1), (1,2), (1,3)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "alter table t1 add primary key (i)",
				ExpectedErr: sql.ErrPrimaryKeyViolation,
			},
			{
				Query: "show create table t1",
				Expected: []sql.Row{{"t1",
					"CREATE TABLE `t1` (\n" +
						"  `i` int,\n" +
						"  `j` int\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:    "alter table t1 add primary key (i, j)",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query: "show create table t1",
				Expected: []sql.Row{{"t1",
					"CREATE TABLE `t1` (\n" +
						"  `i` int NOT NULL,\n" +
						"  `j` int NOT NULL,\n" +
						"  PRIMARY KEY (`i`,`j`)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
		},
	},
	{
		Name: "Drop primary key for table with multiple primary key columns",
		SetUpScript: []string{
			"create table t1 (pk varchar(20), v varchar(20) default (concat(pk, '-foo')), primary key (pk, v))",
			"insert into t1 values ('a1', 'a2'), ('a2', 'a3'), ('a3', 'a4')",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select * from t1 order by pk",
				Expected: []sql.Row{
					{"a1", "a2"},
					{"a2", "a3"},
					{"a3", "a4"},
				},
			},
			{
				Query:    "alter table t1 drop primary key",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query: "select * from t1 order by pk",
				Expected: []sql.Row{
					{"a1", "a2"},
					{"a2", "a3"},
					{"a3", "a4"},
				},
			},
			{
				Query:    "insert into t1 values ('a1', 'a2')",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query: "select * from t1 order by pk",
				Expected: []sql.Row{
					{"a1", "a2"},
					{"a1", "a2"},
					{"a2", "a3"},
					{"a3", "a4"},
				},
			},
			{
				Query:       "alter table t1 add primary key (pk, v)",
				ExpectedErr: sql.ErrPrimaryKeyViolation,
			},
			{
				Query:    "delete from t1 where pk = 'a1' limit 1",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "alter table t1 add primary key (pk, v)",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query: "show create table t1",
				Expected: []sql.Row{{"t1",
					"CREATE TABLE `t1` (\n" +
						"  `pk` varchar(20) NOT NULL,\n" +
						"  `v` varchar(20) NOT NULL DEFAULT (concat(`pk`,'-foo')),\n" +
						"  PRIMARY KEY (`pk`,`v`)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:    "alter table t1 drop primary key",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "alter table t1 add index myidx (v)",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "alter table t1 add primary key (pk)",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "insert into t1 values ('a4', 'a3')",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query: "show create table t1",
				Expected: []sql.Row{{"t1",
					"CREATE TABLE `t1` (\n" +
						"  `pk` varchar(20) NOT NULL,\n" +
						"  `v` varchar(20) NOT NULL DEFAULT (concat(`pk`,'-foo')),\n" +
						"  PRIMARY KEY (`pk`),\n" +
						"  KEY `myidx` (`v`)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query: "select * from t1 where v = 'a3' order by pk",
				Expected: []sql.Row{
					{"a2", "a3"},
					{"a4", "a3"},
				},
			},
			{
				Query:    "alter table t1 drop primary key",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "truncate t1",
				Expected: []sql.Row{{types.NewOkResult(4)}},
			},
			{
				Query:    "alter table t1 drop index myidx",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "alter table t1 add primary key (pk, v)",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "insert into t1 values ('a1', 'a2'), ('a2', 'a3'), ('a3', 'a4')",
				Expected: []sql.Row{{types.NewOkResult(3)}},
			},
		},
	},
	{
		Name: "Drop primary key for table with multiple primary key columns, add smaller primary key in same statement",
		SetUpScript: []string{
			"create table t1 (pk varchar(20), v varchar(20) default (concat(pk, '-foo')), primary key (pk, v))",
			"insert into t1 values ('a1', 'a2'), ('a2', 'a3'), ('a3', 'a4')",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "ALTER TABLE t1 DROP PRIMARY KEY, ADD PRIMARY KEY (v)",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:       "INSERT INTO t1 (pk, v) values ('a100', 'a3')",
				ExpectedErr: sql.ErrPrimaryKeyViolation,
			},
			{
				Query:    "alter table t1 drop primary key",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "ALTER TABLE t1 ADD PRIMARY KEY (pk, v), DROP PRIMARY KEY",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query: "show create table t1",
				Expected: []sql.Row{{"t1",
					"CREATE TABLE `t1` (\n" +
						"  `pk` varchar(20) NOT NULL,\n" +
						"  `v` varchar(20) NOT NULL DEFAULT (concat(`pk`,'-foo'))\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
		},
	},
	{
		Name: "No database selected",
		SetUpScript: []string{
			"create database newdb",
			"create table newdb.tab1 (pk int, c1 int)",
			"ALTER TABLE newdb.tab1 ADD PRIMARY KEY (pk)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW CREATE TABLE newdb.tab1",
				Expected: []sql.Row{{"tab1",
					"CREATE TABLE `tab1` (\n" +
						"  `pk` int NOT NULL,\n" +
						"  `c1` int,\n" +
						"  PRIMARY KEY (`pk`)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:    "alter table newdb.tab1 drop primary key",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query: "SHOW CREATE TABLE newdb.tab1",
				Expected: []sql.Row{{"tab1",
					"CREATE TABLE `tab1` (\n" +
						"  `pk` int NOT NULL,\n" +
						"  `c1` int\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
		},
	},
	{
		Name: "Drop primary key auto increment",
		SetUpScript: []string{
			"CREATE TABLE test(pk int AUTO_INCREMENT PRIMARY KEY, val int)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "ALTER TABLE test DROP PRIMARY KEY",
				ExpectedErr: sql.ErrWrongAutoKey,
			},
			{
				Query:    "ALTER TABLE test modify pk int",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query: "SHOW CREATE TABLE test",
				Expected: []sql.Row{{"test",
					"CREATE TABLE `test` (\n" +
						"  `pk` int NOT NULL,\n" +
						"  `val` int,\n" +
						"  PRIMARY KEY (`pk`)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:    "ALTER TABLE test drop primary key",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query: "SHOW CREATE TABLE test",
				Expected: []sql.Row{{"test",
					"CREATE TABLE `test` (\n" +
						"  `pk` int NOT NULL,\n" +
						"  `val` int\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:       "INSERT INTO test VALUES (1, 1), (NULL, 1)",
				ExpectedErr: sql.ErrInsertIntoNonNullableProvidedNull,
			},
			{
				Query:    "INSERT INTO test VALUES (2, 2), (3, 3)",
				Expected: []sql.Row{{types.NewOkResult(2)}},
			},
			{
				Query: "SELECT * FROM test ORDER BY pk",
				Expected: []sql.Row{
					{2, 2},
					{3, 3},
				},
			},
		},
	},
	{
		Name: "Drop auto-increment primary key with supporting unique index",
		SetUpScript: []string{
			"create table t (id int primary key AUTO_INCREMENT, c1 varchar(255));",
			"insert into t (c1) values ('one');",
		},
		Assertions: []ScriptTestAssertion{
			{
				// Without a supporting index, we can't drop the PK because of the auto_increment property
				Query:       "ALTER TABLE t DROP PRIMARY KEY;",
				ExpectedErr: sql.ErrWrongAutoKey,
			},
			{
				// Adding a unique index on the pk column allows us to drop the PK
				Query:    "ALTER TABLE t ADD UNIQUE KEY id (id);",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "ALTER TABLE t DROP PRIMARY KEY;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query: "show create table t;",
				Expected: []sql.Row{{"t", "CREATE TABLE `t` (\n" +
					"  `id` int NOT NULL AUTO_INCREMENT,\n" +
					"  `c1` varchar(255),\n" +
					"  UNIQUE KEY `id` (`id`)\n" +
					") ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:    "insert into t (c1) values('two');",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1, InsertID: 2}}},
			},
			{
				Query:    "select * from t;",
				Expected: []sql.Row{{1, "one"}, {2, "two"}},
			},
		},
	},
	{
		Name: "Drop auto-increment primary key with supporting non-unique index",
		SetUpScript: []string{
			"create table t (id int primary key AUTO_INCREMENT, c1 varchar(255));",
			"insert into t (c1) values ('one');",
		},
		Assertions: []ScriptTestAssertion{
			{
				// Without a supporting index, we cannot drop the PK
				Query:       "ALTER TABLE t DROP PRIMARY KEY;",
				ExpectedErr: sql.ErrWrongAutoKey,
			},
			{
				// Adding an index on the PK columns allows us to drop the PK
				Query:    "ALTER TABLE t ADD KEY id (id);",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "ALTER TABLE t DROP PRIMARY KEY;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query: "show create table t;",
				Expected: []sql.Row{{"t", "CREATE TABLE `t` (\n" +
					"  `id` int NOT NULL AUTO_INCREMENT,\n" +
					"  `c1` varchar(255),\n" +
					"  KEY `id` (`id`)\n" +
					") ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:    "insert into t (c1) values('two');",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1, InsertID: 2}}},
			},
			{
				Query:    "select * from t;",
				Expected: []sql.Row{{1, "one"}, {2, "two"}},
			},
		},
	},
	{
		Name: "Drop multi-column, auto-increment primary key with supporting non-unique index",
		SetUpScript: []string{
			"create table t (id1 int AUTO_INCREMENT, id2 int not null, c1 varchar(255), primary key (id1, id2));",
			"insert into t (id2, c1) values (-1, 'one');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "ALTER TABLE t DROP PRIMARY KEY;",
				ExpectedErr: sql.ErrWrongAutoKey,
			},
			{
				// Adding an index that doesn't start with the auto_increment column doesn't allow us to drop the PK
				Query:    "ALTER TABLE t ADD KEY c1id1 (c1, id1);",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:       "ALTER TABLE t DROP PRIMARY KEY;",
				ExpectedErr: sql.ErrWrongAutoKey,
			},
			{
				// Adding a supporting key (i.e the first column is the auto_increment column) allows us to drop the PK
				Query:    "ALTER TABLE t ADD KEY id1c1 (id1, c1);",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "ALTER TABLE t DROP PRIMARY KEY;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "insert into t (id2, c1) values(-2, 'two');",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1, InsertID: 2}}},
			},
			{
				Query:    "select * from t;",
				Expected: []sql.Row{{1, -1, "one"}, {2, -2, "two"}},
			},
			{
				Query: "show create table t;",
				Expected: []sql.Row{{"t", "CREATE TABLE `t` (\n" +
					"  `id1` int NOT NULL AUTO_INCREMENT,\n" +
					"  `id2` int NOT NULL,\n" +
					"  `c1` varchar(255),\n" +
					"  KEY `c1id1` (`c1`,`id1`),\n" +
					"  KEY `id1c1` (`id1`,`c1`)\n" +
					") ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
		},
	},
}

var BrokenPrimaryKeyScriptTests = []ScriptTest{
	{
		Name: "Multialter DDL with ADD/DROP Primary Key",
		SetUpScript: []string{
			"CREATE TABLE t(pk int primary key, v1 int)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "ALTER TABLE t ADD COLUMN (v2 int), drop primary key, add primary key (v2)",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query: "DESCRIBE t",
				Expected: []sql.Row{
					{"pk", "int", "NO", "", nil, ""},
					{"v1", "int", "YES", "", nil, ""},
					{"v2", "int", "NO", "PRI", nil, ""},
				},
			},
			{
				Query:       "ALTER TABLE t ADD COLUMN (v3 int), drop primary key, add primary key (notacolumn)",
				ExpectedErr: sql.ErrKeyColumnDoesNotExist,
			},
			{
				Query: "DESCRIBE t",
				Expected: []sql.Row{
					{"pk", "int", "NO", "", nil, ""},
					{"v1", "int", "YES", "", nil, ""},
					{"v2", "int", "NO", "PRI", nil, ""},
				},
			},
			{
				// This last modification ends up with a UNIQUE constraint on pk
				// This is caused by Table.dropColumnFromSchema, not dropping the pkOrdinal, but this causes other problems specific to GMS
				Query:    "ALTER TABLE t ADD column `v4` int NOT NULL, ADD column `v5` int NOT NULL, DROP COLUMN `v1`, ADD COLUMN `v6` int NOT NULL, DROP COLUMN `v2`, ADD COLUMN v7 int NOT NULL",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query: "DESCRIBE t",
				Expected: []sql.Row{
					{"pk", "int", "NO", "", nil, ""},
					{"v4", "int", "NO", "", nil, ""},
					{"v5", "int", "NO", "", nil, ""},
					{"v6", "int", "NO", "", nil, ""},
					{"v7", "int", "NO", "", nil, ""},
				},
			},
		},
	},
}
