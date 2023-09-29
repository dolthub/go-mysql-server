// Copyright 2023 Dolthub, Inc.
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
	"github.com/dolthub/vitess/go/mysql"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

var AlterTableScripts = []ScriptTest{
	{
		// This script relies on setup.Pk_tablesData
		Name: "Error queries",
		Assertions: []ScriptTestAssertion{
			{
				Query:       "ALTER TABLE one_pk_two_idx MODIFY COLUMN v1 BIGINT DEFAULT (pk) AFTER v3",
				ExpectedErr: sql.ErrTableColumnNotFound,
			},
			{
				Query:       "ALTER TABLE one_pk_two_idx ADD COLUMN v4 BIGINT DEFAULT (pk) AFTER v3",
				ExpectedErr: sql.ErrTableColumnNotFound,
			},
			{
				Query:       "ALTER TABLE one_pk_two_idx ADD COLUMN v3 BIGINT DEFAULT 5, RENAME COLUMN v3 to v4",
				ExpectedErr: sql.ErrTableColumnNotFound,
			},
			{
				Query:       "ALTER TABLE one_pk_two_idx ADD COLUMN v3 BIGINT DEFAULT 5, modify column v3 bigint default null",
				ExpectedErr: sql.ErrTableColumnNotFound,
			},
		},
	},
	{
		// https://github.com/dolthub/dolt/issues/6206
		Name: "alter table containing column default value expressions",
		SetUpScript: []string{
			"create table t (pk int primary key, col1 timestamp(6) default current_timestamp(), col2 varchar(1000), index idx1 (pk, col1));",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "alter table t alter column col2 DROP DEFAULT;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "show create table t;",
				Expected: []sql.Row{{"t", "CREATE TABLE `t` (\n  `pk` int NOT NULL,\n  `col1` timestamp(6) DEFAULT (CURRENT_TIMESTAMP()),\n  `col2` varchar(1000),\n  PRIMARY KEY (`pk`),\n  KEY `idx1` (`pk`,`col1`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:    "alter table t alter column col2 SET DEFAULT 'FOO!';",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "show create table t;",
				Expected: []sql.Row{{"t", "CREATE TABLE `t` (\n  `pk` int NOT NULL,\n  `col1` timestamp(6) DEFAULT (CURRENT_TIMESTAMP()),\n  `col2` varchar(1000) DEFAULT 'FOO!',\n  PRIMARY KEY (`pk`),\n  KEY `idx1` (`pk`,`col1`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:    "alter table t drop index idx1;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
		},
	},
	{
		Name: "drop column drops check constraint",
		SetUpScript: []string{
			"create table t34 (i bigint primary key, s varchar(20))",
			"ALTER TABLE t34 ADD COLUMN j int",
			"ALTER TABLE t34 ADD CONSTRAINT test_check CHECK (j < 12345)",
			"ALTER TABLE t34 DROP COLUMN j",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "show create table t34",
				Expected: []sql.Row{{"t34", "CREATE TABLE `t34` (\n" +
					"  `i` bigint NOT NULL,\n" +
					"  `s` varchar(20),\n" +
					"  PRIMARY KEY (`i`)\n" +
					") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
		},
	},
	{
		Name: "drop check as part of alter block",
		SetUpScript: []string{
			"create table t42 (i bigint primary key, j int, CONSTRAINT check1 CHECK (j < 12345), CONSTRAINT check2 CHECK (j > 0))",
			"ALTER TABLE t42 ADD COLUMN s varchar(20), drop check check1",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "show create table t42",
				Expected: []sql.Row{{"t42",
					"CREATE TABLE `t42` (\n" +
						"  `i` bigint NOT NULL,\n" +
						"  `j` int,\n" +
						"  `s` varchar(20),\n" +
						"  PRIMARY KEY (`i`),\n" +
						"  CONSTRAINT `check2` CHECK ((`j` > 0))\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
		},
	},
	{
		Name: "drop constraint as part of alter block",
		SetUpScript: []string{
			"create table t42 (i bigint primary key, j int, CONSTRAINT check1 CHECK (j < 12345), CONSTRAINT check2 CHECK (j > 0))",
			"ALTER TABLE t42 ADD COLUMN s varchar(20), drop constraint check1",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "show create table t42",
				Expected: []sql.Row{{"t42",
					"CREATE TABLE `t42` (\n" +
						"  `i` bigint NOT NULL,\n" +
						"  `j` int,\n" +
						"  `s` varchar(20),\n" +
						"  PRIMARY KEY (`i`),\n" +
						"  CONSTRAINT `check2` CHECK ((`j` > 0))\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
		},
	},
	{
		Name: "drop column drops all relevant check constraints",
		SetUpScript: []string{
			"create table t42 (i bigint primary key, s varchar(20))",
			"ALTER TABLE t42 ADD COLUMN j int",
			"ALTER TABLE t42 ADD CONSTRAINT check1 CHECK (j < 12345)",
			"ALTER TABLE t42 ADD CONSTRAINT check2 CHECK (j > 0)",
			"ALTER TABLE t42 DROP COLUMN j",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "show create table t42",
				Expected: []sql.Row{{"t42", "CREATE TABLE `t42` (\n" +
					"  `i` bigint NOT NULL,\n" +
					"  `s` varchar(20),\n" +
					"  PRIMARY KEY (`i`)\n" +
					") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
		},
	},
	{
		Name: "drop column drops correct check constraint",
		SetUpScript: []string{
			"create table t41 (i bigint primary key, s varchar(20))",
			"ALTER TABLE t41 ADD COLUMN j int",
			"ALTER TABLE t41 ADD COLUMN k int",
			"ALTER TABLE t41 ADD CONSTRAINT j_check CHECK (j < 12345)",
			"ALTER TABLE t41 ADD CONSTRAINT k_check CHECK (k < 123)",
			"ALTER TABLE t41 DROP COLUMN j",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "show create table t41",
				Expected: []sql.Row{{"t41", "CREATE TABLE `t41` (\n" +
					"  `i` bigint NOT NULL,\n" +
					"  `s` varchar(20),\n" +
					"  `k` int,\n" +
					"  PRIMARY KEY (`i`),\n" +
					"  CONSTRAINT `k_check` CHECK ((`k` < 123))\n" +
					") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
		},
	},
	{
		Name: "drop column does not drop when referenced in constraint with other column",
		SetUpScript: []string{
			"create table t43 (i bigint primary key, s varchar(20))",
			"ALTER TABLE t43 ADD COLUMN j int",
			"ALTER TABLE t43 ADD COLUMN k int",
			"ALTER TABLE t43 ADD CONSTRAINT test_check CHECK (j < k)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "alter table t43 drop column j",
				ExpectedErr: sql.ErrCheckConstraintInvalidatedByColumnAlter,
			},
			{
				Query: "show create table t43",
				Expected: []sql.Row{{"t43", "CREATE TABLE `t43` (\n" +
					"  `i` bigint NOT NULL,\n" +
					"  `s` varchar(20),\n" +
					"  `j` int,\n" +
					"  `k` int,\n" +
					"  PRIMARY KEY (`i`),\n" +
					"  CONSTRAINT `test_check` CHECK ((`j` < `k`))\n" +
					") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
		},
	},
	{
		Name: "drop column preserves indexes",
		SetUpScript: []string{
			"create table t35 (i bigint primary key, s varchar(20), s2 varchar(20))",
			"ALTER TABLE t35 ADD unique key test_key (s)",
			"ALTER TABLE t35 DROP COLUMN s2",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "show create table t35",
				Expected: []sql.Row{{"t35", "CREATE TABLE `t35` (\n" +
					"  `i` bigint NOT NULL,\n" +
					"  `s` varchar(20),\n" +
					"  PRIMARY KEY (`i`),\n" +
					"  UNIQUE KEY `test_key` (`s`)\n" +
					") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
		},
	},
	{
		Name: "drop column prevents foreign key violations",
		SetUpScript: []string{
			"create table t36 (i bigint primary key, j varchar(20))",
			"create table t37 (i bigint primary key, j varchar(20))",
			"ALTER TABLE t36 ADD key (j)",
			"ALTER TABLE t37 ADD constraint fk_36 foreign key (j) references t36(j)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "alter table t37 drop column j",
				ExpectedErr: sql.ErrForeignKeyDropColumn,
			},
		},
	},
	{
		Name: "disable keys / enable keys",
		SetUpScript: []string{
			"CREATE TABLE t33(pk BIGINT PRIMARY KEY, v1 int, v2 int)",
			`alter table t33 add column v4 int after pk,
			drop column v2, add constraint v1gt0 check (v1 > 0)`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:                 "ALTER TABLE t33 DISABLE KEYS",
				SkipResultsCheck:      true,
				ExpectedWarning:       mysql.ERNotSupportedYet,
				ExpectedWarningsCount: 1,
			},
			{
				Query:                 "ALTER TABLE t33 ENABLE KEYS",
				SkipResultsCheck:      true,
				ExpectedWarning:       mysql.ERNotSupportedYet,
				ExpectedWarningsCount: 1,
			},
		},
	},
	{
		Name: "adding a unique constraint errors if violations exist",
		SetUpScript: []string{
			"CREATE TABLE t38 (pk int PRIMARY KEY, col1 int)",
			"INSERT INTO t38 VALUES (1, 1)",
			"INSERT INTO t38 VALUES (2, 2)",
			"INSERT INTO t38 VALUES (3, NULL)",
			"INSERT INTO t38 VALUES (4, NULL)",

			"CREATE TABLE t39 (pk int PRIMARY KEY, col1 int, col2 int)",
			"INSERT INTO t39 VALUES (1, 1, 1)",
			"INSERT INTO t39 VALUES (2, 1, 2)",
			"INSERT INTO t39 VALUES (3, 2, 1)",
			"INSERT INTO t39 VALUES (4, 1, NULL)",
			"INSERT INTO t39 VALUES (5, 1, NULL)",
			"INSERT INTO t39 VALUES (6, NULL, 1)",
			"INSERT INTO t39 VALUES (7, NULL, 1)",
			"INSERT INTO t39 VALUES (8, NULL, NULL)",
			"INSERT INTO t39 VALUES (9, NULL, NULL)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "ALTER TABLE t38 ADD UNIQUE u_col1 (col1)",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "ALTER TABLE t39 ADD UNIQUE u_col1_col2 (col1, col2)",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "ALTER TABLE t38 DROP INDEX u_col1;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "INSERT INTO t38 VALUES (5, 1);",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:       "ALTER TABLE t38 ADD UNIQUE u_col1 (col1)",
				ExpectedErr: sql.ErrUniqueKeyViolation,
			},
			{
				Query: "show create table t38;",
				Expected: []sql.Row{{"t38", "CREATE TABLE `t38` (\n" +
					"  `pk` int NOT NULL,\n" +
					"  `col1` int,\n" +
					"  PRIMARY KEY (`pk`)\n" +
					") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:    "ALTER TABLE t39 DROP INDEX u_col1_col2;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "INSERT INTO t39 VALUES (10, 1, 1);",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:       "ALTER TABLE t39 ADD UNIQUE u_col1_col2 (col1, col2)",
				ExpectedErr: sql.ErrUniqueKeyViolation,
			},
			{
				Query: "show create table t39;",
				Expected: []sql.Row{{"t39", "CREATE TABLE `t39` (\n" +
					"  `pk` int NOT NULL,\n" +
					"  `col1` int,\n" +
					"  `col2` int,\n" +
					"  PRIMARY KEY (`pk`)\n" +
					") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
		},
	},
	{
		Name: "ALTER TABLE remove AUTO_INCREMENT",
		SetUpScript: []string{
			"CREATE TABLE t40 (pk int AUTO_INCREMENT PRIMARY KEY, val int)",
			"INSERT into t40 VALUES (1, 1), (NULL, 2), (NULL, 3)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "ALTER TABLE t40 MODIFY COLUMN pk int",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query: "describe t40",
				Expected: []sql.Row{
					{"pk", "int", "NO", "PRI", "NULL", ""},
					{"val", "int", "YES", "", "NULL", ""},
				},
			},
			{
				Query:       "INSERT INTO t40 VALUES (NULL, 4)",
				ExpectedErr: sql.ErrInsertIntoNonNullableProvidedNull,
			},
			{
				Query:    "drop table t40",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "CREATE TABLE t40 (pk int AUTO_INCREMENT PRIMARY KEY, val int)",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query: "INSERT INTO t40 VALUES (NULL, 1)",
				Expected: []sql.Row{{types.OkResult{
					RowsAffected: 1,
					InsertID:     1,
				}}},
			},
			{
				Query:    "SELECT * FROM t40",
				Expected: []sql.Row{{1, 1}},
			},
		},
	},
	{
		Name: "add column unique index",
		SetUpScript: []string{
			"CREATE TABLE t1 (i bigint primary key, s varchar(20))",
			"INSERT INTO t1 VALUES (1, 'a'), (2, 'b'), (3, 'c')",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "alter table t1 add column j int unique",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query: "show create table t1",
				Expected: []sql.Row{{"t1", "CREATE TABLE `t1` (\n" +
					"  `i` bigint NOT NULL,\n" +
					"  `s` varchar(20),\n" +
					"  `j` int,\n" +
					"  PRIMARY KEY (`i`),\n" +
					"  UNIQUE KEY `j` (`j`)\n" +
					") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
		},
	},
	{
		Name: "multi-alter ddl column errors",
		SetUpScript: []string{
			"create table tbl_i (i int primary key)",
			"create table tbl_ij (i int primary key, j int)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "alter table tbl_i add column j int, drop column j",
				ExpectedErr: sql.ErrTableColumnNotFound,
			},
			{
				Query:       "alter table tbl_i add column j int, rename column j to k;",
				ExpectedErr: sql.ErrTableColumnNotFound,
			},
			{
				Query:       "alter table tbl_i add column j int, modify column j varchar(10)",
				ExpectedErr: sql.ErrTableColumnNotFound,
			},
			{
				Query:       "alter table tbl_ij drop column j, rename column j to k;",
				ExpectedErr: sql.ErrTableColumnNotFound,
			},
			{
				Query:       "alter table tbl_ij drop column k, rename column j to k;",
				ExpectedErr: sql.ErrTableColumnNotFound,
			},
			{
				Query:       "alter table tbl_i add index(j), add column j int;",
				ExpectedErr: sql.ErrKeyColumnDoesNotExist,
			},
		},
	},
	{
		Name: "Add column and make unique in separate clauses",
		SetUpScript: []string{
			"create table t (c1 int primary key, c2 int, c3 int)",
			"insert into t values (1, 1, 1), (2, 2, 2), (3, 3, 3)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "alter table t add column c4 int null, add unique index uniq(c4)",
				Expected: []sql.Row{
					{types.NewOkResult(0)},
				},
			},
			{
				Query: "show create table t",
				Expected: []sql.Row{sql.Row{"t",
					"CREATE TABLE `t` (\n" +
						"  `c1` int NOT NULL,\n" +
						"  `c2` int,\n" +
						"  `c3` int,\n" +
						"  `c4` int,\n" +
						"  PRIMARY KEY (`c1`),\n" +
						"  UNIQUE KEY `uniq` (`c4`)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query: "select * from t",
				Expected: []sql.Row{
					{1, 1, 1, nil},
					{2, 2, 2, nil},
					{3, 3, 3, nil},
				},
			},
		},
	},
	{
		Name: "ALTER TABLE does not change column collations",
		SetUpScript: []string{
			"CREATE TABLE test1 (v1 VARCHAR(200), v2 ENUM('a'), v3 SET('a'));",
			"CREATE TABLE test2 (v1 VARCHAR(200), v2 ENUM('a'), v3 SET('a')) COLLATE=utf8mb4_general_ci;",
			"CREATE TABLE test3 (v1 VARCHAR(200) COLLATE utf8mb4_general_ci, v2 ENUM('a'), v3 SET('a') CHARACTER SET utf8mb3) COLLATE=utf8mb4_general_ci",
			"CREATE TABLE test4 (v1 VARCHAR(200) COLLATE utf8mb4_0900_ai_ci, v2 ENUM('a') COLLATE utf8mb4_general_ci, v3 SET('a') COLLATE utf8mb4_unicode_ci) COLLATE=utf8mb4_bin;",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW CREATE TABLE test1",
				Expected: []sql.Row{{"test1",
					"CREATE TABLE `test1` (\n" +
						"  `v1` varchar(200),\n" +
						"  `v2` enum('a'),\n" +
						"  `v3` set('a')\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query: "SHOW CREATE TABLE test2",
				Expected: []sql.Row{{"test2",
					"CREATE TABLE `test2` (\n" +
						"  `v1` varchar(200),\n" +
						"  `v2` enum('a'),\n" +
						"  `v3` set('a')\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci"}},
			},
			{
				Query: "SHOW CREATE TABLE test3",
				Expected: []sql.Row{{"test3",
					"CREATE TABLE `test3` (\n" +
						"  `v1` varchar(200),\n" +
						"  `v2` enum('a'),\n" +
						"  `v3` set('a') CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci"}},
			},
			{
				Query: "SHOW CREATE TABLE test4",
				Expected: []sql.Row{{"test4",
					"CREATE TABLE `test4` (\n" +
						"  `v1` varchar(200) COLLATE utf8mb4_0900_ai_ci,\n" +
						"  `v2` enum('a') COLLATE utf8mb4_general_ci,\n" +
						"  `v3` set('a') COLLATE utf8mb4_unicode_ci\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"}},
			},
			{
				Query:    "ALTER TABLE test1 COLLATE utf8mb4_general_ci;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "ALTER TABLE test2 COLLATE utf8mb4_0900_bin;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "ALTER TABLE test3 COLLATE utf8mb4_0900_bin;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "ALTER TABLE test4 COLLATE utf8mb4_unicode_ci;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query: "SHOW CREATE TABLE test1",
				Expected: []sql.Row{{"test1",
					"CREATE TABLE `test1` (\n" +
						"  `v1` varchar(200) COLLATE utf8mb4_0900_bin,\n" +
						"  `v2` enum('a') COLLATE utf8mb4_0900_bin,\n" +
						"  `v3` set('a') COLLATE utf8mb4_0900_bin\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci"}},
			},
			{
				Query: "SHOW CREATE TABLE test2",
				Expected: []sql.Row{{"test2",
					"CREATE TABLE `test2` (\n" +
						"  `v1` varchar(200) COLLATE utf8mb4_general_ci,\n" +
						"  `v2` enum('a') COLLATE utf8mb4_general_ci,\n" +
						"  `v3` set('a') COLLATE utf8mb4_general_ci\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query: "SHOW CREATE TABLE test3",
				Expected: []sql.Row{{"test3",
					"CREATE TABLE `test3` (\n" +
						"  `v1` varchar(200) COLLATE utf8mb4_general_ci,\n" +
						"  `v2` enum('a') COLLATE utf8mb4_general_ci,\n" +
						"  `v3` set('a') CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query: "SHOW CREATE TABLE test4",
				Expected: []sql.Row{{"test4",
					"CREATE TABLE `test4` (\n" +
						"  `v1` varchar(200) COLLATE utf8mb4_0900_ai_ci,\n" +
						"  `v2` enum('a') COLLATE utf8mb4_general_ci,\n" +
						"  `v3` set('a')\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci"}},
			},
		},
	},
	{
		Name: "ALTER TABLE ... ALTER ADD CHECK / DROP CHECK",
		SetUpScript: []string{
			"CREATE TABLE test (pk BIGINT PRIMARY KEY, v1 BIGINT NOT NULL DEFAULT 88);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "ALTER TABLE test ADD CONSTRAINT cx CHECK (v1 < 100)",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "ALTER TABLE test DROP CHECK cx, ADD CHECK (v1 < 50)",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:       "INSERT INTO test VALUES (1, 99)",
				ExpectedErr: sql.ErrCheckConstraintViolated,
			},
			{
				Query:    "INSERT INTO test VALUES (2, 2)",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
		},
	},
	{
		Name: "ALTER TABLE AUTO INCREMENT no-ops on table with no original auto increment key",
		SetUpScript: []string{
			"CREATE table test (pk int primary key)",
			"ALTER TABLE `test` auto_increment = 2;",
			"INSERT INTO test VALUES (1)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT * FROM test",
				Expected: []sql.Row{{1}},
			},
		},
	},
	{
		Name: "ALTER TABLE MODIFY column with UNIQUE KEY",
		SetUpScript: []string{
			"CREATE table test (pk int primary key, uk int unique)",
			"ALTER TABLE `test` MODIFY column uk int auto_increment",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "describe test",
				Expected: []sql.Row{
					{"pk", "int", "NO", "PRI", "NULL", ""},
					{"uk", "int", "YES", "UNI", "NULL", "auto_increment"},
				},
			},
		},
	},
	{
		Name: "ALTER TABLE MODIFY column making UNIQUE",
		SetUpScript: []string{
			"CREATE table test (pk int primary key, uk int)",
			"ALTER TABLE `test` MODIFY column uk int unique",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "INSERT INTO test VALUES (1, 1), (2, 1)",
				ExpectedErr: sql.ErrUniqueKeyViolation,
			},
		},
	},
	{
		Name: "ALTER TABLE MODIFY column with KEY",
		SetUpScript: []string{
			"CREATE table test (pk int primary key, mk int, index (mk))",
			"ALTER TABLE `test` MODIFY column mk int auto_increment",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "describe test",
				Expected: []sql.Row{
					{"pk", "int", "NO", "PRI", "NULL", ""},
					{"mk", "int", "YES", "MUL", "NULL", "auto_increment"},
				},
			},
		},
	},
	{
		Name: "ALTER TABLE AUTO INCREMENT no-ops on table with no original auto increment key",
		SetUpScript: []string{
			"CREATE table test (pk int primary key)",
			"ALTER TABLE `test` auto_increment = 2;",
			"INSERT INTO test VALUES (1)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT * FROM test",
				Expected: []sql.Row{{1}},
			},
		},
	},
	{
		Name: "Identifier lengths",
		SetUpScript: []string{
			"create table t1 (a int primary key, b int)",
			"create table parent (a int primary key)",
		},
		Assertions: []ScriptTestAssertion{
			{
				// 64 characters
				Query:    "alter table t1 rename to abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijkl",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				// reset name
				Query:    "alter table abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijkl rename to t1",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				// 65 characters
				Query:       "alter table t1 rename to abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklm",
				ExpectedErr: sql.ErrInvalidIdentifier,
			},
			{
				// 64 characters
				Query:    "alter table t1 rename column a to abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijkl",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				// reset name
				Query:    "alter table t1 rename column abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijkl to a",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				// 65 characters
				Query:       "alter table t1 rename column a to abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklm",
				ExpectedErr: sql.ErrInvalidIdentifier,
			},
			{
				// 64 characters
				Query:    "alter table t1 add constraint abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijkl check (a > 0)",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				// 65 characters
				Query:       "alter table t1 add constraint abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklm check (a > 0)",
				ExpectedErr: sql.ErrInvalidIdentifier,
			},
			{
				// 64 characters
				Query:    "alter table t1 add constraint abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijk0 foreign key(a) references parent(a)",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				// 65 characters
				Query:       "alter table t1 add constraint abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklm foreign key(a) references parent(a)",
				ExpectedErr: sql.ErrInvalidIdentifier,
			},
			{
				// 64 characters
				Query:    "alter table t1 add constraint abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijk1 unique key(a)",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				// 65 characters
				Query:       "alter table t1 add constraint abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklm unique key(a)",
				ExpectedErr: sql.ErrInvalidIdentifier,
			},
			{
				// 64 characters
				Query:    "alter table t1 rename index abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijk1 to abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijk2",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				// 65 characters
				Query:       "alter table t1 rename index abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijk2 to abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklm",
				ExpectedErr: sql.ErrInvalidIdentifier,
			},
			{
				// 64 characters
				Query:    "alter table t1 add column abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijk2 int",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				// 65 characters
				Query:       "alter table t1 add column abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklm int",
				ExpectedErr: sql.ErrInvalidIdentifier,
			},
			{
				// 64 characters
				Query:    "alter table t1 change column abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijk2 abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijk3 int",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				// 65 characters
				Query:       "alter table t1 change column abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijk3 abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklm int",
				ExpectedErr: sql.ErrInvalidIdentifier,
			},
			{
				// 64 characters
				Query:    "alter table t1 add index abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijk3 (b)",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				// 65 characters
				Query:       "alter table t1 add index abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklm (b)",
				ExpectedErr: sql.ErrInvalidIdentifier,
			},
			{
				// test of the same in an alter block
				Query:       "alter table t1 add column d int, add index abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklm (b)",
				ExpectedErr: sql.ErrInvalidIdentifier,
			},
		},
	},
	{
		Name: "Add a column with the same case-insensitive name",
		SetUpScript: []string{
			"create table t1 (abc int primary key, def int)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "alter table t1 add column ABC int",
				ExpectedErr: sql.ErrColumnExists,
			},
		},
	},
}

var AlterTableAddAutoIncrementScripts = []ScriptTest{
	{
		Name: "Add primary key column with auto increment",
		SetUpScript: []string{
			"CREATE TABLE t1 (i int, j int);",
			"insert into t1 values (1,1), (2,2), (3,3)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "alter table t1 add column pk int primary key auto_increment;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query: "show create table t1",
				Expected: []sql.Row{{"t1",
					"CREATE TABLE `t1` (\n" +
						"  `i` int,\n" +
						"  `j` int,\n" +
						"  `pk` int NOT NULL AUTO_INCREMENT,\n" +
						"  PRIMARY KEY (`pk`)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query: "select pk from t1 order by pk",
				Expected: []sql.Row{
					{1}, {2}, {3},
				},
			},
		},
	},
	{
		Name: "Add primary key column with auto increment, first",
		SetUpScript: []string{
			"CREATE TABLE t1 (i int, j int);",
			"insert into t1 values (1,1), (2,2), (3,3)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "alter table t1 add column pk int primary key",
				ExpectedErr: sql.ErrPrimaryKeyViolation,
			},
			{
				Query:    "alter table t1 add column pk int primary key auto_increment first",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query: "show create table t1",
				Expected: []sql.Row{{"t1",
					"CREATE TABLE `t1` (\n" +
						"  `pk` int NOT NULL AUTO_INCREMENT,\n" +
						"  `i` int,\n" +
						"  `j` int,\n" +
						"  PRIMARY KEY (`pk`)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query: "select pk from t1 order by pk",
				Expected: []sql.Row{
					{1}, {2}, {3},
				},
			},
		},
	},
	{
		Name: "add column auto_increment, non primary key",
		SetUpScript: []string{
			"CREATE TABLE t1 (i bigint primary key, s varchar(20))",
			"INSERT INTO t1 VALUES (1, 'a'), (2, 'b'), (3, 'c')",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "alter table t1 add column j int auto_increment unique",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query: "show create table t1",
				Expected: []sql.Row{{"t1",
					"CREATE TABLE `t1` (\n" +
						"  `i` bigint NOT NULL,\n" +
						"  `s` varchar(20),\n" +
						"  `j` int AUTO_INCREMENT,\n" +
						"  PRIMARY KEY (`i`),\n" +
						"  UNIQUE KEY `j` (`j`)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query: "select * from t1 order by i",
				Expected: []sql.Row{
					{1, "a", 1},
					{2, "b", 2},
					{3, "c", 3},
				},
			},
		},
	},
	{
		Name: "add column auto_increment, non key",
		SetUpScript: []string{
			"CREATE TABLE t1 (i bigint primary key, s varchar(20))",
			"INSERT INTO t1 VALUES (1, 'a'), (2, 'b'), (3, 'c')",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "alter table t1 add column j int auto_increment",
				ExpectedErr: sql.ErrInvalidAutoIncCols,
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
						"  `v` varchar(20) NOT NULL DEFAULT (concat(pk,'-foo')),\n" +
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
						"  `v` varchar(20) NOT NULL DEFAULT (concat(pk,'-foo')),\n" +
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
						"  `v` varchar(20) NOT NULL DEFAULT (concat(pk,'-foo'))\n" +
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
}
