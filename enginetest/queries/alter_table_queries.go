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
	"github.com/dolthub/go-mysql-server/sql/analyzer/analyzererrors"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"
)

var AlterTableScripts = []ScriptTest{
	{
		Name: "multi alter with invalid schemas",
		SetUpScript: []string{
			"CREATE TABLE t(a int primary key)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "alter table t add column b varchar(16383)",
				ExpectedErr: analyzererrors.ErrInvalidRowLength,
			},
			{
				// 1 char = 4 bytes with default collation
				Query:       "alter table t add column b varchar(16000), add column c varchar(16000)",
				ExpectedErr: analyzererrors.ErrInvalidRowLength,
			},
			{
				Query:    "alter table t add column b varchar(16000), add column c varchar(10)",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
		},
	},
	{
		Name: "variety of alter column statements in a single statement",
		SetUpScript: []string{
			"CREATE TABLE t32(pk BIGINT PRIMARY KEY, v1 int, v2 int, v3 int default (v1), toRename int)",
			`alter table t32 add column v4 int after pk,
			drop column v2, modify v1 varchar(100) not null,
			alter column v3 set default 100, rename column toRename to newName`,
			"CREATE TABLE t32_2(pk BIGINT PRIMARY KEY, v1 int, v2 int, v3 int)",
			`alter table t32_2 drop v1, add v1 int`,
			"CREATE TABLE t32_3(pk BIGINT PRIMARY KEY, v1 int, v2 int, v3 int)",
			`alter table t32_3 rename column v1 to v5, add v1 int`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW FULL COLUMNS FROM t32",
				// | Field | Type | Collation | Null | Key | Default | Extra | Privileges | Comment |
				Expected: []sql.UntypedSqlRow{
					{"pk", "bigint", nil, "NO", "PRI", nil, "", "", ""},
					{"v4", "int", nil, "YES", "", nil, "", "", ""},
					{"v1", "varchar(100)", "utf8mb4_0900_bin", "NO", "", nil, "", "", ""},
					{"v3", "int", nil, "YES", "", "100", "", "", ""},
					{"newName", "int", nil, "YES", "", nil, "", "", ""},
				},
			},
			{
				Query: "SHOW FULL COLUMNS FROM t32_2",
				Expected: []sql.UntypedSqlRow{
					{"pk", "bigint", nil, "NO", "PRI", nil, "", "", ""},
					{"v2", "int", nil, "YES", "", nil, "", "", ""},
					{"v3", "int", nil, "YES", "", nil, "", "", ""},
					{"v1", "int", nil, "YES", "", nil, "", "", ""},
				},
			},
			{
				Query: "SHOW FULL COLUMNS FROM t32_3",
				Expected: []sql.UntypedSqlRow{
					{"pk", "bigint", nil, "NO", "PRI", nil, "", "", ""},
					{"v5", "int", nil, "YES", "", nil, "", "", ""},
					{"v2", "int", nil, "YES", "", nil, "", "", ""},
					{"v3", "int", nil, "YES", "", nil, "", "", ""},
					{"v1", "int", nil, "YES", "", nil, "", "", ""},
				},
			},
			{
				Query:       "alter table t32 add column vnew int, drop column vnew",
				ExpectedErr: sql.ErrTableColumnNotFound,
			},
			{
				Query:       "alter table t32 rename column v3 to v5, drop column v5",
				ExpectedErr: sql.ErrTableColumnNotFound,
			},
			{
				Query:       "alter table t32 rename column v3 to v5, drop column v3",
				ExpectedErr: sql.ErrTableColumnNotFound,
			},
		},
	},
	{
		Name: "mix of alter column, add and drop constraints in one statement",
		SetUpScript: []string{
			"CREATE TABLE t33(pk BIGINT PRIMARY KEY, v1 int, v2 int)",
			`alter table t33 add column v4 int after pk,
			drop column v2, add constraint v1gt0 check (v1 > 0)`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW FULL COLUMNS FROM t33",
				// | Field | Type | Collation | Null | Key | Default | Extra | Privileges | Comment |
				Expected: []sql.UntypedSqlRow{
					{"pk", "bigint", nil, "NO", "PRI", nil, "", "", ""},
					{"v4", "int", nil, "YES", "", nil, "", "", ""},
					{"v1", "int", nil, "YES", "", nil, "", "", ""},
				},
			},
			{
				Query: "SELECT * FROM information_schema.CHECK_CONSTRAINTS",
				Expected: []sql.UntypedSqlRow{
					{"def", "mydb", "v1gt0", "(v1 > 0)"},
				},
			},
		},
	},
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
			"create table t (pk int primary key, col1 timestamp(6) default current_timestamp(6), col2 varchar(1000), index idx1 (pk, col1));",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "alter table t alter column col2 DROP DEFAULT;",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query:    "show create table t;",
				Expected: []sql.UntypedSqlRow{{"t", "CREATE TABLE `t` (\n  `pk` int NOT NULL,\n  `col1` timestamp(6) DEFAULT CURRENT_TIMESTAMP(6),\n  `col2` varchar(1000),\n  PRIMARY KEY (`pk`),\n  KEY `idx1` (`pk`,`col1`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:    "alter table t alter column col2 SET DEFAULT 'FOO!';",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query:    "show create table t;",
				Expected: []sql.UntypedSqlRow{{"t", "CREATE TABLE `t` (\n  `pk` int NOT NULL,\n  `col1` timestamp(6) DEFAULT CURRENT_TIMESTAMP(6),\n  `col2` varchar(1000) DEFAULT 'FOO!',\n  PRIMARY KEY (`pk`),\n  KEY `idx1` (`pk`,`col1`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:    "alter table t drop index idx1;",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
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
				Expected: []sql.UntypedSqlRow{{"t34", "CREATE TABLE `t34` (\n" +
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
				Expected: []sql.UntypedSqlRow{{"t42",
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
				Expected: []sql.UntypedSqlRow{{"t42",
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
				Expected: []sql.UntypedSqlRow{{"t42", "CREATE TABLE `t42` (\n" +
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
				Expected: []sql.UntypedSqlRow{{"t41", "CREATE TABLE `t41` (\n" +
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
				Expected: []sql.UntypedSqlRow{{"t43", "CREATE TABLE `t43` (\n" +
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
				Expected: []sql.UntypedSqlRow{{"t35", "CREATE TABLE `t35` (\n" +
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
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query:    "ALTER TABLE t39 ADD UNIQUE u_col1_col2 (col1, col2)",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query:    "ALTER TABLE t38 DROP INDEX u_col1;",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query:    "INSERT INTO t38 VALUES (5, 1);",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
			},
			{
				Query:       "ALTER TABLE t38 ADD UNIQUE u_col1 (col1)",
				ExpectedErr: sql.ErrUniqueKeyViolation,
			},
			{
				Query: "show create table t38;",
				Expected: []sql.UntypedSqlRow{{"t38", "CREATE TABLE `t38` (\n" +
					"  `pk` int NOT NULL,\n" +
					"  `col1` int,\n" +
					"  PRIMARY KEY (`pk`)\n" +
					") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:    "ALTER TABLE t39 DROP INDEX u_col1_col2;",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query:    "INSERT INTO t39 VALUES (10, 1, 1);",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
			},
			{
				Query:       "ALTER TABLE t39 ADD UNIQUE u_col1_col2 (col1, col2)",
				ExpectedErr: sql.ErrUniqueKeyViolation,
			},
			{
				Query: "show create table t39;",
				Expected: []sql.UntypedSqlRow{{"t39", "CREATE TABLE `t39` (\n" +
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
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query: "describe t40",
				Expected: []sql.UntypedSqlRow{
					{"pk", "int", "NO", "PRI", nil, ""},
					{"val", "int", "YES", "", nil, ""},
				},
			},
			{
				Query:       "INSERT INTO t40 VALUES (NULL, 4)",
				ExpectedErr: sql.ErrInsertIntoNonNullableProvidedNull,
			},
			{
				Query:    "drop table t40",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query:    "CREATE TABLE t40 (pk int AUTO_INCREMENT PRIMARY KEY, val int)",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query: "INSERT INTO t40 VALUES (NULL, 1)",
				Expected: []sql.UntypedSqlRow{{types.OkResult{
					RowsAffected: 1,
					InsertID:     1,
				}}},
			},
			{
				Query:    "SELECT * FROM t40",
				Expected: []sql.UntypedSqlRow{{1, 1}},
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
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query: "show create table t1",
				Expected: []sql.UntypedSqlRow{{"t1", "CREATE TABLE `t1` (\n" +
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
				Expected: []sql.UntypedSqlRow{
					{types.NewOkResult(0)},
				},
			},
			{
				Query: "show create table t",
				Expected: []sql.UntypedSqlRow{{"t",
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
				Expected: []sql.UntypedSqlRow{
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
				Expected: []sql.UntypedSqlRow{{"test1",
					"CREATE TABLE `test1` (\n" +
						"  `v1` varchar(200),\n" +
						"  `v2` enum('a'),\n" +
						"  `v3` set('a')\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query: "SHOW CREATE TABLE test2",
				Expected: []sql.UntypedSqlRow{{"test2",
					"CREATE TABLE `test2` (\n" +
						"  `v1` varchar(200),\n" +
						"  `v2` enum('a'),\n" +
						"  `v3` set('a')\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci"}},
			},
			{
				Query: "SHOW CREATE TABLE test3",
				Expected: []sql.UntypedSqlRow{{"test3",
					"CREATE TABLE `test3` (\n" +
						"  `v1` varchar(200),\n" +
						"  `v2` enum('a'),\n" +
						"  `v3` set('a') CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci"}},
			},
			{
				Query: "SHOW CREATE TABLE test4",
				Expected: []sql.UntypedSqlRow{{"test4",
					"CREATE TABLE `test4` (\n" +
						"  `v1` varchar(200) COLLATE utf8mb4_0900_ai_ci,\n" +
						"  `v2` enum('a') COLLATE utf8mb4_general_ci,\n" +
						"  `v3` set('a') COLLATE utf8mb4_unicode_ci\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"}},
			},
			{
				Query:    "ALTER TABLE test1 COLLATE utf8mb4_general_ci;",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query:    "ALTER TABLE test2 COLLATE utf8mb4_0900_bin;",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query:    "ALTER TABLE test3 COLLATE utf8mb4_0900_bin;",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query:    "ALTER TABLE test4 COLLATE utf8mb4_unicode_ci;",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query: "SHOW CREATE TABLE test1",
				Expected: []sql.UntypedSqlRow{{"test1",
					"CREATE TABLE `test1` (\n" +
						"  `v1` varchar(200) COLLATE utf8mb4_0900_bin,\n" +
						"  `v2` enum('a') COLLATE utf8mb4_0900_bin,\n" +
						"  `v3` set('a') COLLATE utf8mb4_0900_bin\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci"}},
			},
			{
				Query: "SHOW CREATE TABLE test2",
				Expected: []sql.UntypedSqlRow{{"test2",
					"CREATE TABLE `test2` (\n" +
						"  `v1` varchar(200) COLLATE utf8mb4_general_ci,\n" +
						"  `v2` enum('a') COLLATE utf8mb4_general_ci,\n" +
						"  `v3` set('a') COLLATE utf8mb4_general_ci\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query: "SHOW CREATE TABLE test3",
				Expected: []sql.UntypedSqlRow{{"test3",
					"CREATE TABLE `test3` (\n" +
						"  `v1` varchar(200) COLLATE utf8mb4_general_ci,\n" +
						"  `v2` enum('a') COLLATE utf8mb4_general_ci,\n" +
						"  `v3` set('a') CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query: "SHOW CREATE TABLE test4",
				Expected: []sql.UntypedSqlRow{{"test4",
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
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query:    "ALTER TABLE test DROP CHECK cx, ADD CHECK (v1 < 50)",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query:       "INSERT INTO test VALUES (1, 99)",
				ExpectedErr: sql.ErrCheckConstraintViolated,
			},
			{
				Query:    "INSERT INTO test VALUES (2, 2)",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
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
				Expected: []sql.UntypedSqlRow{{1}},
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
				Expected: []sql.UntypedSqlRow{
					{"pk", "int", "NO", "PRI", nil, ""},
					{"uk", "int", "NO", "UNI", nil, "auto_increment"},
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
				Expected: []sql.UntypedSqlRow{
					{"pk", "int", "NO", "PRI", nil, ""},
					{"mk", "int", "NO", "MUL", nil, "auto_increment"},
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
				Expected: []sql.UntypedSqlRow{{1}},
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
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				// reset name
				Query:    "alter table abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijkl rename to t1",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				// 65 characters
				Query:       "alter table t1 rename to abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklm",
				ExpectedErr: sql.ErrInvalidIdentifier,
			},
			{
				// 64 characters
				Query:    "alter table t1 rename column a to abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijkl",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				// reset name
				Query:    "alter table t1 rename column abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijkl to a",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				// 65 characters
				Query:       "alter table t1 rename column a to abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklm",
				ExpectedErr: sql.ErrInvalidIdentifier,
			},
			{
				// 64 characters
				Query:    "alter table t1 add constraint abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijkl check (a > 0)",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				// 65 characters
				Query:       "alter table t1 add constraint abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklm check (a > 0)",
				ExpectedErr: sql.ErrInvalidIdentifier,
			},
			{
				// 64 characters
				Query:    "alter table t1 add constraint abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijk0 foreign key(a) references parent(a)",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				// 65 characters
				Query:       "alter table t1 add constraint abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklm foreign key(a) references parent(a)",
				ExpectedErr: sql.ErrInvalidIdentifier,
			},
			{
				// 64 characters
				Query:    "alter table t1 add constraint abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijk1 unique key(a)",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				// 65 characters
				Query:       "alter table t1 add constraint abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklm unique key(a)",
				ExpectedErr: sql.ErrInvalidIdentifier,
			},
			{
				// 64 characters
				Query:    "alter table t1 rename index abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijk1 to abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijk2",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				// 65 characters
				Query:       "alter table t1 rename index abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijk2 to abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklm",
				ExpectedErr: sql.ErrInvalidIdentifier,
			},
			{
				// 64 characters
				Query:    "alter table t1 add column abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijk2 int",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				// 65 characters
				Query:       "alter table t1 add column abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklm int",
				ExpectedErr: sql.ErrInvalidIdentifier,
			},
			{
				// 64 characters
				Query:    "alter table t1 change column abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijk2 abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijk3 int",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				// 65 characters
				Query:       "alter table t1 change column abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijk3 abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklm int",
				ExpectedErr: sql.ErrInvalidIdentifier,
			},
			{
				// 64 characters
				Query:    "alter table t1 add index abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijk3 (b)",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
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
	{
		Name: "Prefix index with same columns as another index",
		SetUpScript: []string{
			"CREATE table t (pk int primary key, col1 varchar(100));",
			"INSERT into t values (1, '100'), (2, '200');",
			"alter table t add unique index idx1 (col1);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "alter table t add index idx2 (col1(10));",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query: "show create table t",
				Expected: []sql.UntypedSqlRow{{"t",
					"CREATE TABLE `t` (\n" +
						"  `pk` int NOT NULL,\n" +
						"  `col1` varchar(100),\n" +
						"  PRIMARY KEY (`pk`),\n" +
						"  UNIQUE KEY `idx1` (`col1`),\n" +
						"  KEY `idx2` (`col1`(10))\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
		},
	},
	{
		Name: "Index case-insensitivity",
		SetUpScript: []string{
			"create table t1 (i int, KEY myIndex1 (`i`))",
			"create table t2 (i int, KEY myIndex2 (`i`))",
			"create table t3 (i int, KEY myIndex3 (`i`))",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "alter table t1 drop index MYINDEX1;",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query:    "show indexes from t1;",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "alter table t2 rename index myIndex2 to mySecondIndex;",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query:    "show indexes from t2;",
				Expected: []sql.UntypedSqlRow{{"t2", 1, "mySecondIndex", 1, "i", nil, 0, nil, nil, "YES", "BTREE", "", "", "YES", nil}},
			},
			{
				Query:    "alter table t3 rename index MYiNDEX3 to anotherIndex;",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query:    "show indexes from t3;",
				Expected: []sql.UntypedSqlRow{{"t3", 1, "anotherIndex", 1, "i", nil, 0, nil, nil, "YES", "BTREE", "", "", "YES", nil}},
			},
		},
	},
	{
		Name: "alter column and rename table work within same transaction",
		SetUpScript: []string{
			"create table t (i int)",
		},
		Assertions: []ScriptTestAssertion{
			{
				SkipResultsCheck: true,
				Query:            "start transaction;",
			},
			{
				SkipResultsCheck: true,
				Query:            "alter table t change i j int",
			},
			{
				SkipResultsCheck: true,
				Query:            "rename table t to tt",
			},
			{
				SkipResultsCheck: true,
				Query:            "commit;",
			},
			{
				Query:    "select j from tt;",
				Expected: []sql.UntypedSqlRow{},
			},
		},
	},
}

var RenameTableScripts = []ScriptTest{
	{
		Name: "simple rename table",
		Assertions: []ScriptTestAssertion{
			{
				Query:    "RENAME TABLE mytable TO newTableName",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query:       "SELECT COUNT(*) FROM mytable",
				ExpectedErr: sql.ErrTableNotFound,
			},
			{
				Query:    "SELECT COUNT(*) FROM newTableName",
				Expected: []sql.UntypedSqlRow{{3}},
			},
		},
	},
	{
		Name: "rename multiple tables in one stmt",
		Assertions: []ScriptTestAssertion{
			{
				Query:    "RENAME TABLE othertable to othertable2, newTableName to mytable",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query:       "SELECT COUNT(*) FROM othertable",
				ExpectedErr: sql.ErrTableNotFound,
			},
			{
				Query:       "SELECT COUNT(*) FROM newTableName",
				ExpectedErr: sql.ErrTableNotFound,
			},
			{
				Query:    "SELECT COUNT(*) FROM mytable",
				Expected: []sql.UntypedSqlRow{{3}},
			},
			{
				Query:    "SELECT COUNT(*) FROM othertable2",
				Expected: []sql.UntypedSqlRow{{3}},
			},
		},
	},
	{
		Name: "error cases",
		Assertions: []ScriptTestAssertion{
			{
				Query:       "ALTER TABLE not_exist RENAME foo",
				ExpectedErr: sql.ErrTableNotFound,
			},
			{
				Query:       "ALTER TABLE emptytable RENAME niltable",
				ExpectedErr: sql.ErrTableAlreadyExists,
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
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query: "show create table t1",
				Expected: []sql.UntypedSqlRow{{"t1",
					"CREATE TABLE `t1` (\n" +
						"  `i` int,\n" +
						"  `j` int,\n" +
						"  `pk` int NOT NULL AUTO_INCREMENT,\n" +
						"  PRIMARY KEY (`pk`)\n" +
						") ENGINE=InnoDB AUTO_INCREMENT=4 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query: "select pk from t1 order by pk",
				Expected: []sql.UntypedSqlRow{
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
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query: "show create table t1",
				Expected: []sql.UntypedSqlRow{{"t1",
					"CREATE TABLE `t1` (\n" +
						"  `pk` int NOT NULL AUTO_INCREMENT,\n" +
						"  `i` int,\n" +
						"  `j` int,\n" +
						"  PRIMARY KEY (`pk`)\n" +
						") ENGINE=InnoDB AUTO_INCREMENT=4 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query: "select pk from t1 order by pk",
				Expected: []sql.UntypedSqlRow{
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
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query: "show create table t1",
				Expected: []sql.UntypedSqlRow{{"t1",
					"CREATE TABLE `t1` (\n" +
						"  `i` bigint NOT NULL,\n" +
						"  `s` varchar(20),\n" +
						"  `j` int NOT NULL AUTO_INCREMENT,\n" +
						"  PRIMARY KEY (`i`),\n" +
						"  UNIQUE KEY `j` (`j`)\n" +
						") ENGINE=InnoDB AUTO_INCREMENT=4 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query: "select * from t1 order by i",
				Expected: []sql.UntypedSqlRow{
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
				Expected: []sql.UntypedSqlRow{{"t1",
					"CREATE TABLE `t1` (\n" +
						"  `i` int,\n" +
						"  `j` int\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:    "alter table t1 add primary key (i, j)",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query: "show create table t1",
				Expected: []sql.UntypedSqlRow{{"t1",
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
				Expected: []sql.UntypedSqlRow{
					{"a1", "a2"},
					{"a2", "a3"},
					{"a3", "a4"},
				},
			},
			{
				Query:    "alter table t1 drop primary key",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query: "select * from t1 order by pk",
				Expected: []sql.UntypedSqlRow{
					{"a1", "a2"},
					{"a2", "a3"},
					{"a3", "a4"},
				},
			},
			{
				Query:    "insert into t1 values ('a1', 'a2')",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
			},
			{
				Query: "select * from t1 order by pk",
				Expected: []sql.UntypedSqlRow{
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
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
			},
			{
				Query:    "alter table t1 add primary key (pk, v)",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query: "show create table t1",
				Expected: []sql.UntypedSqlRow{{"t1",
					"CREATE TABLE `t1` (\n" +
						"  `pk` varchar(20) NOT NULL,\n" +
						"  `v` varchar(20) NOT NULL DEFAULT (concat(`pk`,'-foo')),\n" +
						"  PRIMARY KEY (`pk`,`v`)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:    "alter table t1 drop primary key",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query:    "alter table t1 add index myidx (v)",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query:    "alter table t1 add primary key (pk)",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query:    "insert into t1 values ('a4', 'a3')",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
			},
			{
				Query: "show create table t1",
				Expected: []sql.UntypedSqlRow{{"t1",
					"CREATE TABLE `t1` (\n" +
						"  `pk` varchar(20) NOT NULL,\n" +
						"  `v` varchar(20) NOT NULL DEFAULT (concat(`pk`,'-foo')),\n" +
						"  PRIMARY KEY (`pk`),\n" +
						"  KEY `myidx` (`v`)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query: "select * from t1 where v = 'a3' order by pk",
				Expected: []sql.UntypedSqlRow{
					{"a2", "a3"},
					{"a4", "a3"},
				},
			},
			{
				Query:    "alter table t1 drop primary key",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query:    "truncate t1",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(4)}},
			},
			{
				Query:    "alter table t1 drop index myidx",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query:    "alter table t1 add primary key (pk, v)",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query:    "insert into t1 values ('a1', 'a2'), ('a2', 'a3'), ('a3', 'a4')",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(3)}},
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
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query:       "INSERT INTO t1 (pk, v) values ('a100', 'a3')",
				ExpectedErr: sql.ErrPrimaryKeyViolation,
			},
			{
				Query:    "alter table t1 drop primary key",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query:    "ALTER TABLE t1 ADD PRIMARY KEY (pk, v), DROP PRIMARY KEY",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query: "show create table t1",
				Expected: []sql.UntypedSqlRow{{"t1",
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
				Expected: []sql.UntypedSqlRow{{"tab1",
					"CREATE TABLE `tab1` (\n" +
						"  `pk` int NOT NULL,\n" +
						"  `c1` int,\n" +
						"  PRIMARY KEY (`pk`)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:    "alter table newdb.tab1 drop primary key",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query: "SHOW CREATE TABLE newdb.tab1",
				Expected: []sql.UntypedSqlRow{{"tab1",
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
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query: "SHOW CREATE TABLE test",
				Expected: []sql.UntypedSqlRow{{"test",
					"CREATE TABLE `test` (\n" +
						"  `pk` int NOT NULL,\n" +
						"  `val` int,\n" +
						"  PRIMARY KEY (`pk`)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:    "ALTER TABLE test drop primary key",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query: "SHOW CREATE TABLE test",
				Expected: []sql.UntypedSqlRow{{"test",
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
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(2)}},
			},
			{
				Query: "SELECT * FROM test ORDER BY pk",
				Expected: []sql.UntypedSqlRow{
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
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query:    "ALTER TABLE t DROP PRIMARY KEY;",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query: "show create table t;",
				Expected: []sql.UntypedSqlRow{{"t", "CREATE TABLE `t` (\n" +
					"  `id` int NOT NULL AUTO_INCREMENT,\n" +
					"  `c1` varchar(255),\n" +
					"  UNIQUE KEY `id` (`id`)\n" +
					") ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:    "insert into t (c1) values('two');",
				Expected: []sql.UntypedSqlRow{{types.OkResult{RowsAffected: 1, InsertID: 2}}},
			},
			{
				Query:    "select * from t;",
				Expected: []sql.UntypedSqlRow{{1, "one"}, {2, "two"}},
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
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query:    "ALTER TABLE t DROP PRIMARY KEY;",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query: "show create table t;",
				Expected: []sql.UntypedSqlRow{{"t", "CREATE TABLE `t` (\n" +
					"  `id` int NOT NULL AUTO_INCREMENT,\n" +
					"  `c1` varchar(255),\n" +
					"  KEY `id` (`id`)\n" +
					") ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:    "insert into t (c1) values('two');",
				Expected: []sql.UntypedSqlRow{{types.OkResult{RowsAffected: 1, InsertID: 2}}},
			},
			{
				Query:    "select * from t;",
				Expected: []sql.UntypedSqlRow{{1, "one"}, {2, "two"}},
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
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query:       "ALTER TABLE t DROP PRIMARY KEY;",
				ExpectedErr: sql.ErrWrongAutoKey,
			},
			{
				// Adding a supporting key (i.e the first column is the auto_increment column) allows us to drop the PK
				Query:    "ALTER TABLE t ADD KEY id1c1 (id1, c1);",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query:    "ALTER TABLE t DROP PRIMARY KEY;",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query:    "insert into t (id2, c1) values(-2, 'two');",
				Expected: []sql.UntypedSqlRow{{types.OkResult{RowsAffected: 1, InsertID: 2}}},
			},
			{
				Query:    "select * from t;",
				Expected: []sql.UntypedSqlRow{{1, -1, "one"}, {2, -2, "two"}},
			},
			{
				Query: "show create table t;",
				Expected: []sql.UntypedSqlRow{{"t", "CREATE TABLE `t` (\n" +
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

var AddColumnScripts = []ScriptTest{
	{
		Name: "column at end with default",
		Assertions: []ScriptTestAssertion{
			{
				Query:    "ALTER TABLE mytable ADD COLUMN i2 INT COMMENT 'hello' default 42",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query: "SHOW FULL COLUMNS FROM mytable",
				// | Field | Type | Collation | Null | Key | Default | Extra | Privileges | Comment |
				// TODO: missing privileges
				Expected: []sql.UntypedSqlRow{
					{"i", "bigint", nil, "NO", "PRI", nil, "", "", ""},
					{"s", "varchar(20)", "utf8mb4_0900_bin", "NO", "UNI", nil, "", "", "column s"},
					{"i2", "int", nil, "YES", "", "42", "", "", "hello"},
				},
			},
			{
				Query: "SELECT * FROM mytable ORDER BY i;",
				Expected: []sql.UntypedSqlRow{
					{int64(1), "first row", int32(42)},
					{int64(2), "second row", int32(42)},
					{int64(3), "third row", int32(42)},
				},
			},
		},
	},
	{
		Name: "in middle, no default",
		Assertions: []ScriptTestAssertion{
			{
				Query:    "ALTER TABLE mytable ADD COLUMN s2 TEXT COMMENT 'hello' AFTER i;",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query: "SHOW FULL COLUMNS FROM mytable",
				Expected: []sql.UntypedSqlRow{
					{"i", "bigint", nil, "NO", "PRI", nil, "", "", ""},
					{"s2", "text", "utf8mb4_0900_bin", "YES", "", nil, "", "", "hello"},
					{"s", "varchar(20)", "utf8mb4_0900_bin", "NO", "UNI", nil, "", "", "column s"},
					{"i2", "int", nil, "YES", "", "42", "", "", "hello"},
				},
			},
			{
				Query: "SELECT * FROM mytable ORDER BY i;",
				Expected: []sql.UntypedSqlRow{
					{int64(1), nil, "first row", int32(42)},
					{int64(2), nil, "second row", int32(42)},
					{int64(3), nil, "third row", int32(42)},
				},
			},
			{
				Query:    "insert into mytable values (4, 's2', 'fourth row', 11);",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
			},
			{
				Query:    "update mytable set s2 = 'updated s2' where i2 = 42;",
				Expected: []sql.UntypedSqlRow{{types.OkResult{RowsAffected: 3, Info: plan.UpdateInfo{Matched: 3, Updated: 3}}}},
			},
			{
				Query: "SELECT * FROM mytable ORDER BY i;",
				Expected: []sql.UntypedSqlRow{
					{int64(1), "updated s2", "first row", int32(42)},
					{int64(2), "updated s2", "second row", int32(42)},
					{int64(3), "updated s2", "third row", int32(42)},
					{int64(4), "s2", "fourth row", int32(11)},
				},
			},
		},
	},
	{
		Name: "first with default",
		Assertions: []ScriptTestAssertion{
			{
				Query:    "ALTER TABLE mytable ADD COLUMN s3 VARCHAR(25) COMMENT 'hello' default 'yay' FIRST",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query: "SHOW FULL COLUMNS FROM mytable",
				Expected: []sql.UntypedSqlRow{
					{"s3", "varchar(25)", "utf8mb4_0900_bin", "YES", "", "'yay'", "", "", "hello"},
					{"i", "bigint", nil, "NO", "PRI", nil, "", "", ""},
					{"s2", "text", "utf8mb4_0900_bin", "YES", "", nil, "", "", "hello"},
					{"s", "varchar(20)", "utf8mb4_0900_bin", "NO", "UNI", nil, "", "", "column s"},
					{"i2", "int", nil, "YES", "", "42", "", "", "hello"},
				},
			},
			{
				Query: "SELECT * FROM mytable ORDER BY i;",
				Expected: []sql.UntypedSqlRow{
					{"yay", int64(1), "updated s2", "first row", int32(42)},
					{"yay", int64(2), "updated s2", "second row", int32(42)},
					{"yay", int64(3), "updated s2", "third row", int32(42)},
					{"yay", int64(4), "s2", "fourth row", int32(11)},
				},
			},
		},
	},
	{
		Name: "middle, no default, non null",
		Assertions: []ScriptTestAssertion{
			{
				Query:    "ALTER TABLE mytable ADD COLUMN s4 VARCHAR(1) not null after s3",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query: "SHOW FULL COLUMNS FROM mytable",
				Expected: []sql.UntypedSqlRow{
					{"s3", "varchar(25)", "utf8mb4_0900_bin", "YES", "", "'yay'", "", "", "hello"},
					{"s4", "varchar(1)", "utf8mb4_0900_bin", "NO", "", nil, "", "", ""},
					{"i", "bigint", nil, "NO", "PRI", nil, "", "", ""},
					{"s2", "text", "utf8mb4_0900_bin", "YES", "", nil, "", "", "hello"},
					{"s", "varchar(20)", "utf8mb4_0900_bin", "NO", "UNI", nil, "", "", "column s"},
					{"i2", "int", nil, "YES", "", "42", "", "", "hello"},
				},
			},
			{
				Query: "SELECT * FROM mytable ORDER BY i;",
				Expected: []sql.UntypedSqlRow{
					{"yay", "", int64(1), "updated s2", "first row", int32(42)},
					{"yay", "", int64(2), "updated s2", "second row", int32(42)},
					{"yay", "", int64(3), "updated s2", "third row", int32(42)},
					{"yay", "", int64(4), "s2", "fourth row", int32(11)},
				},
			},
		},
	},
	{
		Name: "multiple in one statement",
		Assertions: []ScriptTestAssertion{
			{
				Query:    "ALTER TABLE mytable ADD COLUMN s5 VARCHAR(26), ADD COLUMN s6 VARCHAR(27)",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query: "SHOW FULL COLUMNS FROM mytable",
				Expected: []sql.UntypedSqlRow{
					{"s3", "varchar(25)", "utf8mb4_0900_bin", "YES", "", "'yay'", "", "", "hello"},
					{"s4", "varchar(1)", "utf8mb4_0900_bin", "NO", "", nil, "", "", ""},
					{"i", "bigint", nil, "NO", "PRI", nil, "", "", ""},
					{"s2", "text", "utf8mb4_0900_bin", "YES", "", nil, "", "", "hello"},
					{"s", "varchar(20)", "utf8mb4_0900_bin", "NO", "UNI", nil, "", "", "column s"},
					{"i2", "int", nil, "YES", "", "42", "", "", "hello"},
					{"s5", "varchar(26)", "utf8mb4_0900_bin", "YES", "", nil, "", "", ""},
					{"s6", "varchar(27)", "utf8mb4_0900_bin", "YES", "", nil, "", "", ""},
				},
			},
			{
				Query: "SELECT * FROM mytable ORDER BY i;",
				Expected: []sql.UntypedSqlRow{
					{"yay", "", int64(1), "updated s2", "first row", int32(42), nil, nil},
					{"yay", "", int64(2), "updated s2", "second row", int32(42), nil, nil},
					{"yay", "", int64(3), "updated s2", "third row", int32(42), nil, nil},
					{"yay", "", int64(4), "s2", "fourth row", int32(11), nil, nil},
				},
			},
		},
	},
	{
		Name: "error cases",
		Assertions: []ScriptTestAssertion{
			{
				Query:       "ALTER TABLE not_exist ADD COLUMN i2 INT COMMENT 'hello'",
				ExpectedErr: sql.ErrTableNotFound,
			},
			{
				Query:       "ALTER TABLE mytable ADD COLUMN b BIGINT COMMENT 'ok' AFTER not_exist",
				ExpectedErr: sql.ErrTableColumnNotFound,
			},
			{
				Query:       "ALTER TABLE mytable ADD COLUMN i BIGINT COMMENT 'ok'",
				ExpectedErr: sql.ErrColumnExists,
			},
			{
				Query:       "ALTER TABLE mytable ADD COLUMN b INT NOT NULL DEFAULT 'yes'",
				ExpectedErr: sql.ErrIncompatibleDefaultType,
			},
			{
				Query:       "ALTER TABLE mytable ADD COLUMN c int, add c int",
				ExpectedErr: sql.ErrColumnExists,
			},
		},
	},
}

var RenameColumnScripts = []ScriptTest{
	{
		Name: "error cases",
		Assertions: []ScriptTestAssertion{
			{
				Query:       "ALTER TABLE mytable RENAME COLUMN i2 TO iX",
				ExpectedErr: sql.ErrTableColumnNotFound,
			},
			{
				Query:       "ALTER TABLE mytable RENAME COLUMN i TO iX, RENAME COLUMN iX TO i2",
				ExpectedErr: sql.ErrTableColumnNotFound,
			},
			{
				Query:       "ALTER TABLE mytable RENAME COLUMN i TO iX, RENAME COLUMN i TO i2",
				ExpectedErr: sql.ErrTableColumnNotFound,
			},
			{
				Query:       "ALTER TABLE mytable RENAME COLUMN i TO S",
				ExpectedErr: sql.ErrColumnExists,
			},
			{
				Query:       "ALTER TABLE mytable RENAME COLUMN i TO n, RENAME COLUMN s TO N",
				ExpectedErr: sql.ErrColumnExists,
			},
		},
	},
	{
		Name: "simple rename column",
		Assertions: []ScriptTestAssertion{
			{
				Query:    "ALTER TABLE mytable RENAME COLUMN i TO i2, RENAME COLUMN s TO s2",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query: "SHOW FULL COLUMNS FROM mytable",
				Expected: []sql.UntypedSqlRow{
					{"i2", "bigint", nil, "NO", "PRI", nil, "", "", ""},
					{"s2", "varchar(20)", "utf8mb4_0900_bin", "NO", "UNI", nil, "", "", "column s"},
				},
			},
			{
				Query: "select * from mytable order by i2 limit 1",
				Expected: []sql.UntypedSqlRow{
					{1, "first row"},
				},
			},
		},
	},
	{
		Name: "rename column preserves table checks",
		SetUpScript: []string{
			"ALTER TABLE mytable ADD CONSTRAINT test_check CHECK (i2 < 12345)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "ALTER TABLE mytable RENAME COLUMN i2 TO i3",
				ExpectedErr: sql.ErrCheckConstraintInvalidatedByColumnAlter,
			},
			{
				Query:    "ALTER TABLE mytable RENAME COLUMN s2 TO s3",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query: `SELECT TC.CONSTRAINT_NAME, CC.CHECK_CLAUSE, TC.ENFORCED 
FROM information_schema.TABLE_CONSTRAINTS TC, information_schema.CHECK_CONSTRAINTS CC 
WHERE TABLE_SCHEMA = 'mydb' AND TABLE_NAME = 'mytable' AND TC.TABLE_SCHEMA = CC.CONSTRAINT_SCHEMA AND TC.CONSTRAINT_NAME = CC.CONSTRAINT_NAME AND TC.CONSTRAINT_TYPE = 'CHECK';`,
				Expected: []sql.UntypedSqlRow{{"test_check", "(i2 < 12345)", "YES"}},
			},
		},
	},
}

var ModifyColumnScripts = []ScriptTest{
	{
		Name: "column at end with default",
		Assertions: []ScriptTestAssertion{
			{
				Query:    "ALTER TABLE mytable MODIFY COLUMN i bigint NOT NULL COMMENT 'modified'",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query: "SHOW FULL COLUMNS FROM mytable /* 1 */",
				Expected: []sql.UntypedSqlRow{
					{"i", "bigint", nil, "NO", "PRI", nil, "", "", "modified"},
					{"s", "varchar(20)", "utf8mb4_0900_bin", "NO", "", nil, "", "", "column s"},
				},
			},
			{
				Query:    "ALTER TABLE mytable MODIFY COLUMN i TINYINT NOT NULL COMMENT 'yes' AFTER s",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query: "SHOW FULL COLUMNS FROM mytable /* 2 */",
				Expected: []sql.UntypedSqlRow{
					{"s", "varchar(20)", "utf8mb4_0900_bin", "NO", "", nil, "", "", "column s"},
					{"i", "tinyint", nil, "NO", "PRI", nil, "", "", "yes"},
				},
			},
			{
				Query:    "ALTER TABLE mytable MODIFY COLUMN i BIGINT NOT NULL COMMENT 'ok' FIRST",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query: "SHOW FULL COLUMNS FROM mytable /* 3 */",
				Expected: []sql.UntypedSqlRow{
					{"i", "bigint", nil, "NO", "PRI", nil, "", "", "ok"},
					{"s", "varchar(20)", "utf8mb4_0900_bin", "NO", "", nil, "", "", "column s"},
				},
			},
			{
				Query:    "ALTER TABLE mytable MODIFY COLUMN s VARCHAR(20) NULL COMMENT 'changed'",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query: "SHOW FULL COLUMNS FROM mytable /* 4 */",
				Expected: []sql.UntypedSqlRow{
					{"i", "bigint", nil, "NO", "PRI", nil, "", "", "ok"},
					{"s", "varchar(20)", "utf8mb4_0900_bin", "YES", "", nil, "", "", "changed"},
				},
			},
		},
	},
	{
		Name:        "auto increment attribute",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "ALTER TABLE mytable MODIFY i BIGINT auto_increment",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query: "SHOW FULL COLUMNS FROM mytable /* 1 */",
				Expected: []sql.UntypedSqlRow{
					{"i", "bigint", nil, "NO", "PRI", nil, "auto_increment", "", ""},
					{"s", "varchar(20)", "utf8mb4_0900_bin", "YES", "", nil, "", "", "changed"},
				},
			},
			{
				Query: "insert into mytable (s) values ('new row')",
			},
			{
				Query:       "ALTER TABLE mytable add column i2 bigint auto_increment",
				ExpectedErr: sql.ErrInvalidAutoIncCols,
			},
			{
				Query: "alter table mytable add column i2 bigint",
			},
			{
				Query:       "ALTER TABLE mytable modify column i2 bigint auto_increment",
				ExpectedErr: sql.ErrInvalidAutoIncCols,
			},
			{
				Query: "SHOW FULL COLUMNS FROM mytable /* 2 */",
				Expected: []sql.UntypedSqlRow{
					{"i", "bigint", nil, "NO", "PRI", nil, "auto_increment", "", ""},
					{"s", "varchar(20)", "utf8mb4_0900_bin", "YES", "", nil, "", "", "changed"},
					{"i2", "bigint", nil, "YES", "", nil, "", "", ""},
				},
			},
			{
				Query:    "ALTER TABLE mytable MODIFY COLUMN i BIGINT NOT NULL COMMENT 'ok' FIRST",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query: "SHOW FULL COLUMNS FROM mytable /* 3 */",
				Expected: []sql.UntypedSqlRow{
					{"i", "bigint", nil, "NO", "PRI", nil, "", "", "ok"},
					{"s", "varchar(20)", "utf8mb4_0900_bin", "YES", "", nil, "", "", "changed"},
					{"i2", "bigint", nil, "YES", "", nil, "", "", ""},
				},
			},
			{
				Query:    "ALTER TABLE mytable MODIFY COLUMN s VARCHAR(20) NULL COMMENT 'changed'",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query: "SHOW FULL COLUMNS FROM mytable /* 4 */",
				Expected: []sql.UntypedSqlRow{
					{"i", "bigint", nil, "NO", "PRI", nil, "", "", "ok"},
					{"s", "varchar(20)", "utf8mb4_0900_bin", "YES", "", nil, "", "", "changed"},
					{"i2", "bigint", nil, "YES", "", nil, "", "", ""},
				},
			},
		},
	},
	{
		Name:        "error cases",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "ALTER TABLE mytable MODIFY not_exist BIGINT NOT NULL COMMENT 'ok' FIRST",
				ExpectedErr: sql.ErrTableColumnNotFound,
			},
			{
				Query:       "ALTER TABLE mytable MODIFY i BIGINT NOT NULL COMMENT 'ok' AFTER not_exist",
				ExpectedErr: sql.ErrTableColumnNotFound,
			},
			{
				Query:       "ALTER TABLE not_exist MODIFY COLUMN i INT NOT NULL COMMENT 'hello'",
				ExpectedErr: sql.ErrTableNotFound,
			},
			{
				Query:       "ALTER TABLE mytable ADD COLUMN b INT NOT NULL DEFAULT 'yes'",
				ExpectedErr: sql.ErrIncompatibleDefaultType,
			},
			{
				Query:       "ALTER TABLE mytable ADD COLUMN c int, add c int",
				ExpectedErr: sql.ErrColumnExists,
			},
		},
	},
}

var DropColumnScripts = []ScriptTest{
	{
		Name: "drop last column",
		SetUpScript: []string{
			"ALTER TABLE mytable DROP COLUMN s",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SHOW FULL COLUMNS FROM mytable",
				Expected: []sql.UntypedSqlRow{{"i", "bigint", nil, "NO", "PRI", nil, "", "", ""}},
			},
			{
				Query:    "select * from mytable order by i",
				Expected: []sql.UntypedSqlRow{{1}, {2}, {3}},
			},
		},
	},
	{
		Name: "drop first column",
		SetUpScript: []string{
			"CREATE TABLE t1 (a int, b varchar(10), c bigint, k bigint primary key)",
			"insert into t1 values (1, 'abc', 2, 3), (4, 'def', 5, 6)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "ALTER TABLE t1 DROP COLUMN a",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query: "SHOW FULL COLUMNS FROM t1",
				Expected: []sql.UntypedSqlRow{
					{"b", "varchar(10)", "utf8mb4_0900_bin", "YES", "", nil, "", "", ""},
					{"c", "bigint", nil, "YES", "", nil, "", "", ""},
					{"k", "bigint", nil, "NO", "PRI", nil, "", "", ""},
				},
			},
			{
				Query: "SELECT * FROM t1 ORDER BY b",
				Expected: []sql.UntypedSqlRow{
					{"abc", 2, 3},
					{"def", 5, 6},
				},
			},
		},
	},
	{
		Name: "drop middle column",
		SetUpScript: []string{
			"CREATE TABLE t2 (a int, b varchar(10), c bigint, k bigint primary key)",
			"insert into t2 values (1, 'abc', 2, 3), (4, 'def', 5, 6)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "ALTER TABLE t2 DROP COLUMN b",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query: "SHOW FULL COLUMNS FROM t2",
				Expected: []sql.UntypedSqlRow{
					{"a", "int", nil, "YES", "", nil, "", "", ""},
					{"c", "bigint", nil, "YES", "", nil, "", "", ""},
					{"k", "bigint", nil, "NO", "PRI", nil, "", "", ""},
				},
			},
			{
				Query: "SELECT * FROM t2 ORDER BY c",
				Expected: []sql.UntypedSqlRow{
					{1, 2, 3},
					{4, 5, 6},
				},
			},
		},
	},
	{
		// TODO: primary key column drops not well supported yet
		Name: "drop primary key column",
		SetUpScript: []string{
			"CREATE TABLE t3 (a int primary key, b varchar(10), c bigint)",
			"insert into t3 values (1, 'abc', 2), (3, 'def', 4)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Skip:     true,
				Query:    "ALTER TABLE t3 DROP COLUMN a",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Skip:  true,
				Query: "SHOW FULL COLUMNS FROM t3",
				Expected: []sql.UntypedSqlRow{
					{"b", "varchar(10)", "utf8mb4_0900_bin", "YES", "", nil, "", "", ""},
					{"c", "bigint", nil, "YES", "", nil, "", "", ""},
				},
			},
			{
				Skip:  true,
				Query: "SELECT * FROM t3 ORDER BY b",
				Expected: []sql.UntypedSqlRow{
					{"abc", 2},
					{"def", 4},
				},
			},
		},
	},
	{
		Name: "error cases",
		SetUpScript: []string{
			"create table t4 (a int primary key, b int, c int default (b+10))",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "ALTER TABLE not_exist DROP COLUMN s",
				ExpectedErr: sql.ErrTableNotFound,
			},
			{
				Query:       "ALTER TABLE mytable DROP COLUMN s",
				ExpectedErr: sql.ErrTableColumnNotFound,
			},
			{
				Query:       "ALTER TABLE t4 DROP COLUMN b",
				ExpectedErr: sql.ErrDropColumnReferencedInDefault,
			},
		},
	},
}

var DropColumnKeylessTablesScripts = []ScriptTest{
	{
		Name: "drop last column",
		SetUpScript: []string{
			"create table t0 (i bigint, s varchar(20))",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "ALTER TABLE t0 DROP COLUMN s",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query:    "SHOW FULL COLUMNS FROM t0",
				Expected: []sql.UntypedSqlRow{{"i", "bigint", nil, "YES", "", nil, "", "", ""}},
			},
		},
	},
	{
		Name: "drop first column",
		SetUpScript: []string{
			"CREATE TABLE t1 (a int, b varchar(10), c bigint)",
			"insert into t1 values (1, 'abc', 2), (4, 'def', 5)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "ALTER TABLE t1 DROP COLUMN a",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query: "SHOW FULL COLUMNS FROM t1",
				Expected: []sql.UntypedSqlRow{
					{"b", "varchar(10)", "utf8mb4_0900_bin", "YES", "", nil, "", "", ""},
					{"c", "bigint", nil, "YES", "", nil, "", "", ""},
				},
			},
			{
				Query: "SELECT * FROM t1 ORDER BY b",
				Expected: []sql.UntypedSqlRow{
					{"abc", 2},
					{"def", 5},
				},
			},
		},
	},
	{
		Name: "drop middle column",
		SetUpScript: []string{
			"CREATE TABLE t2 (a int, b varchar(10), c bigint)",
			"insert into t2 values (1, 'abc', 2), (4, 'def', 5)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "ALTER TABLE t2 DROP COLUMN b",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query: "SHOW FULL COLUMNS FROM t2",
				Expected: []sql.UntypedSqlRow{
					{"a", "int", nil, "YES", "", nil, "", "", ""},
					{"c", "bigint", nil, "YES", "", nil, "", "", ""},
				},
			},
			{
				Query: "SELECT * FROM t2 ORDER BY c",
				Expected: []sql.UntypedSqlRow{
					{1, 2},
					{4, 5},
				},
			},
		},
	},
	{
		Name:        "error cases",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "ALTER TABLE not_exist DROP COLUMN s",
				ExpectedErr: sql.ErrTableNotFound,
			},
			{
				Query:       "ALTER TABLE t0 DROP COLUMN s",
				ExpectedErr: sql.ErrTableColumnNotFound,
			},
			{
				Query: "SELECT * FROM t2 ORDER BY c",
				Expected: []sql.UntypedSqlRow{
					{1, 2},
					{4, 5},
				},
			},
		},
	},
}
