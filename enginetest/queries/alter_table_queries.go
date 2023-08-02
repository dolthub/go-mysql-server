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
			"create table t (pk int primary key, col1 timestamp default current_timestamp(), col2 varchar(1000), index idx1 (pk, col1));",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "alter table t alter column col2 DROP DEFAULT;",
				Expected: []sql.Row{},
			},
			{
				Query:    "show create table t;",
				Expected: []sql.Row{{"t", "CREATE TABLE `t` (\n  `pk` int NOT NULL,\n  `col1` timestamp(6) DEFAULT (CURRENT_TIMESTAMP()),\n  `col2` varchar(1000),\n  PRIMARY KEY (`pk`),\n  KEY `idx1` (`pk`,`col1`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:    "alter table t alter column col2 SET DEFAULT 'FOO!';",
				Expected: []sql.Row{},
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
}
