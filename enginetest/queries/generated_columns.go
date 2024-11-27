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
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"
)

var GeneratedColumnTests = []ScriptTest{
	{
		Name: "stored generated column",
		SetUpScript: []string{
			"create table t1 (a int primary key, b int as (a + 1) stored)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "show create table t1",
				// TODO: double parens here is a bug
				Expected: []sql.UntypedSqlRow{{"t1",
					"CREATE TABLE `t1` (\n" +
						"  `a` int NOT NULL,\n" +
						"  `b` int GENERATED ALWAYS AS ((`a` + 1)) STORED,\n" +
						"  PRIMARY KEY (`a`)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:       "insert into t1 values (1,2)",
				ExpectedErr: sql.ErrGeneratedColumnValue,
			},
			{
				Query:       "insert into t1(a,b) values (1,2)",
				ExpectedErr: sql.ErrGeneratedColumnValue,
			},
			{
				Query:    "select * from t1 order by a",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "insert into t1(a) values (1), (2), (3)",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(3)}},
			},
			{
				Query:    "select * from t1 order by a",
				Expected: []sql.UntypedSqlRow{{1, 2}, {2, 3}, {3, 4}},
			},
			{
				Query:    "insert into t1(a,b) values (4, DEFAULT)",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
			},
			{
				Query:    "select * from t1 where b = 5 order by a",
				Expected: []sql.UntypedSqlRow{{4, 5}},
			},
			{
				Query:       "update t1 set b = b + 1",
				ExpectedErr: sql.ErrGeneratedColumnValue,
			},
			{
				Query:    "update t1 set a = 10 where a = 1",
				Expected: []sql.UntypedSqlRow{{newUpdateResult(1, 1)}},
			},
			{
				Query:    "select * from t1 order by a",
				Expected: []sql.UntypedSqlRow{{2, 3}, {3, 4}, {4, 5}, {10, 11}},
			},
			{
				Query:    "delete from t1 where b = 11",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
			},
			{
				Query:    "select * from t1 order by a",
				Expected: []sql.UntypedSqlRow{{2, 3}, {3, 4}, {4, 5}},
			},
		},
	},
	{
		Name: "Add stored column first with literal",
		SetUpScript: []string{
			"CREATE TABLE t16(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT '4')",
			"INSERT INTO t16 (pk) VALUES (1), (2)",
			"ALTER TABLE t16 ADD COLUMN v2 BIGINT AS (5) STORED FIRST",
		},
		Assertions: []ScriptTestAssertion{{
			Query:    "SELECT * FROM t16",
			Expected: []sql.UntypedSqlRow{{5, 1, 4}, {5, 2, 4}}},
		},
	},
	{
		Name: "Add stored column first with expression",
		SetUpScript: []string{
			"CREATE TABLE t17(pk BIGINT PRIMARY KEY, v1 BIGINT)",
			"INSERT INTO t17 VALUES (1, 3), (2, 4)",
			"ALTER TABLE t17 ADD COLUMN v2 BIGINT AS (v1 + 2) STORED FIRST",
		},
		Assertions: []ScriptTestAssertion{{
			Query:    "SELECT * FROM t17",
			Expected: []sql.UntypedSqlRow{{5, 1, 3}, {6, 2, 4}}},
		},
	},
	{
		Name: "index on stored generated column",
		SetUpScript: []string{
			"create table t1 (a int primary key, b int as (a + 1) stored)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "create index i1 on t1(b)",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query: "show create table t1",
				Expected: []sql.UntypedSqlRow{{"t1",
					"CREATE TABLE `t1` (\n" +
						"  `a` int NOT NULL,\n" +
						"  `b` int GENERATED ALWAYS AS ((`a` + 1)) STORED,\n" +
						"  PRIMARY KEY (`a`),\n" +
						"  KEY `i1` (`b`)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:    "insert into t1(a) values (1), (2)",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(2)}},
			},
			{
				Query:    "select * from t1 where b = 2 order by a",
				Expected: []sql.UntypedSqlRow{{1, 2}},
			},
			{
				Query:    "select * from t1 order by a",
				Expected: []sql.UntypedSqlRow{{1, 2}, {2, 3}},
			},
			{
				Query:    "select * from t1 order by b",
				Expected: []sql.UntypedSqlRow{{1, 2}, {2, 3}},
			},
			{
				Query:    "update t1 set a = 10 where a = 1",
				Expected: []sql.UntypedSqlRow{{newUpdateResult(1, 1)}},
			},
			{
				Query:    "select * from t1 where b = 11 order by a",
				Expected: []sql.UntypedSqlRow{{10, 11}},
			},
			{
				Query:    "delete from t1 where b = 11",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
			},
			{
				Query:    "select * from t1 where b = 3 order by a",
				Expected: []sql.UntypedSqlRow{{2, 3}},
			},
		},
	},
	{
		Name: "creating index on stored generated column",
		SetUpScript: []string{
			"create table t1 (a int primary key, b int as (a + 1) stored)",
			"insert into t1(a) values (1), (2)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "create index i1 on t1(b)",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query: "show create table t1",
				Expected: []sql.UntypedSqlRow{{"t1",
					"CREATE TABLE `t1` (\n" +
						"  `a` int NOT NULL,\n" +
						"  `b` int GENERATED ALWAYS AS ((`a` + 1)) STORED,\n" +
						"  PRIMARY KEY (`a`),\n" +
						"  KEY `i1` (`b`)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:    "select * from t1 where b = 2 order by a",
				Expected: []sql.UntypedSqlRow{{1, 2}},
			},
			{
				Query:    "select * from t1 order by a",
				Expected: []sql.UntypedSqlRow{{1, 2}, {2, 3}},
			},
			{
				Query:    "select * from t1 order by b",
				Expected: []sql.UntypedSqlRow{{1, 2}, {2, 3}},
			},
		},
	},
	{
		Name: "creating index on stored generated column with type conversion",
		SetUpScript: []string{
			"create table t1 (a int primary key, b float generated always as (a + 1) stored)",
			"insert into t1(a) values (1), (2)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "create index i1 on t1(b)",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query: "show create table t1",
				Expected: []sql.UntypedSqlRow{{"t1",
					"CREATE TABLE `t1` (\n" +
						"  `a` int NOT NULL,\n" +
						"  `b` float GENERATED ALWAYS AS ((`a` + 1)) STORED,\n" +
						"  PRIMARY KEY (`a`),\n" +
						"  KEY `i1` (`b`)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:    "select * from t1 where b = 2 order by a",
				Expected: []sql.UntypedSqlRow{{1, float64(2)}},
			},
			{
				Query:    "select * from t1 order by a",
				Expected: []sql.UntypedSqlRow{{1, float64(2)}, {2, float64(3)}},
			},
			{
				Query:    "select * from t1 order by b",
				Expected: []sql.UntypedSqlRow{{1, float64(2)}, {2, float64(3)}},
			},
		},
	},
	{
		Name: "creating index on stored generated column within multi-alter statement",
		SetUpScript: []string{
			"create table t1 (a int primary key, b int as (a + 1) stored)",
			"insert into t1(a) values (1), (2)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "alter table t1 add column c int as (b+1) stored, add index b1(b), add column d int as (b+2) stored",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query: "show create table t1",
				Expected: []sql.UntypedSqlRow{{"t1",
					"CREATE TABLE `t1` (\n" +
						"  `a` int NOT NULL,\n" +
						"  `b` int GENERATED ALWAYS AS ((`a` + 1)) STORED,\n" +
						"  `c` int GENERATED ALWAYS AS ((`b` + 1)) STORED,\n" +
						"  `d` int GENERATED ALWAYS AS ((`b` + 2)) STORED,\n" +
						"  PRIMARY KEY (`a`),\n" +
						"  KEY `b1` (`b`)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:    "select * from t1 where b = 2 order by a",
				Expected: []sql.UntypedSqlRow{{1, 2, 3, 4}},
			},
			{
				Query:    "select * from t1 order by a",
				Expected: []sql.UntypedSqlRow{{1, 2, 3, 4}, {2, 3, 4, 5}},
			},
			{
				Query:    "select * from t1 order by b",
				Expected: []sql.UntypedSqlRow{{1, 2, 3, 4}, {2, 3, 4, 5}},
			},
		},
	},
	{
		Name: "creating unique index on stored generated column",
		SetUpScript: []string{
			"create table t1 (a int primary key, b int as (a * a) stored, c int as (0) stored)",
			"insert into t1(a) values (-1), (-2)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "create unique index i1 on t1(b)",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query: "show create table t1",
				Expected: []sql.UntypedSqlRow{{"t1",
					"CREATE TABLE `t1` (\n" +
						"  `a` int NOT NULL,\n" +
						"  `b` int GENERATED ALWAYS AS ((`a` * `a`)) STORED,\n" +
						"  `c` int GENERATED ALWAYS AS (0) STORED,\n" +
						"  PRIMARY KEY (`a`),\n" +
						"  UNIQUE KEY `i1` (`b`)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:    "select * from t1 where b = 4 order by a",
				Expected: []sql.UntypedSqlRow{{-2, 4, 0}},
			},
			{
				Query:    "select * from t1 order by a",
				Expected: []sql.UntypedSqlRow{{-2, 4, 0}, {-1, 1, 0}},
			},
			{
				Query:    "select * from t1 order by b",
				Expected: []sql.UntypedSqlRow{{-1, 1, 0}, {-2, 4, 0}},
			},
			{
				Query:       "insert into t1(a) values (2)",
				ExpectedErr: sql.ErrUniqueKeyViolation,
			},
			{
				Query:       "create unique index i2 on t1(c)",
				ExpectedErr: sql.ErrUniqueKeyViolation,
			},
		},
	},
	{
		Name: "creating index on virtual generated column",
		SetUpScript: []string{
			"create table t1 (a int primary key, b int as (a + 1) virtual)",
			"insert into t1(a) values (1), (2)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "create index i1 on t1(b)",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query: "show create table t1",
				Expected: []sql.UntypedSqlRow{{"t1",
					"CREATE TABLE `t1` (\n" +
						"  `a` int NOT NULL,\n" +
						"  `b` int GENERATED ALWAYS AS ((`a` + 1)),\n" +
						"  PRIMARY KEY (`a`),\n" +
						"  KEY `i1` (`b`)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
				Skip: true, // https://github.com/dolthub/dolt/issues/8275
			},
			{
				Query:    "select * from t1 where b = 2 order by a",
				Expected: []sql.UntypedSqlRow{{1, 2}},
			},
			{
				Query:    "select * from t1 order by a",
				Expected: []sql.UntypedSqlRow{{1, 2}, {2, 3}},
			},
			{
				Query:    "select * from t1 order by b",
				Expected: []sql.UntypedSqlRow{{1, 2}, {2, 3}},
			},
		},
	},
	{
		Name: "creating index on stored generated column with type conversion",
		SetUpScript: []string{
			"create table t1 (a int primary key, b float generated always as (a + 1) stored)",
			"insert into t1(a) values (1), (2)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "create index i1 on t1(b)",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query: "show create table t1",
				Expected: []sql.UntypedSqlRow{{"t1",
					"CREATE TABLE `t1` (\n" +
						"  `a` int NOT NULL,\n" +
						"  `b` float GENERATED ALWAYS AS ((`a` + 1)) STORED,\n" +
						"  PRIMARY KEY (`a`),\n" +
						"  KEY `i1` (`b`)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:    "select * from t1 where b = 2 order by a",
				Expected: []sql.UntypedSqlRow{{1, float64(2)}},
			},
			{
				Query:    "select * from t1 order by a",
				Expected: []sql.UntypedSqlRow{{1, float64(2)}, {2, float64(3)}},
			},
			{
				Query:    "select * from t1 order by b",
				Expected: []sql.UntypedSqlRow{{1, float64(2)}, {2, float64(3)}},
			},
		},
	},
	{
		Name: "creating index on virtual generated column with type conversion",
		SetUpScript: []string{
			"create table t1 (a int primary key, b float generated always as (a + 1))",
			"insert into t1(a) values (1), (2)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "create index i1 on t1(b)",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query: "show create table t1",
				Expected: []sql.UntypedSqlRow{{"t1",
					"CREATE TABLE `t1` (\n" +
						"  `a` int NOT NULL,\n" +
						"  `b` float GENERATED ALWAYS AS ((`a` + 1)),\n" +
						"  PRIMARY KEY (`a`),\n" +
						"  KEY `i1` (`b`)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
				Skip: true, // https://github.com/dolthub/dolt/issues/8275
			},
			{
				Query:    "select * from t1 where b = 2 order by a",
				Expected: []sql.UntypedSqlRow{{1, float64(2)}},
			},
			{
				Query:    "select * from t1 order by a",
				Expected: []sql.UntypedSqlRow{{1, float64(2)}, {2, float64(3)}},
			},
			{
				Query:    "select * from t1 order by b",
				Expected: []sql.UntypedSqlRow{{1, float64(2)}, {2, float64(3)}},
			},
		},
	},
	{
		Name: "index on stored generated column and one non-generated column",
		SetUpScript: []string{
			"create table t1 (a int primary key, b int as (a + 1) stored, c int)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "create index i1 on t1(b,c)",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query: "show create table t1",
				Expected: []sql.UntypedSqlRow{{"t1",
					"CREATE TABLE `t1` (\n" +
						"  `a` int NOT NULL,\n" +
						"  `b` int GENERATED ALWAYS AS ((`a` + 1)) STORED,\n" +
						"  `c` int,\n" +
						"  PRIMARY KEY (`a`),\n" +
						"  KEY `i1` (`b`,`c`)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:    "insert into t1(a,c) values (1,3)",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
			},
			{
				Query:    "select * from t1 where b = 2 and c = 3 order by a",
				Expected: []sql.UntypedSqlRow{{1, 2, 3}},
			},
			{
				Query:    "insert into t1(a,c) values (2,4)",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
			},
			{
				Query:    "delete from t1 where b = 3 and c = 4",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
			},
			{
				Query:    "select * from t1 order by a",
				Expected: []sql.UntypedSqlRow{{1, 2, 3}},
			},
			{
				Query:    "update t1 set a = 5, c = 10 where b = 2 and c = 3",
				Expected: []sql.UntypedSqlRow{{newUpdateResult(1, 1)}},
			},
			{
				Query:    "select * from t1 where b = 6 and c = 10 order by a",
				Expected: []sql.UntypedSqlRow{{5, 6, 10}},
			},
			{
				Query:    "select * from t1 order by a",
				Expected: []sql.UntypedSqlRow{{5, 6, 10}},
			},
		},
	},
	{
		Name: "add new generated column",
		SetUpScript: []string{
			"create table t1 (a int primary key, b int)",
			"insert into t1 values (1,2), (2,3), (3,4)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "alter table t1 add column c int as (a + b) stored",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query:    "select * from t1 order by a",
				Expected: []sql.UntypedSqlRow{{1, 2, 3}, {2, 3, 5}, {3, 4, 7}},
			},
			{
				Query: "show create table t1",
				Expected: []sql.UntypedSqlRow{{"t1",
					"CREATE TABLE `t1` (\n" +
						"  `a` int NOT NULL,\n" +
						"  `b` int,\n" +
						"  `c` int GENERATED ALWAYS AS ((`a` + `b`)) STORED,\n" +
						"  PRIMARY KEY (`a`)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
		},
	},
	{
		Name: "stored generated column with spaces",
		SetUpScript: []string{
			"create table tt (`col 1` int, `col 2` int);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "create table t (`col 1` int, `col 2` int, `col 3` int generated always as (`col 1` + `col 2` + pow(`col 1`, `col 2`)) stored);",
				Expected: []sql.UntypedSqlRow{
					{types.NewOkResult(0)},
				},
			},
			{
				Query: "show create table t",
				Expected: []sql.UntypedSqlRow{
					{"t", "CREATE TABLE `t` (\n" +
						"  `col 1` int,\n" +
						"  `col 2` int,\n" +
						"  `col 3` int GENERATED ALWAYS AS (((`col 1` + `col 2`) + power(`col 1`, `col 2`))) STORED\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
			{
				Query: "insert into t (`col 1`, `col 2`) values (1, 2);",
				Expected: []sql.UntypedSqlRow{
					{types.NewOkResult(1)},
				},
			},
			{
				Query: "select * from t",
				Expected: []sql.UntypedSqlRow{
					{1, 2, 4},
				},
			},
			{
				Query: "alter table tt add column `col 3` int generated always as (`col 1` + `col 2` + pow(`col 1`, `col 2`)) stored;",
				Expected: []sql.UntypedSqlRow{
					{types.NewOkResult(0)},
				},
			},
			{
				Query: "show create table tt",
				Expected: []sql.UntypedSqlRow{
					{"tt", "CREATE TABLE `tt` (\n" +
						"  `col 1` int,\n" +
						"  `col 2` int,\n" +
						"  `col 3` int GENERATED ALWAYS AS (((`col 1` + `col 2`) + power(`col 1`, `col 2`))) STORED\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
			{
				Query: "insert into tt (`col 1`, `col 2`) values (1, 2);",
				Expected: []sql.UntypedSqlRow{
					{types.NewOkResult(1)},
				},
			},
			{
				Query: "select * from tt",
				Expected: []sql.UntypedSqlRow{
					{1, 2, 4},
				},
			},
		},
	},
	{
		Name: "virtual generated column with spaces",
		SetUpScript: []string{
			"create table tt (`col 1` int, `col 2` int);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "create table t (`col 1` int, `col 2` int, `col 3` int generated always as (`col 1` + `col 2` + pow(`col 1`, `col 2`)) virtual);",
				Expected: []sql.UntypedSqlRow{
					{types.NewOkResult(0)},
				},
			},
			{
				Query: "show create table t",
				Expected: []sql.UntypedSqlRow{
					{"t", "CREATE TABLE `t` (\n" +
						"  `col 1` int,\n" +
						"  `col 2` int,\n" +
						"  `col 3` int GENERATED ALWAYS AS (((`col 1` + `col 2`) + power(`col 1`, `col 2`)))\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
			{
				Query: "insert into t (`col 1`, `col 2`) values (1, 2);",
				Expected: []sql.UntypedSqlRow{
					{types.NewOkResult(1)},
				},
			},
			{
				Query: "select * from t",
				Expected: []sql.UntypedSqlRow{
					{1, 2, 4},
				},
			},
			{
				Query: "alter table tt add column `col 3` int generated always as (`col 1` + `col 2` + pow(`col 1`, `col 2`)) virtual;",
				Expected: []sql.UntypedSqlRow{
					{types.NewOkResult(0)},
				},
			},
			{
				Query: "show create table tt",
				Expected: []sql.UntypedSqlRow{
					{"tt", "CREATE TABLE `tt` (\n" +
						"  `col 1` int,\n" +
						"  `col 2` int,\n" +
						"  `col 3` int GENERATED ALWAYS AS (((`col 1` + `col 2`) + power(`col 1`, `col 2`)))\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
			{
				Query: "insert into tt (`col 1`, `col 2`) values (1, 2);",
				Expected: []sql.UntypedSqlRow{
					{types.NewOkResult(1)},
				},
			},
			{
				Query: "select * from tt",
				Expected: []sql.UntypedSqlRow{
					{1, 2, 4},
				},
			},
		},
	},
	{
		Name: "Add virtual column first with literal",
		SetUpScript: []string{
			"CREATE TABLE t16(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT '4')",
			"INSERT INTO t16 (pk) VALUES (1), (2)",
			"ALTER TABLE t16 ADD COLUMN v2 BIGINT AS (5) VIRTUAL FIRST",
		},
		Assertions: []ScriptTestAssertion{{
			Query:    "SELECT * FROM t16",
			Expected: []sql.UntypedSqlRow{{5, 1, 4}, {5, 2, 4}}},
		},
	},
	{
		Name: "Add virtual column first with expression",
		SetUpScript: []string{
			"CREATE TABLE t17(pk BIGINT PRIMARY KEY, v1 BIGINT)",
			"INSERT INTO t17 VALUES (1, 3), (2, 4)",
			"ALTER TABLE t17 ADD COLUMN v2 BIGINT AS (v1 + 2) VIRTUAL FIRST",
		},
		Assertions: []ScriptTestAssertion{{
			Query:    "SELECT * FROM t17",
			Expected: []sql.UntypedSqlRow{{5, 1, 3}, {6, 2, 4}}},
		},
	},
	{
		Name: "virtual column inserts, updates, deletes",
		SetUpScript: []string{
			"create table t1 (a int primary key, b int generated always as (a + 1) virtual)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "insert into t1 (a) values (1), (2), (3)",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(3)}},
			},
			{
				Query:    "select * from t1 order by a",
				Expected: []sql.UntypedSqlRow{{1, 2}, {2, 3}, {3, 4}},
			},
			{
				Query: "update t1 set a = 4 where a = 3",
				Expected: []sql.UntypedSqlRow{{types.OkResult{
					RowsAffected: 1,
					Info: plan.UpdateInfo{
						Matched: 1,
						Updated: 1,
					}},
				}},
			},
			{
				Query:    "select * from t1 order by a",
				Expected: []sql.UntypedSqlRow{{1, 2}, {2, 3}, {4, 5}},
			},
			{
				Query:    "delete from t1 where a = 2",
				Expected: []sql.UntypedSqlRow{{types.OkResult{RowsAffected: 1}}},
			},
			{
				Query:    "select * from t1 order by a",
				Expected: []sql.UntypedSqlRow{{1, 2}, {4, 5}},
			},
			{
				Query:       "update t1 set b = b + 1",
				ExpectedErr: sql.ErrGeneratedColumnValue,
			},
		},
	},
	{
		Name: "virtual column selects",
		SetUpScript: []string{
			"create table t1 (a int primary key, b int generated always as (a + 1) virtual)",
			"create table t2 (c int primary key, d int generated always as (c - 1) virtual)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "insert into t1 (a) values (1), (2), (3)",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(3)}},
			},
			{
				Query:    "select * from t1 order by a",
				Expected: []sql.UntypedSqlRow{{1, 2}, {2, 3}, {3, 4}},
			},
			{
				Query:    "insert into t2 (c) values (1), (2), (3)",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(3)}},
			},
			{
				Query:    "select * from t2 order by c",
				Expected: []sql.UntypedSqlRow{{1, 0}, {2, 1}, {3, 2}},
			},
			{
				Query:    "select * from t1 where b = 2 order by a",
				Expected: []sql.UntypedSqlRow{{1, 2}},
			},
			{
				Query:    "select * from t2 where d = 2 order by c",
				Expected: []sql.UntypedSqlRow{{3, 2}},
			},
			{
				Query:    "select sum(b) from t1 where b > 2",
				Expected: []sql.UntypedSqlRow{{7.0}},
			},
			{
				Query:    "select sum(d) from t2 where d >= 1",
				Expected: []sql.UntypedSqlRow{{3.0}},
			},
			{
				Query:    "select a, (select b from t1 t1a where t1a.a = t1.a+1) from t1 order by a",
				Expected: []sql.UntypedSqlRow{{1, 3}, {2, 4}, {3, nil}},
			},
			{
				Query:    "select c, (select d from t2 t2a where t2a.c = t2.c+1) from t2 order by c",
				Expected: []sql.UntypedSqlRow{{1, 1}, {2, 2}, {3, nil}},
			},
			{
				Query:    "select * from t1 join t2 on a = c order by a",
				Expected: []sql.UntypedSqlRow{{1, 2, 1, 0}, {2, 3, 2, 1}, {3, 4, 3, 2}},
			},
			{
				Query:    "select * from t1 join t2 on a = d order by a",
				Expected: []sql.UntypedSqlRow{{1, 2, 2, 1}, {2, 3, 3, 2}},
			},
			{
				Query:    "select * from t1 join t2 on b = d order by a",
				Expected: []sql.UntypedSqlRow{{1, 2, 3, 2}},
			},
			{
				Query:    "select * from t1 join (select * from t2) as t3 on b = d order by a",
				Expected: []sql.UntypedSqlRow{{1, 2, 3, 2}},
			},
		},
	},
	{
		Name: "virtual column in triggers",
		SetUpScript: []string{
			"create table t1 (a int primary key, b int generated always as (a + 1) virtual)",
			"create table t2 (c int primary key, d int generated always as (c - 1) virtual)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "insert into t1 (a) values (1), (2), (3)",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(3)}},
			},
			{
				Query:    "insert into t2 (c) values (1), (2), (3)",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(3)}},
			},
			{
				Query:    "create trigger t1insert before insert on t1 for each row insert into t2 (c) values (new.b + 1)",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query:    "insert into t1 (a) values (4), (5)",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(2)}},
			},
			{
				Query:    "select * from t1 order by a",
				Expected: []sql.UntypedSqlRow{{1, 2}, {2, 3}, {3, 4}, {4, 5}, {5, 6}},
			},
			{
				Query:    "select * from t2 order by c",
				Expected: []sql.UntypedSqlRow{{1, 0}, {2, 1}, {3, 2}, {6, 5}, {7, 6}},
			},
		},
	},
	{
		Name: "virtual column json extract",
		SetUpScript: []string{
			"create table t1 (a int primary key, j json, b int generated always as (j->>'$.b') virtual)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    `insert into t1 (a, j) values (1, '{"a": 1, "b": 2}'), (2, '{"a": 1}'), (3, '{"b": "300"}')`,
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(3)}},
			},
			{
				Query: "select * from t1 order by a",
				Expected: []sql.UntypedSqlRow{
					{1, types.MustJSON(`{"a": 1, "b": 2}`), 2},
					{2, types.MustJSON(`{"a": 1}`), nil},
					{3, types.MustJSON(`{"b": "300"}`), 300}},
			},
		},
	},
	{
		Name: "virtual column with function",
		SetUpScript: []string{
			"create table t1 (a varchar(255) primary key, b varchar(255), c varchar(512) generated always as (concat(a,b)) virtual)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    `insert into t1 (a, b) values ('abc', '123'), ('def', null), ('ghi', '')`,
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(3)}},
			},
			{
				Query: "select * from t1 order by a",
				Expected: []sql.UntypedSqlRow{
					{"abc", "123", "abc123"},
					{"def", nil, nil},
					{"ghi", "", "ghi"},
				},
			},
		},
	},
	{
		Name: "physical columns added after virtual one",
		SetUpScript: []string{
			"create table t (pk int primary key, col1 int as (pk + 1));",
			"insert into t (pk) values (1), (3)",
			"alter table t add index idx1 (col1, pk);",
			"alter table t add index idx2 (col1);",
			"alter table t add column col2 int;",
			"alter table t add column col3 int;",
			"insert into t (pk, col2, col3) values (2, 4, 5);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select * from t order by pk",
				Expected: []sql.UntypedSqlRow{
					{1, 2, nil, nil},
					{2, 3, 4, 5},
					{3, 4, nil, nil},
				},
			},
			{
				Query: "select * from t where col1 = 2",
				Expected: []sql.UntypedSqlRow{
					{1, 2, nil, nil},
				},
			},
			{
				Query: "select * from t where col1 = 3 and pk = 2",
				Expected: []sql.UntypedSqlRow{
					{2, 3, 4, 5},
				},
			},
			{
				Query: "select * from t where pk = 2",
				Expected: []sql.UntypedSqlRow{
					{2, 3, 4, 5},
				},
			},
		},
	},
	{
		Name: "virtual column ordering",
		SetUpScript: []string{
			// virtual is the default for generated columns
			"create table t1 (v1 int generated always as (2), a int, v2 int generated always as (a + v1), c int)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "insert into t1 (a, c) values (1,5), (3,7)",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(2)}},
			},
			{
				Query:    "insert into t1 (c, a) values (5,6), (7,8)",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(2)}},
			},
			{
				Query: "select * from t1 order by a",
				Expected: []sql.UntypedSqlRow{
					{2, 1, 3, 5},
					{2, 3, 5, 7},
					{2, 6, 8, 5},
					{2, 8, 10, 7},
				},
			},
			{
				Query: "update t1 set a = 4 where a = 3",
				Expected: []sql.UntypedSqlRow{{types.OkResult{
					RowsAffected: 1,
					Info: plan.UpdateInfo{
						Matched: 1,
						Updated: 1,
					}},
				}},
			},
			{
				Query: "select * from t1 order by a",
				Expected: []sql.UntypedSqlRow{
					{2, 1, 3, 5},
					{2, 4, 6, 7},
					{2, 6, 8, 5},
					{2, 8, 10, 7},
				},
			},
			{
				Query:    "delete from t1 where v2 = 6",
				Expected: []sql.UntypedSqlRow{{types.OkResult{RowsAffected: 1}}},
			},
			{
				Query: "select * from t1 order by a",
				Expected: []sql.UntypedSqlRow{
					{2, 1, 3, 5},
					{2, 6, 8, 5},
					{2, 8, 10, 7},
				},
			},
		},
	},
	{
		Name: "adding a virtual column",
		SetUpScript: []string{
			"create table t1 (a int primary key, b int)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "insert into t1 (a, b) values (1, 2), (3, 4)",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(2)}},
			},
			{
				Query:    "alter table t1 add column c int generated always as (a + b) virtual",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query:    "select * from t1 order by a",
				Expected: []sql.UntypedSqlRow{{1, 2, 3}, {3, 4, 7}},
			},
		},
	},
	{
		Name: "creating index on virtual generated column",
		SetUpScript: []string{
			"create table t1 (a int primary key, b int as (a + 1) virtual)",
			"insert into t1(a) values (1), (2)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "create index i1 on t1(b)",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query: "show create table t1",
				Expected: []sql.UntypedSqlRow{{"t1",
					"CREATE TABLE `t1` (\n" +
						"  `a` int NOT NULL,\n" +
						"  `b` int GENERATED ALWAYS AS ((`a` + 1)),\n" +
						"  PRIMARY KEY (`a`),\n" +
						"  KEY `i1` (`b`)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
				Skip: true, // https://github.com/dolthub/dolt/issues/8275
			},
			{
				Query:    "select * from t1 where b = 2 order by a",
				Expected: []sql.UntypedSqlRow{{1, 2}},
			},
			{
				Query:    "select * from t1 order by a",
				Expected: []sql.UntypedSqlRow{{1, 2}, {2, 3}},
			},
			{
				Query:    "select * from t1 order by b",
				Expected: []sql.UntypedSqlRow{{1, 2}, {2, 3}},
			},
		},
	},
	{
		Name: "virtual column index",
		SetUpScript: []string{
			"create table t1 (a int primary key, b int, c int generated always as (a + b) virtual, index idx_c (c))",
			"insert into t1 (a, b) values (1, 2), (3, 4)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select * from t1 where c = 7",
				Expected: []sql.UntypedSqlRow{{3, 4, 7}},
			},
			{
				Query:    "select * from t1 where c = 8",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "update t1 set b = 5 where c = 3",
				Expected: []sql.UntypedSqlRow{{newUpdateResult(1, 1)}},
			},
			{
				Query: "select * from t1 order by a",
				Expected: []sql.UntypedSqlRow{
					{1, 5, 6},
					{3, 4, 7},
				},
			},
			{
				Query: "select * from t1 where c = 6",
				Expected: []sql.UntypedSqlRow{
					{1, 5, 6},
				},
			},
			{
				Query:    "delete from t1 where c = 6",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
			},
			{
				Query: "select * from t1 order by a",
				Expected: []sql.UntypedSqlRow{
					{3, 4, 7},
				},
			},
		},
	},
	{
		Name: "virtual column index on a keyless table",
		SetUpScript: []string{
			"create table t1 (j json, v int generated always as (j->>'$.a') virtual, index idx_v (v))",
			"insert into t1(j) values ('{\"a\": 1}'), ('{\"a\": 2}'), ('{\"b\": 3}')",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select * from t1 where v = 2",
				Expected: []sql.UntypedSqlRow{{"{\"a\": 2}", 2}},
			},
			{
				Query:    "update t1 set j = '{\"a\": 5}' where v = 2",
				Expected: []sql.UntypedSqlRow{{newUpdateResult(1, 1)}},
			},
			{
				Query: "select * from t1 order by v",
				Expected: []sql.UntypedSqlRow{
					{"{\"b\": 3}", nil},
					{"{\"a\": 1}", 1},
					{"{\"a\": 5}", 5}},
			},
			{
				Query:    "delete from t1 where v = 5",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
			},
			{
				Query: "select * from t1 order by v",
				Expected: []sql.UntypedSqlRow{
					{"{\"b\": 3}", nil},
					{"{\"a\": 1}", 1},
				},
			},
		},
	},
	{
		Name: "creating index on virtual generated column with type conversion",
		SetUpScript: []string{
			"create table t1 (a int primary key, b float generated always as (a + 1))",
			"insert into t1(a) values (1), (2)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "create index i1 on t1(b)",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query: "show create table t1",
				Expected: []sql.UntypedSqlRow{{"t1",
					"CREATE TABLE `t1` (\n" +
						"  `a` int NOT NULL,\n" +
						"  `b` float GENERATED ALWAYS AS ((`a` + 1)),\n" +
						"  PRIMARY KEY (`a`),\n" +
						"  KEY `i1` (`b`)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
				Skip: true, // https://github.com/dolthub/dolt/issues/8275
			},
			{
				Query:    "select * from t1 where b = 2 order by a",
				Expected: []sql.UntypedSqlRow{{1, float64(2)}},
			},
			{
				Query:    "select * from t1 order by a",
				Expected: []sql.UntypedSqlRow{{1, float64(2)}, {2, float64(3)}},
			},
			{
				Query:    "select * from t1 order by b",
				Expected: []sql.UntypedSqlRow{{1, float64(2)}, {2, float64(3)}},
			},
		},
	},
	{
		Name: "creating index on virtual generated column within multi-alter statement",
		SetUpScript: []string{
			"create table t1 (a int primary key, b int as (a + 1) virtual)",
			"insert into t1(a) values (1), (2)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "alter table t1 add column c int as (b+1) stored, add index b1(b), add column d int as (b+2) stored",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query: "show create table t1",
				Expected: []sql.UntypedSqlRow{{"t1",
					"CREATE TABLE `t1` (\n" +
						"  `a` int NOT NULL,\n" +
						"  `b` int GENERATED ALWAYS AS ((`a` + 1)),\n" +
						"  `c` int GENERATED ALWAYS AS ((`b` + 1)),\n" +
						"  `d` int GENERATED ALWAYS AS ((`b` + 2)),\n" +
						"  PRIMARY KEY (`a`),\n" +
						"  KEY `i1` (`b`)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
				Skip: true, // https://github.com/dolthub/dolt/issues/8275
			},
			{
				Query:    "select * from t1 where b = 2 order by a",
				Expected: []sql.UntypedSqlRow{{1, 2, 3, 4}},
			},
			{
				Query:    "select * from t1 order by a",
				Expected: []sql.UntypedSqlRow{{1, 2, 3, 4}, {2, 3, 4, 5}},
			},
			{
				Query:    "select * from t1 order by b",
				Expected: []sql.UntypedSqlRow{{1, 2, 3, 4}, {2, 3, 4, 5}},
			},
		},
	},
	{
		Name: "creating unique index on virtual generated column",
		SetUpScript: []string{
			"create table t1 (a int primary key, b int as (a * a) virtual, c int as (0) virtual)",
			"insert into t1(a) values (-1), (-2)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "create unique index i1 on t1(b)",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query: "show create table t1",
				Expected: []sql.UntypedSqlRow{{"t1",
					"CREATE TABLE `t1` (\n" +
						"  `a` int NOT NULL,\n" +
						"  `b` int GENERATED ALWAYS AS ((`a` * `a`)),\n" +
						"  `c` int GENERATED ALWAYS AS (0),\n" +
						"  PRIMARY KEY (`a`),\n" +
						"  KEY `i1` (`b`)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
				Skip: true, // https://github.com/dolthub/dolt/issues/8275
			},
			{
				Query:    "select * from t1 where b = 4 order by a",
				Expected: []sql.UntypedSqlRow{{-2, 4, 0}},
			},
			{
				Query:    "select * from t1 order by a",
				Expected: []sql.UntypedSqlRow{{-2, 4, 0}, {-1, 1, 0}},
			},
			{
				Query:    "select * from t1 order by b",
				Expected: []sql.UntypedSqlRow{{-1, 1, 0}, {-2, 4, 0}},
			},
			{
				Query:       "insert into t1(a) values (2)",
				ExpectedErr: sql.ErrUniqueKeyViolation,
				Skip:        true, // https://github.com/dolthub/go-mysql-server/issues/2643
			},
			{
				Query:       "create unique index i2 on t1(c)",
				ExpectedErr: sql.ErrUniqueKeyViolation,
				Skip:        true, // https://github.com/dolthub/go-mysql-server/issues/2643
			},
		},
	},
	{
		Name: "illegal table definitions",
		SetUpScript: []string{
			"create table t2 (a int generated always as (2), b int)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "create table t1 (a int generated always as (2), b int, primary key (a))",
				ExpectedErr: sql.ErrVirtualColumnPrimaryKey,
			},
			{
				Query:       "create table t1 (a int generated always as (2), b int, primary key (a, b))",
				ExpectedErr: sql.ErrVirtualColumnPrimaryKey,
			},
			{
				Query:       "alter table t2 add primary key (a)",
				ExpectedErr: sql.ErrVirtualColumnPrimaryKey,
			},
			{
				Query:       "alter table t2 add primary key (a, b)",
				ExpectedErr: sql.ErrVirtualColumnPrimaryKey,
			},
		},
	},
	{
		Name: "generated columns in primary key",
		SetUpScript: []string{
			"create table t2 (a int, b int generated always as (a + 2) stored, primary key (b))",
			"create table t3 (a int, b int generated always as (a + 2) stored, primary key (a, b))",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "insert into t2 (a) values (1), (2)",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(2)}},
			},
			{
				Query: "select * from t2 order by a",
				Expected: []sql.UntypedSqlRow{
					{1, 3},
					{2, 4},
				},
			},
			{
				Query: "select * from t2 where b = 4",
				Expected: []sql.UntypedSqlRow{
					{2, 4},
				},
			},
			{
				Query:    "insert into t3 (a) values (1), (2)",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(2)}},
			},
			{
				Query: "select * from t3 order by a",
				Expected: []sql.UntypedSqlRow{
					{1, 3},
					{2, 4},
				},
			},
			{
				Query: "select * from t3 where a = 2 and b = 4",
				Expected: []sql.UntypedSqlRow{
					{2, 4},
				},
			},
		},
	},
}

var BrokenGeneratedColumnTests = []ScriptTest{
	{
		Name: "update a virtual column with a trigger",
		SetUpScript: []string{
			"create table t1 (a int primary key, b int, c int generated always as (a + b) virtual)",
			"create table t2 (a int primary key)",
			"create trigger t1insert before update on t1 for each row set new.c = 2",
		},
		Assertions: []ScriptTestAssertion{
			{
				// Not sure if this should be an error at trigger creation time or execution time
				Query:       "insert into t1 (a, b) values (1, 2), (3, 4)",
				ExpectedErr: sql.ErrGeneratedColumnValue,
			},
		},
	},
}
