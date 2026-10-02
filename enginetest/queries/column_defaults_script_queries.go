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
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// ColumnDefaultsScriptTests contains self-contained column defaults script tests.
var ColumnDefaultsScriptTests = []ScriptTest{
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
		Name: "update columns with default",
		SetUpScript: []string{
			"create table t (i int default 10, j varchar(128) default (concat('abc', 'def')));",
			"insert into t values (100, 'a'), (200, 'b');",
			"create table t2 (i int);",
			"insert into t2 values (1), (2), (3);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "update t set i = default where i = 100;",
				Expected: []sql.Row{
					{types.OkResult{RowsAffected: 1, Info: plan.UpdateInfo{Matched: 1, Updated: 1}}},
				},
			},
			{
				Query: "select * from t order by i",
				Expected: []sql.Row{
					{10, "a"},
					{200, "b"},
				},
			},
			{
				Query: "update t set j = default where i = 200;",
				Expected: []sql.Row{
					{types.OkResult{RowsAffected: 1, Info: plan.UpdateInfo{Matched: 1, Updated: 1}}},
				},
			},
			{
				Query: "select * from t order by i",
				Expected: []sql.Row{
					{10, "a"},
					{200, "abcdef"},
				},
			},
			{
				Query: "update t set i = default, j = default;",
				Expected: []sql.Row{
					{types.OkResult{RowsAffected: 2, Info: plan.UpdateInfo{Matched: 2, Updated: 2}}},
				},
			},
			{
				Query: "select * from t order by i",
				Expected: []sql.Row{
					{10, "abcdef"},
					{10, "abcdef"},
				},
			},
			{
				Query: "update t2 set i = default",
				Expected: []sql.Row{
					{types.OkResult{RowsAffected: 3, Info: plan.UpdateInfo{Matched: 3, Updated: 3}}},
				},
			},
			{
				Query: "select * from t2",
				Expected: []sql.Row{
					{nil},
					{nil},
					{nil},
				},
			},
		},
	},
	{
		Name: "preserve now()",
		SetUpScript: []string{
			"create table t1 (i int default (cast(now() as signed)));",
			"create table t2 (i int default (cast(current_timestamp(6) as signed)));",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "show create table t1",
				Expected: []sql.Row{
					{"t1", "CREATE TABLE `t1` (\n" +
						"  `i` int DEFAULT (convert(NOW(), signed))\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
			{
				Query: "show create table t2",
				Expected: []sql.Row{
					{"t2", "CREATE TABLE `t2` (\n" +
						"  `i` int DEFAULT (convert(NOW(6), signed))\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
		},
	},
	{
		Name:    "bit default value",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table t (i int primary key, b bit(2) default 2);",
			"insert into t(i) values (1);",
			"create table tt (b bit(2) default 2 primary key);",
			"insert into tt values ();",
		},
		Assertions: []ScriptTestAssertion{
			{
				Skip:  true, // this fails on server engine, even when skipped
				Query: "select * from t;",
				Expected: []sql.Row{
					{1, uint8(2)},
				},
			},
			{
				Skip:  true, // this fails on server engine, even when skipped
				Query: "select * from tt;",
				Expected: []sql.Row{
					{uint8(2)},
				},
			},
		},
	},
	{
		// https://github.com/dolthub/dolt/issues/11453
		Name:    "DEFAULT(col) expression",
		Dialect: "mysql", // DEFAULT(col) function is not valid Postgres syntax
		SetUpScript: []string{
			"create table t(pk int primary key, i int default 7, j int, k int generated always as (i + 10), l int not null, m int default null);",
			"insert into t(pk, i, l) values (1, 1, 1);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "SELECT DEFAULT(pk) FROM t;",
				ExpectedErr: sql.ErrFieldNoDefaultValue,
			},
			{
				Query:    "SELECT DEFAULT(i) FROM t;",
				Expected: []sql.Row{{7}},
			},
			{
				Query:    "SELECT DEFAULT(i) AS d FROM t;",
				Expected: []sql.Row{{7}},
			},
			{
				Query:    "SELECT DEFAULT(j) FROM t;",
				Expected: []sql.Row{{nil}},
			},
			{
				Query:       "SELECT DEFAULT(k) FROM t;",
				ExpectedErr: sql.ErrFieldNoDefaultValue,
			},
			{
				Query:       "SELECT DEFAULT(l) FROM t;",
				ExpectedErr: sql.ErrFieldNoDefaultValue,
			},
			{
				Query:    "SELECT DEFAULT(m) FROM t;",
				Expected: []sql.Row{{nil}},
			},
			{
				Query:       "SELECT DEFAULT(asdfadf) FROM t;",
				ExpectedErr: sql.ErrColumnNotFound,
			},
		},
	},
	{
		Name: "inserting and updating using default values",
		SetUpScript: []string{
			"create table t1 (i int primary key, j int generated always as (i + 10));",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "insert into t1 (i, j) values (1, default);",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Dialect:  "mysql", // This query should work in Doltgres but currently returns the wrong results. https://github.com/dolthub/doltgresql/issues/3203
				Query:    "select * from t1",
				Expected: []sql.Row{{1, 11}},
			},
			{
				Dialect:  "mysql", // This query should work in Doltgres but currently errors out. https://github.com/dolthub/doltgresql/issues/3204
				Query:    "update t1 set j = default where i = 1;",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 0, Info: plan.UpdateInfo{Matched: 1, Updated: 0}}}},
			},
			{
				Query:       "update t1 set i = default where i = 1;",
				ExpectedErr: sql.ErrFieldNoDefaultValue,
			},
		},
	},
}
