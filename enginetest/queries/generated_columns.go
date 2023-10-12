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
				Expected: []sql.Row{{"t1",
					"CREATE TABLE `t1` (\n" +
						"  `a` int NOT NULL,\n" +
						"  `b` int GENERATED ALWAYS AS ((a + 1)) STORED,\n" +
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
				Expected: []sql.Row{},
			},
			{
				Query:    "insert into t1(a) values (1), (2), (3)",
				Expected: []sql.Row{{types.NewOkResult(3)}},
			},
			{
				Query:    "select * from t1 order by a",
				Expected: []sql.Row{{1, 2}, {2, 3}, {3, 4}},
			},
			// Bug in explicit DEFAULT when a column reference is involved
			{
				Query:    "insert into t1(a,b) values (4, DEFAULT)",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "select * from t1 where b = 5 order by a",
				Expected: []sql.Row{{4, 5}},
			},
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
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query: "show create table t1",
				Expected: []sql.Row{{"t1",
					"CREATE TABLE `t1` (\n" +
						"  `a` int NOT NULL,\n" +
						"  `b` int GENERATED ALWAYS AS ((a + 1)) STORED,\n" +
						"  PRIMARY KEY (`a`),\n" +
						"  KEY `i1` (`b`)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:    "insert into t1(a) values (1)",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "select * from t1 where b = 2 order by a",
				Expected: []sql.Row{{1, 2}},
			},
			{
				Query: "explain select * from t1 where b = 2 order by a",
				Expected: []sql.Row{
					{"Sort(t1.a ASC)"},
					{" └─ IndexedTableAccess(t1)"},
					{"     ├─ index: [t1.b]"},
					{"     ├─ filters: [{[2, 2]}]"},
					{"     └─ columns: [a b]"},
				},
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
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query: "show create table t1",
				Expected: []sql.Row{{"t1",
					"CREATE TABLE `t1` (\n" +
						"  `a` int NOT NULL,\n" +
						"  `b` int GENERATED ALWAYS AS ((a + 1)) STORED,\n" +
						"  `c` int,\n" +
						"  PRIMARY KEY (`a`),\n" +
						"  KEY `i1` (`b`,`c`)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:    "insert into t1(a,c) values (1,3)",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "select * from t1 where b = 2 and c = 3 order by a",
				Expected: []sql.Row{{1, 2, 3}},
			},
			{
				Query: "explain select * from t1 where b = 2 and c = 3 order by a",
				Expected: []sql.Row{
					{"Sort(t1.a ASC)"},
					{" └─ IndexedTableAccess(t1)"},
					{"     ├─ index: [t1.b,t1.c]"},
					{"     ├─ filters: [{[2, 2], [3, 3]}]"},
					{"     └─ columns: [a b c]"},
				},
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
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "select * from t1 order by a",
				Expected: []sql.Row{{1, 2, 3}, {2, 3, 5}, {3, 4, 7}},
			},
			{
				Query: "show create table t1",
				Expected: []sql.Row{{"t1",
					"CREATE TABLE `t1` (\n" +
						"  `a` int NOT NULL,\n" +
						"  `b` int,\n" +
						"  `c` int GENERATED ALWAYS AS ((a + b)) STORED,\n" +
						"  PRIMARY KEY (`a`)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
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
				Expected: []sql.Row{{types.NewOkResult(3)}},
			},
			{
				Query:    "select * from t1 order by a",
				Expected: []sql.Row{{1, 2}, {2, 3}, {3, 4}},
			},
			{
				Query: "update t1 set a = 4 where a = 3",
				Expected: []sql.Row{{types.OkResult{
					RowsAffected: 1,
					Info: plan.UpdateInfo{
						Matched: 1,
						Updated: 1,
					}},
				}},
			},
			{
				Query:    "select * from t1 order by a",
				Expected: []sql.Row{{1, 2}, {2, 3}, {4, 5}},
			},
			{
				Query:    "delete from t1 where a = 2",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1}}},
			},
			{
				Query:    "select * from t1 order by a",
				Expected: []sql.Row{{1, 2}, {4, 5}},
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
				Expected: []sql.Row{{types.NewOkResult(2)}},
			},
			{
				Query:    "insert into t1 (c, a) values (5,6), (7,8)",
				Expected: []sql.Row{{types.NewOkResult(2)}},
			},
			{
				Query: "select * from t1 order by a",
				Expected: []sql.Row{
					{2, 1, 3, 5},
					{2, 3, 5, 7},
					{2, 6, 8, 5},
					{2, 8, 10, 7},
				},
			},
			{
				Query: "update t1 set a = 4 where a = 3",
				Expected: []sql.Row{{types.OkResult{
					RowsAffected: 1,
					Info: plan.UpdateInfo{
						Matched: 1,
						Updated: 1,
					}},
				}},
			},
			{
				Query: "select * from t1 order by a",
				Expected: []sql.Row{
					{2, 1, 3, 5},
					{2, 4, 6, 7},
					{2, 6, 8, 5},
					{2, 8, 10, 7},
				},
			},
			{
				Query:    "delete from t1 where v2 = 6",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1}}},
			},
			{
				Query: "select * from t1 order by a",
				Expected: []sql.Row{
					{2, 1, 3, 5},
					{2, 6, 8, 5},
					{2, 8, 10, 7},
				},
			},
		},
	},
}
