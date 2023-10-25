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
			{
				Query:    "insert into t1(a,b) values (4, DEFAULT)",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "select * from t1 where b = 5 order by a",
				Expected: []sql.Row{{4, 5}},
			},
			{
				Query:       "update t1 set b = b + 1",
				ExpectedErr: sql.ErrGeneratedColumnValue,
			},
			{
				Query:    "update t1 set a = 10 where a = 1",
				Expected: []sql.Row{{newUpdateResult(1, 1)}},
			},
			{
				Query:    "select * from t1 order by a",
				Expected: []sql.Row{{2, 3}, {3, 4}, {4, 5}, {10, 11}},
			},
			{
				Query:    "delete from t1 where b = 11",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "select * from t1 order by a",
				Expected: []sql.Row{{2, 3}, {3, 4}, {4, 5}},
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
				Query:    "insert into t1(a) values (1), (2)",
				Expected: []sql.Row{{types.NewOkResult(2)}},
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
			{
				Query:    "select * from t1 order by a",
				Expected: []sql.Row{{1, 2}, {2, 3}},
			},
			{
				Query:    "select * from t1 order by b",
				Expected: []sql.Row{{1, 2}, {2, 3}},
			},
			{
				Query:    "update t1 set a = 10 where a = 1",
				Expected: []sql.Row{{newUpdateResult(1, 1)}},
			},
			{
				Query:    "select * from t1 where b = 11 order by a",
				Expected: []sql.Row{{10, 11}},
			},
			{
				Query:    "delete from t1 where b = 11",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "select * from t1 where b = 3 order by a",
				Expected: []sql.Row{{2, 3}},
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
			{
				Query:    "insert into t1(a,c) values (2,4)",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query: "explain delete from t1 where b = 3 and c = 4",
				Expected: []sql.Row{
					{"Delete"},
					{" └─ IndexedTableAccess(t1)"},
					{"     ├─ index: [t1.b,t1.c]"},
					{"     └─ filters: [{[3, 3], [4, 4]}]"},
				},
			},
			{
				Query:    "delete from t1 where b = 3 and c = 4",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "select * from t1 order by a",
				Expected: []sql.Row{{1, 2, 3}},
			},
			{
				Query: "explain update t1 set a = 5, c = 10 where b = 2 and c = 3",
				Expected: []sql.Row{
					{"Update"},
					{" └─ UpdateSource(SET t1.a = 5,SET t1.c = 10,SET t1.b = ((t1.a + 1)))"},
					{"     └─ IndexedTableAccess(t1)"},
					{"         ├─ index: [t1.b,t1.c]"},
					{"         └─ filters: [{[2, 2], [3, 3]}]"},
				},
			},
			{
				Query:    "update t1 set a = 5, c = 10 where b = 2 and c = 3",
				Expected: []sql.Row{{newUpdateResult(1, 1)}},
			},
			{
				Query:    "select * from t1 where b = 6 and c = 10 order by a",
				Expected: []sql.Row{{5, 6, 10}},
			},
			{
				Query:    "select * from t1 order by a",
				Expected: []sql.Row{{5, 6, 10}},
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
				Expected: []sql.Row{{types.NewOkResult(3)}},
			},
			{
				Query:    "select * from t1 order by a",
				Expected: []sql.Row{{1, 2}, {2, 3}, {3, 4}},
			},
			{
				Query:    "insert into t2 (c) values (1), (2), (3)",
				Expected: []sql.Row{{types.NewOkResult(3)}},
			},
			{
				Query:    "select * from t2 order by c",
				Expected: []sql.Row{{1, 0}, {2, 1}, {3, 2}},
			},
			{
				Query:    "select * from t1 where b = 2 order by a",
				Expected: []sql.Row{{1, 2}},
			},
			{
				Query:    "select * from t2 where d = 2 order by c",
				Expected: []sql.Row{{3, 2}},
			},
			{
				Query:    "select sum(b) from t1 where b > 2",
				Expected: []sql.Row{{7.0}},
			},
			{
				Query:    "select sum(d) from t2 where d >= 1",
				Expected: []sql.Row{{3.0}},
			},
			{
				Query:    "select a, (select b from t1 t1a where t1a.a = t1.a+1) from t1 order by a",
				Expected: []sql.Row{{1, 3}, {2, 4}, {3, nil}},
			},
			{
				Query:    "select c, (select d from t2 t2a where t2a.c = t2.c+1) from t2 order by c",
				Expected: []sql.Row{{1, 1}, {2, 2}, {3, nil}},
			},
			{
				Query:    "select * from t1 join t2 on a = c order by a",
				Expected: []sql.Row{{1, 2, 1, 0}, {2, 3, 2, 1}, {3, 4, 3, 2}},
			},
			{
				Query:    "select * from t1 join t2 on a = d order by a",
				Expected: []sql.Row{{1, 2, 2, 1}, {2, 3, 3, 2}},
			},
			{
				Query:    "select * from t1 join t2 on b = d order by a",
				Expected: []sql.Row{{1, 2, 3, 2}},
			},
			{
				Query:    "select * from t1 join (select * from t2) as t3 on b = d order by a",
				Expected: []sql.Row{{1, 2, 3, 2}},
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
				Expected: []sql.Row{{types.NewOkResult(3)}},
			},
			{
				Query:    "insert into t2 (c) values (1), (2), (3)",
				Expected: []sql.Row{{types.NewOkResult(3)}},
			},
			{
				Query:    "create trigger t1insert before insert on t1 for each row insert into t2 (c) values (new.b + 1)",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "insert into t1 (a) values (4), (5)",
				Expected: []sql.Row{{types.NewOkResult(2)}},
			},
			{
				Query:    "select * from t1 order by a",
				Expected: []sql.Row{{1, 2}, {2, 3}, {3, 4}, {4, 5}, {5, 6}},
			},
			{
				Query:    "select * from t2 order by c",
				Expected: []sql.Row{{1, 0}, {2, 1}, {3, 2}, {6, 5}, {7, 6}},
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
				Expected: []sql.Row{{types.NewOkResult(3)}},
			},
			{
				Query: "select * from t1 order by a",
				Expected: []sql.Row{
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
				Expected: []sql.Row{{types.NewOkResult(3)}},
			},
			{
				Query: "select * from t1 order by a",
				Expected: []sql.Row{
					{"abc", "123", "abc123"},
					{"def", nil, nil},
					{"ghi", "", "ghi"},
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
	{
		Name: "adding a virtual column",
		SetUpScript: []string{
			"create table t1 (a int primary key, b int)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "insert into t1 (a, b) values (1, 2), (3, 4)",
				Expected: []sql.Row{{types.NewOkResult(2)}},
			},
			{
				Query:    "alter table t1 add column c int generated always as (a + b) virtual",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "select * from t1 order by a",
				Expected: []sql.Row{{1, 2, 3}, {3, 4, 7}},
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
				Expected: []sql.Row{{3, 4, 7}},
			},
			{
				Query: "explain select * from t1 where c = 7",
				Expected: []sql.Row{
					{"IndexedTableAccess(t1)"},
					{" ├─ index: [t1.c]"},
					{" └─ filters: [{[7, 7]}]"},
				},
			},
			{
				Query:    "select * from t1 where c = 8",
				Expected: []sql.Row{},
			},
			{
				Query: "explain update t1 set b = 5 where c = 3",
				Expected: []sql.Row{
					{"Update"},
					{" └─ UpdateSource(SET t1.b = 5,SET t1.c = ((t1.a + t1.b)))"},
					{"     └─ IndexedTableAccess(t1)"},
					{"         ├─ index: [t1.c]"},
					{"         └─ filters: [{[3, 3]}]"},
				},
			},
			{
				Query:    "update t1 set b = 5 where c = 3",
				Expected: []sql.Row{{newUpdateResult(1, 1)}},
			},
			{
				Query: "select * from t1 order by a",
				Expected: []sql.Row{
					{1, 5, 6},
					{3, 4, 7},
				},
			},
			{
				Query: "select * from t1 where c = 6",
				Expected: []sql.Row{
					{1, 5, 6},
				},
			},
			{
				Query: "explain delete from t1 where c = 6",
				Expected: []sql.Row{
					{"Delete"},
					{" └─ IndexedTableAccess(t1)"},
					{"     ├─ index: [t1.c]"},
					{"     └─ filters: [{[6, 6]}]"},
				},
			},
			{
				Query:    "delete from t1 where c = 6",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query: "select * from t1 order by a",
				Expected: []sql.Row{
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
				Expected: []sql.Row{{"{\"a\": 2}", 2}},
			},
			{
				Query: "explain select * from t1 where v = 2",
				Expected: []sql.Row{
					{"IndexedTableAccess(t1)"},
					{" ├─ index: [t1.v]"},
					{" └─ filters: [{[2, 2]}]"},
				},
			},
			{
				Query: "explain update t1 set j = '{\"a\": 5}' where v = 2",
				Expected: []sql.Row{
					{"Update"},
					{" └─ UpdateSource(SET t1.j = '{\"a\": 5}',SET t1.v = (json_unquote(json_extract(t1.j, '$.a'))))"},
					{"     └─ IndexedTableAccess(t1)"},
					{"         ├─ index: [t1.v]"},
					{"         └─ filters: [{[2, 2]}]"},
				},
			},
			{
				Query:    "update t1 set j = '{\"a\": 5}' where v = 2",
				Expected: []sql.Row{{newUpdateResult(1, 1)}},
			},
			{
				Query: "select * from t1 order by v",
				Expected: []sql.Row{
					{"{\"b\": 3}", nil},
					{"{\"a\": 1}", 1},
					{"{\"a\": 5}", 5}},
			},
			{
				Query: "explain delete from t1 where v = 5",
				Expected: []sql.Row{
					{"Delete"},
					{" └─ IndexedTableAccess(t1)"},
					{"     ├─ index: [t1.v]"},
					{"     └─ filters: [{[5, 5]}]"},
				},
			},
			{
				Query:    "delete from t1 where v = 5",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query: "select * from t1 order by v",
				Expected: []sql.Row{
					{"{\"b\": 3}", nil},
					{"{\"a\": 1}", 1},
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
