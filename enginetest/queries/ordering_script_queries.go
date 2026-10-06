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
)

// OrderingScriptTests contains self-contained ordering script tests.
var OrderingScriptTests = []ScriptTest{
	{
		// https://github.com/dolthub/dolt/issues/9836
		Skip: true,
		Name: "Ordering by pk does not change the order of results",
		SetUpScript: []string{
			"CREATE TABLE test(pk VARCHAR(50) PRIMARY KEY)",
			"INSERT INTO test VALUES ('  3 12 4'), ('3. 12 4'), ('3.2 12 4'), ('-3.1234'), ('-3.1a'), ('-5+8'), ('+3.1234')",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT pk FROM test ORDER BY pk",
				Expected: []sql.Row{{"  3 12 4"}, {"-3.1234"}, {"-3.1a"}, {"-5+8"}, {"+3.1234"}, {"3. 12 4"}, {"3.2 12 4"}},
			},
		},
	},
	{
		Name: "topN stable output",
		SetUpScript: []string{
			"create table xy (x int primary key, y int)",
			"insert into xy values (1,0),(2,0),(3,0),(4,0)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select * from xy order by y asc limit 1",
				Expected: []sql.Row{{1, 0}},
			},
			{
				Query:    "select * from xy order by y asc limit 1 offset 1",
				Expected: []sql.Row{{2, 0}},
			},
			{
				Query:    "select * from xy order by y asc limit 1 offset 2",
				Expected: []sql.Row{{3, 0}},
			},
			{
				Query:    "select * from xy order by y asc limit 1 offset 3",
				Expected: []sql.Row{{4, 0}},
			},
			{
				Query:    "(select * from xy order by y asc limit 1 offset 1) union (select * from xy order by y asc limit 1 offset 2)",
				Expected: []sql.Row{{2, 0}, {3, 0}},
			},
			{
				Query:    "with recursive cte as ((select * from xy order by y asc limit 1 offset 1) union (select * from xy order by y asc limit 1 offset 2)) select * from cte",
				Expected: []sql.Row{{2, 0}, {3, 0}},
			},
		},
	},
	{
		Name:    "Issue #499", // https://github.com/dolthub/go-mysql-server/issues/499
		Dialect: "mysql",
		SetUpScript: []string{
			"SET @@SESSION.time_zone = 'UTC';",
			"CREATE TABLE test (time TIMESTAMP, value DOUBLE);",
			`INSERT INTO test VALUES 
			("2021-07-04 10:00:00", 1.0),
			("2021-07-03 10:00:00", 2.0),
			("2021-07-02 10:00:00", 3.0),
			("2021-07-01 10:00:00", 4.0);`,
		},
		Assertions: []ScriptTestAssertion{
			{
				// In the original, reported issue, the order by clause did not qualify the table name
				// for `test.time`. When there is ambiguity between a column name and an expression
				// alias name in the order by clause, MySQL choose the alias; however, if the reference
				// is used in a function call, MySQL instead seems to resolve to the column name.
				// Until we determine the exact rule for this behavior, we've qualified the reference
				// in the order by clause to ensure it selects the table column and not the alias.
				// TODO: Waiting to hear back from MySQL on whether this is intended behavior or not:
				//       https://bugs.mysql.com/bug.php?id=109020
				Query: `SELECT UNIX_TIMESTAMP(time) DIV 60 * 60 AS "time", avg(value) AS "value"
				FROM test GROUP BY 1 ORDER BY UNIX_TIMESTAMP(test.time) DIV 60 * 60`,
				Expected: []sql.Row{
					{int64(1625133600), 4.0},
					{int64(1625220000), 3.0},
					{int64(1625306400), 2.0},
					{int64(1625392800), 1.0},
				},
			},
		},
		// todo(max): fix arithmatic on bindvar typing
		SkipPrepared: true,
	},
	{
		Name: "order by with index",
		SetUpScript: []string{
			"create table t (i int primary key, `100` int);",
			"insert into t values (1, 2), (2, 1)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select * from t order by `100`",
				Expected: []sql.Row{
					{2, 1},
					{1, 2},
				},
			},
			{
				Query:          "select * from t order by 100",
				ExpectedErrStr: "column \"100\" could not be found in any table in scope",
			},
			{
				Query: "select i as `200`, `100` from t order by `200`",
				Expected: []sql.Row{
					{1, 2},
					{2, 1},
				},
			},
			{
				Query:          "select i as `200` from t order by 200",
				ExpectedErrStr: "column \"200\" could not be found in any table in scope",
			},
			{
				Query:          "select * from t order by 0",
				ExpectedErrStr: "column \"0\" could not be found in any table in scope",
			},
			{
				Query: "select * from t order by -999",
				Expected: []sql.Row{
					{1, 2},
					{2, 1},
				},
			},
		},
	},
	{
		// https://github.com/dolthub/dolt/issues/9789
		Name: "order by on empty set from joins",
		SetUpScript: []string{
			"create table t0(c0 int, primary key(c0))",
			"create table t1(c0 int)",
			"create table t2(c0 int)",
			"create table t3(c0 int)",
			"insert into t0 values (1)",
			"insert into t1 values (1)",
			"insert into t2 values (1)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select * from t2, t3, t0, t1",
				Expected: []sql.Row{},
			},
			{
				Query:    "select * from t2, t3, t0, t1 order by t0.c0",
				Expected: []sql.Row{},
			},
		},
	},
	{
		// https://github.com/dolthub/dolt/issues/11300
		Name: "TopN rows (Sort with LIMIT) where sort condition is a subquery",
		SetUpScript: []string{
			"CREATE TABLE foo (id VARCHAR(36) PRIMARY KEY);",
			"CREATE TABLE bar (id VARCHAR(36) PRIMARY KEY, foo_id VARCHAR(36) NOT NULL, approved_on DATETIME(3), FOREIGN KEY (foo_id) REFERENCES foo(id) ON DELETE CASCADE ON UPDATE CASCADE);",
			"INSERT INTO foo VALUES('foo-1'), ('foo-2'), ('foo-3'), ('foo-4'), ('foo-5'), ('foo-6'), ('foo-7'), ('foo-8');",
			`INSERT INTO bar VALUES
('bar-1', 'foo-1', '2026-07-14 07:18:04.000'),
('bar-2', 'foo-2', '2026-07-14 07:18:03.000'),
('bar-3', 'foo-3', '2026-07-14 07:18:05.000'),
('bar-4', 'foo-4', '2026-07-14 07:18:01.000'),
('bar-5', 'foo-5', '2026-07-14 07:18:08.000'),
('bar-6', 'foo-6', '2026-07-14 07:18:02.000'),
('bar-7', 'foo-7', '2026-07-14 07:18:07.000'),
('bar-8', 'foo-8', '2026-07-14 07:18:00.000');`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT * FROM foo ORDER BY (SELECT MAX(r.approved_on) FROM bar r WHERE r.foo_id = foo.id) ASC LIMIT 1",
				Expected: []sql.Row{{"foo-8"}},
			},
			{
				Query:    "SELECT * FROM foo ORDER BY (SELECT MAX(r.approved_on) FROM bar r WHERE r.foo_id = foo.id) DESC LIMIT 1",
				Expected: []sql.Row{{"foo-5"}},
			},
			{
				Query:    "SELECT * FROM foo ORDER BY (SELECT MAX(r.approved_on) FROM bar r WHERE r.foo_id = foo.id) ASC LIMIT 2",
				Expected: []sql.Row{{"foo-8"}, {"foo-4"}},
			},
			{
				Query:    "SELECT * FROM foo ORDER BY (SELECT MAX(r.approved_on) FROM bar r WHERE r.foo_id = foo.id) DESC LIMIT 2",
				Expected: []sql.Row{{"foo-5"}, {"foo-7"}},
			},
			{
				Query:    "SELECT * FROM foo ORDER BY (SELECT MAX(r.approved_on) FROM bar r WHERE r.foo_id = foo.id) ASC LIMIT 4",
				Expected: []sql.Row{{"foo-8"}, {"foo-4"}, {"foo-6"}, {"foo-2"}},
			},
			{
				Query:    "SELECT * FROM foo ORDER BY (SELECT MAX(r.approved_on) FROM bar r WHERE r.foo_id = foo.id) DESC LIMIT 4",
				Expected: []sql.Row{{"foo-5"}, {"foo-7"}, {"foo-3"}, {"foo-1"}},
			},
		},
	},
	{
		Name: "TopN with huge limit",
		SetUpScript: []string{
			"create table t (i int);",
			"insert into t values (1), (2), (3)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Dialect: "mysql", // Postgres does not allow a limit of this size
				Query:   "select * from t order by i limit 18446744073709551615",
				Expected: []sql.Row{
					{1},
					{2},
					{3},
				},
			},
			{
				Query: "select * from t order by i limit 9223372036854775807",
				Expected: []sql.Row{
					{1},
					{2},
					{3},
				},
			},
			{
				Query: "select * from t order by i limit 4294967295",
				Expected: []sql.Row{
					{1},
					{2},
					{3},
				},
			},
			{
				Query: "select * from t order by i limit 2147483647",
				Expected: []sql.Row{
					{1},
					{2},
					{3},
				},
			},
		},
	},
}

var OrderByScriptTests = []ScriptTest{
	{
		// https://github.com/dolthub/dolt/issues/9605
		Name: "Order by wrapped by parentheses",
		SetUpScript: []string{
			"create table t(i int, j int)",
			"insert into t values(2,4),(0,7),(9,10),(4,3)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "with cte(i) as (select i from t) select * from cte order by (i)",
				Expected: []sql.Row{{0}, {2}, {4}, {9}},
			},
			{
				Query:    "with cte(i) as (select i from t) select * from cte order by (((i)))",
				Expected: []sql.Row{{0}, {2}, {4}, {9}},
			},
			{
				Query:    "select * from t order by (i * 10 + j)",
				Expected: []sql.Row{{0, 7}, {2, 4}, {4, 3}, {9, 10}},
			},
		},
	},
}
