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
	"github.com/dolthub/vitess/go/sqltypes"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// NameResolutionScriptTests contains self-contained name resolution script tests.
var NameResolutionScriptTests = []ScriptTest{
	{
		// https://github.com/dolthub/go-mysql-server/issues/3259
		Dialect: "mysql",
		Name:    "Missing column with same name as system variable",
		SetUpScript: []string{
			"CREATE DATABASE IF NOT EXISTS test_db",
			"USE test_db",
			"CREATE TABLE A (id INT)",
			"CREATE TABLE B (id INT)",
			"INSERT INTO A VALUES (1)",
			"INSERT INTO B VALUES (2)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "SELECT UNIX_TIMESTAMP(A.timestamp) FROM A LIMIT 1",
				ExpectedErr: sql.ErrTableColumnNotFound,
			},
			{
				Query:       "SELECT A.timestamp FROM A",
				ExpectedErr: sql.ErrTableColumnNotFound,
			},
			{
				Query:       "SELECT A.version FROM A",
				ExpectedErr: sql.ErrTableColumnNotFound,
			},
			{
				Query:       "SELECT A.max_connections FROM A",
				ExpectedErr: sql.ErrTableColumnNotFound,
			},
			{
				Query:       "SELECT UPPER(A.sql_mode) FROM A",
				ExpectedErr: sql.ErrTableColumnNotFound,
			},
			{
				Query:            "SELECT @@timestamp",
				Expected:         []sql.Row{{float64(0)}},
				SkipResultsCheck: true, // dynamic var
			},
			{
				Query:            "SELECT @@version",
				Expected:         []sql.Row{{""}},
				SkipResultsCheck: true, // dynamic var
			},
			{
				Query:       "SELECT test_db.A.timestamp FROM A",
				ExpectedErr: sql.ErrTableColumnNotFound,
			},
			{
				Query:       "SELECT test_db.A.version FROM test_db.A",
				ExpectedErr: sql.ErrTableColumnNotFound,
			},
			{
				Query:       "SELECT a1.timestamp FROM A a1 JOIN B b1 ON a1.id = b1.id",
				ExpectedErr: sql.ErrTableColumnNotFound,
			},
			{
				Query:       "SELECT b1.max_connections FROM A a1 JOIN B b1 ON a1.id = b1.id",
				ExpectedErr: sql.ErrTableColumnNotFound,
			},
		},
	},
	{
		Name: "CrossDB Queries",
		SetUpScript: []string{
			"CREATE DATABASE test",
			"CREATE TABLE test.x (pk int primary key)",
			"insert into test.x values (1),(2),(3)",
			"DELETE FROM test.x WHERE pk=2",
			"UPDATE test.x set pk=300 where pk=3",
			"create table a (xa int primary key, ya int, za int)",
			"insert into a values (1,2,3)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT pk from test.x",
				Expected: []sql.Row{{1}, {300}},
			},
			{
				Query:    "SELECT * from a",
				Expected: []sql.Row{{1, 2, 3}},
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
		Name: "Multi-db Aliasing",
		SetUpScript: []string{
			"create database db1;",
			"create table db1.t1 (i int primary key);",
			"create table db1.t2 (j int primary key);",
			"insert into db1.t1 values (1);",
			"insert into db1.t2 values (2);",

			"create database db2;",
			"create table db2.t1 (i int primary key);",
			"create table db2.t2 (j int primary key);",
			"insert into db2.t1 values (10);",
			"insert into db2.t2 values (20);",
		},
		Assertions: []ScriptTestAssertion{
			{
				// surprisingly, this works
				Query: "select db1.t1.i from db1.t1 where db1.``.i > 0",
				Expected: []sql.Row{
					{1},
				},
			},
			{
				Query: "select db1.t1.i from db1.t1 where db1.t1.i > 0",
				Expected: []sql.Row{
					{1},
				},
			},
			{
				Query: "select db1.t1.i from db1.t1 order by db1.t1.i",
				Expected: []sql.Row{
					{1},
				},
			},
			{
				Query: "select db1.t1.i from db1.t1 group by db1.t1.i",
				Expected: []sql.Row{
					{1},
				},
			},
			{
				Query: "select db1.t1.i from db1.t1 having db1.t1.i > 0",
				Expected: []sql.Row{
					{1},
				},
			},
			{
				Query: "select (select db1.t1.i from db1.t1 order by db1.t1.i)",
				Expected: []sql.Row{
					{1},
				},
			},
			{
				Query: "select i from (select db1.t1.i from db1.t1 order by db1.t1.i) as t",
				Expected: []sql.Row{
					{1},
				},
			},
			{
				Query: "with cte as (select db1.t1.i from db1.t1 order by db1.t1.i) select * from cte",
				Expected: []sql.Row{
					{1},
				},
			},
			{
				Query: "select i, j from db1.t1 inner join db2.t2 on 20 * i = j",
				Expected: []sql.Row{
					{1, 20},
				},
			},
			{
				Query: "select db1.t1.i, db2.t2.j from db1.t1 inner join db2.t2 on 20 * db1.t1.i = db2.t2.j",
				Expected: []sql.Row{
					{1, 20},
				},
			},
			{
				Query: "select i, j from db1.t1 join db2.t2 order by i, j",
				Expected: []sql.Row{
					{1, 20},
				},
			},
			{
				Query: "select i, j from db1.t1 join db2.t2 group by i order by j",
				Expected: []sql.Row{
					{1, 20},
				},
			},
			{
				Query: "select db1.t1.i, db2.t2.j from db1.t1 join db2.t2 group by db1.t1.i order by db2.t2.j",
				Expected: []sql.Row{
					{1, 20},
				},
			},
			{
				Skip:  true, // incorrectly throws Not unique table/alias: t1
				Query: "select db1.t1.i, db2.t1.i from db1.t1 join db2.t1 order by db1.t1, db2.t1.i",
				Expected: []sql.Row{
					{1, 10},
				},
			},
			{
				// Aliasing solves it
				Query: "select a.i, b.i from db1.t1 a join db2.t1 b order by a.i, b.i",
				Expected: []sql.Row{
					{1, 10},
				},
			},
		},
	},
	{
		Name: "same alias names for result column name and alias table column name",
		SetUpScript: []string{
			"CREATE TABLE tab0(col0 INTEGER, col1 INTEGER, col2 INTEGER)",
			"INSERT INTO tab0 VALUES(83,0,38)",
			"INSERT INTO tab0 VALUES(26,0,79)",
			"INSERT INTO tab0 VALUES(43,81,24)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT + 13 AS col0 FROM tab0 GROUP BY tab0.col0",
				Expected: []sql.Row{{13}, {13}, {13}},
			},
			{
				Query:    "SELECT 82 col1 FROM tab0 AS cor0 GROUP BY cor0.col1",
				Expected: []sql.Row{{82}, {82}},
			},
			{
				Query:    "SELECT - cor0.col2 * - col2 AS col1 FROM tab0 AS cor0 GROUP BY col2, cor0.col1",
				Expected: []sql.Row{{1444}, {6241}, {576}},
			},
			{
				Query:    "SELECT ALL + 40 col1 FROM tab0 AS cor0 GROUP BY cor0.col1",
				Expected: []sql.Row{{40}, {40}},
			},
			{
				Query:    "SELECT DISTINCT - cor0.col1 col1 FROM tab0 AS cor0 GROUP BY cor0.col1, cor0.col2",
				Expected: []sql.Row{{-81}, {0}},
			},
			{
				Query:    "SELECT DISTINCT ( cor0.col0 ) - col0 AS col2 FROM tab0 AS cor0 GROUP BY cor0.col2, cor0.col0, cor0.col0",
				Expected: []sql.Row{{0}},
			},
		},
	},
	{
		Name: "group by having with conflicting aliases test",
		SetUpScript: []string{
			"CREATE TABLE tab2(col0 INTEGER, col1 INTEGER, col2 INTEGER);",
			"INSERT INTO tab2 VALUES(15,61,87);",
			"INSERT INTO tab2 VALUES(91,59,79);",
			"INSERT INTO tab2 VALUES(92,41,58);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `SELECT - col2 AS col0 FROM tab2 GROUP BY col0, col2 HAVING NOT + + col2 <= - col0;`,
				Expected: []sql.Row{
					{-87},
					{-79},
					{-58},
				},
			},
			{
				Query: `SELECT -col2 AS col0 FROM tab2 GROUP BY col0, col2 HAVING NOT col2 <= - col0;`,
				Expected: []sql.Row{
					{-87},
					{-79},
					{-58},
				},
			},
			{
				Query: `SELECT -col2 AS col0 FROM tab2 GROUP BY col0, col2 HAVING col2 > -col0;`,
				Expected: []sql.Row{
					{-87},
					{-79},
					{-58},
				},
			},
			{
				Query: `SELECT 500 * col2 AS col0 FROM tab2 GROUP BY col0, col2 HAVING col2 > -col0;`,
				Expected: []sql.Row{
					{43500},
					{39500},
					{29000},
				},
			},

			{
				Query: `select col2-100 as col0 from tab2 group by col0 having col0 > 0;`,
				Expected: []sql.Row{
					{-13},
					{-21},
					{-42},
				},
			},
			{
				Query:    `select col2-100 as col0 from tab2 group by 1 having col0 > 0;`,
				Expected: []sql.Row{},
			},
			{
				Query: `select col0, count(col0) as c from tab2 group by col0 having c > 0;`,
				Expected: []sql.Row{
					{15, 1},
					{91, 1},
					{92, 1},
				},
			},
			{
				Query: `SELECT col0 as a FROM tab2 GROUP BY a HAVING col0 = a;`,
				Expected: []sql.Row{
					{15},
					{91},
					{92},
				},
			},
			{
				Query: `SELECT col0 as a FROM tab2 GROUP BY col0 HAVING col0 = a;`,
				Expected: []sql.Row{
					{15},
					{91},
					{92},
				},
			},
			{
				Query: `SELECT col0 as a FROM tab2 GROUP BY col0, a HAVING col0 = a;`,
				Expected: []sql.Row{
					{15},
					{91},
					{92},
				},
			},
			{
				Query: `SELECT col0 as a FROM tab2 HAVING col0 = a;`,
				Expected: []sql.Row{
					{15},
					{91},
					{92},
				},
			},
			{
				Query: `select col0, (select col1 having col0 > 0) as asdf from tab2 where col0 < 1000;`,
				Expected: []sql.Row{
					{15, 61},
					{91, 59},
					{92, 41},
				},
			},
			{
				Query: `select col0, sum(col1 * col2) as val from tab2 group by col0 having sum(col1 * col2) > 0;`,
				Expected: []sql.Row{
					{15, 5307.0},
					{91, 4661.0},
					{92, 2378.0},
				},
			},
			{
				Query:       `SELECT col0+1 as a FROM tab2 HAVING col0 = a;`,
				ExpectedErr: sql.ErrColumnNotFound,
			},
			{
				Query:       `select col2-100 as asdf from tab2 group by 1 having col0 > 0;`,
				ExpectedErr: sql.ErrColumnNotFound,
			},
			{
				Query:       `SELECT -col2 AS col0 FROM tab2 HAVING col2 > -col0;`,
				ExpectedErr: sql.ErrColumnNotFound,
			},
			{
				Query:       `insert into tab2(col2) select sin(col2) from tab2 group by 1 having col2 > 1;`,
				ExpectedErr: sql.ErrColumnNotFound,
			},
		},
	},
	{
		Name: "case sensitive subquery column names",
		SetUpScript: []string{
			"create table t(ABC int, dEF int);",
			"insert into t values (1, 2);",
		},

		Assertions: []ScriptTestAssertion{
			{
				ExpectedColumns: sql.Schema{
					{Name: "ABC", Type: types.Int32},
					{Name: "dEF", Type: types.Int32},
				},
				Query: "select * from t ",
				Expected: []sql.Row{
					{1, 2},
				},
			},
			{
				ExpectedColumns: sql.Schema{
					{Name: "ABC", Type: types.Int32},
					{Name: "dEF", Type: types.Int32},
				},
				Query: "select * from (select * from t) sqa",
				Expected: []sql.Row{
					{1, 2},
				},
			},
		},
	},
}

var ColumnAliasQueries = []ScriptTest{
	{
		Name: "column aliases in a single scope",
		SetUpScript: []string{
			"create table xy (x int primary key, y int);",
			"create table uv (u int primary key, v int);",
			"create table wz (w int, z int);",
			"insert into xy values (0,0),(1,1),(2,2),(3,3);",
			"insert into uv values (0,3),(3,0),(2,1),(1,2);",
			"insert into wz values (0, 0), (1, 0), (1, 2)",
		},
		Assertions: []ScriptTestAssertion{
			{
				// Projections can create expression aliases
				Query: `SELECT i AS cOl FROM mytable`,
				ExpectedColumns: sql.Schema{
					{
						Name: "cOl",
						Type: types.Int64,
					},
				},
				Expected: []sql.Row{{int64(1)}, {int64(2)}, {int64(3)}},
			},
			{
				Query: `SELECT i AS cOl, s as COL FROM mytable`,
				ExpectedColumns: sql.Schema{
					{
						Name: "cOl",
						Type: types.Int64,
					},
					{
						Name: "COL",
						Type: types.MustCreateStringWithDefaults(sqltypes.VarChar, 20),
					},
				},
				Expected: []sql.Row{{int64(1), "first row"}, {int64(2), "second row"}, {int64(3), "third row"}},
			},
			{
				// Projection expressions may NOT reference aliases defined in projection expressions
				// in the same scope
				Query:       `SELECT i AS new1, new1 as new2 FROM mytable`,
				ExpectedErr: sql.ErrMisusedAlias,
			},
			{
				// The SQL standard disallows aliases in the same scope from being used in filter conditions
				Query:       `SELECT i AS cOl, s as COL FROM mytable where cOl = 1`,
				ExpectedErr: sql.ErrColumnNotFound,
			},
			{
				// Alias expressions may NOT be used in from clauses
				Query:       "select t1.i as a, t1.s as b from mytable as t1 left join mytable as t2 on a = t2.i;",
				ExpectedErr: sql.ErrColumnNotFound,
			},
			{
				// OrderBy clause may reference expression aliases at current scope
				Query:    "select 1 as a order by a desc;",
				Expected: []sql.Row{{1}},
			},
			{
				// If there is ambiguity between one table column and one alias, the alias gets precedence in the order
				// by clause. (This is different from subqueries in projection expressions.)
				Query:    "select v as u from uv order by u;",
				Expected: []sql.Row{{0}, {1}, {2}, {3}},
			},
			{
				// If there is ambiguity between multiple aliases in an order by clause, it is an error
				Query:       "select u as u, v as u from uv order by u;",
				ExpectedErr: sql.ErrAmbiguousColumnOrAliasName,
			},
			{
				// If there is ambiguity between one selected table column and one alias, the table column gets
				// precedence in the group by clause.
				Query:    "select w, min(z) as w, max(z) as w from wz group by w;",
				Expected: []sql.Row{{0, 0, 0}, {1, 0, 2}},
			},
			{
				// GroupBy may use a column that is selected multiple times.
				Query:    "select w, w from wz group by w;",
				Expected: []sql.Row{{0, 0}, {1, 1}},
			},
			{
				// GroupBy may use expression aliases in grouping expressions
				Query: `SELECT s as COL1, SUM(i) COL2 FROM mytable group by col1 order by col2`,
				ExpectedColumns: sql.Schema{
					{
						Name: "COL1",
						Type: types.MustCreateStringWithDefaults(sqltypes.VarChar, 20),
					},
					{
						Name: "COL2",
						Type: types.Float64,
					},
				},
				Expected: []sql.Row{
					{"first row", float64(1)},
					{"second row", float64(2)},
					{"third row", float64(3)},
				},
			},
			{
				// Having clause may reference expression aliases current scope
				Query:    "select t1.u as a from uv as t1 having a > 0 order by a;",
				Expected: []sql.Row{{1}, {2}, {3}},
			},
			{
				// Having clause may reference expression aliases from current scope
				Query:    "select t1.u as a from uv as t1 having a = t1.u order by a;",
				Expected: []sql.Row{{0}, {1}, {2}, {3}},
			},
			{
				// Expression aliases work when implicitly referenced by ordinal position
				Query: `SELECT s as coL1, SUM(i) coL2 FROM mytable group by 1 order by 2`,
				ExpectedColumns: sql.Schema{
					{
						Name: "coL1",
						Type: types.MustCreateStringWithDefaults(sqltypes.VarChar, 20),
					},
					{
						Name: "coL2",
						Type: types.Float64,
					},
				},
				Expected: []sql.Row{
					{"first row", float64(1)},
					{"second row", float64(2)},
					{"third row", float64(3)},
				},
			},
			{
				// Expression aliases work when implicitly referenced by ordinal position
				Query: `SELECT s as Date, SUM(i) TimeStamp FROM mytable group by 1 order by 2`,
				ExpectedColumns: sql.Schema{
					{
						Name: "Date",
						Type: types.MustCreateStringWithDefaults(sqltypes.VarChar, 20),
					},
					{
						Name: "TimeStamp",
						Type: types.Float64,
					},
				},
				Expected: []sql.Row{
					{"first row", float64(1)},
					{"second row", float64(2)},
					{"third row", float64(3)},
				},
			},
			{
				Query:    "select t1.i as a from mytable as t1 having a = t1.i;",
				Expected: []sql.Row{{1}, {2}, {3}},
			},
		},
	},
	{
		Name: "column aliases in two scopes",
		SetUpScript: []string{
			"create table xy (x int primary key, y int);",
			"create table uv (u int primary key, v int);",
			"insert into xy values (0,0),(1,1),(2,2),(3,3);",
			"insert into uv values (0,3),(3,0),(2,1),(1,2);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    `select "foo" as dummy, (select dummy)`,
				Expected: []sql.Row{{"foo", "foo"}},
			},
			{
				// https://github.com/dolthub/dolt/issues/4344
				Query:    "select x as v, (select u from uv where v = y) as u from xy;",
				Expected: []sql.Row{{0, 3}, {1, 2}, {2, 1}, {3, 0}},
			},
			{
				// GMS currently returns {0, 0, 0} The second alias seems to get overwritten.
				// https://github.com/dolthub/go-mysql-server/issues/1286
				Skip: true,

				// When multiple aliases are defined with the same name, a subquery prefers the first definition
				Query:    "select 0 as a, 1 as a, (SELECT x from xy where x = a);",
				Expected: []sql.Row{{0, 1, 0}},
			},
			{
				Query:    "SELECT 1 as a, (select a) as a;",
				Expected: []sql.Row{{1, 1}},
			},
			{
				Query:    "SELECT 1 as a, (select a) as b;",
				Expected: []sql.Row{{1, 1}},
			},
			{
				Query:    `SELECT 1 as a, (select a union select a) as b;`,
				Expected: []sql.Row{{1, 1}},
			},
			{
				Query:    "SELECT 1 as a, (select a) as b from dual;",
				Expected: []sql.Row{{1, 1}},
			},
			{
				Query:    "SELECT 1 as a, (select a) as b from xy;",
				Expected: []sql.Row{{1, 1}, {1, 1}, {1, 1}, {1, 1}},
			},
			{
				Query:    "select x, (select 1) as y from xy;",
				Expected: []sql.Row{{0, 1}, {1, 1}, {2, 1}, {3, 1}},
			},
			{
				Query:    "SELECT 1 as a, (select a) from xy;",
				Expected: []sql.Row{{1, 1}, {1, 1}, {1, 1}, {1, 1}},
			},
			{
				// https://github.com/dolthub/dolt/issues/4256
				Query: `SELECT *, (select i union select i) as a from mytable;`,
				Expected: []sql.Row{
					{1, "first row", 1},
					{2, "second row", 2},
					{3, "third row", 3}},
			},
			{
				Query:    "select 1 as b, (select b group by b order by b) order by 1;",
				Expected: []sql.Row{{1, 1}},
			},
			{
				Query:       `select 1 as a, (select b), 0 as b;`,
				ExpectedErr: sql.ErrColumnNotFound,
			},
		},
	},
	{
		Name: "column aliases in three scopes",
		SetUpScript: []string{
			"create table xy (x int primary key, y int);",
			"create table uv (u int primary key, v int);",
			"insert into xy values (0,0),(1,1),(2,2),(3,3);",
			"insert into uv values (0,3),(3,0),(2,1),(1,2);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select x, (select 1) as y, (select (select y as q)) as z from (select * from xy) as xy;",
				Expected: []sql.Row{{0, 1, 0}, {1, 1, 1}, {2, 1, 2}, {3, 1, 3}},
			},
		},
	},
	{
		// https://github.com/dolthub/dolt/issues/11502
		Name: "column aliases in window definitions",
		SetUpScript: []string{
			"create table t (id int primary key, g int, k int, v int);",
			"insert into t values (1,1,1,25),(2,0,0,-37),(3,1,-2,85);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `select id, g, k as order_alias,
					lag(v) over (partition by g order by order_alias, id) as prev_v,
					lead(v) over (partition by g order by order_alias, id) as next_v
					from t order by id;`,
				ExpectedErrStr: "Unknown column 'order_alias' in 'window order by'",
			},
			{
				Query:          "select k as order_alias, lag(v) over (order by order_alias) as p from t;",
				ExpectedErrStr: "Unknown column 'order_alias' in 'window order by'",
			},
			{
				Query:          "select k as order_key, lag(v) over (order by ORDER_KEY) as p from t;",
				ExpectedErrStr: "Unknown column 'order_key' in 'window order by'",
			},
			{
				Query:          "select g as part_alias, lag(v) over (partition by part_alias order by id) as p from t;",
				ExpectedErrStr: "Unknown column 'part_alias' in 'window partition by'",
			},
			{
				Query:          "select k as order_alias, lag(v) over (order by nosuchcol) as p from t;",
				ExpectedErrStr: "Unknown column 'nosuchcol' in 'window order by'",
			},
			{
				Query:          "select k as order_alias, lag(v) over w as p from t window w as (order by order_alias);",
				ExpectedErrStr: "Unknown column 'order_alias' in 'window order by'",
			},
			{
				Query:    "select id, k as order_alias, lag(v) over (order by order_alias + 0) as p from t order by id;",
				Expected: []sql.Row{{1, 1, -37}, {2, 0, 85}, {3, -2, nil}},
			},
			{
				Query:    "select id, g as part_alias, lag(v) over (partition by part_alias + 0 order by id) as p from t order by id;",
				Expected: []sql.Row{{1, 1, nil}, {2, 0, nil}, {3, 1, 25}},
			},
			{
				Query:    "select id, k + 10 as ke, lag(v) over (order by ke + 0) as p from t order by id;",
				Expected: []sql.Row{{1, 11, -37}, {2, 10, 85}, {3, 8, nil}},
			},
			{
				Query:    "select id, k as ka, lag(v) over (order by abs(ka)) as p from t order by id;",
				Expected: []sql.Row{{1, 1, -37}, {2, 0, nil}, {3, -2, 25}},
			},
			{
				Query:    "select id, v as k, lag(v) over (order by k) as p from t order by id;",
				Expected: []sql.Row{{1, 25, -37}, {2, -37, 85}, {3, 85, nil}},
			},
			{
				Query:    "select id, k as order_alias, lag(v) over (order by k, id) as p from t order by id;",
				Expected: []sql.Row{{1, 1, -37}, {2, 0, 85}, {3, -2, nil}},
			},
			{
				Query:    "select id, g, k as order_alias, lag(v) over (partition by g order by k, id) as prev_v from t order by order_alias;",
				Expected: []sql.Row{{3, 1, -2, nil}, {2, 0, 0, nil}, {1, 1, 1, 85}},
			},
			{
				Query:    "select id, k as ORDER_ALIAS, lag(v) over (order by order_alias + 0) as p from t order by id;",
				Expected: []sql.Row{{1, 1, -37}, {2, 0, 85}, {3, -2, nil}},
			},
		},
	},
	{
		Name: "various broken alias queries",
		Skip: true,
		Assertions: []ScriptTestAssertion{
			{
				// GMS returns "expression 'dt.two' doesn't appear in the group by expressions", but MySQL will execute
				// this query. https://github.com/dolthub/dolt/issues/9717
				Query: "select 1 as a, one + 1 as mod1, dt.* from mytable as t1, (select 1, 2 from mytable) as dt (one, two) where dt.one > 0 group by one;",
				// column names:  a, mod1, one, two
				Expected: []sql.Row{{1, 2, 1, 2}},
			},
			{
				// MySQL will execute this query but group by validation isn't able to recognize that exprAlias is a
				// literal https://github.com/dolthub/dolt/issues/9717
				Query:    "select 1 as exprAlias, 2, 3, (select exprAlias + count(*) from one_pk_three_idx a cross join one_pk_three_idx b);",
				Expected: []sql.Row{{1, 2, 3, 65}},
			},
		},
	},
}
