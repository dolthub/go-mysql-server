// Copyright 2022 Dolthub, Inc.
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

	"github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/go-mysql-server/sql"
)

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
		Name: "various broken alias queries",
		Skip: true,
		Assertions: []ScriptTestAssertion{
			{
				// GMS returns "expression 'dt.two' doesn't appear in the group by expressions", but MySQL will execute
				// this query.
				Query: "select 1 as a, one + 1 as mod1, dt.* from mytable as t1, (select 1, 2 from mytable) as dt (one, two) where dt.one > 0 group by one;",
				// column names:  a, mod1, one, two
				Expected: []sql.Row{{1, 2, 1, 2}},
			},
		},
	},
}
