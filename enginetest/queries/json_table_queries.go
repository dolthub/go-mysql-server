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
	"github.com/dolthub/go-mysql-server/sql"
)

var JSONTableQueryTests = []QueryTest{
	{
		Query:    "SELECT * FROM JSON_TABLE(NULL,'$[*]' COLUMNS(x int path '$.a')) as t;",
		Expected: []sql.Row{},
	},
	{
		Query: "SELECT * FROM JSON_TABLE('[{\"a\":1},{\"a\":2}]','$[*]' COLUMNS(x varchar(100) path '$.a')) as tt;",
		Expected: []sql.Row{
			{"1"},
			{"2"},
		},
	},
	{
		Query: "SELECT * FROM JSON_TABLE('[{\"a\":1, \"b\":2},{\"a\":3, \"b\":4}]',\"$[*]\" COLUMNS(x int path '$.a', y int path '$.b')) as tt;",
		Expected: []sql.Row{
			{1, 2},
			{3, 4},
		},
	},
	{
		Query: "SELECT * FROM JSON_TABLE('[{\"a\":1.5, \"b\":2.25},{\"a\":3.125, \"b\":4.0625}]','$[*]' COLUMNS(x float path '$.a', y float path '$.b')) as tt;",
		Expected: []sql.Row{
			{1.5, 2.25},
			{3.125, 4.0625},
		},
	},
	{
		Query: "SELECT * FROM JSON_TABLE(concat('[{},','{}]'),'$[*]' COLUMNS(x varchar(100) path '$.a',y varchar(100) path '$.b')) as t;",
		Expected: []sql.Row{
			{nil, nil},
			{nil, nil},
		},
	},
	{
		Query: "select * from JSON_TABLE('[{\"a\":1},{\"a\":2}]', '$[*]' COLUMNS(x int path '$.a')) as t1 join JSON_TABLE('[{\"a\":1},{\"a\":2}]', '$[*]' COLUMNS(x int path '$.a')) as t2;",
		Expected: []sql.Row{
			{1, 1},
			{1, 2},
			{2, 1},
			{2, 2},
		},
	},
	{
		Query: "select * from JSON_TABLE('[{\"a\":1},{\"a\":2}]', '$[*]' COLUMNS(x int path '$.a')) as t1 join one_pk order by x, pk;",
		Expected: []sql.Row{
			{1, 0, 0, 1, 2, 3, 4},
			{1, 1, 10, 11, 12, 13, 14},
			{1, 2, 20, 21, 22, 23, 24},
			{1, 3, 30, 31, 32, 33, 34},
			{2, 0, 0, 1, 2, 3, 4},
			{2, 1, 10, 11, 12, 13, 14},
			{2, 2, 20, 21, 22, 23, 24},
			{2, 3, 30, 31, 32, 33, 34},
		},
	},
	{
		Query: "select * from one_pk join JSON_TABLE('[{\"a\":1},{\"a\":2}]', '$[*]' COLUMNS(x int path '$.a')) as t1 order by x, pk;",
		Expected: []sql.Row{
			{0, 0, 1, 2, 3, 4, 1},
			{1, 10, 11, 12, 13, 14, 1},
			{2, 20, 21, 22, 23, 24, 1},
			{3, 30, 31, 32, 33, 34, 1},
			{0, 0, 1, 2, 3, 4, 2},
			{1, 10, 11, 12, 13, 14, 2},
			{2, 20, 21, 22, 23, 24, 2},
			{3, 30, 31, 32, 33, 34, 2},
		},
	},
	{
		Query: "select * from JSON_TABLE('[{\"a\":1},{\"a\":2}]', '$[*]' COLUMNS(x int path '$.a')) as t1 union select * from JSON_TABLE('[{\"b\":3},{\"b\":4}]', '$[*]' COLUMNS(y int path '$.b')) as t2",
		Expected: []sql.Row{
			{1},
			{2},
			{3},
			{4},
		},
	},
	{
		Query: "select * from one_pk where pk in (select x from JSON_TABLE('[{\"a\":1},{\"a\":2}]', '$[*]' COLUMNS(x int path '$.a')) as t)",
		Expected: []sql.Row{
			{1, 10, 11, 12, 13, 14},
			{2, 20, 21, 22, 23, 24},
		},
	},
	{
		Query: "select * from JSON_TABLE('[{\"a\":1},{\"a\":2}]', '$[*]' COLUMNS(x int path '$.a')) t1 where x in (select y from JSON_TABLE('[{\"b\":1},{\"b\":100}]', '$[*]' COLUMNS(y int path '$.b')) as t2)",
		Expected: []sql.Row{
			{1},
		},
	},
	{
		Query: "with c as (select jt.a from json_table('[{\"a\":1,\"b\":2,\"c\":3},{\"a\":4,\"b\":5,\"c\":6},{\"a\":7,\"b\":8,\"c\":9}]', '$[*]' columns (a int path '$.a')) as jt) select * from c",
		Expected: []sql.Row{
			{1},
			{4},
			{7},
		},
	},
	{
		Query: "select * from json_table('[{\"a\":1,\"b\":2,\"c\":3},{\"a\":4,\"b\":5,\"c\":6},{\"a\":7,\"b\":8,\"c\":9}]', '$[*]' columns (a int path '$.a')) as jt\nunion\nselect * from json_table('[{\"a\":1,\"b\":2,\"c\":3},{\"a\":4,\"b\":5,\"c\":6},{\"a\":7,\"b\":8,\"c\":9}]', '$[*]' columns (b int path '$.b')) as jt\nunion\nselect * from json_table('[{\"a\":1,\"b\":2,\"c\":3},{\"a\":4,\"b\":5,\"c\":6},{\"a\":7,\"b\":8,\"c\":9}]', '$[*]' columns (c int path '$.c')) as jt;",
		Expected: []sql.Row{
			{1},
			{4},
			{7},
			{2},
			{5},
			{8},
			{3},
			{6},
			{9},
		},
	},
}

var JSONTableScriptTests = []ScriptTest{
	{
		Name: "create table from json column not qualified simple",
		SetUpScript: []string{
			"create table organizations (organization varchar(10), members json)",
			`insert into organizations values ("orgA", '["bob","john"]'), ("orgB", '["alice","mary"]')`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select names from organizations, JSON_TABLE(organizations.members, '$[*]' columns (names varchar(100) path '$')) as jt;",
				Expected: []sql.Row{
					{"bob"},
					{"john"},
					{"alice"},
					{"mary"},
				},
			},
			{
				Query: "select names from organizations, JSON_TABLE(members, '$[*]' columns (names varchar(100) path '$')) as jt;",
				Expected: []sql.Row{
					{"bob"},
					{"john"},
					{"alice"},
					{"mary"},
				},
			},
		},
	},
	{
		Name: "create table from json column not qualified complex",
		SetUpScript: []string{
			"create table organizations(organization varchar(10), members json);",
			`insert into organizations values("orgA", '["bob", "john"]'), ("orgB", '["alice", "mary"]'), ('orgC', '["kevin", "john"]'), ('orgD', '["alice", "alice"]')`,
		},
		Query: "SELECT names, COUNT(names) AS count FROM organizations, JSON_TABLE(organizations.members, '$[*]' COLUMNS (names varchar(100) path '$')) AS jt GROUP BY names ORDER BY names asc;",
		Expected: []sql.Row{
			{"alice", 3},
			{"bob", 1},
			{"john", 2},
			{"kevin", 1},
			{"mary", 1},
		},
	},
	{
		Name: "create table from json column qualified complex",
		SetUpScript: []string{
			"create table organizations(organization varchar(10), members json);",
			`insert into organizations values("orgA", '["bob", "john"]'), ("orgB", '["alice", "mary"]'), ('orgC', '["kevin", "john"]'), ('orgD', '["alice", "alice"]')`,
		},
		Query: "SELECT jt.names, COUNT(jt.names) AS count FROM organizations AS o, JSON_TABLE(o.members, '$[*]' COLUMNS (names varchar(100) path '$')) AS jt GROUP BY jt.names ORDER BY jt.names asc;",
		Expected: []sql.Row{
			{"alice", 3},
			{"bob", 1},
			{"john", 2},
			{"kevin", 1},
			{"mary", 1},
		},
	},
	{
		Name: "create table from json column aliased",
		SetUpScript: []string{
			"create table organizations (organization varchar(10), members json)",
			`insert into organizations values ("orgA", '["bob","john"]'), ("orgB", '["alice","mary"]')`,
		},
		Query: "select names from organizations o, JSON_TABLE(o.members, '$[*]' columns (names varchar(100) path '$')) as jt;",
		Expected: []sql.Row{
			{"bob"},
			{"john"},
			{"alice"},
			{"mary"},
		},
	},
	{
		Name: "create table from json column aliased column",
		SetUpScript: []string{
			"create table organizations (organization varchar(10), members json)",
			`insert into organizations values ("orgA", '["bob","john"]'), ("orgB", '["alice","mary"]')`,
		},
		Query: "select o.organization, jt.names from organizations o, JSON_TABLE(o.members, '$[*]' columns (names varchar(100) path '$')) as jt;",
		Expected: []sql.Row{
			{"orgA", "bob"},
			{"orgA", "john"},
			{"orgB", "alice"},
			{"orgB", "mary"},
		},
	},
	{
		Name: "cross join json table",
		SetUpScript: []string{
			"create table organizations (organization varchar(10), members json)",
			`insert into organizations values ("orgA", '["bob","john"]'), ("orgB", '["alice","mary"]')`,
		},
		Query: "select o.organization, jt.names from organizations o CROSS JOIN JSON_TABLE(o.members, '$[*]' columns (names varchar(100) path '$')) as jt;",
		Expected: []sql.Row{
			{"orgA", "bob"},
			{"orgA", "john"},
			{"orgB", "alice"},
			{"orgB", "mary"},
		},
	},
	{
		Name: "natural join json table",
		SetUpScript: []string{
			"create table organizations (organization varchar(10), members json)",
			`insert into organizations values ("orgA", '["bob","john"]'), ("orgB", '["alice","mary"]')`,
		},
		Query: "select o.organization, jt.names from organizations o NATURAL JOIN JSON_TABLE(o.members, '$[*]' columns (names varchar(100) path '$')) as jt;",
		Expected: []sql.Row{
			{"orgA", "bob"},
			{"orgA", "john"},
			{"orgB", "alice"},
			{"orgB", "mary"},
		},
	},
	{
		Name: "inner join json table with condition",
		SetUpScript: []string{
			"create table organizations (organization varchar(10), members json)",
			`insert into organizations values ("orgA", '["bob","john"]'), ("orgB", '["alice","mary"]')`,
		},
		Query: "select o.organization, jt.names from organizations o INNER JOIN JSON_TABLE(o.members, '$[*]' columns (names varchar(100) path '$')) as jt on o.organization = 'orgA';",
		Expected: []sql.Row{
			{"orgA", "bob"},
			{"orgA", "john"},
		},
	},
	{
		Name: "inner join json table with condition in subquery",
		SetUpScript: []string{
			`create table p (i int primary key)`,
			`insert into p values (1),(2),(3)`,
		},
		Query: `select (select jt.i from p inner join JSON_TABLE('[1,2,3]', '$[*]' columns (i int path '$')) as jt where p.i >= jt.i LIMIT 1);`,
		Expected: []sql.Row{
			{1},
		},
	},
	{
		Name: "left join json table with condition",
		SetUpScript: []string{
			`create table p (i int primary key)`,
			`insert into p values (1),(2),(3)`,
		},
		Query: `select * from p left join JSON_TABLE('[1,2,3]', '$[*]' columns (i int path '$')) as jt on p.i > jt.i;`,
		Expected: []sql.Row{
			{1, nil},
			{2, 1},
			{3, 1},
			{3, 2},
		},
	},
	{
		Name: "right join json table with condition",
		SetUpScript: []string{
			`create table p (i int primary key)`,
			`insert into p values (1),(2),(3)`,
		},
		Query: `select * from p right join JSON_TABLE('[1,2,3]', '$[*]' columns (i int path '$')) as jt on p.i > jt.i;`,
		Expected: []sql.Row{
			{2, 1},
			{3, 1},
			{3, 2},
			{nil, 3},
		},
	},
	{
		Name: "json table in subquery references parent data",
		SetUpScript: []string{
			"create table t (i int, j json)",
			`insert into t values (1, '["test"]')`,
		},
		Query: "select i, (select names from JSON_Table(t.j, '$[*]' columns (names varchar(100) path '$')) jt) from t;",
		Expected: []sql.Row{
			{1, "test"},
		},
	},
	{
		Name: "json table in subquery qualified with parent",
		SetUpScript: []string{
			"create table t (i int, j json)",
			`insert into t values (1, '["test"]')`,
		},
		Query: "select (select names from JSON_Table(t.j, '$[*]' columns (names varchar(100) path '$')) jt) from t;",
		Expected: []sql.Row{
			{"test"},
		},
	},
	{
		Name: "json table in cte",
		SetUpScript: []string{
			`create table tbl (i int primary key, j json)`,
			`insert into tbl values (0, '[{"a":1,"b":2,"c":3},{"a":4,"b":5,"c":6},{"a":7,"b":8,"c":9}]')`,
		},
		Query: "with c as (select jt.a from tbl, json_table(tbl.j, '$[*]' columns (a int path '$.a')) as jt) select * from c",
		Expected: []sql.Row{
			{1},
			{4},
			{7},
		},
	},
	{
		Name: "json table join with cte",
		SetUpScript: []string{
			`create table t (i int primary key)`,
			`insert into t values (1), (2)`,
		},
		Query: "with tt as (select * from t) select * from tt, json_table('[{\"a\":3}]', '$[*]' columns (a int path '$.a')) as jt",
		Expected: []sql.Row{
			{1, 3},
			{2, 3},
		},
	},
	{
		Name: "join table, json_table, json_table",
		SetUpScript: []string{
			`create table tbl (i int primary key, j json)`,
			`insert into tbl values (0, '[{"a":1,"b":2,"c":3},{"a":4,"b":5,"c":6},{"a":7,"b":8,"c":9}]')`,
		},
		Query: "select j1.a, j2.b, j3.c from tbl, json_table(tbl.j, '$[*]' columns (a int path '$.a')) as j1, json_table(tbl.j, '$[*]' columns (b int path '$.b')) as j2, json_table(tbl.j, '$[*]' columns (c int path '$.c')) as j3;",
		Expected: []sql.Row{
			{1, 2, 3},
			{1, 2, 6},
			{1, 2, 9},
			{1, 5, 3},
			{1, 5, 6},
			{1, 5, 9},
			{1, 8, 3},
			{1, 8, 6},
			{1, 8, 9},
			{4, 2, 3},
			{4, 2, 6},
			{4, 2, 9},
			{4, 5, 3},
			{4, 5, 6},
			{4, 5, 9},
			{4, 8, 3},
			{4, 8, 6},
			{4, 8, 9},
			{7, 2, 3},
			{7, 2, 6},
			{7, 2, 9},
			{7, 5, 3},
			{7, 5, 6},
			{7, 5, 9},
			{7, 8, 3},
			{7, 8, 6},
			{7, 8, 9},
		},
	},
	{
		Name: "join table, table, json_table",
		SetUpScript: []string{
			`create table t1 (x int primary key)`,
			`insert into t1 values (1), (2)`,
			`create table t2 (y int primary key)`,
			`insert into t2 values (3), (4)`,
			`create table tbl (j json)`,
			`insert into tbl values ('[{"a":5},{"a":6}]')`,
		},
		Query: "select t1.x, t2.y, jt.a from t1, t2, tbl, json_table(tbl.j, '$[*]' columns (a int path '$.a')) as jt",
		Expected: []sql.Row{
			{1, 3, 5},
			{1, 3, 6},
			{1, 4, 5},
			{1, 4, 6},
			{2, 3, 5},
			{2, 3, 6},
			{2, 4, 5},
			{2, 4, 6},
		},
	},
	{
		Name: "join table, table, json_table two references past one node",
		SetUpScript: []string{
			`create table t1 (i int, x json)`,
			`insert into t1 values (1, '[{"a":5},{"a":6}]')`,
			`create table t2 (y int primary key)`,
			`insert into t2 values (3), (4)`,
			`create table tbl (j json)`,
			`insert into tbl values ('[{"a":5},{"a":6}]')`,
		},
		Query: "select t1.i, t2.y, jt.a from t1, t2, tbl, json_table(t1.x, '$[*]' columns (a int path '$.a')) as jt",
		Expected: []sql.Row{
			{1, 3, 5},
			{1, 3, 6},
			{1, 4, 5},
			{1, 4, 6},
		},
	},
	{
		Name: "table union cross join with json table",
		SetUpScript: []string{
			"create table t (i int, j json)",
			`insert into t values (1, '["test"]')`,
		},
		Query: "select t.j from t union select a from t, json_table(t.j, '$[*]' columns (a varchar(10) path '$')) as jt;",
		Expected: []sql.Row{
			{"[\"test\"]"},
			{"test"},
		},
	},

	// Error tests
	{
		Name: "non existent unqualified column",
		SetUpScript: []string{
			"create table t (i int, j json)",
		},
		Query:       "select j.a from t, json_table(k, '$[*]' columns (a INT path '$.a')) AS j",
		ExpectedErr: sql.ErrColumnNotFound,
	},
	{
		Name: "non existent qualified column",
		SetUpScript: []string{
			"create table t (i int, j json)",
		},
		Query:       "select t.a from t, json_table(t.k, '$[*]' columns (a INT path '$.a')) AS j",
		ExpectedErr: sql.ErrTableColumnNotFound,
	},
	{
		Name: "select from non existent json table column",
		SetUpScript: []string{
			"create table t (i int, j json)",
		},
		Query:       "select j.b from t, json_table(t.j, '$[*]' columns (a INT path '$.a')) AS j",
		ExpectedErr: sql.ErrTableColumnNotFound,
	},
	{
		Name: "subquery argument to json_table not allowed",
		SetUpScript: []string{
			"create table t (i int, j json)",
			`insert into t values (1, '["test"]')`,
		},
		Query:       "select * from json_table((select j from t), '$[*]' columns (a varchar(10) path '$')) as jt;",
		ExpectedErr: sql.ErrInvalidArgument,
	},
}

var BrokenJSONTableScriptTests = []ScriptTest{
	{
		// subqueries start using the dummy schema for some reason, not isolated to JSON_Tables
		Name: "json table in cross join in subquery",
		SetUpScript: []string{
			"create table t (j json)",
		},
		Query: "select (select jt.a from t, json_table('[\"abc\"]', '$[*]' columns (a varchar(10) path '$')) as jt)",
		Expected: []sql.Row{
			{"abc"},
		},
	},
	{
		// subqueries start using the dummy schema for some reason, not isolated to JSON_Tables
		Name: "json table in cross join in subquery with reference to left",
		SetUpScript: []string{
			"create table t (i int, j json)",
			`insert into t values (1, '["test"]')`,
		},
		Query: "select (select a from t, json_table(t.j, '$[*]' columns (a varchar(10) path '$')) as jt)",
		Expected: []sql.Row{
			{1},
		},
	},
	{
		// wrong error
		Name: "json_table out of cte",
		SetUpScript: []string{
			"create table t (i int, j json)",
			`insert into t values (1, '["test"]')`,
		},
		Query:       "with tt as (select * from t) select * from json_table(tt.j, '$[*]' columns (a varchar(10) path '$')) as jt;",
		ExpectedErr: sql.ErrTableNotFound,
	},
	{
		// this should error with incorrect arguments
		Name: "json_table out of cte with join",
		SetUpScript: []string{
			"create table t (i int, j json)",
			`insert into t values (1, '["test"]')`,
		},
		Query:       "with tt as (select * from t) select * from tt, json_table(tt.j, '$[*]' columns (a varchar(10) path '$')) as jt;",
		ExpectedErr: sql.ErrInvalidArgument,
	},
}
