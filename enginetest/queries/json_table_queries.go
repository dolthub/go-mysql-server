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
	"github.com/dolthub/go-mysql-server/sql/types"
)

var JSONTableQueryTests = []QueryTest{
	{
		Query:    "SELECT * FROM JSON_TABLE(NULL,'$[*]' COLUMNS(x int path '$.a')) as t;",
		Expected: []sql.UntypedSqlRow{},
	},
	{
		Query:    "SELECT * FROM JSON_TABLE('{}','$[*]' COLUMNS(x int path '$.a')) as t;",
		Expected: []sql.UntypedSqlRow{},
	},
	{
		Query:    "SELECT * FROM JSON_TABLE('{\"a\":1}','$.b' COLUMNS(x varchar(100) path '$.a')) as tt;",
		Expected: []sql.UntypedSqlRow{},
	},
	{
		Query: "SELECT * FROM JSON_TABLE('[{\"a\":1},{\"a\":2}]','$[*]' COLUMNS(x varchar(100) path '$.a')) as tt;",
		Expected: []sql.UntypedSqlRow{
			{"1"},
			{"2"},
		},
	},
	{
		Query: "SELECT * FROM JSON_TABLE('[{\"a\":1, \"b\":2},{\"a\":3, \"b\":4}]',\"$[*]\" COLUMNS(x int path '$.a', y int path '$.b')) as tt;",
		Expected: []sql.UntypedSqlRow{
			{1, 2},
			{3, 4},
		},
	},
	{
		Query: "SELECT * FROM JSON_TABLE('[{\"a\":1.5, \"b\":2.25},{\"a\":3.125, \"b\":4.0625}]','$[*]' COLUMNS(x float path '$.a', y float path '$.b')) as tt;",
		Expected: []sql.UntypedSqlRow{
			{1.5, 2.25},
			{3.125, 4.0625},
		},
	},
	{
		Query: "SELECT * FROM JSON_TABLE(concat('[{},','{}]'),'$[*]' COLUMNS(x varchar(100) path '$.a',y varchar(100) path '$.b')) as t;",
		Expected: []sql.UntypedSqlRow{
			{nil, nil},
			{nil, nil},
		},
	},
	{
		Query: "select * from JSON_TABLE('[{\"a\":1},{\"a\":2}]', '$[*]' COLUMNS(x int path '$.a')) as t1 join JSON_TABLE('[{\"a\":1},{\"a\":2}]', '$[*]' COLUMNS(x int path '$.a')) as t2;",
		Expected: []sql.UntypedSqlRow{
			{1, 1},
			{1, 2},
			{2, 1},
			{2, 2},
		},
	},
	{
		Query: "select * from JSON_TABLE('[{\"a\":1},{\"a\":2}]', '$[*]' COLUMNS(x int path '$.a')) as t1 join one_pk order by x, pk;",
		Expected: []sql.UntypedSqlRow{
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
		Expected: []sql.UntypedSqlRow{
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
		Expected: []sql.UntypedSqlRow{
			{1},
			{2},
			{3},
			{4},
		},
	},
	{
		Query: "select * from one_pk where pk in (select x from JSON_TABLE('[{\"a\":1},{\"a\":2}]', '$[*]' COLUMNS(x int path '$.a')) as t)",
		Expected: []sql.UntypedSqlRow{
			{1, 10, 11, 12, 13, 14},
			{2, 20, 21, 22, 23, 24},
		},
	},
	{
		Query: "select * from JSON_TABLE('[{\"a\":1},{\"a\":2}]', '$[*]' COLUMNS(x int path '$.a')) t1 where x in (select y from JSON_TABLE('[{\"b\":1},{\"b\":100}]', '$[*]' COLUMNS(y int path '$.b')) as t2)",
		Expected: []sql.UntypedSqlRow{
			{1},
		},
	},
	{
		Query: "with c as (select jt.a from json_table('[{\"a\":1,\"b\":2,\"c\":3},{\"a\":4,\"b\":5,\"c\":6},{\"a\":7,\"b\":8,\"c\":9}]', '$[*]' columns (a int path '$.a')) as jt) select * from c",
		Expected: []sql.UntypedSqlRow{
			{1},
			{4},
			{7},
		},
	},
	{
		Query: "select * from json_table('[{\"a\":1,\"b\":2,\"c\":3},{\"a\":4,\"b\":5,\"c\":6},{\"a\":7,\"b\":8,\"c\":9}]', '$[*]' columns (a int path '$.a')) as jt\nunion\nselect * from json_table('[{\"a\":1,\"b\":2,\"c\":3},{\"a\":4,\"b\":5,\"c\":6},{\"a\":7,\"b\":8,\"c\":9}]', '$[*]' columns (b int path '$.b')) as jt\nunion\nselect * from json_table('[{\"a\":1,\"b\":2,\"c\":3},{\"a\":4,\"b\":5,\"c\":6},{\"a\":7,\"b\":8,\"c\":9}]', '$[*]' columns (c int path '$.c')) as jt;",
		Expected: []sql.UntypedSqlRow{
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
		Name: "create table from json column",
		SetUpScript: []string{
			"create table organizations (organization varchar(10), members json)",
			`insert into organizations values("orgA", '["bob", "john"]'), ("orgB", '["alice", "mary"]'), ('orgC', '["kevin", "john"]'), ('orgD', '["alice", "alice"]')`,
			"create table t1(json_col json);",
			"insert into t1 values ('{ \"people\": [{\"name\":\"John Smith\", \"address\":\"780 Mission St, San Francisco, CA 94103\"}, { \"name\":\"Sally Brown\", \"address\":\"75 37th Ave S, St Cloud, MN 94103\"}, { \"name\":\"John Johnson\", \"address\":\"1262 Roosevelt Trail, Raymond, ME 04071\"}]}')",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select names from organizations, JSON_TABLE(members, '$[*]' columns (names varchar(100) path '$')) as jt;",
				Expected: []sql.UntypedSqlRow{
					{"bob"},
					{"john"},
					{"alice"},
					{"mary"},
					{"kevin"},
					{"john"},
					{"alice"},
					{"alice"},
				},
			},
			{
				Query: "SELECT names, COUNT(names) AS count FROM organizations, JSON_TABLE(members, '$[*]' COLUMNS (names varchar(100) path '$')) AS jt GROUP BY names ORDER BY names asc;",
				Expected: []sql.UntypedSqlRow{
					{"alice", 3},
					{"bob", 1},
					{"john", 2},
					{"kevin", 1},
					{"mary", 1},
				},
			},
			{
				// qualified json column name
				Query: "select names from organizations, JSON_TABLE(organizations.members, '$[*]' columns (names varchar(100) path '$')) as jt;",
				Expected: []sql.UntypedSqlRow{
					{"bob"},
					{"john"},
					{"alice"},
					{"mary"},
					{"kevin"},
					{"john"},
					{"alice"},
					{"alice"},
				},
			},
			{
				// aliased without as keyword
				Query: "select names from organizations o, JSON_TABLE(o.members, '$[*]' columns (names varchar(100) path '$')) as jt;",
				Expected: []sql.UntypedSqlRow{
					{"bob"},
					{"john"},
					{"alice"},
					{"mary"},
					{"kevin"},
					{"john"},
					{"alice"},
					{"alice"},
				},
			},
			{
				// aliased table name
				Query: "SELECT jt.names, COUNT(jt.names) AS count FROM organizations AS o, JSON_TABLE(o.members, '$[*]' COLUMNS (names varchar(100) path '$')) AS jt GROUP BY jt.names ORDER BY jt.names asc;",
				Expected: []sql.UntypedSqlRow{
					{"alice", 3},
					{"bob", 1},
					{"john", 2},
					{"kevin", 1},
					{"mary", 1},
				},
			},
			{
				// aliased and qualified select
				Query: "select o.organization, jt.names from organizations o, JSON_TABLE(o.members, '$[*]' columns (names varchar(100) path '$')) as jt;",
				Expected: []sql.UntypedSqlRow{
					{"orgA", "bob"},
					{"orgA", "john"},
					{"orgB", "alice"},
					{"orgB", "mary"},
					{"orgC", "kevin"},
					{"orgC", "john"},
					{"orgD", "alice"},
					{"orgD", "alice"},
				},
			},
			{
				Query: "SELECT people.* FROM t1, JSON_TABLE(t1.json_col, '$.people[*]' COLUMNS (name VARCHAR(40) PATH '$.name', address VARCHAR(100) PATH '$.address')) people;",
				Expected: []sql.UntypedSqlRow{
					{"John Smith", "780 Mission St, San Francisco, CA 94103"},
					{"Sally Brown", "75 37th Ave S, St Cloud, MN 94103"},
					{"John Johnson", "1262 Roosevelt Trail, Raymond, ME 04071"},
				},
			},
		},
	},
	{
		Name: "test other join types",
		SetUpScript: []string{
			"create table organizations (organization varchar(10), members json)",
			`insert into organizations values ("orgA", '["bob","john"]'), ("orgB", '["alice","mary"]')`,
			`create table p (i int primary key)`,
			`insert into p values (1),(2),(3)`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select o.organization, jt.names from organizations o CROSS JOIN JSON_TABLE(o.members, '$[*]' columns (names varchar(100) path '$')) as jt;",
				Expected: []sql.UntypedSqlRow{
					{"orgA", "bob"},
					{"orgA", "john"},
					{"orgB", "alice"},
					{"orgB", "mary"},
				},
			},
			{
				Query: "select o.organization, jt.names from organizations o NATURAL JOIN JSON_TABLE(o.members, '$[*]' columns (names varchar(100) path '$')) as jt;",
				Expected: []sql.UntypedSqlRow{
					{"orgA", "bob"},
					{"orgA", "john"},
					{"orgB", "alice"},
					{"orgB", "mary"},
				},
			},
			{
				Query: "select o.organization, jt.names from organizations o INNER JOIN JSON_TABLE(o.members, '$[*]' columns (names varchar(100) path '$')) as jt on o.organization = 'orgA';",
				Expected: []sql.UntypedSqlRow{
					{"orgA", "bob"},
					{"orgA", "john"},
				},
			},
			{
				Query: `select (select jt.i from p inner join JSON_TABLE('[1,2,3]', '$[*]' columns (i int path '$')) as jt where p.i >= jt.i LIMIT 1);`,
				Expected: []sql.UntypedSqlRow{
					{1},
				},
			},
			{
				Query: `select * from p left join JSON_TABLE('[1,2,3]', '$[*]' columns (i int path '$')) as jt on p.i > jt.i;`,
				Expected: []sql.UntypedSqlRow{
					{1, nil},
					{2, 1},
					{3, 1},
					{3, 2},
				},
			},
			{
				Query: `select * from p right join JSON_TABLE('[1,2,3]', '$[*]' columns (i int path '$')) as jt on p.i > jt.i;`,
				Expected: []sql.UntypedSqlRow{
					{2, 1},
					{3, 1},
					{3, 2},
					{nil, 3},
				},
			},
		},
	},
	{
		Name: "json table in subquery references parent data",
		SetUpScript: []string{
			"create table t (i int, j json)",
			`insert into t values (1, '["test"]')`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select i, (select names from JSON_Table(t.j, '$[*]' columns (names varchar(100) path '$')) jt) from t;",
				Expected: []sql.UntypedSqlRow{
					{1, "test"},
				},
			},
			{
				Query: "select (select jt.a from t, json_table('[\"abc\"]', '$[*]' columns (a varchar(10) path '$')) as jt)",
				Expected: []sql.UntypedSqlRow{
					{"abc"},
				},
			},
			{
				Query: "select (select a from t, json_table(t.j, '$[*]' columns (a varchar(10) path '$')) as jt)",
				Expected: []sql.UntypedSqlRow{
					{"test"},
				},
			},
		},
	},
	{
		Name: "json table in cte",
		SetUpScript: []string{
			`create table tbl (i int primary key, j json)`,
			`insert into tbl values (0, '[{"a":1,"b":2,"c":3},{"a":4,"b":5,"c":6},{"a":7,"b":8,"c":9}]')`,
			`create table t (i int primary key)`,
			`insert into t values (1), (2)`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "with c as (select jt.a from tbl, json_table(tbl.j, '$[*]' columns (a int path '$.a')) as jt) select * from c",
				Expected: []sql.UntypedSqlRow{
					{1},
					{4},
					{7},
				},
			},
			{
				Query: "with tt as (select * from t) select * from tt, json_table('[{\"a\":3}]', '$[*]' columns (a int path '$.a')) as jt",
				Expected: []sql.UntypedSqlRow{
					{1, 3},
					{2, 3},
				},
			},
		},
	},
	{
		Name: "table union cross join with json table",
		SetUpScript: []string{
			"create table t (i int, j json)",
			`insert into t values (1, '["test"]')`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select t.j from t union select a from t, json_table(t.j, '$[*]' columns (a varchar(10) path '$')) as jt;",
				Expected: []sql.UntypedSqlRow{
					{"[\"test\"]"},
					{"test"},
				},
			},
		},
	},
	{
		Name: "join table, json_table, json_table",
		SetUpScript: []string{
			`create table tbl (i int primary key, j json)`,
			`insert into tbl values (0, '[{"a":1,"b":2,"c":3},{"a":4,"b":5,"c":6},{"a":7,"b":8,"c":9}]')`,
		},
		Query: "select j1.a, j2.b, j3.c from tbl, json_table(tbl.j, '$[*]' columns (a int path '$.a')) as j1, json_table(tbl.j, '$[*]' columns (b int path '$.b')) as j2, json_table(tbl.j, '$[*]' columns (c int path '$.c')) as j3;",
		Expected: []sql.UntypedSqlRow{
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
		Expected: []sql.UntypedSqlRow{
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
		Expected: []sql.UntypedSqlRow{
			{1, 3, 5},
			{1, 3, 6},
			{1, 4, 5},
			{1, 4, 6},
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

	{
		Name:        "test FOR ORDINALITY",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM JSON_TABLE('{}', '$' COLUMNS( pk FOR ORDINALITY, c1 INT PATH '$.c1')) as jt;",
				Expected: []sql.UntypedSqlRow{
					{1, nil},
				},
			},
			{
				Query:    "SELECT * FROM JSON_TABLE('{}', '$[*]' COLUMNS( pk FOR ORDINALITY, c1 INT PATH '$.c1')) as jt;",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query: "SELECT * FROM JSON_TABLE('[{\"c1\": 333}, {\"c1\": 222}, {\"c1\": 111}]', '$[*]' COLUMNS( pk FOR ORDINALITY, c1 INT PATH '$.c1')) as jt;",
				Expected: []sql.UntypedSqlRow{
					{1, 333},
					{2, 222},
					{3, 111},
				},
			},
			{
				Query: "SELECT * FROM JSON_TABLE('[{\"c1\": 333}, {\"c1\": 222}, {\"c1\": 111}]', '$[*]' COLUMNS( pk1 FOR ORDINALITY, pk2 FOR ORDINALITY, c1 INT PATH '$.c1')) as jt;",
				Expected: []sql.UntypedSqlRow{
					{1, 1, 333},
					{2, 2, 222},
					{3, 3, 111},
				},
			},
		},
	},
	{
		Name:        "test EXISTS",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM JSON_TABLE('{}', '$' COLUMNS(c1 INT EXISTS PATH '$.c1')) as jt;",
				Expected: []sql.UntypedSqlRow{
					{0},
				},
			},
			{
				Query: "SELECT * FROM JSON_TABLE('{\"c1\": 123}', '$' COLUMNS(c1 INT EXISTS PATH '$.c1')) as jt;",
				Expected: []sql.UntypedSqlRow{
					{1},
				},
			},
			{
				Query: "SELECT * FROM JSON_TABLE('[{\"c1\": 333}, {\"c1\": 222}, {\"c1\": 111}, {\"notc1\": 123}]', '$[*]' COLUMNS(c1 INT EXISTS PATH '$.c1')) as jt;",
				Expected: []sql.UntypedSqlRow{
					{1},
					{1},
					{1},
					{0},
				},
			},
			{
				Query: "SELECT * FROM JSON_TABLE('[{\"a\": 333}, {\"b\": 222}, {\"a\": 111}, {\"b\": 123}]', '$[*]' COLUMNS(a INT EXISTS PATH '$.a', b INT EXISTS PATH '$.b')) as jt;",
				Expected: []sql.UntypedSqlRow{
					{1, 0},
					{0, 1},
					{1, 0},
					{0, 1},
				},
			},
		},
	},
	{
		Name:        "test DEFAULT ON ERROR",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM JSON_TABLE('{}', '$' COLUMNS(c1 INT PATH '$.c1' DEFAULT '123' ON ERROR)) as jt;",
				Expected: []sql.UntypedSqlRow{
					{nil},
				},
			},
			{
				Query: "SELECT * FROM JSON_TABLE('{\"c1\":\"abc\"}', '$' COLUMNS(c1 INT PATH '$.c1' DEFAULT '123' ON ERROR)) as jt;",
				Expected: []sql.UntypedSqlRow{
					{123},
				},
			},
			{
				Query:          "SELECT * FROM JSON_TABLE('{\"c1\":\"abc\"}', '$' COLUMNS(c1 INT PATH '$.c1' DEFAULT 'def' ON ERROR)) as jt;",
				ExpectedErrStr: "error: 'def' is not a valid value for 'int'",
			},
		},
	},
	{
		Name:        "test DEFAULT ON EMPTY",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM JSON_TABLE('{}', '$' COLUMNS(c1 INT PATH '$.c1' DEFAULT '123' ON EMPTY)) as jt;",
				Expected: []sql.UntypedSqlRow{
					{123},
				},
			},
			{
				Query: "SELECT * FROM JSON_TABLE('{\"notc1\": \"321321\"}', '$' COLUMNS(c1 INT PATH '$.c1' DEFAULT '123' ON EMPTY)) as jt;",
				Expected: []sql.UntypedSqlRow{
					{123},
				},
			},
			{
				// MySQL only supports string type for DEFAULT
				Query: "SELECT * FROM JSON_TABLE('{}', '$' COLUMNS(c1 INT PATH '$.c1' DEFAULT 123 ON EMPTY)) as jt;",
				Expected: []sql.UntypedSqlRow{
					{123},
				},
			},
		},
	},
	{
		Name:        "test ERROR ON ERROR",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM JSON_TABLE('{}', '$' COLUMNS(c1 INT PATH '$.c1' ERROR ON ERROR)) as jt;",
				Expected: []sql.UntypedSqlRow{
					{nil},
				},
			},
			{
				Query:          "SELECT * FROM JSON_TABLE('{\"c1\":\"abc\"}', '$' COLUMNS(c1 INT PATH '$.c1' ERROR ON ERROR)) as jt;",
				ExpectedErrStr: "error: 'abc' is not a valid value for 'int'",
			},
		},
	},
	{
		Name:        "test ERROR ON EMPTY",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query:          "SELECT * FROM JSON_TABLE('{}', '$' COLUMNS(c1 INT PATH '$.c1' ERROR ON EMPTY)) as jt;",
				ExpectedErrStr: "missing value for JSON_TABLE column 'c1'",
			},
			{
				Query:          "SELECT * FROM JSON_TABLE('{\"notc1\": \"321321\"}', '$' COLUMNS(c1 INT PATH '$.c1' ERROR ON EMPTY)) as jt;",
				ExpectedErrStr: "missing value for JSON_TABLE column 'c1'",
			},
		},
	},
	{
		Name:        "test NESTED simple",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM JSON_TABLE('[{\"a\": 1, \"b\": [11,111]}, {\"a\": 2, \"b\": [22,222]}]', '$[*]' COLUMNS(a INT PATH '$.a', NESTED PATH '$.b[*]' COLUMNS (b1 INT PATH '$'))) AS jt;",
				Expected: []sql.UntypedSqlRow{
					{1, 11},
					{1, 111},
					{2, 22},
					{2, 222},
				},
			},
			{
				Query: "SELECT * FROM  JSON_TABLE('[{\"a\": 1, \"b\": [11,111]}, {\"a\": 2, \"b\": [22,222]}]', '$[*]' COLUMNS( a INT PATH '$.a', NESTED PATH '$.b[*]' COLUMNS (b1 INT PATH '$', b2 INT PATH '$'))) AS jt;",
				Expected: []sql.UntypedSqlRow{
					{1, 11, 11},
					{1, 111, 111},
					{2, 22, 22},
					{2, 222, 222},
				},
			},
			// TODO: the path '$.b' and `$.b[*]` are equivalent, but somehow MySQL can differentiate this from the above test
			{
				Query: "SELECT * FROM  JSON_TABLE('[{\"a\": 1, \"b\": [11,111]}, {\"a\": 2, \"b\": [22,222]}]', '$[*]' COLUMNS( a INT PATH '$.a', NESTED PATH '$.b' COLUMNS (b1 INT PATH '$[0]', b2 INT PATH '$[1]'))) AS jt;",
				Skip:  true,
				Expected: []sql.UntypedSqlRow{
					{1, 11, 111},
					{2, 22, 222},
				},
			},
		},
	},
	{
		Name:        "test NESTED siblings",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM  JSON_TABLE('[{\"a\": 1, \"b\": [11,111]}, {\"a\": 2, \"b\": [22,222]}]', '$[*]' COLUMNS( a INT PATH '$.a', NESTED PATH '$.b[*]' COLUMNS (b1 INT PATH '$'), NESTED PATH '$.b[*]' COLUMNS (b2 INT PATH '$'), NESTED PATH '$.b[*]' COLUMNS (b3 INT PATH '$'))) AS jt;",
				Expected: []sql.UntypedSqlRow{
					{1, 11, nil, nil},
					{1, 111, nil, nil},
					{1, nil, 11, nil},
					{1, nil, 111, nil},
					{1, nil, nil, 11},
					{1, nil, nil, 111},
					{2, 22, nil, nil},
					{2, 222, nil, nil},
					{2, nil, 22, nil},
					{2, nil, 222, nil},
					{2, nil, nil, 22},
					{2, nil, nil, 222},
				},
			},
		},
	},
	{
		Name:        "test NESTED NESTED",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			// TODO: double check the for ordinal column
			{
				Query: `
SELECT *
FROM
JSON_TABLE(
    '{"a": [123, 456]}',
    '$.a[*]' COLUMNS(
        id1 FOR ORDINALITY,
        a1 INT PATH '$',
        b1 INT PATH '$',
        c1 INT PATH '$',
        NESTED PATH '$' COLUMNS (
            id2 FOR ORDINALITY,
            i1 INT PATH '$',
            j1 INT PATH '$',
            k1 INT PATH '$',
            NESTED PATH '$' COLUMNS (
                id4 FOR ORDINALITY,
                x1 INT PATH '$',
                y1 INT PATH '$',
                z1 INT PATH '$'
            ),
            NESTED PATH '$' COLUMNS (
                id5 FOR ORDINALITY,
                x2 INT PATH '$',
                y2 INT PATH '$',
                z2 INT PATH '$'
            )
        ),
        NESTED PATH '$' COLUMNS (
            id6 FOR ORDINALITY,
            i2 INT PATH '$',
            j2 INT PATH '$',
            k2 INT PATH '$',
            NESTED PATH '$' COLUMNS (
                id7 FOR ORDINALITY,
                x3 INT PATH '$',
                y3 INT PATH '$',
                z3 INT PATH '$'
            ),
            NESTED PATH '$' COLUMNS (
                id8 FOR ORDINALITY,
                x4 INT PATH '$',
                y4 INT PATH '$',
                z4 INT PATH '$'
            )
        )
    )
) as jt;
`,
				Expected: []sql.UntypedSqlRow{
					{1, 123, 123, 123, 1, 123, 123, 123, 1, 123, 123, 123, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil},
					{1, 123, 123, 123, 1, 123, 123, 123, nil, nil, nil, nil, 1, 123, 123, 123, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil},
					{1, 123, 123, 123, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, 1, 123, 123, 123, 1, 123, 123, 123, nil, nil, nil, nil},
					{1, 123, 123, 123, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, 1, 123, 123, 123, nil, nil, nil, nil, 1, 123, 123, 123},
					{2, 456, 456, 456, 1, 456, 456, 456, 1, 456, 456, 456, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil},
					{2, 456, 456, 456, 1, 456, 456, 456, nil, nil, nil, nil, 1, 456, 456, 456, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil},
					{2, 456, 456, 456, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, 1, 456, 456, 456, 1, 456, 456, 456, nil, nil, nil, nil},
					{2, 456, 456, 456, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, 1, 456, 456, 456, nil, nil, nil, nil, 1, 456, 456, 456},
				},
			},
		},
	},
	{
		Name:        "test combinations of options",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				// From MySQL docs
				Query: "SELECT * FROM JSON_TABLE('[{\"a\":\"3\"},{\"a\":2},{\"b\":1},{\"a\":0},{\"a\":[1,2]}]', \"$[*]\" COLUMNS (rowid FOR ORDINALITY, ac VARCHAR(100) PATH \"$.a\" DEFAULT '111' ON EMPTY DEFAULT '999' ON ERROR, aj JSON PATH \"$.a\" DEFAULT '{\"x\": 333}' ON EMPTY, bx INT EXISTS PATH \"$.b\")) AS tt;",
				Expected: []sql.UntypedSqlRow{
					{1, "3", types.MustJSON("3"), 0},
					{2, "2", types.MustJSON("2"), 0},
					{3, "111", types.MustJSON("{\"x\": 333}"), 1},
					{4, "0", types.MustJSON("0"), 0},
					{5, "999", types.MustJSON("[1, 2]"), 0},
				},
			},
			{
				Query: "SELECT * FROM JSON_TABLE('[{\"x\":2,\"y\":\"8\"},{\"x\":\"3\",\"y\":\"7\"},{\"x\":\"4\",\"y\":6}]', \"$[*]\" COLUMNS (xval VARCHAR(100) PATH \"$.x\", yval VARCHAR(100) PATH \"$.y\")) AS  jt1;",
				Expected: []sql.UntypedSqlRow{
					{"2", "8"},
					{"3", "7"},
					{"4", "6"},
				},
			},
			{
				Query: "SELECT * FROM JSON_TABLE('[{\"x\":2,\"y\":\"8\"},{\"x\":\"3\",\"y\":\"7\"},{\"x\":\"4\",\"y\":6}]', \"$[1]\" COLUMNS (xval VARCHAR(100) PATH \"$.x\", yval VARCHAR(100) PATH \"$.y\")) AS  jt1;",
				Expected: []sql.UntypedSqlRow{
					{"3", "7"},
				},
			},
		},
	},
}

var BrokenJSONTableScriptTests = []ScriptTest{
	{
		// wrong error
		Name: "json_table out of cte",
		SetUpScript: []string{
			"create table t (i int, j json)",
			`insert into t values (1, '["test"]')`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "with tt as (select * from t) select * from json_table(tt.j, '$[*]' columns (a varchar(10) path '$')) as jt;",
				ExpectedErr: sql.ErrUnknownTable,
			},
			{
				Query:       "with tt as (select * from t) select * from tt, json_table(tt.j, '$[*]' columns (a varchar(10) path '$')) as jt;",
				ExpectedErr: sql.ErrInvalidArgument,
			},
		},
	},
	{
		// Unsupported functionality
		Name:        "json_table out of cte",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM JSON_TABLE('[ {\"c1\": null} ]', '$[*]' COLUMNS( c1 INT PATH '$.c1' ERROR ON ERROR )) as jt;",
				Expected: []sql.UntypedSqlRow{
					{nil},
				},
			},
			{
				Query: "SELECT * FROM JSON_TABLE('[{\"a\":\"3\"},{\"a\":2},{\"b\":1},{\"a\":0},{\"a\":[1,2]}]', \"$[*]\" COLUMNS(rowid FOR ORDINALITY, ac VARCHAR(100) PATH \"$.a\" DEFAULT '111' ON EMPTY DEFAULT '999' ON ERROR, aj JSON PATH \"$.a\" DEFAULT '{\"x\": 333}' ON EMPTY, bx INT EXISTS PATH \"$.b\")) AS tt;",
				Expected: []sql.UntypedSqlRow{
					{1, 3, "3", 0},
					{2, 2, 2, 0},
					{3, 111, types.MustJSON("{\"x\": 333}"), 1},
					{4, 0, 0, 0},
					{5, 999, types.MustJSON("[1, 2]"), 0},
				},
			},
			{
				Query: "SELECT * FROM JSON_TABLE('[ {\"a\": 1, \"b\": [11,111]}, {\"a\": 2, \"b\": [22,222]}, {\"a\":3}]', '$[*]' COLUMNS(a INT PATH '$.a', NESTED PATH '$.b[*]' COLUMNS (b INT PATH '$'))) AS jt WHERE b IS NOT NULL;",
				Expected: []sql.UntypedSqlRow{
					{1, 11},
					{1, 111},
					{2, 22},
					{2, 222},
				},
			},
		},
	},
}
