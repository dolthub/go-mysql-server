// Copyright 2020-2021 Dolthub, Inc.
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
	"github.com/dolthub/go-mysql-server/sql/analyzer/analyzererrors"
	"github.com/dolthub/go-mysql-server/sql/planbuilder"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// AggregationScriptTests contains self-contained script tests for aggregation.
var AggregationScriptTests = []ScriptTest{
	{
		// https://github.com/dolthub/dolt/issues/9987
		Name: "GROUP BY nil pointer dereference in Dispose when Next() never called",
		SetUpScript: []string{
			"CREATE TABLE test_table (id INT PRIMARY KEY, value INT, category VARCHAR(50))",
			"INSERT INTO test_table VALUES (1, 100, 'A'), (2, 200, 'B'), (3, 300, 'A')",
		},
		Assertions: []ScriptTestAssertion{
			{
				// LIMIT 0 causes the iterator to close without ever calling Next() on groupByIter
				// This leaves all buffer elements as nil causing panic in Dispose(), or empty depending data struct
				Query:    "SELECT category, SUM(value) FROM test_table GROUP BY category LIMIT 0",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT category, COUNT(*) FROM test_table GROUP BY category LIMIT 0",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT SUM(value) FROM test_table LIMIT 0",
				Expected: []sql.Row{},
			},
		},
	},
	{
		// https://github.com/dolthub/go-mysql-server/issues/3216
		Name: "UNION ALL with BLOB columns",
		SetUpScript: []string{
			"CREATE TABLE a(name VARCHAR(255), data BLOB)",
			"CREATE TABLE b(name VARCHAR(255), data BLOB)",
			"INSERT INTO a VALUES ('a-data', UNHEX('deadbeef'))",
			"INSERT INTO b VALUES ('b-nodata', NULL)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT name, data FROM a UNION ALL SELECT name, data FROM b",
				Expected: []sql.Row{
					{"a-data", []byte{0xde, 0xad, 0xbe, 0xef}},
					{"b-nodata", nil},
				},
			},
			{
				Query: "SELECT name, HEX(data) as data_hex FROM a UNION ALL SELECT name, HEX(data) as data_hex FROM b",
				Expected: []sql.Row{
					{"a-data", "DEADBEEF"},
					{"b-nodata", nil},
				},
			},
			{
				Query: "SELECT name, data FROM a UNION ALL SELECT name, NULL FROM b",
				Expected: []sql.Row{
					{"a-data", []byte{0xde, 0xad, 0xbe, 0xef}},
					{"b-nodata", nil},
				},
			},
			{
				Query: "SELECT name, HEX(data) as data_hex FROM a UNION ALL SELECT name, HEX(NULL) as data_hex FROM b",
				Expected: []sql.Row{
					{"a-data", "DEADBEEF"},
					{"b-nodata", nil},
				},
			},
			{
				Query: "SELECT name, data FROM a UNION ALL SELECT name, UNHEX('') FROM b",
				Expected: []sql.Row{
					{"a-data", []byte{0xde, 0xad, 0xbe, 0xef}},
					{"b-nodata", []byte{}},
				},
			},
			{
				Query: "SELECT name, HEX(data) as data_hex FROM a UNION ALL SELECT name, HEX(UNHEX('')) as data_hex FROM b",
				Expected: []sql.Row{
					{"a-data", "DEADBEEF"},
					{"b-nodata", ""},
				},
			},
		},
	},
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
		// Regression test for https://github.com/dolthub/dolt/issues/9641
		Name: "bit union max1err dolt#9641",
		SetUpScript: []string{
			"CREATE TABLE report_card (id INT PRIMARY KEY, archived BIT(1))",
			"INSERT INTO report_card VALUES (1, 0)",
		},
		Assertions: []ScriptTestAssertion{
			{
				// max1err
				Query: `SELECT archived FROM report_card WHERE id = 1
					UNION ALL  
					SELECT 48 FROM report_card WHERE id = 1`,
				Expected: []sql.Row{{int64(0)}, {int64(48)}},
			},
		},
	},
	{
		// Regression test for https://github.com/dolthub/dolt/issues/9641
		Name:    "bit union comprehensive regression test dolt#9641",
		Dialect: "mysql",
		SetUpScript: []string{
			`CREATE TABLE t1 (
				id int PRIMARY KEY,
				name varchar(254),
				archived bit(1) NOT NULL DEFAULT b'0',
				archived_directly bit(1) NOT NULL DEFAULT b'0'
			)`,
			`CREATE TABLE t2 (
				id int PRIMARY KEY,
				name varchar(254),
				archived bit(1) NOT NULL DEFAULT b'0'
			)`,
			"INSERT INTO t1 VALUES (1, 'Card1', b'0', b'0')",
			"INSERT INTO t2 VALUES (2, 'Collection1', b'0')",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `SELECT *,
					COUNT(*) OVER () AS total_count
				FROM (
					SELECT
						5 AS model_ranking,
						id,
						name,
						archived,
						archived_directly
					FROM t1
					WHERE archived_directly = FALSE
					
					UNION ALL
					
					SELECT
						7 AS model_ranking,
						id,
						name,
						archived,
						NULL AS archived_directly
					FROM t2
					WHERE archived = FALSE AND id <> 1
				) AS dummy_alias`,
				Expected: []sql.Row{
					{int64(5), int64(1), "Card1", uint64(0), int64(0), int64(2)},
					{int64(7), int64(2), "Collection1", uint64(0), nil, int64(2)},
				},
			},
		},
	},
	{
		Name: "count nullable columns in a keyless table",
		SetUpScript: []string{
			"CREATE TABLE keyless_count (p BIGINT, q BIGINT, r BIGINT)",
			"INSERT INTO keyless_count VALUES (1, NULL, NULL), (2, 20, NULL), (2, 20, NULL), (3, 30, 300), (NULL, NULL, NULL)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT COUNT(*) FROM keyless_count",
				Expected: []sql.Row{{5}},
			},
			{
				Query:    "SELECT COUNT(p) FROM keyless_count",
				Expected: []sql.Row{{4}},
			},
			{
				Query:    "SELECT COUNT(q) FROM keyless_count",
				Expected: []sql.Row{{3}},
			},
			{
				Query:    "SELECT COUNT(r) FROM keyless_count",
				Expected: []sql.Row{{1}},
			},
		},
	},
	{
		Name: "set op schema merge",
		SetUpScript: []string{
			"create table `left` (i int primary key, j mediumint, k varchar(20));",
			"create table `right` (i int primary key, j bigint, k text);",
			"insert into `left` values (1,2, 'a')",
			"insert into `right` values (3,4, 'b')",

			"create table t1 (i int);",
			"insert into t1 values (1), (2), (3);",
			"create table t2 (i int);",
			"insert into t2 values (1), (3);",
			"create table t3 (j int);",
			"insert into t3 values (1), (3);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select i, j from `left` union select i, j from `right`",
				ExpectedColumns: sql.Schema{
					{
						Name: "i",
						Type: types.Int32,
					},
					{
						Name: "j",
						Type: types.Int64,
					},
				},
				Expected: []sql.Row{{1, 2}, {3, 4}},
			},
			{
				Query: "select i, k from `left` union select i, k from `right`",
				ExpectedColumns: sql.Schema{
					{
						Name: "i",
						Type: types.Int32,
					},
					{
						Name: "k",
						Type: types.LongText,
					},
				},
				Expected: []sql.Row{{1, "a"}, {3, "b"}},
			},
			{
				Query: "select i, j, k from `left` union select i, j, k from `right`",
				ExpectedColumns: sql.Schema{
					{
						Name: "i",
						Type: types.Int32,
					},
					{
						Name: "j",
						Type: types.Int64,
					},
					{
						Name: "k",
						Type: types.LongText,
					},
				},
				Expected: []sql.Row{{1, 2, "a"}, {3, 4, "b"}},
			},
			{
				Query:       "select i, k from `left` union select i, j, k from `right`",
				ExpectedErr: planbuilder.ErrSelectsDifferentLength,
			},
			{
				Query:       "select i, j, k from `left` union select i, j from `right`",
				ExpectedErr: planbuilder.ErrSelectsDifferentLength,
			},
			{
				Query: "table t1 union table t2 order by i;",
				ExpectedColumns: sql.Schema{
					{
						Name: "i",
						Type: types.Int32,
					},
				},
				Expected: []sql.Row{
					{1},
					{2},
					{3},
				},
			},
			{
				Query: "table t1 union table t2 order by i;",
				ExpectedColumns: sql.Schema{
					{
						Name: "i",
						Type: types.Int32,
					},
				},
				Expected: []sql.Row{
					{1},
					{2},
					{3},
				},
			},
			{
				Query: "table t3 union table t1 order by j;",
				ExpectedColumns: sql.Schema{
					{
						Name: "j",
						Type: types.Int32,
					},
				},
				Expected: []sql.Row{
					{1},
					{2},
					{3},
				},
			},
			{
				Query: "select j as i from t3 union table t1 order by i;",
				ExpectedColumns: sql.Schema{
					{
						Name: "i",
						Type: types.Int32,
					},
				},
				Expected: []sql.Row{
					{1},
					{2},
					{3},
				},
			},
			{
				Query: "table t1 union table t2 order by 1;",
				ExpectedColumns: sql.Schema{
					{
						Name: "i",
						Type: types.Int32,
					},
				},
				Expected: []sql.Row{
					{1},
					{2},
					{3},
				},
			},
			{
				Query: "table t1 union table t3 order by 1;",
				ExpectedColumns: sql.Schema{
					{
						Name: "i",
						Type: types.Int32,
					},
				},
				Expected: []sql.Row{
					{1},
					{2},
					{3},
				},
			},
			{
				// This looks wrong, but it actually matches MySQL
				Query: "table t1 union select i as j from t2 order by i;",
				ExpectedColumns: sql.Schema{
					{
						Name: "i",
						Type: types.Int32,
					},
				},
				Expected: []sql.Row{
					{1},
					{2},
					{3},
				},
			},
			{
				Query:       "table t1 union table t3 order by j;",
				ExpectedErr: sql.ErrColumnNotFound,
			},
			{
				Query:       "table t1 union table t2 order by t1.i;",
				ExpectedErr: planbuilder.ErrQualifiedOrderBy,
			},
			{
				Query:       "table t1 union table t2 order by t2.i;",
				ExpectedErr: planbuilder.ErrQualifiedOrderBy,
			},
			{
				Query:       "table t1 union table t3 order by t3.i;",
				ExpectedErr: planbuilder.ErrQualifiedOrderBy,
			},
			{
				Query:       "table t1 union table t3 order by t3.j;",
				ExpectedErr: planbuilder.ErrQualifiedOrderBy,
			},
			{
				Query:       "table t1 union table t3 order by t1.j;",
				ExpectedErr: planbuilder.ErrQualifiedOrderBy,
			},
			{
				Query:       "table t1 union table t2 order by count(*);",
				ExpectedErr: sql.ErrSetOpOrderByAggregation,
			},
			{
				Query:       "table t1 union all table t2 order by max(i);",
				ExpectedErr: sql.ErrSetOpOrderByAggregation,
			},
			{
				Query:       "table t1 union table t2 order by row_number() over (order by i);",
				ExpectedErr: sql.ErrSetOpOrderByAggregation,
			},
			{
				Query:       "table t1 union table t2 order by sum(i) over ();",
				ExpectedErr: sql.ErrSetOpOrderByAggregation,
			},
			{
				Query:       "table t1 union table t2 order by count(*) + 1;",
				ExpectedErr: sql.ErrSetOpOrderByAggregation,
			},
			{
				Query:          "table t1 union table t2 order by i, count(*);",
				ExpectedErrStr: "Expression #2 of ORDER BY contains aggregate function and applies to a UNION, EXCEPT or INTERSECT",
			},
			{
				Query: "select i, sum(i) over () as s from t1 union all select i, sum(i) over () as s from t2 order by s, i;",
				Expected: []sql.Row{
					{1, 4.0},
					{3, 4.0},
					{1, 6.0},
					{2, 6.0},
					{3, 6.0},
				},
			},
			{
				Query: "select i, sum(i) over () as s from t1 union all select i, sum(i) over () as s from t2 order by 2, 1;",
				Expected: []sql.Row{
					{1, 4.0},
					{3, 4.0},
					{1, 6.0},
					{2, 6.0},
					{3, 6.0},
				},
			},
		},
	},
	{
		Name: "intersection and except tests",
		SetUpScript: []string{
			"create table a (m int, n int);",
			"insert into a values (1,2), (2,3), (3,4);",
			"create table b (m int, n int);",
			"insert into b values (1,2), (1,3), (3,4);",
			"create table c (m int, n int);",
			"insert into c values (1,3), (1,3), (3,4);",
			"create table t1 (i int);",
			"insert into t1 values (1), (2), (3);",
			"create table t2 (i float);",
			"insert into t2 values (1.0), (1.99), (3.0);",
			"create table l (i int);",
			"insert into l values (1), (1), (1);",
			"create table r (i int);",
			"insert into r values (1);",
			"create table x (i int);",
			"insert into x values (1), (2), (3);",
			"create table y (i bigint);",
			"insert into y values (1), (3);",
		},
		Assertions: []ScriptTestAssertion{
			// Intersect tests
			{
				Query: "table a intersect table b order by m, n;",
				Expected: []sql.Row{
					{1, 2},
					{3, 4},
				},
			},
			{
				Query: "table a intersect table c order by m, n;",
				Expected: []sql.Row{
					{3, 4},
				},
			},
			{
				Query: "table c intersect distinct table c order by m, n;",
				Expected: []sql.Row{
					{1, 3},
					{3, 4},
				},
			},
			{
				Query: "table c intersect all table c order by m, n;",
				Expected: []sql.Row{
					{1, 3},
					{1, 3},
					{3, 4},
				},
			},
			{
				Query: "table a intersect table b intersect table c;",
				Expected: []sql.Row{
					{3, 4},
				},
			},
			{
				Query: "(table b order by m limit 1 offset 1) intersect (table c order by m limit 1);",
				Expected: []sql.Row{
					{1, 3},
				},
			},
			{
				Query: "table x intersect table y order by i;",
				Expected: []sql.Row{
					{1},
					{3},
				},
			},
			{
				Query: "table x intersect table y order by 1;",
				Expected: []sql.Row{
					{1},
					{3},
				},
			},
			{
				Query:       "table x intersect table y order by max(i);",
				ExpectedErr: sql.ErrSetOpOrderByAggregation,
			},
			{
				Query:       "table x intersect table y order by row_number() over (order by i);",
				ExpectedErr: sql.ErrSetOpOrderByAggregation,
			},
			{
				// Resulting type is string for some reason
				Skip:  true,
				Query: "table t1 intersect table t2;",
				Expected: []sql.Row{
					{1},
					{3},
				},
			},

			// Except tests
			{
				Query: "table a except table b order by m, n;",
				Expected: []sql.Row{
					{2, 3},
				},
			},
			{
				Query: "table a except table c order by m, n;",
				Expected: []sql.Row{
					{1, 2},
					{2, 3},
				},
			},
			{
				Query: "table b except table c order by m, n;",
				Expected: []sql.Row{
					{1, 2},
				},
			},
			{
				Query: "table c except distinct table a order by m, n;",
				Expected: []sql.Row{
					{1, 3},
				},
			},
			{
				Query: "table c except all table a order by m, n;",
				Expected: []sql.Row{
					{1, 3},
					{1, 3},
				},
			},
			{
				Query: "(table a order by m limit 1 offset 1) except (table c order by m limit 1);",
				Expected: []sql.Row{
					{2, 3},
				},
			},
			{
				Query: "table a except table b except table c;",
				Expected: []sql.Row{
					{2, 3},
				},
			},
			{
				Query: "table x except table y order by i;",
				Expected: []sql.Row{
					{2},
				},
			},
			{
				Query:       "table x except table y order by count(*);",
				ExpectedErr: sql.ErrSetOpOrderByAggregation,
			},
			{
				Query:    "table l except table r;",
				Expected: []sql.Row{},
			},
			{
				Query:    "table l except distinct table r;",
				Expected: []sql.Row{},
			},
			{
				Query: "table l except all table r;",
				Expected: []sql.Row{
					{1},
					{1},
				},
			},

			// Multiple set operation tests
			{
				Query: "table a except table b intersect table c order by m;",
				Expected: []sql.Row{
					{1, 2},
					{2, 3},
				},
			},
			{
				Query: "table a intersect table b except table c order by m;",
				Expected: []sql.Row{
					{1, 2},
				},
			},
			{
				Query: "table a union table a intersect table b except table c order by m;",
				Expected: []sql.Row{
					{1, 2},
					{2, 3},
				},
			},

			// CTE tests
			{
				Query: "with cte as (table a union table a intersect table b except table c order by m) select * from cte",
				Expected: []sql.Row{
					{1, 2},
					{2, 3},
				},
			},
			{
				Query: "with recursive cte(x) as (select 1) select * from cte",
				Expected: []sql.Row{
					{1},
				},
			},
			{
				Query:       "with recursive cte (x,y) as (select 1, 2, 3 union select x, y from cte where x < 5) select * from cte;",
				ExpectedErr: planbuilder.ErrSelectsDifferentLength,
			},
			{
				Query: "with recursive cte (x,y) as (select 1, 1 intersect select 1, 1 union select x + 1, y + 2 from cte where x < 5) select * from cte;",
				Expected: []sql.Row{
					{1, 1},
					{2, 3},
					{3, 5},
					{4, 7},
					{5, 9},
				},
			},
			{
				Query: "with recursive cte(x) as (select 1 union select 2 union select x in (select i from t1) from cte) select x, row_number() over (order by x) as rn from cte order by x;",
				Expected: []sql.Row{
					{1, 1},
					{2, 2},
				},
				// PostgreSQL rejects UNION between integer and boolean instead of coercing boolean to integer.
				Dialect: "mysql",
			},
			{
				Query:    "with recursive cte(x) as (select cast(1 as signed) union select cast(x as unsigned) from cte) select count(*) from cte;",
				Expected: []sql.Row{{1}},
				Dialect:  "mysql",
			},
			{
				Query:    "with recursive cte(x) as (select cast('1' as char(3)) union select cast(x as signed) from cte) select count(*) from cte;",
				Expected: []sql.Row{{1}},
				Dialect:  "mysql",
			},
			{
				Query:    "with recursive cte(d, f, n) as (select cast('2024-01-02' as date), cast(1.5 as double), cast(2.50 as decimal(5,2)) union select cast(d as char), cast(f as decimal(5,2)), cast(n as double) from cte) select count(*) from cte;",
				Expected: []sql.Row{{1}},
				Dialect:  "mysql",
			},
			{
				Query: "with recursive cte(x) as (select cast(null as signed) union select ifnull(x, 0) from cte) select x from cte order by x;",
				Expected: []sql.Row{
					{nil},
					{0},
				},
				Dialect: "mysql",
			},
			{
				Query:    "with recursive cte(x) as (select cast('a' as binary(1)) union select cast(x as char) from cte) select count(*), hex(min(x)) from cte;",
				Expected: []sql.Row{{1, "61"}},
				Dialect:  "mysql",
			},
			{
				Query: "with recursive cte(x, n) as (select cast(1 as signed), 1 union all select cast(x as unsigned), n + 1 from cte where n < 3) select x, n from cte order by n;",
				Expected: []sql.Row{
					{1, 1},
					{1, 2},
					{1, 3},
				},
				Dialect: "mysql",
			},
			{
				Query: "WITH RECURSIVE\n" +
					"    rt (foo) AS (\n" +
					"        SELECT 1 as foo\n" +
					"        UNION ALL\n" +
					"        SELECT foo + 1 as foo FROM rt WHERE foo < 5\n" +
					"    ),\n" +
					"        ladder (depth, foo) AS (\n" +
					"        SELECT 1 as depth, NULL as foo from rt\n" +
					"        UNION ALL\n" +
					"        SELECT ladder.depth + 1 as depth, rt.foo\n" +
					"        FROM ladder JOIN rt WHERE ladder.foo = rt.foo\n" +
					"    )\n" +
					"SELECT * FROM ladder;",
				Expected: []sql.Row{
					{1, nil},
					{1, nil},
					{1, nil},
					{1, nil},
					{1, nil},
				},
			},
			{
				Query:       "with recursive cte (x,y) as (select 1, 1 intersect select 1, 1 intersect select x + 1, y + 2 from cte where x < 5) select * from cte;",
				ExpectedErr: sql.ErrRecursiveCTEMissingUnion,
			},
			{
				Query:       "with recursive cte (x,y) as (select 1, 1 union select 1, 1 intersect select x + 1, y + 2 from cte where x < 5) select * from cte;",
				ExpectedErr: sql.ErrRecursiveCTENotUnion,
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
		Name:    "Group Concat Queries",
		Dialect: "mysql",
		SetUpScript: []string{
			"CREATE TABLE x (pk int)",
			"INSERT INTO x VALUES (1),(2),(3),(4),(NULL)",

			"create table t (o_id int, attribute longtext, value longtext)",
			"INSERT INTO t VALUES (2, 'color', 'red'), (2, 'fabric', 'silk')",
			"INSERT INTO t VALUES (3, 'color', 'green'), (3, 'shape', 'square')",

			"create table nulls(pk int)",
			"INSERT INTO nulls VALUES (NULL)",

			"CREATE TABLE group_concat_students (id INT PRIMARY KEY, first_name VARCHAR(20), last_name VARCHAR(20))",
			"INSERT INTO group_concat_students VALUES (1, 'Alice', 'Smith'), (2, 'Bob', 'Jones'), (3, 'Alice', 'Brown'), (4, 'Eve', NULL), (5, '', 'White')",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT GROUP_CONCAT(first_name, ' ', last_name ORDER BY id SEPARATOR '; ') FROM group_concat_students",
				Expected: []sql.Row{{"Alice Smith; Bob Jones; Alice Brown;  White"}},
			},
			{
				Query:    "SELECT GROUP_CONCAT(DISTINCT first_name, ' ', last_name ORDER BY id SEPARATOR '; ') FROM group_concat_students",
				Expected: []sql.Row{{"Alice Smith; Bob Jones; Alice Brown;  White"}},
			},
			{
				Query: `SELECT FIRST_VALUE(x) OVER () FROM (
					SELECT GROUP_CONCAT(v ORDER BY id SEPARATOR '|') AS x
					FROM (SELECT 1 AS id, '' AS v UNION ALL SELECT 2, 'a' UNION ALL SELECT 3, '') t
				) q`,
				Expected: []sql.Row{{"|a|"}},
			},
			{
				Query:    `SELECT group_concat(pk ORDER BY pk) FROM x;`,
				Expected: []sql.Row{{"1,2,3,4"}},
			},
			{
				Query:    `SELECT group_concat(DISTINCT pk ORDER BY pk) FROM x;`,
				Expected: []sql.Row{{"1,2,3,4"}},
			},
			{
				Query:    `SELECT group_concat(DISTINCT pk ORDER BY pk SEPARATOR '-') FROM x;`,
				Expected: []sql.Row{{"1-2-3-4"}},
			},
			{
				Query:    "SELECT group_concat(`attribute` ORDER BY `attribute`) FROM t group by o_id order by o_id asc",
				Expected: []sql.Row{{"color,fabric"}, {"color,shape"}},
			},
			{
				Query:    "SELECT group_concat(DISTINCT `attribute` ORDER BY value DESC SEPARATOR ';') FROM t group by o_id order by o_id asc",
				Expected: []sql.Row{{"fabric;color"}, {"shape;color"}},
			},
			{
				Query:    "SELECT group_concat(DISTINCT `attribute` ORDER BY `attribute`) FROM t",
				Expected: []sql.Row{{"color,fabric,shape"}},
			},
			{
				Query:    "SELECT group_concat(`attribute` ORDER BY `attribute`) FROM t",
				Expected: []sql.Row{{"color,color,fabric,shape"}},
			},
			{
				Query:    `SELECT group_concat((SELECT 2)) FROM x;`,
				Expected: []sql.Row{{"2,2,2,2,2"}},
			},
			{
				Query:    `SELECT group_concat(DISTINCT (SELECT 2)) FROM x;`,
				Expected: []sql.Row{{"2"}},
			},
			{
				Query:    "SELECT group_concat(DISTINCT `attribute` ORDER BY `attribute` ASC) FROM t",
				Expected: []sql.Row{{"color,fabric,shape"}},
			},
			{
				Query:    "SELECT group_concat(DISTINCT `attribute` ORDER BY `attribute` DESC) FROM t",
				Expected: []sql.Row{{"shape,fabric,color"}},
			},
			{
				Query:    `SELECT group_concat(pk) FROM nulls`,
				Expected: []sql.Row{{nil}},
			},
			{
				Query:       `SELECT group_concat((SELECT * FROM t LIMIT 1)) from t`,
				ExpectedErr: sql.ErrInvalidOperandColumns,
			},
			{
				Query:       `SELECT group_concat((SELECT * FROM x)) from t`,
				ExpectedErr: sql.ErrExpectedSingleRow,
			},
			{
				Query:    "SELECT group_concat(`attribute` order by attribute) FROM t where o_id=2 order by attribute",
				Expected: []sql.Row{{"color,fabric"}},
			},
			{
				Query:    "SELECT group_concat(`attribute` order by attribute desc) FROM t where o_id=2 order by attribute",
				Expected: []sql.Row{{"fabric,color"}},
			},
			{
				Query:    "SELECT group_concat(DISTINCT `attribute` ORDER BY value DESC SEPARATOR ';') FROM t group by o_id order by o_id asc",
				Expected: []sql.Row{{"fabric;color"}, {"shape;color"}},
			},
			{
				Query:    "SELECT group_concat(o_id order by o_id) FROM t WHERE `attribute`='color' order by o_id",
				Expected: []sql.Row{{"2,3"}},
			},
			{
				Query:    "SELECT group_concat(attribute order by attribute separator '') FROM t WHERE o_id=2 ORDER BY attribute",
				Expected: []sql.Row{{"colorfabric"}},
			},
		},
	},
	{
		Name: "Run through some complex queries with DISTINCT and aggregates",
		SetUpScript: []string{
			"CREATE TABLE tab1(col0 INTEGER, col1 INTEGER, col2 INTEGER)",
			"CREATE TABLE tab2(col0 INTEGER, col1 INTEGER, col2 INTEGER)",
			"INSERT INTO tab1 VALUES(51,14,96)",
			"INSERT INTO tab1 VALUES(85,5,59)",
			"INSERT INTO tab1 VALUES(91,47,68)",
			"INSERT INTO tab2 VALUES(64,77,40)",
			"INSERT INTO tab2 VALUES(75,67,58)",
			"INSERT INTO tab2 VALUES(46,51,23)",
			"CREATE TABLE mytable (pk int, v1 int)",
			"INSERT INTO mytable VALUES(1,1)",
			"INSERT INTO mytable VALUES(1,1)",
			"INSERT INTO mytable VALUES(2,2)",
			"INSERT INTO mytable VALUES(1,2)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT - SUM( DISTINCT - - 71 ) AS col2 FROM tab2 cor0",
				Expected: []sql.Row{{float64(-71)}},
			},
			{
				Query:    "SELECT - SUM ( DISTINCT - - 71 ) AS col2 FROM tab2 cor0",
				Expected: []sql.Row{{float64(-71)}},
			},
			{
				Query:    "SELECT + MAX( DISTINCT ( - col0 ) ) FROM tab1 AS cor0",
				Expected: []sql.Row{{-51}},
			},
			{
				Query:    "SELECT SUM( DISTINCT + col1 ) * - 22 - - ( - COUNT( * ) ) col0 FROM tab1 AS cor0",
				Expected: []sql.Row{{float64(-1455)}},
			},
			{
				Query:    "SELECT MIN (DISTINCT col1) from tab1 GROUP BY col0 ORDER BY col0",
				Expected: []sql.Row{{14}, {5}, {47}},
			},
			{
				Query:    "SELECT SUM (DISTINCT col1) from tab1 GROUP BY col0 ORDER BY col0",
				Expected: []sql.Row{{float64(14)}, {float64(5)}, {float64(47)}},
			},
			{
				Query:    "SELECT pk, SUM(DISTINCT v1), MAX(v1) FROM mytable GROUP BY pk ORDER BY pk",
				Expected: []sql.Row{{int64(1), float64(3), int64(2)}, {int64(2), float64(2), int64(2)}},
			},
			{
				Query:    "SELECT pk, MIN(DISTINCT v1), MAX(DISTINCT v1) FROM mytable GROUP BY pk ORDER BY pk",
				Expected: []sql.Row{{int64(1), int64(1), int64(2)}, {int64(2), int64(2), int64(2)}},
			},
			{
				Query:    "SELECT SUM(DISTINCT pk * v1) from mytable",
				Expected: []sql.Row{{float64(7)}},
			},
			{
				Query:    "SELECT SUM(DISTINCT POWER(v1, 2)) FROM mytable",
				Expected: []sql.Row{{float64(5)}},
			},
			{
				Query:    "SELECT + + 97 FROM tab1 GROUP BY tab1.col1",
				Expected: []sql.Row{{97}, {97}, {97}},
			},
			{
				Query:    "SELECT rand(10) FROM tab1 GROUP BY tab1.col1",
				Expected: []sql.Row{{0.5660920659323543}, {0.5660920659323543}, {0.5660920659323543}},
			},
			{
				Query:    "SELECT ALL - cor0.col0 * + cor0.col0 AS col2 FROM tab1 AS cor0 GROUP BY cor0.col0",
				Expected: []sql.Row{{-2601}, {-7225}, {-8281}},
			},
			{
				Query:    "SELECT cor0.col0 * cor0.col0 + cor0.col0 AS col2 FROM tab1 AS cor0 GROUP BY cor0.col0 order by 1",
				Expected: []sql.Row{{2652}, {7310}, {8372}},
			},
			{
				Query:    "SELECT - floor(cor0.col0) * ceil(cor0.col0) AS col2 FROM tab1 AS cor0 GROUP BY cor0.col0",
				Expected: []sql.Row{{-2601}, {-7225}, {-8281}},
			},
			{
				Query:    "SELECT col0 FROM tab1 AS cor0 GROUP BY cor0.col0",
				Expected: []sql.Row{{51}, {85}, {91}},
			},
			{
				Query:    "SELECT - cor0.col0 FROM tab1 AS cor0 GROUP BY cor0.col0",
				Expected: []sql.Row{{-51}, {-85}, {-91}},
			},
			{
				Query:    "SELECT col0 BETWEEN 2 and 4 from tab1 group by col0",
				Expected: []sql.Row{{false}, {false}, {false}},
			},
			{
				Query:       "SELECT col0, col1 FROM tab1 GROUP by col0;",
				ExpectedErr: analyzererrors.ErrValidationGroupBy,
			},
			{
				Query:       "SELECT col0, floor(col1) FROM tab1 GROUP by col0;",
				ExpectedErr: analyzererrors.ErrValidationGroupBy,
			},
			{
				Query:       "SELECT floor(cor0.col1) * ceil(cor0.col0) AS col2 FROM tab1 AS cor0 GROUP BY cor0.col0",
				ExpectedErr: analyzererrors.ErrValidationGroupBy,
			},
		},
	},
	{
		Name: "having clause without groupby clause, all rows implicitly form a single aggregate group",
		SetUpScript: []string{
			"create table numbers (val int);",
			"insert into numbers values (1), (2), (3);",
			"insert into numbers values (2), (4);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select val from numbers;",
				Expected: []sql.Row{{1}, {2}, {3}, {2}, {4}},
			},
			{
				Query:    "select val as a from numbers having a = val;",
				Expected: []sql.Row{{1}, {2}, {3}, {2}, {4}},
			},
			{
				Query:    "select val as a from numbers group by val having a = val;",
				Expected: []sql.Row{{1}, {2}, {3}, {4}},
			},
			{
				Query:    "select val as a from numbers as t1 group by t1.val having a = t1.val;",
				Expected: []sql.Row{{1}, {2}, {3}, {4}},
			},
			{
				Query:    "select t1.val as a from numbers as t1 group by 1 having a = t1.val;",
				Expected: []sql.Row{{1}, {2}, {3}, {4}},
			},
			{
				Query:    "select t1.val as a from numbers as t1 having a = t1.val;",
				Expected: []sql.Row{{1}, {2}, {3}, {2}, {4}},
			},
			{
				Query:    "select count(*) from numbers having count(*) = 5;",
				Expected: []sql.Row{{5}},
			},
			{
				// MySQL returns `Unknown column 'val' in 'having clause'` error for this query,
				// but GMS builds GroupBy for any aggregate function.
				Skip:  true,
				Query: "select count(*) from numbers having count(*) > val;",
				// ExpectedErrStr:   "found HAVING clause with no GROUP BY", // not the exact error we want
			},
			{
				Query:    "select count(*) from numbers group by val having count(*) < val;",
				Expected: []sql.Row{{1}, {1}},
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
		Name:    "std, stdev, stddev_pop, variance, var_pop, var_samp tests",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table t (i int);",
			"create table tt (i int, j int);",
			"insert into tt values (0, 1), (0, 2), (0, 3);",
			"insert into tt values (1, 123), (1, 456), (1, 789);",
			"create table td (v decimal(10,2));",
			"insert into td values (1.00), (2.00), (3.00);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select std(i), stddev(i), stddev_pop(i), stddev_samp(i) from t;",
				Expected: []sql.Row{
					{nil, nil, nil, nil},
				},
			},
			{
				Query: "select variance(i), var_pop(i), var_samp(i) from t;",
				Expected: []sql.Row{
					{nil, nil, nil},
				},
			},
			{
				Query: "insert into t values (1);",
				Expected: []sql.Row{
					{types.NewOkResult(1)},
				},
			},
			{
				Query: "select std(i), stddev(i), stddev_pop(i), stddev_samp(i) from t;",
				Expected: []sql.Row{
					{0.0, 0.0, 0.0, nil},
				},
			},
			{
				Query: "select variance(i), var_pop(i), var_samp(i) from t;",
				Expected: []sql.Row{
					{0.0, 0.0, nil},
				},
			},
			{
				Query: "insert into t values (2);",
				Expected: []sql.Row{
					{types.NewOkResult(1)},
				},
			},
			{
				Query: "select std(i), stddev(i), stddev_pop(i), stddev_samp(i) from t;",
				Expected: []sql.Row{
					{0.5, 0.5, 0.5, 0.7071067811865476},
				},
			},
			{
				Query: "select variance(i), var_pop(i), var_samp(i) from t;",
				Expected: []sql.Row{
					{0.25, 0.25, 0.5},
				},
			},
			{
				Query: "insert into t values (3);",
				Expected: []sql.Row{
					{types.NewOkResult(1)},
				},
			},
			{
				Query: "select std(i), stddev(i), stddev_pop(i), stddev_samp(i) from t;",
				Expected: []sql.Row{
					{0.816496580927726, 0.816496580927726, 0.816496580927726, 1.0},
				},
			},
			{
				Query: "select variance(i), var_pop(i), var_samp(i) from t;",
				Expected: []sql.Row{
					{0.6666666666666666, 0.6666666666666666, 1.0},
				},
			},
			{
				Query: "insert into t values (null), (null);",
				Expected: []sql.Row{
					{types.NewOkResult(2)},
				},
			},
			{
				Query: "select std(i), stddev(i), stddev_pop(i), stddev_samp(i) from t;",
				Expected: []sql.Row{
					{0.816496580927726, 0.816496580927726, 0.816496580927726, 1.0},
				},
			},
			{
				Query: "select variance(i), var_pop(i), var_samp(i) from t;",
				Expected: []sql.Row{
					{0.6666666666666666, 0.6666666666666666, 1.0},
				},
			},
			{
				Query: "select i, std(j), stddev_samp(j) from tt group by i;",
				Expected: []sql.Row{
					{0, 0.816496580927726, 1.0},
					{1, 271.89336144893275, 333.0},
				},
			},
			{
				Query: "select i, variance(i), var_samp(i) from tt group by i;",
				Expected: []sql.Row{
					{0, 0.0, 0.0},
					{1, 0.0, 0.0},
				},
			},
			{
				Query: "select std(i) over(), std(j) over(), stddev_samp(j) over() from tt order by i;",
				Expected: []sql.Row{
					{0.5, 297.47660972475353, 325.86929895281634},
					{0.5, 297.47660972475353, 325.86929895281634},
					{0.5, 297.47660972475353, 325.86929895281634},
					{0.5, 297.47660972475353, 325.86929895281634},
					{0.5, 297.47660972475353, 325.86929895281634},
					{0.5, 297.47660972475353, 325.86929895281634},
				},
			},
			{
				Query: "select i, std(j) over(partition by i), stddev_samp(j) over(partition by i) from tt order by i;",
				Expected: []sql.Row{
					{0, 0.816496580927726, 1.0},
					{0, 0.816496580927726, 1.0},
					{0, 0.816496580927726, 1.0},
					{1, 271.89336144893275, 333.0},
					{1, 271.89336144893275, 333.0},
					{1, 271.89336144893275, 333.0},
				},
			},
			{
				Query: "select i, variance(i) over(), var_samp(i) over() from tt order by i;",
				Expected: []sql.Row{
					{0, 0.25, 0.3},
					{0, 0.25, 0.3},
					{0, 0.25, 0.3},
					{1, 0.25, 0.3},
					{1, 0.25, 0.3},
					{1, 0.25, 0.3},
				},
			},
			{
				Query: "select i, variance(j) over(partition by i), var_samp(i) over(partition by i) from tt order by i;",
				Expected: []sql.Row{
					{0, 0.6666666666666666, 0.0},
					{0, 0.6666666666666666, 0.0},
					{0, 0.6666666666666666, 0.0},
					{1, 73926.0, 0.0},
					{1, 73926.0, 0.0},
					{1, 73926.0, 0.0},
				},
			},
			{
				Query: "insert into tt values (null, null);",
				Expected: []sql.Row{
					{types.NewOkResult(1)},
				},
			},
			{
				Query: "select i, std(i) over(), std(j) over(), stddev_samp(j) over() from tt order by i;",
				Expected: []sql.Row{
					{nil, 0.5, 297.47660972475353, 325.86929895281634},
					{0, 0.5, 297.47660972475353, 325.86929895281634},
					{0, 0.5, 297.47660972475353, 325.86929895281634},
					{0, 0.5, 297.47660972475353, 325.86929895281634},
					{1, 0.5, 297.47660972475353, 325.86929895281634},
					{1, 0.5, 297.47660972475353, 325.86929895281634},
					{1, 0.5, 297.47660972475353, 325.86929895281634},
				},
			},
			{
				Query: "select i, std(j) over(partition by i), stddev_samp(j) over(partition by i) from tt order by i;",
				Expected: []sql.Row{
					{nil, nil, nil},
					{0, 0.816496580927726, 1.0},
					{0, 0.816496580927726, 1.0},
					{0, 0.816496580927726, 1.0},
					{1, 271.89336144893275, 333.0},
					{1, 271.89336144893275, 333.0},
					{1, 271.89336144893275, 333.0},
				},
			},
			{
				Query: "select i, variance(i) over(), var_samp(i) over() from tt order by i;",
				Expected: []sql.Row{
					{nil, 0.25, 0.3},
					{0, 0.25, 0.3},
					{0, 0.25, 0.3},
					{0, 0.25, 0.3},
					{1, 0.25, 0.3},
					{1, 0.25, 0.3},
					{1, 0.25, 0.3},
				},
			},
			{
				Query: "select i, variance(j) over(partition by i), var_samp(i) over(partition by i) from tt order by i;",
				Expected: []sql.Row{
					{nil, nil, nil},
					{0, 0.6666666666666666, 0.0},
					{0, 0.6666666666666666, 0.0},
					{0, 0.6666666666666666, 0.0},
					{1, 73926.0, 0.0},
					{1, 73926.0, 0.0},
					{1, 73926.0, 0.0},
				},
			},
			{
				Query: "select i, stddev_pop(j) over w, stddev_samp(j) over w, variance(j) over w, var_samp(i) over w from tt window w as (partition by i) order by i;",
				Expected: []sql.Row{
					{nil, nil, nil, nil, nil},
					{0, 0.816496580927726, 1.0, 0.6666666666666666, 0.0},
					{0, 0.816496580927726, 1.0, 0.6666666666666666, 0.0},
					{0, 0.816496580927726, 1.0, 0.6666666666666666, 0.0},
					{1, 271.89336144893275, 333.0, 73926.0, 0.0},
					{1, 271.89336144893275, 333.0, 73926.0, 0.0},
					{1, 271.89336144893275, 333.0, 73926.0, 0.0},
				},
			},
			{
				Query: "select std(v), stddev(v), stddev_pop(v), stddev_samp(v), variance(v), var_pop(v), var_samp(v) from td;",
				Expected: []sql.Row{
					{0.816496580927726, 0.816496580927726, 0.816496580927726, 1.0, 0.6666666666666666, 0.6666666666666666, 1.0},
				},
			},
		},
	},
	{
		// https://github.com/dolthub/dolt/issues/9628
		Name: "UNION column mapping bug dolt#9628",
		SetUpScript: []string{
			"CREATE TABLE report_card (id INT PRIMARY KEY, name VARCHAR(255), entity_id INT, dashboard_id INT, description TEXT, display TEXT, collection_preview TEXT, dataset_query TEXT, collection_id INT, archived_directly BOOLEAN DEFAULT FALSE, collection_position INT, database_id INT, archived BOOLEAN DEFAULT FALSE, last_used_at DATETIME, table_id INT, query_type VARCHAR(50), type VARCHAR(50))",
			"CREATE TABLE collection (id INT PRIMARY KEY, name VARCHAR(255), entity_id INT, location VARCHAR(500), authority_level VARCHAR(50), personal_owner_id INT, archived_directly BOOLEAN DEFAULT FALSE, type VARCHAR(50), archived BOOLEAN DEFAULT FALSE, namespace VARCHAR(100))",
			"CREATE TABLE report_dashboard (id INT PRIMARY KEY, name VARCHAR(255), entity_id INT, description TEXT, collection_id INT, archived_directly BOOLEAN DEFAULT FALSE, collection_position INT, archived BOOLEAN DEFAULT FALSE, last_viewed_at DATETIME)",
			"INSERT INTO report_card (id, name, entity_id, dashboard_id, type, archived_directly, archived, collection_position) VALUES (2, 'Card 2', 1002, NULL, 'question', FALSE, FALSE, NULL), (3, 'Card 3', 1003, NULL, 'question', FALSE, FALSE, NULL)",
			"INSERT INTO collection (id, name, entity_id, location, type, archived, personal_owner_id, namespace) VALUES (2, 'Collection 2', 4002, '/test2/', 'normal', FALSE, NULL, NULL), (3, 'Collection 3', 4003, '/test3/', 'normal', FALSE, NULL, NULL)",
			"INSERT INTO report_dashboard (id, name, entity_id, description, collection_id, archived_directly, archived, collection_position) VALUES (1, 'Dashboard 1', 3001, 'Test dashboard description', NULL, FALSE, FALSE, NULL), (2, 'Dashboard 2', 3002, 'Test dashboard 2 description', NULL, FALSE, FALSE, NULL)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `WITH visible_collection_ids AS (SELECT id FROM collection AS c WHERE (1 <> c.id)) 
				SELECT 5 AS model_ranking, c.id, c.name, c.description, c.entity_id, c.display, c.collection_preview, c.dataset_query, c.collection_id, 'card' AS model 
				FROM report_card AS c WHERE (c.dashboard_id IS NULL) AND (archived = FALSE) AND (c.type = 'question') 
				UNION 
				SELECT 7 AS model_ranking, id, name, name AS description, entity_id, NULL AS display, NULL AS collection_preview, NULL AS dataset_query, id AS collection_id, 'collection' AS model 
				FROM collection AS col WHERE (archived = FALSE) AND (id <> 1) AND (personal_owner_id IS NULL) AND (namespace IS NULL) 
				UNION 
				SELECT 1 AS model_ranking, d.id, d.name, d.description, d.entity_id, NULL AS display, NULL AS collection_preview, NULL AS dataset_query, NULL AS collection_id, 'dashboard' AS model 
				FROM report_dashboard AS d WHERE (archived = FALSE)`,
				Expected: []sql.Row{
					{5, 2, "Card 2", nil, 1002, nil, nil, nil, nil, "card"},
					{5, 3, "Card 3", nil, 1003, nil, nil, nil, nil, "card"},
					{7, 2, "Collection 2", "Collection 2", 4002, nil, nil, nil, 2, "collection"},
					{7, 3, "Collection 3", "Collection 3", 4003, nil, nil, nil, 3, "collection"},
					{1, 1, "Dashboard 1", "Test dashboard description", 3001, nil, nil, nil, nil, "dashboard"},
					{1, 2, "Dashboard 2", "Test dashboard 2 description", 3002, nil, nil, nil, nil, "dashboard"},
				},
			},
		},
	},
	{
		Name:    "aggregate function with match",
		Dialect: "mysql",
		SetUpScript: []string{
			"CREATE TABLE test (pk INT PRIMARY KEY, doc TEXT, FULLTEXT idx (doc));",
			"INSERT INTO test VALUES (2, 'g hhhh aaaab ooooo aaaa'), (1, 'bbbb ff cccc ddd eee'), (4, 'AAAA aaaa aaaac aaaa Aaaa aaaa'), (3, 'aaaA ff j kkkk llllllll');",
		},
		Assertions: []ScriptTestAssertion{
			{
				// https://github.com/dolthub/dolt/issues/9761
				Skip:        true,
				Query:       "SELECT pk, count(pk), MATCH(doc) AGAINST('aaaa') AS relevancy FROM test ORDER BY relevancy DESC;",
				ExpectedErr: sql.ErrNonAggregatedColumnWithoutGroupBy,
			},
			{
				Query:    "SET SESSION sql_mode = REPLACE(@@SESSION.sql_mode, 'ONLY_FULL_GROUP_BY', '');",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},

			{
				Query:    "SELECT pk, count(pk), MATCH(doc) AGAINST('aaaa') AS relevancy FROM test ORDER BY relevancy DESC;",
				Expected: []sql.Row{{1, 4, float64(0)}},
			},
		},
	},
	{
		Name:    "nonaggregated column in aggregated query",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table emptytable(i mediumint primary key, s varchar(20))",
			"create table mytable(i bigint primary key, s varchar(20))",
			"insert into mytable values (1, 'first'), (2, 'second'), (3, 'third');",
			"create table two_pk (pk1 tinyint, pk2 tinyint, c1 tinyint NOT NULL, primary key (pk1, pk2))",
			"insert into two_pk values (0,0,0), (0,1,10), (1,0,20), (1,1,30)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "SELECT count(*), i, concat(i, i), 123, 'abc', concat('abc', 'def') FROM emptytable;",
				ExpectedErr: sql.ErrNonAggregatedColumnWithoutGroupBy,
			},
			{
				Query:       "SELECT count(*), i, concat(i, i), 123, 'abc', concat('abc', 'def') FROM mytable where false;",
				ExpectedErr: sql.ErrNonAggregatedColumnWithoutGroupBy,
			},
			{
				Query:       "SELECT pk1, SUM(c1) FROM two_pk",
				ExpectedErr: sql.ErrNonAggregatedColumnWithoutGroupBy,
			},
			{
				Query:    "SET SESSION sql_mode = REPLACE(@@SESSION.sql_mode, 'ONLY_FULL_GROUP_BY', '');",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query: "SELECT count(*), i, concat(i, i), 123, 'abc', concat('abc', 'def') FROM emptytable;",
				Expected: []sql.Row{
					{0, nil, nil, 123, "abc", "abcdef"},
				},
			},
			{
				Query: "SELECT count(*), i, concat(i, i), 123, 'abc', concat('abc', 'def') FROM mytable where false;",
				Expected: []sql.Row{
					{0, nil, nil, 123, "abc", "abcdef"},
				},
			},
			{
				Query:    "SELECT pk1, SUM(c1) FROM two_pk",
				Expected: []sql.Row{{0, 60.0}},
			},
		},
	},
	{
		// https://github.com/dolthub/dolt/issues/10064
		Name: "Hoist out of scope filters for left and right sides of union",
		SetUpScript: []string{
			"create table t1(c0 int, c1 varchar(500))",
			"insert into t1(c1, c0) values ('-1',1),('-2',2)",
		},
		Assertions: []ScriptTestAssertion{
			{
				// Ensures that out of scope filters are hoisted
				Query: "SELECT * FROM t1 WHERE NOT EXISTS (SELECT 1 FROM (SELECT NULL WHERE FALSE) AS sub0 WHERE (t1.c0)*(t1.c0)) union all SELECT * FROM t1 WHERE NOT EXISTS (SELECT 1 FROM (SELECT NULL WHERE FALSE) AS sub0 WHERE (t1.c0)*(t1.c0));",
				Expected: []sql.Row{
					{1, "-1"},
					{2, "-2"},
					{1, "-1"},
					{2, "-2"},
				},
			},
			{
				// Ensures that antijoin iterator works correctly
				Query: "SELECT * FROM t1 WHERE NOT EXISTS (SELECT 1 FROM (SELECT NULL WHERE FALSE) AS sub0) union all SELECT * FROM t1 WHERE NOT EXISTS (SELECT 1 FROM (SELECT NULL WHERE FALSE) AS sub0);",
				Expected: []sql.Row{
					{1, "-1"},
					{2, "-2"},
					{1, "-1"},
					{2, "-2"},
				},
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
