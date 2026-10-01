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
	"github.com/dolthub/go-mysql-server/sql/planbuilder"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// SetOperationsScriptTests contains self-contained script tests for UNION, INTERSECT, EXCEPT, and their output schemas.
var SetOperationsScriptTests = []ScriptTest{
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
		// TODO: This test currently fails in Doltgres because Doltgres does not allow `create table...as select...`
		//   even though it's a valid Postgres query. Remove Dialect tag once fixed in Doltgres
		//   https://github.com/dolthub/doltgresql/issues/1669
		Dialect: "mysql",
		Name:    "union field indexes",
		SetUpScript: []string{
			"create table t(id int primary key auto_increment, words varchar(100))",
			"insert into t(words) values ('foo'),('bar'),('baz'),('zap')",
			"create table t2 as select * from t",
			"update t2 set words = 'boo' where id = 1",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `
select * from (
    select id, words from t
    union
    select id, words from t2
) as combined
where
    combined.id = 1;
`,
				Expected: []sql.Row{
					{1, "foo"},
					{1, "boo"},
				},
			},
			{
				Query: `
select * from (
    select 'parent' as tbl, id, words from t
    union
    select 'child' as tbl, id, words from t2
) as combined
where
    combined.id = 1;
`,
				Expected: []sql.Row{
					{"parent", 1, "foo"},
					{"child", 1, "boo"},
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
		// TODO: Doltgres does not support this query
		// https://github.com/dolthub/dolt/issues/9631
		Name:    "test union/intersect/except over subqueries over joins",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table t1 (i int primary key);",
			"create table t2 (j int primary key);",
			"insert into t1 values (0), (1);",
			"insert into t2 values (0), (2);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `
select * from t1 union (
    select j from t2
    where (
        j > 10
        or
        j in (
            select j from t1 join t2
        )
    )
);
`,
				Expected: []sql.Row{
					{0},
					{1},
					{2},
				},
			},
			{
				Query: `
select * from t1 intersect (
    select j from t2
    where (
        j > 10
        or
        j in (
            select j from t1 join t2
        )
    )
);
`,
				Expected: []sql.Row{
					{0},
				},
			},
			{
				Query: `
select * from t1 except (
    select j from t2
    where (
        j > 10
        or
        j in (
            select j from t1 join t2
        )
    )
);
`,
				Expected: []sql.Row{
					{1},
				},
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
}
