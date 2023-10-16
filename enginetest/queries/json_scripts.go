// Copyright 2021 Dolthub, Inc.
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
	querypb "github.com/dolthub/vitess/go/vt/proto/query"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

var JsonScripts = []ScriptTest{
	{
		Name: "json_value",
		SetUpScript: []string{
			"CREATE TABLE xy (x bigint primary key, y JSON)",
			`INSERT INTO xy VALUES (0, CAST('["a", "b"]' AS JSON)), (1, CAST('["a", "b", "c", "d"]' AS JSON));`,
			`INSERT INTO xy VALUES (2, CAST('{"a": [{"b": 1}, {"c": 2}]}' AS JSON)), (3, CAST('{"a": {"b": ["c","d"]}}' AS JSON)), (4,NULL);`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `select json_value(y, '$.a', 'json') from xy`,
				Expected: []sql.Row{
					{nil},
					{nil},
					{types.MustJSON("[{\"b\": 1}, {\"c\": 2}]")},
					{types.MustJSON("{\"b\": [\"c\",\"d\"]}")},
					{nil},
				},
			},
			{
				Query: `select json_value(y, '$.a[0].b', 'signed') from xy where x = 2`,
				Expected: []sql.Row{
					{int64(1)},
				},
			},
			{
				Query: `select json_value(y, '$.a[0].b') from xy where x = 2`,
				Expected: []sql.Row{
					{"1"},
				},
			},
			//{
			//	Query: `select json_value(y, '$.a.b', 'signed') from xy where x = 2`,
			//	Expected: []sql.Row{
			//		{nil},
			//	},
			//},
		},
	},
	{
		Name: "json_length",
		SetUpScript: []string{
			"CREATE TABLE xy (x bigint primary key, y JSON)",
			`INSERT INTO xy VALUES (0, CAST('["a", "b"]' AS JSON)), (1, CAST('["a", "b", "c", "d"]' AS JSON));`,
			`INSERT INTO xy VALUES (2, CAST('{"a": [{"b": 1}, {"c": 2}]}' AS JSON)), (3, CAST('{"a": {"b": ["c","d"]}}' AS JSON)), (4,NULL);`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `select json_length(y) from xy`,
				Expected: []sql.Row{
					{2},
					{4},
					{1},
					{1},
					{nil},
				},
			},
			{
				Query:          `select json_length(json_extract(x, "$.a")) from xy`,
				ExpectedErrStr: "failed to extract from expression 'xy.x'; object is not map",
			},
			{
				Query: `select json_length(json_extract(y, "$.a")) from xy`,
				Expected: []sql.Row{
					{nil},
					{nil},
					{2},
					{1},
					{nil},
				},
			},
			{
				Query: `select json_length(json_extract(y, "$.a.b")) from xy where x = 3`,
				Expected: []sql.Row{
					{2},
				},
			},
			{
				Query: `select json_length(y, "$.a.b") from xy where x = 3`,
				Expected: []sql.Row{
					{2},
				},
			},
			{
				Query: `select json_length(y, "$.a[0].b") from xy where x = 2`,
				Expected: []sql.Row{
					{1},
				},
			},
		},
	},
	{
		// https://github.com/dolthub/go-mysql-server/issues/1855",
		Name: "JSON_ARRAY properly handles CHAR bind vars",
		SetUpScript: []string{
			"CREATE TABLE `users` (`id` bigint unsigned AUTO_INCREMENT,`name` longtext,`languages` JSON, PRIMARY KEY (`id`))",
			`INSERT INTO users (name, languages) VALUES ('Tom', CAST('["ZH", "EN"]' AS JSON));`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `SELECT * FROM users WHERE JSON_CONTAINS (languages, JSON_ARRAY(?)) ORDER BY users.id LIMIT 1`,
				// CHAR bind vars are converted to VAR_BINARY on the wire path
				Bindings: map[string]*querypb.BindVariable{
					"v1": {Type: querypb.Type_VARBINARY, Value: []byte("ZH")},
				},
				Expected: []sql.Row{{uint64(1), "Tom", types.JSONDocument{Val: []interface{}{"ZH", "EN"}}}},
			},
		},
	},
	{
		Name: "JSON_ARRAYAGG on one column",
		SetUpScript: []string{
			"create table t (o_id int primary key)",
			"INSERT INTO t VALUES (1),(2)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT JSON_ARRAYAGG(o_id) FROM (SELECT * FROM t ORDER BY o_id) as sub",
				Expected: []sql.Row{
					{
						types.MustJSON(`[1,2]`),
					},
				},
			},
		},
	},
	{
		Name: "Simple JSON_ARRAYAGG on two columns",
		SetUpScript: []string{
			"create table t (o_id int primary key, attribute longtext)",
			"INSERT INTO t VALUES (1, 'color'), (2, 'fabric')",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT JSON_ARRAYAGG(o_id), JSON_ARRAYAGG(`attribute`) FROM (SELECT * FROM t ORDER BY o_id) as sub;",
				Expected: []sql.Row{
					{
						types.MustJSON(`[1,2]`),
						types.MustJSON(`["color","fabric"]`),
					},
				},
			},
		},
	},
	{
		Name: "JSON_ARRAYAGG on column with string values w/ groupby",
		SetUpScript: []string{
			"create table t (o_id int primary key, c0 int, attribute longtext, value longtext)",
			"INSERT INTO t VALUES (1, 2, 'color', 'red'), (2, 2, 'fabric', 'silk')",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT c0, JSON_ARRAYAGG(`attribute`) FROM (SELECT * FROM t ORDER BY o_id) as sub GROUP BY c0",
				Expected: []sql.Row{
					{
						2,
						types.MustJSON(`["color","fabric"]`),
					},
				},
			},
			{
				Query: "SELECT c0, JSON_ARRAYAGG(value) FROM (SELECT * FROM t ORDER BY o_id) as sub GROUP BY c0",
				Expected: []sql.Row{
					{
						2,
						types.MustJSON(`["red","silk"]`),
					},
				},
			},
		},
	},
	{
		Name: "JSON_ARRAYAGG on column with int values w/ groupby",
		SetUpScript: []string{
			"create table t2 (o_id int primary key, val int)",
			"INSERT INTO t2 VALUES (1,1), (2,1), (3,1)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT val, JSON_ARRAYAGG(o_id) FROM (SELECT * FROM t2 ORDER BY o_id) AS sub GROUP BY val",
				Expected: []sql.Row{
					{
						1,
						types.MustJSON(`[1,2,3]`),
					},
				},
			},
		},
	},
	{
		Name: "JSON_ARRAYAGG on unknown column throws error",
		SetUpScript: []string{
			"create table t2 (o_id int primary key, val int)",
			"INSERT INTO t2 VALUES (1,1), (2,2), (3,3)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "SELECT o_id, JSON_ARRAYAGG(val2) FROM t2 GROUP BY o_id",
				ExpectedErr: sql.ErrColumnNotFound,
			},
		},
	},
	{
		Name: "JSON_ARRAYAGG on column with no rows returns NULL",
		SetUpScript: []string{
			"create table t2 (o_id int primary key)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT JSON_ARRAYAGG(o_id) FROM t2",
				Expected: []sql.Row{
					{
						types.MustJSON(`[]`),
					},
				},
			},
		},
	},
	{
		Name: "JSON_ARRAYAGG on row with 1 value, 1 null is fine",
		SetUpScript: []string{
			"create table x(pk int primary key, c1 int)",
			"INSERT INTO x VALUES (1,NULL)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT pk, JSON_ARRAYAGG(c1) FROM x GROUP BY pk",
				Expected: []sql.Row{
					{1, types.MustJSON(`[null]`)},
				},
			},
			{
				Query: "SELECT JSON_ARRAYAGG(c1) FROM x",
				Expected: []sql.Row{
					{types.MustJSON(`[null]`)},
				},
			},
		},
	},
	{
		Name: "JSON_ARRAYAGG and group by use the same field.",
		SetUpScript: []string{
			"create table x(pk int primary key, c1 int)",
			"INSERT INTO x VALUES (1, 1)",
			"INSERT INTO x VALUES (2, 1)",
			"INSERT INTO x VALUES (3, 3)",
			"INSERT INTO x VALUES (4, 3)",
			"INSERT INTO x VALUES (5, 5)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT JSON_ARRAYAGG(pk) FROM (SELECT * FROM x ORDER BY pk) as sub GROUP BY c1",
				Expected: []sql.Row{
					{types.MustJSON(`[1,2]`)},
					{types.MustJSON(`[3,4]`)},
					{types.MustJSON(`[5]`)},
				},
			},
		},
	},
	{
		Name: "JSON_ARRAGG with simple and nested json objects.",
		SetUpScript: []string{
			"create table j(pk int primary key, field JSON)",
			`INSERT INTO j VALUES(1, '{"key1": {"key": "value"}}')`,
			`INSERT INTO j VALUES(2, '{"key1": "value1", "key2": "value2"}')`,
			`INSERT INTO j VALUES(3, '{"key1": {"key": [2,3]}}')`,
			`INSERT INTO j VALUES(4, '["a", 1]')`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT pk, JSON_ARRAYAGG(field) FROM (SELECT * FROM j ORDER BY pk) as sub GROUP BY field ORDER BY pk",
				Expected: []sql.Row{
					{1, types.MustJSON(`[{"key1": {"key": "value"}}]`)},
					{2, types.MustJSON(`[{"key1": "value1", "key2": "value2"}]`)},
					{3, types.MustJSON(`[{"key1":{"key":[2,3]}}]`)},
					{4, types.MustJSON(`[["a",1]]`)},
				},
			},
		},
	},
	{
		Name: "Simple JSON_OBJECTAGG with GROUP BY",
		SetUpScript: []string{
			"create table t2 (o_id int primary key, val int)",
			"INSERT INTO t2 VALUES (1,1), (2,1), (3,1)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT JSON_OBJECTAGG(val, o_id) FROM (SELECT * FROM t2 ORDER BY o_id) as sub GROUP BY val",
				Expected: []sql.Row{
					{types.MustJSON(`{"1": 3}`)},
				},
			},
		},
	},
	{
		Name: "More complex JSON_OBJECTAGG WITH GROUP BY",
		SetUpScript: []string{
			"create table t (o_id int primary key, c0 int, attribute longtext, value longtext)",
			"INSERT INTO t VALUES (1, 2, 'color', 'red'), (2, 2, 'fabric', 'silk')",
			"INSERT INTO t VALUES (3, 3, 'color', 'green'), (4, 3, 'shape', 'square')",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT c0, JSON_OBJECTAGG(`attribute`, value) FROM (SELECT * FROM t ORDER BY o_id) as sub GROUP BY c0",
				Expected: []sql.Row{
					{2, types.MustJSON(`{"color": "red", "fabric": "silk"}`)},
					{3, types.MustJSON(`{"color": "green", "shape": "square"}`)},
				},
			},
			{
				Query: `SELECT c0, JSON_OBJECTAGG(c0, value) FROM (SELECT * FROM t ORDER BY o_id) as sub GROUP BY c0`,
				Expected: []sql.Row{
					{2, types.MustJSON(`{"2": "silk"}`)},
					{3, types.MustJSON(`{"3": "square"}`)},
				},
			},
		},
	},
	{
		Name: "3 column table that uses JSON_OBJECTAGG without groupby",
		SetUpScript: []string{
			"create table t (o_id int primary key, c0 int, attribute longtext, value longtext)",
			"INSERT INTO t VALUES (1, 2, 'color', 'red'), (2, 2, 'fabric', 'silk')",
			"INSERT INTO t VALUES (3, 3, 'color', 'green'), (4, 3, 'shape', 'square')",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `select JSON_OBJECTAGG(c0, value) from (SELECT * FROM t ORDER BY o_id) as sub`,
				Expected: []sql.Row{
					{types.MustJSON(`{"2": "silk", "3": "square"}`)},
				},
			},
			{
				Query: "select JSON_OBJECTAGG(`attribute`, value) from (SELECT * FROM t ORDER BY o_id) as sub",
				Expected: []sql.Row{
					{types.MustJSON(`{"color": "green", "fabric": "silk", "shape": "square"}`)},
				},
			},
		},
	},
	{
		Name: "JSON_OBJECTAGG and null values",
		SetUpScript: []string{
			`create table test (pk int primary key, val longtext)`,
			`insert into test values (1, NULL)`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `SELECT JSON_OBJECTAGG(pk, val) from test`,
				Expected: []sql.Row{
					{types.MustJSON(`{"1": null}`)},
				},
			},
		},
	},
	{
		Name: "JSON_OBJECTAGG and nested json values",
		SetUpScript: []string{
			"create table j(pk int primary key, c0 int, val JSON)",
			`INSERT INTO j VALUES(1, 1, '{"key1": "value1", "key2": "value2"}')`,
			`INSERT INTO j VALUES(2, 1, '{"key1": {"key": [2,3]}}')`,
			`INSERT INTO j VALUES(3, 2, '["a", 1]')`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `SELECT JSON_OBJECTAGG(c0, val) from (SELECT * FROM j ORDER BY pk) as sub`,
				Expected: []sql.Row{
					{types.MustJSON(`{"1": {"key1": {"key": [2, 3]}}, "2": ["a", 1]}`)},
				},
			},
		},
	},
	{
		Name: "JSON_OBJECTAGG correctly returns null when no rows are present",
		SetUpScript: []string{
			`create table test (pk int primary key, val longtext)`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `SELECT JSON_OBJECTAGG(pk, val) from test`,
				Expected: []sql.Row{
					{nil},
				},
			},
		},
	},
	{
		Name: "JSON_OBJECTAGG handles errors appropriately",
		SetUpScript: []string{
			`create table test (pk int primary key, c0 int, val longtext)`,
			`insert into test values (1, 1, NULL)`,
			`insert into test values (2, NULL, 1)`, // NULL keys are not allowed in JSON_OBJECTAGG
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       `SELECT JSON_OBJECTAGG(c0, notval) from test`,
				ExpectedErr: sql.ErrColumnNotFound,
			},
			{
				Query:       `SELECT JSON_OBJECTAGG(notpk, val) from test`,
				ExpectedErr: sql.ErrColumnNotFound,
			},
			{
				Query:       `SELECT JSON_OBJECTAGG(c0, val) from nottest`,
				ExpectedErr: sql.ErrTableNotFound,
			},
			{
				Query:       `SELECT JSON_OBJECTAGG(c0, val, 'badarg') from test`,
				ExpectedErr: sql.ErrInvalidArgumentNumber,
			},
			{
				Query:       `SELECT JSON_OBJECTAGG(c0, val, badarg) from test`,
				ExpectedErr: sql.ErrColumnNotFound,
			},
			{
				Query:       `SELECT JSON_OBJECTAGG(c0) from test`,
				ExpectedErr: sql.ErrInvalidArgumentNumber,
			},
			{
				Query:       `SELECT JSON_OBJECTAGG(c0, val) from test`,
				ExpectedErr: sql.ErrJSONObjectAggNullKey,
			},
		},
	},
	{
		Name: "JSON -> and ->> operator support",
		SetUpScript: []string{
			"create table t (pk int primary key, col1 JSON, col2 JSON);",
			`insert into t values (1, JSON_OBJECT('key1', 1, 'key2', '"abc"'), JSON_ARRAY(3,10,5,17,"z"));`,
			`insert into t values (2, JSON_OBJECT('key1', 100, 'key2', '"ghi"'), JSON_ARRAY(3,10,5,17,JSON_ARRAY(22,"y",66)));`,
			`CREATE TABLE t2 (i INT PRIMARY KEY, j JSON);`,
			`INSERT INTO t2 VALUES (0, '{"a": "123", "outer": {"inner": 456}}');`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    `select col1->'$.key1' from t;`,
				Expected: []sql.Row{{types.MustJSON("1")}, {types.MustJSON("100")}},
			},
			{
				Query:    `select col1->>'$.key2' from t;`,
				Expected: []sql.Row{{"abc"}, {"ghi"}},
			},
			{
				Query:    `select pk, col1 from t where col1->'$.key1' = 1;`,
				Expected: []sql.Row{{1, types.MustJSON(`{"key1":1, "key2":"\"abc\""}`)}},
			},
			{
				Query:    `select pk, col1 from t where col1->>'$.key2' = 'abc';`,
				Expected: []sql.Row{{1, types.MustJSON(`{"key1":1, "key2":"\"abc\""}`)}},
			},
			{
				Query:    `select * from t where col1->>'$.key2' = 'def';`,
				Expected: []sql.Row{},
			},
			{
				Query:    `SELECT col2->"$[3]", col2->>"$[3]" FROM t;`,
				Expected: []sql.Row{{types.MustJSON("17"), "17"}, {types.MustJSON("17"), "17"}},
			},
			{
				Query:    `SELECT col2->"$[4]", col2->>"$[4]" FROM t where pk=1;`,
				Expected: []sql.Row{{types.MustJSON("\"z\""), "z"}},
			},
			{
				// TODO: JSON_Extract doesn't seem able to handle a JSON path expression that references a nested array
				//       This errors with "object is not Slice"
				Skip:     true,
				Query:    `SELECT col2->>"$[3]", col2->>"$[4][0]" FROM t;`,
				Expected: []sql.Row{{17, 44}, {17, "y"}},
			},
			{
				Query:    `SELECT k->"$.inner" from (SELECT j->"$.outer" AS k FROM t2) sq;`,
				Expected: []sql.Row{{types.MustJSON("456")}},
			},
		},
	},
	// from https://dev.mysql.com/doc/refman/8.0/en/json.html#json-converting-between-types:~:text=information%20and%20examples.-,Comparison%20and%20Ordering%20of%20JSON%20Values,-JSON%20values%20can
	{
		Name: "json is ordered correctly",
		SetUpScript: []string{
			"create table t (pk int primary key, col1 json);",
			"insert into t values (1, null);",
			"insert into t values (2, '{}');",
			"insert into t values (3, (select json_extract('{\"a\": null}', '$.a')));",
			"insert into t values (4, 0);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select * from t order by col1 asc;",
				Expected: []sql.Row{
					{1, nil},
					{3, types.MustJSON("null")},
					{4, types.MustJSON("0")},
					{2, types.MustJSON("{}")},
				},
			},
			{
				Query: "select * from t order by col1 desc;",
				Expected: []sql.Row{
					{2, types.MustJSON("{}")},
					{4, types.MustJSON("0")},
					{3, types.MustJSON("null")},
					{1, nil},
				},
			},
		},
	},
	{
		// https://github.com/dolthub/dolt/issues/4499
		Name: "json is formatted correctly",
		SetUpScript: []string{
			"create table t (pk int primary key, col1 json);",

			// formatted json
			`insert into t values (1, '{"a": 1, "b": 2}');`,
			// unordered keys with correct spacing
			`insert into t values (2, '{"b": 2, "a": 1}');`,
			// ordered keys with no spacing
			`insert into t values (3, '{"a":1,"b":2}');`,
			// unordered keys with no spacing
			`insert into t values (4, '{"b":2,"a":1}');`,
			// unordered keys with extra spacing
			`insert into t values (5, '{ "b": 2 , "a" : 1 }');`,

			// ordered keys with arrays of primitives without spaces
			`insert into t values (6, '{"a":[1,2,3],"b":[4,5,6]}');`,
			// unordered keys with arrays of primitives without spaces
			`insert into t values (7, '{"b":[4,5,6],"a":[1,2,3]}');`,
			// ordered keys with arrays of primitives with extra spaces
			`insert into t values (8, '{ "a" : [ 1 , 2 , 3 ] , "b" : [ 4 , 5 , 6 ] }');`,
			// unordered keys with arrays of primitives with extra spaces
			`insert into t values (9, '{ "b" : [ 4 , 5 , 6 ] , "a" : [ 1 , 2 , 3 ] }');`,

			// ordered keys with arrays of objects without spaces
			`insert into t values (10, '{"a":[{"a":1},{"b":2}],"b":[{"c":3},{"d":4}]}');`,
			// ordered keys with arrays of objects with extra spaces
			`insert into t values (11, '{ "a" : [ { "a" : 1 } , { "b" : 2 } ] , "b" : [ { "c" : 3 } , { "d" : 4 } ] }');`,
			// unordered keys with arrays of objects without spaces
			`insert into t values (12, '{"b":[{"c":3},{"d":4}],"a":[{"a":1},{"b":2}]}');`,
			// unordered keys with arrays of objects with extra spaces
			`insert into t values (13, '{ "b" : [ { "c" : 3 } , { "d" : 4 } ] , "a" : [ { "a" : 1 } , { "b" : 2 } ] }');`,

			// formatted json with special characters
			`insert into t values (14, '[{"a":"<>&"}]');`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select pk, cast(col1 as char) from t order by pk asc;",
				Expected: []sql.Row{
					{1, `{"a": 1, "b": 2}`},
					{2, `{"a": 1, "b": 2}`},
					{3, `{"a": 1, "b": 2}`},
					{4, `{"a": 1, "b": 2}`},
					{5, `{"a": 1, "b": 2}`},
					{6, `{"a": [1, 2, 3], "b": [4, 5, 6]}`},
					{7, `{"a": [1, 2, 3], "b": [4, 5, 6]}`},
					{8, `{"a": [1, 2, 3], "b": [4, 5, 6]}`},
					{9, `{"a": [1, 2, 3], "b": [4, 5, 6]}`},
					{10, `{"a": [{"a": 1}, {"b": 2}], "b": [{"c": 3}, {"d": 4}]}`},
					{11, `{"a": [{"a": 1}, {"b": 2}], "b": [{"c": 3}, {"d": 4}]}`},
					{12, `{"a": [{"a": 1}, {"b": 2}], "b": [{"c": 3}, {"d": 4}]}`},
					{13, `{"a": [{"a": 1}, {"b": 2}], "b": [{"c": 3}, {"d": 4}]}`},
					{14, `[{"a": "<>&"}]`},
				},
			},
		},
	},
	{
		Name: "json_extract returns missing keys as sql null and handles json null literals correctly",
		SetUpScript: []string{
			"create table t (pk int primary key, col1 json);",
			"insert into t values (1, '{\"items\": {\"1\": 1, \"2\": 2}}');",
			"insert into t values (2, null);",
			"insert into t values (3, '{}');",
			"insert into t values (4, '{\"items\": null}');",
			"insert into t values (5, (select json_extract('{\"a\": null}', '$.a')));",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select pk, json_extract(col1, '$.items') from t order by pk;",
				Expected: []sql.Row{
					{1, types.MustJSON("{\"1\":1,\"2\":2}")},
					{2, nil},
					{3, nil},
					{4, types.MustJSON("null")},
					{5, nil},
				},
			},
			{
				Query: "select pk, json_extract(col1, '$') from t order by pk;",
				Expected: []sql.Row{
					{1, types.MustJSON("{\"items\": {\"1\": 1, \"2\": 2}}")},
					{2, nil},
					{3, types.MustJSON("{}")},
					{4, types.MustJSON("{\"items\": null}")},
					{5, types.MustJSON("null")},
				},
			},
			{
				Query: "select pk, json_extract(col1, '$.items') is null from t order by pk;",
				Expected: []sql.Row{
					{1, false},
					{2, true},
					{3, true},
					{4, false},
					{5, true},
				},
			},
			{
				Query: "select pk, json_extract(col1, '$.items') <> null from t order by pk;",
				Expected: []sql.Row{
					{1, nil},
					{2, nil},
					{3, nil},
					{4, nil},
					{5, nil},
				},
			},
			{
				Query: "select pk, json_extract(col1, '$.items.*') from t order by pk;",
				Expected: []sql.Row{
					{1, types.MustJSON("[1, 2]")},
					{2, nil},
					{3, nil},
					{4, types.MustJSON("null")},
					{5, nil},
				},
			},
			{
				Query:    "select pk from t where json_extract(col1, '$.items') is null;",
				Expected: []sql.Row{{2}, {3}, {5}},
			},
			{
				Query:    "select pk from t where json_extract(col1, '$.items') <> null;",
				Expected: []sql.Row{},
			},
		},
	},
	{
		Name: "json_contains_path returns true if the path exists",
		SetUpScript: []string{
			`create table t (pk int primary key, col1 json);`,
			`insert into t values (1, '{"a": 1}');`,
			`insert into t values (2, '{"a": 1, "b": 2, "c": {"d": 4}}');`,
			`insert into t values (3, '{"w": 1, "x": 2, "c": {"d": 4}}');`,
			`insert into t values (4, '{}');`,
			`insert into t values (5, null);`,
		},

		Assertions: []ScriptTestAssertion{
			{
				Query: "select pk, json_contains_path(col1, 'one', '$.a') from t order by pk;",
				Expected: []sql.Row{
					{1, true},
					{2, true},
					{3, false},
					{4, false},
					{5, nil},
				},
			},
			{
				Query: "select pk, json_contains_path(col1, 'one', '$.a', '$.x', '$.c.d') from t order by pk;",
				Expected: []sql.Row{
					{1, true},
					{2, true},
					{3, true},
					{4, false},
					{5, nil},
				},
			},
			{
				Query: "select pk, json_contains_path(col1, 'all', '$.a', '$.x') from t order by pk;",
				Expected: []sql.Row{
					{1, false},
					{2, false},
					{3, false},
					{4, false},
					{5, nil},
				},
			},
			{
				Query: "select pk, json_contains_path(col1, 'all', '$.c.d', '$.x') from t order by pk;",
				Expected: []sql.Row{
					{1, false},
					{2, false},
					{3, true},
					{4, false},
					{5, nil},
				},
			},
			{
				Query:          "select pk, json_contains_path(col1, 'other', '$.c.d', '$.x') from t order by pk;",
				ExpectedErrStr: "The oneOrAll argument to json_contains_path may take these values: 'one' or 'all'",
			},
		},
	},
	{
		Name: "json type value compared with number type value",
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT JSON_EXTRACT('0.4', '$')",
				Expected: []sql.Row{{types.MustJSON(`0.4`)}},
			},
			{
				Query:    "SELECT JSON_EXTRACT('0.4', '$') > 0;",
				Expected: []sql.Row{{true}},
			},
			{
				Query:    "SELECT JSON_EXTRACT('0.4', '$') <= 0;",
				Expected: []sql.Row{{false}},
			}, {
				Query:    "SELECT JSON_EXTRACT('0.4', '$') = 0;",
				Expected: []sql.Row{{false}},
			},
			{
				Query:    "SELECT JSON_EXTRACT('0.4', '$') = 0.4;",
				Expected: []sql.Row{{true}},
			},
		},
	},
}
