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
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

var JsonScripts = []ScriptTest{
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
				Query:       `SELECT JSON_OBJECTAGG(c0, val, badarg) from test`,
				ExpectedErr: sql.ErrInvalidArgumentNumber,
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
				Query:    "select pk from t where json_extract(col1, '$.items') is null;",
				Expected: []sql.Row{{2}, {3}, {5}},
			},
			{
				Query:    "select pk from t where json_extract(col1, '$.items') <> null;",
				Expected: []sql.Row{},
			},
		},
	},
}
