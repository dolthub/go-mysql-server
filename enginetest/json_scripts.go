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

package enginetest

import "github.com/dolthub/go-mysql-server/sql"

var JsonScripts = []ScriptTest{
	{
		Name: "JSON_ARRAYAGG on one column",
		SetUpScript: []string{
			"create table t (o_id int)",
			"INSERT INTO t VALUES (1),(2)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT JSON_ARRAYAGG(o_id) FROM t",
				Expected: []sql.Row{
					{
						sql.MustJSON(`[1,2]`),
					},
				},
			},
		},
	},
	{
		Name: "Simple JSON_ARRAYAGG on two columns",
		SetUpScript: []string{
			"create table t (o_id int, attribute longtext)",
			"INSERT INTO t VALUES (2, 'color'), (2, 'fabric')",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT JSON_ARRAYAGG(o_id), JSON_ARRAYAGG(attribute) FROM t",
				Expected: []sql.Row{
					{
						sql.MustJSON(`[2,2]`),
						sql.MustJSON(`["color","fabric"]`),
					},
				},
			},
		},
	},
	{
		Name: "JSON_ARRAYAGG on column with string values w/ groupby",
		SetUpScript: []string{
			"create table t (o_id int, attribute longtext, value longtext)",
			"INSERT INTO t VALUES (2, 'color', 'red'), (2, 'fabric', 'silk')",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT o_id, JSON_ARRAYAGG(attribute) FROM t GROUP BY o_id",
				Expected: []sql.Row{
					{
						2,
						sql.MustJSON(`["color","fabric"]`),
					},
				},
			},
			{
				Query: "SELECT o_id, JSON_ARRAYAGG(value) FROM t GROUP BY o_id",
				Expected: []sql.Row{
					{
						2,
						sql.MustJSON(`["red","silk"]`),
					},
				},
			},
		},
	},
	{
		Name: "JSON_ARRAYAGG on column with int values w/ groupby",
		SetUpScript: []string{
			"create table t2 (o_id int, val int)",
			"INSERT INTO t2 VALUES (1,1), (1,2), (1,3)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT o_id, JSON_ARRAYAGG(val) FROM t2 GROUP BY o_id",
				Expected: []sql.Row{
					{
						1,
						sql.MustJSON(`[1,2,3]`),
					},
				},
			},
		},
	},
	{
		Name: "JSON_ARRAYAGG on unknown column throws error",
		SetUpScript: []string{
			"create table t2 (o_id int, val int)",
			"INSERT INTO t2 VALUES (1,1), (1,2), (1,3)",
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
			"create table t2 (o_id int)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT JSON_ARRAYAGG(o_id) FROM t2",
				Expected: []sql.Row{
					{
						sql.MustJSON(`[]`),
					},
				},
			},
		},
	},
	{
		Name: "JSON_ARRAYAGG on row with 1 value, 1 null is fine",
		SetUpScript: []string{
			"create table x(pk int, c1 int)",
			"INSERT INTO x VALUES (1,NULL)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT pk, JSON_ARRAYAGG(c1) FROM x GROUP BY pk",
				Expected: []sql.Row{
					{
						1,
						sql.MustJSON(`[null]`),
					},
				},
			},
			{
				Query: "SELECT JSON_ARRAYAGG(c1) FROM x",
				Expected: []sql.Row{
					{
						sql.MustJSON(`[null]`),
					},
				},
			},
		},
	},
	{
		Name: "JSON_ARRAYAGG and group by use the same field.",
		SetUpScript: []string{
			"create table x(pk int, c1 int)",
			"INSERT INTO x VALUES (1, 1)",
			"INSERT INTO x VALUES (1, 2)",
			"INSERT INTO x VALUES (2, 3)",
			"INSERT INTO x VALUES (2, 3)",
			"INSERT INTO x VALUES (3, 5)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT JSON_ARRAYAGG(pk) FROM x GROUP BY pk",
				Expected: []sql.Row{
					{
						sql.MustJSON(`[1,1]`),
					},
					{
						sql.MustJSON(`[2,2]`),
					},
					{
						sql.MustJSON(`[3]`),
					},
				},
			},
		},
	},
	{
		Name: "JSON_ARRAGG with simple and nested json objects.",
		SetUpScript: []string{
			"create table j(field JSON)",
			`INSERT INTO j VALUES('{"key1": {"key": "value"}}')`,
			`INSERT INTO j VALUES('{"key1": "value1", "key2": "value2"}')`,
			`INSERT INTO j VALUES('{"key1": {"key": [2,3]}}')`,
			`INSERT INTO j VALUES('["a", 1]')`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT JSON_ARRAYAGG(field) FROM j GROUP BY field",
				Expected: []sql.Row{
					{
						sql.MustJSON(`[{"key1": "value1", "key2": "value2"}]`),
					},
					{
						sql.MustJSON(`[{"key1": {"key": "value"}}]`),
					},
					{
						sql.MustJSON(`[{"key1":{"key":[2,3]}}]`),
					},
					{
						sql.MustJSON(`[["a",1]]`),
					},
				},
			},
		},
	},
	{
		Name: "Simple JSON_OBJECTAGG with GROUP BY",
		SetUpScript: []string{
			"create table t2 (o_id int, val int)",
			"INSERT INTO t2 VALUES (1,1), (1,2), (1,3)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT JSON_OBJECTAGG(o_id, val) FROM t2 GROUP BY o_id",
				Expected: []sql.Row{
					{
						sql.MustJSON(`{"1": 3}`),
					},
				},
			},
		},
	},
	{
		Name: "More complex JSON_OBJECTAGG WITH GROUP BY",
		SetUpScript: []string{
			"create table t (o_id int, attribute longtext, value longtext)",
			"INSERT INTO t VALUES (2, 'color', 'red'), (2, 'fabric', 'silk')",
			"INSERT INTO t VALUES (3, 'color', 'green'), (3, 'shape', 'square')",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `SELECT o_id, JSON_OBJECTAGG(attribute, value) FROM t GROUP BY o_id`,
				Expected: []sql.Row{
					{
						2, sql.MustJSON(`{"color": "red", "fabric": "silk"}`),
					},
					{
						3, sql.MustJSON(`{"color": "green", "shape": "square"}`),
					},
				},
			},
			{
				Query: `SELECT o_id, JSON_OBJECTAGG(o_id, value) FROM t GROUP BY o_id`,
				Expected: []sql.Row{
					{
						2, sql.MustJSON(`{"2": "silk"}`),
					},
					{
						3, sql.MustJSON(`{"3": "square"}`),
					},
				},
			},
		},
	},
	{
		Name: "3 column table that uses JSON_OBJECTAGG without groupby",
		SetUpScript: []string{
			"create table t (o_id int, attribute longtext, value longtext)",
			"INSERT INTO t VALUES (2, 'color', 'red'), (2, 'fabric', 'silk')",
			"INSERT INTO t VALUES (3, 'color', 'green'), (3, 'shape', 'square')",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `select JSON_OBJECTAGG(o_id, value) from t`,
				Expected: []sql.Row{
					{
						sql.MustJSON(`{"2": "silk", "3": "square"}`),
					},
				},
			},
			{
				Query: `select JSON_OBJECTAGG(attribute, value) from t`,
				Expected: []sql.Row{
					{
						sql.MustJSON(`{"color": "green", "fabric": "silk", "shape": "square"}`),
					},
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
					{
						sql.MustJSON(`{"1": null}`),
					},
				},
			},
		},
	},
	{
		Name: "JSON_OBJECTAGG and nested json values",
		SetUpScript: []string{
			"create table j(pk int, val JSON)",
			`INSERT INTO j VALUES(1, '{"key1": "value1", "key2": "value2"}')`,
			`INSERT INTO j VALUES(1, '{"key1": {"key": [2,3]}}')`,
			`INSERT INTO j VALUES(2, '["a", 1]')`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `SELECT JSON_OBJECTAGG(pk, val) from j`,
				Expected: []sql.Row{
					{
						sql.MustJSON(`{"1": {"key1": {"key": [2, 3]}}, "2": ["a", 1]}`),
					},
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
					{
						nil,
					},
				},
			},
		},
	},
	{
		Name: "JSON_OBJECTAGG handles errors appropriately",
		SetUpScript: []string{
			`create table test (pk int, val longtext)`,
			`insert into test values (1, NULL)`,
			`insert into test values (NULL, 1)`, // NULL keys are not allowed in JSON_OBJECTAGG
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       `SELECT JSON_OBJECTAGG(pk, notval) from test`,
				ExpectedErr: sql.ErrColumnNotFound,
			},
			{
				Query:       `SELECT JSON_OBJECTAGG(notpk, val) from test`,
				ExpectedErr: sql.ErrColumnNotFound,
			},
			{
				Query:       `SELECT JSON_OBJECTAGG(pk, val) from nottest`,
				ExpectedErr: sql.ErrTableNotFound,
			},
			{
				Query:       `SELECT JSON_OBJECTAGG(pk, val, badarg) from test`,
				ExpectedErr: sql.ErrInvalidArgumentNumber,
			},
			{
				Query:       `SELECT JSON_OBJECTAGG(pk) from test`,
				ExpectedErr: sql.ErrInvalidArgumentNumber,
			},
			{
				Query:       `SELECT JSON_OBJECTAGG(pk, val) from test`,
				ExpectedErr: sql.ErrJSONObjectAggNullKey,
			},
		},
	},
}
