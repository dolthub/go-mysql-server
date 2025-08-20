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
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// VectorDDLQueries tests VECTOR type creation, insertion, and querying
var VectorDDLQueries = []ScriptTest{
	{
		Name: "basic VECTOR type creation and manipulation",
		SetUpScript: []string{
			`CREATE TABLE test_vectors (
				id INT PRIMARY KEY,
				small_vec VECTOR(2),
				medium_vec VECTOR(10),
				large_vec VECTOR(1000)
			)`,
			`INSERT INTO test_vectors VALUES 
				(1, STRING_TO_VECTOR('[1.0, 2.0]'), STRING_TO_VECTOR('[1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]'), NULL),
				(2, STRING_TO_VECTOR('[3.5, 4.5]'), NULL, NULL
			)`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    `SHOW CREATE TABLE test_vectors`,
				Expected: []sql.Row{{"test_vectors", "CREATE TABLE `test_vectors` (\n  `id` int NOT NULL,\n  `small_vec` VECTOR(2),\n  `medium_vec` VECTOR(10),\n  `large_vec` VECTOR(1000),\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:    `SELECT id, small_vec, medium_vec FROM test_vectors WHERE id = 2`,
				Expected: []sql.Row{{2, floatsToBytes(3.5, 4.5), nil}},
			},
			{
				Query:    `SELECT id, small_vec, medium_vec FROM test_vectors WHERE id = 1`,
				Expected: []sql.Row{{1, floatsToBytes(1.0, 2.0), floatsToBytes(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0)}},
			},
			{
				Query: `SELECT id, small_vec FROM test_vectors ORDER BY id`,
				Expected: []sql.Row{
					{1, floatsToBytes(1.0, 2.0)},
					{2, floatsToBytes(3.5, 4.5)},
				},
			},
			{
				Query:    `UPDATE test_vectors SET small_vec = STRING_TO_VECTOR('[10.0, 20.0]') WHERE id = 1`,
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1, Info: plan.UpdateInfo{Matched: 1, Updated: 1}}}},
			},
			{
				Query:    `SELECT small_vec FROM test_vectors WHERE id = 1`,
				Expected: []sql.Row{{floatsToBytes(10.0, 20.0)}},
			},
			{
				Query:    `INSERT INTO test_vectors VALUES (3, 0x0000204100002041, NULL, NULL)`, // [10.0, 10.0]
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    `SELECT small_vec FROM test_vectors WHERE id = 3`,
				Expected: []sql.Row{{floatsToBytes(10.0, 10.0)}},
			},
		},
	},
	{
		Name: "VECTOR type error conditions",
		SetUpScript: []string{
			`CREATE TABLE error_vectors (id INT PRIMARY KEY, vec3 VECTOR(3))`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:          `INSERT INTO error_vectors VALUES (1, '[1.0, 2.0]')`,
				ExpectedErrStr: "value of type string cannot be converted to 'vector' type",
			},
			{
				Query:          `INSERT INTO error_vectors VALUES (1, STRING_TO_VECTOR('[1.0, 2.0]'))`,
				ExpectedErrStr: "VECTOR dimension mismatch: expected 3, got 2",
			},
			{
				Query:          `INSERT INTO error_vectors VALUES (2, STRING_TO_VECTOR('[1.0, 2.0, 3.0, 4.0]'))`,
				ExpectedErrStr: "VECTOR dimension mismatch: expected 3, got 4",
			},
			{
				Query:          `INSERT INTO error_vectors VALUES (3, STRING_TO_VECTOR('[1.0, invalid, 3.0]'))`,
				ExpectedErrStr: "can't convert JSON to vector: invalid character 'i' looking for beginning of value",
			},
			{
				Query:          `INSERT INTO error_vectors VALUES (4, STRING_TO_VECTOR('invalid_json'))`,
				ExpectedErrStr: "can't convert JSON to vector: invalid character 'i' looking for beginning of value",
			},
			{
				Query:          `INSERT INTO error_vectors VALUES (5, STRING_TO_VECTOR('[1.0, "not an array"]'))`,
				ExpectedErrStr: "can't convert JSON to vector; expected array of floats, but array contained string",
			},
			{
				Query:          `INSERT INTO error_vectors VALUES (5, STRING_TO_VECTOR('"not an array"'))`,
				ExpectedErrStr: "can't convert JSON to vector; expected array, got string",
			},
			{
				Query:          `CREATE TABLE error_vectors (id INT PRIMARY KEY, vec3 VECTOR(-3))`,
				ExpectedErrStr: "syntax error at position 62 near 'VECTOR'",
			},
			{
				Query:       `CREATE TABLE error_vectors (id INT PRIMARY KEY, vec3 VECTOR(0))`,
				ExpectedErr: sql.ErrInvalidColTypeDefinition,
			},
			{
				Query:       `CREATE TABLE error_vectors (id INT PRIMARY KEY, vec3 VECTOR(17000))`,
				ExpectedErr: sql.ErrInvalidColTypeDefinition,
			},
		},
	},
	{
		Name: "VECTOR type with different data formats",
		SetUpScript: []string{
			`CREATE TABLE format_vectors (id INT PRIMARY KEY, vec2 VECTOR(2))`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `INSERT INTO format_vectors VALUES 
					(1, STRING_TO_VECTOR('[1.0, 2.0]')),
					(2, STRING_TO_VECTOR('[3, 4]')),
					(3, STRING_TO_VECTOR('[55e-1, 67e2]'))`,
				Expected: []sql.Row{{types.NewOkResult(3)}},
			},
			{
				Query: `SELECT id, vec2 FROM format_vectors ORDER BY id`,
				Expected: []sql.Row{
					{1, floatsToBytes(1.0, 2.0)},
					{2, floatsToBytes(3.0, 4.0)},
					{3, floatsToBytes(5.5, 6700)},
				},
			},
		},
	},
}
