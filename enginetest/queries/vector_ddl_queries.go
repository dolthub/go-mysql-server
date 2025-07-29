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
		Name: "basic vector column manipulation",
		SetUpScript: []string{
			`CREATE TABLE vectors (id INT PRIMARY KEY, vec3 VECTOR(3), vec128 VECTOR(128))`,
			`INSERT INTO vectors VALUES (1, '[1.0, 2.0, 3.0]', '[1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0, 17.0, 18.0, 19.0, 20.0, 21.0, 22.0, 23.0, 24.0, 25.0, 26.0, 27.0, 28.0, 29.0, 30.0, 31.0, 32.0, 33.0, 34.0, 35.0, 36.0, 37.0, 38.0, 39.0, 40.0, 41.0, 42.0, 43.0, 44.0, 45.0, 46.0, 47.0, 48.0, 49.0, 50.0, 51.0, 52.0, 53.0, 54.0, 55.0, 56.0, 57.0, 58.0, 59.0, 60.0, 61.0, 62.0, 63.0, 64.0, 65.0, 66.0, 67.0, 68.0, 69.0, 70.0, 71.0, 72.0, 73.0, 74.0, 75.0, 76.0, 77.0, 78.0, 79.0, 80.0, 81.0, 82.0, 83.0, 84.0, 85.0, 86.0, 87.0, 88.0, 89.0, 90.0, 91.0, 92.0, 93.0, 94.0, 95.0, 96.0, 97.0, 98.0, 99.0, 100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 106.0, 107.0, 108.0, 109.0, 110.0, 111.0, 112.0, 113.0, 114.0, 115.0, 116.0, 117.0, 118.0, 119.0, 120.0, 121.0, 122.0, 123.0, 124.0, 125.0, 126.0, 127.0, 128.0]')`,
			`INSERT INTO vectors VALUES (2, '[4.5, 5.5, 6.5]', NULL)`,
			`CREATE TABLE test_vectors (
				id INT PRIMARY KEY,
				small_vec VECTOR(2),
				medium_vec VECTOR(10),
				large_vec VECTOR(1000)
			)`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    `SHOW CREATE TABLE vectors`,
				Expected: []sql.Row{{"vectors", "CREATE TABLE `vectors` (\n  `id` int NOT NULL,\n  `vec3` VECTOR(3),\n  `vec128` VECTOR(128),\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:    `SELECT id, vec3 FROM vectors WHERE id = 1`,
				Expected: []sql.Row{{1, []float64{1.0, 2.0, 3.0}}},
			},
			{
				Query:    `SELECT id, vec3, vec128 FROM vectors WHERE id = 2`,
				Expected: []sql.Row{{2, []float64{4.5, 5.5, 6.5}, nil}},
			},
		},
	},
	{
		Name: "VECTOR type creation and manipulation",
		SetUpScript: []string{
			`CREATE TABLE test_vectors (
				id INT PRIMARY KEY,
				small_vec VECTOR(2),
				medium_vec VECTOR(10),
				large_vec VECTOR(1000)
			)`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `INSERT INTO test_vectors VALUES 
					(1, '[1.0, 2.0]', '[1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]', NULL),
					(2, '[3.5, 4.5]', NULL, NULL)`,
				Expected: []sql.Row{{types.NewOkResult(2)}},
			},
			{
				Query: `SELECT id, small_vec FROM test_vectors ORDER BY id`,
				Expected: []sql.Row{
					{1, []float64{1.0, 2.0}},
					{2, []float64{3.5, 4.5}},
				},
			},
			{
				Query:    `UPDATE test_vectors SET small_vec = '[10.0, 20.0]' WHERE id = 1`,
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1, Info: plan.UpdateInfo{Matched: 1, Updated: 1}}}},
			},
			{
				Query:    `SELECT small_vec FROM test_vectors WHERE id = 1`,
				Expected: []sql.Row{{[]float64{10.0, 20.0}}},
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
				ExpectedErrStr: "VECTOR dimension mismatch: expected 3, got 2",
			},
			{
				Query:          `INSERT INTO error_vectors VALUES (2, '[1.0, 2.0, 3.0, 4.0]')`,
				ExpectedErrStr: "VECTOR dimension mismatch: expected 3, got 4",
			},
			{
				Query:          `INSERT INTO error_vectors VALUES (3, '[1.0, invalid, 3.0]')`,
				ExpectedErrStr: "invalid VECTOR JSON format",
			},
			{
				Query:          `INSERT INTO error_vectors VALUES (4, 'not_an_array')`,
				ExpectedErrStr: "VECTOR must be in JSON array format",
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
					(1, '[1.0, 2.0]'),
					(2, '[3, 4]'),
					(3, '[5.5, 6.7]')`,
				Expected: []sql.Row{{types.NewOkResult(3)}},
			},
			{
				Query: `SELECT id, vec2 FROM format_vectors ORDER BY id`,
				Expected: []sql.Row{
					{1, []float64{1.0, 2.0}},
					{2, []float64{3.0, 4.0}},
					{3, []float64{5.5, 6.7}},
				},
			},
		},
	},
	{
		Name:        "VECTOR boundary dimensions",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query:    `CREATE TABLE min_vector (id INT, vec VECTOR(1))`,
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    `INSERT INTO min_vector VALUES (1, '[42.0]')`,
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    `SELECT vec FROM min_vector`,
				Expected: []sql.Row{{[]float64{42.0}}},
			},
			{
				Query:    `CREATE TABLE max_vector (id INT, vec VECTOR(16000))`,
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
		},
	},
}

// VectorTypeErrorTests tests error conditions for VECTOR type creation
var VectorTypeErrorTests = []QueryErrorTest{
	{
		Query:       `CREATE TABLE invalid_vector1 (id INT, vec VECTOR(0))`,
		ExpectedErr: sql.ErrInvalidColTypeDefinition,
	},
	{
		Query:       `CREATE TABLE invalid_vector2 (id INT, vec VECTOR(16001))`,
		ExpectedErr: sql.ErrInvalidColTypeDefinition,
	},
	{
		Query:       `CREATE TABLE invalid_vector3 (id INT, vec VECTOR(-1))`,
		ExpectedErr: sql.ErrInvalidColTypeDefinition,
	},
}
