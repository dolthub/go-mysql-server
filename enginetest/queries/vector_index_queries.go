// Copyright 2024 Dolthub, Inc.
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

var VectorIndexQueries = []ScriptTest{
	{
		Name: "basic JSON vector index",
		SetUpScript: []string{
			"create table vectors (id int primary key, v json not null);",
			`insert into vectors values (1, '[4.0,3.0]'), (2, '[0.0,0.0]'), (3, '[-1.0,1.0]'), (4, '[0.0,-2.0]');`,
			`create vector index v_idx on vectors(v);`,
			`set @query_vec = '[0.0,0.0]';`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "show create table vectors",
				Expected: []sql.Row{
					{"vectors", "CREATE TABLE `vectors` (\n  `id` int NOT NULL,\n  `v` json NOT NULL,\n  PRIMARY KEY (`id`),\n  VECTOR KEY `v_idx` (`v`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
			{
				Query: "select * from vectors order by VEC_DISTANCE('[0.0,0.0]', v) limit 4",
				Expected: []sql.Row{
					{2, types.MustJSON(`[0.0, 0.0]`)},
					{3, types.MustJSON(`[-1.0, 1.0]`)},
					{4, types.MustJSON(`[0.0, -2.0]`)},
					{1, types.MustJSON(`[4.0, 3.0]`)},
				},
				ExpectedIndexes: []string{"v_idx"},
			},
			{
				// Queries against a user var can be optimized.
				Query: "select * from vectors order by VEC_DISTANCE(@query_vec, v) limit 4",
				Expected: []sql.Row{
					{2, types.MustJSON(`[0.0, 0.0]`)},
					{3, types.MustJSON(`[-1.0, 1.0]`)},
					{4, types.MustJSON(`[0.0, -2.0]`)},
					{1, types.MustJSON(`[4.0, 3.0]`)},
				},
				ExpectedIndexes: []string{"v_idx"},
			},
			{
				// Use the index even when there's a projection involved.
				Query: "select `id`+1 from vectors order by VEC_DISTANCE('[0.0,0.0]', v) limit 4",
				Expected: []sql.Row{
					{3},
					{4},
					{5},
					{2},
				},
				ExpectedIndexes: []string{"v_idx"},
			},
			{
				// Only queries with a limit can use a vector index.
				Query: "select * from vectors order by VEC_DISTANCE('[0.0,0.0]', v)",
				Expected: []sql.Row{
					{2, types.MustJSON(`[0.0, 0.0]`)},
					{3, types.MustJSON(`[-1.0, 1.0]`)},
					{4, types.MustJSON(`[0.0, -2.0]`)},
					{1, types.MustJSON(`[4.0, 3.0]`)},
				},
				ExpectedIndexes: nil,
			},
			{
				Query: "select * from vectors order by VEC_DISTANCE_L2_SQUARED('[0.0,-2.0]', v) limit 4",
				Expected: []sql.Row{
					{4, types.MustJSON(`[0.0, -2.0]`)},
					{2, types.MustJSON(`[0.0, 0.0]`)},
					{3, types.MustJSON(`[-1.0, 1.0]`)},
					{1, types.MustJSON(`[4.0, 3.0]`)},
				},
				ExpectedIndexes: []string{"v_idx"},
			},
			{
				// Ensure vector index is not used for range lookups.
				Query: "select * from vectors order by v limit 4",
				Expected: []sql.Row{
					{3, types.MustJSON(`[-1.0, 1.0]`)},
					{4, types.MustJSON(`[0.0, -2.0]`)},
					{2, types.MustJSON(`[0.0, 0.0]`)},
					{1, types.MustJSON(`[4.0, 3.0]`)},
				},
				ExpectedIndexes: []string{},
			},
			{
				// Modify the index after creation.
				Query: "insert into vectors values (5, '[1.0,0.0]')",
			},
			{
				Query: "select * from vectors order by VEC_DISTANCE('[0.0,0.0]', v)",
				Expected: []sql.Row{
					{2, types.MustJSON(`[0.0, 0.0]`)},
					{5, types.MustJSON(`[1.0, 0.0]`)},
					{3, types.MustJSON(`[-1.0, 1.0]`)},
					{4, types.MustJSON(`[0.0, -2.0]`)},
					{1, types.MustJSON(`[4.0, 3.0]`)},
				},
				ExpectedIndexes: []string{},
			},
		},
	},
	{
		Name: "basic VECTOR vector index",
		SetUpScript: []string{
			"create table vectors (id int primary key, v vector(2) not null);",
			`insert into vectors values (1, STRING_TO_VECTOR('[4.0,3.0]')), (2, STRING_TO_VECTOR('[0.0,0.0]')), (3, STRING_TO_VECTOR('[-1.0,1.0]')), (4, STRING_TO_VECTOR('[0.0,-2.0]'));`,
			`create vector index v_idx on vectors(v);`,
			`set @query_vec = '[0.0,0.0]';`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "show create table vectors",
				Expected: []sql.Row{
					{"vectors", "CREATE TABLE `vectors` (\n  `id` int NOT NULL,\n  `v` VECTOR(2) NOT NULL,\n  PRIMARY KEY (`id`),\n  VECTOR KEY `v_idx` (`v`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
			{
				Query: "select * from vectors order by VEC_DISTANCE('[0.0,0.0]', v) limit 4",
				Expected: []sql.Row{
					{2, floatsToBytes(0.0, 0.0)},
					{3, floatsToBytes(-1.0, 1.0)},
					{4, floatsToBytes(0.0, -2.0)},
					{1, floatsToBytes(4.0, 3.0)},
				},
				ExpectedIndexes: []string{"v_idx"},
			},
			{
				// Queries against a user var can be optimized.
				Query: "select * from vectors order by VEC_DISTANCE(@query_vec, v) limit 4",
				Expected: []sql.Row{
					{2, floatsToBytes(0.0, 0.0)},
					{3, floatsToBytes(-1.0, 1.0)},
					{4, floatsToBytes(0.0, -2.0)},
					{1, floatsToBytes(4.0, 3.0)},
				},
				ExpectedIndexes: []string{"v_idx"},
			},
			{
				// Use the index even when there's a projection involved.
				Query: "select `id`+1 from vectors order by VEC_DISTANCE('[0.0,0.0]', v) limit 4",
				Expected: []sql.Row{
					{3},
					{4},
					{5},
					{2},
				},
				ExpectedIndexes: []string{"v_idx"},
			},
			{
				// Only queries with a limit can use a vector index.
				Query: "select * from vectors order by VEC_DISTANCE('[0.0,0.0]', v)",
				Expected: []sql.Row{
					{2, floatsToBytes(0.0, 0.0)},
					{3, floatsToBytes(-1.0, 1.0)},
					{4, floatsToBytes(0.0, -2.0)},
					{1, floatsToBytes(4.0, 3.0)},
				},
				ExpectedIndexes: nil,
			},
			{
				Query: "select * from vectors order by VEC_DISTANCE_L2_SQUARED('[0.0,-2.0]', v) limit 4",
				Expected: []sql.Row{
					{4, floatsToBytes(0.0, -2.0)},
					{2, floatsToBytes(0.0, 0.0)},
					{3, floatsToBytes(-1.0, 1.0)},
					{1, floatsToBytes(4.0, 3.0)},
				},
				ExpectedIndexes: []string{"v_idx"},
			},
			{
				// Ensure vector index is not used for range lookups.
				Query: "select * from vectors order by v limit 4",
				Expected: []sql.Row{
					{2, floatsToBytes(0.0, 0.0)},
					{4, floatsToBytes(0.0, -2.0)},
					{1, floatsToBytes(4.0, 3.0)},
					{3, floatsToBytes(-1.0, 1.0)},
				},
				ExpectedIndexes: []string{},
			},
			{
				// Modify the index after creation.
				Query: "insert into vectors values (5, STRING_TO_VECTOR('[1.0,0.0]'))",
			},
			{
				Query: "select * from vectors order by VEC_DISTANCE('[0.0,0.0]', v)",
				Expected: []sql.Row{
					{2, floatsToBytes(0.0, 0.0)},
					{5, floatsToBytes(1.0, 0.0)},
					{3, floatsToBytes(-1.0, 1.0)},
					{4, floatsToBytes(0.0, -2.0)},
					{1, floatsToBytes(4.0, 3.0)},
				},
				ExpectedIndexes: []string{},
			},
		},
	},
	{
		Name: "vector index errors",
		SetUpScript: []string{
			"create table vectors (id int primary key, j json, v vector(2));",
			`insert into vectors values
                        (1, '[4.0,3.0]', STRING_TO_VECTOR('[4.0,3.0]')),
                        (2, '[0.0,0.0]', STRING_TO_VECTOR('[0.0,0.0]')),
                        (3, '[-1.0,1.0]', STRING_TO_VECTOR('[-1.0,1.0]')),
                        (4, '[0.0,-2.0]', STRING_TO_VECTOR('[0.0,-2.0]'));`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:          `create vector index v_idx2 on vectors(j, v);`,
				ExpectedErrStr: "a vector index must have exactly one column",
			},
			{
				Query:       `create vector index v_idx2 on vectors(id);`,
				ExpectedErr: sql.ErrVectorInvalidColumnType,
			},
		},
	},
	{
		Name: "vector index on nullable column errors",
		SetUpScript: []string{
			"create table vectors_nullable (id int primary key, v vector(2) null);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       `create vector index v_idx on vectors_nullable(v);`,
				ExpectedErr: sql.ErrNullableVectorIdx,
			},
			{
				Query:       `create table bad_vector_idx (id int primary key, v vector(2), vector index (v));`,
				ExpectedErr: sql.ErrNullableVectorIdx,
			},
		},
	},
}
