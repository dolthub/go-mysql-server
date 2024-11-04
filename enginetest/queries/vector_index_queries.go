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
		Name: "basic vector index",
		SetUpScript: []string{
			"create table vectors (id int primary key, v json);",
			`insert into vectors values (1, '[4.0,3.0]'), (2, '[0.0,0.0]'), (3, '[-1.0,1.0]'), (4, '[0.0,-2.0]');`,
			`create vector index v_idx on vectors(v);`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "show create table vectors",
				Expected: []sql.Row{
					{"vectors", "CREATE TABLE `vectors` (\n  `id` int NOT NULL,\n  `v` json,\n  PRIMARY KEY (`id`),\n  VECTOR KEY `v_idx` (`v`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
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
}
