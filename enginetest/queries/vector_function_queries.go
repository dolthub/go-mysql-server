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

var VectorFunctionQueries = []ScriptTest{
	{
		Name: "basic usage of VEC_DISTANCE without index",
		SetUpScript: []string{
			"create table vectors (id int primary key, v json);",
			`insert into vectors values (1, '[3.0,4.0]'), (2, '[0.0,0.0]'), (3, '[1.0,-1.0]'), (4, '[-2.0,0.0]');`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select VEC_DISTANCE('[10.0]', '[20.0]');",
				Expected: []sql.Row{{100.0}},
			},
			{
				Query:    "select VEC_DISTANCE_L2_SQUARED('[1.0, 2.0]', '[5.0, 5.0]');",
				Expected: []sql.Row{{25.0}},
			},
			{
				Query: "select * from vectors order by VEC_DISTANCE('[0.0,0.0]', v)",
				Expected: []sql.Row{
					{2, types.MustJSON(`[0.0, 0.0]`)},
					{3, types.MustJSON(`[1.0, -1.0]`)},
					{4, types.MustJSON(`[-2.0, 0.0]`)},
					{1, types.MustJSON(`[3.0, 4.0]`)},
				},
			},
			{
				Query: "select * from vectors order by VEC_DISTANCE_L2_SQUARED('[-2.0,0.0]', v)",
				Expected: []sql.Row{
					{4, types.MustJSON(`[-2.0, 0.0]`)},
					{2, types.MustJSON(`[0.0, 0.0]`)},
					{3, types.MustJSON(`[1.0, -1.0]`)},
					{1, types.MustJSON(`[3.0, 4.0]`)},
				},
			},
		},
	},
}
