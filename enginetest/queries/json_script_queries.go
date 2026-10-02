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
	"github.com/dolthub/go-mysql-server/sql/types"
)

// JSONFunctionsScriptTests contains self-contained json script tests.
var JSONFunctionsScriptTests = []ScriptTest{
	{
		Name: "test json search",
		SetUpScript: []string{
			`create table t (i int primary key, j json);`,
			`insert into t values (0, '{"a": "abc"}'), (1, '{"b": "abc"}'), (2, '{"c": "abc"}');`,
			`insert into t values (3, '{"d": "def"}'), (4, '{"e": "def"}'), (5, '{"f": "def"}');`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select i, json_search(j, 'all', 'abc') from t order by i",
				Expected: []sql.Row{
					{0, types.MustJSON(`"$.a"`)},
					{1, types.MustJSON(`"$.b"`)},
					{2, types.MustJSON(`"$.c"`)},
					{3, nil},
					{4, nil},
					{5, nil},
				},
			},
			{
				Query: "select i, json_search(j, 'all', 'def') from t order by i",
				Expected: []sql.Row{
					{0, nil},
					{1, nil},
					{2, nil},
					{3, types.MustJSON(`"$.d"`)},
					{4, types.MustJSON(`"$.e"`)},
					{5, types.MustJSON(`"$.f"`)},
				},
			},
			{
				Query: "select i, json_search(j, 'all', 'abc', '', '$.a', '$.b') from t order by i",
				Expected: []sql.Row{
					{0, types.MustJSON(`"$.a"`)},
					{1, types.MustJSON(`"$.b"`)},
					{2, nil},
					{3, nil},
					{4, nil},
					{5, nil},
				},
			},
		},
	},
}
