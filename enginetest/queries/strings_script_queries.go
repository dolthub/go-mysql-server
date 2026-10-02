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
)

// StringsScriptTests contains self-contained strings script tests.
var StringsScriptTests = []ScriptTest{
	{
		Name: "mismatched collation using hash in tuples",
		SetUpScript: []string{
			"create table t (t1 text collate utf8mb4_0900_bin, t2 text collate utf8mb4_0900_ai_ci)",
			"insert into t values ('ABC', 'DEF')",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select * from t where (t1, t2) in (('ABC', 'DEF'));",
				Expected: []sql.Row{
					{"ABC", "DEF"},
				},
			},
			{
				Query: "select * from t where (t1, t2) in (('ABC', 'def'));",
				Expected: []sql.Row{
					{"ABC", "DEF"},
				},
			},
			{
				Query:    "select * from t where (t1, t2) in (('abc', 'DEF'));",
				Expected: []sql.Row{},
			},
		},
	},
}
