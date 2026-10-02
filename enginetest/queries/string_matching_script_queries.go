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

// StringMatchingScriptTests contains self-contained string matching script tests.
var StringMatchingScriptTests = []ScriptTest{
	{
		Name:    "LIKE expression with ESCAPE clause",
		Dialect: "mysql",
		SetUpScript: []string{
			"CREATE TABLE t(value VARCHAR(1), pattern VARCHAR(1));",
			"INSERT INTO t VALUES ('a', 'a');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT FIRST_VALUE(value LIKE pattern ESCAPE '') OVER () AS actual FROM t;",
				Expected: []sql.Row{
					{true},
				},
			},
		},
	},
	{
		// https://github.com/dolthub/dolt/issues/11910
		Name:    "LIKE default backslash escape and explicit ESCAPE clause",
		Dialect: "mysql",
		SetUpScript: []string{
			"CREATE TABLE t (id INT PRIMARY KEY, s VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin);",
			"INSERT INTO t VALUES (1, '100%'), (2, '100_'), (3, '100\\\\'), (4, '100x'), (5, '100xx');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT id FROM t WHERE s LIKE '100\\\\%' ORDER BY id;",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "SELECT id FROM t WHERE s LIKE '100#%' ESCAPE '#' ORDER BY id;",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "SELECT id FROM t WHERE s LIKE '100\\\\_' ORDER BY id;",
				Expected: []sql.Row{{2}},
			},
			{
				Query:    "SELECT id FROM t WHERE s LIKE '100#_' ESCAPE '#' ORDER BY id;",
				Expected: []sql.Row{{2}},
			},
			{
				Query:    "SELECT id FROM t WHERE s LIKE '100\\\\\\\\' ORDER BY id;",
				Expected: []sql.Row{{3}},
			},
			{
				Query:    "SELECT id FROM t WHERE s LIKE '100#\\\\' ESCAPE '#' ORDER BY id;",
				Expected: []sql.Row{{3}},
			},
		},
	},
}
