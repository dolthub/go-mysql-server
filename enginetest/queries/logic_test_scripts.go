// Copyright 2023 Dolthub, Inc.
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

// SQLLogicJoinTests is a list of all the logic tests that are run against the sql engine.
var SQLLogicJoinTests = []ScriptTest{
	{
		Name: "joining on different types panics",
		SetUpScript: []string{
			"CREATE TABLE foo (  a INT,  b INT,  c FLOAT,  d FLOAT)",
			"INSERT INTO foo VALUES  (1, 1, 1, 1),  (2, 2, 2, 2),  (3, 3, 3, 3)",
			"CREATE TABLE bar (  a INT,  b FLOAT,  c FLOAT,  d INT)",
			"INSERT INTO bar VALUES  (1, 1, 1, 1),  (2, 2, 2, 2),  (3, 3, 3, 3)",
		},
		Assertions: []ScriptTestAssertion{
			{
				// This panics somewhere in the memo
				Skip:           true,
				Query:          "SELECT * FROM foo JOIN bar ON max(foo.c) < 2",
				ExpectedErrStr: "invalid use of group function",
			},
			{
				// SQLLogicTests incorrectly reports this as an error
				Query: "SELECT * FROM foo NATURAL JOIN bar",
				Expected: []sql.Row{
					{1, 1, 1.0, 1.0},
					{2, 2, 2.0, 2.0},
					{3, 3, 3.0, 3.0},
				},
			},
		},
	},
	{
		Name: "case insensitive join with using clause",
		SetUpScript: []string{
			"CREATE TABLE str1 (a INT PRIMARY KEY, s TEXT COLLATE utf8mb4_0900_ai_ci)",
			"INSERT INTO str1 VALUES (1, 'a' COLLATE utf8mb4_0900_ai_ci), (2, 'A' COLLATE utf8mb4_0900_ai_ci), (3, 'c' COLLATE utf8mb4_0900_ai_ci), (4, 'D' COLLATE utf8mb4_0900_ai_ci)",
			"CREATE TABLE str2 (a INT PRIMARY KEY, s TEXT COLLATE utf8mb4_0900_ai_ci)",
			"INSERT INTO str2 VALUES (1, 'A' COLLATE utf8mb4_0900_ai_ci), (2, 'B' COLLATE utf8mb4_0900_ai_ci), (3, 'C' COLLATE utf8mb4_0900_ai_ci), (4, 'E' COLLATE utf8mb4_0900_ai_ci)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Skip:  true,
				Query: "SELECT s, str1.s, str2.s FROM str1 INNER JOIN str2 USING(s)",
				Expected: []sql.Row{
					{"A", "A", "A"},
					{"a", "a", "A"},
					{"c", "c", "C"},
				},
			},
			{
				Skip:  true,
				Query: "SELECT s, str1.s, str2.s FROM str1 LEFT OUTER JOIN str2 USING(s)",
				Expected: []sql.Row{
					{"a", "a", "A"},
					{"A", "A", "A"},
					{"c", "c", "C"},
					{"D", "D", nil},
				},
			},
			{
				Skip:  true,
				Query: "SELECT s, str1.s, str2.s FROM str1 RIGHT OUTER JOIN str2 USING(s)",
				Expected: []sql.Row{
					{"A", "A", "A"},
					{"A", "a", "A"},
					{"B", nil, "B"},
					{"C", "c", "C"},
					{"E", nil, "E"},
				},
			},
		},
	},
	{
		Name: "values and rows",
		SetUpScript: []string{
			"CREATE TABLE xy (x INT PRIMARY KEY, y INT)",
		},
		Assertions: []ScriptTestAssertion{
			{
				// Syntax error
				Skip:  true,
				Query: "INSERT INTO xy (VALUES ROW(1, 1))",
				Expected: []sql.Row{
					{types.NewOkResult(1)},
				},
			},
		},
	},
}
