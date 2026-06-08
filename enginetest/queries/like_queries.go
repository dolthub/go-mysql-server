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

import "github.com/dolthub/go-mysql-server/sql"

// LikeScriptTests holds the script tests for LIKE pattern matching, covering the constant-prefix
// range optimization across collations, NOT LIKE, joins, code-point boundaries, and index use.
var LikeScriptTests = []ScriptTest{
	{
		// See https://github.com/dolthub/dolt/issues/11182
		Name: "LIKE with a space terminated prefix matches rows with a multibyte character after the prefix",
		SetUpScript: []string{
			"CREATE TABLE m (id INT PRIMARY KEY, txt LONGTEXT);",
			"INSERT INTO m VALUES (1, '## 中 body'), (2, 'plain'), (3, '## ascii body'), (4, '##nospaces'), (5, '## 中');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT id FROM m WHERE txt LIKE '## %' ORDER BY id;",
				Expected: []sql.Row{{1}, {3}, {5}},
			},
			{
				Query:    "SELECT COUNT(*) FROM m WHERE txt LIKE '## %';",
				Expected: []sql.Row{{3}},
			},
			{
				Query:    "SELECT SUM(txt LIKE '## %') FROM m;",
				Expected: []sql.Row{{float64(3)}},
			},
			{
				Query:    "SELECT id FROM m WHERE txt LIKE '##%' ORDER BY id;",
				Expected: []sql.Row{{1}, {3}, {4}, {5}},
			},
		},
	},
	{
		// See https://github.com/dolthub/dolt/issues/11182
		Name: "LIKE with a constant prefix keeps rows that sort after the prefix across collations, NOT LIKE, and joins",
		SetUpScript: []string{
			"CREATE TABLE z (id INT PRIMARY KEY, txt VARCHAR(100) COLLATE utf8mb4_0900_ai_ci);",
			"INSERT INTO z VALUES (1, '## z'), (2, '## y'), (3, '## za'), (4, '## x');",
			"CREATE TABLE n (id INT PRIMARY KEY, txt VARCHAR(100));",
			"INSERT INTO n VALUES (1, '## 中'), (2, 'plain'), (3, '## a');",
			"CREATE TABLE e (id INT PRIMARY KEY, txt LONGTEXT);",
			"INSERT INTO e VALUES (1, '## 😀 x'), (2, '## ascii');",
			"CREATE TABLE a (id INT PRIMARY KEY, txt VARCHAR(50));",
			"CREATE TABLE b (id INT PRIMARY KEY);",
			"INSERT INTO a VALUES (1, '## 中 x'), (2, '## y');",
			"INSERT INTO b VALUES (1), (2);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT id FROM z WHERE txt LIKE '## %' ORDER BY id;",
				// Under an accent insensitive collation, plain ASCII letters that sort
				// after the optimization upper bound must still match the prefix.
				Expected: []sql.Row{{1}, {2}, {3}, {4}},
			},
			{
				Query:    "SELECT SUM(txt LIKE '## %') FROM z;",
				Expected: []sql.Row{{float64(4)}},
			},
			{
				Query: "SELECT id FROM n WHERE txt NOT LIKE '## %' ORDER BY id;",
				// NOT LIKE must exclude every row that LIKE matches, so only 'plain' remains.
				Expected: []sql.Row{{2}},
			},
			{
				Query:    "SELECT id FROM e WHERE txt LIKE '## %' ORDER BY id;",
				Expected: []sql.Row{{1}, {2}},
			},
			{
				Query:    "SELECT a.id FROM a JOIN b ON a.id = b.id AND a.txt LIKE '## %' ORDER BY a.id;",
				Expected: []sql.Row{{1}, {2}},
			},
		},
	},
	{
		// See https://github.com/dolthub/dolt/issues/11182
		Name: "LIKE with a binary-collation prefix ending at a code-point boundary excludes non-matching rows",
		SetUpScript: []string{
			"CREATE TABLE bnd (id INT PRIMARY KEY, txt VARCHAR(50) COLLATE utf8mb4_0900_bin);",
			"INSERT INTO bnd VALUES (1, 'a\U0010FFFF'), (2, 'a\U0010FFFFx'), (3, 'b'), (4, 'a\uD7FF'), (5, 'a\uD7FFx'), (6, 'a\uE000'), (7, 'a\u07FF'), (8, 'a\u07FFx'), (9, 'a\u0800');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT id FROM bnd WHERE txt LIKE 'a\U0010FFFF%' ORDER BY id;",
				// The prefix ends at the maximum code point, so the range has no upper bound and
				// 'b' sorts after it and must be excluded by the retained LIKE.
				Expected: []sql.Row{{1}, {2}},
			},
			{
				Query: "SELECT id FROM bnd WHERE txt LIKE 'a\uD7FF%' ORDER BY id;",
				// The successor of U+D7FF skips the UTF-16 surrogate block to U+E000, so the
				// U+E000 row is just outside the range and must be excluded.
				Expected: []sql.Row{{4}, {5}},
			},
			{
				Query: "SELECT id FROM bnd WHERE txt LIKE 'a\u07FF%' ORDER BY id;",
				// The successor of U+07FF is U+0800, a longer UTF-8 sequence, so the U+0800 row is
				// just outside the range and must be excluded.
				Expected: []sql.Row{{7}, {8}},
			},
		},
	},
	{
		// See https://github.com/dolthub/dolt/issues/11182
		Name: "LIKE with a constant prefix on an indexed column uses an index range scan",
		SetUpScript: []string{
			"CREATE TABLE idx (id INT PRIMARY KEY, txt VARCHAR(50) COLLATE utf8mb4_0900_bin, KEY(txt));",
			"INSERT INTO idx VALUES (1, 'apple'), (2, 'apricot'), (3, 'banana'), (4, 'ap\u4E2D');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:           "SELECT id FROM idx WHERE txt LIKE 'ap%' ORDER BY id;",
				Expected:        []sql.Row{{1}, {2}, {4}},
				ExpectedIndexes: []string{"txt"},
			},
			{
				Query:           "SELECT id FROM idx WHERE txt LIKE 'apple';",
				Expected:        []sql.Row{{1}},
				ExpectedIndexes: []string{"txt"},
			},
		},
	},
	{
		// See https://github.com/dolthub/dolt/issues/11182
		Name: "LIKE with a constant prefix drops the LIKE only for a binary collation",
		SetUpScript: []string{
			"CREATE TABLE cp (id INT PRIMARY KEY, txt VARCHAR(50) COLLATE utf8mb4_0900_bin, KEY(txt));",
			"CREATE TABLE pad (id INT PRIMARY KEY, txt VARCHAR(50) COLLATE utf8mb4_bin, KEY(txt));",
			"INSERT INTO cp VALUES (1, 'apple'), (2, 'banana');",
			"INSERT INTO pad VALUES (1, 'apple'), (2, 'banana');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT id FROM cp WHERE txt LIKE 'ap%';",
				// utf8mb4_0900_bin orders by code point, so the prefix becomes an exact range and
				// the LIKE is dropped, leaving no residual filter above the index access.
				ExpectedPlan: "Project\n" +
					" ├─ columns: [cp.id:0!null]\n" +
					" └─ IndexedTableAccess(cp)\n" +
					"     ├─ index: [cp.txt]\n" +
					"     ├─ static: [{[ap, aq)}]\n" +
					"     ├─ colSet: (1,2)\n" +
					"     ├─ tableId: 1\n" +
					"     └─ Table\n" +
					"         ├─ name: cp\n" +
					"         └─ columns: [id txt]\n",
			},
			{
				Query: "SELECT id FROM pad WHERE txt LIKE 'ap%';",
				// utf8mb4_bin is PAD SPACE, so the LIKE is kept as a residual filter above the
				// lower-bound index range.
				ExpectedPlan: "Project\n" +
					" ├─ columns: [pad.id:0!null]\n" +
					" └─ Filter\n" +
					"     ├─ pad.txt LIKE 'ap%'\n" +
					"     └─ IndexedTableAccess(pad)\n" +
					"         ├─ index: [pad.txt]\n" +
					"         ├─ static: [{[ap, ∞)}]\n" +
					"         ├─ colSet: (1,2)\n" +
					"         ├─ tableId: 1\n" +
					"         └─ Table\n" +
					"             ├─ name: pad\n" +
					"             └─ columns: [id txt]\n",
			},
		},
	},
}
