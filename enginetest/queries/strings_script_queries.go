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

// StringsScriptTests contains self-contained strings script tests.
var StringsScriptTests = []ScriptTest{
	{
		// https://github.com/dolthub/dolt/issues/9872
		Name:        "TEXT(m) syntax support",
		SetUpScript: []string{},
		Dialect:     "mysql",
		Assertions: []ScriptTestAssertion{
			{
				Query: "CREATE TABLE task_instance_note (ti_id VARCHAR(36) NOT NULL, user_id VARCHAR(128), content TEXT(1000), created_at TIMESTAMP(6) NOT NULL, updated_at TIMESTAMP(6) NOT NULL, CONSTRAINT task_instance_note_pkey PRIMARY KEY (ti_id))",
				Expected: []sql.Row{
					{types.NewOkResult(0)},
				},
			},
			{
				Query: "DESCRIBE task_instance_note",
				Expected: []sql.Row{
					{"ti_id", "varchar(36)", "NO", "PRI", nil, ""},
					{"user_id", "varchar(128)", "YES", "", nil, ""},
					{"content", "text", "YES", "", nil, ""},
					{"created_at", "timestamp(6)", "NO", "", nil, ""},
					{"updated_at", "timestamp(6)", "NO", "", nil, ""},
				},
			},
			{
				Query: "CREATE TABLE tiny (t TEXT(255))",
				Expected: []sql.Row{
					{types.NewOkResult(0)},
				},
			},
			{
				Query: "DESCRIBE tiny",
				Expected: []sql.Row{
					{"t", "tinytext", "YES", "", nil, ""},
				},
			},
			{
				Query: "CREATE TABLE smallt (s TEXT(65535))",
				Expected: []sql.Row{
					{types.NewOkResult(0)},
				},
			},
			{
				Query: "DESCRIBE smallt",
				Expected: []sql.Row{
					{"s", "text", "YES", "", nil, ""},
				},
			},
			{
				Query: "CREATE TABLE mediumt (m TEXT(16777215))",
				Expected: []sql.Row{
					{types.NewOkResult(0)},
				},
			},
			{
				Query: "DESCRIBE mediumt",
				Expected: []sql.Row{
					{"m", "mediumtext", "YES", "", nil, ""},
				},
			},
			{
				Query: "CREATE TABLE longt (l TEXT(4294967295))",
				Expected: []sql.Row{
					{types.NewOkResult(0)},
				},
			},
			{
				Query: "DESCRIBE longt",
				Expected: []sql.Row{
					{"l", "longtext", "YES", "", nil, ""},
				},
			},
			{
				Query: "CREATE TABLE d (t TEXT)",
				Expected: []sql.Row{
					{types.NewOkResult(0)},
				},
			},
			{
				Query: "DESCRIBE d",
				Expected: []sql.Row{
					{"t", "text", "YES", "", nil, ""},
				},
			},
		},
	},
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
