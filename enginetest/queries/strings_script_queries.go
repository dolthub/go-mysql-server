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
		// https://github.com/dolthub/dolt/issues/9794
		Name: "UPDATE with TRIM function on TEXT column",
		SetUpScript: []string{
			"create table my_table (txt text);",
			"insert into my_table values('foobar');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:            "update my_table set txt = trim(txt);",
				SkipResultsCheck: true,
			},
			{
				Query:    "select txt from my_table;",
				Expected: []sql.Row{{"foobar"}},
			},
		},
	},
	{
		// https://github.com/dolthub/dolt/issues/9794
		Name:    "String functions with TextStorage (comprehensive test)",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table test_strings (id int primary key, content text);",
			"insert into test_strings values (1, '  Hello World  '), (2, 'Test String'), (3, 'LOWERCASE'), (4, 'abc123def');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select id, trim(content) from test_strings order by id;",
				Expected: []sql.Row{{1, "Hello World"}, {2, "Test String"}, {3, "LOWERCASE"}, {4, "abc123def"}},
			},
			{
				Query:    "select id, upper(content) from test_strings order by id;",
				Expected: []sql.Row{{1, "  HELLO WORLD  "}, {2, "TEST STRING"}, {3, "LOWERCASE"}, {4, "ABC123DEF"}},
			},
			{
				Query:    "select id, lower(content) from test_strings order by id;",
				Expected: []sql.Row{{1, "  hello world  "}, {2, "test string"}, {3, "lowercase"}, {4, "abc123def"}},
			},
			{
				Query:    "select id, reverse(content) from test_strings order by id;",
				Expected: []sql.Row{{1, "  dlroW olleH  "}, {2, "gnirtS tseT"}, {3, "ESACREWOL"}, {4, "fed321cba"}},
			},
			{
				Query:    "select id, substring(content, 1, 5) from test_strings order by id;",
				Expected: []sql.Row{{1, "  Hel"}, {2, "Test "}, {3, "LOWER"}, {4, "abc12"}},
			},
			{
				Query:    "select id, length(content) from test_strings order by id;",
				Expected: []sql.Row{{1, 15}, {2, 11}, {3, 9}, {4, 9}},
			},
			{
				Query:    "select id, left(content, 3) from test_strings order by id;",
				Expected: []sql.Row{{1, "  H"}, {2, "Tes"}, {3, "LOW"}, {4, "abc"}},
			},
			{
				Query:    "select id, right(content, 3) from test_strings order by id;",
				Expected: []sql.Row{{1, "d  "}, {2, "ing"}, {3, "ASE"}, {4, "def"}},
			},
			{
				Query:    "select id, ltrim(content) from test_strings order by id;",
				Expected: []sql.Row{{1, "Hello World  "}, {2, "Test String"}, {3, "LOWERCASE"}, {4, "abc123def"}},
			},
			{
				Query:    "select id, rtrim(content) from test_strings order by id;",
				Expected: []sql.Row{{1, "  Hello World"}, {2, "Test String"}, {3, "LOWERCASE"}, {4, "abc123def"}},
			},
			{
				Query:    "select id, replace(content, 'e', 'X') from test_strings order by id;",
				Expected: []sql.Row{{1, "  HXllo World  "}, {2, "TXst String"}, {3, "LOWERCASE"}, {4, "abc123dXf"}},
			},
			{
				Query:    "select id, repeat(substring(content, 1, 2), 2) from test_strings order by id;",
				Expected: []sql.Row{{1, "    "}, {2, "TeTe"}, {3, "LOLO"}, {4, "abab"}},
			},
			{
				Query:    "select id, lpad(content, 12, '*') from test_strings where id = 4;",
				Expected: []sql.Row{{4, "***abc123def"}},
			},
			{
				Query:    "select id, rpad(content, 12, '*') from test_strings where id = 4;",
				Expected: []sql.Row{{4, "abc123def***"}},
			},
			{
				Query:    "select id, locate('o', content) from test_strings order by id;",
				Expected: []sql.Row{{1, 7}, {2, 0}, {3, 2}, {4, 0}},
			},
			{
				Query:    "select id, position('o' in content) from test_strings order by id;",
				Expected: []sql.Row{{1, 7}, {2, 0}, {3, 2}, {4, 0}},
			},
			{
				Query:    "select id, substr(content, 2, 4) from test_strings order by id;",
				Expected: []sql.Row{{1, " Hel"}, {2, "est "}, {3, "OWER"}, {4, "bc12"}},
			},
			{
				Query:    "select id, mid(content, 3, 3) from test_strings order by id;",
				Expected: []sql.Row{{1, "Hel"}, {2, "st "}, {3, "WER"}, {4, "c12"}},
			},
			{
				Query:    "select id, char_length(content) from test_strings order by id;",
				Expected: []sql.Row{{1, 15}, {2, 11}, {3, 9}, {4, 9}},
			},
			{
				Query:    "select id, character_length(content) from test_strings order by id;",
				Expected: []sql.Row{{1, 15}, {2, 11}, {3, 9}, {4, 9}},
			},
			{
				Query:    "select id, octet_length(content) from test_strings order by id;",
				Expected: []sql.Row{{1, 15}, {2, 11}, {3, 9}, {4, 9}},
			},
			{
				Query:    "select id, lcase(content) from test_strings order by id;",
				Expected: []sql.Row{{1, "  hello world  "}, {2, "test string"}, {3, "lowercase"}, {4, "abc123def"}},
			},
			{
				Query:    "select id, ucase(content) from test_strings order by id;",
				Expected: []sql.Row{{1, "  HELLO WORLD  "}, {2, "TEST STRING"}, {3, "LOWERCASE"}, {4, "ABC123DEF"}},
			},
			{
				Query:    "select id, ascii(content) from test_strings order by id;",
				Expected: []sql.Row{{1, uint64(32)}, {2, uint64(84)}, {3, uint64(76)}, {4, uint64(97)}},
			},
			{
				Query:    "select id, hex(content) from test_strings where id = 4;",
				Expected: []sql.Row{{4, "616263313233646566"}},
			},
			{
				Query:    "select id, unhex(hex(content)) = content from test_strings where id = 4;",
				Expected: []sql.Row{{4, true}},
			},
			{
				Query:    "select id, substring_index(content, 'e', 1) from test_strings order by id;",
				Expected: []sql.Row{{1, "  H"}, {2, "T"}, {3, "LOWERCASE"}, {4, "abc123d"}},
			},
			{
				Query:    "select id, insert(content, 2, 3, 'XYZ') from test_strings where id = 4;",
				Expected: []sql.Row{{4, "aXYZ23def"}},
			},
			{
				Query:            "update test_strings set content = concat(trim(content), '!');",
				SkipResultsCheck: true,
			},
			{
				Query:    "select id, content from test_strings order by id;",
				Expected: []sql.Row{{1, "Hello World!"}, {2, "Test String!"}, {3, "LOWERCASE!"}, {4, "abc123def!"}},
			},
			{
				Query:    "SELECT CONCAT_WS(',', content, 'suffix') FROM test_strings WHERE id = 2;",
				Expected: []sql.Row{{"Test String!,suffix"}},
			},
			{
				Query:    "SELECT EXPORT_SET(5, content, 'off') FROM test_strings WHERE id = 2;",
				Expected: []sql.Row{{"Test String!,off,Test String!,off,off,off,off,off,off,off,off,off,off,off,off,off,off,off,off,off,off,off,off,off,off,off,off,off,off,off,off,off,off,off,off,off,off,off,off,off,off,off,off,off,off,off,off,off,off,off,off,off,off,off,off,off,off,off,off,off,off,off,off,off"}},
			},
			{
				Query:    "SELECT FIND_IN_SET('String', content) FROM test_strings WHERE id = 2;",
				Expected: []sql.Row{{int32(0)}},
			},
			{
				Query:    "SELECT MAKE_SET(3, content, 'second', 'third') FROM test_strings WHERE id = 2;",
				Expected: []sql.Row{{"Test String!,second"}},
			},
			{
				Query:    "SELECT SOUNDEX(content) FROM test_strings WHERE id = 2;",
				Expected: []sql.Row{{"T2323652"}},
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
		Name:    "substring function tests with wrappers",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table tbl (t text);",
			"insert into tbl values ('abcdef');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select left(t, 3) from tbl;",
				Expected: []sql.Row{
					{"abc"},
				},
			},
			{
				Query: "select right(t, 3) from tbl;",
				Expected: []sql.Row{
					{"def"},
				},
			},
			{
				Query: "select instr(t, 'bcd') from tbl;",
				Expected: []sql.Row{
					{2},
				},
			},
		},
	},
	{
		Name:    "pipes as concat mode",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table names(first_name varchar(20), last_name varchar(20))",
			"insert into names values ('john', 'smith'), ('bob','burger')",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select true || false",
				Expected: []sql.Row{{true}},
			},
			{
				Query:    "select '0' || '0'",
				Expected: []sql.Row{{false}},
			},
			{
				Query:    "select 'Hello' || ' ' || 'World'",
				Expected: []sql.Row{{false}},
			},
			{
				Query:    "select 1 || 0",
				Expected: []sql.Row{{true}},
			},
			{
				Query:    "select first_name || ' ' || last_name as full_name from names order by full_name",
				Expected: []sql.Row{{false}, {false}},
			},
			{
				Query:    "select 1 + 2 || 3 + 4",
				Expected: []sql.Row{{true}},
			},
			{
				Query:    "select true || 1 || 'abc'",
				Expected: []sql.Row{{true}},
			},
			{
				Query:    "select (1 || 2) || (3 || 4)",
				Expected: []sql.Row{{true}},
			},
			{
				Query:    "select (1 + 2) || (3 + 4)",
				Expected: []sql.Row{{true}},
			},
			{
				Query:    "select ((1 || 2) || 3) || 4",
				Expected: []sql.Row{{true}},
			},
			{
				Query:    "select ((1 + 2) || 3) + 4",
				Expected: []sql.Row{{5}},
			},
			{
				Query:    "SET SESSION sql_mode = CONCAT(@@SESSION.sql_mode, ',PIPES_AS_CONCAT');",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "select true || false",
				Expected: []sql.Row{{"10"}},
			},
			{
				Query:    "select '0' || '0'",
				Expected: []sql.Row{{"00"}},
			},
			{
				Query:    "select 'Hello' || ' ' || 'World'",
				Expected: []sql.Row{{"Hello World"}},
			},
			{
				Query:    "select 1 || 0",
				Expected: []sql.Row{{"10"}},
			},
			{
				Query:    "select first_name || ' ' || last_name as full_name from names order by full_name",
				Expected: []sql.Row{{"bob burger"}, {"john smith"}},
			},
			{
				Query:    "select 1 + 2 || 3 + 4",
				Expected: []sql.Row{{float64(28)}},
			},
			{
				Query:    "select true || 1 || 'abc'",
				Expected: []sql.Row{{"11abc"}},
			},
			{
				Query:    "select (1 || 2) || (3 || 4)",
				Expected: []sql.Row{{"1234"}},
			},
			{
				Query:    "select (1 + 2) || (3 + 4)",
				Expected: []sql.Row{{"37"}},
			},
			{
				Query:    "select ((1 || 2) || 3) || 4",
				Expected: []sql.Row{{"1234"}},
			},
			{
				Query:    "select ((1 + 2) || 3) + 4",
				Expected: []sql.Row{{float64(37)}},
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
