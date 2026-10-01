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
	"github.com/dolthub/vitess/go/mysql"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// StringsScriptTests contains self-contained script tests for text values, string functions, character sets, and collations.
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
		Name:    "CONVERT USING still converts between incompatible character sets",
		Dialect: "mysql",
		SetUpScript: []string{
			"CREATE TABLE test (pk BIGINT PRIMARY KEY, v1 VARCHAR(200)) COLLATE=utf8mb4_0900_ai_ci;",
			"INSERT INTO test VALUES (1, '63273াম'), (2, 'GHD30r'), (3, '8জ্রিয277'), (4, NULL);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT pk, v1, CONVERT(CONVERT(v1 USING latin1) USING utf8mb4) AS round_trip FROM test WHERE v1 <> CONVERT(CONVERT(v1 USING latin1) USING utf8mb4);",
				Expected: []sql.Row{{int64(1), "63273াম", "63273??"}, {int64(3), "8জ্রিয277", "8?????277"}},
			},
		},
	},
	{
		Name:    "Check support for deprecated BINARY attribute after character set",
		Dialect: "mysql",
		SetUpScript: []string{
			"CREATE TABLE test (pk BIGINT PRIMARY KEY, v1 VARCHAR(255) COLLATE utf8mb4_0900_bin);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SHOW CREATE TABLE test;",
				Expected: []sql.Row{{"test", "CREATE TABLE `test` (\n  `pk` bigint NOT NULL,\n  `v1` varchar(255),\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:    "ALTER TABLE test CHANGE v1 v2 VARCHAR(255) CHARACTER SET utf8mb4 BINARY NOT NULL;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "SHOW CREATE TABLE test;",
				Expected: []sql.Row{{"test", "CREATE TABLE `test` (\n  `pk` bigint NOT NULL,\n  `v2` varchar(255) COLLATE utf8mb4_bin NOT NULL,\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:    "CREATE TABLE test2 (pk BIGINT PRIMARY KEY, v1 VARCHAR(255) CHARACTER SET utf8mb4 BINARY);",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "SHOW CREATE TABLE test2;",
				Expected: []sql.Row{{"test2", "CREATE TABLE `test2` (\n  `pk` bigint NOT NULL,\n  `v1` varchar(255) COLLATE utf8mb4_bin,\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
		},
	},
	{
		Name: "invalid utf8 encoding strings",
		SetUpScript: []string{
			"create table t (c char(10), v varchar(10), txt text, b blob, bi binary(10));",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:          "insert into t(c) values (X'9876543210');",
				ExpectedErrStr: "Incorrect string value: '\\x98vT2\\x10' for column 'c' at row 1",
			},
			{
				Query:          "insert into t(v) values (X'9876543210');",
				ExpectedErrStr: "Incorrect string value: '\\x98vT2\\x10' for column 'v' at row 1",
			},
			{
				Query:          "insert into t(txt) values (X'9876543210');",
				ExpectedErrStr: "Incorrect string value: '\\x98vT2\\x10' for column 'txt' at row 1",
			},
			{
				Query: "insert into t(b) values (X'9876543210');",
				Expected: []sql.Row{
					{types.OkResult{RowsAffected: 1}},
				},
			},
			{
				Query: "insert into t(bi) values (X'9876543210');",
				Expected: []sql.Row{
					{types.OkResult{RowsAffected: 1}},
				},
			},
		},
	},
	{
		Name:    "charset validation strict vs non-strict mode",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table charset_test (c char(10), v varchar(10), txt text) character set utf8mb4;",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "set sql_mode = 'STRICT_TRANS_TABLES';",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 0}}},
			},
			{
				Query:          "insert into charset_test(c) values (UNHEX('446F6C744C6162AE'));",
				ExpectedErrStr: "Incorrect string value: '\\xAE' for column 'c' at row 1",
			},
			{
				Query:          "insert into charset_test(v) values (UNHEX('446F6C744C6162AE'));",
				ExpectedErrStr: "Incorrect string value: '\\xAE' for column 'v' at row 1",
			},
			{
				Query:          "insert into charset_test(txt) values (UNHEX('446F6C744C6162AE'));",
				ExpectedErrStr: "Incorrect string value: '\\xAE' for column 'txt' at row 1",
			},
			{
				Query:    "set sql_mode = '';",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 0}}},
			},
			{
				Query: "insert into charset_test(c) values (UNHEX('446F6C744C6162AE'));",
				Expected: []sql.Row{
					{types.OkResult{RowsAffected: 1}},
				},
			},
			{
				Query: "insert into charset_test(v) values (UNHEX('446F6C744C6162AE'));",
				Expected: []sql.Row{
					{types.OkResult{RowsAffected: 1}},
				},
			},
			{
				Query: "insert into charset_test(txt) values (UNHEX('446F6C744C6162AE'));",
				Expected: []sql.Row{
					{types.OkResult{RowsAffected: 1}},
				},
			},
			{
				Query: "select HEX(c), LENGTH(c) from charset_test where c is not null;",
				Expected: []sql.Row{
					{"446F6C744C6162", 7},
				},
			},
			{
				Query: "select HEX(v), LENGTH(v) from charset_test where v is not null;",
				Expected: []sql.Row{
					{"446F6C744C6162", 7},
				},
			},
			{
				Query: "select HEX(txt), LENGTH(txt) from charset_test where txt is not null;",
				Expected: []sql.Row{
					{"446F6C744C6162", 7},
				},
			},
		},
	},
	{
		Name:    "charset validation issue #8893 - customer scenario",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table products (id int primary key, name text character set utf8mb4);",
		},
		Assertions: []ScriptTestAssertion{
			// Test charset validation with invalid UTF-8 data
			{
				Query:          "insert into products values (1, UNHEX('446F6C744C6162AE'));", // "DoltLab" + invalid byte 0xAE
				ExpectedErrStr: "Incorrect string value: '\\xAE' for column 'name' at row 1",
			},
			// Test non-strict mode truncation behavior
			{
				Query:    "set sql_mode = '';",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 0}}},
			},
			{
				Query:    "insert into products values (1, UNHEX('446F6C744C6162AE'));", // Now succeeds with truncation
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1}}},
			},
			// Verify data was truncated at invalid byte (MySQL behavior)
			{
				Query: "select id, name, HEX(name) from products;",
				Expected: []sql.Row{
					{1, "DoltLab", "446F6C744C6162"}, // Invalid byte 0xAE was truncated
				},
			},
			// Customer can now query and work with the data
			{
				Query: "select id, name from products where name like '%Lab%';",
				Expected: []sql.Row{
					{1, "DoltLab"},
				},
			},
		},
	},
	{
		Name:    "charset validation edge cases - formatInvalidByteForError testing",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table charset_edge_test (c char(10), v varchar(20), t text);",
		},
		Assertions: []ScriptTestAssertion{
			// STRICT MODE TESTS
			{
				Query:    "set sql_mode = 'STRICT_TRANS_TABLES';",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 0}}},
			},
			// Single invalid byte (0xAE)
			{
				Query:          "insert into charset_edge_test(c) values (UNHEX('AE'));",
				ExpectedErrStr: "Incorrect string value: '\\xAE' for column 'c' at row 1",
			},
			{
				Query:          "insert into charset_edge_test(v) values (UNHEX('AE'));",
				ExpectedErrStr: "Incorrect string value: '\\xAE' for column 'v' at row 1",
			},
			{
				Query:          "insert into charset_edge_test(t) values (UNHEX('AE'));",
				ExpectedErrStr: "Incorrect string value: '\\xAE' for column 't' at row 1",
			},
			// Multiple invalid bytes
			{
				Query:          "insert into charset_edge_test(c) values (UNHEX('AEAEAE'));",
				ExpectedErrStr: "Incorrect string value: '\\xAE\\xAE\\xAE' for column 'c' at row 1",
			},
			// Overlong sequences
			{
				Query:          "insert into charset_edge_test(c) values (UNHEX('C0C1'));",
				ExpectedErrStr: "Incorrect string value: '\\xC0\\xC1' for column 'c' at row 1",
			},
			// Invalid bytes 0xFE, 0xFF
			{
				Query:          "insert into charset_edge_test(c) values (UNHEX('FE'));",
				ExpectedErrStr: "Incorrect string value: '\\xFE' for column 'c' at row 1",
			},
			{
				Query:          "insert into charset_edge_test(c) values (UNHEX('FF'));",
				ExpectedErrStr: "Incorrect string value: '\\xFF' for column 'c' at row 1",
			},
			// Surrogate pairs
			{
				Query:          "insert into charset_edge_test(c) values (UNHEX('EDA080'));",
				ExpectedErrStr: "Incorrect string value: '\\xED\\xA0\\x80' for column 'c' at row 1",
			},
			{
				Query:          "insert into charset_edge_test(c) values (UNHEX('EDBFBF'));",
				ExpectedErrStr: "Incorrect string value: '\\xED\\xBF\\xBF' for column 'c' at row 1",
			},
			// More overlong sequences
			{
				Query:          "insert into charset_edge_test(c) values (UNHEX('C080'));",
				ExpectedErrStr: "Incorrect string value: '\\xC0\\x80' for column 'c' at row 1",
			},
			{
				Query:          "insert into charset_edge_test(c) values (UNHEX('E08080'));",
				ExpectedErrStr: "Incorrect string value: '\\xE0\\x80\\x80' for column 'c' at row 1",
			},
			{
				Query:          "insert into charset_edge_test(c) values (UNHEX('F0808080'));",
				ExpectedErrStr: "Incorrect string value: '\\xF0\\x80\\x80\\x80' for column 'c' at row 1",
			},
			// Out of range (beyond U+10FFFF)
			{
				Query:          "insert into charset_edge_test(c) values (UNHEX('F4908080'));",
				ExpectedErrStr: "Incorrect string value: '\\xF4\\x90\\x80\\x80' for column 'c' at row 1",
			},
			// Continuation bytes without start byte
			{
				Query:          "insert into charset_edge_test(c) values (UNHEX('80'));",
				ExpectedErrStr: "Incorrect string value: '\\x80' for column 'c' at row 1",
			},
			{
				Query:          "insert into charset_edge_test(c) values (UNHEX('BF'));",
				ExpectedErrStr: "Incorrect string value: '\\xBF' for column 'c' at row 1",
			},
			// Incomplete sequences
			{
				Query:          "insert into charset_edge_test(c) values (UNHEX('C2'));",
				ExpectedErrStr: "Incorrect string value: '\\xC2' for column 'c' at row 1",
			},
			{
				Query:          "insert into charset_edge_test(c) values (UNHEX('E0A0'));",
				ExpectedErrStr: "Incorrect string value: '\\xE0\\xA0' for column 'c' at row 1",
			},
			{
				Query:          "insert into charset_edge_test(c) values (UNHEX('F09080'));",
				ExpectedErrStr: "Incorrect string value: '\\xF0\\x90\\x80' for column 'c' at row 1",
			},
			// Long sequence (tests truncation with ...)
			{
				Query:          "insert into charset_edge_test(c) values (UNHEX('999897969594939291'));",
				ExpectedErrStr: "Incorrect string value: '\\x99\\x98\\x97\\x96\\x95\\x94...' for column 'c' at row 1",
			},
			// Valid UTF-8 with invalid bytes
			{
				Query:          "insert into charset_edge_test(c) values (UNHEX('446F6C744C6162AE'));",
				ExpectedErrStr: "Incorrect string value: '\\xAE' for column 'c' at row 1",
			},

			// NON-STRICT MODE TESTS (should truncate)
			{
				Query:    "set sql_mode = '';",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 0}}},
			},
			{
				Query:    "insert into charset_edge_test(c) values (UNHEX('446F6C744C6162AE'));",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1}}},
			},
			{
				Query:    "insert into charset_edge_test(v) values (UNHEX('48656C6C6FC0'));",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1}}},
			},
			{
				Query:    "insert into charset_edge_test(t) values (UNHEX('54657374FF'));",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1}}},
			},
			// Verify truncated data
			{
				Query: "select HEX(c), LENGTH(c) from charset_edge_test where c is not null;",
				Expected: []sql.Row{
					{"446F6C744C6162", 7},
				},
			},
			{
				Query: "select HEX(v), LENGTH(v) from charset_edge_test where v is not null;",
				Expected: []sql.Row{
					{"48656C6C6F", 5},
				},
			},
			{
				Query: "select HEX(t), LENGTH(t) from charset_edge_test where t is not null;",
				Expected: []sql.Row{
					{"54657374", 4},
				},
			},
		},
	},
	{
		Name:    "charset validation ASCII range tests",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table ascii_test (c char(10), v varchar(20), t text);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "set sql_mode = 'STRICT_TRANS_TABLES';",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 0}}},
			},
			// ASCII range 0x00-0x7F
			{
				Query:    "insert into ascii_test(c) values (UNHEX('00'));",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1}}},
			},
			{
				Query:    "insert into ascii_test(c) values (UNHEX('20'));",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1}}},
			},
			{
				Query:    "insert into ascii_test(c) values (UNHEX('41'));",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1}}},
			},
			{
				Query:    "insert into ascii_test(c) values (UNHEX('7F'));",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1}}},
			},
			{
				Query:    "insert into ascii_test(v) values (UNHEX('48656C6C6F'));", // "Hello"
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1}}},
			},
			{
				Query:    "insert into ascii_test(t) values (UNHEX('00207F41'));",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1}}},
			},
			// Verify ASCII data
			{
				Query: "select HEX(c), LENGTH(c) from ascii_test where c is not null order by c;",
				Expected: []sql.Row{
					{"", 0},
					{"00", 1},
					{"41", 1},
					{"7F", 1},
				},
			},
			{
				Query: "select HEX(v), LENGTH(v) from ascii_test where v is not null;",
				Expected: []sql.Row{
					{"48656C6C6F", 5}, // "Hello"
				},
			},
			{
				Query: "select HEX(t), LENGTH(t) from ascii_test where t is not null;",
				Expected: []sql.Row{
					{"00207F41", 4}, // NULL + SPACE + DEL + A
				},
			},
			// Boundary cases
			{
				Query:    "insert into ascii_test(c) values (UNHEX('7E'));", // 0x7E is valid ASCII
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1}}},
			},
			{
				Query:          "insert into ascii_test(c) values (UNHEX('81'));", // 0x81 is invalid
				ExpectedErrStr: "Incorrect string value: '\\x81' for column 'c' at row 1",
			},
			// Mixed ASCII and invalid (non-strict mode)
			{
				Query:    "set sql_mode = '';", // Non-strict mode
				Expected: []sql.Row{{types.OkResult{RowsAffected: 0}}},
			},
			{
				Query:    "insert into ascii_test(c) values (UNHEX('41424380'));", // ABC + 0x80 (invalid)
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1}}},
			},
			// Verify truncation
			{
				Query: "select HEX(c), LENGTH(c) from ascii_test where HEX(c) = '414243';",
				Expected: []sql.Row{
					{"414243", 3}, // "ABC" - truncated at invalid byte
				},
			},
			// Valid UTF-8 sequences
			{
				Query:    "set sql_mode = 'STRICT_TRANS_TABLES';", // Back to strict mode
				Expected: []sql.Row{{types.OkResult{RowsAffected: 0}}},
			},
			{
				Query:    "insert into ascii_test(c) values (UNHEX('C3A9'));", // é (2-byte UTF-8)
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1}}},
			},
			{
				Query:    "insert into ascii_test(c) values (UNHEX('E282AC'));", // € (3-byte UTF-8)
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1}}},
			},
			{
				Query:    "insert into ascii_test(c) values (UNHEX('F09D849E'));", // 𝄞 (4-byte UTF-8)
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1}}},
			},
			// Function boundary constants (asciiMin=32, asciiMax=127)
			{
				Query:    "insert into ascii_test(c) values (UNHEX('1F'));", // ASCII 31 (below asciiMin=32) - valid ASCII but non-printable
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1}}},
			},
			// Note: UNHEX('80') test is covered in edge cases test above
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
		Name:    "subquery with case insensitive collation",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table tbl (t text) collate=utf8mb4_0900_ai_ci;",
			"insert into tbl values ('abcdef');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select 'AbCdEf' in (select t from tbl);",
				Expected: []sql.Row{
					{true},
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
		// See https://github.com/dolthub/dolt/issues/10924
		Name:    "INSERT IGNORE truncates invalid UTF-8 at first bad byte",
		Dialect: "mysql",
		SetUpScript: []string{
			"CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(255));",
		},
		Assertions: []ScriptTestAssertion{
			{
				// 0x85 is invalid UTF-8; the row is inserted with the value truncated to the valid prefix.
				Query:                 "INSERT IGNORE INTO t VALUES (1, UNHEX('5353442031544220322E3585204E564D65'));",
				Expected:              []sql.Row{{types.OkResult{RowsAffected: 1}}},
				ExpectedWarning:       mysql.ERTruncatedWrongValueForField,
				ExpectedWarningsCount: 1,
			},
			{
				Query:    "SELECT name, HEX(name) FROM t WHERE id = 1;",
				Expected: []sql.Row{{"SSD 1TB 2.5", "5353442031544220322E35"}},
			},
		},
	},
	{
		// See https://github.com/dolthub/dolt/issues/10924
		Name:    "LIKE with invalid UTF-8 pattern issues warning and returns no match",
		Dialect: "mysql",
		SetUpScript: []string{
			"CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(255));",
			"INSERT INTO t VALUES (1, 'hello');",
		},
		Assertions: []ScriptTestAssertion{
			{
				// 0x85 is invalid UTF-8 in the pattern literal; LIKE issues warning 1300 and returns no match.
				Query:                           "SELECT id FROM t WHERE name LIKE '%" + "\x85" + "%';",
				Expected:                        []sql.Row{},
				ExpectedWarning:                 mysql.ERInvalidCharacterString,
				ExpectedWarningsCount:           1,
				ExpectedWarningMessageSubstring: "invalid string for character set",
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
