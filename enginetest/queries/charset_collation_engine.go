// Copyright 2022 Dolthub, Inc.
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

// CharsetCollationEngineTest is used to test character sets.
type CharsetCollationEngineTest struct {
	Name        string
	SetUpScript []string
	Queries     []CharsetCollationEngineTestQuery
}

// CharsetCollationEngineTestQuery is a query within a CharsetCollationEngineTest.
type CharsetCollationEngineTestQuery struct {
	Query    string
	Expected []sql.Row
	Error    bool
}

// CharsetCollationEngineTests are used to ensure that character sets and collations have the correct behavior over the
// engine. Return values should all have the `utf8mb4` encoding, as it's returning the internal encoding type.
var CharsetCollationEngineTests = []CharsetCollationEngineTest{
	{
		Name: "Insert multiple character sets",
		SetUpScript: []string{
			"CREATE TABLE test (v1 VARCHAR(255) COLLATE utf16_unicode_ci);",
		},
		Queries: []CharsetCollationEngineTestQuery{
			{
				Query:    "INSERT INTO test VALUES ('hey');",
				Expected: []sql.Row{{sql.NewOkResult(1)}},
			},
			{
				Query:    "INSERT INTO test VALUES (_utf16'\x00h\x00i');",
				Expected: []sql.Row{{sql.NewOkResult(1)}},
			},
			{
				Query:    "INSERT INTO test VALUES (_utf8mb4'\x68\x65\x6c\x6c\x6f');",
				Expected: []sql.Row{{sql.NewOkResult(1)}},
			},
			{
				Query:    "SELECT * FROM test ORDER BY 1;",
				Expected: []sql.Row{{"hello"}, {"hey"}, {"hi"}},
			},
		},
	},
	{
		Name: "Sorting differences",
		SetUpScript: []string{
			"CREATE TABLE test1 (v1 VARCHAR(255) COLLATE utf8mb4_0900_bin);",
			"CREATE TABLE test2 (v1 VARCHAR(255) COLLATE utf16_unicode_ci);",
		},
		Queries: []CharsetCollationEngineTestQuery{
			{
				Query:    "INSERT INTO test1 VALUES ('HEY2'), ('hey1');",
				Expected: []sql.Row{{sql.NewOkResult(2)}},
			},
			{
				Query:    "INSERT INTO test2 VALUES ('HEY2'), ('hey1');",
				Expected: []sql.Row{{sql.NewOkResult(2)}},
			},
			{
				Query:    "SELECT * FROM test1 ORDER BY 1;",
				Expected: []sql.Row{{"HEY2"}, {"hey1"}},
			},
			{
				Query:    "SELECT * FROM test2 ORDER BY 1;",
				Expected: []sql.Row{{"hey1"}, {"HEY2"}},
			},
		},
	},
	{
		Name: "Character set introducer with invalid collate",
		Queries: []CharsetCollationEngineTestQuery{
			{
				Query: "SELECT _utf16'\x00a' COLLATE utf8mb4_0900_bin;",
				Error: true,
			},
			{
				Query: "SELECT _utf16'\x00a' COLLATE binary;",
				Error: true,
			},
		},
	},
	{
		Name: "Order by behaves differently according to case-sensitivity",
		SetUpScript: []string{
			"CREATE TABLE test1 (pk BIGINT PRIMARY KEY, v1 VARCHAR(255) COLLATE utf16_unicode_ci, INDEX(v1));",
			"CREATE TABLE test2 (pk BIGINT PRIMARY KEY, v1 VARCHAR(255) COLLATE utf8mb4_0900_bin, INDEX(v1));",
			"INSERT INTO test1 VALUES (1, 'abc'), (2, 'ABC'), (3, 'aBc'), (4, 'AbC');",
			"INSERT INTO test2 VALUES (1, 'abc'), (2, 'ABC'), (3, 'aBc'), (4, 'AbC');",
		},
		Queries: []CharsetCollationEngineTestQuery{
			{
				Query: "SELECT v1, pk FROM test1 ORDER BY pk;",
				Expected: []sql.Row{
					{"abc", int64(1)}, {"ABC", int64(2)}, {"aBc", int64(3)}, {"AbC", int64(4)},
				},
			},
			{
				Query: "SELECT v1, pk FROM test1 ORDER BY v1, pk;",
				Expected: []sql.Row{
					{"abc", int64(1)}, {"ABC", int64(2)}, {"aBc", int64(3)}, {"AbC", int64(4)},
				},
			},
			{
				Query: "SELECT v1, pk FROM test2 ORDER BY pk;",
				Expected: []sql.Row{
					{"abc", int64(1)}, {"ABC", int64(2)}, {"aBc", int64(3)}, {"AbC", int64(4)},
				},
			},
			{
				Query: "SELECT v1, pk FROM test2 ORDER BY v1, pk;",
				Expected: []sql.Row{
					{"ABC", int64(2)}, {"AbC", int64(4)}, {"aBc", int64(3)}, {"abc", int64(1)},
				},
			},
		},
	},
	{
		Name: "Proper index access",
		SetUpScript: []string{
			"CREATE TABLE test1 (pk BIGINT PRIMARY KEY, v1 VARCHAR(255) COLLATE utf16_unicode_ci, INDEX(v1));",
			"CREATE TABLE test2 (pk BIGINT PRIMARY KEY, v1 VARCHAR(255) COLLATE utf8mb4_0900_bin, INDEX(v1));",
			"INSERT INTO test1 VALUES (1, 'abc'), (2, 'ABC'), (3, 'aBc'), (4, 'AbC');",
			"INSERT INTO test2 VALUES (1, 'abc'), (2, 'ABC'), (3, 'aBc'), (4, 'AbC');",
		},
		Queries: []CharsetCollationEngineTestQuery{
			{
				Query:    "SELECT v1, pk FROM test1 WHERE v1 > 'AbC' ORDER BY v1, pk;",
				Expected: []sql.Row(nil),
			},
			{
				Query: "SELECT v1, pk FROM test1 WHERE v1 >= 'AbC' ORDER BY v1, pk;",
				Expected: []sql.Row{
					{"abc", int64(1)}, {"ABC", int64(2)}, {"aBc", int64(3)}, {"AbC", int64(4)},
				},
			},
			{
				Query: "SELECT v1, pk FROM test1 WHERE v1 <= 'aBc' ORDER BY v1, pk;",
				Expected: []sql.Row{
					{"abc", int64(1)}, {"ABC", int64(2)}, {"aBc", int64(3)}, {"AbC", int64(4)},
				},
			},
			{
				Query: "SELECT v1, pk FROM test1 WHERE v1 = 'ABC' ORDER BY v1, pk;",
				Expected: []sql.Row{
					{"abc", int64(1)}, {"ABC", int64(2)}, {"aBc", int64(3)}, {"AbC", int64(4)},
				},
			},
			{
				Query: "SELECT v1, pk FROM test1 WHERE v1 BETWEEN 'ABC' AND 'AbC' ORDER BY v1, pk;",
				Expected: []sql.Row{
					{"abc", int64(1)}, {"ABC", int64(2)}, {"aBc", int64(3)}, {"AbC", int64(4)},
				},
			},
			{
				Query: "SELECT v1, pk FROM test1 WHERE v1 IN ('abc') ORDER BY v1, pk;",
				Expected: []sql.Row{
					{"abc", int64(1)}, {"ABC", int64(2)}, {"aBc", int64(3)}, {"AbC", int64(4)},
				},
			},
			{
				Query: "SELECT v1, pk FROM test2 WHERE v1 > 'AbC' ORDER BY v1, pk;",
				Expected: []sql.Row{
					{"aBc", int64(3)}, {"abc", int64(1)},
				},
			},
			{
				Query: "SELECT v1, pk FROM test2 WHERE v1 >= 'AbC' ORDER BY v1, pk;",
				Expected: []sql.Row{
					{"AbC", int64(4)}, {"aBc", int64(3)}, {"abc", int64(1)},
				},
			},
			{
				Query: "SELECT v1, pk FROM test2 WHERE v1 <= 'aBc' ORDER BY v1, pk;",
				Expected: []sql.Row{
					{"ABC", int64(2)}, {"AbC", int64(4)}, {"aBc", int64(3)},
				},
			},
			{
				Query: "SELECT v1, pk FROM test2 WHERE v1 = 'ABC' ORDER BY v1, pk;",
				Expected: []sql.Row{
					{"ABC", int64(2)},
				},
			},
			{
				Query: "SELECT v1, pk FROM test2 WHERE v1 BETWEEN 'ABC' AND 'AbC' ORDER BY v1, pk;",
				Expected: []sql.Row{
					{"ABC", int64(2)}, {"AbC", int64(4)},
				},
			},
			{
				Query: "SELECT v1, pk FROM test2 WHERE v1 IN ('abc') ORDER BY v1, pk;",
				Expected: []sql.Row{
					{"abc", int64(1)},
				},
			},
		},
	},
	{
		Name: "ENUM collation handling",
		SetUpScript: []string{
			"CREATE TABLE test1 (pk BIGINT PRIMARY KEY, v1 ENUM('abc','def','ghi') COLLATE utf16_unicode_ci);",
			"CREATE TABLE test2 (pk BIGINT PRIMARY KEY, v1 ENUM('abc','def','ghi') COLLATE utf8mb4_0900_bin);",
		},
		Queries: []CharsetCollationEngineTestQuery{
			{
				Query: "INSERT INTO test1 VALUES (1, 'ABC');",
				Expected: []sql.Row{
					{sql.NewOkResult(1)},
				},
			},
			{
				Query: "INSERT INTO test2 VALUES (1, 'ABC');",
				Error: true,
			},
			{
				Query: "INSERT INTO test1 VALUES (2, _utf16'\x00d\x00e\x00f' COLLATE utf16_unicode_ci);",
				Expected: []sql.Row{
					{sql.NewOkResult(1)},
				},
			},
			{
				Query: "INSERT INTO test2 VALUES (2, _utf16'\x00d\x00e\x00f' COLLATE utf16_unicode_ci);",
				Expected: []sql.Row{
					{sql.NewOkResult(1)},
				},
			},
			{
				Query: "SELECT * FROM test1 ORDER BY pk;",
				Expected: []sql.Row{
					{int64(1), uint16(1)}, {int64(2), uint16(2)},
				},
			},
			{
				Query: "SELECT * FROM test2 ORDER BY pk;",
				Expected: []sql.Row{
					{int64(2), uint16(2)},
				},
			},
		},
	},
	{
		Name: "SET collation handling",
		SetUpScript: []string{
			"CREATE TABLE test1 (pk BIGINT PRIMARY KEY, v1 SET('a','b','c') COLLATE utf16_unicode_ci);",
			"CREATE TABLE test2 (pk BIGINT PRIMARY KEY, v1 SET('a','b','c') COLLATE utf8mb4_0900_bin);",
		},
		Queries: []CharsetCollationEngineTestQuery{
			{
				Query: "INSERT INTO test1 VALUES (1, 'A');",
				Expected: []sql.Row{
					{sql.NewOkResult(1)},
				},
			},
			{
				Query: "INSERT INTO test2 VALUES (1, 'A');",
				Error: true,
			},
			{
				Query: "INSERT INTO test1 VALUES (2, _utf16'\x00b\x00,\x00c' COLLATE utf16_unicode_ci);",
				Expected: []sql.Row{
					{sql.NewOkResult(1)},
				},
			},
			{
				Query: "INSERT INTO test2 VALUES (2, _utf16'\x00b\x00,\x00c' COLLATE utf16_unicode_ci);",
				Expected: []sql.Row{
					{sql.NewOkResult(1)},
				},
			},
			{
				Query: "SELECT * FROM test1 ORDER BY pk;",
				Expected: []sql.Row{
					{int64(1), uint64(1)}, {int64(2), uint64(6)},
				},
			},
			{
				Query: "SELECT * FROM test2 ORDER BY pk;",
				Expected: []sql.Row{
					{int64(2), uint64(6)},
				},
			},
		},
	},
	{
		Name: "LENGTH() function",
		Queries: []CharsetCollationEngineTestQuery{
			{
				Query: "SELECT LENGTH(_utf16'\x00a\x00b\x00c' COLLATE utf16_unicode_ci);",
				Expected: []sql.Row{
					{int32(6)},
				},
			},
			{
				Query: "SELECT LENGTH(_utf8mb4'abc' COLLATE utf8mb4_0900_bin);",
				Expected: []sql.Row{
					{int32(3)},
				},
			},
			{
				Query: "SELECT LENGTH(_utf8mb4'\x00a\x00b\x00c' COLLATE utf8mb4_0900_bin);",
				Expected: []sql.Row{
					{int32(6)},
				},
			},
		},
	},
	{
		Name: "CHAR_LENGTH() function",
		Queries: []CharsetCollationEngineTestQuery{
			{
				Query: "SELECT CHAR_LENGTH(_utf16'\x00a\x00b\x00c' COLLATE utf16_unicode_ci);",
				Expected: []sql.Row{
					{int32(3)},
				},
			},
			{
				Query: "SELECT CHAR_LENGTH(_utf8mb4'abc' COLLATE utf8mb4_0900_bin);",
				Expected: []sql.Row{
					{int32(3)},
				},
			},
			{
				Query: "SELECT CHAR_LENGTH(_utf8mb4'\x00a\x00b\x00c' COLLATE utf8mb4_0900_bin);",
				Expected: []sql.Row{
					{int32(6)},
				},
			},
		},
	},
	{
		Name: "UPPER() function",
		Queries: []CharsetCollationEngineTestQuery{
			{
				Query: "SELECT UPPER(_utf16'\x00a\x00B\x00c' COLLATE utf16_unicode_ci);",
				Expected: []sql.Row{
					{"ABC"},
				},
			},
			{
				Query: "SELECT UPPER(_utf8mb4'aBc' COLLATE utf8mb4_0900_bin);",
				Expected: []sql.Row{
					{"ABC"},
				},
			},
			{
				Query: "SELECT UPPER(_utf8mb4'\x00a\x00B\x00c' COLLATE utf8mb4_0900_bin);",
				Expected: []sql.Row{
					{"\x00A\x00B\x00C"},
				},
			},
		},
	},
	{
		Name: "LOWER() function",
		Queries: []CharsetCollationEngineTestQuery{
			{
				Query: "SELECT LOWER(_utf16'\x00A\x00b\x00C' COLLATE utf16_unicode_ci);",
				Expected: []sql.Row{
					{"abc"},
				},
			},
			{
				Query: "SELECT LOWER(_utf8mb4'AbC' COLLATE utf8mb4_0900_bin);",
				Expected: []sql.Row{
					{"abc"},
				},
			},
			{
				Query: "SELECT LOWER(_utf8mb4'\x00A\x00b\x00C' COLLATE utf8mb4_0900_bin);",
				Expected: []sql.Row{
					{"\x00a\x00b\x00c"},
				},
			},
		},
	},
	{
		Name: "RPAD() function",
		Queries: []CharsetCollationEngineTestQuery{
			{
				Query: "SELECT RPAD(_utf16'\x00a\x00b\x00c' COLLATE utf16_unicode_ci, 6, 'z');",
				Expected: []sql.Row{
					{"abczzz"},
				},
			},
			{
				Query: "SELECT RPAD(_utf16'\x00a\x00b\x00c' COLLATE utf16_unicode_ci, 6, _utf8mb4'z' COLLATE utf8mb4_0900_bin);",
				Expected: []sql.Row{
					{"abczzz"},
				},
			},
			{
				Query: "SELECT RPAD(_utf16'\x00a\x00b\x00c' COLLATE utf16_unicode_ci, 6, _utf16'\x00z' COLLATE utf16_unicode_ci);",
				Expected: []sql.Row{
					{"abczzz"},
				},
			},
			{
				Query: "SELECT RPAD(_utf8mb4'abc' COLLATE utf8mb4_0900_bin, 6, _utf8mb4'z' COLLATE utf8mb4_0900_bin);",
				Expected: []sql.Row{
					{"abczzz"},
				},
			},
			{
				Query: "SELECT RPAD(_utf8mb4'abc' COLLATE utf8mb4_0900_bin, 6, _utf16'\x00z' COLLATE utf16_unicode_ci);",
				Expected: []sql.Row{
					{"abczzz"},
				},
			},
			{
				Query: "SELECT RPAD(_utf8mb4'\x00a\x00b\x00c' COLLATE utf8mb4_0900_bin, 6, _utf16'\x00z' COLLATE utf16_unicode_ci);",
				Expected: []sql.Row{
					{"\x00a\x00b\x00c"},
				},
			},
			{
				Query: "SELECT RPAD(_utf8mb4'\x00a\x00b\x00c' COLLATE utf8mb4_0900_bin, 9, _utf16'\x00z' COLLATE utf16_unicode_ci);",
				Expected: []sql.Row{
					{"\x00a\x00b\x00czzz"},
				},
			},
		},
	},
	{
		Name: "LPAD() function",
		Queries: []CharsetCollationEngineTestQuery{
			{
				Query: "SELECT LPAD(_utf16'\x00a\x00b\x00c' COLLATE utf16_unicode_ci, 6, 'z');",
				Expected: []sql.Row{
					{"zzzabc"},
				},
			},
			{
				Query: "SELECT LPAD(_utf16'\x00a\x00b\x00c' COLLATE utf16_unicode_ci, 6, _utf8mb4'z' COLLATE utf8mb4_0900_bin);",
				Expected: []sql.Row{
					{"zzzabc"},
				},
			},
			{
				Query: "SELECT LPAD(_utf16'\x00a\x00b\x00c' COLLATE utf16_unicode_ci, 6, _utf16'\x00z' COLLATE utf16_unicode_ci);",
				Expected: []sql.Row{
					{"zzzabc"},
				},
			},
			{
				Query: "SELECT LPAD(_utf8mb4'abc' COLLATE utf8mb4_0900_bin, 6, _utf8mb4'z' COLLATE utf8mb4_0900_bin);",
				Expected: []sql.Row{
					{"zzzabc"},
				},
			},
			{
				Query: "SELECT LPAD(_utf8mb4'abc' COLLATE utf8mb4_0900_bin, 6, _utf16'\x00z' COLLATE utf16_unicode_ci);",
				Expected: []sql.Row{
					{"zzzabc"},
				},
			},
			{
				Query: "SELECT LPAD(_utf8mb4'\x00a\x00b\x00c' COLLATE utf8mb4_0900_bin, 6, _utf16'\x00z' COLLATE utf16_unicode_ci);",
				Expected: []sql.Row{
					{"\x00a\x00b\x00c"},
				},
			},
			{
				Query: "SELECT LPAD(_utf8mb4'\x00a\x00b\x00c' COLLATE utf8mb4_0900_bin, 9, _utf16'\x00z' COLLATE utf16_unicode_ci);",
				Expected: []sql.Row{
					{"zzz\x00a\x00b\x00c"},
				},
			},
		},
	},
	{
		Name: "HEX() function",
		Queries: []CharsetCollationEngineTestQuery{
			{
				Query: "SELECT HEX(_utf16'\x00a\x00b\x00c' COLLATE utf16_unicode_ci);",
				Expected: []sql.Row{
					{"006100620063"},
				},
			},
			{
				Query: "SELECT HEX(_utf8mb4'abc' COLLATE utf8mb4_0900_bin);",
				Expected: []sql.Row{
					{"616263"},
				},
			},
			{
				Query: "SELECT HEX(_utf8mb4'\x00a\x00b\x00c' COLLATE utf8mb4_0900_bin);",
				Expected: []sql.Row{
					{"006100620063"},
				},
			},
		},
	},
	{
		Name: "UNHEX() function",
		Queries: []CharsetCollationEngineTestQuery{
			{
				Query: "SELECT UNHEX(_utf16'\x006\x001\x006\x002\x006\x003' COLLATE utf16_unicode_ci);",
				Expected: []sql.Row{
					{[]byte("abc")},
				},
			},
			{
				Query: "SELECT UNHEX(_utf8mb4'616263' COLLATE utf8mb4_0900_bin);",
				Expected: []sql.Row{
					{[]byte("abc")},
				},
			},
		},
	},
	{
		Name: "SUBSTRING() function",
		Queries: []CharsetCollationEngineTestQuery{
			{
				Query: "SELECT SUBSTRING(_utf16'\x00a\x00b\x00c\x00d' COLLATE utf16_unicode_ci, 2, 2);",
				Expected: []sql.Row{
					{"bc"},
				},
			},
			{
				Query: "SELECT SUBSTRING(_utf8mb4'abcd' COLLATE utf8mb4_0900_bin, 2, 2);",
				Expected: []sql.Row{
					{"bc"},
				},
			},
			{
				Query: "SELECT SUBSTRING(_utf8mb4'\x00a\x00b\x00c\x00d' COLLATE utf8mb4_0900_bin, 2, 2);",
				Expected: []sql.Row{
					{"a\x00"},
				},
			},
		},
	},
	{
		Name: "TO_BASE64() function",
		Queries: []CharsetCollationEngineTestQuery{
			{
				Query: "SELECT TO_BASE64(_utf16'\x00a\x00b\x00c' COLLATE utf16_unicode_ci);",
				Expected: []sql.Row{
					{"AGEAYgBj"},
				},
			},
			{
				Query: "SELECT TO_BASE64(_utf8mb4'abc' COLLATE utf8mb4_0900_bin);",
				Expected: []sql.Row{
					{"YWJj"},
				},
			},
			{
				Query: "SELECT TO_BASE64(_utf8mb4'\x00a\x00b\x00c' COLLATE utf8mb4_0900_bin);",
				Expected: []sql.Row{
					{"AGEAYgBj"},
				},
			},
		},
	},
	{
		Name: "FROM_BASE64() function",
		Queries: []CharsetCollationEngineTestQuery{
			{
				Query: "SELECT FROM_BASE64(_utf16'\x00Y\x00W\x00J\x00j' COLLATE utf16_unicode_ci);",
				Expected: []sql.Row{
					{[]byte("abc")},
				},
			},
			{
				Query: "SELECT FROM_BASE64(_utf8mb4'YWJj' COLLATE utf8mb4_0900_bin);",
				Expected: []sql.Row{
					{[]byte("abc")},
				},
			},
		},
	},
	{
		Name: "TRIM() function",
		Queries: []CharsetCollationEngineTestQuery{
			{
				Query: "SELECT TRIM(_utf16'\x00 \x00a\x00b\x00c\x00 ' COLLATE utf16_unicode_ci);",
				Expected: []sql.Row{
					{"abc"},
				},
			},
			{
				Query: "SELECT TRIM(_utf8mb4' abc ' COLLATE utf8mb4_0900_bin);",
				Expected: []sql.Row{
					{"abc"},
				},
			},
			{
				Query: "SELECT TRIM(_utf8mb4'\x00 \x00a\x00b\x00c\x00 ' COLLATE utf8mb4_0900_bin);",
				Expected: []sql.Row{
					{"\x00 \x00a\x00b\x00c\x00"},
				},
			},
		},
	},
	{
		Name: "RTRIM() function",
		Queries: []CharsetCollationEngineTestQuery{
			{
				Query: "SELECT RTRIM(_utf16'\x00 \x00a\x00b\x00c\x00 ' COLLATE utf16_unicode_ci);",
				Expected: []sql.Row{
					{" abc"},
				},
			},
			{
				Query: "SELECT RTRIM(_utf8mb4' abc ' COLLATE utf8mb4_0900_bin);",
				Expected: []sql.Row{
					{" abc"},
				},
			},
			{
				Query: "SELECT RTRIM(_utf8mb4'\x00 \x00a\x00b\x00c\x00 ' COLLATE utf8mb4_0900_bin);",
				Expected: []sql.Row{
					{"\x00 \x00a\x00b\x00c\x00"},
				},
			},
		},
	},
	{
		Name: "LTRIM() function",
		Queries: []CharsetCollationEngineTestQuery{
			{
				Query: "SELECT LTRIM(_utf16'\x00 \x00a\x00b\x00c\x00 ' COLLATE utf16_unicode_ci);",
				Expected: []sql.Row{
					{"abc "},
				},
			},
			{
				Query: "SELECT LTRIM(_utf8mb4' abc ' COLLATE utf8mb4_0900_bin);",
				Expected: []sql.Row{
					{"abc "},
				},
			},
			{
				Query: "SELECT LTRIM(_utf8mb4'\x00 \x00a\x00b\x00c\x00 ' COLLATE utf8mb4_0900_bin);",
				Expected: []sql.Row{
					{"\x00 \x00a\x00b\x00c\x00 "},
				},
			},
		},
	},
	{
		Name: "BINARY() function",
		Queries: []CharsetCollationEngineTestQuery{
			{
				Query: "SELECT BINARY(_utf16'\x00a\x00b\x00c' COLLATE utf16_unicode_ci);",
				Expected: []sql.Row{
					{[]byte("\x00a\x00b\x00c")},
				},
			},
			{
				Query: "SELECT BINARY(_utf8mb4'abc' COLLATE utf8mb4_0900_bin);",
				Expected: []sql.Row{
					{[]byte("abc")},
				},
			},
			{
				Query: "SELECT BINARY(_utf8mb4'\x00a\x00b\x00c' COLLATE utf8mb4_0900_bin);",
				Expected: []sql.Row{
					{[]byte("\x00a\x00b\x00c")},
				},
			},
		},
	},
	{
		Name: "CAST(... AS BINARY) function",
		Queries: []CharsetCollationEngineTestQuery{
			{
				Query: "SELECT CAST(_utf16'\x00a\x00b\x00c' COLLATE utf16_unicode_ci AS BINARY);",
				Expected: []sql.Row{
					{[]byte("\x00a\x00b\x00c")},
				},
			},
			{
				Query: "SELECT CAST(_utf8mb4'abc' COLLATE utf8mb4_0900_bin AS BINARY);",
				Expected: []sql.Row{
					{[]byte("abc")},
				},
			},
			{
				Query: "SELECT CAST(_utf8mb4'\x00a\x00b\x00c' COLLATE utf8mb4_0900_bin AS BINARY);",
				Expected: []sql.Row{
					{[]byte("\x00a\x00b\x00c")},
				},
			},
		},
	},
}
