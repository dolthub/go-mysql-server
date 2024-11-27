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

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// CharsetCollationWireTest is used to test character sets.
type CharsetCollationWireTest struct {
	Name        string
	SetUpScript []string
	Queries     []CharsetCollationWireTestQuery
}

// CharsetCollationWireTestQuery is a query within a CharsetCollationWireTest.
type CharsetCollationWireTestQuery struct {
	Query    string
	Expected []sql.UntypedSqlRow
	Error    bool
	// ExpectedCollations is an optional field, and when populated the test framework will assert that
	// the MySQL field metadata has these expected collation IDs.
	ExpectedCollations []sql.CollationID
}

// CharsetCollationWireTests are used to ensure that character sets and collations have the correct behavior over the
// wire. Return values should all have the table encoding, as it's returning the table's encoding type.
var CharsetCollationWireTests = []CharsetCollationWireTest{
	{
		Name: "Uppercase and lowercase collations",
		Queries: []CharsetCollationWireTestQuery{
			{
				Query:    "CREATE TABLE test1 (v1 VARCHAR(255) COLLATE utf16_unicode_ci, v2 VARCHAR(255) COLLATE UTF16_UNICODE_CI);",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query:    "CREATE TABLE test2 (v1 VARCHAR(255) CHARACTER SET utf16, v2 VARCHAR(255) CHARACTER SET UTF16);",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
		},
	},
	{
		Name: "Insert multiple character sets",
		SetUpScript: []string{
			"SET character_set_results = 'binary';",
			"CREATE TABLE test (v1 VARCHAR(255) COLLATE utf16_unicode_ci);",
		},
		Queries: []CharsetCollationWireTestQuery{
			{
				Query:    "INSERT INTO test VALUES ('hey');",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
			},
			{
				Query:    "INSERT INTO test VALUES (_utf16'\x00h\x00i');",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
			},
			{
				Query:    "INSERT INTO test VALUES (_utf8mb4'\x68\x65\x6c\x6c\x6f');",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
			},
			{
				Query:    "SELECT * FROM test ORDER BY 1;",
				Expected: []sql.UntypedSqlRow{{"\x00h\x00e\x00l\x00l\x00o"}, {"\x00h\x00e\x00y"}, {"\x00h\x00i"}},
			},
		},
	},
	{
		Name: "Sorting differences",
		SetUpScript: []string{
			"SET character_set_results = 'binary';",
			"CREATE TABLE test1 (v1 VARCHAR(255) COLLATE utf8mb4_0900_bin);",
			"CREATE TABLE test2 (v1 VARCHAR(255) COLLATE utf16_unicode_ci);",
		},
		Queries: []CharsetCollationWireTestQuery{
			{
				Query:    "INSERT INTO test1 VALUES ('HEY2'), ('hey1');",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(2)}},
			},
			{
				Query:    "INSERT INTO test2 VALUES ('HEY2'), ('hey1');",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(2)}},
			},
			{
				Query:    "SELECT * FROM test1 ORDER BY 1;",
				Expected: []sql.UntypedSqlRow{{"HEY2"}, {"hey1"}},
			},
			{
				Query:    "SELECT * FROM test2 ORDER BY 1;",
				Expected: []sql.UntypedSqlRow{{"\x00h\x00e\x00y\x001"}, {"\x00H\x00E\x00Y\x002"}},
			},
		},
	},
	{
		Name: "Order by behaves differently according to case-sensitivity",
		SetUpScript: []string{
			"SET character_set_results = 'binary';",
			"CREATE TABLE test1 (pk BIGINT PRIMARY KEY, v1 VARCHAR(255) COLLATE utf16_unicode_ci, INDEX(v1));",
			"CREATE TABLE test2 (pk BIGINT PRIMARY KEY, v1 VARCHAR(255) COLLATE utf8mb4_0900_bin, INDEX(v1));",
			"INSERT INTO test1 VALUES (1, 'abc'), (2, 'ABC'), (3, 'aBc'), (4, 'AbC');",
			"INSERT INTO test2 VALUES (1, 'abc'), (2, 'ABC'), (3, 'aBc'), (4, 'AbC');",
		},
		Queries: []CharsetCollationWireTestQuery{
			{
				Query: "SELECT v1, pk FROM test1 ORDER BY pk;",
				Expected: []sql.UntypedSqlRow{
					{"\x00a\x00b\x00c", "1"}, {"\x00A\x00B\x00C", "2"}, {"\x00a\x00B\x00c", "3"}, {"\x00A\x00b\x00C", "4"},
				},
			},
			{
				Query: "SELECT v1, pk FROM test1 ORDER BY v1, pk;",
				Expected: []sql.UntypedSqlRow{
					{"\x00a\x00b\x00c", "1"}, {"\x00A\x00B\x00C", "2"}, {"\x00a\x00B\x00c", "3"}, {"\x00A\x00b\x00C", "4"},
				},
			},
			{
				Query: "SELECT v1, pk FROM test2 ORDER BY pk;",
				Expected: []sql.UntypedSqlRow{
					{"abc", "1"}, {"ABC", "2"}, {"aBc", "3"}, {"AbC", "4"},
				},
			},
			{
				Query: "SELECT v1, pk FROM test2 ORDER BY v1, pk;",
				Expected: []sql.UntypedSqlRow{
					{"ABC", "2"}, {"AbC", "4"}, {"aBc", "3"}, {"abc", "1"},
				},
			},
		},
	},
	{
		Name: "Proper index access",
		SetUpScript: []string{
			"SET character_set_results = 'binary';",
			"CREATE TABLE test1 (pk BIGINT PRIMARY KEY, v1 VARCHAR(255) COLLATE utf16_unicode_ci, INDEX(v1));",
			"CREATE TABLE test2 (pk BIGINT PRIMARY KEY, v1 VARCHAR(255) COLLATE utf8mb4_0900_bin, INDEX(v1));",
			"INSERT INTO test1 VALUES (1, 'abc'), (2, 'ABC'), (3, 'aBc'), (4, 'AbC');",
			"INSERT INTO test2 VALUES (1, 'abc'), (2, 'ABC'), (3, 'aBc'), (4, 'AbC');",
		},
		Queries: []CharsetCollationWireTestQuery{
			{
				Query:    "SELECT v1, pk FROM test1 WHERE v1 > 'AbC' ORDER BY v1, pk;",
				Expected: []sql.UntypedSqlRow(nil),
			},
			{
				Query: "SELECT v1, pk FROM test1 WHERE v1 >= 'AbC' ORDER BY v1, pk;",
				Expected: []sql.UntypedSqlRow{
					{"\x00a\x00b\x00c", "1"}, {"\x00A\x00B\x00C", "2"}, {"\x00a\x00B\x00c", "3"}, {"\x00A\x00b\x00C", "4"},
				},
			},
			{
				Query: "SELECT v1, pk FROM test1 WHERE v1 <= 'aBc' ORDER BY v1, pk;",
				Expected: []sql.UntypedSqlRow{
					{"\x00a\x00b\x00c", "1"}, {"\x00A\x00B\x00C", "2"}, {"\x00a\x00B\x00c", "3"}, {"\x00A\x00b\x00C", "4"},
				},
			},
			{
				Query: "SELECT v1, pk FROM test1 WHERE v1 = 'ABC' ORDER BY v1, pk;",
				Expected: []sql.UntypedSqlRow{
					{"\x00a\x00b\x00c", "1"}, {"\x00A\x00B\x00C", "2"}, {"\x00a\x00B\x00c", "3"}, {"\x00A\x00b\x00C", "4"},
				},
			},
			{
				Query: "SELECT v1, pk FROM test1 WHERE v1 BETWEEN 'ABC' AND 'AbC' ORDER BY v1, pk;",
				Expected: []sql.UntypedSqlRow{
					{"\x00a\x00b\x00c", "1"}, {"\x00A\x00B\x00C", "2"}, {"\x00a\x00B\x00c", "3"}, {"\x00A\x00b\x00C", "4"},
				},
			},
			{
				Query: "SELECT v1, pk FROM test1 WHERE v1 IN ('abc') ORDER BY v1, pk;",
				Expected: []sql.UntypedSqlRow{
					{"\x00a\x00b\x00c", "1"}, {"\x00A\x00B\x00C", "2"}, {"\x00a\x00B\x00c", "3"}, {"\x00A\x00b\x00C", "4"},
				},
			},
			{
				Query: "SELECT v1, pk FROM test2 WHERE v1 > 'AbC' ORDER BY v1, pk;",
				Expected: []sql.UntypedSqlRow{
					{"aBc", "3"}, {"abc", "1"},
				},
			},
			{
				Query: "SELECT v1, pk FROM test2 WHERE v1 >= 'AbC' ORDER BY v1, pk;",
				Expected: []sql.UntypedSqlRow{
					{"AbC", "4"}, {"aBc", "3"}, {"abc", "1"},
				},
			},
			{
				Query: "SELECT v1, pk FROM test2 WHERE v1 <= 'aBc' ORDER BY v1, pk;",
				Expected: []sql.UntypedSqlRow{
					{"ABC", "2"}, {"AbC", "4"}, {"aBc", "3"},
				},
			},
			{
				Query: "SELECT v1, pk FROM test2 WHERE v1 = 'ABC' ORDER BY v1, pk;",
				Expected: []sql.UntypedSqlRow{
					{"ABC", "2"},
				},
			},
			{
				Query: "SELECT v1, pk FROM test2 WHERE v1 BETWEEN 'ABC' AND 'AbC' ORDER BY v1, pk;",
				Expected: []sql.UntypedSqlRow{
					{"ABC", "2"}, {"AbC", "4"},
				},
			},
			{
				Query: "SELECT v1, pk FROM test2 WHERE v1 IN ('abc') ORDER BY v1, pk;",
				Expected: []sql.UntypedSqlRow{
					{"abc", "1"},
				},
			},
		},
	},
	{
		Name: "SET NAMES does not interfere with column charset",
		SetUpScript: []string{
			"SET NAMES utf8mb3;",
			"CREATE TABLE test(pk BIGINT PRIMARY KEY, v1 VARCHAR(100) COLLATE utf8mb4_0900_bin);",
			"INSERT INTO test VALUES (1, 'a'), (2, 'b');",
		},
		Queries: []CharsetCollationWireTestQuery{
			{
				Query:    "SELECT * FROM test ORDER BY v1 COLLATE utf8mb4_bin ASC;",
				Expected: []sql.UntypedSqlRow{{"1", "a"}, {"2", "b"}},
			},
			{
				Query: "SELECT * FROM test ORDER BY v1 COLLATE utf8mb3_bin ASC;",
				Error: true,
			},
			{
				Query:    "SELECT 'a' COLLATE utf8mb3_bin;",
				Expected: []sql.UntypedSqlRow{{"a"}},
			},
			{
				Query: "SELECT 'a' COLLATE utf8mb4_bin;",
				Error: true,
			},
		},
	},
	{
		Name: "SET validates character set and collation variables",
		Queries: []CharsetCollationWireTestQuery{
			{
				Query: "SET character_set_client = 'does_not_exist';",
				Error: true,
			},
			{
				Query: "SET character_set_connection = 'invalid_charset';",
				Error: true,
			},
			{
				Query: "SET character_set_results = 'whoops';",
				Error: true,
			},
			{
				Query: "SET collation_connection = 'cant_be';",
				Error: true,
			},
			{
				Query: "SET collation_database = 'something_else';",
				Error: true,
			},
			{
				Query: "SET collation_server = 'why_try';",
				Error: true,
			},
			{
				Query: "SET NAMES outside_correct;",
				Error: true,
			},
		},
	},
	{
		Name: "Coercibility test using HEX",
		SetUpScript: []string{
			"SET NAMES utf8mb4;",
		},
		Queries: []CharsetCollationWireTestQuery{
			{
				Query:    "SELECT HEX(UNHEX('c0a80000')) = 'c0a80000'",
				Expected: []sql.UntypedSqlRow{{"1"}},
			},
			{
				Query:    "SET collation_connection = 'utf8mb4_0900_bin';",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query:    "SELECT HEX(UNHEX('c0a80000')) = 'c0a80000'",
				Expected: []sql.UntypedSqlRow{{"0"}},
			},
		},
	},
	{
		Name: "ENUM collation handling",
		SetUpScript: []string{
			"SET character_set_results = 'binary';",
			"CREATE TABLE test1 (pk BIGINT PRIMARY KEY, v1 ENUM('abc','def','ghi') COLLATE utf16_unicode_ci);",
			"CREATE TABLE test2 (pk BIGINT PRIMARY KEY, v1 ENUM('abc','def','ghi') COLLATE utf8mb4_0900_bin);",
		},
		Queries: []CharsetCollationWireTestQuery{
			{
				Query: "INSERT INTO test1 VALUES (1, 'ABC');",
				Expected: []sql.UntypedSqlRow{
					{types.NewOkResult(1)},
				},
			},
			{
				Query: "INSERT INTO test2 VALUES (1, 'ABC');",
				Error: true,
			},
			{
				Query: "INSERT INTO test1 VALUES (2, _utf16'\x00d\x00e\x00f' COLLATE utf16_unicode_ci);",
				Expected: []sql.UntypedSqlRow{
					{types.NewOkResult(1)},
				},
			},
			{
				Query: "INSERT INTO test2 VALUES (2, _utf16'\x00d\x00e\x00f' COLLATE utf16_unicode_ci);",
				Expected: []sql.UntypedSqlRow{
					{types.NewOkResult(1)},
				},
			},
			{
				Query: "SELECT * FROM test1 ORDER BY pk;",
				Expected: []sql.UntypedSqlRow{
					{"1", "\x00a\x00b\x00c"}, {"2", "\x00d\x00e\x00f"},
				},
			},
			{
				Query: "SELECT * FROM test2 ORDER BY pk;",
				Expected: []sql.UntypedSqlRow{
					{"2", "def"},
				},
			},
		},
	},
	{
		Name: "SET collation handling",
		SetUpScript: []string{
			"SET character_set_results = 'binary';",
			"CREATE TABLE test1 (pk BIGINT PRIMARY KEY, v1 SET('a','b','c') COLLATE utf16_unicode_ci);",
			"CREATE TABLE test2 (pk BIGINT PRIMARY KEY, v1 SET('a','b','c') COLLATE utf8mb4_0900_bin);",
		},
		Queries: []CharsetCollationWireTestQuery{
			{
				Query: "INSERT INTO test1 VALUES (1, 'A');",
				Expected: []sql.UntypedSqlRow{
					{types.NewOkResult(1)},
				},
			},
			{
				Query: "INSERT INTO test2 VALUES (1, 'A');",
				Error: true,
			},
			{
				Query: "INSERT INTO test1 VALUES (2, _utf16'\x00b\x00,\x00c' COLLATE utf16_unicode_ci);",
				Expected: []sql.UntypedSqlRow{
					{types.NewOkResult(1)},
				},
			},
			{
				Query: "INSERT INTO test2 VALUES (2, _utf16'\x00b\x00,\x00c' COLLATE utf16_unicode_ci);",
				Expected: []sql.UntypedSqlRow{
					{types.NewOkResult(1)},
				},
			},
			{
				Query: "SELECT * FROM test1 ORDER BY pk;",
				Expected: []sql.UntypedSqlRow{
					{"1", "\x00a"}, {"2", "\x00b\x00,\x00c"},
				},
			},
			{
				Query: "SELECT * FROM test2 ORDER BY pk;",
				Expected: []sql.UntypedSqlRow{
					{"2", "b,c"},
				},
			},
		},
	},
	{
		Name: "Correct behavior with `character_set_results`",
		SetUpScript: []string{
			"SET character_set_results = 'binary';",
			"CREATE TABLE test (v1 VARCHAR(255) COLLATE utf16_unicode_ci);",
			"INSERT INTO test VALUES (_utf8mb4'hey');",
		},
		Queries: []CharsetCollationWireTestQuery{
			{
				Query:              "SELECT * FROM test;",
				Expected:           []sql.UntypedSqlRow{{"\x00h\x00e\x00y"}},
				ExpectedCollations: []sql.CollationID{sql.Collation_binary},
			},
			{
				Query:    "SET character_set_results = 'utf8mb4';",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query:              "SELECT * FROM test;",
				Expected:           []sql.UntypedSqlRow{{"hey"}},
				ExpectedCollations: []sql.CollationID{sql.Collation_utf8mb4_0900_ai_ci},
			},
			{
				Query:    "SET character_set_results = 'utf32';",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query:              "SELECT * FROM test;",
				Expected:           []sql.UntypedSqlRow{{"\x00\x00\x00h\x00\x00\x00e\x00\x00\x00y"}},
				ExpectedCollations: []sql.CollationID{sql.Collation_utf32_general_ci},
			},
			{
				Query:    "SET character_set_results = NULL;",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query:              "SELECT * FROM test;",
				Expected:           []sql.UntypedSqlRow{{"\x00h\x00e\x00y"}},
				ExpectedCollations: []sql.CollationID{sql.Collation_utf16_general_ci},
			},
		},
	},
	{
		Name: "LIKE respects table collations",
		SetUpScript: []string{
			"SET NAMES utf8mb4;",
			"CREATE TABLE test(v1 VARCHAR(100) COLLATE utf8mb4_0900_bin, v2 VARCHAR(100) COLLATE utf8mb4_0900_ai_ci);",
			"INSERT INTO test VALUES ('abc', 'abc'), ('ABC', 'ABC');",
		},
		Queries: []CharsetCollationWireTestQuery{
			{
				Query: "SELECT COUNT(*) FROM test WHERE v1 LIKE 'ABC';",
				Expected: []sql.UntypedSqlRow{
					{"1"},
				},
			},
			{
				Query: "SELECT COUNT(*) FROM test WHERE v2 LIKE 'ABC';",
				Expected: []sql.UntypedSqlRow{
					{"2"},
				},
			},
			{
				Query: "SELECT COUNT(*) FROM test WHERE v1 LIKE 'A%';",
				Expected: []sql.UntypedSqlRow{
					{"1"},
				},
			},
			{
				Query: "SELECT COUNT(*) FROM test WHERE v2 LIKE 'A%';",
				Expected: []sql.UntypedSqlRow{
					{"2"},
				},
			},
			{
				Query: "SELECT COUNT(*) FROM test WHERE v1 LIKE '%C';",
				Expected: []sql.UntypedSqlRow{
					{"1"},
				},
			},
			{
				Query: "SELECT COUNT(*) FROM test WHERE v2 LIKE '%C';",
				Expected: []sql.UntypedSqlRow{
					{"2"},
				},
			},
			{
				Query:    "SET collation_connection = 'utf8mb4_0900_bin';",
				Expected: []sql.UntypedSqlRow{{}},
			},
			{
				Query: "SELECT COUNT(*) FROM test WHERE v1 LIKE 'ABC';",
				Expected: []sql.UntypedSqlRow{
					{"1"},
				},
			},
			{
				Query: "SELECT COUNT(*) FROM test WHERE v2 LIKE 'ABC';",
				Expected: []sql.UntypedSqlRow{
					{"2"},
				},
			},
			{
				Query: "SELECT COUNT(*) FROM test WHERE v1 LIKE 'ABC' COLLATE utf8mb4_0900_ai_ci;",
				Expected: []sql.UntypedSqlRow{
					{"2"},
				},
			},
		},
	},
	{
		Name: "LIKE respects connection collation",
		SetUpScript: []string{
			"SET NAMES utf8mb4;",
		},
		Queries: []CharsetCollationWireTestQuery{
			{
				Query: "SELECT 'abc' LIKE 'ABC';",
				Expected: []sql.UntypedSqlRow{
					{"1"},
				},
			},
			{
				Query: "SELECT 'abc' COLLATE utf8mb4_0900_bin LIKE 'ABC';",
				Expected: []sql.UntypedSqlRow{
					{"0"},
				},
			},
			{
				Query: "SELECT 'abc' LIKE 'ABC' COLLATE utf8mb4_0900_bin;",
				Expected: []sql.UntypedSqlRow{
					{"0"},
				},
			},
			{
				Query: "SELECT 'abc' COLLATE utf8mb4_0900_ai_ci LIKE 'ABC';",
				Expected: []sql.UntypedSqlRow{
					{"1"},
				},
			},
			{
				Query: "SELECT 'abc' LIKE 'ABC' COLLATE utf8mb4_0900_ai_ci;",
				Expected: []sql.UntypedSqlRow{
					{"1"},
				},
			},
			{
				Query:    "SET collation_connection = 'utf8mb4_0900_bin';",
				Expected: []sql.UntypedSqlRow{{}},
			},
			{
				Query: "SELECT 'abc' LIKE 'ABC';",
				Expected: []sql.UntypedSqlRow{
					{"0"},
				},
			},
			{
				Query: "SELECT 'abc' COLLATE utf8mb4_0900_ai_ci LIKE 'ABC';",
				Expected: []sql.UntypedSqlRow{
					{"1"},
				},
			},
			{
				Query: "SELECT 'abc' LIKE 'ABC' COLLATE utf8mb4_0900_ai_ci;",
				Expected: []sql.UntypedSqlRow{
					{"1"},
				},
			},
			{
				Query: "SELECT 'abc' COLLATE utf8mb4_0900_bin LIKE 'ABC';",
				Expected: []sql.UntypedSqlRow{
					{"0"},
				},
			},
			{
				Query: "SELECT 'abc' LIKE 'ABC' COLLATE utf8mb4_0900_bin;",
				Expected: []sql.UntypedSqlRow{
					{"0"},
				},
			},
			{
				Query: "SELECT _utf8mb4'abc' LIKE 'ABC';",
				Expected: []sql.UntypedSqlRow{
					{"0"},
				},
			},
			{
				Query: "SELECT 'abc' LIKE _utf8mb4'ABC';",
				Expected: []sql.UntypedSqlRow{
					{"0"},
				},
			},
		},
	},
	{
		Name: "STRCMP() function",
		Queries: []CharsetCollationWireTestQuery{
			// TODO: returning different results from MySQL
			/*{
				// collation with the lowest coercibility is used
				Query: "SELECT STRCMP(_utf8mb4'A' COLLATE utf8mb4_0900_ai_ci, 'a')",
				Expected: []sql.UntypedSqlRow{
					{"0"},
				},
			},
			{
				// same coercibility, both unicode
				Query:   "SELECT STRCMP(_utf8mb4'A' COLLATE utf8mb4_0900_ai_ci, _utf8mb4'a' COLLATE utf8mb4_0900_as_cs)",
				Error: true,
			},
			{
				// same coercibility, both not unicode
				Query: "SELECT STRCMP(_latin1'A' COLLATE latin1_general_ci, _latin1'a' COLLATE latin1_german1_ci)",
				Error: true,
			},*/
			{
				// same coercibility, one unicode and one not unicode
				Query: "SELECT STRCMP(_utf8mb4'A' COLLATE utf8mb4_0900_ai_ci, _latin1'b' COLLATE latin1_general_cs)",
				Expected: []sql.UntypedSqlRow{
					{"-1"},
				},
			},
		},
	},
	{
		Name: "LENGTH() function",
		Queries: []CharsetCollationWireTestQuery{
			{
				Query: "SELECT LENGTH(_utf16'\x00a\x00b\x00c' COLLATE utf16_unicode_ci);",
				Expected: []sql.UntypedSqlRow{
					{"6"},
				},
			},
			{
				Query: "SELECT LENGTH(_utf8mb4'abc' COLLATE utf8mb4_0900_bin);",
				Expected: []sql.UntypedSqlRow{
					{"3"},
				},
			},
			{
				Query: "SELECT LENGTH(_utf8mb4'\x00a\x00b\x00c' COLLATE utf8mb4_0900_bin);",
				Expected: []sql.UntypedSqlRow{
					{"6"},
				},
			},
		},
	},
	{
		Name: "CHAR_LENGTH() function",
		Queries: []CharsetCollationWireTestQuery{
			{
				Query: "SELECT CHAR_LENGTH(_utf16'\x00a\x00b\x00c' COLLATE utf16_unicode_ci);",
				Expected: []sql.UntypedSqlRow{
					{"3"},
				},
			},
			{
				Query: "SELECT CHAR_LENGTH(_utf8mb4'abc' COLLATE utf8mb4_0900_bin);",
				Expected: []sql.UntypedSqlRow{
					{"3"},
				},
			},
			{
				Query: "SELECT CHAR_LENGTH(_utf8mb4'\x00a\x00b\x00c' COLLATE utf8mb4_0900_bin);",
				Expected: []sql.UntypedSqlRow{
					{"6"},
				},
			},
		},
	},
	{
		Name: "UPPER() function",
		SetUpScript: []string{
			"SET character_set_results = 'binary';",
		},
		Queries: []CharsetCollationWireTestQuery{
			{
				Query: "SELECT UPPER(_utf16'\x00a\x00B\x00c' COLLATE utf16_unicode_ci);",
				Expected: []sql.UntypedSqlRow{
					{"\x00A\x00B\x00C"},
				},
			},
			{
				Query: "SELECT UPPER(_utf8mb4'aBc' COLLATE utf8mb4_0900_bin);",
				Expected: []sql.UntypedSqlRow{
					{"ABC"},
				},
			},
			{
				Query: "SELECT UPPER(_utf8mb4'\x00a\x00B\x00c' COLLATE utf8mb4_0900_bin);",
				Expected: []sql.UntypedSqlRow{
					{"\x00A\x00B\x00C"},
				},
			},
		},
	},
	{
		Name: "LOWER() function",
		SetUpScript: []string{
			"SET character_set_results = 'binary';",
		},
		Queries: []CharsetCollationWireTestQuery{
			{
				Query: "SELECT LOWER(_utf16'\x00A\x00b\x00C' COLLATE utf16_unicode_ci);",
				Expected: []sql.UntypedSqlRow{
					{"\x00a\x00b\x00c"},
				},
			},
			{
				Query: "SELECT LOWER(_utf8mb4'AbC' COLLATE utf8mb4_0900_bin);",
				Expected: []sql.UntypedSqlRow{
					{"abc"},
				},
			},
			{
				Query: "SELECT LOWER(_utf8mb4'\x00A\x00b\x00C' COLLATE utf8mb4_0900_bin);",
				Expected: []sql.UntypedSqlRow{
					{"\x00a\x00b\x00c"},
				},
			},
		},
	},
	{
		Name: "RPAD() function",
		Queries: []CharsetCollationWireTestQuery{
			{
				Query: "SELECT RPAD(_utf16'\x00a\x00b\x00c' COLLATE utf16_unicode_ci, 6, 'z');",
				Expected: []sql.UntypedSqlRow{
					{"abczzz"},
				},
			},
			{
				Query: "SELECT RPAD(_utf16'\x00a\x00b\x00c' COLLATE utf16_unicode_ci, 6, _utf8mb4'z' COLLATE utf8mb4_0900_bin);",
				Expected: []sql.UntypedSqlRow{
					{"abczzz"},
				},
			},
			{
				Query: "SELECT RPAD(_utf16'\x00a\x00b\x00c' COLLATE utf16_unicode_ci, 6, _utf16'\x00z' COLLATE utf16_unicode_ci);",
				Expected: []sql.UntypedSqlRow{
					{"abczzz"},
				},
			},
			{
				Query: "SELECT RPAD(_utf8mb4'abc' COLLATE utf8mb4_0900_bin, 6, _utf8mb4'z' COLLATE utf8mb4_0900_bin);",
				Expected: []sql.UntypedSqlRow{
					{"abczzz"},
				},
			},
			{
				Query: "SELECT RPAD(_utf8mb4'abc' COLLATE utf8mb4_0900_bin, 6, _utf16'\x00z' COLLATE utf16_unicode_ci);",
				Expected: []sql.UntypedSqlRow{
					{"abczzz"},
				},
			},
			{
				Query: "SELECT RPAD(_utf8mb4'\x00a\x00b\x00c' COLLATE utf8mb4_0900_bin, 6, _utf16'\x00z' COLLATE utf16_unicode_ci);",
				Expected: []sql.UntypedSqlRow{
					{"\x00a\x00b\x00c"},
				},
			},
			{
				Query: "SELECT RPAD(_utf8mb4'\x00a\x00b\x00c' COLLATE utf8mb4_0900_bin, 9, _utf16'\x00z' COLLATE utf16_unicode_ci);",
				Expected: []sql.UntypedSqlRow{
					{"\x00a\x00b\x00czzz"},
				},
			},
		},
	},
	{
		Name: "LPAD() function",
		Queries: []CharsetCollationWireTestQuery{
			{
				Query: "SELECT LPAD(_utf16'\x00a\x00b\x00c' COLLATE utf16_unicode_ci, 6, 'z');",
				Expected: []sql.UntypedSqlRow{
					{"zzzabc"},
				},
			},
			{
				Query: "SELECT LPAD(_utf16'\x00a\x00b\x00c' COLLATE utf16_unicode_ci, 6, _utf8mb4'z' COLLATE utf8mb4_0900_bin);",
				Expected: []sql.UntypedSqlRow{
					{"zzzabc"},
				},
			},
			{
				Query: "SELECT LPAD(_utf16'\x00a\x00b\x00c' COLLATE utf16_unicode_ci, 6, _utf16'\x00z' COLLATE utf16_unicode_ci);",
				Expected: []sql.UntypedSqlRow{
					{"zzzabc"},
				},
			},
			{
				Query: "SELECT LPAD(_utf8mb4'abc' COLLATE utf8mb4_0900_bin, 6, _utf8mb4'z' COLLATE utf8mb4_0900_bin);",
				Expected: []sql.UntypedSqlRow{
					{"zzzabc"},
				},
			},
			{
				Query: "SELECT LPAD(_utf8mb4'abc' COLLATE utf8mb4_0900_bin, 6, _utf16'\x00z' COLLATE utf16_unicode_ci);",
				Expected: []sql.UntypedSqlRow{
					{"zzzabc"},
				},
			},
			{
				Query: "SELECT LPAD(_utf8mb4'\x00a\x00b\x00c' COLLATE utf8mb4_0900_bin, 6, _utf16'\x00z' COLLATE utf16_unicode_ci);",
				Expected: []sql.UntypedSqlRow{
					{"\x00a\x00b\x00c"},
				},
			},
			{
				Query: "SELECT LPAD(_utf8mb4'\x00a\x00b\x00c' COLLATE utf8mb4_0900_bin, 9, _utf16'\x00z' COLLATE utf16_unicode_ci);",
				Expected: []sql.UntypedSqlRow{
					{"zzz\x00a\x00b\x00c"},
				},
			},
		},
	},
	{
		Name: "HEX() function",
		Queries: []CharsetCollationWireTestQuery{
			{
				Query: "SELECT HEX(_utf16'\x00a\x00b\x00c' COLLATE utf16_unicode_ci);",
				Expected: []sql.UntypedSqlRow{
					{"006100620063"},
				},
			},
			{
				Query: "SELECT HEX(_utf8mb4'abc' COLLATE utf8mb4_0900_bin);",
				Expected: []sql.UntypedSqlRow{
					{"616263"},
				},
			},
			{
				Query: "SELECT HEX(_utf8mb4'\x00a\x00b\x00c' COLLATE utf8mb4_0900_bin);",
				Expected: []sql.UntypedSqlRow{
					{"006100620063"},
				},
			},
		},
	},
	{
		Name: "UNHEX() function",
		Queries: []CharsetCollationWireTestQuery{
			{
				Query: "SELECT UNHEX(_utf16'\x006\x001\x006\x002\x006\x003' COLLATE utf16_unicode_ci);",
				Expected: []sql.UntypedSqlRow{
					{"abc"},
				},
			},
			{
				Query: "SELECT UNHEX(_utf8mb4'616263' COLLATE utf8mb4_0900_bin);",
				Expected: []sql.UntypedSqlRow{
					{"abc"},
				},
			},
		},
	},
	{
		Name: "SUBSTRING() function",
		SetUpScript: []string{
			"SET character_set_results = 'binary';",
		},
		Queries: []CharsetCollationWireTestQuery{
			{
				Query: "SELECT SUBSTRING(_utf16'\x00a\x00b\x00c\x00d' COLLATE utf16_unicode_ci, 2, 2);",
				Expected: []sql.UntypedSqlRow{
					{"\x00b\x00c"},
				},
			},
			{
				Query: "SELECT SUBSTRING(_utf8mb4'abcd' COLLATE utf8mb4_0900_bin, 2, 2);",
				Expected: []sql.UntypedSqlRow{
					{"bc"},
				},
			},
			{
				Query: "SELECT SUBSTRING(_utf8mb4'\x00a\x00b\x00c\x00d' COLLATE utf8mb4_0900_bin, 2, 2);",
				Expected: []sql.UntypedSqlRow{
					{"a\x00"},
				},
			},
		},
	},
	{
		Name: "TO_BASE64() function",
		Queries: []CharsetCollationWireTestQuery{
			{
				Query: "SELECT TO_BASE64(_utf16'\x00a\x00b\x00c' COLLATE utf16_unicode_ci);",
				Expected: []sql.UntypedSqlRow{
					{"AGEAYgBj"},
				},
			},
			{
				Query: "SELECT TO_BASE64(_utf8mb4'abc' COLLATE utf8mb4_0900_bin);",
				Expected: []sql.UntypedSqlRow{
					{"YWJj"},
				},
			},
			{
				Query: "SELECT TO_BASE64(_utf8mb4'\x00a\x00b\x00c' COLLATE utf8mb4_0900_bin);",
				Expected: []sql.UntypedSqlRow{
					{"AGEAYgBj"},
				},
			},
		},
	},
	{
		Name: "FROM_BASE64() function",
		Queries: []CharsetCollationWireTestQuery{
			{
				Query: "SELECT FROM_BASE64(_utf16'\x00Y\x00W\x00J\x00j' COLLATE utf16_unicode_ci);",
				Expected: []sql.UntypedSqlRow{
					{"abc"},
				},
			},
			{
				Query: "SELECT FROM_BASE64(_utf8mb4'YWJj' COLLATE utf8mb4_0900_bin);",
				Expected: []sql.UntypedSqlRow{
					{"abc"},
				},
			},
		},
	},
	{
		Name: "TRIM() function",
		SetUpScript: []string{
			"SET character_set_results = 'binary';",
		},
		Queries: []CharsetCollationWireTestQuery{
			{
				Query: "SELECT TRIM(_utf16'\x00 \x00a\x00b\x00c\x00 ' COLLATE utf16_unicode_ci);",
				Expected: []sql.UntypedSqlRow{
					{"\x00a\x00b\x00c"},
				},
			},
			{
				Query: "SELECT TRIM(_utf8mb4' abc ' COLLATE utf8mb4_0900_bin);",
				Expected: []sql.UntypedSqlRow{
					{"abc"},
				},
			},
			{
				Query: "SELECT TRIM(_utf8mb4'\x00 \x00a\x00b\x00c\x00 ' COLLATE utf8mb4_0900_bin);",
				Expected: []sql.UntypedSqlRow{
					{"\x00 \x00a\x00b\x00c\x00"},
				},
			},
		},
	},
	{
		Name: "RTRIM() function",
		SetUpScript: []string{
			"SET character_set_results = 'binary';",
		},
		Queries: []CharsetCollationWireTestQuery{
			{
				Query: "SELECT RTRIM(_utf16'\x00 \x00a\x00b\x00c\x00 ' COLLATE utf16_unicode_ci);",
				Expected: []sql.UntypedSqlRow{
					{"\x00 \x00a\x00b\x00c"},
				},
			},
			{
				Query: "SELECT RTRIM(_utf8mb4' abc ' COLLATE utf8mb4_0900_bin);",
				Expected: []sql.UntypedSqlRow{
					{" abc"},
				},
			},
			{
				Query: "SELECT RTRIM(_utf8mb4'\x00 \x00a\x00b\x00c\x00 ' COLLATE utf8mb4_0900_bin);",
				Expected: []sql.UntypedSqlRow{
					{"\x00 \x00a\x00b\x00c\x00"},
				},
			},
		},
	},
	{
		Name: "LTRIM() function",
		SetUpScript: []string{
			"SET character_set_results = 'binary';",
		},
		Queries: []CharsetCollationWireTestQuery{
			{
				Query: "SELECT LTRIM(_utf16'\x00 \x00a\x00b\x00c\x00 ' COLLATE utf16_unicode_ci);",
				Expected: []sql.UntypedSqlRow{
					{"\x00a\x00b\x00c\x00 "},
				},
			},
			{
				Query: "SELECT LTRIM(_utf8mb4' abc ' COLLATE utf8mb4_0900_bin);",
				Expected: []sql.UntypedSqlRow{
					{"abc "},
				},
			},
			{
				Query: "SELECT LTRIM(_utf8mb4'\x00 \x00a\x00b\x00c\x00 ' COLLATE utf8mb4_0900_bin);",
				Expected: []sql.UntypedSqlRow{
					{"\x00 \x00a\x00b\x00c\x00 "},
				},
			},
		},
	},
	{
		Name: "BINARY() function",
		Queries: []CharsetCollationWireTestQuery{
			{
				Query: "SELECT BINARY(_utf16'\x00a\x00b\x00c' COLLATE utf16_unicode_ci);",
				Expected: []sql.UntypedSqlRow{
					{"\x00a\x00b\x00c"},
				},
			},
			{
				Query: "SELECT BINARY(_utf8mb4'abc' COLLATE utf8mb4_0900_bin);",
				Expected: []sql.UntypedSqlRow{
					{"abc"},
				},
			},
			{
				Query: "SELECT BINARY(_utf8mb4'\x00a\x00b\x00c' COLLATE utf8mb4_0900_bin);",
				Expected: []sql.UntypedSqlRow{
					{"\x00a\x00b\x00c"},
				},
			},
		},
	},
	{
		Name: "CAST(... AS BINARY) function",
		Queries: []CharsetCollationWireTestQuery{
			{
				Query: "SELECT CAST(_utf16'\x00a\x00b\x00c' COLLATE utf16_unicode_ci AS BINARY);",
				Expected: []sql.UntypedSqlRow{
					{"\x00a\x00b\x00c"},
				},
			},
			{
				Query: "SELECT CAST(_utf8mb4'abc' COLLATE utf8mb4_0900_bin AS BINARY);",
				Expected: []sql.UntypedSqlRow{
					{"abc"},
				},
			},
			{
				Query: "SELECT CAST(_utf8mb4'\x00a\x00b\x00c' COLLATE utf8mb4_0900_bin AS BINARY);",
				Expected: []sql.UntypedSqlRow{
					{"\x00a\x00b\x00c"},
				},
			},
		},
	},
}

// DatabaseCollationWireTests are used to validate that CREATE DATABASE and ALTER DATABASE correctly handle having their
// character set and collations modified.
var DatabaseCollationWireTests = []CharsetCollationWireTest{
	{
		Name: "CREATE DATABASE default collation",
		SetUpScript: []string{
			"CREATE DATABASE test_db;",
		},
		Queries: []CharsetCollationWireTestQuery{
			{
				Query:    "USE test_db;",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query: "SELECT @@character_set_database, @@collation_database;",
				Expected: []sql.UntypedSqlRow{
					{"utf8mb4", "utf8mb4_0900_bin"},
				},
			},
			{
				Query:    "DROP DATABASE test_db;",
				Expected: []sql.UntypedSqlRow{},
			},
		},
	},
	{
		Name: "CREATE DATABASE set character set only",
		SetUpScript: []string{
			"CREATE DATABASE test_db CHARACTER SET utf8mb3;",
		},
		Queries: []CharsetCollationWireTestQuery{
			{
				Query:    "USE test_db;",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query: "SELECT @@character_set_database, @@collation_database;",
				Expected: []sql.UntypedSqlRow{
					{"utf8mb3", "utf8mb3_general_ci"},
				},
			},
			{
				Query:    "DROP DATABASE test_db;",
				Expected: []sql.UntypedSqlRow{},
			},
		},
	},
	{
		Name: "CREATE DATABASE set collation only",
		SetUpScript: []string{
			"CREATE DATABASE test_db_a COLLATE latin1_general_ci;",
			"CREATE DATABASE test_db_b COLLATE latin1_general_cs;",
		},
		Queries: []CharsetCollationWireTestQuery{
			{
				Query:    "USE test_db_a;",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query: "SELECT @@character_set_database, @@collation_database;",
				Expected: []sql.UntypedSqlRow{
					{"latin1", "latin1_general_ci"},
				},
			},
			{
				Query:    "USE test_db_b;",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query: "SELECT @@character_set_database, @@collation_database;",
				Expected: []sql.UntypedSqlRow{
					{"latin1", "latin1_general_cs"},
				},
			},
			{
				Query:    "DROP DATABASE test_db_a;",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "DROP DATABASE test_db_b;",
				Expected: []sql.UntypedSqlRow{},
			},
		},
	},
	{
		Name: "CREATE DATABASE set character set and collation",
		SetUpScript: []string{
			"CREATE DATABASE test_db CHARACTER SET utf8mb3 COLLATE utf8mb3_bin;",
		},
		Queries: []CharsetCollationWireTestQuery{
			{
				Query:    "USE test_db;",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query: "SELECT @@character_set_database, @@collation_database;",
				Expected: []sql.UntypedSqlRow{
					{"utf8mb3", "utf8mb3_bin"},
				},
			},
			{
				Query: "CREATE DATABASE invalid_db CHARACTER SET utf8mb4 COLLATE ascii_bin;",
				Error: true,
			},
			{
				Query:    "DROP DATABASE test_db;",
				Expected: []sql.UntypedSqlRow{},
			},
		},
	},
	{
		Name: "ALTER DATABASE requires character set or collation",
		SetUpScript: []string{
			"CREATE DATABASE test_db;",
		},
		Queries: []CharsetCollationWireTestQuery{
			{
				Query: "ALTER DATABASE test_db;",
				Error: true,
			},
			{
				Query:    "DROP DATABASE test_db;",
				Expected: []sql.UntypedSqlRow{},
			},
		},
	},
	{
		Name: "ALTER DATABASE set character set only",
		SetUpScript: []string{
			"CREATE DATABASE test_db;",
		},
		Queries: []CharsetCollationWireTestQuery{
			{
				Query:    "USE test_db;",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query: "SELECT @@character_set_database, @@collation_database;",
				Expected: []sql.UntypedSqlRow{
					{"utf8mb4", "utf8mb4_0900_bin"},
				},
			},
			{
				Query:    "ALTER DATABASE test_db CHARACTER SET utf8mb3;",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query: "SELECT @@character_set_database, @@collation_database;",
				Expected: []sql.UntypedSqlRow{
					{"utf8mb3", "utf8mb3_general_ci"},
				},
			},
			{
				Query:    "DROP DATABASE test_db;",
				Expected: []sql.UntypedSqlRow{},
			},
		},
	},
	{
		Name: "ALTER DATABASE set collation only",
		SetUpScript: []string{
			"CREATE DATABASE test_db_a COLLATE latin1_general_ci;",
			"CREATE DATABASE test_db_b COLLATE latin1_general_cs;",
		},
		Queries: []CharsetCollationWireTestQuery{
			{
				Query:    "USE test_db_a;",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query: "SELECT @@character_set_database, @@collation_database;",
				Expected: []sql.UntypedSqlRow{
					{"latin1", "latin1_general_ci"},
				},
			},
			{
				Query:    "USE test_db_b;",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query: "SELECT @@character_set_database, @@collation_database;",
				Expected: []sql.UntypedSqlRow{
					{"latin1", "latin1_general_cs"},
				},
			},
			{
				Query:    "ALTER DATABASE test_db_a COLLATE utf8mb3_bin;",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "ALTER DATABASE test_db_b COLLATE utf8mb3_general_ci;",
				Expected: []sql.UntypedSqlRow{},
			},
			{ // Still on test_db_b
				Query: "SELECT @@character_set_database, @@collation_database;",
				Expected: []sql.UntypedSqlRow{
					{"utf8mb3", "utf8mb3_general_ci"},
				},
			},
			{
				Query:    "USE test_db_a;",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query: "SELECT @@character_set_database, @@collation_database;",
				Expected: []sql.UntypedSqlRow{
					{"utf8mb3", "utf8mb3_bin"},
				},
			},
			{
				Query:    "DROP DATABASE test_db_a;",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "DROP DATABASE test_db_b;",
				Expected: []sql.UntypedSqlRow{},
			},
		},
	},
	{
		Name: "ALTER DATABASE set character set and collation",
		SetUpScript: []string{
			"CREATE DATABASE test_db CHARACTER SET utf8mb3 COLLATE utf8mb3_bin;",
		},
		Queries: []CharsetCollationWireTestQuery{
			{
				Query:    "USE test_db;",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query: "SELECT @@character_set_database, @@collation_database;",
				Expected: []sql.UntypedSqlRow{
					{"utf8mb3", "utf8mb3_bin"},
				},
			},
			{
				Query:    "ALTER DATABASE test_db CHARACTER SET ascii COLLATE ascii_bin;",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query: "SELECT @@character_set_database, @@collation_database;",
				Expected: []sql.UntypedSqlRow{
					{"ascii", "ascii_bin"},
				},
			},
			{
				Query:    "DROP DATABASE test_db;",
				Expected: []sql.UntypedSqlRow{},
			},
		},
	},
	{
		Name: "Tables inherit database collation",
		SetUpScript: []string{
			"CREATE DATABASE test_db COLLATE utf8mb3_bin;",
			"CREATE TABLE test_db.other (pk VARCHAR(20) PRIMARY KEY) COLLATE utf8mb3_unicode_ci;",
		},
		Queries: []CharsetCollationWireTestQuery{
			{
				Query:    "USE test_db;",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query: "CREATE TABLE test_a (pk VARCHAR(20) PRIMARY KEY);",
				Expected: []sql.UntypedSqlRow{
					{types.NewOkResult(0)},
				},
			},
			{ // LIKE should inherit the table's collation, NOT the database's collation
				Query: "CREATE TABLE test_b LIKE other;",
				Expected: []sql.UntypedSqlRow{
					{types.NewOkResult(0)},
				},
			},
			{ // AS SELECT should inherit the database's collation, but the column retains the original collation
				Query: "CREATE TABLE test_c AS SELECT * FROM other;",
				Expected: []sql.UntypedSqlRow{
					{types.NewOkResult(0)},
				},
			},
			{
				Query: "SHOW CREATE TABLE test_a;",
				Expected: []sql.UntypedSqlRow{
					{"test_a", "CREATE TABLE `test_a` (\n  `pk` varchar(20) NOT NULL,\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_bin"},
				},
			},
			{
				Query: "SHOW CREATE TABLE test_b;",
				Expected: []sql.UntypedSqlRow{
					{"test_b", "CREATE TABLE `test_b` (\n  `pk` varchar(20) NOT NULL,\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_unicode_ci"},
				},
			},
			{
				Query: "SHOW CREATE TABLE test_c;",
				Expected: []sql.UntypedSqlRow{
					{"test_c", "CREATE TABLE `test_c` (\n  `pk` varchar(20) COLLATE utf8mb3_unicode_ci NOT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_bin"},
				},
			},
			{
				Query:    "ALTER DATABASE test_db COLLATE utf8mb3_general_ci;",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query: "CREATE TABLE test_d (pk VARCHAR(20) PRIMARY KEY);",
				Expected: []sql.UntypedSqlRow{
					{types.NewOkResult(0)},
				},
			},
			{
				Query: "SHOW CREATE TABLE test_d;",
				Expected: []sql.UntypedSqlRow{
					{"test_d", "CREATE TABLE `test_d` (\n  `pk` varchar(20) NOT NULL,\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci"},
				},
			},
			{
				Query:    "DROP DATABASE test_db;",
				Expected: []sql.UntypedSqlRow{},
			},
		},
	},
	{
		Name: "INFORMATION_SCHEMA shows character set and collation",
		SetUpScript: []string{
			"CREATE DATABASE test_db_a COLLATE latin1_general_ci;",
			"CREATE DATABASE test_db_b COLLATE latin1_general_cs;",
		},
		Queries: []CharsetCollationWireTestQuery{
			{
				Query:    "USE test_db_a;",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query: "SELECT DEFAULT_CHARACTER_SET_NAME, DEFAULT_COLLATION_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = 'test_db_a';",
				Expected: []sql.UntypedSqlRow{
					{"latin1", "latin1_general_ci"},
				},
			},
			{
				Query: "SELECT DEFAULT_CHARACTER_SET_NAME, DEFAULT_COLLATION_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = 'test_db_b';",
				Expected: []sql.UntypedSqlRow{
					{"latin1", "latin1_general_cs"},
				},
			},
			{
				Query:    "ALTER DATABASE test_db_a COLLATE utf8mb3_general_ci;",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query: "SELECT DEFAULT_CHARACTER_SET_NAME, DEFAULT_COLLATION_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = 'test_db_a';",
				Expected: []sql.UntypedSqlRow{
					{"utf8mb3", "utf8mb3_general_ci"},
				},
			},
			{
				Query:    "DROP DATABASE test_db_a;",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "DROP DATABASE test_db_b;",
				Expected: []sql.UntypedSqlRow{},
			},
		},
	},
	{
		Name: "Issue #5482",
		Queries: []CharsetCollationWireTestQuery{
			{
				Query: `SELECT T.TABLE_NAME AS label, 'connection.table' as type, T.TABLE_SCHEMA AS 'schema',
T.TABLE_SCHEMA AS 'database', T.TABLE_CATALOG AS 'catalog',
0 AS isView FROM INFORMATION_SCHEMA.TABLES AS T WHERE T.TABLE_CATALOG = 'def' AND
                                                      UPPER(T.TABLE_TYPE) = 'BASE TABLE' ORDER BY T.TABLE_NAME;`,
				Expected: []sql.UntypedSqlRow(nil),
			},
		},
	},
}
