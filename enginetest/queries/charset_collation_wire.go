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

// CharsetCollationWireTest is used to test character sets.
type CharsetCollationWireTest struct {
	Name        string
	SetUpScript []string
	Queries     []CharsetCollationWireTestQuery
}

// CharsetCollationWireTestQuery is a query within a CharsetCollationWireTest.
type CharsetCollationWireTestQuery struct {
	Query    string
	Expected []sql.Row
	Error    bool
}

// CharsetCollationWireTests are used to ensure that character sets and collations have the correct behavior over the
// wire. Return values should all have the table encoding, as it's returning the table's encoding type.
var CharsetCollationWireTests = []CharsetCollationWireTest{
	{
		Name: "Insert multiple character sets",
		SetUpScript: []string{
			"CREATE TABLE test (v1 VARCHAR(255) COLLATE utf16_unicode_ci);",
		},
		Queries: []CharsetCollationWireTestQuery{
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
				Expected: []sql.Row{{"\x00h\x00e\x00l\x00l\x00o"}, {"\x00h\x00e\x00y"}, {"\x00h\x00i"}},
			},
		},
	},
	{
		Name: "Sorting differences",
		SetUpScript: []string{
			"CREATE TABLE test1 (v1 VARCHAR(255) COLLATE utf8mb4_0900_bin);",
			"CREATE TABLE test2 (v1 VARCHAR(255) COLLATE utf16_unicode_ci);",
		},
		Queries: []CharsetCollationWireTestQuery{
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
				Expected: []sql.Row{{"\x00h\x00e\x00y\x001"}, {"\x00H\x00E\x00Y\x002"}},
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
		Queries: []CharsetCollationWireTestQuery{
			{
				Query: "SELECT v1, pk FROM test1 ORDER BY pk;",
				Expected: []sql.Row{
					{"\x00a\x00b\x00c", "1"}, {"\x00A\x00B\x00C", "2"}, {"\x00a\x00B\x00c", "3"}, {"\x00A\x00b\x00C", "4"},
				},
			},
			{
				Query: "SELECT v1, pk FROM test1 ORDER BY v1, pk;",
				Expected: []sql.Row{
					{"\x00a\x00b\x00c", "1"}, {"\x00A\x00B\x00C", "2"}, {"\x00a\x00B\x00c", "3"}, {"\x00A\x00b\x00C", "4"},
				},
			},
			{
				Query: "SELECT v1, pk FROM test2 ORDER BY pk;",
				Expected: []sql.Row{
					{"abc", "1"}, {"ABC", "2"}, {"aBc", "3"}, {"AbC", "4"},
				},
			},
			{
				Query: "SELECT v1, pk FROM test2 ORDER BY v1, pk;",
				Expected: []sql.Row{
					{"ABC", "2"}, {"AbC", "4"}, {"aBc", "3"}, {"abc", "1"},
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
		Queries: []CharsetCollationWireTestQuery{
			{
				Query:    "SELECT v1, pk FROM test1 WHERE v1 > 'AbC' ORDER BY v1, pk;",
				Expected: []sql.Row(nil),
			},
			{
				Query: "SELECT v1, pk FROM test1 WHERE v1 >= 'AbC' ORDER BY v1, pk;",
				Expected: []sql.Row{
					{"\x00a\x00b\x00c", "1"}, {"\x00A\x00B\x00C", "2"}, {"\x00a\x00B\x00c", "3"}, {"\x00A\x00b\x00C", "4"},
				},
			},
			{
				Query: "SELECT v1, pk FROM test1 WHERE v1 <= 'aBc' ORDER BY v1, pk;",
				Expected: []sql.Row{
					{"\x00a\x00b\x00c", "1"}, {"\x00A\x00B\x00C", "2"}, {"\x00a\x00B\x00c", "3"}, {"\x00A\x00b\x00C", "4"},
				},
			},
			{
				Query: "SELECT v1, pk FROM test1 WHERE v1 = 'ABC' ORDER BY v1, pk;",
				Expected: []sql.Row{
					{"\x00a\x00b\x00c", "1"}, {"\x00A\x00B\x00C", "2"}, {"\x00a\x00B\x00c", "3"}, {"\x00A\x00b\x00C", "4"},
				},
			},
			{
				Query: "SELECT v1, pk FROM test1 WHERE v1 BETWEEN 'ABC' AND 'AbC' ORDER BY v1, pk;",
				Expected: []sql.Row{
					{"\x00a\x00b\x00c", "1"}, {"\x00A\x00B\x00C", "2"}, {"\x00a\x00B\x00c", "3"}, {"\x00A\x00b\x00C", "4"},
				},
			},
			{
				Query: "SELECT v1, pk FROM test1 WHERE v1 IN ('abc') ORDER BY v1, pk;",
				Expected: []sql.Row{
					{"\x00a\x00b\x00c", "1"}, {"\x00A\x00B\x00C", "2"}, {"\x00a\x00B\x00c", "3"}, {"\x00A\x00b\x00C", "4"},
				},
			},
			{
				Query: "SELECT v1, pk FROM test2 WHERE v1 > 'AbC' ORDER BY v1, pk;",
				Expected: []sql.Row{
					{"aBc", "3"}, {"abc", "1"},
				},
			},
			{
				Query: "SELECT v1, pk FROM test2 WHERE v1 >= 'AbC' ORDER BY v1, pk;",
				Expected: []sql.Row{
					{"AbC", "4"}, {"aBc", "3"}, {"abc", "1"},
				},
			},
			{
				Query: "SELECT v1, pk FROM test2 WHERE v1 <= 'aBc' ORDER BY v1, pk;",
				Expected: []sql.Row{
					{"ABC", "2"}, {"AbC", "4"}, {"aBc", "3"},
				},
			},
			{
				Query: "SELECT v1, pk FROM test2 WHERE v1 = 'ABC' ORDER BY v1, pk;",
				Expected: []sql.Row{
					{"ABC", "2"},
				},
			},
			{
				Query: "SELECT v1, pk FROM test2 WHERE v1 BETWEEN 'ABC' AND 'AbC' ORDER BY v1, pk;",
				Expected: []sql.Row{
					{"ABC", "2"}, {"AbC", "4"},
				},
			},
			{
				Query: "SELECT v1, pk FROM test2 WHERE v1 IN ('abc') ORDER BY v1, pk;",
				Expected: []sql.Row{
					{"abc", "1"},
				},
			},
		},
	},
}
