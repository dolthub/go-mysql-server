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
	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/go-mysql-server/sql"
)

// CharsetCollationEngineTest is used to test character sets.
type CharsetCollationEngineTest struct {
	Name        string
	SetUpScript []string
	Queries     []CharsetCollationEngineTestQuery
}

// CharsetCollationEngineTestQuery is a query within a CharsetCollationEngineTest. If `Error` is true but `ErrKind` is
// nil, then just tests that an error has occurred. If `ErrKind` is not nil, then tests that an error is returned and
// matches the stated kind (has higher precedence than the `Error` field). Only checks the `Expected` rows when both
// `Error` and `ErrKind` are nil.
type CharsetCollationEngineTestQuery struct {
	Query    string
	Expected []sql.UntypedSqlRow
	Error    bool
	ErrKind  *errors.Kind
}

// CharsetCollationEngineTests are used to ensure that character sets and collations have the correct behavior over the
// engine. Return values should all have the `utf8mb4` encoding, as it's returning the internal encoding type.
var CharsetCollationEngineTests = []CharsetCollationEngineTest{
	{
		Name: "Uppercase and lowercase collations",
		Queries: []CharsetCollationEngineTestQuery{
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
			"CREATE TABLE test (v1 VARCHAR(255) COLLATE utf16_unicode_ci);",
		},
		Queries: []CharsetCollationEngineTestQuery{
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
				Expected: []sql.UntypedSqlRow{{"hello"}, {"hey"}, {"hi"}},
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
				Expected: []sql.UntypedSqlRow{{"hey1"}, {"HEY2"}},
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
		Name: "Properly block using not-yet-implemented character sets/collations",
		Queries: []CharsetCollationEngineTestQuery{
			{
				Query:   "CREATE TABLE test1 (pk BIGINT PRIMARY KEY, v1 VARCHAR(255) CHARACTER SET utf16le);",
				ErrKind: sql.ErrCharSetNotYetImplementedTemp,
			},
			{
				Query:   "CREATE TABLE test2 (pk BIGINT PRIMARY KEY, v1 VARCHAR(255) COLLATE utf16le_general_ci);",
				ErrKind: sql.ErrCharSetNotYetImplementedTemp,
			},
			{
				Query:    "CREATE TABLE test3 (pk BIGINT PRIMARY KEY, v1 VARCHAR(255) CHARACTER SET utf8mb4);",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query:   "ALTER TABLE test3 MODIFY COLUMN v1 VARCHAR(255) COLLATE utf8mb4_sr_latn_0900_as_cs;",
				ErrKind: sql.ErrCollationNotYetImplementedTemp,
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
				Expected: []sql.UntypedSqlRow{
					{"abc", int64(1)}, {"ABC", int64(2)}, {"aBc", int64(3)}, {"AbC", int64(4)},
				},
			},
			{
				Query: "SELECT v1, pk FROM test1 ORDER BY v1, pk;",
				Expected: []sql.UntypedSqlRow{
					{"abc", int64(1)}, {"ABC", int64(2)}, {"aBc", int64(3)}, {"AbC", int64(4)},
				},
			},
			{
				Query: "SELECT v1, pk FROM test2 ORDER BY pk;",
				Expected: []sql.UntypedSqlRow{
					{"abc", int64(1)}, {"ABC", int64(2)}, {"aBc", int64(3)}, {"AbC", int64(4)},
				},
			},
			{
				Query: "SELECT v1, pk FROM test2 ORDER BY v1, pk;",
				Expected: []sql.UntypedSqlRow{
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
				Expected: []sql.UntypedSqlRow(nil),
			},
			{
				Query: "SELECT v1, pk FROM test1 WHERE v1 >= 'AbC' ORDER BY v1, pk;",
				Expected: []sql.UntypedSqlRow{
					{"abc", int64(1)}, {"ABC", int64(2)}, {"aBc", int64(3)}, {"AbC", int64(4)},
				},
			},
			{
				Query: "SELECT v1, pk FROM test1 WHERE v1 <= 'aBc' ORDER BY v1, pk;",
				Expected: []sql.UntypedSqlRow{
					{"abc", int64(1)}, {"ABC", int64(2)}, {"aBc", int64(3)}, {"AbC", int64(4)},
				},
			},
			{
				Query: "SELECT v1, pk FROM test1 WHERE v1 = 'ABC' ORDER BY v1, pk;",
				Expected: []sql.UntypedSqlRow{
					{"abc", int64(1)}, {"ABC", int64(2)}, {"aBc", int64(3)}, {"AbC", int64(4)},
				},
			},
			{
				Query: "SELECT v1, pk FROM test1 WHERE v1 BETWEEN 'ABC' AND 'AbC' ORDER BY v1, pk;",
				Expected: []sql.UntypedSqlRow{
					{"abc", int64(1)}, {"ABC", int64(2)}, {"aBc", int64(3)}, {"AbC", int64(4)},
				},
			},
			{
				Query: "SELECT v1, pk FROM test1 WHERE v1 IN ('abc') ORDER BY v1, pk;",
				Expected: []sql.UntypedSqlRow{
					{"abc", int64(1)}, {"ABC", int64(2)}, {"aBc", int64(3)}, {"AbC", int64(4)},
				},
			},
			{
				Query: "SELECT v1, pk FROM test2 WHERE v1 > 'AbC' ORDER BY v1, pk;",
				Expected: []sql.UntypedSqlRow{
					{"aBc", int64(3)}, {"abc", int64(1)},
				},
			},
			{
				Query: "SELECT v1, pk FROM test2 WHERE v1 >= 'AbC' ORDER BY v1, pk;",
				Expected: []sql.UntypedSqlRow{
					{"AbC", int64(4)}, {"aBc", int64(3)}, {"abc", int64(1)},
				},
			},
			{
				Query: "SELECT v1, pk FROM test2 WHERE v1 <= 'aBc' ORDER BY v1, pk;",
				Expected: []sql.UntypedSqlRow{
					{"ABC", int64(2)}, {"AbC", int64(4)}, {"aBc", int64(3)},
				},
			},
			{
				Query: "SELECT v1, pk FROM test2 WHERE v1 = 'ABC' ORDER BY v1, pk;",
				Expected: []sql.UntypedSqlRow{
					{"ABC", int64(2)},
				},
			},
			{
				Query: "SELECT v1, pk FROM test2 WHERE v1 BETWEEN 'ABC' AND 'AbC' ORDER BY v1, pk;",
				Expected: []sql.UntypedSqlRow{
					{"ABC", int64(2)}, {"AbC", int64(4)},
				},
			},
			{
				Query: "SELECT v1, pk FROM test2 WHERE v1 IN ('abc') ORDER BY v1, pk;",
				Expected: []sql.UntypedSqlRow{
					{"abc", int64(1)},
				},
			},
		},
	},
	{
		Name: "Table collation is respected",
		SetUpScript: []string{
			"CREATE TABLE test1 (pk BIGINT PRIMARY KEY, v1 VARCHAR(255)) COLLATE utf16_unicode_ci;",
			"CREATE TABLE test2 (pk BIGINT PRIMARY KEY, v1 VARCHAR(255)) COLLATE utf8mb4_unicode_ci;",
			"CREATE TABLE test3 LIKE test2;",
			"INSERT INTO test1 VALUES (1, 'abc'), (2, 'ABC'), (3, 'aBc'), (4, 'AbC');",
			"INSERT INTO test2 VALUES (1, 'abc'), (2, 'ABC'), (3, 'aBc'), (4, 'AbC');",
			"INSERT INTO test3 VALUES (1, 'abc'), (2, 'ABC'), (3, 'aBc'), (4, 'AbC');",
			"CREATE TABLE test4 AS SELECT * FROM test2;",
		},
		Queries: []CharsetCollationEngineTestQuery{
			{
				Query: "SELECT v1, pk FROM test1 WHERE v1 <= 'aBc' ORDER BY v1, pk;",
				Expected: []sql.UntypedSqlRow{
					{"abc", int64(1)}, {"ABC", int64(2)}, {"aBc", int64(3)}, {"AbC", int64(4)},
				},
			},
			{
				Query: "SELECT v1, pk FROM test2 WHERE v1 <= 'aBc' ORDER BY v1, pk;",
				Expected: []sql.UntypedSqlRow{
					{"abc", int64(1)}, {"ABC", int64(2)}, {"aBc", int64(3)}, {"AbC", int64(4)},
				},
			},
			{
				Query: "ALTER TABLE test2 MODIFY COLUMN v1 VARCHAR(100);",
				Expected: []sql.UntypedSqlRow{
					{types.NewOkResult(0)},
				},
			},
			{
				Query: "SELECT v1, pk FROM test2 WHERE v1 <= 'aBc' ORDER BY v1, pk;",
				Expected: []sql.UntypedSqlRow{
					{"abc", int64(1)}, {"ABC", int64(2)}, {"aBc", int64(3)}, {"AbC", int64(4)},
				},
			},
			{
				Query: "SELECT v1, pk FROM test3 WHERE v1 <= 'aBc' ORDER BY v1, pk;",
				Expected: []sql.UntypedSqlRow{
					{"abc", int64(1)}, {"ABC", int64(2)}, {"aBc", int64(3)}, {"AbC", int64(4)},
				},
			},
			{
				Query: "SELECT v1, pk FROM test4 WHERE v1 <= 'aBc' ORDER BY v1, pk;",
				Expected: []sql.UntypedSqlRow{
					{"abc", int64(1)}, {"ABC", int64(2)}, {"aBc", int64(3)}, {"AbC", int64(4)},
				},
			},
			{
				Query: "SHOW CREATE TABLE test1;",
				Expected: []sql.UntypedSqlRow{
					{"test1", "CREATE TABLE `test1` (\n  `pk` bigint NOT NULL,\n  `v1` varchar(255),\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf16 COLLATE=utf16_unicode_ci"},
				},
			},
			{
				Query: "SHOW CREATE TABLE test2;",
				Expected: []sql.UntypedSqlRow{
					{"test2", "CREATE TABLE `test2` (\n  `pk` bigint NOT NULL,\n  `v1` varchar(100),\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci"},
				},
			},
			{
				Query: "SHOW CREATE TABLE test3;",
				Expected: []sql.UntypedSqlRow{
					{"test3", "CREATE TABLE `test3` (\n  `pk` bigint NOT NULL,\n  `v1` varchar(255),\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci"},
				},
			},
			{
				Query: "SHOW CREATE TABLE test4;",
				Expected: []sql.UntypedSqlRow{
					{"test4", "CREATE TABLE `test4` (\n  `pk` bigint NOT NULL,\n  `v1` varchar(255) COLLATE utf8mb4_unicode_ci\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
			{
				Query: "ALTER TABLE test3 ADD COLUMN v2 VARCHAR(255);",
				Expected: []sql.UntypedSqlRow{
					{types.NewOkResult(0)},
				},
			},
			{
				Query: "SHOW CREATE TABLE test3;",
				Expected: []sql.UntypedSqlRow{
					{"test3", "CREATE TABLE `test3` (\n  `pk` bigint NOT NULL,\n  `v1` varchar(255),\n  `v2` varchar(255),\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci"},
				},
			},
			{
				Query: "ALTER TABLE test2 CHANGE COLUMN v1 v1 VARCHAR(220);",
				Expected: []sql.UntypedSqlRow{
					{types.NewOkResult(0)},
				},
			},
			{
				Query: "SHOW CREATE TABLE test2;",
				Expected: []sql.UntypedSqlRow{
					{"test2", "CREATE TABLE `test2` (\n  `pk` bigint NOT NULL,\n  `v1` varchar(220),\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci"},
				},
			},
			{
				Query: "ALTER TABLE test2 CHARACTER SET latin1 COLLATE utf8mb4_bin;",
				Error: true,
			},
			{
				Query: "ALTER TABLE test2 COLLATE utf8mb4_bin;",
				Expected: []sql.UntypedSqlRow{
					{types.NewOkResult(0)},
				},
			},
			{
				Query: "ALTER TABLE test2 ADD COLUMN v2 VARCHAR(255);",
				Expected: []sql.UntypedSqlRow{
					{types.NewOkResult(0)},
				},
			},
			{
				Query: "REPLACE INTO test2 VALUES (1, 'abc', 'abc'), (2, 'ABC', 'ABC'), (3, 'aBc', 'aBc'), (4, 'AbC', 'AbC');",
				Expected: []sql.UntypedSqlRow{
					{types.NewOkResult(8)},
				},
			},
			{
				Query: "SELECT v1, pk FROM test2 WHERE v1 <= 'aBc' ORDER BY v1, pk;",
				Expected: []sql.UntypedSqlRow{
					{"abc", int64(1)}, {"ABC", int64(2)}, {"aBc", int64(3)}, {"AbC", int64(4)},
				},
			},
			{
				Query: "SELECT v2, pk FROM test2 WHERE v2 <= 'aBc' ORDER BY v2, pk;",
				Expected: []sql.UntypedSqlRow{
					{"ABC", int64(2)}, {"AbC", int64(4)}, {"aBc", int64(3)},
				},
			},
			{
				Query: "SHOW CREATE TABLE test2;",
				Expected: []sql.UntypedSqlRow{
					{"test2", "CREATE TABLE `test2` (\n  `pk` bigint NOT NULL,\n  `v1` varchar(220) COLLATE utf8mb4_unicode_ci,\n  `v2` varchar(255),\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"},
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
		Queries: []CharsetCollationEngineTestQuery{
			{
				Query:    "SELECT * FROM test ORDER BY v1 COLLATE utf8mb4_bin ASC;",
				Expected: []sql.UntypedSqlRow{{int64(1), "a"}, {int64(2), "b"}},
			},
			{
				Query:   "SELECT * FROM test ORDER BY v1 COLLATE utf8mb3_bin ASC;",
				ErrKind: sql.ErrCollationInvalidForCharSet,
			},
			{
				Query:    "SELECT 'a' COLLATE utf8mb3_bin;",
				Expected: []sql.UntypedSqlRow{{"a"}},
			},
			{
				Query:   "SELECT 'a' COLLATE utf8mb4_bin;",
				ErrKind: sql.ErrCollationInvalidForCharSet,
			},
		},
	},
	{
		Name: "SET validates character set and collation variables",
		Queries: []CharsetCollationEngineTestQuery{
			{
				Query:   "SET character_set_client = 'am_i_wrong';",
				ErrKind: sql.ErrCharSetUnknown,
			},
			{
				Query:   "SET character_set_connection = 'to_believe';",
				ErrKind: sql.ErrCharSetUnknown,
			},
			{
				Query:   "SET character_set_results = 'in_crusty_cheese';",
				ErrKind: sql.ErrCharSetUnknown,
			},
			{
				Query:   "SET collation_connection = 'is_it_wrong';",
				ErrKind: sql.ErrCollationUnknown,
			},
			{
				Query:   "SET collation_database = 'to_believe';",
				ErrKind: sql.ErrCollationUnknown,
			},
			{
				Query:   "SET collation_server = 'in_deez';",
				ErrKind: sql.ErrCollationUnknown,
			},
			{
				Query:   "SET NAMES things;",
				ErrKind: sql.ErrCharSetUnknown,
			},
		},
	},
	{
		Name: "setting charset/collation sets the other",
		Queries: []CharsetCollationEngineTestQuery{
			{
				Query: "select @@session.character_set_connection, @@session.collation_connection;",
				Expected: []sql.UntypedSqlRow{
					{"utf8mb4", "utf8mb4_0900_bin"},
				},
			},
			{
				Query:    "set @@session.character_set_connection = 'latin1';",
				Expected: []sql.UntypedSqlRow{{}},
			},
			{
				Query: "select @@session.character_set_connection, @@session.collation_connection;",
				Expected: []sql.UntypedSqlRow{
					{"latin1", "latin1_swedish_ci"},
				},
			},
			{
				Query:    "set @@session.collation_connection = 'utf8mb4_0900_bin';",
				Expected: []sql.UntypedSqlRow{{}},
			},
			{
				Query: "select @@session.character_set_connection, @@session.collation_connection;",
				Expected: []sql.UntypedSqlRow{
					{"utf8mb4", "utf8mb4_0900_bin"},
				},
			},

			{
				Query: "select @@global.character_set_connection, @@global.collation_connection;",
				Expected: []sql.UntypedSqlRow{
					{"utf8mb4", "utf8mb4_0900_bin"},
				},
			},
			{
				Query:    "set @@global.character_set_connection = 'latin1';",
				Expected: []sql.UntypedSqlRow{{}},
			},
			{
				Query: "select @@global.character_set_connection, @@global.collation_connection;",
				Expected: []sql.UntypedSqlRow{
					{"latin1", "latin1_swedish_ci"},
				},
			},
			{
				Query:    "set @@global.collation_connection = 'utf8mb4_0900_bin';",
				Expected: []sql.UntypedSqlRow{{}},
			},
			{
				Query: "select @@global.character_set_connection, @@global.collation_connection;",
				Expected: []sql.UntypedSqlRow{
					{"utf8mb4", "utf8mb4_0900_bin"},
				},
			},

			{
				Query: "select @@session.character_set_server, @@session.collation_server;",
				Expected: []sql.UntypedSqlRow{
					{"utf8mb4", "utf8mb4_0900_bin"},
				},
			},
			{
				Query:    "set @@session.character_set_server = 'latin1';",
				Expected: []sql.UntypedSqlRow{{}},
			},
			{
				Query: "select @@session.character_set_server, @@session.collation_server;",
				Expected: []sql.UntypedSqlRow{
					{"latin1", "latin1_swedish_ci"},
				},
			},
			{
				Query:    "set @@session.collation_server = 'utf8mb4_0900_bin';",
				Expected: []sql.UntypedSqlRow{{}},
			},
			{
				Query: "select @@session.character_set_server, @@session.collation_server;",
				Expected: []sql.UntypedSqlRow{
					{"utf8mb4", "utf8mb4_0900_bin"},
				},
			},

			{
				Query: "select @@global.character_set_server, @@global.collation_server;",
				Expected: []sql.UntypedSqlRow{
					{"utf8mb4", "utf8mb4_0900_bin"},
				},
			},
			{
				Query:    "set @@global.character_set_server = 'latin1';",
				Expected: []sql.UntypedSqlRow{{}},
			},
			{
				Query: "select @@global.character_set_server, @@global.collation_server;",
				Expected: []sql.UntypedSqlRow{
					{"latin1", "latin1_swedish_ci"},
				},
			},
			{
				Query:    "set @@global.collation_server = 'utf8mb4_0900_bin';",
				Expected: []sql.UntypedSqlRow{{}},
			},
			{
				Query: "select @@global.character_set_server, @@global.collation_server;",
				Expected: []sql.UntypedSqlRow{
					{"utf8mb4", "utf8mb4_0900_bin"},
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
					{int64(1), uint16(1)}, {int64(2), uint16(2)},
				},
			},
			{
				Query: "SELECT * FROM test2 ORDER BY pk;",
				Expected: []sql.UntypedSqlRow{
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
					{int64(1), uint64(1)}, {int64(2), uint64(6)},
				},
			},
			{
				Query: "SELECT * FROM test2 ORDER BY pk;",
				Expected: []sql.UntypedSqlRow{
					{int64(2), uint64(6)},
				},
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
		Queries: []CharsetCollationEngineTestQuery{
			{
				Query: "SELECT COUNT(*) FROM test WHERE v1 LIKE 'ABC';",
				Expected: []sql.UntypedSqlRow{
					{int64(1)},
				},
			},
			{
				Query: "SELECT COUNT(*) FROM test WHERE v2 LIKE 'ABC';",
				Expected: []sql.UntypedSqlRow{
					{int64(2)},
				},
			},
			{
				Query: "SELECT COUNT(*) FROM test WHERE v1 LIKE 'A%';",
				Expected: []sql.UntypedSqlRow{
					{int64(1)},
				},
			},
			{
				Query: "SELECT COUNT(*) FROM test WHERE v2 LIKE 'A%';",
				Expected: []sql.UntypedSqlRow{
					{int64(2)},
				},
			},
			{
				Query: "SELECT COUNT(*) FROM test WHERE v1 LIKE '%C';",
				Expected: []sql.UntypedSqlRow{
					{int64(1)},
				},
			},
			{
				Query: "SELECT COUNT(*) FROM test WHERE v2 LIKE '%C';",
				Expected: []sql.UntypedSqlRow{
					{int64(2)},
				},
			},
			{
				Query:    "SET collation_connection = 'utf8mb4_0900_bin';",
				Expected: []sql.UntypedSqlRow{{}},
			},
			{
				Query: "SELECT COUNT(*) FROM test WHERE v1 LIKE 'ABC';",
				Expected: []sql.UntypedSqlRow{
					{int64(1)},
				},
			},
			{
				Query: "SELECT COUNT(*) FROM test WHERE v2 LIKE 'ABC';",
				Expected: []sql.UntypedSqlRow{
					{int64(2)},
				},
			},
			{
				Query: "SELECT COUNT(*) FROM test WHERE v1 LIKE 'ABC' COLLATE utf8mb4_0900_ai_ci;",
				Expected: []sql.UntypedSqlRow{
					{int64(2)},
				},
			},
		},
	},
	{
		Name: "LIKE respects connection collation",
		SetUpScript: []string{
			"SET NAMES utf8mb4;",
		},
		Queries: []CharsetCollationEngineTestQuery{
			{
				Query: "SELECT 'abc' LIKE 'ABC';",
				Expected: []sql.UntypedSqlRow{
					{true},
				},
			},
			{
				Query: "SELECT 'abc' COLLATE utf8mb4_0900_bin LIKE 'ABC';",
				Expected: []sql.UntypedSqlRow{
					{false},
				},
			},
			{
				Query: "SELECT 'abc' LIKE 'ABC' COLLATE utf8mb4_0900_bin;",
				Expected: []sql.UntypedSqlRow{
					{false},
				},
			},
			{
				Query: "SELECT 'abc' COLLATE utf8mb4_0900_ai_ci LIKE 'ABC';",
				Expected: []sql.UntypedSqlRow{
					{true},
				},
			},
			{
				Query: "SELECT 'abc' LIKE 'ABC' COLLATE utf8mb4_0900_ai_ci;",
				Expected: []sql.UntypedSqlRow{
					{true},
				},
			},
			{
				Query:    "SET collation_connection = 'utf8mb4_0900_bin';",
				Expected: []sql.UntypedSqlRow{{}},
			},
			{
				Query: "SELECT 'abc' LIKE 'ABC';",
				Expected: []sql.UntypedSqlRow{
					{false},
				},
			},
			{
				Query: "SELECT 'abc' COLLATE utf8mb4_0900_ai_ci LIKE 'ABC';",
				Expected: []sql.UntypedSqlRow{
					{true},
				},
			},
			{
				Query: "SELECT 'abc' LIKE 'ABC' COLLATE utf8mb4_0900_ai_ci;",
				Expected: []sql.UntypedSqlRow{
					{true},
				},
			},
			{
				Query: "SELECT 'abc' COLLATE utf8mb4_0900_bin LIKE 'ABC';",
				Expected: []sql.UntypedSqlRow{
					{false},
				},
			},
			{
				Query: "SELECT 'abc' LIKE 'ABC' COLLATE utf8mb4_0900_bin;",
				Expected: []sql.UntypedSqlRow{
					{false},
				},
			},
			{
				Query: "SELECT _utf8mb4'abc' LIKE 'ABC';",
				Expected: []sql.UntypedSqlRow{
					{false},
				},
			},
			{
				Query: "SELECT 'abc' LIKE _utf8mb4'ABC';",
				Expected: []sql.UntypedSqlRow{
					{false},
				},
			},
		},
	},
	{
		Name: "STRCMP() function",
		Queries: []CharsetCollationEngineTestQuery{
			// TODO: returning different results from MySQL
			/*{
				// collation with the lowest coercibility is used
				Query: "SELECT STRCMP(_utf8mb4'A' COLLATE utf8mb4_0900_ai_ci, 'a')",
				Expected: []sql.UntypedSqlRow{
					{int(0)},
				},
			},
			{
				// same coercibility, both unicode
				Query:   "SELECT STRCMP(_utf8mb4'A' COLLATE utf8mb4_0900_ai_ci, _utf8mb4'a' COLLATE utf8mb4_0900_as_cs)",
				ErrKind: sql.ErrCollationIllegalMix,
			},
			{
				// same coercibility, both not unicode
				Query:   "SELECT STRCMP(_latin1'A' COLLATE latin1_general_ci, _latin1'a' COLLATE latin1_german1_ci)",
				ErrKind: sql.ErrCollationIllegalMix,
			},*/
			{
				// same coercibility, one unicode and one not unicode
				Query: "SELECT STRCMP(_utf8mb4'A' COLLATE utf8mb4_0900_ai_ci, _latin1'b' COLLATE latin1_general_cs)",
				Expected: []sql.UntypedSqlRow{
					{int(-1)},
				},
			},
		},
	},
	{
		Name: "LENGTH() function",
		Queries: []CharsetCollationEngineTestQuery{
			{
				Query: "SELECT LENGTH(_utf16'\x00a\x00b\x00c' COLLATE utf16_unicode_ci);",
				Expected: []sql.UntypedSqlRow{
					{int32(6)},
				},
			},
			{
				Query: "SELECT LENGTH(_utf8mb4'abc' COLLATE utf8mb4_0900_bin);",
				Expected: []sql.UntypedSqlRow{
					{int32(3)},
				},
			},
			{
				Query: "SELECT LENGTH(_utf8mb4'\x00a\x00b\x00c' COLLATE utf8mb4_0900_bin);",
				Expected: []sql.UntypedSqlRow{
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
				Expected: []sql.UntypedSqlRow{
					{int32(3)},
				},
			},
			{
				Query: "SELECT CHAR_LENGTH(_utf8mb4'abc' COLLATE utf8mb4_0900_bin);",
				Expected: []sql.UntypedSqlRow{
					{int32(3)},
				},
			},
			{
				Query: "SELECT CHAR_LENGTH(_utf8mb4'\x00a\x00b\x00c' COLLATE utf8mb4_0900_bin);",
				Expected: []sql.UntypedSqlRow{
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
				Expected: []sql.UntypedSqlRow{
					{"ABC"},
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
		Queries: []CharsetCollationEngineTestQuery{
			{
				Query: "SELECT LOWER(_utf16'\x00A\x00b\x00C' COLLATE utf16_unicode_ci);",
				Expected: []sql.UntypedSqlRow{
					{"abc"},
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
		Queries: []CharsetCollationEngineTestQuery{
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
		Queries: []CharsetCollationEngineTestQuery{
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
		Queries: []CharsetCollationEngineTestQuery{
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
		Queries: []CharsetCollationEngineTestQuery{
			{
				Query: "SELECT UNHEX(_utf16'\x006\x001\x006\x002\x006\x003' COLLATE utf16_unicode_ci);",
				Expected: []sql.UntypedSqlRow{
					{[]byte("abc")},
				},
			},
			{
				Query: "SELECT UNHEX(_utf8mb4'616263' COLLATE utf8mb4_0900_bin);",
				Expected: []sql.UntypedSqlRow{
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
				Expected: []sql.UntypedSqlRow{
					{"bc"},
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
		Queries: []CharsetCollationEngineTestQuery{
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
		Queries: []CharsetCollationEngineTestQuery{
			{
				Query: "SELECT FROM_BASE64(_utf16'\x00Y\x00W\x00J\x00j' COLLATE utf16_unicode_ci);",
				Expected: []sql.UntypedSqlRow{
					{[]byte("abc")},
				},
			},
			{
				Query: "SELECT FROM_BASE64(_utf8mb4'YWJj' COLLATE utf8mb4_0900_bin);",
				Expected: []sql.UntypedSqlRow{
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
				Expected: []sql.UntypedSqlRow{
					{"abc"},
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
		Queries: []CharsetCollationEngineTestQuery{
			{
				Query: "SELECT RTRIM(_utf16'\x00 \x00a\x00b\x00c\x00 ' COLLATE utf16_unicode_ci);",
				Expected: []sql.UntypedSqlRow{
					{" abc"},
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
		Queries: []CharsetCollationEngineTestQuery{
			{
				Query: "SELECT LTRIM(_utf16'\x00 \x00a\x00b\x00c\x00 ' COLLATE utf16_unicode_ci);",
				Expected: []sql.UntypedSqlRow{
					{"abc "},
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
		Queries: []CharsetCollationEngineTestQuery{
			{
				Query: "SELECT BINARY(_utf16'\x00a\x00b\x00c' COLLATE utf16_unicode_ci);",
				Expected: []sql.UntypedSqlRow{
					{[]byte("\x00a\x00b\x00c")},
				},
			},
			{
				Query: "SELECT BINARY(_utf8mb4'abc' COLLATE utf8mb4_0900_bin);",
				Expected: []sql.UntypedSqlRow{
					{[]byte("abc")},
				},
			},
			{
				Query: "SELECT BINARY(_utf8mb4'\x00a\x00b\x00c' COLLATE utf8mb4_0900_bin);",
				Expected: []sql.UntypedSqlRow{
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
				Expected: []sql.UntypedSqlRow{
					{[]byte("\x00a\x00b\x00c")},
				},
			},
			{
				Query: "SELECT CAST(_utf8mb4'abc' COLLATE utf8mb4_0900_bin AS BINARY);",
				Expected: []sql.UntypedSqlRow{
					{[]byte("abc")},
				},
			},
			{
				Query: "SELECT CAST(_utf8mb4'\x00a\x00b\x00c' COLLATE utf8mb4_0900_bin AS BINARY);",
				Expected: []sql.UntypedSqlRow{
					{[]byte("\x00a\x00b\x00c")},
				},
			},
		},
	},
	{
		Name: "Issue #5482",
		Queries: []CharsetCollationEngineTestQuery{
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
