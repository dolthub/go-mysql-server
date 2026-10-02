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
	"github.com/dolthub/vitess/go/mysql"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// CharsetCollationEngineTests are used to ensure that character sets and collations have the correct behavior over the
// engine. Return values should all have the `utf8mb4` encoding, as it's returning the internal encoding type.
var CharsetCollationEngineTests = []ScriptTest{
	{
		Name: "Uppercase and lowercase collations",
		Assertions: []ScriptTestAssertion{
			{
				Query:    "CREATE TABLE test1 (v1 VARCHAR(255) COLLATE utf16_unicode_ci, v2 VARCHAR(255) COLLATE UTF16_UNICODE_CI);",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "CREATE TABLE test2 (v1 VARCHAR(255) CHARACTER SET utf16, v2 VARCHAR(255) CHARACTER SET UTF16);",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
		},
	},
	{
		Name: "Insert multiple character sets",
		SetUpScript: []string{
			"CREATE TABLE test (v1 VARCHAR(255) COLLATE utf16_unicode_ci);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "INSERT INTO test VALUES ('hey');",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "INSERT INTO test VALUES (_utf16'\x00h\x00i');",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "INSERT INTO test VALUES (_utf8mb4'\x68\x65\x6c\x6c\x6f');",
				Expected: []sql.Row{{types.NewOkResult(1)}},
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
		Assertions: []ScriptTestAssertion{
			{
				Query:    "INSERT INTO test1 VALUES ('HEY2'), ('hey1');",
				Expected: []sql.Row{{types.NewOkResult(2)}},
			},
			{
				Query:    "INSERT INTO test2 VALUES ('HEY2'), ('hey1');",
				Expected: []sql.Row{{types.NewOkResult(2)}},
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
		Assertions: []ScriptTestAssertion{
			{
				Query:       "SELECT _utf16'\x00a' COLLATE utf8mb4_0900_bin;",
				ExpectedErr: sql.ErrCollationInvalidForCharSet,
			},
			{
				Query:       "SELECT _utf16'\x00a' COLLATE binary;",
				ExpectedErr: sql.ErrCollationInvalidForCharSet,
			},
		},
	},
	{
		Name: "Properly block using not-yet-implemented character sets/collations",
		Assertions: []ScriptTestAssertion{
			{
				Query:       "CREATE TABLE test1 (pk BIGINT PRIMARY KEY, v1 VARCHAR(255) CHARACTER SET utf16le);",
				ExpectedErr: sql.ErrCharSetNotYetImplementedTemp,
			},
			{
				Query:       "CREATE TABLE test2 (pk BIGINT PRIMARY KEY, v1 VARCHAR(255) COLLATE utf16le_general_ci);",
				ExpectedErr: sql.ErrCharSetNotYetImplementedTemp,
			},
			{
				Query:    "CREATE TABLE test3 (pk BIGINT PRIMARY KEY, v1 VARCHAR(255) CHARACTER SET utf8mb4);",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:       "ALTER TABLE test3 MODIFY COLUMN v1 VARCHAR(255) COLLATE utf8mb4_sr_latn_0900_as_cs;",
				ExpectedErr: sql.ErrCollationNotYetImplementedTemp,
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
		Assertions: []ScriptTestAssertion{
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
		Assertions: []ScriptTestAssertion{
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
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT v1, pk FROM test1 WHERE v1 <= 'aBc' ORDER BY v1, pk;",
				Expected: []sql.Row{
					{"abc", int64(1)}, {"ABC", int64(2)}, {"aBc", int64(3)}, {"AbC", int64(4)},
				},
			},
			{
				Query: "SELECT v1, pk FROM test2 WHERE v1 <= 'aBc' ORDER BY v1, pk;",
				Expected: []sql.Row{
					{"abc", int64(1)}, {"ABC", int64(2)}, {"aBc", int64(3)}, {"AbC", int64(4)},
				},
			},
			{
				Query: "ALTER TABLE test2 MODIFY COLUMN v1 VARCHAR(100);",
				Expected: []sql.Row{
					{types.NewOkResult(0)},
				},
			},
			{
				Query: "SELECT v1, pk FROM test2 WHERE v1 <= 'aBc' ORDER BY v1, pk;",
				Expected: []sql.Row{
					{"abc", int64(1)}, {"ABC", int64(2)}, {"aBc", int64(3)}, {"AbC", int64(4)},
				},
			},
			{
				Query: "SELECT v1, pk FROM test3 WHERE v1 <= 'aBc' ORDER BY v1, pk;",
				Expected: []sql.Row{
					{"abc", int64(1)}, {"ABC", int64(2)}, {"aBc", int64(3)}, {"AbC", int64(4)},
				},
			},
			{
				Query: "SELECT v1, pk FROM test4 WHERE v1 <= 'aBc' ORDER BY v1, pk;",
				Expected: []sql.Row{
					{"abc", int64(1)}, {"ABC", int64(2)}, {"aBc", int64(3)}, {"AbC", int64(4)},
				},
			},
			{
				Query: "SHOW CREATE TABLE test1;",
				Expected: []sql.Row{
					{"test1", "CREATE TABLE `test1` (\n  `pk` bigint NOT NULL,\n  `v1` varchar(255),\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf16 COLLATE=utf16_unicode_ci"},
				},
			},
			{
				Query: "SHOW CREATE TABLE test2;",
				Expected: []sql.Row{
					{"test2", "CREATE TABLE `test2` (\n  `pk` bigint NOT NULL,\n  `v1` varchar(100),\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci"},
				},
			},
			{
				Query: "SHOW CREATE TABLE test3;",
				Expected: []sql.Row{
					{"test3", "CREATE TABLE `test3` (\n  `pk` bigint NOT NULL,\n  `v1` varchar(255),\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci"},
				},
			},
			{
				Query: "SHOW CREATE TABLE test4;",
				Expected: []sql.Row{
					{"test4", "CREATE TABLE `test4` (\n  `pk` bigint NOT NULL,\n  `v1` varchar(255) COLLATE utf8mb4_unicode_ci\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
			{
				Query: "ALTER TABLE test3 ADD COLUMN v2 VARCHAR(255);",
				Expected: []sql.Row{
					{types.NewOkResult(0)},
				},
			},
			{
				Query: "SHOW CREATE TABLE test3;",
				Expected: []sql.Row{
					{"test3", "CREATE TABLE `test3` (\n  `pk` bigint NOT NULL,\n  `v1` varchar(255),\n  `v2` varchar(255),\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci"},
				},
			},
			{
				Query: "ALTER TABLE test2 CHANGE COLUMN v1 v1 VARCHAR(220);",
				Expected: []sql.Row{
					{types.NewOkResult(0)},
				},
			},
			{
				Query: "SHOW CREATE TABLE test2;",
				Expected: []sql.Row{
					{"test2", "CREATE TABLE `test2` (\n  `pk` bigint NOT NULL,\n  `v1` varchar(220),\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci"},
				},
			},
			{
				Query:          "ALTER TABLE test2 CHARACTER SET latin1 COLLATE utf8mb4_bin;",
				ExpectedErrStr: "latin1 is not a valid character set for utf8mb4_bin",
			},
			{
				Query: "ALTER TABLE test2 COLLATE utf8mb4_bin;",
				Expected: []sql.Row{
					{types.NewOkResult(0)},
				},
			},
			{
				Query: "ALTER TABLE test2 ADD COLUMN v2 VARCHAR(255);",
				Expected: []sql.Row{
					{types.NewOkResult(0)},
				},
			},
			{
				Query: "REPLACE INTO test2 VALUES (1, 'abc', 'abc'), (2, 'ABC', 'ABC'), (3, 'aBc', 'aBc'), (4, 'AbC', 'AbC');",
				Expected: []sql.Row{
					{types.NewOkResult(8)},
				},
			},
			{
				Query: "SELECT v1, pk FROM test2 WHERE v1 <= 'aBc' ORDER BY v1, pk;",
				Expected: []sql.Row{
					{"abc", int64(1)}, {"ABC", int64(2)}, {"aBc", int64(3)}, {"AbC", int64(4)},
				},
			},
			{
				Query: "SELECT v2, pk FROM test2 WHERE v2 <= 'aBc' ORDER BY v2, pk;",
				Expected: []sql.Row{
					{"ABC", int64(2)}, {"AbC", int64(4)}, {"aBc", int64(3)},
				},
			},
			{
				Query: "SHOW CREATE TABLE test2;",
				Expected: []sql.Row{
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
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT * FROM test ORDER BY v1 COLLATE utf8mb4_bin ASC;",
				Expected: []sql.Row{{int64(1), "a"}, {int64(2), "b"}},
			},
			{
				Query:       "SELECT * FROM test ORDER BY v1 COLLATE utf8mb3_bin ASC;",
				ExpectedErr: sql.ErrCollationInvalidForCharSet,
			},
			{
				Query:    "SELECT 'a' COLLATE utf8mb3_bin;",
				Expected: []sql.Row{{"a"}},
			},
			{
				Query:       "SELECT 'a' COLLATE utf8mb4_bin;",
				ExpectedErr: sql.ErrCollationInvalidForCharSet,
			},
		},
	},
	{
		Name: "SET validates character set and collation variables",
		Assertions: []ScriptTestAssertion{
			{
				Query:       "SET character_set_client = 'am_i_wrong';",
				ExpectedErr: sql.ErrCharSetUnknown,
			},
			{
				Query:       "SET character_set_connection = 'to_believe';",
				ExpectedErr: sql.ErrCharSetUnknown,
			},
			{
				Query:       "SET character_set_results = 'in_crusty_cheese';",
				ExpectedErr: sql.ErrCharSetUnknown,
			},
			{
				Query:       "SET collation_connection = 'is_it_wrong';",
				ExpectedErr: sql.ErrCollationUnknown,
			},
			{
				Query:       "SET collation_database = 'to_believe';",
				ExpectedErr: sql.ErrCollationUnknown,
			},
			{
				Query:       "SET collation_server = 'in_deez';",
				ExpectedErr: sql.ErrCollationUnknown,
			},
			{
				Query:       "SET NAMES things;",
				ExpectedErr: sql.ErrCharSetUnknown,
			},
		},
	},
	{
		Name: "setting charset/collation sets the other",
		Assertions: []ScriptTestAssertion{
			{
				Query: "select @@session.character_set_connection, @@session.collation_connection;",
				Expected: []sql.Row{
					{"utf8mb4", "utf8mb4_0900_bin"},
				},
			},
			{
				Query:    "set @@session.character_set_connection = 'latin1';",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query: "select @@session.character_set_connection, @@session.collation_connection;",
				Expected: []sql.Row{
					{"latin1", "latin1_swedish_ci"},
				},
			},
			{
				Query:    "set @@session.collation_connection = 'utf8mb4_0900_bin';",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query: "select @@session.character_set_connection, @@session.collation_connection;",
				Expected: []sql.Row{
					{"utf8mb4", "utf8mb4_0900_bin"},
				},
			},

			{
				Query: "select @@global.character_set_connection, @@global.collation_connection;",
				Expected: []sql.Row{
					{"utf8mb4", "utf8mb4_0900_bin"},
				},
			},
			{
				Query:    "set @@global.character_set_connection = 'latin1';",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query: "select @@global.character_set_connection, @@global.collation_connection;",
				Expected: []sql.Row{
					{"latin1", "latin1_swedish_ci"},
				},
			},
			{
				Query:    "set @@global.collation_connection = 'utf8mb4_0900_bin';",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query: "select @@global.character_set_connection, @@global.collation_connection;",
				Expected: []sql.Row{
					{"utf8mb4", "utf8mb4_0900_bin"},
				},
			},

			{
				Query: "select @@session.character_set_server, @@session.collation_server;",
				Expected: []sql.Row{
					{"utf8mb4", "utf8mb4_0900_bin"},
				},
			},
			{
				Query:    "set @@session.character_set_server = 'latin1';",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query: "select @@session.character_set_server, @@session.collation_server;",
				Expected: []sql.Row{
					{"latin1", "latin1_swedish_ci"},
				},
			},
			{
				Query:    "set @@session.collation_server = 'utf8mb4_0900_bin';",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query: "select @@session.character_set_server, @@session.collation_server;",
				Expected: []sql.Row{
					{"utf8mb4", "utf8mb4_0900_bin"},
				},
			},

			{
				Query: "select @@global.character_set_server, @@global.collation_server;",
				Expected: []sql.Row{
					{"utf8mb4", "utf8mb4_0900_bin"},
				},
			},
			{
				Query:    "set @@global.character_set_server = 'latin1';",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query: "select @@global.character_set_server, @@global.collation_server;",
				Expected: []sql.Row{
					{"latin1", "latin1_swedish_ci"},
				},
			},
			{
				Query:    "set @@global.collation_server = 'utf8mb4_0900_bin';",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query: "select @@global.character_set_server, @@global.collation_server;",
				Expected: []sql.Row{
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
		Assertions: []ScriptTestAssertion{
			{
				Query: "INSERT INTO test1 VALUES (1, 'ABC');",
				Expected: []sql.Row{
					{types.NewOkResult(1)},
				},
			},
			{
				Query:       "INSERT INTO test2 VALUES (1, 'ABC');",
				ExpectedErr: types.ErrDataTruncatedForColumnAtRow,
			},
			{
				Query: "INSERT INTO test1 VALUES (2, _utf16'\x00d\x00e\x00f' COLLATE utf16_unicode_ci);",
				Expected: []sql.Row{
					{types.NewOkResult(1)},
				},
			},
			{
				Query: "INSERT INTO test2 VALUES (2, _utf16'\x00d\x00e\x00f' COLLATE utf16_unicode_ci);",
				Expected: []sql.Row{
					{types.NewOkResult(1)},
				},
			},
			{
				Query: "SELECT * FROM test1 ORDER BY pk;",
				Expected: []sql.Row{
					{int64(1), "abc"}, {int64(2), "def"},
				},
			},
			{
				Query: "SELECT * FROM test2 ORDER BY pk;",
				Expected: []sql.Row{
					{int64(2), "def"},
				},
			},
			{
				Query:       "create table t (e enum('abc', 'ABC') collate utf8mb4_0900_ai_ci))",
				ExpectedErr: sql.ErrSyntaxError,
			},
		},
	},
	{
		Name: "SET collation handling",
		SetUpScript: []string{
			"CREATE TABLE test1 (pk BIGINT PRIMARY KEY, v1 SET('a','b','c') COLLATE utf16_unicode_ci);",
			"CREATE TABLE test2 (pk BIGINT PRIMARY KEY, v1 SET('a','b','c') COLLATE utf8mb4_0900_bin);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "INSERT INTO test1 VALUES (1, 'A');",
				Expected: []sql.Row{
					{types.NewOkResult(1)},
				},
			},
			{
				Query:       "INSERT INTO test2 VALUES (1, 'A');",
				ExpectedErr: types.ErrDataTruncatedForColumnAtRow,
			},
			{
				Query: "INSERT INTO test1 VALUES (2, _utf16'\x00b\x00,\x00c' COLLATE utf16_unicode_ci);",
				Expected: []sql.Row{
					{types.NewOkResult(1)},
				},
			},
			{
				Query: "INSERT INTO test2 VALUES (2, _utf16'\x00b\x00,\x00c' COLLATE utf16_unicode_ci);",
				Expected: []sql.Row{
					{types.NewOkResult(1)},
				},
			},
			{
				Query: "SELECT * FROM test1 ORDER BY pk;",
				Expected: []sql.Row{
					{int64(1), "a"}, {int64(2), "b,c"},
				},
			},
			{
				Query: "SELECT * FROM test2 ORDER BY pk;",
				Expected: []sql.Row{
					{int64(2), "b,c"},
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
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT COUNT(*) FROM test WHERE v1 LIKE 'ABC';",
				Expected: []sql.Row{
					{int64(1)},
				},
			},
			{
				Query: "SELECT COUNT(*) FROM test WHERE v2 LIKE 'ABC';",
				Expected: []sql.Row{
					{int64(2)},
				},
			},
			{
				Query: "SELECT COUNT(*) FROM test WHERE v1 LIKE 'A%';",
				Expected: []sql.Row{
					{int64(1)},
				},
			},
			{
				Query: "SELECT COUNT(*) FROM test WHERE v2 LIKE 'A%';",
				Expected: []sql.Row{
					{int64(2)},
				},
			},
			{
				Query: "SELECT COUNT(*) FROM test WHERE v1 LIKE '%C';",
				Expected: []sql.Row{
					{int64(1)},
				},
			},
			{
				Query: "SELECT COUNT(*) FROM test WHERE v2 LIKE '%C';",
				Expected: []sql.Row{
					{int64(2)},
				},
			},
			{
				Query:    "SET collation_connection = 'utf8mb4_0900_bin';",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query: "SELECT COUNT(*) FROM test WHERE v1 LIKE 'ABC';",
				Expected: []sql.Row{
					{int64(1)},
				},
			},
			{
				Query: "SELECT COUNT(*) FROM test WHERE v2 LIKE 'ABC';",
				Expected: []sql.Row{
					{int64(2)},
				},
			},
			{
				Query: "SELECT COUNT(*) FROM test WHERE v1 LIKE 'ABC' COLLATE utf8mb4_0900_ai_ci;",
				Expected: []sql.Row{
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
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT 'abc' LIKE 'ABC';",
				Expected: []sql.Row{
					{true},
				},
			},
			{
				Query: "SELECT 'abc' COLLATE utf8mb4_0900_bin LIKE 'ABC';",
				Expected: []sql.Row{
					{false},
				},
			},
			{
				Query: "SELECT 'abc' LIKE 'ABC' COLLATE utf8mb4_0900_bin;",
				Expected: []sql.Row{
					{false},
				},
			},
			{
				Query: "SELECT 'abc' COLLATE utf8mb4_0900_ai_ci LIKE 'ABC';",
				Expected: []sql.Row{
					{true},
				},
			},
			{
				Query: "SELECT 'abc' LIKE 'ABC' COLLATE utf8mb4_0900_ai_ci;",
				Expected: []sql.Row{
					{true},
				},
			},
			{
				Query:    "SET collation_connection = 'utf8mb4_0900_bin';",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query: "SELECT 'abc' LIKE 'ABC';",
				Expected: []sql.Row{
					{false},
				},
			},
			{
				Query: "SELECT 'abc' COLLATE utf8mb4_0900_ai_ci LIKE 'ABC';",
				Expected: []sql.Row{
					{true},
				},
			},
			{
				Query: "SELECT 'abc' LIKE 'ABC' COLLATE utf8mb4_0900_ai_ci;",
				Expected: []sql.Row{
					{true},
				},
			},
			{
				Query: "SELECT 'abc' COLLATE utf8mb4_0900_bin LIKE 'ABC';",
				Expected: []sql.Row{
					{false},
				},
			},
			{
				Query: "SELECT 'abc' LIKE 'ABC' COLLATE utf8mb4_0900_bin;",
				Expected: []sql.Row{
					{false},
				},
			},
			{
				Query: "SELECT _utf8mb4'abc' LIKE 'ABC';",
				Expected: []sql.Row{
					{false},
				},
			},
			{
				Query: "SELECT 'abc' LIKE _utf8mb4'ABC';",
				Expected: []sql.Row{
					{false},
				},
			},
		},
	},
	{
		Name: "STRCMP() function",
		Assertions: []ScriptTestAssertion{
			// TODO: returning different results from MySQL
			/*{
				// collation with the lowest coercibility is used
				Query: "SELECT STRCMP(_utf8mb4'A' COLLATE utf8mb4_0900_ai_ci, 'a')",
				Expected: []sql.Row{
					{int(0)},
				},
			},
			{
				// same coercibility, both unicode
				Query:   "SELECT STRCMP(_utf8mb4'A' COLLATE utf8mb4_0900_ai_ci, _utf8mb4'a' COLLATE utf8mb4_0900_as_cs)",
				ExpectedErr: sql.ErrCollationIllegalMix,
			},
			{
				// same coercibility, both not unicode
				Query:   "SELECT STRCMP(_latin1'A' COLLATE latin1_general_ci, _latin1'a' COLLATE latin1_german1_ci)",
				ExpectedErr: sql.ErrCollationIllegalMix,
			},*/
			{
				// same coercibility, one unicode and one not unicode
				Query: "SELECT STRCMP(_utf8mb4'A' COLLATE utf8mb4_0900_ai_ci, _latin1'b' COLLATE latin1_general_cs)",
				Expected: []sql.Row{
					{int(-1)},
				},
			},
		},
	},
	{
		Name: "LENGTH() function",
		Assertions: []ScriptTestAssertion{
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
		Assertions: []ScriptTestAssertion{
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
			{
				// See https://github.com/dolthub/dolt/issues/11088
				Query: "SELECT CHAR_LENGTH(_utf8mb4'\xef\xbf\xbd' COLLATE utf8mb4_0900_bin);",
				Expected: []sql.Row{
					{int32(1)},
				},
			},
		},
	},
	{
		// See https://github.com/dolthub/go-mysql-server/issues/3837
		Name: "CONVERT() USING with malformed multi-byte strings",
		Assertions: []ScriptTestAssertion{
			{
				Query:                           "SELECT CONVERT(0x61FF62 USING utf8mb4);",
				Expected:                        []sql.Row{{nil}},
				ExpectedWarning:                 mysql.ERInvalidCharacterString,
				ExpectedWarningsCount:           1,
				ExpectedWarningMessageSubstring: "invalid string for character set",
			},
			{
				Query:                           "SELECT CHAR_LENGTH(CONVERT(0x61FF62 USING utf8mb4));",
				Expected:                        []sql.Row{{nil}},
				ExpectedWarning:                 mysql.ERInvalidCharacterString,
				ExpectedWarningsCount:           1,
				ExpectedWarningMessageSubstring: "invalid string for character set",
			},
			{
				Query:                           "SELECT HEX(LPAD(CONVERT(0x61FF62 USING utf8mb4), 5, 'x'));",
				Expected:                        []sql.Row{{nil}},
				ExpectedWarning:                 mysql.ERInvalidCharacterString,
				ExpectedWarningsCount:           1,
				ExpectedWarningMessageSubstring: "invalid string for character set",
			},
			{
				// Valid multi-byte sequences are still converted and padded.
				Query:    "SELECT HEX(CONVERT(0x61C3A962 USING utf8mb4));",
				Expected: []sql.Row{{"61C3A962"}},
			},
			{
				// A binary target accepts any byte sequence.
				Query:    "SELECT HEX(CONVERT(0x61FF62 USING binary));",
				Expected: []sql.Row{{"61FF62"}},
			},
			{
				Query:    "SELECT HEX(LPAD(CONVERT(0x61C3A962 USING utf8mb4), 5, 'x'));",
				Expected: []sql.Row{{"787861C3A962"}},
			},
		},
	},
	{
		Name: "UPPER() function",
		Assertions: []ScriptTestAssertion{
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
		Assertions: []ScriptTestAssertion{
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
		Assertions: []ScriptTestAssertion{
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
		Assertions: []ScriptTestAssertion{
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
		Assertions: []ScriptTestAssertion{
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
		Assertions: []ScriptTestAssertion{
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
		Assertions: []ScriptTestAssertion{
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
		Assertions: []ScriptTestAssertion{
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
		Assertions: []ScriptTestAssertion{
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
		Assertions: []ScriptTestAssertion{
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
		Assertions: []ScriptTestAssertion{
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
		Assertions: []ScriptTestAssertion{
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
		Assertions: []ScriptTestAssertion{
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
		Assertions: []ScriptTestAssertion{
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
	{
		Name: "Issue #5482",
		Assertions: []ScriptTestAssertion{
			{
				Query: `SELECT T.TABLE_NAME AS label, 'connection.table' as type, T.TABLE_SCHEMA AS 'schema',
T.TABLE_SCHEMA AS 'database', T.TABLE_CATALOG AS 'catalog',
0 AS isView FROM INFORMATION_SCHEMA.TABLES AS T WHERE T.TABLE_CATALOG = 'def' AND
                                                      UPPER(T.TABLE_TYPE) = 'BASE TABLE' ORDER BY T.TABLE_NAME;`,
				Expected: []sql.Row(nil),
			},
		},
	},
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

	// https://github.com/dolthub/dolt/issues/11907
	{
		Name: "IN predicate with accent-insensitive collation",
		SetUpScript: []string{
			"CREATE TABLE t (id INT PRIMARY KEY, label VARCHAR(96) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL);",
			"INSERT INTO t VALUES (1, CONVERT(X'636166C3A9' USING utf8mb4)), (2, 'cafe'), (3, ' Cafe '), (4, 'other'), (5, 'CAFÉ'), (6, 'cafë');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT id FROM t WHERE LOWER(TRIM(label)) = 'cafe' ORDER BY id;",
				Expected: []sql.Row{{1}, {2}, {3}, {5}, {6}},
			},
			{
				Query:    "SELECT id FROM t WHERE LOWER(TRIM(label)) IN ('cafe') ORDER BY id;",
				Expected: []sql.Row{{1}, {2}, {3}, {5}, {6}},
			},
			{
				Query:    "SELECT id FROM t WHERE label = 'cafe' ORDER BY id;",
				Expected: []sql.Row{{1}, {2}, {5}, {6}},
			},
			{
				Query:    "SELECT id FROM t WHERE label IN ('cafe') ORDER BY id;",
				Expected: []sql.Row{{1}, {2}, {5}, {6}},
			},
			{
				Query:    "SELECT id FROM t WHERE label IN ('cafe', 'other') ORDER BY id;",
				Expected: []sql.Row{{1}, {2}, {4}, {5}, {6}},
			},
			{
				Query:    "SELECT id FROM t WHERE label NOT IN ('cafe', 'other') ORDER BY id;",
				Expected: []sql.Row{{3}},
			},
			{
				Query:    "SELECT id FROM t WHERE (id, label) IN ((1, 'cafe'), (2, 'cafe'), (4, 'other')) ORDER BY id;",
				Expected: []sql.Row{{1}, {2}, {4}},
			},
			{
				Query:    "SELECT id FROM t WHERE label IN ('cafe' COLLATE utf8mb4_bin, 'other') ORDER BY id;",
				Expected: []sql.Row{{2}, {4}},
			},
			{
				Query:    "SELECT id, (label IN ('cafe')) FROM t ORDER BY id;",
				Expected: []sql.Row{{1, true}, {2, true}, {3, false}, {4, false}, {5, true}, {6, true}},
			},
		},
	},
	{
		Name: "CHECK constraint with IN predicate and collation",
		SetUpScript: []string{
			"CREATE TABLE c (id INT PRIMARY KEY, a VARCHAR(10) COLLATE utf8mb4_0900_ai_ci, b VARCHAR(10) COLLATE utf8mb4_0900_ai_ci, CHECK (a IN (b)));",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "INSERT INTO c VALUES (1, 'A', 'a');",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1}}},
			},
			{
				Query:    "SELECT * FROM c;",
				Expected: []sql.Row{{1, "A", "a"}},
			},
		},
	},
}

// CharsetCollationScriptTests contains self-contained charset collation script tests.
var CharsetCollationScriptTests = []ScriptTest{
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
}
