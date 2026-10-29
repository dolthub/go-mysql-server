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
	"time"

	"github.com/dolthub/vitess/go/mysql"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

var InsertIgnoreScripts = []ScriptTest{
	{
		// https://github.com/dolthub/dolt/issues/11918
		Name:    "insert ignore negative string with dangling exponent into unsigned column",
		Dialect: "mysql",
		SetUpScript: []string{
			"CREATE TABLE t (pk INT PRIMARY KEY, i INT, u INT UNSIGNED);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:                           "INSERT IGNORE INTO t VALUES (1, '-1e', '-1e');",
				Expected:                        []sql.Row{{types.NewOkResult(1)}},
				ExpectedWarning:                 mysql.ERWarnDataOutOfRange,
				ExpectedWarningsCount:           1,
				ExpectedWarningMessageSubstring: "Out of range value for column 'u' at row 1",
			},
			{
				Query:    "SELECT * FROM t;",
				Expected: []sql.Row{{1, -1, uint32(0)}},
			},
		},
	},
	{
		Name: "Test that INSERT IGNORE with Non nullable columns works",
		SetUpScript: []string{
			"CREATE TABLE x (pk int primary key, c1 varchar(20) NOT NULL);",
			"INSERT IGNORE INTO x VALUES (1, NULL)",
			"CREATE TABLE y (pk int primary key, c1 int NOT NULL);",
			"INSERT IGNORE INTO y VALUES (1, NULL);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM x",
				Expected: []sql.Row{
					{1, ""},
				},
			},
			{
				Query: "SELECT * FROM y",
				Expected: []sql.Row{
					{1, 0},
				},
			},
			{
				Query: "INSERT IGNORE INTO y VALUES (2, NULL)",
				Expected: []sql.Row{
					{types.OkResult{RowsAffected: 1}},
				},
				ExpectedWarningsCount: 1,
				ExpectedWarning:       mysql.ERBadNullError,
			},
		},
	},
	{
		Name: "Test that INSERT IGNORE properly addresses data conversion",
		SetUpScript: []string{
			"CREATE TABLE t1 (pk int primary key, v1 int)",
			"CREATE TABLE t2 (pk int primary key, v2 varchar(1))",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "INSERT IGNORE INTO t1 VALUES (1, 'dasd')",
				Expected: []sql.Row{
					{types.OkResult{RowsAffected: 1}},
				},
				ExpectedWarningsCount: 1,
				ExpectedWarning:       mysql.ERTruncatedWrongValueForField,
			},
			{
				Query: "SELECT * FROM t1",
				Expected: []sql.Row{
					{1, 0},
				},
			},
			{
				Query: "INSERT IGNORE INTO t2 values (1, 'adsda')",
				Expected: []sql.Row{
					{types.OkResult{RowsAffected: 1}},
				},
				ExpectedWarningsCount: 1,
				ExpectedWarning:       mysql.ERWarnDataTruncated,
			},
			{
				Query: "SELECT * FROM t2",
				Expected: []sql.Row{
					{1, "a"},
				},
			},
		},
	},
	{
		Name: "Insert Ignore works correctly with ON DUPLICATE UPDATE",
		SetUpScript: []string{
			"CREATE TABLE t1 (id INT PRIMARY KEY, v int);",
			"INSERT INTO t1 VALUES (1,1)",
			"CREATE TABLE t2 (pk int primary key, v2 varchar(1))",
			"ALTER TABLE t2 ADD CONSTRAINT cx CHECK (pk < 100)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "INSERT IGNORE INTO t1 VALUES (1,2) ON DUPLICATE KEY UPDATE v='dsd';",
				Expected: []sql.Row{
					{types.OkResult{RowsAffected: 2}},
				},
				ExpectedWarningsCount: 1,
				ExpectedWarning:       mysql.ERTruncatedWrongValueForField,
			},
			{
				Query: "SELECT * FROM t1",
				Expected: []sql.Row{
					{1, 0},
				},
			},
			{
				Query: "INSERT IGNORE INTO t2 values (1, 'adsda')",
				Expected: []sql.Row{
					{types.OkResult{RowsAffected: 1}},
				},
				ExpectedWarningsCount: 1,
				ExpectedWarning:       mysql.ERWarnDataTruncated,
			},
			{
				Query: "SELECT * FROM t2",
				Expected: []sql.Row{
					{1, "a"},
				},
			},
			{
				Query:    "INSERT IGNORE INTO t2 VALUES (1, 's') ON DUPLICATE KEY UPDATE pk = 1000", // violates constraint
				Expected: []sql.Row{{types.OkResult{RowsAffected: 0}}},
			},
			{
				Query: "SELECT * FROM t2",
				Expected: []sql.Row{
					{1, "a"},
				},
			},
		},
	},
	{
		Name: "Test that INSERT IGNORE INTO works with unique keys",
		SetUpScript: []string{
			"CREATE TABLE one_uniq(pk int PRIMARY KEY, col1 int UNIQUE)",
			"CREATE TABLE two_uniq(pk int PRIMARY KEY, col1 int, col2 int, UNIQUE KEY col1_col2_uniq (col1, col2))",
			"INSERT INTO one_uniq values (1, 1)",
			"INSERT INTO two_uniq values (1, 1, 1)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "INSERT IGNORE INTO one_uniq VALUES (3, 2), (2, 1), (4, null), (5, null)",
				Expected: []sql.Row{
					{types.OkResult{RowsAffected: 3}},
				},
				ExpectedWarningsCount: 1,
				ExpectedWarning:       mysql.ERDupEntry,
			},
			{
				Query: "SELECT * from one_uniq;",
				Expected: []sql.Row{
					{1, 1}, {3, 2}, {4, nil}, {5, nil},
				},
			},
			{
				Query: "INSERT IGNORE INTO two_uniq VALUES (4, 1, 2), (5, 2, 1), (6, null, 1), (7, null, 1), (12, 1, 1), (8, 1, null), (9, 1, null), (10, null, null), (11, null, null)",
				Expected: []sql.Row{
					{types.OkResult{RowsAffected: 8}},
				},
				ExpectedWarningsCount: 1,
				ExpectedWarning:       mysql.ERDupEntry,
			},
			{
				Query: "SELECT * from two_uniq;",
				Expected: []sql.Row{
					{1, 1, 1}, {4, 1, 2}, {5, 2, 1}, {6, nil, 1}, {7, nil, 1}, {8, 1, nil}, {9, 1, nil}, {10, nil, nil}, {11, nil, nil},
				},
			},
		},
	},
	{
		// https://github.com/dolthub/dolt/issues/8611
		Name: "issue 8611: insert ignore on enum type column",
		SetUpScript: []string{
			"create table test_table (x int auto_increment primary key, y enum('hello','bye'))",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "insert into test_table values (1, 'invalid'), (2, 'comparative politics'), (3, null)",
				ExpectedErr: types.ErrDataTruncatedForColumnAtRow,
			},
			{
				Query:    "insert ignore into test_table values (1, 'invalid'), (2, 'bye'), (3, null)",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 3, InsertID: 1}}},
				//ExpectedWarning: mysql.ERWarnDataTruncated, // TODO: incorrect code
			},
			{
				Query:    "select * from test_table",
				Expected: []sql.Row{{1, ""}, {2, "bye"}, {3, nil}},
			},
		},
	},
	{
		// https://github.com/dolthub/dolt/issues/12218
		Name:    "INSERT IGNORE truncates values to the column character length",
		Dialect: "mysql",
		SetUpScript: []string{
			"CREATE TABLE v (b VARCHAR(4))",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:                 "INSERT IGNORE INTO v VALUES ('éééééé')",
				Expected:              []sql.Row{{types.NewOkResult(1)}},
				ExpectedWarning:       mysql.ERWarnDataTruncated,
				ExpectedWarningsCount: 1,
			},
			{
				Query:                 "INSERT IGNORE INTO v VALUES (CAST('2020-01-01' AS DATETIME))",
				Expected:              []sql.Row{{types.NewOkResult(1)}},
				ExpectedWarning:       mysql.ERWarnDataTruncated,
				ExpectedWarningsCount: 1,
			},
			{
				Query:                 "INSERT IGNORE INTO v VALUES (123456)",
				Expected:              []sql.Row{{types.NewOkResult(1)}},
				ExpectedWarning:       mysql.ERWarnDataTruncated,
				ExpectedWarningsCount: 1,
			},
			{
				Query:                 "INSERT IGNORE INTO v VALUES (X'616263646566')",
				Expected:              []sql.Row{{types.NewOkResult(1)}},
				ExpectedWarning:       mysql.ERWarnDataTruncated,
				ExpectedWarningsCount: 1,
			},
			{
				Query:                 "INSERT IGNORE INTO v VALUES ('ééééé'), ('abcdefgh')",
				Expected:              []sql.Row{{types.NewOkResult(2)}},
				ExpectedWarning:       mysql.ERWarnDataTruncated,
				ExpectedWarningsCount: 2,
			},
			{
				Query:    "SELECT b, CHAR_LENGTH(b) FROM v ORDER BY b",
				Expected: []sql.Row{{"1234", 4}, {"2020", 4}, {"abcd", 4}, {"abcd", 4}, {"éééé", 4}, {"éééé", 4}},
			},
		},
	},
	{
		// https://github.com/dolthub/dolt/issues/12218
		Name:    "INSERT IGNORE truncates values to the length of each string type",
		Dialect: "mysql",
		SetUpScript: []string{
			"CREATE TABLE s (c CHAR(4), vb VARBINARY(4), t TINYTEXT)",
			"CREATE TABLE s2 (b BINARY(4))",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:                 "INSERT IGNORE INTO s VALUES ('éééééé', 'éééééé', REPEAT('é', 200))",
				Expected:              []sql.Row{{types.NewOkResult(1)}},
				ExpectedWarning:       mysql.ERWarnDataTruncated,
				ExpectedWarningsCount: 3,
			},
			{
				Query: "SELECT c, HEX(vb), CHAR_LENGTH(t), LENGTH(t) FROM s",
				// TINYTEXT < 255 because multibyte characters cannot split.
				Expected: []sql.Row{{"éééé", "C3A9C3A9", 127, 254}},
			},
			{
				Query:                 "INSERT IGNORE INTO s2 VALUES (X'616263646566')",
				Expected:              []sql.Row{{types.NewOkResult(1)}},
				ExpectedWarning:       mysql.ERWarnDataTruncated,
				ExpectedWarningsCount: 1,
			},
			{
				Query:    "SELECT HEX(b) FROM s2",
				Expected: []sql.Row{{"61626364"}},
			},
		},
	},
	{
		// https://github.com/dolthub/dolt/issues/12218
		Name:    "INSERT IGNORE ON DUPLICATE KEY UPDATE truncates values to the column character length",
		Dialect: "mysql",
		SetUpScript: []string{
			"CREATE TABLE o (id INT PRIMARY KEY, b VARCHAR(4))",
			"INSERT INTO o VALUES (1, 'abcd')",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:                 "INSERT IGNORE INTO o VALUES (1, 'x') ON DUPLICATE KEY UPDATE b = CAST('2020-01-01' AS DATETIME)",
				Expected:              []sql.Row{{types.NewOkResult(2)}},
				ExpectedWarning:       mysql.ERWarnDataTruncated,
				ExpectedWarningsCount: 1,
			},
			{
				Query:    "SELECT id, b FROM o",
				Expected: []sql.Row{{1, "2020"}},
			},
		},
	},
	{
		// https://github.com/dolthub/dolt/issues/12218
		Name:    "INSERT IGNORE truncates VARCHAR by character count in unicode and western charsets",
		Dialect: "mysql",
		SetUpScript: []string{
			"CREATE TABLE l (b VARCHAR(2) CHARACTER SET latin1)",
			"CREATE TABLE u16 (b VARCHAR(3) CHARACTER SET utf16)",
			"CREATE TABLE u32 (b VARCHAR(3) CHARACTER SET utf32)",
			"CREATE TABLE m3 (b VARCHAR(3) CHARACTER SET utf8mb3)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:                 "INSERT IGNORE INTO l VALUES ('ééé')",
				Expected:              []sql.Row{{types.NewOkResult(1)}},
				ExpectedWarning:       mysql.ERWarnDataTruncated,
				ExpectedWarningsCount: 1,
			},
			{
				Query:    "SELECT b, HEX(b), CHAR_LENGTH(b), LENGTH(b) FROM l",
				Expected: []sql.Row{{"éé", "E9E9", 2, 2}},
			},
			{
				Query:       "INSERT INTO l VALUES (UNHEX('E9E9E9'))",
				ExpectedErr: types.ErrLengthBeyondLimit,
			},
			{
				Query:                 "INSERT IGNORE INTO l VALUES (UNHEX('E9E9E9'))",
				Expected:              []sql.Row{{types.NewOkResult(1)}},
				ExpectedWarning:       mysql.ERWarnDataTruncated,
				ExpectedWarningsCount: 1,
			},
			{
				Query:                 "UPDATE IGNORE l SET b = UNHEX('F1F1F1')",
				Expected:              []sql.Row{{NewUpdateResult(2, 2)}},
				ExpectedWarning:       mysql.ERWarnDataTruncated,
				ExpectedWarningsCount: 2,
			},
			{
				Query:    "SELECT HEX(b) FROM l",
				Expected: []sql.Row{{"F1F1"}, {"F1F1"}},
			},
			{
				Query:                 "INSERT IGNORE INTO u16 VALUES ('abcd'), ('éééé')",
				Expected:              []sql.Row{{types.NewOkResult(2)}},
				ExpectedWarning:       mysql.ERWarnDataTruncated,
				ExpectedWarningsCount: 2,
			},
			{
				Query:    "SELECT b, CHAR_LENGTH(b) FROM u16 ORDER BY b",
				Expected: []sql.Row{{"abc", 3}, {"ééé", 3}},
			},
			{
				Query:                 "INSERT IGNORE INTO u32 VALUES ('abcd'), ('éééé')",
				Expected:              []sql.Row{{types.NewOkResult(2)}},
				ExpectedWarning:       mysql.ERWarnDataTruncated,
				ExpectedWarningsCount: 2,
			},
			{
				Query:    "SELECT b, CHAR_LENGTH(b) FROM u32 ORDER BY b",
				Expected: []sql.Row{{"abc", 3}, {"ééé", 3}},
			},
			{
				Query:                 "INSERT IGNORE INTO m3 VALUES ('éééé')",
				Expected:              []sql.Row{{types.NewOkResult(1)}},
				ExpectedWarning:       mysql.ERWarnDataTruncated,
				ExpectedWarningsCount: 1,
			},
			{
				Query:    "SELECT b, CHAR_LENGTH(b) FROM m3",
				Expected: []sql.Row{{"ééé", 3}},
			},
		},
	},
	{
		// https://github.com/dolthub/dolt/issues/12218
		Name:    "INSERT IGNORE truncates TINYTEXT by bytes of the column character set",
		Dialect: "mysql",
		SetUpScript: []string{
			"CREATE TABLE l (b TINYTEXT CHARACTER SET latin1)",
			"CREATE TABLE u16 (b TINYTEXT CHARACTER SET utf16)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "INSERT INTO l VALUES (REPEAT('é', 255))",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:       "INSERT INTO l VALUES (REPEAT('é', 256))",
				ExpectedErr: types.ErrLengthBeyondLimit,
			},
			{
				Query:                 "INSERT IGNORE INTO l VALUES (REPEAT('é', 256))",
				Expected:              []sql.Row{{types.NewOkResult(1)}},
				ExpectedWarning:       mysql.ERWarnDataTruncated,
				ExpectedWarningsCount: 1,
			},
			{
				Query:    "SELECT CHAR_LENGTH(b) FROM l",
				Expected: []sql.Row{{255}, {255}},
			},
			{
				Query:    "INSERT INTO u16 VALUES (REPEAT('a', 127))",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:       "INSERT INTO u16 VALUES (REPEAT('a', 128))",
				ExpectedErr: types.ErrLengthBeyondLimit,
			},
			{
				Query:                 "INSERT IGNORE INTO u16 VALUES (REPEAT('a', 200))",
				Expected:              []sql.Row{{types.NewOkResult(1)}},
				ExpectedWarning:       mysql.ERWarnDataTruncated,
				ExpectedWarningsCount: 1,
			},
			{
				Query:    "SELECT CHAR_LENGTH(b) FROM u16",
				Expected: []sql.Row{{127}, {127}},
			},
		},
	},
	{
		// https://github.com/dolthub/dolt/issues/12218
		Name:    "INSERT IGNORE truncates VARCHAR at an invalid UTF-8 byte before or at character limit",
		Dialect: "mysql",
		SetUpScript: []string{
			"CREATE TABLE bad_late (b VARCHAR(3))",
			"CREATE TABLE bad_early (b VARCHAR(4))",
			"CREATE TABLE bad_past (b VARCHAR(3))",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:                 "INSERT IGNORE INTO bad_late VALUES (UNHEX('C3A9C3A985'))",
				Expected:              []sql.Row{{types.NewOkResult(1)}},
				ExpectedWarning:       mysql.ERTruncatedWrongValueForField,
				ExpectedWarningsCount: 1,
			},
			{
				Query:    "SELECT HEX(b), CHAR_LENGTH(b) FROM bad_late",
				Expected: []sql.Row{{"C3A9C3A9", 2}},
			},
			{
				Query:                 "INSERT IGNORE INTO bad_early VALUES (UNHEX('C3A985C3A9C3A9C3A9'))",
				Expected:              []sql.Row{{types.NewOkResult(1)}},
				ExpectedWarning:       mysql.ERTruncatedWrongValueForField,
				ExpectedWarningsCount: 1,
			},
			{
				Query:    "SELECT HEX(b), CHAR_LENGTH(b) FROM bad_early",
				Expected: []sql.Row{{"C3A9", 1}},
			},
			{
				Query:                 "INSERT IGNORE INTO bad_past VALUES (UNHEX('C3A9C3A9C3A985'))",
				Expected:              []sql.Row{{types.NewOkResult(1)}},
				ExpectedWarning:       mysql.ERWarnDataTruncated,
				ExpectedWarningsCount: 1,
			},
			{
				Query:    "SELECT HEX(b), CHAR_LENGTH(b) FROM bad_past",
				Expected: []sql.Row{{"C3A9C3A9C3A9", 3}},
			},
		},
	},
	{
		// https://github.com/dolthub/dolt/issues/12218
		Name:    "INSERT IGNORE truncates ascii VARCHAR at the character limit or the first invalid byte",
		Dialect: "mysql",
		SetUpScript: []string{
			"CREATE TABLE a (b VARCHAR(3) CHARACTER SET ascii)",
		},
		Assertions: []ScriptTestAssertion{
			{
				// abcd, bad byte
				Query:       "INSERT INTO a VALUES (UNHEX('61626364FF'))",
				ExpectedErr: types.ErrLengthBeyondLimit,
			},
			{
				// a, bad byte, bcd
				Query:       "INSERT INTO a VALUES (UNHEX('61FF626364'))",
				ExpectedErr: types.ErrBadCharsetString,
			},
			{
				Query:                 "INSERT IGNORE INTO a VALUES (UNHEX('61626364FF'))",
				Expected:              []sql.Row{{types.NewOkResult(1)}},
				ExpectedWarning:       mysql.ERWarnDataTruncated,
				ExpectedWarningsCount: 1,
			},
			{
				Query:                 "INSERT IGNORE INTO a VALUES (UNHEX('61FF626364'))",
				Expected:              []sql.Row{{types.NewOkResult(1)}},
				ExpectedWarning:       mysql.ERTruncatedWrongValueForField,
				ExpectedWarningsCount: 1,
			},
			{
				Query:    "SELECT HEX(b) FROM a ORDER BY 1",
				Expected: []sql.Row{{"61"}, {"616263"}},
			},
		},
	},
	{
		// https://github.com/dolthub/dolt/issues/12218
		Name:    "INSERT IGNORE truncates utf16 VARCHAR at the character limit or the first unpaired surrogate",
		Dialect: "mysql",
		SetUpScript: []string{
			"CREATE TABLE u (b VARCHAR(2) CHARACTER SET utf16)",
		},
		Assertions: []ScriptTestAssertion{
			{
				// abcd, unpaired surrogate
				Query:       "INSERT INTO u VALUES (UNHEX('006100620063D800'))",
				ExpectedErr: types.ErrLengthBeyondLimit,
			},
			{
				// a, unpaired surrogate, b
				Query:       "INSERT INTO u VALUES (UNHEX('0061D8000062'))",
				ExpectedErr: types.ErrBadCharsetString,
			},
			{
				Query:                 "INSERT IGNORE INTO u VALUES (UNHEX('006100620063D800'))",
				Expected:              []sql.Row{{types.NewOkResult(1)}},
				ExpectedWarning:       mysql.ERWarnDataTruncated,
				ExpectedWarningsCount: 1,
			},
			{
				Query:                 "INSERT IGNORE INTO u VALUES (UNHEX('0061D8000062'))",
				Expected:              []sql.Row{{types.NewOkResult(1)}},
				ExpectedWarning:       mysql.ERTruncatedWrongValueForField,
				ExpectedWarningsCount: 1,
			},
			{
				Query:    "SELECT HEX(b) FROM u ORDER BY 1",
				Expected: []sql.Row{{"0061"}, {"00610062"}},
			},
		},
	},
}

var IgnoreWithDuplicateUniqueKeyKeylessScripts = []ScriptTest{
	{
		Name: "Test that INSERT IGNORE INTO works with unique keys on a keyless table",
		SetUpScript: []string{
			"CREATE TABLE one_uniq(not_pk int, value int UNIQUE)",
			"CREATE TABLE two_uniq(not_pk int, col1 int, col2 int, UNIQUE KEY col1_col2_uniq (col1, col2));",
			"INSERT INTO one_uniq values (1, 1)",
			"INSERT INTO two_uniq values (1, 1, 1)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "INSERT IGNORE INTO one_uniq VALUES (3, 2), (2, 1), (4, null), (5, null)",
				Expected: []sql.Row{
					{types.OkResult{RowsAffected: 3}},
				},
				ExpectedWarningsCount: 1,
				ExpectedWarning:       mysql.ERDupEntry,
			},
			{
				Query: "SELECT * from one_uniq;",
				Expected: []sql.Row{
					{1, 1}, {3, 2}, {4, nil}, {5, nil},
				},
			},
			{
				Query: "INSERT IGNORE INTO two_uniq VALUES (4, 1, 2), (5, 2, 1), (6, null, 1), (7, null, 1), (12, 1, 1), (8, 1, null), (9, 1, null), (10, null, null), (11, null, null)",
				Expected: []sql.Row{
					{types.OkResult{RowsAffected: 8}},
				},
				ExpectedWarningsCount: 1,
				ExpectedWarning:       mysql.ERDupEntry,
			},
			{
				Query: "SELECT * from two_uniq;",
				Expected: []sql.Row{
					{1, 1, 1}, {4, 1, 2}, {5, 2, 1}, {6, nil, 1}, {7, nil, 1}, {8, 1, nil}, {9, 1, nil}, {10, nil, nil}, {11, nil, nil},
				},
			},
		},
	},
	{
		Name: "INSERT IGNORE INTO multiple violations of a unique secondary index",
		SetUpScript: []string{
			"CREATE TABLE keyless(pk int, val int)",
			"INSERT INTO keyless values (1, 1), (2, 2), (3, 3)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "INSERT IGNORE INTO keyless VALUES (1, 2);",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:       "ALTER TABLE keyless ADD CONSTRAINT c UNIQUE(val)",
				ExpectedErr: sql.ErrUniqueKeyViolation,
			},
			{
				Query:    "DELETE FROM keyless where pk = 1 and val = 2",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "ALTER TABLE keyless ADD CONSTRAINT c UNIQUE(val)",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:                 "INSERT IGNORE INTO keyless VALUES (1, 3)",
				Expected:              []sql.Row{{types.NewOkResult(0)}},
				ExpectedWarningsCount: 1,
				ExpectedWarning:       mysql.ERDupEntry,
			},
		},
	},
	{
		Name: "UPDATE IGNORE keyless tables and secondary indexes",
		SetUpScript: []string{
			"CREATE TABLE keyless(pk int, val int)",
			"INSERT INTO keyless VALUES (1, 1), (2, 2), (3, 3)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "UPDATE IGNORE keyless SET val = 2 where pk = 1",
				Expected: []sql.Row{{NewUpdateResult(1, 1)}},
			},
			{
				Query:    "SELECT * FROM keyless ORDER BY pk",
				Expected: []sql.Row{{1, 2}, {2, 2}, {3, 3}},
			},
			{
				Query:       "ALTER TABLE keyless ADD CONSTRAINT c UNIQUE(val)",
				ExpectedErr: sql.ErrUniqueKeyViolation,
			},
			{
				Query:    "UPDATE IGNORE keyless SET val = 1 where pk = 1",
				Expected: []sql.Row{{NewUpdateResult(1, 1)}},
			},
			{
				Query:    "ALTER TABLE keyless ADD CONSTRAINT c UNIQUE(val)",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:                 "UPDATE IGNORE keyless SET val = 3 where pk = 1",
				Expected:              []sql.Row{{NewUpdateResult(1, 0)}},
				ExpectedWarningsCount: 1,
				ExpectedWarning:       mysql.ERDupEntry,
			},
			{
				Query:    "SELECT * FROM keyless ORDER BY pk",
				Expected: []sql.Row{{1, 1}, {2, 2}, {3, 3}},
			},
			{
				Query:                 "UPDATE IGNORE keyless SET val = val + 1 ORDER BY pk",
				Expected:              []sql.Row{{NewUpdateResult(3, 1)}},
				ExpectedWarningsCount: 2,
				ExpectedWarning:       mysql.ERDupEntry,
			},
			{
				Query:    "SELECT * FROM keyless ORDER BY pk",
				Expected: []sql.Row{{1, 1}, {2, 2}, {3, 4}},
			},
		},
	},
}

var InsertIgnoreRegressionScriptTests = []ScriptTest{
	{
		Name:    "INSERT IGNORE correctly truncates column data",
		Dialect: "mysql",
		SetUpScript: []string{
			`CREATE TABLE t (
				pk int primary key,
				col1 boolean,
				col2 integer,
				col3 tinyint,
				col4 smallint,
				col5 mediumint,
				col6 int,
				col7 bigint,
				col8 decimal,
				col9 float,
				col10 double,
				col11 date,
				col12 time,
				col13 datetime,
				col14 timestamp,
				col15 year,
				col16 ENUM('first', 'second'),
				col17 SET('a', 'b')
			);`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `
					INSERT IGNORE INTO t VALUES (
						1, 'val1', 'val2', 'val3', 'val4', 'val5', 'val6', 'val7', 'val8', 'val9', 'val10',
						'val11', 'val12', 'val13', 'val14', 'val15', 'val16', 'val17'
					);
				`,
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				SkipResultCheckOnServerEngine: true, // the datetime returned is not non-zero
				Query:                         "SELECT * from t",
				Expected: []sql.Row{
					{
						1,
						0,
						0,
						0,
						0,
						0,
						0,
						0,
						"0",
						float64(0),
						float64(0),
						time.Date(0, 0, 0, 0, 0, 0, 0, time.UTC),
						types.Timespan(0),
						time.Date(0, 0, 0, 0, 0, 0, 0, time.UTC),
						time.Date(0, 0, 0, 0, 0, 0, 0, time.UTC),
						0,
						"",
						"",
					},
				},
			},
		},
	},
	{
		Name: "INSERT IGNORE throws an error when json is badly formatted",
		SetUpScript: []string{
			"CREATE TABLE t (pk int primary key, col1 json);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "INSERT IGNORE into t VALUES (1, 'val1');",
				ExpectedErr: sql.ErrInvalidJson,
			},
		},
	},
}
