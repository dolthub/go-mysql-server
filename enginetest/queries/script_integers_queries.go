// Copyright 2020-2021 Dolthub, Inc.
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
	"math"
)

// IntegersScriptTests contains self-contained script tests for integers.
var IntegersScriptTests = []ScriptTest{
	{
		// https://github.com/dolthub/dolt/issues/11906
		Name:    "cast out-of-range bigint unsigned to signed",
		Dialect: "mysql",
		SetUpScript: []string{
			"CREATE TABLE t0 (id INT PRIMARY KEY, c0 BIGINT UNSIGNED NULL);",
			"INSERT INTO t0 VALUES (1, 18446744073709551615), (2, 9223372036854775808), (3, 9223372036854775807), (4, 1), (5, NULL);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT CAST(CAST(18446744073709551615 AS UNSIGNED) AS SIGNED);",
				Expected: []sql.Row{{int64(-1)}},
			},
			{
				Query:    "SELECT CAST(CAST(9223372036854775808 AS UNSIGNED) AS SIGNED);",
				Expected: []sql.Row{{int64(-9223372036854775808)}},
			},
			{
				Query:    "SELECT CAST(CAST(9223372036854775807 AS UNSIGNED) AS SIGNED);",
				Expected: []sql.Row{{int64(9223372036854775807)}},
			},
			{
				Query: "SELECT id, CAST(c0 AS SIGNED) FROM t0 ORDER BY id;",
				Expected: []sql.Row{
					{1, int64(-1)},
					{2, int64(-9223372036854775808)},
					{3, int64(9223372036854775807)},
					{4, int64(1)},
					{5, nil},
				},
			},
			{
				Query:    "SELECT id FROM t0 WHERE CAST(c0 AS SIGNED) < 0 ORDER BY id;",
				Expected: []sql.Row{{1}, {2}},
			},
		},
	},
	{
		Name:    "cast out-of-range integer strings to signed",
		Dialect: "mysql",
		SetUpScript: []string{
			"CREATE TABLE t0 (id INT PRIMARY KEY, c0 VARCHAR(30));",
			"INSERT INTO t0 VALUES (1, '18446744073709551615'), (2, '9223372036854775808'), (3, '9223372036854775807'), (4, NULL);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:                           "SELECT CAST('18446744073709551615' AS SIGNED);",
				Expected:                        []sql.Row{{int64(-1)}},
				ExpectedWarning:                 1105,
				ExpectedWarningsCount:           1,
				ExpectedWarningMessageSubstring: "negative complement",
			},
			{
				Query:                 "SELECT CONVERT('9223372036854775808', SIGNED);",
				Expected:              []sql.Row{{int64(-9223372036854775808)}},
				ExpectedWarning:       1105,
				ExpectedWarningsCount: 1,
			},
			{
				Query:                 "SELECT CAST('18446744073709551616' AS SIGNED);",
				Expected:              []sql.Row{{int64(-1)}},
				ExpectedWarning:       1292,
				ExpectedWarningsCount: 1,
			},
			{
				Query: "SELECT id, CAST(c0 AS SIGNED) FROM t0 ORDER BY id;",
				Expected: []sql.Row{
					{1, int64(-1)},
					{2, int64(-9223372036854775808)},
					{3, int64(9223372036854775807)},
					{4, nil},
				},
				ExpectedWarning:       1105,
				ExpectedWarningsCount: 2,
			},
		},
	},
	{
		// https://github.com/dolthub/dolt/issues/9927
		// https://github.com/dolthub/dolt/issues/9053
		Name:    "double negation of integer minimum values",
		Dialect: "mysql",
		SetUpScript: []string{
			"CREATE TABLE t0(c0 BIGINT);",
			"INSERT INTO t0(c0) VALUES (-9223372036854775808);",
			"CREATE TABLE t1(c0 INT);",
			"INSERT INTO t1(c0) VALUES (-2147483648);",
			"CREATE TABLE t2(c0 SMALLINT);",
			"INSERT INTO t2(c0) VALUES (-32768);",
			"CREATE TABLE t3(c0 TINYINT);",
			"INSERT INTO t3(c0) VALUES (-128);",
			"CREATE TABLE tab1 (col4 INT)",
			"INSERT INTO tab1 VALUES (10), (20), (30)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:           "SELECT -(-128);",
				Expected:        []sql.Row{{int16(128)}},
				ExpectedColumns: sql.Schema{{Name: "-(-128)", Type: types.Int64}},
			},
			{
				Query:           "SELECT -(-32768);",
				Expected:        []sql.Row{{int32(32768)}},
				ExpectedColumns: sql.Schema{{Name: "-(-32768)", Type: types.Int64}},
			},
			{
				Query:           "SELECT -(-2147483648);",
				Expected:        []sql.Row{{int64(2147483648)}},
				ExpectedColumns: sql.Schema{{Name: "-(-2147483648)", Type: types.Int64}},
			},
			{
				Query:           "SELECT -(-9223372036854775808)",
				Expected:        []sql.Row{{"9223372036854775808"}},
				ExpectedColumns: sql.Schema{{Name: "-(-9223372036854775808)", Type: types.InternalDecimalType}},
			},
			{
				Query:       "SELECT -t0.c0 FROM t0;",
				ExpectedErr: sql.ErrValueOutOfRange,
			},
			{
				Query:    "SELECT -t1.c0 FROM t1;",
				Expected: []sql.Row{{2147483648}},
			},
			{
				Query:    "SELECT -t2.c0 FROM t2;",
				Expected: []sql.Row{{32768}},
			},
			{
				Query:    "SELECT -t3.c0 FROM t3;",
				Expected: []sql.Row{{128}},
			},
			{
				Query:    "SELECT -(-t1.c0 + 1) FROM t1;",
				Expected: []sql.Row{{-2147483649}},
			},
			{
				Query:    "SELECT -(-(t2.c0 - 1)) FROM t2;",
				Expected: []sql.Row{{-32769}},
			},
			{
				Query:    "SELECT -(-t3.c0 * 2) FROM t3;",
				Expected: []sql.Row{{-256}},
			},
			{
				Query:    "SELECT -(-(-128));",
				Expected: []sql.Row{{int8(-128)}},
			},
			{
				Query:    "SELECT -(-(-(-128)));",
				Expected: []sql.Row{{int16(128)}},
			},
			{
				Query:    "SELECT -(-NULL);",
				Expected: []sql.Row{{nil}},
			},
			{
				Query:           "SELECT -(-CAST(-128 AS SIGNED));",
				Expected:        []sql.Row{{int64(-128)}},
				ExpectedColumns: sql.Schema{{Name: "-(-CAST(-128 AS SIGNED))", Type: types.Int64}},
			},
			{
				Query:    "SELECT * FROM tab1 AS cor0 WHERE NOT - CAST(NULL AS SIGNED) < +35 * +col4 + - -39",
				Expected: []sql.Row{},
			},
		},
	},
	{
		// https://github.com/dolthub/dolt/issues/11411
		Name: "integer arithmetic rejects signed and unsigned BIGINT overflow",
		// MySQL-only: PostgreSQL does not support unsigned integer types.
		Dialect: "mysql",
		SetUpScript: []string{
			"CREATE TABLE integer_bounds (id INT PRIMARY KEY, u BIGINT UNSIGNED, s BIGINT)",
			"INSERT INTO integer_bounds VALUES (1, 18446744073709551615, 9223372036854775807)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT u, u + 0, u + -1, u - 1 FROM integer_bounds",
				Expected: []sql.Row{{uint64(math.MaxUint64), uint64(math.MaxUint64), uint64(math.MaxUint64 - 1), uint64(math.MaxUint64 - 1)}},
			},
			{
				Query:       "SELECT u + 1 FROM integer_bounds",
				ExpectedErr: sql.ErrIntegerOutOfRange,
			},
			{
				Query:       "SELECT CAST(18446744073709551615 AS UNSIGNED) * 2",
				ExpectedErr: sql.ErrIntegerOutOfRange,
			},
			{
				Query:       "SELECT CAST(0 AS UNSIGNED) - 1",
				ExpectedErr: sql.ErrIntegerOutOfRange,
			},
			{
				Query:    "SELECT -1 + CAST(1 AS UNSIGNED)",
				Expected: []sql.Row{{uint64(0)}},
			},
			{
				Query:    "SELECT CAST(-1 AS SIGNED) * CAST(0 AS UNSIGNED), CAST(0 AS UNSIGNED) * CAST(-1 AS SIGNED)",
				Expected: []sql.Row{{uint64(0), uint64(0)}},
			},
			{
				Query:    "SELECT CAST(1 AS UNSIGNED) - -1, 2 - CAST(1 AS UNSIGNED)",
				Expected: []sql.Row{{uint64(2), uint64(1)}},
			},
			{
				Query:       "SELECT -2 + CAST(1 AS UNSIGNED)",
				ExpectedErr: sql.ErrIntegerOutOfRange,
			},
			{
				Query:       "SELECT 1 - CAST(2 AS UNSIGNED)",
				ExpectedErr: sql.ErrIntegerOutOfRange,
			},
			{
				Query:       "SELECT CAST(1 AS UNSIGNED) * -1",
				ExpectedErr: sql.ErrIntegerOutOfRange,
			},
			{
				Query:       "SELECT s + 1 FROM integer_bounds",
				ExpectedErr: sql.ErrIntegerOutOfRange,
			},
			{
				Query:       "SELECT CAST(-9223372036854775807 AS SIGNED) - 2",
				ExpectedErr: sql.ErrIntegerOutOfRange,
			},
			{
				Query:       "SELECT CAST(3037000500 AS SIGNED) * CAST(3037000500 AS SIGNED)",
				ExpectedErr: sql.ErrIntegerOutOfRange,
			},
		},
	},
	{
		Name: "NO_UNSIGNED_SUBTRACTION returns signed BIGINT arithmetic results",
		// MySQL-only: NO_UNSIGNED_SUBTRACTION is a MySQL SQL mode.
		Dialect: "mysql",
		SetUpScript: []string{
			"SET SESSION sql_mode = 'NO_UNSIGNED_SUBTRACTION'",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT CAST(0 AS UNSIGNED) - 1",
				Expected: []sql.Row{{-1}},
			},
			{
				Query:    "SELECT 1 - CAST(2 AS UNSIGNED)",
				Expected: []sql.Row{{-1}},
			},
			{
				Query:    "SELECT CAST(9223372036854775808 AS UNSIGNED) - 1",
				Expected: []sql.Row{{math.MaxInt64}},
			},
			{
				Query:    "SELECT CAST(9223372036854775807 AS SIGNED) - CAST(18446744073709551615 AS UNSIGNED)",
				Expected: []sql.Row{{math.MinInt64}},
			},
			{
				Query:       "SELECT CAST(18446744073709551615 AS UNSIGNED) - 1",
				ExpectedErr: sql.ErrIntegerOutOfRange,
			},
			{
				Query:       "SELECT CAST(-9223372036854775808 AS SIGNED) - CAST(1 AS UNSIGNED)",
				ExpectedErr: sql.ErrIntegerOutOfRange,
			},
		},
	},
	{
		Name:    "negative int limits",
		Dialect: "mysql",
		SetUpScript: []string{
			"CREATE TABLE t(i8 tinyint, i16 smallint, i24 mediumint, i32 int, i64 bigint);",
			"INSERT INTO t VALUES(-128, -32768, -8388608, -2147483648, -9223372036854775808);",
		},
		Assertions: []ScriptTestAssertion{
			{
				SkipResultCheckOnServerEngine: true,
				Query:                         "SELECT -i8, -i16, -i24, -i32 from t;",
				Expected: []sql.Row{
					{128, 32768, 8388608, 2147483648},
				},
			},
			{
				Query:          "SELECT -i64 from t;",
				ExpectedErrStr: "BIGINT out of range for -9223372036854775808",
			},
		},
	},
	{
		Name:    "negative int limits",
		Dialect: "mysql",
		SetUpScript: []string{
			"CREATE TABLE t(i8 tinyint, i16 smallint, i24 mediumint, i32 int, i64 bigint);",
			"INSERT INTO t VALUES(-128, -32768, -8388608, -2147483648, -9223372036854775808);",
		},
		Assertions: []ScriptTestAssertion{
			{
				SkipResultCheckOnServerEngine: true,
				Query:                         "SELECT -i8, -i16, -i24, -i32 from t;",
				Expected: []sql.Row{
					{128, 32768, 8388608, 2147483648},
				},
			},
			{
				Query:          "SELECT -i64 from t;",
				ExpectedErrStr: "BIGINT out of range for -9223372036854775808",
			},
		},
	},

	// Bit Tests
	{
		Name:        "bit with auto_increment",
		Dialect:     "mysql",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query:          "create table bad (b bit(1) primary key auto_increment);",
				ExpectedErrStr: "Incorrect column specifier for column 'b'",
			},
			{
				Query:          "create table bad (b bit(64) primary key auto_increment);",
				ExpectedErrStr: "Incorrect column specifier for column 'b'",
			},
		},
	},

	// Bool Tests
	{
		Name:    "bool with auto_increment",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table bool_tbl (b bool primary key auto_increment);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "show create table bool_tbl;",
				Expected: []sql.Row{
					{"bool_tbl", "CREATE TABLE `bool_tbl` (\n" +
						"  `b` tinyint(1) NOT NULL AUTO_INCREMENT,\n" +
						"  PRIMARY KEY (`b`)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
		},
	},

	// Int Tests
	{
		// https://github.com/dolthub/dolt/issues/9530
		Name:    "int with auto_increment",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table tinyint_tbl (i tinyint primary key auto_increment);",
			"create table smallint_tbl (i smallint primary key auto_increment);",
			"create table mediumint_tbl (i mediumint primary key auto_increment);",
			"create table int_tbl (i int primary key auto_increment);",
			"create table bigint_tbl (i bigint primary key auto_increment);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "insert into tinyint_tbl values (999)",
				ExpectedErr: sql.ErrValueOutOfRange,
			},
			{
				Query: "insert into tinyint_tbl values (127)",
				Expected: []sql.Row{
					{types.OkResult{
						RowsAffected: 1,
						InsertID:     127,
					}},
				},
			},
			{
				Query: "show create table tinyint_tbl;",
				Expected: []sql.Row{
					{"tinyint_tbl", "CREATE TABLE `tinyint_tbl` (\n" +
						"  `i` tinyint NOT NULL AUTO_INCREMENT,\n" +
						"  PRIMARY KEY (`i`)\n" +
						") ENGINE=InnoDB AUTO_INCREMENT=127 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},

			{
				Query:       "insert into smallint_tbl values (99999);",
				ExpectedErr: sql.ErrValueOutOfRange,
			},
			{
				Query: "insert into smallint_tbl values (32767);",
				Expected: []sql.Row{
					{types.OkResult{
						RowsAffected: 1,
						InsertID:     32767,
					}},
				},
			},
			{
				Query: "show create table smallint_tbl;",
				Expected: []sql.Row{
					{"smallint_tbl", "CREATE TABLE `smallint_tbl` (\n" +
						"  `i` smallint NOT NULL AUTO_INCREMENT,\n" +
						"  PRIMARY KEY (`i`)\n" +
						") ENGINE=InnoDB AUTO_INCREMENT=32767 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},

			{
				Query:       "insert into mediumint_tbl values (99999999);",
				ExpectedErr: sql.ErrValueOutOfRange,
			},
			{
				Query: "insert into mediumint_tbl values (8388607);",
				Expected: []sql.Row{
					{types.OkResult{
						RowsAffected: 1,
						InsertID:     8388607,
					}},
				},
			},
			{
				Query: "show create table mediumint_tbl;",
				Expected: []sql.Row{
					{"mediumint_tbl", "CREATE TABLE `mediumint_tbl` (\n" +
						"  `i` mediumint NOT NULL AUTO_INCREMENT,\n" +
						"  PRIMARY KEY (`i`)\n" +
						") ENGINE=InnoDB AUTO_INCREMENT=8388607 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},

			{
				Query:       "insert into int_tbl values (99999999999)",
				ExpectedErr: sql.ErrValueOutOfRange,
			},
			{
				Query: "insert into int_tbl values (2147483647)",
				Expected: []sql.Row{
					{types.OkResult{
						RowsAffected: 1,
						InsertID:     2147483647,
					}},
				},
			},
			{
				Query: "show create table int_tbl;",
				Expected: []sql.Row{
					{"int_tbl", "CREATE TABLE `int_tbl` (\n" +
						"  `i` int NOT NULL AUTO_INCREMENT,\n" +
						"  PRIMARY KEY (`i`)\n" +
						") ENGINE=InnoDB AUTO_INCREMENT=2147483647 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},

			{
				Query:       "insert into bigint_tbl values (99999999999999999999);",
				ExpectedErr: sql.ErrValueOutOfRange,
			},
			{
				Query: "insert into bigint_tbl values (9223372036854775807);",
				Expected: []sql.Row{
					{types.OkResult{
						RowsAffected: 1,
						InsertID:     9223372036854775807,
					}},
				},
			},
			{
				Query: "show create table bigint_tbl;",
				Expected: []sql.Row{
					{"bigint_tbl", "CREATE TABLE `bigint_tbl` (\n" +
						"  `i` bigint NOT NULL AUTO_INCREMENT,\n" +
						"  PRIMARY KEY (`i`)\n" +
						") ENGINE=InnoDB AUTO_INCREMENT=9223372036854775807 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
		},
	},
	{
		// https://github.com/dolthub/dolt/issues/9530
		Name:    "unsigned int with auto_increment",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table tinyint_tbl (i tinyint unsigned primary key auto_increment);",
			"create table smallint_tbl (i smallint unsigned primary key auto_increment);",
			"create table mediumint_tbl (i mediumint unsigned primary key auto_increment);",
			"create table int_tbl (i int unsigned primary key auto_increment);",
			"create table bigint_tbl (i bigint unsigned primary key auto_increment);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "insert into tinyint_tbl values (999)",
				ExpectedErr: sql.ErrValueOutOfRange,
			},
			{
				Query: "insert into tinyint_tbl values (255)",
				Expected: []sql.Row{
					{types.OkResult{
						RowsAffected: 1,
						InsertID:     255,
					}},
				},
			},
			{
				Query: "show create table tinyint_tbl;",
				Expected: []sql.Row{
					{"tinyint_tbl", "CREATE TABLE `tinyint_tbl` (\n" +
						"  `i` tinyint unsigned NOT NULL AUTO_INCREMENT,\n" +
						"  PRIMARY KEY (`i`)\n" +
						") ENGINE=InnoDB AUTO_INCREMENT=255 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},

			{
				Query:       "insert into smallint_tbl values (99999);",
				ExpectedErr: sql.ErrValueOutOfRange,
			},
			{
				Query: "insert into smallint_tbl values (65535);",
				Expected: []sql.Row{
					{types.OkResult{
						RowsAffected: 1,
						InsertID:     65535,
					}},
				},
			},
			{
				Query: "show create table smallint_tbl;",
				Expected: []sql.Row{
					{"smallint_tbl", "CREATE TABLE `smallint_tbl` (\n" +
						"  `i` smallint unsigned NOT NULL AUTO_INCREMENT,\n" +
						"  PRIMARY KEY (`i`)\n" +
						") ENGINE=InnoDB AUTO_INCREMENT=65535 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},

			{
				Query:       "insert into mediumint_tbl values (999999999);",
				ExpectedErr: sql.ErrValueOutOfRange,
			},
			{
				Query: "insert into mediumint_tbl values (16777215);",
				Expected: []sql.Row{
					{types.OkResult{
						RowsAffected: 1,
						InsertID:     16777215,
					}},
				},
			},
			{
				Query: "show create table mediumint_tbl;",
				Expected: []sql.Row{
					{"mediumint_tbl", "CREATE TABLE `mediumint_tbl` (\n" +
						"  `i` mediumint unsigned NOT NULL AUTO_INCREMENT,\n" +
						"  PRIMARY KEY (`i`)\n" +
						") ENGINE=InnoDB AUTO_INCREMENT=16777215 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},

			{
				Query:       "insert into int_tbl values (99999999999)",
				ExpectedErr: sql.ErrValueOutOfRange,
			},
			{
				Query: "insert into int_tbl values (4294967295)",
				Expected: []sql.Row{
					{types.OkResult{
						RowsAffected: 1,
						InsertID:     4294967295,
					}},
				},
			},
			{
				Query: "show create table int_tbl;",
				Expected: []sql.Row{
					{"int_tbl", "CREATE TABLE `int_tbl` (\n" +
						"  `i` int unsigned NOT NULL AUTO_INCREMENT,\n" +
						"  PRIMARY KEY (`i`)\n" +
						") ENGINE=InnoDB AUTO_INCREMENT=4294967295 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},

			{
				Query:       "insert into bigint_tbl values (999999999999999999999);",
				ExpectedErr: sql.ErrValueOutOfRange,
			},
			{
				Query: "insert into bigint_tbl values (18446744073709551615);",
				Expected: []sql.Row{
					{types.OkResult{
						RowsAffected: 1,
						InsertID:     18446744073709551615,
					}},
				},
			},
			{
				Query: "show create table bigint_tbl;",
				Expected: []sql.Row{
					{"bigint_tbl", "CREATE TABLE `bigint_tbl` (\n" +
						"  `i` bigint unsigned NOT NULL AUTO_INCREMENT,\n" +
						"  PRIMARY KEY (`i`)\n" +
						") ENGINE=InnoDB AUTO_INCREMENT=18446744073709551615 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
		},
	},
	{
		Name:    "signed int with overflowing filters",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table ti8  (i tinyint primary key);",
			"insert into ti8 values (-128), (-1), (0), (1), (127);",

			"create table ti16 (i smallint primary key);",
			"insert into ti16 values (-32768), (-1), (0), (1), (32767);",

			"create table ti24 (i mediumint primary key);",
			"insert into ti24 values (-8388608), (-1), (0), (1), (8388607);",

			"create table ti32 (i int primary key);",
			"insert into ti32 values (-2147483648), (-1), (0), (1), (2147483647);",

			"create table ti64 (i bigint primary key);",
			"insert into ti64 values (-9223372036854775808), (-1), (0), (1), (9223372036854775807);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select * from ti8 where i = 999;",
				Expected: []sql.Row{},
			},
			{
				Query:    "select * from ti8 where i = -999;",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from ti8 where i != 999;",
				Expected: []sql.Row{
					{-128},
					{-1},
					{0},
					{1},
					{127},
				},
			},
			{
				Query: "select * from ti8 where i != -999;",
				Expected: []sql.Row{
					{-128},
					{-1},
					{0},
					{1},
					{127},
				},
			},
			{
				Query:    "select * from ti8 where i > 999;",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from ti8 where i > -999;",
				Expected: []sql.Row{
					{-128},
					{-1},
					{0},
					{1},
					{127},
				},
			},
			{
				Query:    "select * from ti8 where i >= 999;",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from ti8 where i >= -999;",
				Expected: []sql.Row{
					{-128},
					{-1},
					{0},
					{1},
					{127},
				},
			},
			{
				Query: "select * from ti8 where i < 999;",
				Expected: []sql.Row{
					{-128},
					{-1},
					{0},
					{1},
					{127},
				},
			},
			{
				Query:    "select * from ti8 where i < -999;",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from ti8 where i <= 999;",
				Expected: []sql.Row{
					{-128},
					{-1},
					{0},
					{1},
					{127},
				},
			},
			{
				Query:    "select * from ti8 where i <= -999;",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from ti8 where i in (0, 999);",
				Expected: []sql.Row{
					{0},
				},
			},
			{
				Query: "select * from ti8 where i in (0, -999);",
				Expected: []sql.Row{
					{0},
				},
			},
			{
				Query: "select * from ti8 where i not in (0, 999);",
				Expected: []sql.Row{
					{-128},
					{-1},
					{1},
					{127},
				},
			},
			{
				Query: "select * from ti8 where i not in (0, -999);",
				Expected: []sql.Row{
					{-128},
					{-1},
					{1},
					{127},
				},
			},

			{
				Query:    "select * from ti16 where i = 99999;",
				Expected: []sql.Row{},
			},
			{
				Query:    "select * from ti16 where i = -99999;",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from ti16 where i != 99999;",
				Expected: []sql.Row{
					{-32768},
					{-1},
					{0},
					{1},
					{32767},
				},
			},
			{
				Query: "select * from ti16 where i != -99999;",
				Expected: []sql.Row{
					{-32768},
					{-1},
					{0},
					{1},
					{32767},
				},
			},
			{
				Query:    "select * from ti16 where i > 99999;",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from ti16 where i > -99999;",
				Expected: []sql.Row{
					{-32768},
					{-1},
					{0},
					{1},
					{32767},
				},
			},
			{
				Query:    "select * from ti16 where i >= 99999;",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from ti16 where i >= -99999;",
				Expected: []sql.Row{
					{-32768},
					{-1},
					{0},
					{1},
					{32767},
				},
			},
			{
				Query: "select * from ti16 where i < 99999;",
				Expected: []sql.Row{
					{-32768},
					{-1},
					{0},
					{1},
					{32767},
				},
			},
			{
				Query:    "select * from ti16 where i < -99999;",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from ti16 where i <= 99999;",
				Expected: []sql.Row{
					{-32768},
					{-1},
					{0},
					{1},
					{32767},
				},
			},
			{
				Query:    "select * from ti16 where i <= -99999;",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from ti16 where i in (0, 99999);",
				Expected: []sql.Row{
					{0},
				},
			},
			{
				Query: "select * from ti16 where i in (0, -99999);",
				Expected: []sql.Row{
					{0},
				},
			},
			{
				Query: "select * from ti16 where i not in (0, 99999);",
				Expected: []sql.Row{
					{-32768},
					{-1},
					{1},
					{32767},
				},
			},
			{
				Query: "select * from ti16 where i not in (0, -99999);",
				Expected: []sql.Row{
					{-32768},
					{-1},
					{1},
					{32767},
				},
			},

			{
				Query:    "select * from ti24 where i = 9999999;",
				Expected: []sql.Row{},
			},
			{
				Query:    "select * from ti24 where i = -999999;",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from ti24 where i != 9999999;",
				Expected: []sql.Row{
					{-8388608},
					{-1},
					{0},
					{1},
					{8388607},
				},
			},
			{
				Query: "select * from ti24 where i != -9999999;",
				Expected: []sql.Row{
					{-8388608},
					{-1},
					{0},
					{1},
					{8388607},
				},
			},
			{
				Query:    "select * from ti24 where i > 9999999;",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from ti24 where i > -9999999;",
				Expected: []sql.Row{
					{-8388608},
					{-1},
					{0},
					{1},
					{8388607},
				},
			},
			{
				Query:    "select * from ti24 where i >= 9999999;",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from ti24 where i >= -9999999;",
				Expected: []sql.Row{
					{-8388608},
					{-1},
					{0},
					{1},
					{8388607},
				},
			},
			{
				Query: "select * from ti24 where i < 9999999;",
				Expected: []sql.Row{
					{-8388608},
					{-1},
					{0},
					{1},
					{8388607},
				},
			},
			{
				Query:    "select * from ti24 where i < -9999999;",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from ti24 where i <= 9999999;",
				Expected: []sql.Row{
					{-8388608},
					{-1},
					{0},
					{1},
					{8388607},
				},
			},
			{
				Query:    "select * from ti24 where i <= -9999999;",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from ti24 where i in (0, 9999999);",
				Expected: []sql.Row{
					{0},
				},
			},
			{
				Query: "select * from ti24 where i in (0, -9999999);",
				Expected: []sql.Row{
					{0},
				},
			},
			{
				Query: "select * from ti24 where i not in (0, 9999999);",
				Expected: []sql.Row{
					{-8388608},
					{-1},
					{1},
					{8388607},
				},
			},
			{
				Query: "select * from ti24 where i not in (0, -9999999);",
				Expected: []sql.Row{
					{-8388608},
					{-1},
					{1},
					{8388607},
				},
			},

			{
				Query:    "select * from ti32 where i = 9999999999;",
				Expected: []sql.Row{},
			},
			{
				Query:    "select * from ti32 where i = -9999999999;",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from ti32 where i != 9999999999;",
				Expected: []sql.Row{
					{-2147483648},
					{-1},
					{0},
					{1},
					{2147483647},
				},
			},
			{
				Query: "select * from ti32 where i != -9999999999;",
				Expected: []sql.Row{
					{-2147483648},
					{-1},
					{0},
					{1},
					{2147483647},
				},
			},
			{
				Query:    "select * from ti32 where i > 9999999999;",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from ti32 where i > -9999999999;",
				Expected: []sql.Row{
					{-2147483648},
					{-1},
					{0},
					{1},
					{2147483647},
				},
			},
			{
				Query:    "select * from ti32 where i >= 9999999999;",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from ti32 where i >= -9999999999;",
				Expected: []sql.Row{
					{-2147483648},
					{-1},
					{0},
					{1},
					{2147483647},
				},
			},
			{
				Query: "select * from ti32 where i < 9999999999;",
				Expected: []sql.Row{
					{-2147483648},
					{-1},
					{0},
					{1},
					{2147483647},
				},
			},
			{
				Query:    "select * from ti32 where i < -9999999999;",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from ti32 where i <= 9999999999;",
				Expected: []sql.Row{
					{-2147483648},
					{-1},
					{0},
					{1},
					{2147483647},
				},
			},
			{
				Query:    "select * from ti32 where i <= -9999999999;",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from ti32 where i in (0, 9999999999);",
				Expected: []sql.Row{
					{0},
				},
			},
			{
				Query: "select * from ti32 where i in (0, -9999999999);",
				Expected: []sql.Row{
					{0},
				},
			},
			{
				Query: "select * from ti32 where i not in (0, 9999999999);",
				Expected: []sql.Row{
					{-2147483648},
					{-1},
					{1},
					{2147483647},
				},
			},
			{
				Query: "select * from ti32 where i not in (0, -9999999999);",
				Expected: []sql.Row{
					{-2147483648},
					{-1},
					{1},
					{2147483647},
				},
			},

			{
				Query:    "select * from ti64 where i = 9999999999999999999;",
				Expected: []sql.Row{},
			},
			{
				Query:    "select * from ti64 where i = -9999999999999999999;",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from ti64 where i != 9999999999999999999;",
				Expected: []sql.Row{
					{-9223372036854775808},
					{-1},
					{0},
					{1},
					{9223372036854775807},
				},
			},
			{
				Query: "select * from ti64 where i != -9999999999999999999;",
				Expected: []sql.Row{
					{-9223372036854775808},
					{-1},
					{0},
					{1},
					{9223372036854775807},
				},
			},
			{
				Query:    "select * from ti64 where i > 9999999999999999999;",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from ti64 where i > -9999999999999999999;",
				Expected: []sql.Row{
					{-9223372036854775808},
					{-1},
					{0},
					{1},
					{9223372036854775807},
				},
			},
			{
				Query:    "select * from ti64 where i >= 9999999999999999999;",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from ti64 where i >= -9999999999999999999;",
				Expected: []sql.Row{
					{-9223372036854775808},
					{-1},
					{0},
					{1},
					{9223372036854775807},
				},
			},
			{
				Query: "select * from ti64 where i < 9999999999999999999;",
				Expected: []sql.Row{
					{-9223372036854775808},
					{-1},
					{0},
					{1},
					{9223372036854775807},
				},
			},
			{
				Query:    "select * from ti64 where i < -9999999999999999999;",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from ti64 where i <= 9999999999999999999;",
				Expected: []sql.Row{
					{-9223372036854775808},
					{-1},
					{0},
					{1},
					{9223372036854775807},
				},
			},
			{
				Query:    "select * from ti64 where i <= -9999999999999999999;",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from ti64 where i in (0, 9999999999999999999);",
				Expected: []sql.Row{
					{0},
				},
			},
			{
				Query: "select * from ti64 where i in (0, -9999999999999999999);",
				Expected: []sql.Row{
					{0},
				},
			},
			{
				Query: "select * from ti64 where i not in (0, 9999999999999999999);",
				Expected: []sql.Row{
					{-9223372036854775808},
					{-1},
					{1},
					{9223372036854775807},
				},
			},
			{
				Query: "select * from ti64 where i not in (0, -9999999999999999999);",
				Expected: []sql.Row{
					{-9223372036854775808},
					{-1},
					{1},
					{9223372036854775807},
				},
			},
		},
	},
	{
		Name:    "unsigned int with overflowing filters",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table tui8 (i tinyint unsigned primary key);",
			"insert into tui8 values (0), (1), (255);",

			"create table tui16 (i smallint unsigned primary key);",
			"insert into tui16 values (0), (1), (65535);",

			"create table tui24 (i mediumint unsigned primary key);",
			"insert into tui24 values (0), (1), (16777215);",

			"create table tui32 (i int unsigned primary key);",
			"insert into tui32 values (0), (1), (4294967295);",

			"create table tui64 (i bigint unsigned primary key);",
			"insert into tui64 values (0), (1), (18446744073709551615);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select * from tui8 where i = 999;",
				Expected: []sql.Row{},
			},
			{
				Query:    "select * from tui8 where i = -999;",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from tui8 where i != 999;",
				Expected: []sql.Row{
					{uint64(0)},
					{uint64(1)},
					{uint64(255)},
				},
			},
			{
				Query: "select * from tui8 where i != -999;",
				Expected: []sql.Row{
					{uint64(0)},
					{uint64(1)},
					{uint64(255)},
				},
			},
			{
				Query:    "select * from tui8 where i > 999;",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from tui8 where i > -999;",
				Expected: []sql.Row{
					{uint64(0)},
					{uint64(1)},
					{uint64(255)},
				},
			},
			{
				Query:    "select * from tui8 where i >= 999;",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from tui8 where i >= -999;",
				Expected: []sql.Row{
					{uint64(0)},
					{uint64(1)},
					{uint64(255)},
				},
			},
			{
				Query: "select * from tui8 where i < 999;",
				Expected: []sql.Row{
					{uint64(0)},
					{uint64(1)},
					{uint64(255)},
				},
			},
			{
				Query:    "select * from tui8 where i < -999;",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from tui8 where i <= 999;",
				Expected: []sql.Row{
					{uint64(0)},
					{uint64(1)},
					{uint64(255)},
				},
			},
			{
				Query:    "select * from tui8 where i <= -999;",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from tui8 where i in (0, 999);",
				Expected: []sql.Row{
					{uint64(0)},
				},
			},
			{
				Query: "select * from tui8 where i in (0, -999);",
				Expected: []sql.Row{
					{uint64(0)},
				},
			},
			{
				Query: "select * from tui8 where i not in (0, 999);",
				Expected: []sql.Row{
					{uint64(1)},
					{uint64(255)},
				},
			},
			{
				Query: "select * from tui8 where i not in (0, -999);",
				Expected: []sql.Row{
					{uint64(1)},
					{uint64(255)},
				},
			},

			{
				Query:    "select * from tui16 where i = 99999;",
				Expected: []sql.Row{},
			},
			{
				Query:    "select * from tui16 where i = -99999;",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from tui16 where i != 99999;",
				Expected: []sql.Row{
					{uint64(0)},
					{uint64(1)},
					{uint64(65535)},
				},
			},
			{
				Query: "select * from tui16 where i != -99999;",
				Expected: []sql.Row{
					{uint64(0)},
					{uint64(1)},
					{uint64(65535)},
				},
			},
			{
				Query:    "select * from tui16 where i > 99999;",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from tui16 where i > -99999;",
				Expected: []sql.Row{
					{uint64(0)},
					{uint64(1)},
					{uint64(65535)},
				},
			},
			{
				Query:    "select * from tui16 where i >= 99999;",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from tui16 where i >= -99999;",
				Expected: []sql.Row{
					{uint64(0)},
					{uint64(1)},
					{uint64(65535)},
				},
			},
			{
				Query: "select * from tui16 where i < 99999;",
				Expected: []sql.Row{
					{uint64(0)},
					{uint64(1)},
					{uint64(65535)},
				},
			},
			{
				Query:    "select * from tui16 where i < -99999;",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from tui16 where i <= 99999;",
				Expected: []sql.Row{
					{uint64(0)},
					{uint64(1)},
					{uint64(65535)},
				},
			},
			{
				Query:    "select * from tui16 where i <= -99999;",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from tui16 where i in (0, 99999);",
				Expected: []sql.Row{
					{uint64(0)},
				},
			},
			{
				Query: "select * from tui16 where i in (0, -99999);",
				Expected: []sql.Row{
					{uint64(0)},
				},
			},
			{
				Query: "select * from tui16 where i not in (0, 99999);",
				Expected: []sql.Row{
					{uint64(1)},
					{uint64(65535)},
				},
			},
			{
				Query: "select * from tui16 where i not in (0, -99999);",
				Expected: []sql.Row{
					{uint64(1)},
					{uint64(65535)},
				},
			},

			{
				Query:    "select * from tui24 where i = 99999999;",
				Expected: []sql.Row{},
			},
			{
				Query:    "select * from tui24 where i = -9999999;",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from tui24 where i != 99999999;",
				Expected: []sql.Row{
					{uint64(0)},
					{uint64(1)},
					{uint64(16777215)},
				},
			},
			{
				Query: "select * from tui24 where i != -99999999;",
				Expected: []sql.Row{
					{uint64(0)},
					{uint64(1)},
					{uint64(16777215)},
				},
			},
			{
				Query:    "select * from tui24 where i > 99999999;",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from tui24 where i > -99999999;",
				Expected: []sql.Row{
					{uint64(0)},
					{uint64(1)},
					{uint64(16777215)},
				},
			},
			{
				Query:    "select * from tui24 where i >= 99999999;",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from tui24 where i >= -99999999;",
				Expected: []sql.Row{
					{uint64(0)},
					{uint64(1)},
					{uint64(16777215)},
				},
			},
			{
				Query: "select * from tui24 where i < 99999999;",
				Expected: []sql.Row{
					{uint64(0)},
					{uint64(1)},
					{uint64(16777215)},
				},
			},
			{
				Query:    "select * from tui24 where i < -99999999;",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from tui24 where i <= 99999999;",
				Expected: []sql.Row{
					{uint64(0)},
					{uint64(1)},
					{uint64(16777215)},
				},
			},
			{
				Query:    "select * from tui24 where i <= -99999999;",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from tui24 where i in (0, 99999999);",
				Expected: []sql.Row{
					{uint64(0)},
				},
			},
			{
				Query: "select * from tui24 where i in (0, -99999999);",
				Expected: []sql.Row{
					{uint64(0)},
				},
			},
			{
				Query: "select * from tui24 where i not in (0, 99999999);",
				Expected: []sql.Row{
					{uint64(1)},
					{uint64(16777215)},
				},
			},
			{
				Query: "select * from tui24 where i not in (0, -99999999);",
				Expected: []sql.Row{
					{uint64(1)},
					{uint64(16777215)},
				},
			},

			{
				Query:    "select * from tui32 where i = 9999999999;",
				Expected: []sql.Row{},
			},
			{
				Query:    "select * from tui32 where i = -9999999999;",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from tui32 where i != 9999999999;",
				Expected: []sql.Row{
					{uint64(0)},
					{uint64(1)},
					{uint64(4294967295)},
				},
			},
			{
				Query: "select * from tui32 where i != -9999999999;",
				Expected: []sql.Row{
					{uint64(0)},
					{uint64(1)},
					{uint64(4294967295)},
				},
			},
			{
				Query:    "select * from tui32 where i > 9999999999;",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from tui32 where i > -9999999999;",
				Expected: []sql.Row{
					{uint64(0)},
					{uint64(1)},
					{uint64(4294967295)},
				},
			},
			{
				Query:    "select * from tui32 where i >= 9999999999;",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from tui32 where i >= -9999999999;",
				Expected: []sql.Row{
					{uint64(0)},
					{uint64(1)},
					{uint64(4294967295)},
				},
			},
			{
				Query: "select * from tui32 where i < 9999999999;",
				Expected: []sql.Row{
					{uint64(0)},
					{uint64(1)},
					{uint64(4294967295)},
				},
			},
			{
				Query:    "select * from tui32 where i < -9999999999;",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from tui32 where i <= 9999999999;",
				Expected: []sql.Row{
					{uint64(0)},
					{uint64(1)},
					{uint64(4294967295)},
				},
			},
			{
				Query:    "select * from tui32 where i <= -9999999999;",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from tui32 where i in (0, 9999999999);",
				Expected: []sql.Row{
					{uint64(0)},
				},
			},
			{
				Query: "select * from tui32 where i in (0, -9999999999);",
				Expected: []sql.Row{
					{uint64(0)},
				},
			},
			{
				Query: "select * from tui32 where i not in (0, 9999999999);",
				Expected: []sql.Row{
					{uint64(1)},
					{uint64(4294967295)},
				},
			},
			{
				Query: "select * from tui32 where i not in (0, -9999999999);",
				Expected: []sql.Row{
					{uint64(1)},
					{uint64(4294967295)},
				},
			},

			{
				Query:    "select * from tui64 where i = 99999999999999999999;",
				Expected: []sql.Row{},
			},
			{
				Query:    "select * from tui64 where i = -99999999999999999999;",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from tui64 where i != 99999999999999999999;",
				Expected: []sql.Row{
					{uint64(0)},
					{uint64(1)},
					{uint64(18446744073709551615)},
				},
			},
			{
				Query: "select * from tui64 where i != -99999999999999999999;",
				Expected: []sql.Row{
					{uint64(0)},
					{uint64(1)},
					{uint64(18446744073709551615)},
				},
			},
			{
				Query:    "select * from tui64 where i > 99999999999999999999;",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from tui64 where i > -99999999999999999999;",
				Expected: []sql.Row{
					{uint64(0)},
					{uint64(1)},
					{uint64(18446744073709551615)},
				},
			},
			{
				Query:    "select * from tui64 where i >= 99999999999999999999;",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from tui64 where i >= -99999999999999999999;",
				Expected: []sql.Row{
					{uint64(0)},
					{uint64(1)},
					{uint64(18446744073709551615)},
				},
			},
			{
				Query: "select * from tui64 where i < 99999999999999999999;",
				Expected: []sql.Row{
					{uint64(0)},
					{uint64(1)},
					{uint64(18446744073709551615)},
				},
			},
			{
				Query:    "select * from tui64 where i < -99999999999999999999;",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from tui64 where i <= 99999999999999999999;",
				Expected: []sql.Row{
					{uint64(0)},
					{uint64(1)},
					{uint64(18446744073709551615)},
				},
			},
			{
				Query:    "select * from tui64 where i <= -99999999999999999999;",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from tui64 where i in (0, 99999999999999999999);",
				Expected: []sql.Row{
					{uint64(0)},
				},
			},
			{
				Query: "select * from tui64 where i in (0, -99999999999999999999);",
				Expected: []sql.Row{
					{uint64(0)},
				},
			},
			{
				Query: "select * from tui64 where i not in (0, 99999999999999999999);",
				Expected: []sql.Row{
					{uint64(1)},
					{uint64(18446744073709551615)},
				},
			},
			{
				Query: "select * from tui64 where i not in (0, -99999999999999999999);",
				Expected: []sql.Row{
					{uint64(1)},
					{uint64(18446744073709551615)},
				},
			},
		},
	},
}
