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
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// EnumsScriptTests contains self-contained enums script tests.
var EnumsScriptTests = []ScriptTest{

	// Enum tests
	{
		Name:    "enum errors",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table t (i int primary key, e enum('abc', 'def', 'ghi'));",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:          "insert into t values (1, 500)",
				ExpectedErrStr: "Data truncated for column 'e' at row 1",
			},
			{
				Query:          "insert into t values (1, -1)",
				ExpectedErrStr: "Data truncated for column 'e' at row 1",
			},
		},
	},
	{
		Name:    "enums with default, case-sensitive collation (utf8mb4_0900_bin)",
		Dialect: "mysql",
		SetUpScript: []string{
			"CREATE TABLE enumtest1 (pk int primary key, e enum('abc', 'XYZ'));",
			"CREATE TABLE enumtest2 (pk int PRIMARY KEY, e enum('x ', 'X ', 'y', 'Y'));",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "INSERT INTO enumtest1 VALUES (1, 'abc'), (2, 'abc'), (3, 'XYZ');",
				Expected: []sql.Row{{types.NewOkResult(3)}},
			},
			{
				Query:    "SELECT * FROM enumtest1;",
				Expected: []sql.Row{{1, "abc"}, {2, "abc"}, {3, "XYZ"}},
			},
			{
				// enum values must match EXACTLY for case-sensitive collations
				Query:          "INSERT INTO enumtest1 VALUES (10, 'ABC'), (11, 'aBc'), (12, 'xyz');",
				ExpectedErrStr: "Data truncated for column 'e' at row 1",
			},
			{
				Query: "SHOW CREATE TABLE enumtest1;",
				Expected: []sql.Row{{
					"enumtest1",
					"CREATE TABLE `enumtest1` (\n  `pk` int NOT NULL,\n  `e` enum('abc','XYZ'),\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				// Trailing whitespace should be removed from enum values, except when using the "binary" charset and collation
				Query: "SHOW CREATE TABLE enumtest2;",
				Expected: []sql.Row{{
					"enumtest2",
					"CREATE TABLE `enumtest2` (\n  `pk` int NOT NULL,\n  `e` enum('x','X','y','Y'),\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query: "DESCRIBE enumtest1;",
				Expected: []sql.Row{
					{"pk", "int", "NO", "PRI", nil, ""},
					{"e", "enum('abc','XYZ')", "YES", "", nil, ""}},
			},
			{
				Query: "DESCRIBE enumtest2;",
				Expected: []sql.Row{
					{"pk", "int", "NO", "PRI", nil, ""},
					{"e", "enum('x','X','y','Y')", "YES", "", nil, ""}},
			},
			{
				Query:    "select data_type, column_type from information_schema.columns where table_name='enumtest1' and column_name='e';",
				Expected: []sql.Row{{"enum", "enum('abc','XYZ')"}},
			},
			{
				Query:    "select data_type, column_type from information_schema.columns where table_name='enumtest2' and column_name='e';",
				Expected: []sql.Row{{"enum", "enum('x','X','y','Y')"}},
			},
		},
	},
	{
		Name:    "enum columns work as expected in when clauses",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table enums (e enum('a'));",
			"insert into enums values ('a');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select (case e when 'a' then 42 end) from enums",
				Expected: []sql.Row{{42}},
			},
			{
				Query:    "select (case 'a' when e then 42 end) from enums",
				Expected: []sql.Row{{42}},
			},
		},
	},
	{
		Name:    "enums with case-insensitive collation (utf8mb4_0900_ai_ci)",
		Dialect: "mysql",
		SetUpScript: []string{
			"CREATE TABLE enumtest1 (pk int primary key, e enum('abc', 'XYZ') collate utf8mb4_0900_ai_ci);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "INSERT INTO enumtest1 VALUES (1, 'abc'), (2, 'abc'), (3, 'XYZ');",
				Expected: []sql.Row{{types.NewOkResult(3)}},
			},
			{
				Query: "SHOW CREATE TABLE enumtest1;",
				Expected: []sql.Row{{
					"enumtest1",
					"CREATE TABLE `enumtest1` (\n  `pk` int NOT NULL,\n  `e` enum('abc','XYZ') COLLATE utf8mb4_0900_ai_ci,\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query: "DESCRIBE enumtest1;",
				Expected: []sql.Row{
					{"pk", "int", "NO", "PRI", nil, ""},
					{"e", "enum('abc','XYZ') COLLATE utf8mb4_0900_ai_ci", "YES", "", nil, ""}},
			},
			{
				Query:    "select data_type, column_type from information_schema.columns where table_name='enumtest1' and column_name='e';",
				Expected: []sql.Row{{"enum", "enum('abc','XYZ')"}},
			},
			{
				Query:    "CREATE TABLE enumtest2 (pk int PRIMARY KEY, e enum('x ', 'X ', 'y', 'Y'));",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "INSERT INTO enumtest1 VALUES (10, 'ABC'), (11, 'aBc'), (12, 'xyz');",
				Expected: []sql.Row{{types.NewOkResult(3)}},
			},
		},
	},
	{
		Name:    "special case for not null default enum",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table t (i int primary key, e enum('abc', 'def', 'ghi') not null);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "show create table t;",
				Expected: []sql.Row{
					{"t", "CREATE TABLE `t` (\n" +
						"  `i` int NOT NULL,\n" +
						"  `e` enum('abc','def','ghi') NOT NULL,\n" +
						"  PRIMARY KEY (`i`)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
			{
				Query: "insert into t(i) values (1)",
				Expected: []sql.Row{
					{types.NewOkResult(1)},
				},
			},
			{
				Query:       "insert into t values (2, null)",
				ExpectedErr: sql.ErrInsertIntoNonNullableProvidedNull,
			},
			{
				Skip:  true,
				Query: "insert into t values (2, default)",
				Expected: []sql.Row{
					{types.NewOkResult(1)},
				},
			},
			{
				Query: "select * from t;",
				Expected: []sql.Row{
					{1, "abc"},
				},
			},
		},
	},
	{
		Name:    "ensure that special case does not apply for nullable enums",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table t (i int primary key, e enum('abc', 'def', 'ghi'));",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "insert into t(i) values (1)",
				Expected: []sql.Row{
					{types.NewOkResult(1)},
				},
			},
			{
				Query: "select * from t;",
				Expected: []sql.Row{
					{1, nil},
				},
			},
		},
	},
	{
		Name:        "enums with default values",
		Dialect:     "mysql",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "create table bad (e enum('a') primary key default null);",
				ExpectedErr: sql.ErrIncompatibleDefaultType,
			},
			{
				Query:       "create table bad (e enum('a') default 0);",
				ExpectedErr: sql.ErrInvalidColumnDefaultValue,
			},
			{
				Query:       "create table bad (e enum('a') default '');",
				ExpectedErr: sql.ErrIncompatibleDefaultType,
			},
			{
				Query:       "create table bad (e enum('a') default '1');",
				ExpectedErr: sql.ErrInvalidColumnDefaultValue,
			},
			{
				Query:       "create table bad (e enum('a') default 1);",
				ExpectedErr: sql.ErrInvalidColumnDefaultValue,
			},

			{
				Query: "create table t1 (e enum('a') default 'a');",
				Expected: []sql.Row{
					{types.NewOkResult(0)},
				},
			},
			{
				// TODO: while this is round-trippable, it doesn't match MySQL
				Skip:  true,
				Query: "show create table t1;",
				Expected: []sql.Row{
					{"t1", "CREATE TABLE `t1` (\n" +
						"  `e` enum('a') DEFAULT 'a'\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
			{
				Query: "insert into t1 values (default);",
				Expected: []sql.Row{
					{types.NewOkResult(1)},
				},
			},
			{
				Query: "insert into t1 values ();",
				Expected: []sql.Row{
					{types.NewOkResult(1)},
				},
			},
			{
				Query: "insert into t1() values ();",
				Expected: []sql.Row{
					{types.NewOkResult(1)},
				},
			},
			{
				Query: "select * from t1 order by e;",
				Expected: []sql.Row{
					{"a"},
					{"a"},
					{"a"},
				},
			},
			{
				Query: "insert into t1 values (null)",
				Expected: []sql.Row{
					{types.NewOkResult(1)},
				},
			},
			{
				Query: "select * from t1 order by e;",
				Expected: []sql.Row{
					{nil},
					{"a"},
					{"a"},
					{"a"},
				},
			},

			{
				Query: "create table t2 (e enum('a') default (1));",
				Expected: []sql.Row{
					{types.NewOkResult(0)},
				},
			},
			{
				Query: "show create table t2;",
				Expected: []sql.Row{
					{"t2", "CREATE TABLE `t2` (\n" +
						"  `e` enum('a') DEFAULT (1)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
			{
				Query: "insert into t2 values (default);",
				Expected: []sql.Row{
					{types.NewOkResult(1)},
				},
			},
			{
				Query: "insert into t2 values ();",
				Expected: []sql.Row{
					{types.NewOkResult(1)},
				},
			},
			{
				Query: "insert into t2() values ();",
				Expected: []sql.Row{
					{types.NewOkResult(1)},
				},
			},
			{
				Query: "select * from t2 order by e;",
				Expected: []sql.Row{
					{"a"},
					{"a"},
					{"a"},
				},
			},
			{
				Query: "insert into t2 values (null)",
				Expected: []sql.Row{
					{types.NewOkResult(1)},
				},
			},
			{
				Query: "select * from t2 order by e;",
				Expected: []sql.Row{
					{nil},
					{"a"},
					{"a"},
					{"a"},
				},
			},

			{
				Query: "create table t3 (e enum('a') default ('1'));",
				Expected: []sql.Row{
					{types.NewOkResult(0)},
				},
			},
			{
				// TODO: we don't print the collation before the string
				Skip:  true,
				Query: "show create table t3;",
				Expected: []sql.Row{
					{"t3", "CREATE TABLE `t3` (\n" +
						"  `e` enum('a') DEFAULT (_utf8mb4'1')\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
			{
				Query: "insert into t3 values (default);",
				Expected: []sql.Row{
					{types.NewOkResult(1)},
				},
			},
			{
				Query: "insert into t3 values ();",
				Expected: []sql.Row{
					{types.NewOkResult(1)},
				},
			},
			{
				Query: "insert into t3() values ();",
				Expected: []sql.Row{
					{types.NewOkResult(1)},
				},
			},
			{
				Query: "select * from t3 order by e;",
				Expected: []sql.Row{
					{"a"},
					{"a"},
					{"a"},
				},
			},
			{
				Query: "insert into t3 values (null)",
				Expected: []sql.Row{
					{types.NewOkResult(1)},
				},
			},
			{
				Query: "select * from t3 order by e;",
				Expected: []sql.Row{
					{nil},
					{"a"},
					{"a"},
					{"a"},
				},
			},
		},
	},
	{
		// This is with STRICT_TRANS_TABLES or STRICT_ALL_TABLES in sql_mode
		Name:    "enums with zero",
		Dialect: "mysql",
		SetUpScript: []string{
			"SET sql_mode = 'STRICT_TRANS_TABLES';",
			"create table t (e enum('a', 'b', 'c'));",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:          "insert into t values (0);",
				ExpectedErrStr: "Data truncated for column 'e' at row 1",
			},
			{
				Query:          "insert into t values ('a'), (0), ('b');",
				ExpectedErrStr: "Data truncated for column 'e' at row 2",
			},
			{
				Query:       "create table tt (e enum('a', 'b', 'c') default 0)",
				ExpectedErr: sql.ErrInvalidColumnDefaultValue,
			},
			{
				Query: "create table et (e enum('a', 'b', '', 'c'));",
				Expected: []sql.Row{
					{types.NewOkResult(0)},
				},
			},
			{
				Query:          "insert into et values (0);",
				ExpectedErrStr: "Data truncated for column 'e' at row 1",
			},
		},
	},
	{
		Name:    "enums with zero strict all tables",
		Dialect: "mysql",
		SetUpScript: []string{
			"SET sql_mode = 'STRICT_ALL_TABLES';",
			"create table t (e enum('a', 'b', 'c'));",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:          "insert into t values (0);",
				ExpectedErrStr: "Data truncated for column 'e' at row 1",
			},
			{
				Query:          "insert into t values ('a'), (0), ('b');",
				ExpectedErrStr: "Data truncated for column 'e' at row 2",
			},
			{
				Query:       "create table tt (e enum('a', 'b', 'c') default 0)",
				ExpectedErr: sql.ErrInvalidColumnDefaultValue,
			},
		},
	},
	{
		Name:    "enums with zero non-strict mode",
		Dialect: "mysql",
		SetUpScript: []string{
			"SET sql_mode = '';",
			"create table t (e enum('a', 'b', 'c'));",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "insert into t values (0);",
				Expected: []sql.Row{
					{types.NewOkResult(1)},
				},
			},
			{
				Query: "select * from t;",
				Expected: []sql.Row{
					{""},
				},
			},
		},
	},
	{
		Name:    "enum import error message validation",
		Dialect: "mysql",
		SetUpScript: []string{
			"SET sql_mode = 'STRICT_TRANS_TABLES';",
			"CREATE TABLE shirts (name VARCHAR(40), size ENUM('x-small', 'small', 'medium', 'large', 'x-large'), color ENUM('red', 'blue'));",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "INSERT INTO shirts VALUES ('shirt1', 'x-small', 'red');",
				Expected: []sql.Row{
					{types.NewOkResult(1)},
				},
			},
			{
				Query:          "INSERT INTO shirts VALUES ('shirt2', 'other', 'green');",
				ExpectedErrStr: "Data truncated for column 'size' at row 1",
			},
		},
	},
	{
		Name:    "enum default null validation",
		Dialect: "mysql",
		SetUpScript: []string{
			"SET sql_mode = 'STRICT_TRANS_TABLES';",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "CREATE TABLE test_enum (pk int NOT NULL, e enum('a','b') DEFAULT NULL, PRIMARY KEY (pk));",
				Expected: []sql.Row{
					{types.NewOkResult(0)},
				},
			},
			{
				Query: "INSERT INTO test_enum (pk) VALUES (1);",
				Expected: []sql.Row{
					{types.NewOkResult(1)},
				},
			},
			{
				Query: "SELECT pk, e FROM test_enum;",
				Expected: []sql.Row{
					{1, nil},
				},
			},
		},
	},
	{
		// This is with STRICT_TRANS_TABLES or STRICT_ALL_TABLES in sql_mode
		Skip:    true, // TODO: Fix error type to match MySQL exactly (should be ErrInvalidColumnDefaultValue)
		Name:    "enums with empty string",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table t (e enum('a', 'b', 'c'));",
			"create table et (e enum('a', 'b', '', 'c'));",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:          "insert into t values ('');",
				ExpectedErrStr: "Data truncated for column 'e'", // TODO should be truncated error
			},
			{
				Query:       "create table tt (e enum('a', 'b', 'c') default '')",
				ExpectedErr: sql.ErrInvalidColumnDefaultValue,
			},
			{
				Query: "insert into et values (1), (2), (3), (4), ('');",
				Expected: []sql.Row{
					{types.NewOkResult(5)},
				},
			},
			{
				Query: "select e, cast(e as signed) from et order by e;",
				Expected: []sql.Row{
					{"a", 1},
					{"b", 2},
					{"", 3},
					{"", 3},
					{"c", 4},
				},
			},
		},
	},
	{
		Name:    "enum conversions",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table t (e enum('abc', 'defg', 'hijkl'));",
			"insert into t values(1), (2), (3);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select e, length(e) from t order by e;",
				Expected: []sql.Row{
					{"abc", 3},
					{"defg", 4},
					{"hijkl", 5},
				},
			},
			{
				Query: "select e, bit_length(e) from t order by e;",
				Expected: []sql.Row{
					{"abc", 24},
					{"defg", 32},
					{"hijkl", 40},
				},
			},
			{
				Query: "select e, concat(e, 'test') from t order by e;",
				Expected: []sql.Row{
					{"abc", "abctest"},
					{"defg", "defgtest"},
					{"hijkl", "hijkltest"},
				},
			},
			{
				Query: "select e, e like 'a%', e like '%g' from t order by e;",
				Expected: []sql.Row{
					{"abc", true, false},
					{"defg", false, true},
					{"hijkl", false, false},
				},
			},
			{
				Query: "select e from t where e like 'a%' order by e;",
				Expected: []sql.Row{
					{"abc"},
				},
			},
			{
				Query: "select group_concat(e order by e) as grouped from t;",
				Expected: []sql.Row{
					{"abc,defg,hijkl"},
				},
			},
			{
				Query: "select e from t where e = 'abc';",
				Expected: []sql.Row{
					{"abc"},
				},
			},
			{
				Query: "select count(*) from t where e = 'defg';",
				Expected: []sql.Row{
					{1},
				},
			},
			{
				Query: "select (case e when 'abc' then 42 end) from t order by e;",
				Expected: []sql.Row{
					{42},
					{nil},
					{nil},
				},
			},
			{
				Query: "select case when e = 'abc' then 'abc' when e = 'defg' then 123 else e end from t order by e;",
				Expected: []sql.Row{
					{"abc"},
					{"123"},
					{"hijkl"},
				},
			},
			{
				Query: "select (case 'abc' when e then 42 end) from t order by e;",
				Expected: []sql.Row{
					{42},
					{nil},
					{nil},
				},
			},
			{
				Query: "select (case e when 'abc' then e when 'defg' then e when 'hijkl' then e end) as e from t order by e;",
				Expected: []sql.Row{
					{"abc"},
					{"defg"},
					{"hijkl"},
				},
			},
			{
				// https://github.com/dolthub/dolt/issues/8598
				Query: "select (case e when 'abc' then e when 'defg' then e when 'hijkl' then 'something' end) as e from t order by e;",
				Expected: []sql.Row{
					{"abc"},
					{"defg"},
					{"something"},
				},
			},
			{
				// https://github.com/dolthub/dolt/issues/8598
				Query: "select (case e when 'abc' then e when 'defg' then e when 'hijkl' then 123 end) as e from t order by e;",
				Expected: []sql.Row{
					{"123"},
					{"abc"},
					{"defg"},
				},
			},
			{
				Query: "select e, cast(e as signed) from t order by e;",
				Expected: []sql.Row{
					{"abc", 1},
					{"defg", 2},
					{"hijkl", 3},
				},
			},
			{
				Query: "select e, cast(e as char) from t order by e;",
				Expected: []sql.Row{
					{"abc", "abc"},
					{"defg", "defg"},
					{"hijkl", "hijkl"},
				},
			},
			{
				Query: "select e, cast(e as binary) from t order by e;",
				Expected: []sql.Row{
					{"abc", []uint8("abc")},
					{"defg", []uint8("defg")},
					{"hijkl", []uint8("hijkl")},
				},
			},
			{
				Query: "select e from t where e like 'a%'",
				Expected: []sql.Row{
					{"abc"},
				},
			},
			{
				Query: "select e, cast(e as unsigned) from t order by e;",
				Expected: []sql.Row{
					{"abc", uint64(1)},
					{"defg", uint64(2)},
					{"hijkl", uint64(3)},
				},
			},
			{
				Query: "select e, cast(e as decimal) from t order by e;",
				Expected: []sql.Row{
					{"abc", "1"},
					{"defg", "2"},
					{"hijkl", "3"},
				},
			},
			{
				Query: "select e, cast(e as float) from t order by e;",
				Expected: []sql.Row{
					{"abc", float32(1)},
					{"defg", float32(2)},
					{"hijkl", float32(3)},
				},
			},
			{
				Query: "select e, cast(e as double) from t order by e;",
				Expected: []sql.Row{
					{"abc", float64(1)},
					{"defg", float64(2)},
					{"hijkl", float64(3)},
				},
			},
		},
	},
	{
		Name:    "enum conversion with system variables",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table t (e enum('ON', 'OFF', 'AUTO'));",
			"set autocommit = 'ON';",
			"insert into t values(@@autocommit), ('OFF'), ('AUTO');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select e, @@autocommit, e = @@autocommit from t order by e;",
				Expected: []sql.Row{
					{"ON", 1, true},
					{"OFF", 1, false},
					{"AUTO", 1, false},
				},
			},
			{
				Query: "select e, concat(e, @@version_comment) from t order by e;",
				Expected: []sql.Row{
					{"ON", "ONDolt"},
					{"OFF", "OFFDolt"},
					{"AUTO", "AUTODolt"},
				},
			},
		},
	},
	{
		Name:    "Convert enum columns to string columns with alter table",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table t(pk int primary key, c0 enum('a', 'b', 'c'));",
			"insert into t values(0, 'a'), (1, 'b'), (2, 'c');",
			"alter table t modify column c0 varchar(100);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select * from t",
				Expected: []sql.Row{{0, "a"}, {1, "b"}, {2, "c"}},
			},
		},
	},
	{
		// https://github.com/dolthub/dolt/issues/9613
		Skip:    true,
		Name:    "Convert enum columns to string columns when copying table",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table t(pk int primary key, c0 enum('a', 'b', 'c'));",
			"insert into t values(0, 'a'), (1, 'b'), (2, 'c');",
			"create table tt(pk int primary key, c0 varchar(10))",
			"insert into tt select * from t",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select * from tt",
				Expected: []sql.Row{{0, "a"}, {1, "b"}, {2, "c"}},
			},
		},
	},
	{
		Name:    "enums in update and delete statements",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table t (pk int primary key, e enum('abc', 'def', 'ghi'));",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "insert into t values (1, 1), (2, 3), (3, 2);",
				Expected: []sql.Row{
					{types.NewOkResult(3)},
				},
			},
			{
				Query: "update t set e = 2 where e = 'ghi';",
				Expected: []sql.Row{
					{types.OkResult{
						RowsAffected: 1,
						Info: plan.UpdateInfo{
							Matched:  1,
							Updated:  1,
							Warnings: 0,
						},
					}},
				},
			},
			{
				Query: "update t set e = 'ghi' where e = '3';",
				Expected: []sql.Row{
					{types.OkResult{
						RowsAffected: 0,
						Info: plan.UpdateInfo{
							Matched:  0,
							Updated:  0,
							Warnings: 0,
						},
					}},
				},
			},
			{
				Query: "select * from t;",
				Expected: []sql.Row{
					{1, "abc"},
					{2, "def"},
					{3, "def"},
				},
			},
			{
				Query: "delete from t where e = 2;",
				Expected: []sql.Row{
					{types.NewOkResult(2)},
				},
			},
			{
				Query: "select * from t",
				Expected: []sql.Row{
					{1, "abc"},
				},
			},
		},
	},
	{
		// https://github.com/dolthub/dolt/issues/9024
		Name:    "subqueries should coerce union types to enum",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table enum_table (i int primary key, e enum('a','b') not null)",
			"insert into enum_table values (1,'a'),(2,'b')",
			"create table uv (u int primary key, v varchar(10))",
			"insert into uv values (0, 'bug'),(1,'ant'),(3, null)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select * from (select e from enum_table union select v from uv) sq",
				Expected: []sql.Row{{"a"}, {"b"}, {"bug"}, {"ant"}, {nil}},
			},
			{
				Query:    "with a as (select e from enum_table union select v from uv) select * from a",
				Expected: []sql.Row{{"a"}, {"b"}, {"bug"}, {"ant"}, {nil}},
			},
		},
	},
}
