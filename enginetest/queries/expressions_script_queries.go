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

// ExpressionsScriptTests contains self-contained expressions script tests.
var ExpressionsScriptTests = []ScriptTest{
	{
		// https://github.com/dolthub/go-mysql-server/issues/3259
		Dialect: "mysql",
		Name:    "Missing column with same name as system variable",
		SetUpScript: []string{
			"CREATE DATABASE IF NOT EXISTS test_db",
			"USE test_db",
			"CREATE TABLE A (id INT)",
			"CREATE TABLE B (id INT)",
			"INSERT INTO A VALUES (1)",
			"INSERT INTO B VALUES (2)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "SELECT UNIX_TIMESTAMP(A.timestamp) FROM A LIMIT 1",
				ExpectedErr: sql.ErrTableColumnNotFound,
			},
			{
				Query:       "SELECT A.timestamp FROM A",
				ExpectedErr: sql.ErrTableColumnNotFound,
			},
			{
				Query:       "SELECT A.version FROM A",
				ExpectedErr: sql.ErrTableColumnNotFound,
			},
			{
				Query:       "SELECT A.max_connections FROM A",
				ExpectedErr: sql.ErrTableColumnNotFound,
			},
			{
				Query:       "SELECT UPPER(A.sql_mode) FROM A",
				ExpectedErr: sql.ErrTableColumnNotFound,
			},
			{
				Query:            "SELECT @@timestamp",
				Expected:         []sql.Row{{float64(0)}},
				SkipResultsCheck: true, // dynamic var
			},
			{
				Query:            "SELECT @@version",
				Expected:         []sql.Row{{""}},
				SkipResultsCheck: true, // dynamic var
			},
			{
				Query:       "SELECT test_db.A.timestamp FROM A",
				ExpectedErr: sql.ErrTableColumnNotFound,
			},
			{
				Query:       "SELECT test_db.A.version FROM test_db.A",
				ExpectedErr: sql.ErrTableColumnNotFound,
			},
			{
				Query:       "SELECT a1.timestamp FROM A a1 JOIN B b1 ON a1.id = b1.id",
				ExpectedErr: sql.ErrTableColumnNotFound,
			},
			{
				Query:       "SELECT b1.max_connections FROM A a1 JOIN B b1 ON a1.id = b1.id",
				ExpectedErr: sql.ErrTableColumnNotFound,
			},
		},
	},
	{
		// https://github.com/dolthub/dolt/issues/9857
		Name:        "UUID_SHORT() function returns 64-bit unsigned integers with proper construction",
		Dialect:     "mysql",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT UUID_SHORT() > 0",
				Expected: []sql.Row{
					{true}, // Should return positive values
				},
			},
			{
				Query: "SELECT UUID_SHORT() != UUID_SHORT()",
				Expected: []sql.Row{
					{true}, // Should return different values on each call
				},
			},
			{
				Query: "SELECT UUID_SHORT() + 0 > 0",
				Expected: []sql.Row{
					{true}, // Should work in arithmetic expressions
				},
			},
			{
				Query: "SELECT CAST(UUID_SHORT() AS CHAR) != ''",
				Expected: []sql.Row{
					{true}, // Should cast to non-empty string
				},
			},
			{
				Query: "SELECT UUID_SHORT() BETWEEN 1 AND 18446744073709551615",
				Expected: []sql.Row{
					{true}, // Should be within uint64 range
				},
			},
			{
				Query: "SELECT (UUID_SHORT() & 0xFF00000000000000) >> 56 BETWEEN 0 AND 255",
				Expected: []sql.Row{
					{true}, // Server ID should be 0-255
				},
			},
			{
				Query: "SET @@global.server_id = 253",
				Expected: []sql.Row{
					{types.NewOkResult(0)},
				},
			},
			{
				Query: "SELECT (UUID_SHORT() & 0xFF00000000000000) >> 56 BETWEEN 0 AND 255",
				Expected: []sql.Row{
					{true}, // server time won't let us pin this down further
				},
			},
			{
				Query: "SET @@global.server_id = 1",
				Expected: []sql.Row{
					{types.NewOkResult(0)},
				},
			},
		},
	},
	{
		Name:    "UUIDs used in the wild.",
		Dialect: "mysql",
		SetUpScript: []string{
			"SET @uuid = '6ccd780c-baba-1026-9564-5b8c656024db'",
			"SET @binuuid = '0011223344556677'",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    `SELECT IS_UUID(UUID())`,
				Expected: []sql.Row{{true}},
			},
			{
				Query:    `SELECT IS_UUID(@uuid)`,
				Expected: []sql.Row{{true}},
			},
			{
				Query:    `SELECT BIN_TO_UUID(UUID_TO_BIN(@uuid))`,
				Expected: []sql.Row{{"6ccd780c-baba-1026-9564-5b8c656024db"}},
			},
			{
				Query:    `SELECT BIN_TO_UUID(UUID_TO_BIN(@uuid, 1), 1)`,
				Expected: []sql.Row{{"6ccd780c-baba-1026-9564-5b8c656024db"}},
			},
			{
				// https://github.com/dolthub/dolt/issues/11457
				Query:    `SELECT BIN_TO_UUID(UUID_TO_BIN(@uuid), null)`,
				Expected: []sql.Row{{"6ccd780c-baba-1026-9564-5b8c656024db"}},
			},
			{
				Query:    `SELECT BIN_TO_UUID(UUID_TO_BIN(@uuid), 3000)`,
				Expected: []sql.Row{{"baba1026-780c-6ccd-9564-5b8c656024db"}},
			},
			{
				Query:    `SELECT BIN_TO_UUID(UUID_TO_BIN(@uuid), -10)`,
				Expected: []sql.Row{{"baba1026-780c-6ccd-9564-5b8c656024db"}},
			},
			{
				Query:    `SELECT UUID_TO_BIN(NULL)`,
				Expected: []sql.Row{{nil}},
			},
			{
				Query:    `SELECT HEX(UUID_TO_BIN(@uuid))`,
				Expected: []sql.Row{{"6CCD780CBABA102695645B8C656024DB"}},
			},
			{
				Query:       `SELECT UUID_TO_BIN(123)`,
				ExpectedErr: sql.ErrUuidUnableToParse,
			},
			{
				Query:       `SELECT BIN_TO_UUID(123)`,
				ExpectedErr: sql.ErrUuidUnableToParse,
			},
			{
				Query:    `SELECT BIN_TO_UUID(X'00112233445566778899aabbccddeeff')`,
				Expected: []sql.Row{{"00112233-4455-6677-8899-aabbccddeeff"}},
			},
			{
				Query:    `SELECT BIN_TO_UUID('0011223344556677')`,
				Expected: []sql.Row{{"30303131-3232-3333-3434-353536363737"}},
			},
			{
				Query:    `SELECT BIN_TO_UUID(@binuuid)`,
				Expected: []sql.Row{{"30303131-3232-3333-3434-353536363737"}},
			},
		},
	},
	{
		Name: "CrossDB Queries",
		SetUpScript: []string{
			"CREATE DATABASE test",
			"CREATE TABLE test.x (pk int primary key)",
			"insert into test.x values (1),(2),(3)",
			"DELETE FROM test.x WHERE pk=2",
			"UPDATE test.x set pk=300 where pk=3",
			"create table a (xa int primary key, ya int, za int)",
			"insert into a values (1,2,3)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT pk from test.x",
				Expected: []sql.Row{{1}, {300}},
			},
			{
				Query:    "SELECT * from a",
				Expected: []sql.Row{{1, 2, 3}},
			},
		},
	},
	{
		Name:    "basic test on tables dual and `dual`",
		Dialect: "mysql",
		SetUpScript: []string{
			"CREATE TABLE `dual` (id int)",
			"INSERT INTO `dual` VALUES (2)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT * from `dual`;",
				Expected: []sql.Row{{2}},
			},
			{
				Query:    "SELECT 3 from dual;",
				Expected: []sql.Row{{3}},
			},
			{
				Dialect:     "mysql",
				Query:       "SELECT * from dual;",
				ExpectedErr: sql.ErrNoTablesUsed,
			},
		},
	},

	{
		Name:    "coalesce tests",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table c select coalesce(NULL, 1);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select * from c;",
				Expected: []sql.Row{
					{1},
				},
			},
			{
				Query: "select COLUMN_NAME, DATA_TYPE from INFORMATION_SCHEMA.COLUMNS where TABLE_NAME='c';",
				Expected: []sql.Row{
					{"coalesce(NULL, 1)", "int"},
				},
			},
		},
	},
	{
		Name: "different cases of function name should result in the same outcome",
		SetUpScript: []string{
			"create table t (b binary(2) primary key);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "select hex(*) from t;",
				ExpectedErr: sql.ErrStarUnsupported,
			},
			{
				Query:       "select HEX(*) from t;",
				ExpectedErr: sql.ErrStarUnsupported,
			},
			{
				Query:       "select HeX(*) from t;",
				ExpectedErr: sql.ErrStarUnsupported,
			},
		},
	},
	{
		Name: "Multi-db Aliasing",
		SetUpScript: []string{
			"create database db1;",
			"create table db1.t1 (i int primary key);",
			"create table db1.t2 (j int primary key);",
			"insert into db1.t1 values (1);",
			"insert into db1.t2 values (2);",

			"create database db2;",
			"create table db2.t1 (i int primary key);",
			"create table db2.t2 (j int primary key);",
			"insert into db2.t1 values (10);",
			"insert into db2.t2 values (20);",
		},
		Assertions: []ScriptTestAssertion{
			{
				// surprisingly, this works
				Query: "select db1.t1.i from db1.t1 where db1.``.i > 0",
				Expected: []sql.Row{
					{1},
				},
			},
			{
				Query: "select db1.t1.i from db1.t1 where db1.t1.i > 0",
				Expected: []sql.Row{
					{1},
				},
			},
			{
				Query: "select db1.t1.i from db1.t1 order by db1.t1.i",
				Expected: []sql.Row{
					{1},
				},
			},
			{
				Query: "select db1.t1.i from db1.t1 group by db1.t1.i",
				Expected: []sql.Row{
					{1},
				},
			},
			{
				Query: "select db1.t1.i from db1.t1 having db1.t1.i > 0",
				Expected: []sql.Row{
					{1},
				},
			},
			{
				Query: "select (select db1.t1.i from db1.t1 order by db1.t1.i)",
				Expected: []sql.Row{
					{1},
				},
			},
			{
				Query: "select i from (select db1.t1.i from db1.t1 order by db1.t1.i) as t",
				Expected: []sql.Row{
					{1},
				},
			},
			{
				Query: "with cte as (select db1.t1.i from db1.t1 order by db1.t1.i) select * from cte",
				Expected: []sql.Row{
					{1},
				},
			},
			{
				Query: "select i, j from db1.t1 inner join db2.t2 on 20 * i = j",
				Expected: []sql.Row{
					{1, 20},
				},
			},
			{
				Query: "select db1.t1.i, db2.t2.j from db1.t1 inner join db2.t2 on 20 * db1.t1.i = db2.t2.j",
				Expected: []sql.Row{
					{1, 20},
				},
			},
			{
				Query: "select i, j from db1.t1 join db2.t2 order by i, j",
				Expected: []sql.Row{
					{1, 20},
				},
			},
			{
				Query: "select i, j from db1.t1 join db2.t2 group by i order by j",
				Expected: []sql.Row{
					{1, 20},
				},
			},
			{
				Query: "select db1.t1.i, db2.t2.j from db1.t1 join db2.t2 group by db1.t1.i order by db2.t2.j",
				Expected: []sql.Row{
					{1, 20},
				},
			},
			{
				Skip:  true, // incorrectly throws Not unique table/alias: t1
				Query: "select db1.t1.i, db2.t1.i from db1.t1 join db2.t1 order by db1.t1, db2.t1.i",
				Expected: []sql.Row{
					{1, 10},
				},
			},
			{
				// Aliasing solves it
				Query: "select a.i, b.i from db1.t1 a join db2.t1 b order by a.i, b.i",
				Expected: []sql.Row{
					{1, 10},
				},
			},
		},
	},
	{
		Name: "test json search",
		SetUpScript: []string{
			`create table t (i int primary key, j json);`,
			`insert into t values (0, '{"a": "abc"}'), (1, '{"b": "abc"}'), (2, '{"c": "abc"}');`,
			`insert into t values (3, '{"d": "def"}'), (4, '{"e": "def"}'), (5, '{"f": "def"}');`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select i, json_search(j, 'all', 'abc') from t order by i",
				Expected: []sql.Row{
					{0, types.MustJSON(`"$.a"`)},
					{1, types.MustJSON(`"$.b"`)},
					{2, types.MustJSON(`"$.c"`)},
					{3, nil},
					{4, nil},
					{5, nil},
				},
			},
			{
				Query: "select i, json_search(j, 'all', 'def') from t order by i",
				Expected: []sql.Row{
					{0, nil},
					{1, nil},
					{2, nil},
					{3, types.MustJSON(`"$.d"`)},
					{4, types.MustJSON(`"$.e"`)},
					{5, types.MustJSON(`"$.f"`)},
				},
			},
			{
				Query: "select i, json_search(j, 'all', 'abc', '', '$.a', '$.b') from t order by i",
				Expected: []sql.Row{
					{0, types.MustJSON(`"$.a"`)},
					{1, types.MustJSON(`"$.b"`)},
					{2, nil},
					{3, nil},
					{4, nil},
					{5, nil},
				},
			},
		},
	},
	{
		Name:    "name_const queries",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table t (i int primary key);",
			"insert into t values (1), (2), (3);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select name_const(123, 123)",
				ExpectedColumns: sql.Schema{
					{Name: "123", Type: types.Int8, Nullable: false},
				},
				Expected: []sql.Row{
					{123},
				},
			},
			{
				Query: "select name_const('abc', 123)",
				ExpectedColumns: sql.Schema{
					{Name: "abc", Type: types.Int8, Nullable: false},
				},
				Expected: []sql.Row{
					{123},
				},
			},
			{
				Query: "select name_const('abc', 'abc')",
				ExpectedColumns: sql.Schema{
					{Name: "abc", Type: types.Text, Nullable: false},
				},
				Expected: []sql.Row{
					{"abc"},
				},
			},
			{
				Query: "select name_const(date '2000-01-02', 123)",
				ExpectedColumns: sql.Schema{
					{Name: "2000-01-02", Type: types.Int8, Nullable: false},
				},
				Expected: []sql.Row{
					{123},
				},
			},
			{
				Query: "select name_const('abc', date '2001-02-03')",
				ExpectedColumns: sql.Schema{
					{Name: "abc", Type: types.Text, Nullable: false},
				},
				Expected: []sql.Row{
					{"2001-02-03"},
				},
			},

			{
				Query:          "select name_const('abc', 1+1)",
				ExpectedErrStr: "incorrect arguments to: NAME_CONST",
			},
			{
				Query:          "select name_const(1+1, 123)",
				ExpectedErrStr: "incorrect arguments to: NAME_CONST",
			},
			{
				Query:          "select name_const(i, 123) from t",
				ExpectedErrStr: "incorrect arguments to: NAME_CONST",
			},
			{
				Query:          "select name_const(123, i) from t",
				ExpectedErrStr: "incorrect arguments to: NAME_CONST",
			},
			{
				Query:          "select name_const()",
				ExpectedErrStr: "incorrect parameter count in the call to native function NAME_CONST",
			},
			{
				Query:          "select name_const(1)",
				ExpectedErrStr: "incorrect parameter count in the call to native function NAME_CONST",
			},
			{
				Query:          "select name_const(1, 2, 3)",
				ExpectedErrStr: "incorrect parameter count in the call to native function NAME_CONST",
			},
		},
	},
	{
		Name: "coalesce with system types",
		SetUpScript: []string{
			"create table t as select @@admin_port as port1, @@port as port2, COALESCE(@@admin_port, @@port) as\n port3;",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "describe t;",
				Expected: []sql.Row{
					{"port1", "bigint", "YES", "", nil, ""},
					{"port2", "bigint", "YES", "", nil, ""},
					{"port3", "bigint", "YES", "", nil, ""},
				},
			},
		},
	},

	{
		Name:    "not expression optimization",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table t (i int);",
			"insert into t values (123);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select * from t where 1 = (not(not(i)))",
				Expected: []sql.Row{
					{123},
				},
			},
			{
				Query: "select * from t where true = (not(not(i)))",
				Expected: []sql.Row{
					{123},
				},
			},
			{
				Query: "select * from t where true = (not(not(i = 123)))",
				Expected: []sql.Row{
					{123},
				},
			},
			{
				Query: "select * from t where false = (not(not(i != 123)))",
				Expected: []sql.Row{
					{123},
				},
			},
			{
				Query: "select * from t where i != (false or i);",
				Expected: []sql.Row{
					{123},
				},
			},
			{
				Query: "select * from t where ((true and -1) >= 0);",
				Expected: []sql.Row{
					{123},
				},
			},
		},
	},
	{
		Name:    "hash tuples",
		Dialect: "mysql",
		SetUpScript: []string{
			"CREATE TABLE test (id longtext);",
			"INSERT INTO test (id) VALUES ('test_id');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM test WHERE id IN ('test_id');",
				Expected: []sql.Row{
					{"test_id"},
				},
			},
		},
	},
	{
		Name: "tinyint column does not restrict IF or IFNULL output",
		// https://github.com/dolthub/dolt/issues/9321
		SetUpScript: []string{
			"create table t0 (c0 tinyint);",
			"insert into t0 values (null);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select ifnull(t0.c0, 128) as ref0 from t0",
				Expected: []sql.Row{
					{128},
				},
			},
			{
				Query:    "select if(t0.c0 = 1, t0.c0, 128) as ref0 from t0",
				Expected: []sql.Row{{128}},
			},
		},
	},
	{
		// https://github.com/dolthub/dolt/issues/10243
		Dialect: "mysql",
		Name:    "OR filters are simplified to correct type",
		SetUpScript: []string{
			"create table t0(c1 boolean)",
			"insert into t0 values (true)",
			"create table t1(c1 int)",
			"insert into t1 values (2)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select 1 from t0 where (19 or 's') != (7 != t0.c1);",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT * FROM t1 WHERE NOT EXISTS (SELECT 1 FROM t0 WHERE ((((19)OR('s')))!=(((7)!=(t0.c1)))));",
				Expected: []sql.Row{{2}},
			},
		},
	},
	{
		Name: "Between filter",
		SetUpScript: []string{
			"create table test(x int, y int, z int);",
			`insert into test values
                     (null, 0, 0),
                     (1, null, 1),
                     (2, 2, null),
                     (3, 2, 4),
                     (4, 2, 3),
                     (5, 6, 7),
                     (6, 6, 5),
                     (7, 8, 7),
                     (8, 9, 7)`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `select x, y, z,
       						(x between y and z), (x between x and z), (x between y and x), (x between y and y),
       						(x between x and x) from test order by x`,
				Expected: []sql.Row{
					{nil, 0, 0, nil, nil, nil, nil, nil},
					{1, nil, 1, nil, true, nil, nil, true},
					{2, 2, nil, nil, nil, true, true, true},
					{3, 2, 4, true, true, true, false, true},
					{4, 2, 3, false, false, true, false, true},
					{5, 6, 7, false, true, false, false, true},
					{6, 6, 5, false, false, true, true, true},
					{7, 8, 7, false, true, false, false, true},
					{8, 9, 7, false, false, false, false, true},
				},
			},
			{
				Query:    "select x from test where (x between y and z) order by x",
				Expected: []sql.Row{{3}},
			},
			{
				Query:    "select x from test where (x between x and z) order by x",
				Expected: []sql.Row{{1}, {3}, {5}, {7}},
			},
			{
				Query:    "select x from test where (x between y and x) order by x",
				Expected: []sql.Row{{2}, {3}, {4}, {6}},
			},
			{
				Query:    "select x from test where (x between y and y) order by x",
				Expected: []sql.Row{{2}, {6}},
			},
			{
				Query:    "select x from test where (x between x and x) order by x",
				Expected: []sql.Row{{1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}},
			},
		},
	},
}
