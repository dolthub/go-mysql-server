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

// SessionScriptTests contains self-contained script tests for session.
var SessionScriptTests = []ScriptTest{
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
		// https://github.com/dolthub/dolt/issues/9865
		Name:    "Stored procedure containing a transaction does not return EOF",
		Dialect: "mysql",
		SetUpScript: []string{
			"CREATE TABLE test_table (id INT PRIMARY KEY, name TEXT)",
			`CREATE PROCEDURE my_proc()
BEGIN
    START TRANSACTION;
    INSERT INTO test_table VALUES (1, 'test');
    COMMIT;
END`,
			`CREATE PROCEDURE empty_procedure()
BEGIN
END`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "CALL my_proc()",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 0, InsertID: 0, Info: nil}}},
			},
			{
				Query:    "SELECT * FROM test_table",
				Expected: []sql.Row{{1, "test"}},
			},
			{
				Query:    "CALL empty_procedure()",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 0, InsertID: 0, Info: nil}}},
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
		Name:    "Test cases on select into statement",
		Dialect: "mysql",
		SetUpScript: []string{
			"SELECT * FROM (VALUES ROW(22,44,88)) AS t INTO @x,@y,@z",
			"CREATE TABLE tab1 (id int primary key, v1 int)",
			"INSERT INTO tab1 VALUES (1, 1), (2, 3), (3, 6)",
			"SELECT id FROM tab1 ORDER BY id DESC LIMIT 1 INTO @myVar",
			"CREATE TABLE tab2 (i2 int primary key, s text)",
			"INSERT INTO tab2 VALUES (1, 'b'), (2, 'm'), (3, 'g')",
			"SELECT m.id, t.s FROM tab1 m JOIN tab2 t on m.id = t.i2 ORDER BY t.s DESC LIMIT 1 INTO @myId, @myText",
			// TODO: union statement does not handle order by and limit clauses
			// "SELECT id FROM tab1 UNION select s FROM tab2 LIMIT 1 INTO @myUnion",
			"SELECT id FROM tab1 WHERE id > 3 UNION select s FROM tab2 WHERE s < 'f' INTO @mustSingleVar",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:                         `SELECT 1 INTO @abc`,
				SkipResultCheckOnServerEngine: true,
				Expected:                      []sql.Row{{types.NewOkResult(1)}},
				ExpectedColumns:               nil,
			},
			{
				Query:    `SELECT @abc`,
				Expected: []sql.Row{{int8(1)}},
			},
			{
				Query:    `SELECT @z, @x, @y`,
				Expected: []sql.Row{{88, 22, 44}},
			},
			{
				Query:    `SELECT @myVar, @mustSingleVar`,
				Expected: []sql.Row{{3, "b"}},
			},
			{
				Query:    `SELECT @myId, @myText, @myUnion`,
				Expected: []sql.Row{{2, "m", nil}},
			},
			{
				Query:       `SELECT id FROM tab1 ORDER BY id DESC INTO @myvar`,
				ExpectedErr: sql.ErrMoreThanOneRow,
			},
			{
				Query:       `SELECT id INTO DUMPFILE 'baddump.out' FROM tab1 ORDER BY id DESC LIMIT 15`,
				ExpectedErr: sql.ErrMoreThanOneRow,
			},
			{
				Query:       `select 1, 2, 3 into @my1, @my2`,
				ExpectedErr: sql.ErrColumnNumberDoesNotMatch,
			},
			{
				Query:          `SELECT id, v1 INTO @myFirstVar FROM tab1 ORDER BY id DESC LIMIT 1 INTO @mySecondVar`,
				ExpectedErrStr: "Multiple INTO clauses in one query block at position 84 near '@mySecondVar'",
			},
			{
				Query:          `SELECT id FROM tab1 WHERE id > 3 UNION select s INTO @mustSingleVar FROM tab2 WHERE s < 'f' ORDER BY s DESC`,
				ExpectedErrStr: "INTO clause is not allowed at position 98 near 'ORDER'",
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
		// All DECLARE statements are only allowed under BEGIN/END blocks
		Name: "Top-level DECLARE statements",
		Assertions: []ScriptTestAssertion{
			{
				Query:       "DECLARE no_such_table CONDITION FOR SQLSTATE '42S02'",
				ExpectedErr: sql.ErrSyntaxError,
			},
			{
				Query:       "DECLARE no_such_table CONDITION FOR 1051",
				ExpectedErr: sql.ErrSyntaxError,
			},
			{
				Query:       "DECLARE a CHAR(16)",
				ExpectedErr: sql.ErrSyntaxError,
			},
			{
				Query:       "DECLARE cur2 CURSOR FOR SELECT i FROM test.t2",
				ExpectedErr: sql.ErrSyntaxError,
			},
			{
				Query:       "DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE",
				ExpectedErr: sql.ErrSyntaxError,
			},
		},
	},
	{
		Name:    "row_count() behavior",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table b (x int primary key)",
			"insert into b values (1), (2), (3), (4)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select row_count()",
				Expected: []sql.Row{{4}},
			},
			{
				Query:    "replace into b values (1)",
				Expected: []sql.Row{{types.NewOkResult(2)}},
			},
			{
				Query:    "select row_count()",
				Expected: []sql.Row{{2}},
			},
			{
				Query:    "select row_count()",
				Expected: []sql.Row{{-1}},
			},
			{
				Query:    "select count(*) from b",
				Expected: []sql.Row{{4}},
			},
			{
				Query:    "select row_count()",
				Expected: []sql.Row{{-1}},
			},
			{
				Query: "update b set x = x + 10 where x <> 2",
				Expected: []sql.Row{{types.OkResult{
					RowsAffected: 3,
					Info: plan.UpdateInfo{
						Matched: 3,
						Updated: 3,
					},
				}}},
			},
			{
				Query:    "select row_count()",
				Expected: []sql.Row{{3}},
			},
			{
				Query:    "select row_count()",
				Expected: []sql.Row{{-1}},
			},
			{
				Query:    "delete from b where x <> 2",
				Expected: []sql.Row{{types.NewOkResult(3)}},
			},
			{
				Query:    "select row_count()",
				Expected: []sql.Row{{3}},
			},
			{
				Query:    "select row_count()",
				Expected: []sql.Row{{-1}},
			},
			{
				Query:    "alter table b add column y int null",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "select row_count()",
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "select row_count()",
				Expected: []sql.Row{{-1}},
			},
		},
	},
	{
		Name:    "found_rows() behavior",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table b (x int primary key)",
			"insert into b values (1), (2), (3), (4)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select * from b where x < 2",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "select found_rows()",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "select * from b",
				Expected: []sql.Row{{1}, {2}, {3}, {4}},
			},
			{
				Query:    "select found_rows()",
				Expected: []sql.Row{{4}},
			},
			{
				Query:    "select found_rows()",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "select * from b order by x  limit 3",
				Expected: []sql.Row{{1}, {2}, {3}},
			},
			{
				Query:    "select found_rows()",
				Expected: []sql.Row{{3}},
			},
			{
				Query:    "select found_rows()",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "select * from b order by x limit 100",
				Expected: []sql.Row{{1}, {2}, {3}, {4}},
			},
			{
				Query:    "select found_rows()",
				Expected: []sql.Row{{4}},
			},
			{
				Query:    "select found_rows()",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "select sql_calc_found_rows * from b order by x limit 3",
				Expected: []sql.Row{{1}, {2}, {3}},
			},
			{
				Query:    "select found_rows()",
				Expected: []sql.Row{{4}},
			},
			{
				Query:    "select found_rows()",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "select sql_calc_found_rows * from b where x <= 2 order by x limit 1",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "select found_rows()",
				Expected: []sql.Row{{2}},
			},
			{
				Query:    "select sql_calc_found_rows * from b where x <= 2 order by x limit 1",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "insert into b values (10), (11), (12), (13)",
				Expected: []sql.Row{{types.NewOkResult(4)}},
			},
			{
				Query:    "select found_rows()",
				Expected: []sql.Row{{2}},
			},
			{
				Query:    "update b set x = x where x < 40",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 0, InsertID: 0, Info: plan.UpdateInfo{Matched: 8}}}},
			},
			{
				Query:    "select found_rows()",
				Expected: []sql.Row{{8}},
			},
			{
				Query:    "update b set x = x where x > 10",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 0, InsertID: 0, Info: plan.UpdateInfo{Matched: 3}}}},
			},
			{
				Query:    "select found_rows()",
				Expected: []sql.Row{{3}},
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
		Name: "validate_password_strength and validate_password.length",
		SetUpScript: []string{
			"set @orig = @@global.validate_password.length",
			"set @@global.validate_password.length = 0",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select validate_password_strength('')",
				Expected: []sql.Row{
					{0},
				},
			},
			{
				Query: "select validate_password_strength('123')",
				Expected: []sql.Row{
					{0},
				},
			},
			{
				Query: "select validate_password_strength('1234')",
				Expected: []sql.Row{
					{50},
				},
			},
			{
				SkipResultsCheck: true,
				Query:            "set @@global.validate_password.length = 1000",
			},
			{
				Query: "select validate_password_strength('ABCabc123!@!#')",
				Expected: []sql.Row{
					{25},
				},
			},
			{
				Query:          "set @@session.validate_password.length = 123",
				ExpectedErrStr: "Variable 'validate_password.length' is a GLOBAL variable and should be set with SET GLOBAL",
			},
			{
				SkipResultsCheck: true,
				Query:            "set @@global.validate_password.length = @orig",
			},
		},
	},
	{
		Name: "validate_password_strength and validate_password.number_count",
		SetUpScript: []string{
			"set @orig = @@global.validate_password.number_count",
			"set @@global.validate_password.number_count = 0",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select validate_password_strength('ABCabc!@#')",
				Expected: []sql.Row{
					{100},
				},
			},
			{
				SkipResultsCheck: true,
				Query:            "set @@global.validate_password.number_count = 1000",
			},
			{
				Query: "select validate_password_strength('ABCabc!!!!123456789012345678901234567890')",
				Expected: []sql.Row{
					{50},
				},
			},
			{
				Query:          "set @@session.validate_password.number_count = 123",
				ExpectedErrStr: "Variable 'validate_password.number_count' is a GLOBAL variable and should be set with SET GLOBAL",
			},
			{
				SkipResultsCheck: true,
				Query:            "set @@global.validate_password.number_count = @orig",
			},
		},
	},
	{
		Name: "validate_password_strength and validate_password.mixed_case_count",
		SetUpScript: []string{
			"set @orig = @@global.validate_password.mixed_case_count",
			"set @@global.validate_password.mixed_case_count = 0",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select validate_password_strength('abcabc!@#123')",
				Expected: []sql.Row{
					{100},
				},
			},
			{
				Query: "select validate_password_strength('ABCABC!@#123')",
				Expected: []sql.Row{
					{100},
				},
			},
			{
				SkipResultsCheck: true,
				Query:            "set @@global.validate_password.mixed_case_count = 1000",
			},
			{
				Query: "select validate_password_strength('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ123456!?!?!?')",
				Expected: []sql.Row{
					{50},
				},
			},
			{
				Query:          "set @@session.validate_password.mixed_case_count = 123",
				ExpectedErrStr: "Variable 'validate_password.mixed_case_count' is a GLOBAL variable and should be set with SET GLOBAL",
			},
			{
				SkipResultsCheck: true,
				Query:            "set @@global.validate_password.mixed_case_count = @orig",
			},
		},
	},
	{
		Name: "validate_password_strength and validate_password.special_char_count",
		SetUpScript: []string{
			"set @orig = @@global.validate_password.special_char_count",
			"set @@global.validate_password.special_char_count = 0",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select validate_password_strength('abcABC123')",
				Expected: []sql.Row{
					{100},
				},
			},
			{
				SkipResultsCheck: true,
				Query:            "set @@global.validate_password.special_char_count = 1000",
			},
			{
				Query: "select validate_password_strength('abcABC123!@#$%^&*()                            ')",
				Expected: []sql.Row{
					{50},
				},
			},
			{
				Query:          "set @@session.validate_password.special_char_count = 123",
				ExpectedErrStr: "Variable 'validate_password.special_char_count' is a GLOBAL variable and should be set with SET GLOBAL",
			},
			{
				SkipResultsCheck: true,
				Query:            "set @@global.validate_password.special_char_count = @orig",
			},
		},
	},
}
