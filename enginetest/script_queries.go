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

package enginetest

import (
	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/sql/analyzer"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

type ScriptTest struct {
	// Name of the script test
	Name string
	// The sql statements to execute as setup, in order. Results are not checked, but statements must not error.
	SetUpScript []string
	// The set of assertions to make after setup, in order
	Assertions []ScriptTestAssertion
	// For tests that make a single assertion, Query can be set for the single assertion
	Query string
	// For tests that make a single assertion, Expected can be set for the single assertion
	Expected []sql.Row
	// For tests that make a single assertion, ExpectedErr can be set for the expected error
	ExpectedErr *errors.Kind
}

type ScriptTestAssertion struct {
	Query       string
	Expected    []sql.Row
	ExpectedErr *errors.Kind
	// ExpectedErrStr should be set for tests that expect a specific error string this is not linked to a custom error.
	// In most cases, errors should be linked to a custom error, however there are exceptions where this is not possible,
	// such as the use of the SIGNAL statement.
	ExpectedErrStr string

	// ExpectedWarning is used for queries that should generate warnings but not errors.
	ExpectedWarning int
}

// ScriptTests are a set of test scripts to run.
// Unlike other engine tests, ScriptTests must be self-contained. No other tables are created outside the definition of
// the tests.
var ScriptTests = []ScriptTest{
	{
		Name: "failed statements data validation for INSERT, UPDATE",
		SetUpScript: []string{
			"CREATE TABLE test (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX (v1));",
			"INSERT INTO test VALUES (1,1), (4,4), (5,5);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:          "INSERT INTO test VALUES (2,2), (3,3), (1,1);",
				ExpectedErrStr: "duplicate primary key given: [1]",
			},
			{
				Query:    "SELECT * FROM test;",
				Expected: []sql.Row{{1, 1}, {4, 4}, {5, 5}},
			},
			{
				Query:          "UPDATE test SET pk = pk + 1;",
				ExpectedErrStr: "duplicate primary key given: [5]",
			},
			{
				Query:    "SELECT * FROM test;",
				Expected: []sql.Row{{1, 1}, {4, 4}, {5, 5}},
			},
		},
	},
	{
		Name: "failed statements data validation for DELETE, REPLACE",
		SetUpScript: []string{
			"CREATE TABLE test (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX (v1));",
			"INSERT INTO test VALUES (1,1), (4,4), (5,5);",
			"CREATE TABLE test2 (pk BIGINT PRIMARY KEY, CONSTRAINT fk_test FOREIGN KEY (pk) REFERENCES test (v1));",
			"INSERT INTO test2 VALUES (4);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "DELETE FROM test WHERE pk > 0;",
				ExpectedErr: sql.ErrForeignKeyChildViolation,
			},
			{
				Query:    "SELECT * FROM test;",
				Expected: []sql.Row{{1, 1}, {4, 4}, {5, 5}},
			},
			{
				Query:    "SELECT * FROM test2;",
				Expected: []sql.Row{{4}},
			},
			{
				Query:       "REPLACE INTO test VALUES (1,7), (4,8), (5,9);",
				ExpectedErr: sql.ErrForeignKeyChildViolation,
			},
			{
				Query:    "SELECT * FROM test;",
				Expected: []sql.Row{{1, 1}, {4, 4}, {5, 5}},
			},
			{
				Query:    "SELECT * FROM test2;",
				Expected: []sql.Row{{4}},
			},
		},
	},
	{
		Name: "delete with in clause",
		SetUpScript: []string{
			"create table a (x int primary key)",
			"insert into a values (1), (3), (5)",
			"delete from a where x in (1, 3)",
		},
		Query: "select x from a order by 1",
		Expected: []sql.Row{
			{5},
		},
	},
	{
		Name: "sqllogictest evidence/slt_lang_aggfunc.test",
		SetUpScript: []string{
			"CREATE TABLE t1( x INTEGER, y VARCHAR(8) )",
			"INSERT INTO t1 VALUES(1,'true')",
			"INSERT INTO t1 VALUES(0,'false')",
			"INSERT INTO t1 VALUES(NULL,'NULL')",
		},
		Query: "SELECT count(DISTINCT x) FROM t1",
		Expected: []sql.Row{
			{2},
		},
	},
	{
		Name: "sqllogictest index/commute/10/slt_good_1.test",
		SetUpScript: []string{
			"CREATE TABLE tab0(pk INTEGER PRIMARY KEY, col0 INTEGER, col1 FLOAT, col2 TEXT, col3 INTEGER, col4 FLOAT, col5 TEXT)",
			"INSERT INTO tab0 VALUES(0,42,58.92,'fnbtk',54,68.41,'xmttf')",
			"INSERT INTO tab0 VALUES(1,31,46.55,'sksjf',46,53.20,'wiuva')",
			"INSERT INTO tab0 VALUES(2,30,31.11,'oldqn',41,5.26,'ulaay')",
			"INSERT INTO tab0 VALUES(3,77,44.90,'pmsir',70,84.14,'vcmyo')",
			"INSERT INTO tab0 VALUES(4,23,95.26,'qcwxh',32,48.53,'rvtbr')",
			"INSERT INTO tab0 VALUES(5,43,6.75,'snvwg',3,14.38,'gnfxz')",
			"INSERT INTO tab0 VALUES(6,47,98.26,'bzzva',60,15.2,'imzeq')",
			"INSERT INTO tab0 VALUES(7,98,40.9,'lsrpi',78,66.30,'ephwy')",
			"INSERT INTO tab0 VALUES(8,19,15.16,'ycvjz',55,38.70,'dnkkz')",
			"INSERT INTO tab0 VALUES(9,7,84.4,'ptovf',17,2.46,'hrxsf')",
			"CREATE TABLE tab1(pk INTEGER PRIMARY KEY, col0 INTEGER, col1 FLOAT, col2 TEXT, col3 INTEGER, col4 FLOAT, col5 TEXT)",
			"CREATE INDEX idx_tab1_0 on tab1 (col0)",
			"CREATE INDEX idx_tab1_1 on tab1 (col1)",
			"CREATE INDEX idx_tab1_3 on tab1 (col3)",
			"CREATE INDEX idx_tab1_4 on tab1 (col4)",
			"INSERT INTO tab1 SELECT * FROM tab0",
			"CREATE TABLE tab2(pk INTEGER PRIMARY KEY, col0 INTEGER, col1 FLOAT, col2 TEXT, col3 INTEGER, col4 FLOAT, col5 TEXT)",
			"CREATE UNIQUE INDEX idx_tab2_1 ON tab2 (col4 DESC,col3)",
			"CREATE UNIQUE INDEX idx_tab2_2 ON tab2 (col3 DESC,col0)",
			"CREATE UNIQUE INDEX idx_tab2_3 ON tab2 (col3 DESC,col1)",
			"INSERT INTO tab2 SELECT * FROM tab0",
			"CREATE TABLE tab3(pk INTEGER PRIMARY KEY, col0 INTEGER, col1 FLOAT, col2 TEXT, col3 INTEGER, col4 FLOAT, col5 TEXT)",
			"CREATE INDEX idx_tab3_0 ON tab3 (col3 DESC)",
			"INSERT INTO tab3 SELECT * FROM tab0",
			"CREATE TABLE tab4(pk INTEGER PRIMARY KEY, col0 INTEGER, col1 FLOAT, col2 TEXT, col3 INTEGER, col4 FLOAT, col5 TEXT)",
			"CREATE INDEX idx_tab4_0 ON tab4 (col0 DESC)",
			"CREATE UNIQUE INDEX idx_tab4_2 ON tab4 (col4 DESC,col3)",
			"CREATE INDEX idx_tab4_3 ON tab4 (col3 DESC)",
			"INSERT INTO tab4 SELECT * FROM tab0",
		},
		Query: "SELECT pk FROM tab2 WHERE 78 < col0 AND 19 < col3",
		Expected: []sql.Row{
			{7},
		},
	},
	{
		Name: "3 tables, linear join",
		SetUpScript: []string{
			"create table a (xa int primary key, ya int, za int)",
			"create table b (xb int primary key, yb int, zb int)",
			"create table c (xc int primary key, yc int, zc int)",
			"insert into a values (1,2,3)",
			"insert into b values (1,2,3)",
			"insert into c values (1,2,3)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select ya from a join b on ya - 1= xb join c on xc = zb - 2",
				Expected: []sql.Row{{2}},
			},
		},
	},
	{
		Name: "3 tables, v join",
		SetUpScript: []string{
			"create table a (xa int primary key, ya int, za int)",
			"create table b (xb int primary key, yb int, zb int)",
			"create table c (xc int primary key, yc int, zc int)",
			"insert into a values (1,2,3)",
			"insert into b values (1,2,3)",
			"insert into c values (1,2,3)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select za from a join b on ya - 1 = xb join c on xa = xc",
				Expected: []sql.Row{{3}},
			},
		},
	},
	{
		Name: "3 tables, linear join, indexes on A,C",
		SetUpScript: []string{
			"create table a (xa int primary key, ya int, za int)",
			"create table b (xb int primary key, yb int, zb int)",
			"create table c (xc int primary key, yc int, zc int)",
			"insert into a values (1,2,3)",
			"insert into b values (1,2,3)",
			"insert into c values (1,2,3)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select xa from a join b on xa = yb - 1 join c on yb - 1 = xc",
				Expected: []sql.Row{{1}},
			},
		},
	},
	{
		Name: "4 tables, linear join",
		SetUpScript: []string{
			"create table a (xa int primary key, ya int, za int)",
			"create table b (xb int primary key, yb int, zb int)",
			"create table c (xc int primary key, yc int, zc int)",
			"create table d (xd int primary key, yd int, zd int)",
			"insert into a values (1,2,3)",
			"insert into b values (1,2,3)",
			"insert into c values (1,2,3)",
			"insert into d values (1,2,3)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select xa from a join b on ya - 1 = xb join c on xb = xc join d on xc = xd",
				Expected: []sql.Row{{1}},
			},
		},
	},
	{
		Name: "4 tables, linear join, index on D",
		SetUpScript: []string{
			"create table a (xa int primary key, ya int, za int)",
			"create table b (xb int primary key, yb int, zb int)",
			"create table c (xc int primary key, yc int, zc int)",
			"create table d (xd int primary key, yd int, zd int)",
			"insert into a values (1,2,3)",
			"insert into b values (1,2,3)",
			"insert into c values (1,2,3)",
			"insert into d values (1,2,3)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select xa from a join b on ya = yb join c on yb = yc join d on yc - 1 = xd",
				Expected: []sql.Row{{1}},
			},
		},
	},
	{
		Name: "4 tables, left join, indexes on all tables",
		SetUpScript: []string{
			"create table a (xa int primary key, ya int, za int)",
			"create table b (xb int primary key, yb int, zb int)",
			"create table c (xc int primary key, yc int, zc int)",
			"create table d (xd int primary key, yd int, zd int)",
			"insert into a values (1,2,3)",
			"insert into b values (1,2,3)",
			"insert into c values (1,2,3)",
			"insert into d values (1,2,3)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select xa from a left join b on ya = yb left join c on yb = yc left join d on yc - 1 = xd",
				Expected: []sql.Row{{1}},
			},
		},
	},
	{
		Name: "4 tables, linear join, index on B, D",
		SetUpScript: []string{
			"create table a (xa int primary key, ya int, za int)",
			"create table b (xb int primary key, yb int, zb int)",
			"create table c (xc int primary key, yc int, zc int)",
			"create table d (xd int primary key, yd int, zd int)",
			"insert into a values (1,2,3)",
			"insert into b values (1,2,3)",
			"insert into c values (1,2,3)",
			"insert into d values (1,2,3)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select xa from a join b on ya - 1 = xb join c on yc = za - 1 join d on yc - 1 = xd",
				Expected: []sql.Row{{1}},
			},
		},
	},
	{
		Name: "4 tables, all joined to A",
		SetUpScript: []string{
			"create table a (xa int primary key, ya int, za int)",
			"create table b (xb int primary key, yb int, zb int)",
			"create table c (xc int primary key, yc int, zc int)",
			"create table d (xd int primary key, yd int, zd int)",
			"insert into a values (1,2,3)",
			"insert into b values (1,2,3)",
			"insert into c values (1,2,3)",
			"insert into d values (1,2,3)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select xa from a join b on xa = xb join c on ya - 1 = xc join d on za - 2 = xd",
				Expected: []sql.Row{{1}},
			},
		},
	},
	// {
	// 	Name: "4 tables, all joined to D",
	// 	SetUpScript: []string{
	// 		"create table a (xa int primary key, ya int, za int)",
	// 		"create table b (xb int primary key, yb int, zb int)",
	// 		"create table c (xc int primary key, yc int, zc int)",
	// 		"create table d (xd int primary key, yd int, zd int)",
	// 		"insert into a values (1,2,3)",
	// 		"insert into b values (1,2,3)",
	// 		"insert into c values (1,2,3)",
	// 		"insert into d values (1,2,3)",
	// 	},
	// 	Assertions: []ScriptTestAssertion{
	// 		{
	// 			// gives an error in mysql, a needs an alias
	// 			Query: "select xa from d join a on yd = xa join c on yd = xc join a on xa = yd",
	// 			Expected: []sql.Row{{1}},
	// 		},
	// 	},
	// },
	{
		Name: "4 tables, all joined to D",
		SetUpScript: []string{
			"create table a (xa int primary key, ya int, za int)",
			"create table b (xb int primary key, yb int, zb int)",
			"create table c (xc int primary key, yc int, zc int)",
			"create table d (xd int primary key, yd int, zd int)",
			"insert into a values (1,2,3)",
			"insert into b values (1,2,3)",
			"insert into c values (1,2,3)",
			"insert into d values (1,2,3)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select xa from d join a on yd - 1 = xa join c on zd - 2 = xc join b on xb = zd - 2",
				Expected: []sql.Row{{1}},
			},
		},
	},
	{
		Name: "5 tables, complex join conditions",
		SetUpScript: []string{
			"create table a (xa int primary key, ya int, za int)",
			"create table b (xb int primary key, yb int, zb int)",
			"create table c (xc int primary key, yc int, zc int)",
			"create table d (xd int primary key, yd int, zd int)",
			"create table e (xe int, ye int, ze int, primary key(xe, ye))",
			"insert into a values (1,2,3)",
			"insert into b values (1,2,3)",
			"insert into c values (1,2,3)",
			"insert into d values (1,2,3)",
			"insert into e values (1,2,3)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `select xa from a
									join b on ya - 1 = xb
									join c on xc = za - 2
									join d on xd = yb - 1
									join e on xe = zb - 2 and ye = yc`,
				Expected: []sql.Row{{1}},
			},
		},
	},
	{
		Name: "UUIDs used in the wild.",
		SetUpScript: []string{
			"SET @uuid = '6ccd780c-baba-1026-9564-5b8c656024db'",
			"SET @binuuid = '0011223344556677'",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    `SELECT IS_UUID(UUID())`,
				Expected: []sql.Row{{int8(1)}},
			},
			{
				Query:    `SELECT IS_UUID(@uuid)`,
				Expected: []sql.Row{{int8(1)}},
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
		Name: "last_insert_id() behavior",
		SetUpScript: []string{
			"create table a (x int primary key auto_increment, y int)",
			"create table b (x int primary key)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select last_insert_id()",
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "insert into a (y) values (1)",
				Expected: []sql.Row{{sql.NewOkResult(1)}},
			},
			{
				Query:    "select last_insert_id()",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "insert into a (y) values (2), (3)",
				Expected: []sql.Row{{sql.NewOkResult(2)}},
			},
			{
				Query:    "select last_insert_id()",
				Expected: []sql.Row{{2}},
			},
			{
				Query:    "insert into b (x) values (1), (2)",
				Expected: []sql.Row{{sql.NewOkResult(2)}},
			},
			{
				Query:    "select last_insert_id()",
				Expected: []sql.Row{{2}},
			},
		},
	},
	{
		Name: "row_count() behavior",
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
				Expected: []sql.Row{{sql.NewOkResult(2)}},
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
				Expected: []sql.Row{{sql.OkResult{
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
				Expected: []sql.Row{{sql.NewOkResult(3)}},
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
				Expected: []sql.Row{},
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
		Name: "found_rows() behavior",
		SetUpScript: []string{
			"create table b (x int primary key)",
			"insert into b values (1), (2), (3), (4)",
		},
		Assertions: []ScriptTestAssertion{
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
				Expected: []sql.Row{{sql.NewOkResult(4)}},
			},
			{
				Query:    "select found_rows()",
				Expected: []sql.Row{{2}},
			},
			{
				Query:    "update b set x = x where x < 40",
				Expected: []sql.Row{{sql.OkResult{RowsAffected: 0, InsertID: 0, Info: plan.UpdateInfo{Matched: 8}}}},
			},
			{
				Query:    "select found_rows()",
				Expected: []sql.Row{{8}},
			},
			{
				Query:    "update b set x = x where x > 10",
				Expected: []sql.Row{{sql.OkResult{RowsAffected: 0, InsertID: 0, Info: plan.UpdateInfo{Matched: 3}}}},
			},
			{
				Query:    "select found_rows()",
				Expected: []sql.Row{{3}},
			},
		},
	},
	{
		Name: "INSERT INTO ... SELECT with AUTO_INCREMENT",
		SetUpScript: []string{
			"create table ai (pk int primary key auto_increment, c0 int);",
			"create table other (pk int primary key);",
			"insert into other values (1), (2), (3)",
			"insert into ai (c0) select * from other order by other.pk;",
		},
		Query: "select * from ai;",
		Expected: []sql.Row{
			{1, 1},
			{2, 2},
			{3, 3},
		},
	},
	{
		Name: "Indexed Join On Keyless Table",
		SetUpScript: []string{
			"create table l (pk int primary key, c0 int, c1 int);",
			"create table r (c0 int, c1 int, third int);",
			"create index r_c0 on r (c0);",
			"create index r_c1 on r (c1);",
			"create index r_third on r (third);",
			"insert into l values (0, 0, 0), (1, 0, 1), (2, 1, 0), (3, 0, 2), (4, 2, 0), (5, 1, 2), (6, 2, 1), (7, 2, 2);",
			"insert into l values (256, 1024, 4096);",
			"insert into r values (1, 1, -1), (2, 2, -1), (2, 2, -1);",
			"insert into r values (-1, -1, 256);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select pk, l.c0, l.c1 from l join r on l.c0 = r.c0 or l.c1 = r.c1 order by 1, 2, 3;",
				Expected: []sql.Row{
					{1, 0, 1},
					{2, 1, 0},
					{3, 0, 2},
					{3, 0, 2},
					{4, 2, 0},
					{4, 2, 0},
					{5, 1, 2},
					{5, 1, 2},
					{5, 1, 2},
					{6, 2, 1},
					{6, 2, 1},
					{6, 2, 1},
					{7, 2, 2},
					{7, 2, 2},
				},
			},
			{
				Query: "select pk, l.c0, l.c1 from l join r on l.c0 = r.c0 or l.c1 = r.c1 or l.pk = r.third order by 1, 2, 3;",
				Expected: []sql.Row{
					{1, 0, 1},
					{2, 1, 0},
					{3, 0, 2},
					{3, 0, 2},
					{4, 2, 0},
					{4, 2, 0},
					{5, 1, 2},
					{5, 1, 2},
					{5, 1, 2},
					{6, 2, 1},
					{6, 2, 1},
					{6, 2, 1},
					{7, 2, 2},
					{7, 2, 2},
					{256, 1024, 4096},
				},
			},
			{
				Query: "select pk, l.c0, l.c1 from l join r on l.c0 = r.c0 or l.c1 < 4 and l.c1 = r.c1 or l.c1 >= 4 and l.c1 = r.c1 order by 1, 2, 3;",
				Expected: []sql.Row{
					{1, 0, 1},
					{2, 1, 0},
					{3, 0, 2},
					{3, 0, 2},
					{4, 2, 0},
					{4, 2, 0},
					{5, 1, 2},
					{5, 1, 2},
					{5, 1, 2},
					{6, 2, 1},
					{6, 2, 1},
					{6, 2, 1},
					{7, 2, 2},
					{7, 2, 2},
				},
			},
		},
	},
	{
		Name: "Group Concat Queries",
		SetUpScript: []string{
			"CREATE TABLE x (pk int)",
			"INSERT INTO x VALUES (1),(2),(3),(4),(NULL)",

			"create table t (o_id int, attribute longtext, value longtext)",
			"INSERT INTO t VALUES (2, 'color', 'red'), (2, 'fabric', 'silk')",
			"INSERT INTO t VALUES (3, 'color', 'green'), (3, 'shape', 'square')",

			"create table nulls(pk int)",
			"INSERT INTO nulls VALUES (NULL)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    `SELECT group_concat(pk ORDER BY pk) FROM x;`,
				Expected: []sql.Row{{"1,2,3,4"}},
			},
			{
				Query:    `SELECT group_concat(DISTINCT pk ORDER BY pk) FROM x;`,
				Expected: []sql.Row{{"1,2,3,4"}},
			},
			{
				Query:    `SELECT group_concat(DISTINCT pk ORDER BY pk SEPARATOR '-') FROM x;`,
				Expected: []sql.Row{{"1-2-3-4"}},
			},
			{
				Query:    `SELECT group_concat(attribute ORDER BY attribute) FROM t group by o_id`,
				Expected: []sql.Row{{"color,fabric"}, {"color,shape"}},
			},
			{
				Query:    `SELECT group_concat(DISTINCT attribute ORDER BY value DESC SEPARATOR ';') FROM t group by o_id`,
				Expected: []sql.Row{{"fabric;color"}, {"shape;color"}},
			},
			{
				Query:    `SELECT group_concat(DISTINCT attribute ORDER BY attribute) FROM t`,
				Expected: []sql.Row{{"color,fabric,shape"}},
			},
			{
				Query:    `SELECT group_concat(attribute ORDER BY attribute) FROM t`,
				Expected: []sql.Row{{"color,color,fabric,shape"}},
			},
			{
				Query:    `SELECT group_concat((SELECT 2)) FROM x;`,
				Expected: []sql.Row{{"2,2,2,2,2"}},
			},
			{
				Query:    `SELECT group_concat(DISTINCT (SELECT 2)) FROM x;`,
				Expected: []sql.Row{{"2"}},
			},
			{
				Query:    `SELECT group_concat(DISTINCT attribute ORDER BY attribute ASC) FROM t`,
				Expected: []sql.Row{{"color,fabric,shape"}},
			},
			{
				Query:    `SELECT group_concat(DISTINCT attribute ORDER BY attribute DESC) FROM t`,
				Expected: []sql.Row{{"shape,fabric,color"}},
			},
			{
				Query:    `SELECT group_concat(pk) FROM nulls`,
				Expected: []sql.Row{{nil}},
			},
			{
				Query:       `SELECT group_concat((SELECT * FROM t LIMIT 1)) from t`,
				ExpectedErr: sql.ErrSubqueryMultipleColumns,
			},
			{
				Query:       `SELECT group_concat((SELECT * FROM x)) from t`,
				ExpectedErr: sql.ErrExpectedSingleRow,
			},
			{
				Query:    `SELECT group_concat(attribute) FROM t where o_id=2`,
				Expected: []sql.Row{{"color,fabric"}},
			},
			{
				Query:    `SELECT group_concat(DISTINCT attribute ORDER BY value DESC SEPARATOR ';') FROM t group by o_id`,
				Expected: []sql.Row{{"fabric;color"}, {"shape;color"}},
			},
			{
				Query:    `SELECT group_concat(o_id) FROM t WHERE attribute='color'`,
				Expected: []sql.Row{{"2,3"}},
			},
		},
	},
	{
		Name: "ALTER TABLE ... ALTER COLUMN SET / DROP DEFAULT",
		SetUpScript: []string{
			"CREATE TABLE test (pk BIGINT PRIMARY KEY, v1 BIGINT NOT NULL DEFAULT 88);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "INSERT INTO test (pk) VALUES (1);",
				Expected: []sql.Row{{sql.NewOkResult(1)}},
			},
			{
				Query:    "SELECT * FROM test;",
				Expected: []sql.Row{{1, 88}},
			},
			{
				Query:    "ALTER TABLE test ALTER v1 SET DEFAULT (CONVERT('42', SIGNED));",
				Expected: []sql.Row{},
			},
			{
				Query:    "INSERT INTO test (pk) VALUES (2);",
				Expected: []sql.Row{{sql.NewOkResult(1)}},
			},
			{
				Query:    "SELECT * FROM test;",
				Expected: []sql.Row{{1, 88}, {2, 42}},
			},
			{
				Query:       "ALTER TABLE test ALTER v2 SET DEFAULT 1;",
				ExpectedErr: sql.ErrTableColumnNotFound,
			},
			{
				Query:    "ALTER TABLE test ALTER v1 DROP DEFAULT;",
				Expected: []sql.Row{},
			},
			{
				Query:       "INSERT INTO test (pk) VALUES (3);",
				ExpectedErr: sql.ErrInsertIntoNonNullableDefaultNullColumn,
			},
			{
				Query:       "ALTER TABLE test ALTER v2 DROP DEFAULT;",
				ExpectedErr: sql.ErrTableColumnNotFound,
			},
			{ // Just confirms that the last INSERT didn't do anything
				Query:    "SELECT * FROM test;",
				Expected: []sql.Row{{1, 88}, {2, 42}},
			},
		},
	},
	{
		Name: "Run through some complex queries with DISTINCT and aggregates",
		SetUpScript: []string{
			"CREATE TABLE tab1(col0 INTEGER, col1 INTEGER, col2 INTEGER)",
			"CREATE TABLE tab2(col0 INTEGER, col1 INTEGER, col2 INTEGER)",
			"INSERT INTO tab1 VALUES(51,14,96)",
			"INSERT INTO tab1 VALUES(85,5,59)",
			"INSERT INTO tab1 VALUES(91,47,68)",
			"INSERT INTO tab2 VALUES(64,77,40)",
			"INSERT INTO tab2 VALUES(75,67,58)",
			"INSERT INTO tab2 VALUES(46,51,23)",
			"CREATE TABLE mytable (pk int, v1 int)",
			"INSERT INTO mytable VALUES(1,1)",
			"INSERT INTO mytable VALUES(1,1)",
			"INSERT INTO mytable VALUES(1,2)",
			"INSERT INTO mytable VALUES(2,2)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT - SUM( DISTINCT - - 71 ) AS col2 FROM tab2 cor0",
				Expected: []sql.Row{{float64(-71)}},
			},
			{
				Query:    "SELECT - SUM ( DISTINCT - - 71 ) AS col2 FROM tab2 cor0",
				Expected: []sql.Row{{float64(-71)}},
			},
			{
				Query:    "SELECT + MAX( DISTINCT ( - col0 ) ) FROM tab1 AS cor0",
				Expected: []sql.Row{{-51}},
			},
			{
				Query:    "SELECT SUM( DISTINCT + col1 ) * - 22 - - ( - COUNT( * ) ) col0 FROM tab1 AS cor0",
				Expected: []sql.Row{{float64(-1455)}},
			},
			{
				Query:    "SELECT MIN (DISTINCT col1) from tab1 GROUP BY col0 ORDER BY col0",
				Expected: []sql.Row{{14}, {5}, {47}},
			},
			{
				Query:    "SELECT SUM (DISTINCT col1) from tab1 GROUP BY col0 ORDER BY col0",
				Expected: []sql.Row{{float64(14)}, {float64(5)}, {float64(47)}},
			},
			{
				Query:    "SELECT pk, SUM(DISTINCT v1), MAX(v1) FROM mytable GROUP BY pk",
				Expected: []sql.Row{{int64(1), float64(3), int64(2)}, {int64(2), float64(2), int64(2)}},
			},
			{
				Query:    "SELECT pk, MIN(DISTINCT v1), MAX(DISTINCT v1) FROM mytable GROUP BY pk",
				Expected: []sql.Row{{int64(1), int64(1), int64(2)}, {int64(2), int64(2), int64(2)}},
			},
			{
				Query:    "SELECT SUM(DISTINCT pk * v1) from mytable",
				Expected: []sql.Row{{float64(7)}},
			},
			{
				Query:    "SELECT SUM(DISTINCT POWER(v1, 2)) FROM mytable",
				Expected: []sql.Row{{float64(5)}},
			},
			{
				Query:    "SELECT + + 97 FROM tab1 GROUP BY tab1.col1",
				Expected: []sql.Row{{97}, {97}, {97}},
			},
			{
				Query:    "SELECT rand(10) FROM tab1 GROUP BY tab1.col1",
				Expected: []sql.Row{{0.5660920659323543}, {0.5660920659323543}, {0.5660920659323543}},
			},
			{
				Query:    "SELECT ALL - cor0.col0 * + cor0.col0 AS col2 FROM tab1 AS cor0 GROUP BY cor0.col0",
				Expected: []sql.Row{{-2601}, {-7225}, {-8281}},
			},
			{
				Query:    "SELECT cor0.col0 * cor0.col0 + cor0.col0 AS col2 FROM tab1 AS cor0 GROUP BY cor0.col0 order by 1",
				Expected: []sql.Row{{2652}, {7310}, {8372}},
			},
			{
				Query:    "SELECT - floor(cor0.col0) * ceil(cor0.col0) AS col2 FROM tab1 AS cor0 GROUP BY cor0.col0",
				Expected: []sql.Row{{-2601}, {-7225}, {-8281}},
			},
			{
				Query:    "SELECT col0 FROM tab1 AS cor0 GROUP BY cor0.col0",
				Expected: []sql.Row{{51}, {85}, {91}},
			},
			{
				Query:    "SELECT - cor0.col0 FROM tab1 AS cor0 GROUP BY cor0.col0",
				Expected: []sql.Row{{-51}, {-85}, {-91}},
			},
			{
				Query:    "SELECT col0 BETWEEN 2 and 4 from tab1 group by col0",
				Expected: []sql.Row{{false}, {false}, {false}},
			},
			{
				Query:       "SELECT col0, col1 FROM tab1 GROUP by col0;",
				ExpectedErr: analyzer.ErrValidationGroupBy,
			},
			{
				Query:       "SELECT col0, floor(col1) FROM tab1 GROUP by col0;",
				ExpectedErr: analyzer.ErrValidationGroupBy,
			},
			{
				Query:       "SELECT floor(cor0.col1) * ceil(cor0.col0) AS col2 FROM tab1 AS cor0 GROUP BY cor0.col0",
				ExpectedErr: analyzer.ErrValidationGroupBy,
			},
		},
	},
	{
		Name: "Nested Subquery projections (NTC)",
		SetUpScript: []string{
			`CREATE TABLE dcim_site (id char(32) NOT NULL,created date,last_updated datetime,_custom_field_data json NOT NULL,name varchar(100) NOT NULL,_name varchar(100) NOT NULL,slug varchar(100) NOT NULL,facility varchar(50) NOT NULL,asn bigint,time_zone varchar(63) NOT NULL,description varchar(200) NOT NULL,physical_address varchar(200) NOT NULL,shipping_address varchar(200) NOT NULL,latitude decimal(8,6),longitude decimal(9,6),contact_name varchar(50) NOT NULL,contact_phone varchar(20) NOT NULL,contact_email varchar(254) NOT NULL,comments longtext NOT NULL,region_id char(32),status_id char(32),tenant_id char(32),PRIMARY KEY (id),KEY dcim_site_region_id_45210932 (region_id),KEY dcim_site_status_id_e6a50f56 (status_id),KEY dcim_site_tenant_id_15e7df63 (tenant_id),UNIQUE KEY name (name),UNIQUE KEY slug (slug)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`,
			`CREATE TABLE dcim_rackgroup (id char(32) NOT NULL,created date,last_updated datetime,_custom_field_data json NOT NULL,name varchar(100) NOT NULL,slug varchar(100) NOT NULL,description varchar(200) NOT NULL,lft int unsigned NOT NULL,rght int unsigned NOT NULL,tree_id int unsigned NOT NULL,level int unsigned NOT NULL,parent_id char(32),site_id char(32) NOT NULL,PRIMARY KEY (id),KEY dcim_rackgroup_parent_id_cc315105 (parent_id),KEY dcim_rackgroup_site_id_13520e89 (site_id),KEY dcim_rackgroup_slug_3f4582a7 (slug),KEY dcim_rackgroup_tree_id_9c2ad6f4 (tree_id),UNIQUE KEY site_idname (site_id,name),UNIQUE KEY site_idslug (site_id,slug),CONSTRAINT dcim_rackgroup_parent_id_cc315105_fk_dcim_rackgroup_id FOREIGN KEY (parent_id) REFERENCES dcim_rackgroup (id),CONSTRAINT dcim_rackgroup_site_id_13520e89_fk_dcim_site_id FOREIGN KEY (site_id) REFERENCES dcim_site (id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`,
			`CREATE TABLE dcim_rack (id char(32) NOT NULL,created date,last_updated datetime,_custom_field_data json NOT NULL,name varchar(100) NOT NULL,_name varchar(100) NOT NULL,facility_id varchar(50),serial varchar(50) NOT NULL,asset_tag varchar(50),type varchar(50) NOT NULL,width smallint unsigned NOT NULL,u_height smallint unsigned NOT NULL,desc_units tinyint NOT NULL,outer_width smallint unsigned,outer_depth smallint unsigned,outer_unit varchar(50) NOT NULL,comments longtext NOT NULL,group_id char(32),role_id char(32),site_id char(32) NOT NULL,status_id char(32),tenant_id char(32),PRIMARY KEY (id),UNIQUE KEY asset_tag (asset_tag),KEY dcim_rack_group_id_44e90ea9 (group_id),KEY dcim_rack_role_id_62d6919e (role_id),KEY dcim_rack_site_id_403c7b3a (site_id),KEY dcim_rack_status_id_ee3dee3e (status_id),KEY dcim_rack_tenant_id_7cdf3725 (tenant_id),UNIQUE KEY group_idfacility_id (group_id,facility_id),UNIQUE KEY group_idname (group_id,name),CONSTRAINT dcim_rack_group_id_44e90ea9_fk_dcim_rackgroup_id FOREIGN KEY (group_id) REFERENCES dcim_rackgroup (id),CONSTRAINT dcim_rack_site_id_403c7b3a_fk_dcim_site_id FOREIGN KEY (site_id) REFERENCES dcim_site (id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`,
			`INSERT INTO dcim_site (id, created, last_updated, _custom_field_data, status_id, name, _name, slug, region_id, tenant_id, facility, asn, time_zone, description, physical_address, shipping_address, latitude, longitude, contact_name, contact_phone, contact_email, comments) VALUES ('f0471f313b694d388c8ec39d9590e396', '2021-05-20', '2021-05-20 18:51:46.416695', '{}', NULL, 'Site 1', 'Site 00000001', 'site-1', NULL, NULL, '', NULL, '', '', '', '', NULL, NULL, '', '', '', '')`,
			`INSERT INTO dcim_site (id, created, last_updated, _custom_field_data, status_id, name, _name, slug, region_id, tenant_id, facility, asn, time_zone, description, physical_address, shipping_address, latitude, longitude, contact_name, contact_phone, contact_email, comments) VALUES ('442bab8b517149ab87207e8fb5ba1569', '2021-05-20', '2021-05-20 18:51:47.333720', '{}', NULL, 'Site 2', 'Site 00000002', 'site-2', NULL, NULL, '', NULL, '', '', '', '', NULL, NULL, '', '', '', '')`,
			`INSERT INTO dcim_rackgroup (id, created, last_updated, _custom_field_data, name, slug, site_id, parent_id, description, lft, rght, tree_id, level) VALUES ('5c107f979f434bf7a7820622f18a5211', '2021-05-20', '2021-05-20 18:51:48.150116', '{}', 'Parent Rack Group 1', 'parent-rack-group-1', 'f0471f313b694d388c8ec39d9590e396', NULL, '', 1, 2, 1, 0)`,
			`INSERT INTO dcim_rackgroup (id, created, last_updated, _custom_field_data, name, slug, site_id, parent_id, description, lft, rght, tree_id, level) VALUES ('6707c20336a2406da6a9d394477f7e8c', '2021-05-20', '2021-05-20 18:51:48.969713', '{}', 'Parent Rack Group 2', 'parent-rack-group-2', '442bab8b517149ab87207e8fb5ba1569', NULL, '', 1, 2, 2, 0)`,
			`INSERT INTO dcim_rackgroup (id, created, last_updated, _custom_field_data, name, slug, site_id, parent_id, description, lft, rght, tree_id, level) VALUES ('6bc0d9b1affe46918b09911359241db6', '2021-05-20', '2021-05-20 18:51:50.566160', '{}', 'Rack Group 1', 'rack-group-1', 'f0471f313b694d388c8ec39d9590e396', '5c107f979f434bf7a7820622f18a5211', '', 2, 3, 1, 1)`,
			`INSERT INTO dcim_rackgroup (id, created, last_updated, _custom_field_data, name, slug, site_id, parent_id, description, lft, rght, tree_id, level) VALUES ('a773cac9dc9842228cdfd8c97a67136e', '2021-05-20', '2021-05-20 18:51:52.126952', '{}', 'Rack Group 2', 'rack-group-2', 'f0471f313b694d388c8ec39d9590e396', '5c107f979f434bf7a7820622f18a5211', '', 4, 5, 1, 1)`,
			`INSERT INTO dcim_rackgroup (id, created, last_updated, _custom_field_data, name, slug, site_id, parent_id, description, lft, rght, tree_id, level) VALUES ('a35a843eb181404bb9da2126c6580977', '2021-05-20', '2021-05-20 18:51:53.706000', '{}', 'Rack Group 3', 'rack-group-3', 'f0471f313b694d388c8ec39d9590e396', '5c107f979f434bf7a7820622f18a5211', '', 6, 7, 1, 1)`,
			`INSERT INTO dcim_rackgroup (id, created, last_updated, _custom_field_data, name, slug, site_id, parent_id, description, lft, rght, tree_id, level) VALUES ('f09a02c95b064533b823e25374f5962a', '2021-05-20', '2021-05-20 18:52:03.037056', '{}', 'Test Rack Group 4', 'test-rack-group-4', '442bab8b517149ab87207e8fb5ba1569', '6707c20336a2406da6a9d394477f7e8c', '', 2, 3, 2, 1)`,
			`INSERT INTO dcim_rackgroup (id, created, last_updated, _custom_field_data, name, slug, site_id, parent_id, description, lft, rght, tree_id, level) VALUES ('ecff5b528c5140d4a58f1b24a1c80ebc', '2021-05-20', '2021-05-20 18:52:05.390373', '{}', 'Test Rack Group 5', 'test-rack-group-5', '442bab8b517149ab87207e8fb5ba1569', '6707c20336a2406da6a9d394477f7e8c', '', 4, 5, 2, 1)`,
			`INSERT INTO dcim_rackgroup (id, created, last_updated, _custom_field_data, name, slug, site_id, parent_id, description, lft, rght, tree_id, level) VALUES ('d31b3772910e4418bdd5725d905e2699', '2021-05-20', '2021-05-20 18:52:07.758547', '{}', 'Test Rack Group 6', 'test-rack-group-6', '442bab8b517149ab87207e8fb5ba1569', '6707c20336a2406da6a9d394477f7e8c', '', 6, 7, 2, 1)`,
			`INSERT INTO dcim_rack (id,created,last_updated,_custom_field_data,name,_name,facility_id,serial,asset_tag,type,width,u_height,desc_units,outer_width,outer_depth,outer_unit,comments,group_id,role_id,site_id,status_id,tenant_id) VALUES ('abc123',  '2021-05-20', '2021-05-20 18:51:48.150116', '{}', "name", "name", "facility", "serial", "assettag", "type", 1, 1, 1, 1, 1, "outer units", "comment", "6bc0d9b1affe46918b09911359241db6", "role", "f0471f313b694d388c8ec39d9590e396", "status", "tenant")`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `SELECT 
                             ((
                               SELECT COUNT(*)
                               FROM dcim_rack
                               WHERE group_id 
                               IN (
                                 SELECT m2.id
                                 FROM dcim_rackgroup m2
                                 WHERE m2.tree_id = dcim_rackgroup.tree_id
                                   AND m2.lft BETWEEN dcim_rackgroup.lft
                                   AND dcim_rackgroup.rght
                               )
                             )) AS rack_count,
                             dcim_rackgroup.id,
                             dcim_rackgroup._custom_field_data,
                             dcim_rackgroup.name,
                             dcim_rackgroup.slug,
                             dcim_rackgroup.site_id,
                             dcim_rackgroup.parent_id,
                             dcim_rackgroup.description,
                             dcim_rackgroup.lft,
                             dcim_rackgroup.rght,
                             dcim_rackgroup.tree_id,
                             dcim_rackgroup.level 
                           FROM dcim_rackgroup
							order by 2 limit 1`,
				Expected: []sql.Row{{1, "5c107f979f434bf7a7820622f18a5211", sql.JSONDocument{Val: map[string]interface{}{}}, "Parent Rack Group 1", "parent-rack-group-1", "f0471f313b694d388c8ec39d9590e396", interface{}(nil), "", uint64(1), uint64(2), uint64(1), uint64(0)}},
			},
		},
	},
	{
		Name: "CREATE TABLE SELECT Queries",
		SetUpScript: []string{
			`CREATE TABLE t1 (pk int PRIMARY KEY, v1 varchar(10))`,
			`INSERT INTO t1 VALUES (1,"1"), (2,"2"), (3,"3")`,
			`CREATE TABLE t2 AS SELECT * FROM t1`,
			//`CREATE TABLE t3(v0 int) AS SELECT pk FROM t1`, // parser problems
			`CREATE TABLE t3 AS SELECT pk FROM t1`,
			`CREATE TABLE t4 AS SELECT pk, v1 FROM t1`,
			`CREATE TABLE t5 SELECT * FROM t1 ORDER BY pk LIMIT 1`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    `SELECT * FROM t2`,
				Expected: []sql.Row{{1, "1"}, {2, "2"}, {3, "3"}},
			},
			{
				Query:    `SELECT * FROM t3`,
				Expected: []sql.Row{{1}, {2}, {3}},
			},
			{
				Query:    `SELECT * FROM t4`,
				Expected: []sql.Row{{1, "1"}, {2, "2"}, {3, "3"}},
			},
			{
				Query:    `SELECT * FROM t5`,
				Expected: []sql.Row{{1, "1"}},
			},
			{
				Query: `CREATE TABLE test SELECT * FROM t1`,
				Expected: []sql.Row{sql.Row{sql.OkResult{
					RowsAffected: 3,
					InsertID:     0,
					Info:         nil,
				}}},
			},
		},
	},
}

var CreateCheckConstraintsScripts = []ScriptTest{
	{
		Name: "Run SHOW CREATE TABLE with different types of check constraints",
		SetUpScript: []string{
			"CREATE TABLE mytable1(pk int PRIMARY KEY, CONSTRAINT check1 CHECK (pk = 5))",
			"ALTER TABLE mytable1 ADD CONSTRAINT check11 CHECK (pk < 6)",
			"CREATE TABLE mytable2(pk int PRIMARY KEY, v int, CONSTRAINT check2 CHECK (v < 5))",
			"ALTER TABLE mytable2 ADD CONSTRAINT check12 CHECK (pk  + v = 6)",
			"CREATE TABLE mytable3(pk int PRIMARY KEY, v int, CONSTRAINT check3 CHECK (pk > 2 AND v < 5))",
			"ALTER TABLE mytable3 ADD CONSTRAINT check13 CHECK (pk BETWEEN 2 AND 100)",
			"CREATE TABLE mytable4(pk int PRIMARY KEY, v int, CONSTRAINT check4 CHECK (pk > 2 AND v < 5 AND pk < 9))",
			"CREATE TABLE mytable5(pk int PRIMARY KEY, v int, CONSTRAINT check5 CHECK (pk > 2 OR (v < 5 AND pk < 9)))",
			"CREATE TABLE mytable6(pk int PRIMARY KEY, v int, CONSTRAINT check6 CHECK (NOT pk))",
			"CREATE TABLE mytable7(pk int PRIMARY KEY, v int, CONSTRAINT check7 CHECK (pk != v))",
			"CREATE TABLE mytable8(pk int PRIMARY KEY, v int, CONSTRAINT check8 CHECK (pk > 2 OR v < 5 OR pk < 10))",
			"CREATE TABLE mytable9(pk int PRIMARY KEY, v int, CONSTRAINT check9 CHECK ((pk + v) / 2 >= 1))",
			"CREATE TABLE mytable10(pk int PRIMARY KEY, v int, CONSTRAINT check10 CHECK (v < 5) NOT ENFORCED)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW CREATE TABLE mytable1",
				Expected: []sql.Row{
					{
						"mytable1",
						"CREATE TABLE `mytable1` (\n  `pk` int NOT NULL,\n" +
							"  PRIMARY KEY (`pk`),\n" +
							"  CONSTRAINT `check1` CHECK (`pk` = 5),\n" +
							"  CONSTRAINT `check11` CHECK (`pk` < 6)\n" +
							") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
					},
				},
			},
			{
				Query: "SHOW CREATE TABLE mytable2",
				Expected: []sql.Row{
					{
						"mytable2",
						"CREATE TABLE `mytable2` (\n  `pk` int NOT NULL,\n" +
							"  `v` int,\n" +
							"  PRIMARY KEY (`pk`),\n" +
							"  CONSTRAINT `check2` CHECK (`v` < 5),\n" +
							"  CONSTRAINT `check12` CHECK ((`pk` + `v`) = 6)\n" +
							") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
					},
				},
			},
			{
				Query: "SHOW CREATE TABLE mytable3",
				Expected: []sql.Row{
					{
						"mytable3",
						"CREATE TABLE `mytable3` (\n  `pk` int NOT NULL,\n" +
							"  `v` int,\n" +
							"  PRIMARY KEY (`pk`),\n" +
							"  CONSTRAINT `check3` CHECK ((`pk` > 2) AND (`v` < 5)),\n" +
							"  CONSTRAINT `check13` CHECK (`pk` BETWEEN 2 AND 100)\n" +
							") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
					},
				},
			},
			{
				Query: "SHOW CREATE TABLE mytable4",
				Expected: []sql.Row{
					{
						"mytable4",
						"CREATE TABLE `mytable4` (\n  `pk` int NOT NULL,\n" +
							"  `v` int,\n" +
							"  PRIMARY KEY (`pk`),\n" +
							"  CONSTRAINT `check4` CHECK (((`pk` > 2) AND (`v` < 5)) AND (`pk` < 9))\n" +
							") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
					},
				},
			},
			{
				Query: "SHOW CREATE TABLE mytable5",
				Expected: []sql.Row{
					{
						"mytable5",
						"CREATE TABLE `mytable5` (\n  `pk` int NOT NULL,\n" +
							"  `v` int,\n" +
							"  PRIMARY KEY (`pk`),\n" +
							"  CONSTRAINT `check5` CHECK ((`pk` > 2) OR ((`v` < 5) AND (`pk` < 9)))\n" +
							") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
					},
				},
			},
			{
				Query: "SHOW CREATE TABLE mytable6",
				Expected: []sql.Row{
					{
						"mytable6",
						"CREATE TABLE `mytable6` (\n  `pk` int NOT NULL,\n" +
							"  `v` int,\n" +
							"  PRIMARY KEY (`pk`),\n" +
							"  CONSTRAINT `check6` CHECK (NOT(`pk`))\n" +
							") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
					},
				},
			},
			{
				Query: "SHOW CREATE TABLE mytable7",
				Expected: []sql.Row{
					{
						"mytable7",
						"CREATE TABLE `mytable7` (\n  `pk` int NOT NULL,\n" +
							"  `v` int,\n" +
							"  PRIMARY KEY (`pk`),\n" +
							"  CONSTRAINT `check7` CHECK (NOT((`pk` = `v`)))\n" +
							") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
					},
				},
			},
			{
				Query: "SHOW CREATE TABLE mytable8",
				Expected: []sql.Row{
					{
						"mytable8",
						"CREATE TABLE `mytable8` (\n  `pk` int NOT NULL,\n" +
							"  `v` int,\n" +
							"  PRIMARY KEY (`pk`),\n" +
							"  CONSTRAINT `check8` CHECK (((`pk` > 2) OR (`v` < 5)) OR (`pk` < 10))\n" +
							") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
					},
				},
			},
			{
				Query: "SHOW CREATE TABLE mytable9",
				Expected: []sql.Row{
					{
						"mytable9",
						"CREATE TABLE `mytable9` (\n  `pk` int NOT NULL,\n" +
							"  `v` int,\n" +
							"  PRIMARY KEY (`pk`),\n" +
							"  CONSTRAINT `check9` CHECK (((`pk` + `v`) / 2) >= 1)\n" +
							") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
					},
				},
			},
			{
				Query: "SHOW CREATE TABLE mytable10",
				Expected: []sql.Row{
					{
						"mytable10",
						"CREATE TABLE `mytable10` (\n  `pk` int NOT NULL,\n" +
							"  `v` int,\n" +
							"  PRIMARY KEY (`pk`),\n" +
							"  CONSTRAINT `check10` CHECK (`v` < 5) /*!80016 NOT ENFORCED */\n" +
							") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
					},
				},
			},
		},
	},
	{
		Name: "Create a table with a check and validate that it appears in check_constraints and table_constraints",
		SetUpScript: []string{
			"CREATE TABLE mytable (pk int primary key, test_score int, height int, CONSTRAINT mycheck CHECK (test_score >= 50), CONSTRAINT hcheck CHECK (height < 10), CONSTRAINT vcheck CHECK (height > 0))",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * from information_schema.check_constraints where constraint_name IN ('mycheck', 'hcheck') ORDER BY constraint_name",
				Expected: []sql.Row{
					{"def", "mydb", "hcheck", "(height < 10)"},
					{"def", "mydb", "mycheck", "(test_score >= 50)"},
				},
			},
			{
				Query: "SELECT * FROM information_schema.table_constraints where table_name='mytable' ORDER BY constraint_type,constraint_name",
				Expected: []sql.Row{
					{"def", "mydb", "hcheck", "mydb", "mytable", "CHECK", "YES"},
					{"def", "mydb", "mycheck", "mydb", "mytable", "CHECK", "YES"},
					{"def", "mydb", "vcheck", "mydb", "mytable", "CHECK", "YES"},
					{"def", "mydb", "PRIMARY", "mydb", "mytable", "PRIMARY KEY", "YES"},
				},
			},
		},
	},
}
