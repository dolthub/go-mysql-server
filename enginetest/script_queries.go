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
				Query:          "UPDATE test SET pk = pk + 1 ORDER BY pk;",
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
				Expected: []sql.Row{{sql.OkResult{RowsAffected: 1, InsertID: 1}}},
			},
			{
				Query:    "select last_insert_id()",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "insert into a (y) values (2), (3)",
				Expected: []sql.Row{{sql.OkResult{RowsAffected: 2, InsertID: 2}}},
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
				Query:    "SELECT group_concat(`attribute` ORDER BY `attribute`) FROM t group by o_id order by o_id asc",
				Expected: []sql.Row{{"color,fabric"}, {"color,shape"}},
			},
			{
				Query:    "SELECT group_concat(DISTINCT `attribute` ORDER BY value DESC SEPARATOR ';') FROM t group by o_id order by o_id asc",
				Expected: []sql.Row{{"fabric;color"}, {"shape;color"}},
			},
			{
				Query:    "SELECT group_concat(DISTINCT `attribute` ORDER BY `attribute`) FROM t",
				Expected: []sql.Row{{"color,fabric,shape"}},
			},
			{
				Query:    "SELECT group_concat(`attribute` ORDER BY `attribute`) FROM t",
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
				Query:    "SELECT group_concat(DISTINCT `attribute` ORDER BY `attribute` ASC) FROM t",
				Expected: []sql.Row{{"color,fabric,shape"}},
			},
			{
				Query:    "SELECT group_concat(DISTINCT `attribute` ORDER BY `attribute` DESC) FROM t",
				Expected: []sql.Row{{"shape,fabric,color"}},
			},
			{
				Query:    `SELECT group_concat(pk) FROM nulls`,
				Expected: []sql.Row{{nil}},
			},
			{
				Query:       `SELECT group_concat((SELECT * FROM t LIMIT 1)) from t`,
				ExpectedErr: sql.ErrInvalidOperandColumns,
			},
			{
				Query:       `SELECT group_concat((SELECT * FROM x)) from t`,
				ExpectedErr: sql.ErrExpectedSingleRow,
			},
			{
				Query:    "SELECT group_concat(`attribute`) FROM t where o_id=2",
				Expected: []sql.Row{{"color,fabric"}},
			},
			{
				Query:    "SELECT group_concat(DISTINCT `attribute` ORDER BY value DESC SEPARATOR ';') FROM t group by o_id order by o_id asc",
				Expected: []sql.Row{{"fabric;color"}, {"shape;color"}},
			},
			{
				Query:    "SELECT group_concat(o_id) FROM t WHERE `attribute`='color'",
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
		Name: "ALTER TABLE AUTO INCREMENT no-ops on table with no original auto increment key",
		SetUpScript: []string{
			"CREATE table test (pk int primary key)",
			"ALTER TABLE `test` auto_increment = 2;",
			"INSERT INTO test VALUES (1)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT * FROM test",
				Expected: []sql.Row{{1}},
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
			"INSERT INTO mytable VALUES(2,2)",
			"INSERT INTO mytable VALUES(1,2)",
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
				Query:    "SELECT pk, SUM(DISTINCT v1), MAX(v1) FROM mytable GROUP BY pk ORDER BY pk",
				Expected: []sql.Row{{int64(1), float64(3), int64(2)}, {int64(2), float64(2), int64(2)}},
			},
			{
				Query:    "SELECT pk, MIN(DISTINCT v1), MAX(DISTINCT v1) FROM mytable GROUP BY pk ORDER BY pk",
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
	{
		Name: "Issue #499",
		SetUpScript: []string{
			"CREATE TABLE test (time TIMESTAMP, value DOUBLE);",
			`INSERT INTO test VALUES 
			("2021-07-04 10:00:00", 1.0),
			("2021-07-03 10:00:00", 2.0),
			("2021-07-02 10:00:00", 3.0),
			("2021-07-01 10:00:00", 4.0);`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `SELECT
				UNIX_TIMESTAMP(time) DIV 60 * 60 AS "time",
				avg(value) AS "value"
				FROM test
				GROUP BY 1
				ORDER BY UNIX_TIMESTAMP(time) DIV 60 * 60`,
				Expected: []sql.Row{
					{1625133600, 4.0},
					{1625220000, 3.0},
					{1625306400, 2.0},
					{1625392800, 1.0},
				},
			},
		},
	},
	{
		Name: "WHERE clause considers ENUM/SET types for comparisons",
		SetUpScript: []string{
			"CREATE TABLE test (pk BIGINT PRIMARY KEY, v1 ENUM('a', 'b', 'c'), v2 SET('a', 'b', 'c'));",
			"INSERT INTO test VALUES (1, 2, 2), (2, 1, 1);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT * FROM test;",
				Expected: []sql.Row{{1, "b", "b"}, {2, "a", "a"}},
			},
			{
				Query:    "UPDATE test SET v1 = 3 WHERE v1 = 2;",
				Expected: []sql.Row{{sql.OkResult{RowsAffected: 1, InsertID: 0, Info: plan.UpdateInfo{Matched: 1, Updated: 1}}}},
			},
			{
				Query:    "SELECT * FROM test;",
				Expected: []sql.Row{{1, "c", "b"}, {2, "a", "a"}},
			},
			{
				Query:    "UPDATE test SET v2 = 3 WHERE 2 = v2;",
				Expected: []sql.Row{{sql.OkResult{RowsAffected: 1, InsertID: 0, Info: plan.UpdateInfo{Matched: 1, Updated: 1}}}},
			},
			{
				Query:    "SELECT * FROM test;",
				Expected: []sql.Row{{1, "c", "a,b"}, {2, "a", "a"}},
			},
		},
	},
	{
		Name: "Slightly more complex example for the Exists Clause",
		SetUpScript: []string{
			"create table store(store_id int, item_id int, primary key (store_id, item_id))",
			"create table items(item_id int primary key, price int)",
			"insert into store values (0, 1), (0,2),(0,3),(1,2),(1,4),(2,1)",
			"insert into items values (1, 10), (2, 20), (3, 30),(4,40)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT * from store WHERE EXISTS (SELECT price from items where price > 10 and store.item_id = items.item_id)",
				Expected: []sql.Row{{0, 2}, {0, 3}, {1, 2}, {1, 4}},
			},
		},
	},
	{
		Name: "Simple Update Join test that manipulates two tables",
		SetUpScript: []string{
			"CREATE TABLE test (pk int primary key);",
			`CREATE TABLE test2 (pk int primary key, val int);`,
			`INSERT into test values (0),(1),(2),(3)`,
			`INSERT into test2 values (0, 0),(1, 1),(2, 2),(3, 3)`,
			`CREATE TABLE test3(k int, val int, primary key (k, val))`,
			`INSERT into test3 values (1,2),(1,3),(1,4)`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `update test2 inner join (select * from test3 order by val) as t3 on test2.pk = t3.k SET test2.val=t3.val`,
				Expected: []sql.Row{{sql.OkResult{RowsAffected: 1, Info: plan.UpdateInfo{
					Matched:  1,
					Updated:  1,
					Warnings: 0,
				}}}},
			},
			{
				Query: "SELECT val FROM test2 where pk = 1",
				Expected: []sql.Row{
					{2},
				},
			},
			{
				Query: `update test inner join test2 on test.pk = test2.pk SET test.pk=test.pk*10, test2.pk = test2.pk * 4 where test.pk < 10;`,
				Expected: []sql.Row{{sql.OkResult{RowsAffected: 6, Info: plan.UpdateInfo{
					Matched:  6, // TODO: The answer should be 8
					Updated:  6,
					Warnings: 0,
				}}}},
			},
			{
				Query: "SELECT * FROM test",
				Expected: []sql.Row{
					{0},
					{10},
					{20},
					{30},
				},
			},
			{
				Query: "SELECT * FROM test2",
				Expected: []sql.Row{
					{0, 0},
					{4, 2},
					{8, 2},
					{12, 3},
				},
			},
		},
	},
	{
		Name: "Partial indexes are used and return the expected result",
		SetUpScript: []string{
			"CREATE TABLE test (pk BIGINT PRIMARY KEY, v1 BIGINT, v2 BIGINT, v3 BIGINT, INDEX vx (v3, v2, v1));",
			"INSERT INTO test VALUES (1,2,3,4), (2,3,4,5), (3,4,5,6), (4,5,6,7), (5,6,7,8);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "EXPLAIN SELECT * FROM test WHERE v3 = 4;",
				Expected: []sql.Row{{"Filter(test.v3 = 4)"},
					{" └─ Projected table access on [pk v1 v2 v3]"},
					{"     └─ IndexedTableAccess(test on [test.v3,test.v2,test.v1])"}},
			},
			{
				Query:    "SELECT * FROM test WHERE v3 = 4;",
				Expected: []sql.Row{{1, 2, 3, 4}},
			},
			{
				Query: "EXPLAIN SELECT * FROM test WHERE v3 = 8 AND v2 = 7;",
				Expected: []sql.Row{{"Filter((test.v3 = 8) AND (test.v2 = 7))"},
					{" └─ Projected table access on [pk v1 v2 v3]"},
					{"     └─ IndexedTableAccess(test on [test.v3,test.v2,test.v1])"}},
			},
			{
				Query:    "SELECT * FROM test WHERE v3 = 8 AND v2 = 7;",
				Expected: []sql.Row{{5, 6, 7, 8}},
			},
			{
				Query: "EXPLAIN SELECT * FROM test WHERE v3 >= 6 AND v2 >= 6;",
				Expected: []sql.Row{{"Filter((test.v3 >= 6) AND (test.v2 >= 6))"},
					{" └─ Projected table access on [pk v1 v2 v3]"},
					{"     └─ IndexedTableAccess(test on [test.v3,test.v2,test.v1])"}},
			},
			{
				Query:    "SELECT * FROM test WHERE v3 >= 6 AND v2 >= 6;",
				Expected: []sql.Row{{4, 5, 6, 7}, {5, 6, 7, 8}},
			},
			{
				Query: "EXPLAIN SELECT * FROM test WHERE v3 = 7 AND v2 >= 6;",
				Expected: []sql.Row{{"Filter((test.v3 = 7) AND (test.v2 >= 6))"},
					{" └─ Projected table access on [pk v1 v2 v3]"},
					{"     └─ IndexedTableAccess(test on [test.v3,test.v2,test.v1])"}},
			},
			{
				Query:    "SELECT * FROM test WHERE v3 = 7 AND v2 >= 6;",
				Expected: []sql.Row{{4, 5, 6, 7}},
			},
		},
	},
	{
		Name: "Multiple indexes on the same columns in a different order",
		SetUpScript: []string{
			"CREATE TABLE test (pk BIGINT PRIMARY KEY, v1 BIGINT, v2 BIGINT, v3 BIGINT, INDEX v123 (v1, v2, v3), INDEX v321 (v3, v2, v1), INDEX v132 (v1, v3, v2));",
			"INSERT INTO test VALUES (1,2,3,4), (2,3,4,5), (3,4,5,6), (4,5,6,7), (5,6,7,8);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "EXPLAIN SELECT * FROM test WHERE v1 = 2 AND v2 > 1;",
				Expected: []sql.Row{{"Filter((test.v1 = 2) AND (test.v2 > 1))"},
					{" └─ Projected table access on [pk v1 v2 v3]"},
					{"     └─ IndexedTableAccess(test on [test.v1,test.v2,test.v3])"}},
			},
			{
				Query:    "SELECT * FROM test WHERE v1 = 2 AND v2 > 1;",
				Expected: []sql.Row{{1, 2, 3, 4}},
			},
			{
				Query: "EXPLAIN SELECT * FROM test WHERE v2 = 4 AND v3 > 1;",
				Expected: []sql.Row{{"Filter((test.v2 = 4) AND (test.v3 > 1))"},
					{" └─ Projected table access on [pk v1 v2 v3]"},
					{"     └─ IndexedTableAccess(test on [test.v3,test.v2,test.v1])"}},
			},
			{
				Query:    "SELECT * FROM test WHERE v2 = 4 AND v3 > 1;",
				Expected: []sql.Row{{2, 3, 4, 5}},
			},
			{
				Query: "EXPLAIN SELECT * FROM test WHERE v3 = 6 AND v1 > 1;",
				Expected: []sql.Row{{"Filter((test.v3 = 6) AND (test.v1 > 1))"},
					{" └─ Projected table access on [pk v1 v2 v3]"},
					{"     └─ IndexedTableAccess(test on [test.v1,test.v3,test.v2])"}},
			},
			{
				Query:    "SELECT * FROM test WHERE v3 = 6 AND v1 > 1;",
				Expected: []sql.Row{{3, 4, 5, 6}},
			},
			{
				Query: "EXPLAIN SELECT * FROM test WHERE v1 = 5 AND v3 <= 10 AND v2 >= 1;",
				Expected: []sql.Row{{"Filter(((test.v1 = 5) AND (test.v3 <= 10)) AND (test.v2 >= 1))"},
					{" └─ Projected table access on [pk v1 v2 v3]"},
					{"     └─ IndexedTableAccess(test on [test.v1,test.v2,test.v3])"}},
			},
			{
				Query:    "SELECT * FROM test WHERE v1 = 5 AND v3 <= 10 AND v2 >= 1;",
				Expected: []sql.Row{{4, 5, 6, 7}},
			},
		},
	},
	{
		Name: "Ensure proper DECIMAL support (found by fuzzer)",
		SetUpScript: []string{
			"CREATE TABLE `GzaKtwgIya` (`K7t5WY` DECIMAL(64,5), `qBjVrN` VARBINARY(1000), `PvqQtc` SET('c3q6y','kxMqhfkK','XlRI8','dF0N63H','hMPjt0KXRLwCGRr','27fi2s','1FSJ','NcPzIN','Za18lbIgxmZ','on4BKKXykVTbJ','WBfO','RMNG','Sd7','FDzbEO','cLRdLOj1y','syo4','Ul','jfsfDCx6s','yEW3','JyQcWFDl'), `1kv7el` FLOAT, `Y3vfRG` BLOB, `Ijq8CK` TINYTEXT, `tzeStN` MEDIUMINT, `Ak83FQ` BINARY(64), `8Nbp3L` DOUBLE, PRIMARY KEY (`K7t5WY`));",
			"REPLACE INTO `GzaKtwgIya` VALUES ('58567047399981325523662211357420045483361289734772861386428.89028','bvo5~Tt8%kMW2nm2!8HghaeulI6!pMadE+j-J2LeU1O1*-#@Lm8Ibh00bTYiA*H1Q8P1_kQq 24Rrd4@HeF%#7#C#U7%mqOMrQ0%!HVrGV1li.XyYa:7#3V^DtAMDTQ9 cY=07T4|DStrwy4.MAQxOG#1d#fcq+7675$y0e96-2@8-WlQ^p|%E!a^TV!Yj2_eqZZys1z:883l5I%zAT:i56K^T!cx#us $60Tb#gH$1#$P.709E#VrH9FbQ5QZK2hZUH!qUa4Xl8*I*0fT~oAha$8jU5AoWs+Uv!~:14Yq%pLXpP9RlZ:Gd1g|*$Qa.9*^K~YlYWVaxwY~_g6zOMpU$YijT+!_*m3=||cMNn#uN0!!OyCg~GTQlJ11+#@Ohqc7b#2|Jp2Aei56GOmq^I=7cQ=sQh~V.D^HzwK5~4E$QzFXfWNVN5J_w2b4dkR~bB~7F%=@R@9qE~e:-_RnoJcOLfBS@0:*hTIP$5ui|5Ea-l+qU4nx98X6rV2bLBxn8am@p~:xLF#T^_9kJVN76q^18=i *FJo.v-xA2GP==^C^Jz3yBF0OY4bIxC59Y#6G=$w:xh71kMxBcYJKf3+$Ci_uWx0P*AfFNne0_1E0Lwv#3J8vm:. 8Wo~F3VT:@w.t@w .JZz$bok9Tls7RGo=~4 Y$~iELr$s@53YuTPM8oqu!x*1%GswpJR=0K#qs00nW-1MqEUc:0wZv#X4qY^pzVDb:!:!yDhjhh+KIT%2%w@+t8c!f~o!%EnwBIr_OyzL6e1$-R8n0nWPU.toODd*|fW3H$9ZLc9!dMS:QfjI0M$nK 8aGvUVP@9kS~W#Y=Q%=37$@pAUkDTXkJo~-DRvCG6phPp*Xji@9|AEODHi+-6p%X4YM5Y3WasPHcZQ8QgTwi9 N=2RQD_MtVU~0J~3SAx*HrMlKvCPTswZq#q_96ny_A@7g!E2jyaxWFJD:C233onBdchW$WdAc.LZdZHYDR^uwZb9B9p-q.BkD1I',608583,'-7.276514330627342e-28','FN3O_E:$ 5S40T7^Vu1g!Ktn^N|4RE!9GnZiW5dG:%SJb5|SNuuI.d2^qnMY.Xn*_fRfk Eo7OhqY8OZ~pA0^ !2P.uN~r@pZ2!A0+4b*%nxO.tm%S6=$CZ9+c1zu-p $b:7:fOkC%@E3951st@2Q93~8hj:ZGeJ6S@nw-TAG+^lad37aB#xN*rD^9TO0|hleA#.Nh28S2PB72L*TxD0$|XE3S5eVVmbI*pkzE~lPecopX1fUyFj#LC+%~pjmab7^ Kdd4B%8I!ohOCQV.oiw++N|#W2=D4:_sK0@~kTTeNA8_+FMKRwro.M0| LdKHf-McKm0Z-R9+H%!9r l6%7UEB50yNH-ld%eW8!f=LKgZLc*TuTP2DA_o0izvzZokNp3ShR+PA7Fk* 1RcSt5KXe+8tLc+WGP','3RvfN2N.Q1tIffE965#2r=u_-4!u:9w!F1p7+mSsO8ckio|ib 1t@~GtgUkJX',1858932,'DJMaQcI=vS-Jk2L#^2N8qZcRpMJ2Ga!30A+@I!+35d-9bwVEVi5-~i.a%!KdoF5h','1.0354401044541863e+255');",
			"INSERT INTO `GzaKtwgIya` VALUES ('91198031969464085142628031466155813748261645250257051732159.65596','96Lu=focmodq4otVAUN6TD-F$@k^4443higo=KH!1WBDH9|vpEGdO* 1uF6yWjT4:7G|altXnWSv+d:c8Km8vL!b%-nuB8mAxO9E|a5N5#v@z!ij5ifeIEoZGXrhBJl.m*Rx-@%g~t:y$3Pp3Q7Bd3y$=YG%6yibqXWO9$SS+g=*6QzdSCzuR~@v!:.ATye0A@y~DG=uq!PaZd6wN7.2S Aq868-RN3RM61V#N+Qywqo=%iYV*554@h6GPKZ| pmNwQw=PywuyBhr*MHAOXV+u9_-#imKI-wT4gEcA1~lGg1cfL2IvhkwOXRhrjAx-8+R3#4!Ai J6SYP|YUuuGalJ_N8k_8K^~h!JyiH$0JbGQ4AOxO3-eW=BaopOd8FF1.cfFMK!tXR ^I15g:npOuZZO$Vq3yQ4bl4s$E9:t2^.4f.:I4_@u9_UI1ApBthJZNiv~o#*uhs9K@ufZ1YPJQY-pMj$v-lQ2#%=Uu!iEAO3%vQ^5YITKcWRk~$kd1H#F675r@P5#M%*F_xP3Js7$YuEC4YuQjZ A74tMw:KwQ8dR:k_ Sa85G~42-K3%:jk5G9csC@iW3nY|@-:_dg~5@J!FWF5F+nyBgz4fDpdkdk9^:_.t$A3W-C@^Ax.~o|Rq96_i%HeG*7jBjOGhY-e1k@aD@WW.@GmpGAI|T-84gZFG3BU9@#9lpL|U2YCEA.BEA%sxDZ Kw:n+d$Y!SZw0Iml$Bdtyr:02Np=DZpiI%$N9*U=%Jq#$P5BI60WOTK+UynVx9Dd**5q8y9^v+I|PPa#_2XheV5YQU.ONdQQNJxsiRaEl!*=xv4bTWj1wBH#_-eM3T',490529,'-8.419238802182018e+25','|WD!NpWJOfN+_Au 1y!|XF8l38#%%R5%$TRUEaFt%4ywKQ8 O1LD-3qRDrnHAXboH~0uivbo87f+V%=q9~Mvz1EIxsU!whSmPqtb9r*11346R_@L+H#@@Z9H-Dc6j%.D0o##m@B9o7jO#~N81ACI|f#J3z4dho:jc54Xws$8r%cxuov^1$w_58Fv2*.8qbAW$TF153A:8wwj4YIhkd#^Q7 |g7I0iQG0p+yE64rk!Pu!SA-z=ELtLNOCJBk_4!lV$izn%sB6JwM+uq~ 49I7','v|eUA_h2@%t~bn26ci8Ngjm@Lk*G=l2MhxhceV2V|ka#c',8150267,'nX-=1Q$3riw_jlukGuHmjodT_Y_SM$xRbEt$%$%hlIUF1+GpRp~U6JvRX^: k@n#','7.956726808353253e+267');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "DELETE FROM `GzaKtwgIya` WHERE `K7t5WY` = '58567047399981325523662211357420045483361289734772861386428.89028';",
				Expected: []sql.Row{{sql.NewOkResult(1)}},
			},
			{
				Query:    "SELECT COUNT(*) FROM GzaKtwgIya",
				Expected: []sql.Row{{1}},
			},
		},
	},
	{
		Name: "JOIN on non-index-prefix columns do not panic (Dolt Issue #2366)",
		SetUpScript: []string{
			"CREATE TABLE `player_season_stat_totals` (`player_id` int NOT NULL, `team_id` int NOT NULL, `season_id` int NOT NULL, `minutes` int, `games_started` int, `games_played` int, `2pm` int, `2pa` int, `3pm` int, `3pa` int, `ftm` int, `fta` int, `ast` int, `stl` int, `blk` int, `tov` int, `pts` int, `orb` int, `drb` int, `trb` int, `pf` int, `season_type_id` int NOT NULL, `league_id` int NOT NULL DEFAULT 0, PRIMARY KEY (`player_id`,`team_id`,`season_id`,`season_type_id`,`league_id`));",
			"CREATE TABLE `team_seasons` (`team_id` int NOT NULL, `league_id` int NOT NULL, `season_id` int NOT NULL, `prefix` varchar(100), `nickname` varchar(100), `abbreviation` varchar(100), `city` varchar(100), `state` varchar(100), `country` varchar(100), PRIMARY KEY (`team_id`,`league_id`,`season_id`));",
			"INSERT INTO player_season_stat_totals VALUES (1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1);",
			"INSERT INTO team_seasons VALUES (1,1,1,'','','','','','');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT stats.* FROM player_season_stat_totals stats LEFT JOIN team_seasons ON team_seasons.team_id = stats.team_id AND team_seasons.season_id = stats.season_id;",
				Expected: []sql.Row{{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}},
			},
		},
	},
	{
		Name: "Issue #709",
		SetUpScript: []string{
			"create table a(id int primary key, v int , key (v));",
		},
	},
	{
		Name: "Show create table with various keys and constraints",
		SetUpScript: []string{
			"create table t1(a int primary key, b varchar(10) not null default 'abc')",
			"alter table t1 add constraint ck1 check (b like '%abc%')",
			"create index t1b on t1(b)",
			"create table t2(c int primary key, d varchar(10))",
			"alter table t2 add constraint fk1 foreign key (d) references t1 (b)",
			"alter table t2 add constraint t2du unique (d)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "show create table t1",
				Expected: []sql.Row{
					{"t1", "CREATE TABLE `t1` (\n" +
						"  `a` int NOT NULL,\n" +
						"  `b` varchar(10) NOT NULL DEFAULT \"abc\",\n" +
						"  PRIMARY KEY (`a`),\n" +
						"  KEY `t1b` (`b`),\n" +
						"  CONSTRAINT `ck1` CHECK (`b` LIKE \"%abc%\")\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"},
				},
			},
			{
				Query: "show create table t2",
				Expected: []sql.Row{
					{"t2", "CREATE TABLE `t2` (\n" +
						"  `c` int NOT NULL,\n" +
						"  `d` varchar(10),\n" +
						"  PRIMARY KEY (`c`),\n" +
						"  UNIQUE KEY `t2.d` (`d`),\n" +
						"  CONSTRAINT `fk1` FOREIGN KEY (`d`) REFERENCES `t1` (`b`)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"},
				},
			},
		},
	},
	{
		Name: "table with defaults, insert with on duplicate key update",
		SetUpScript: []string{
			"create table t (a int primary key, b int default 100);",
			"insert into t values (1, 1), (2, 2)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "insert into t values (1, 10) on duplicate key update b = 10",
				Expected: []sql.Row{{sql.NewOkResult(2)}},
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
							"  CONSTRAINT `check1` CHECK ((`pk` = 5)),\n" +
							"  CONSTRAINT `check11` CHECK ((`pk` < 6))\n" +
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
							"  CONSTRAINT `check2` CHECK ((`v` < 5)),\n" +
							"  CONSTRAINT `check12` CHECK (((`pk` + `v`) = 6))\n" +
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
							"  CONSTRAINT `check3` CHECK (((`pk` > 2) AND (`v` < 5))),\n" +
							"  CONSTRAINT `check13` CHECK ((`pk` BETWEEN 2 AND 100))\n" +
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
							"  CONSTRAINT `check4` CHECK ((((`pk` > 2) AND (`v` < 5)) AND (`pk` < 9)))\n" +
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
							"  CONSTRAINT `check5` CHECK (((`pk` > 2) OR ((`v` < 5) AND (`pk` < 9))))\n" +
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
							"  CONSTRAINT `check6` CHECK ((NOT(`pk`)))\n" +
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
							"  CONSTRAINT `check7` CHECK ((NOT((`pk` = `v`))))\n" +
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
							"  CONSTRAINT `check8` CHECK ((((`pk` > 2) OR (`v` < 5)) OR (`pk` < 10)))\n" +
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
							"  CONSTRAINT `check9` CHECK ((((`pk` + `v`) / 2) >= 1))\n" +
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
							"  CONSTRAINT `check10` CHECK ((`v` < 5)) /*!80016 NOT ENFORCED */\n" +
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
	{
		Name: "multi column index, lower()",
		SetUpScript: []string{
			"CREATE TABLE test (pk BIGINT PRIMARY KEY, v1 varchar(100), v2 varchar(100), INDEX (v1,v2));",
			"INSERT INTO test VALUES (1,'happy','birthday'), (2,'HAPPY','BIRTHDAY'), (3,'hello','sailor');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT pk FROM test where lower(v1) = 'happy' and lower(v2) = 'birthday' order by 1",
				Expected: []sql.Row{{1}, {2}},
			},
		},
	},
	{
		Name: "adding check constraint to a table that violates said constraint correctly throws an error",
		SetUpScript: []string{
			"CREATE TABLE test (pk int)",
			"INSERT INTO test VALUES (1),(2),(300)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "ALTER TABLE test ADD CONSTRAINT bad_check CHECK (pk < 5)",
				ExpectedErr: plan.ErrCheckViolated,
			},
		},
	},
}
