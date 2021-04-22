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
	// For tests where a non go-errors package error is returned
	ExpectedErrStr string
}

type ScriptTestAssertion struct {
	Query       string
	Expected    []sql.Row
	ExpectedErr *errors.Kind
	// ExpectedErrStr should be set for tests that expect a specific error string this is not linked to a custom error.
	// In most cases, errors should be linked to a custom error, however there are exceptions where this is not possible,
	// such as the use of the SIGNAL statement.
	ExpectedErrStr string
}

// Unlike other engine tests, ScriptTests must be self-contained. No other tables are created outside the definition of
// the tests.
var ScriptTests = []ScriptTest{
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
}
