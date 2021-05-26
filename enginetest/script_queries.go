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
	"github.com/dolthub/go-mysql-server/sql/analyzer"
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
			`INSERT INTO dcim_rack (id,created,last_updated,_custom_field_data,name,_name,facility_id,serial,asset_tag,type,width,u_height,desc_units,outer_width,outer_depth,outer_unit,comments,group_id,role_id,site_id,status_id,tenant_id) VALUES ('abc123',  '2021-05-20', '2021-05-20 18:51:48.150116', '{}', "name", "name", "facility", "serial", "assettag", "type", 1, 1, 1, 1, 1, "outer units", "comment", "6bc0d9b1affe46918b09911359241db6", "role", "site", "status", "tenant")`,
			`INSERT INTO dcim_rackgroup (id, created, last_updated, _custom_field_data, name, slug, site_id, parent_id, description, lft, rght, tree_id, level) VALUES ('5c107f979f434bf7a7820622f18a5211', '2021-05-20', '2021-05-20 18:51:48.150116', '{}', 'Parent Rack Group 1', 'parent-rack-group-1', 'f0471f313b694d388c8ec39d9590e396', NULL, '', 1, 2, 1, 0)`,
			`INSERT INTO dcim_rackgroup (id, created, last_updated, _custom_field_data, name, slug, site_id, parent_id, description, lft, rght, tree_id, level) VALUES ('6707c20336a2406da6a9d394477f7e8c', '2021-05-20', '2021-05-20 18:51:48.969713', '{}', 'Parent Rack Group 2', 'parent-rack-group-2', '442bab8b517149ab87207e8fb5ba1569', NULL, '', 1, 2, 2, 0)`,
			`INSERT INTO dcim_rackgroup (id, created, last_updated, _custom_field_data, name, slug, site_id, parent_id, description, lft, rght, tree_id, level) VALUES ('6bc0d9b1affe46918b09911359241db6', '2021-05-20', '2021-05-20 18:51:50.566160', '{}', 'Rack Group 1', 'rack-group-1', 'f0471f313b694d388c8ec39d9590e396', '5c107f979f434bf7a7820622f18a5211', '', 2, 3, 1, 1)`,
			`INSERT INTO dcim_rackgroup (id, created, last_updated, _custom_field_data, name, slug, site_id, parent_id, description, lft, rght, tree_id, level) VALUES ('a773cac9dc9842228cdfd8c97a67136e', '2021-05-20', '2021-05-20 18:51:52.126952', '{}', 'Rack Group 2', 'rack-group-2', 'f0471f313b694d388c8ec39d9590e396', '5c107f979f434bf7a7820622f18a5211', '', 4, 5, 1, 1)`,
			`INSERT INTO dcim_rackgroup (id, created, last_updated, _custom_field_data, name, slug, site_id, parent_id, description, lft, rght, tree_id, level) VALUES ('a35a843eb181404bb9da2126c6580977', '2021-05-20', '2021-05-20 18:51:53.706000', '{}', 'Rack Group 3', 'rack-group-3', 'f0471f313b694d388c8ec39d9590e396', '5c107f979f434bf7a7820622f18a5211', '', 6, 7, 1, 1)`,
			`INSERT INTO dcim_rackgroup (id, created, last_updated, _custom_field_data, name, slug, site_id, parent_id, description, lft, rght, tree_id, level) VALUES ('f09a02c95b064533b823e25374f5962a', '2021-05-20', '2021-05-20 18:52:03.037056', '{}', 'Test Rack Group 4', 'test-rack-group-4', '442bab8b517149ab87207e8fb5ba1569', '6707c20336a2406da6a9d394477f7e8c', '', 2, 3, 2, 1)`,
			`INSERT INTO dcim_rackgroup (id, created, last_updated, _custom_field_data, name, slug, site_id, parent_id, description, lft, rght, tree_id, level) VALUES ('ecff5b528c5140d4a58f1b24a1c80ebc', '2021-05-20', '2021-05-20 18:52:05.390373', '{}', 'Test Rack Group 5', 'test-rack-group-5', '442bab8b517149ab87207e8fb5ba1569', '6707c20336a2406da6a9d394477f7e8c', '', 4, 5, 2, 1)`,
			`INSERT INTO dcim_rackgroup (id, created, last_updated, _custom_field_data, name, slug, site_id, parent_id, description, lft, rght, tree_id, level) VALUES ('d31b3772910e4418bdd5725d905e2699', '2021-05-20', '2021-05-20 18:52:07.758547', '{}', 'Test Rack Group 6', 'test-rack-group-6', '442bab8b517149ab87207e8fb5ba1569', '6707c20336a2406da6a9d394477f7e8c', '', 6, 7, 2, 1)`,
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
							order by 1 limit 1`,
				Expected: []sql.Row{{0, "6707c20336a2406da6a9d394477f7e8c", sql.JSONDocument{Val: map[string]interface{}{}}, "Parent Rack Group 2", "parent-rack-group-2", "442bab8b517149ab87207e8fb5ba1569", interface{}(nil), "", uint64(1), uint64(2), uint64(2), uint64(0)}},
			},
		},
	},
	{
		Name: "Nested Subquery projections (NTC)",
		SetUpScript: []string{
			`CREATE TABLE dcim_interface ( id char(32) NOT NULL, _custom_field_data json NOT NULL, name varchar(64) NOT NULL, label varchar(64) NOT NULL, description varchar(200) NOT NULL, _cable_peer_id char(32), enabled tinyint NOT NULL, mac_address varchar(18), mtu int unsigned, mode varchar(50) NOT NULL, _name varchar(100) NOT NULL, type varchar(50) NOT NULL, mgmt_only tinyint NOT NULL, _cable_peer_type_id int, _path_id char(32), cable_id char(32), device_id char(32) NOT NULL, lag_id char(32), untagged_vlan_id char(32), PRIMARY KEY (id), KEY dcim_interface__cable_peer_type_id_ce52cb81 (_cable_peer_type_id), KEY dcim_interface__path_id_f8f4f7f0 (_path_id), KEY dcim_interface_cable_id_1b264edb (cable_id), KEY dcim_interface_device_id_359c6115 (device_id), KEY dcim_interface_lag_id_ea1a1d12 (lag_id), KEY dcim_interface_untagged_vlan_id_838dc7be (untagged_vlan_id), UNIQUE KEY device_idname (device_id,name)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`,
			`CREATE TABLE dcim_device ( id char(32) NOT NULL, created date, last_updated datetime, _custom_field_data json NOT NULL, local_context_data json, local_context_data_owner_object_id char(32), name varchar(64), _name varchar(100), serial varchar(50) NOT NULL, asset_tag varchar(50), position smallint unsigned, face varchar(50) NOT NULL, vc_position smallint unsigned, vc_priority smallint unsigned, comments longtext NOT NULL, cluster_id char(32), device_role_id char(32) NOT NULL, device_type_id char(32) NOT NULL, local_context_data_owner_content_type_id int, platform_id char(32), primary_ip4_id char(32), primary_ip6_id char(32), rack_id char(32), site_id char(32) NOT NULL, status_id char(32), tenant_id char(32), virtual_chassis_id char(32), PRIMARY KEY (id), UNIQUE KEY asset_tag (asset_tag), KEY dcim_device_cluster_id_cf852f78 (cluster_id), KEY dcim_device_device_role_id_682e8188 (device_role_id), KEY dcim_device_device_type_id_d61b4086 (device_type_id), KEY dcim_device_local_context_data_owner_content_type_id_5d06013b (local_context_data_owner_content_type_id), KEY dcim_device_platform_id_468138f1 (platform_id), KEY dcim_device_rack_id_23bde71f (rack_id), KEY dcim_device_site_id_ff897cf6 (site_id), KEY dcim_device_status_id_96d2fc6f (status_id), KEY dcim_device_tenant_id_dcea7969 (tenant_id), KEY dcim_device_virtual_chassis_id_aed51693 (virtual_chassis_id), KEY primary_ip4_id (primary_ip4_id), KEY primary_ip6_id (primary_ip6_id), UNIQUE KEY rack_idpositionface (rack_id,position,face), UNIQUE KEY site_idtenant_idname (site_id,tenant_id,name), UNIQUE KEY virtual_chassis_idvc_position (virtual_chassis_id,vc_position)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`,
			`INSERT INTO dcim_device (id, created, last_updated, _custom_field_data, status_id, local_context_data, local_context_data_owner_content_type_id, local_context_data_owner_object_id, device_type_id, device_role_id, tenant_id, platform_id, name, _name, serial, asset_tag, site_id, rack_id, position, face, primary_ip4_id, primary_ip6_id, cluster_id, virtual_chassis_id, vc_position, vc_priority, comments) VALUES ('ac8f75eb70b947ed9f1122c261441902', '2021-05-25', '2021-05-25 23:06:24.896510', '{}', NULL, NULL, NULL, NULL, '445a5bc61a1e4d15a960a436f7346a56', 'f4463a85c0514c93a1c104a879b41390', NULL, NULL, 'Device 1', 'Device 00000001', '', NULL, 'd5b2db2cdb8a4335ba1ebc73774f58b1', NULL, NULL, '', NULL, NULL, NULL, NULL, NULL, NULL, ''), ('472bc03428ff40f38aece3d196b2db6a', '2021-05-25', '2021-05-25 23:06:25.040411', '{}', NULL, NULL, NULL, NULL, '445a5bc61a1e4d15a960a436f7346a56', 'f4463a85c0514c93a1c104a879b41390', NULL, NULL, 'Device 2', 'Device 00000002', '', NULL, 'd5b2db2cdb8a4335ba1ebc73774f58b1', NULL, NULL, '', NULL, NULL, NULL, NULL, NULL, NULL, ''), ('5a07d342093f4546967b4f5f59b7f1a1', '2021-05-25', '2021-05-25 23:06:25.185785', '{}', NULL, NULL, NULL, NULL, '445a5bc61a1e4d15a960a436f7346a56', 'f4463a85c0514c93a1c104a879b41390', NULL, NULL, 'Device 3', 'Device 00000003', '', NULL, 'd5b2db2cdb8a4335ba1ebc73774f58b1', NULL, NULL, '', NULL, NULL, NULL, NULL, NULL, NULL, ''), ('3dee57881e404c6f9f05a29000919b0c', '2021-05-25', '2021-05-25 23:06:25.308650', '{}', NULL, NULL, NULL, NULL, '445a5bc61a1e4d15a960a436f7346a56', 'f4463a85c0514c93a1c104a879b41390', NULL, NULL, 'Device 4', 'Device 00000004', '', NULL, 'd5b2db2cdb8a4335ba1ebc73774f58b1', NULL, NULL, '', NULL, NULL, NULL, NULL, NULL, NULL, ''), ('2863677820d9411897b280970c02a8df', '2021-05-25', '2021-05-25 23:06:25.429588', '{}', NULL, NULL, NULL, NULL, '445a5bc61a1e4d15a960a436f7346a56', 'f4463a85c0514c93a1c104a879b41390', NULL, NULL, 'Device 5', 'Device 00000005', '', NULL, 'd5b2db2cdb8a4335ba1ebc73774f58b1', NULL, NULL, '', NULL, NULL, NULL, NULL, NULL, NULL, ''), ('c02582598a6a4173b4e2142a2fa79136', '2021-05-25', '2021-05-25 23:06:25.552599', '{}', NULL, NULL, NULL, NULL, '445a5bc61a1e4d15a960a436f7346a56', 'f4463a85c0514c93a1c104a879b41390', NULL, NULL, 'Device 6', 'Device 00000006', '', NULL, 'd5b2db2cdb8a4335ba1ebc73774f58b1', NULL, NULL, '', NULL, NULL, NULL, NULL, NULL, NULL, ''), ('1e4f1d8d82f5496ea1d106553290b100', '2021-05-25', '2021-05-25 23:06:25.677895', '{}', NULL, NULL, NULL, NULL, '445a5bc61a1e4d15a960a436f7346a56', 'f4463a85c0514c93a1c104a879b41390', NULL, NULL, 'Device 7', 'Device 00000007', '', NULL, 'd5b2db2cdb8a4335ba1ebc73774f58b1', NULL, NULL, '', NULL, NULL, NULL, NULL, NULL, NULL, ''), ('fb1bd899aa154fa38bb20eeed46f258d', '2021-05-25', '2021-05-25 23:06:25.796185', '{}', NULL, NULL, NULL, NULL, '445a5bc61a1e4d15a960a436f7346a56', 'f4463a85c0514c93a1c104a879b41390', NULL, NULL, 'Device 8', 'Device 00000008', '', NULL, 'd5b2db2cdb8a4335ba1ebc73774f58b1', NULL, NULL, '', NULL, NULL, NULL, NULL, NULL, NULL, ''), ('8eef839fc2ac41639ae5fa38c7c47530', '2021-05-25', '2021-05-25 23:06:25.915469', '{}', NULL, NULL, NULL, NULL, '445a5bc61a1e4d15a960a436f7346a56', 'f4463a85c0514c93a1c104a879b41390', NULL, NULL, 'Device 9', 'Device 00000009', '', NULL, 'd5b2db2cdb8a4335ba1ebc73774f58b1', NULL, NULL, '', NULL, NULL, NULL, NULL, NULL, NULL, ''), ('9b779fbd746f4ab8bffd202f0ced8d19', '2021-05-25', '2021-05-25 23:06:26.034533', '{}', NULL, NULL, NULL, NULL, '445a5bc61a1e4d15a960a436f7346a56', 'f4463a85c0514c93a1c104a879b41390', NULL, NULL, 'Device 10', 'Device 00000010', '', NULL, 'd5b2db2cdb8a4335ba1ebc73774f58b1', NULL, NULL, '', NULL, NULL, NULL, NULL, NULL, NULL, ''), ('b289cfc65fcd4111a104c48d3bbce1b2', '2021-05-25', '2021-05-25 23:06:26.158382', '{}', NULL, NULL, NULL, NULL, '445a5bc61a1e4d15a960a436f7346a56', 'f4463a85c0514c93a1c104a879b41390', NULL, NULL, 'Device 11', 'Device 00000011', '', NULL, 'd5b2db2cdb8a4335ba1ebc73774f58b1', NULL, NULL, '', NULL, NULL, NULL, NULL, NULL, NULL, ''), ('76c7aa720a0e45cdbed085aac602bd42', '2021-05-25', '2021-05-25 23:06:26.283855', '{}', NULL, NULL, NULL, NULL, '445a5bc61a1e4d15a960a436f7346a56', 'f4463a85c0514c93a1c104a879b41390', NULL, NULL, 'Device 12', 'Device 00000012', '', NULL, 'd5b2db2cdb8a4335ba1ebc73774f58b1', NULL, NULL, '', NULL, NULL, NULL, NULL, NULL, NULL, '');`,
			`INSERT INTO dcim_interface (id, _custom_field_data, device_id, name, label, description, cable_id, _cable_peer_type_id, _cable_peer_id, _path_id, enabled, mac_address, mtu, mode, _name, lag_id, type, mgmt_only, untagged_vlan_id) VALUES ('f59ee9c8d9be400e92c12876c91ec63e', '{}', 'ac8f75eb70b947ed9f1122c261441902', '1/0', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0001999999999999000000............', NULL, '1000base-t', 0, NULL), ('0368adc2acab47f7955dd2932a18d22d', '{}', 'ac8f75eb70b947ed9f1122c261441902', '1/1', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0001999999999999000001............', NULL, '1000base-t', 0, NULL), ('184d93298a6047cab3e095d431d1ceac', '{}', 'ac8f75eb70b947ed9f1122c261441902', '1/2', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0001999999999999000002............', NULL, '1000base-t', 0, NULL), ('b2a11adbf86240a0afd77e09176d2d91', '{}', 'ac8f75eb70b947ed9f1122c261441902', '1/3', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0001999999999999000003............', NULL, '1000base-t', 0, NULL), ('a47ee0165910484abe48689eb5d4ddad', '{}', 'ac8f75eb70b947ed9f1122c261441902', '1/4', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0001999999999999000004............', NULL, '1000base-t', 0, NULL), ('dd08854c5af14e3a8460270c199ddc88', '{}', 'ac8f75eb70b947ed9f1122c261441902', '1/5', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0001999999999999000005............', NULL, '1000base-t', 0, NULL), ('90b288e5e608471fa2e03ae5a4f0855e', '{}', 'ac8f75eb70b947ed9f1122c261441902', '1/6', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0001999999999999000006............', NULL, '1000base-t', 0, NULL), ('ef8dcdbbd16a45d2ad5858ce8dd1f812', '{}', 'ac8f75eb70b947ed9f1122c261441902', '1/7', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0001999999999999000007............', NULL, '1000base-t', 0, NULL), ('84d2c6ac834c42cba3dac4b40399a4df', '{}', 'ac8f75eb70b947ed9f1122c261441902', '1/8', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0001999999999999000008............', NULL, '1000base-t', 0, NULL), ('379dfa1d9d3b4648847acd99d1f60876', '{}', 'ac8f75eb70b947ed9f1122c261441902', '1/9', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0001999999999999000009............', NULL, '1000base-t', 0, NULL), ('f1887641e5ba457885c4413ff4d45e61', '{}', 'ac8f75eb70b947ed9f1122c261441902', '1/10', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0001999999999999000010............', NULL, '1000base-t', 0, NULL), ('7a10addee4be4a81b88a996c95df4501', '{}', 'ac8f75eb70b947ed9f1122c261441902', '1/11', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0001999999999999000011............', NULL, '1000base-t', 0, NULL), ('155d4481ffcc4bb9af1efcf9f2be1ec4', '{}', 'ac8f75eb70b947ed9f1122c261441902', '1/12', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0001999999999999000012............', NULL, '1000base-t', 0, NULL), ('e99b1fb06a9246119984d270006bf250', '{}', '472bc03428ff40f38aece3d196b2db6a', '2/0', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0002999999999999000000............', NULL, '1000base-t', 0, NULL), ('2fce292f27e2466a95800d6b1ccec072', '{}', '472bc03428ff40f38aece3d196b2db6a', '2/1', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0002999999999999000001............', NULL, '1000base-t', 0, NULL), ('25fc5fcf96414ac78ae7d955e9eac1b1', '{}', '472bc03428ff40f38aece3d196b2db6a', '2/2', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0002999999999999000002............', NULL, '1000base-t', 0, NULL), ('a644aeb247d3416c8fe85915244f44eb', '{}', '472bc03428ff40f38aece3d196b2db6a', '2/3', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0002999999999999000003............', NULL, '1000base-t', 0, NULL), ('cbd32465b0be440aa7b92a1b0ad93e42', '{}', '472bc03428ff40f38aece3d196b2db6a', '2/4', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0002999999999999000004............', NULL, '1000base-t', 0, NULL), ('09096326e0e04829a023e8aecb3dbb7a', '{}', '472bc03428ff40f38aece3d196b2db6a', '2/5', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0002999999999999000005............', NULL, '1000base-t', 0, NULL), ('cc8c93a3038747b9a31b07fc738d53d0', '{}', '472bc03428ff40f38aece3d196b2db6a', '2/6', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0002999999999999000006............', NULL, '1000base-t', 0, NULL), ('984f33c5dcdc49189a986fde6d3cdb7f', '{}', '472bc03428ff40f38aece3d196b2db6a', '2/7', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0002999999999999000007............', NULL, '1000base-t', 0, NULL), ('d16a9653f2684a638669b6b656abe849', '{}', '472bc03428ff40f38aece3d196b2db6a', '2/8', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0002999999999999000008............', NULL, '1000base-t', 0, NULL), ('b91b42b9a94c43fe97a15530bf1a467c', '{}', '472bc03428ff40f38aece3d196b2db6a', '2/9', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0002999999999999000009............', NULL, '1000base-t', 0, NULL), ('1eca9516530c47aaba0c974ce171c79d', '{}', '472bc03428ff40f38aece3d196b2db6a', '2/10', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0002999999999999000010............', NULL, '1000base-t', 0, NULL), ('7fabeed7ba144ba69dc0bfe420e25743', '{}', '472bc03428ff40f38aece3d196b2db6a', '2/11', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0002999999999999000011............', NULL, '1000base-t', 0, NULL), ('d1345e6785434272a357acafb5aee209', '{}', '472bc03428ff40f38aece3d196b2db6a', '2/12', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0002999999999999000012............', NULL, '1000base-t', 0, NULL), ('34ae9d8a51934325aa2e07292a9ab669', '{}', '5a07d342093f4546967b4f5f59b7f1a1', '3/0', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0003999999999999000000............', NULL, '1000base-t', 0, NULL), ('6db667bb23d44c99b48c39eba6cdbea2', '{}', '5a07d342093f4546967b4f5f59b7f1a1', '3/1', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0003999999999999000001............', NULL, '1000base-t', 0, NULL), ('24485ac4709a4d1f96b7473e67b5fb36', '{}', '5a07d342093f4546967b4f5f59b7f1a1', '3/2', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0003999999999999000002............', NULL, '1000base-t', 0, NULL), ('11bbe4aa92a945788618ed86ee38a3eb', '{}', '5a07d342093f4546967b4f5f59b7f1a1', '3/3', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0003999999999999000003............', NULL, '1000base-t', 0, NULL), ('17e4b3d49e994d3089243a111945b05e', '{}', '5a07d342093f4546967b4f5f59b7f1a1', '3/4', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0003999999999999000004............', NULL, '1000base-t', 0, NULL), ('d8f2f7b26f2b49bcbf7688a5bd6ebdc2', '{}', '5a07d342093f4546967b4f5f59b7f1a1', '3/5', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0003999999999999000005............', NULL, '1000base-t', 0, NULL), ('f4d16b1655ce4bff918c502ef7257465', '{}', '5a07d342093f4546967b4f5f59b7f1a1', '3/6', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0003999999999999000006............', NULL, '1000base-t', 0, NULL), ('030b47e357654480b31811829325f832', '{}', '5a07d342093f4546967b4f5f59b7f1a1', '3/7', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0003999999999999000007............', NULL, '1000base-t', 0, NULL), ('8f3a835c43d24f68b43cd720d6e6db5b', '{}', '5a07d342093f4546967b4f5f59b7f1a1', '3/8', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0003999999999999000008............', NULL, '1000base-t', 0, NULL), ('dedf0df4cf444f6d85f7d3207c8b371c', '{}', '5a07d342093f4546967b4f5f59b7f1a1', '3/9', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0003999999999999000009............', NULL, '1000base-t', 0, NULL), ('57d43e96204b446d8edf548dba1f2e1a', '{}', '5a07d342093f4546967b4f5f59b7f1a1', '3/10', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0003999999999999000010............', NULL, '1000base-t', 0, NULL), ('eabed12f76bd4ba78e6f6365086489cf', '{}', '5a07d342093f4546967b4f5f59b7f1a1', '3/11', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0003999999999999000011............', NULL, '1000base-t', 0, NULL), ('3d8a90402c544ba89715213a57e747d8', '{}', '5a07d342093f4546967b4f5f59b7f1a1', '3/12', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0003999999999999000012............', NULL, '1000base-t', 0, NULL), ('4721387ccf1d41f2b27863ccc0e1cf0b', '{}', '3dee57881e404c6f9f05a29000919b0c', '1/0', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0001999999999999000000............', NULL, '1000base-t', 0, NULL), ('8da6b53a3f30492e9a65bf53047d56b1', '{}', '3dee57881e404c6f9f05a29000919b0c', '1/1', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0001999999999999000001............', NULL, '1000base-t', 0, NULL), ('c98c9124068145b2b84f7e01d08ba482', '{}', '3dee57881e404c6f9f05a29000919b0c', '1/2', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0001999999999999000002............', NULL, '1000base-t', 0, NULL), ('b4c0da653b0244ada7ab65c9aeb71329', '{}', '3dee57881e404c6f9f05a29000919b0c', '1/3', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0001999999999999000003............', NULL, '1000base-t', 0, NULL), ('726965fab54240d2b1f0b65c1c9171a3', '{}', '3dee57881e404c6f9f05a29000919b0c', '1/4', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0001999999999999000004............', NULL, '1000base-t', 0, NULL), ('5a15a866581449b5b91331986bef95ff', '{}', '3dee57881e404c6f9f05a29000919b0c', '1/5', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0001999999999999000005............', NULL, '1000base-t', 0, NULL), ('acb4d46587264265bd6cc94801ac4618', '{}', '3dee57881e404c6f9f05a29000919b0c', '1/6', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0001999999999999000006............', NULL, '1000base-t', 0, NULL), ('a1f4740641634936b74fc53098da8d63', '{}', '3dee57881e404c6f9f05a29000919b0c', '1/7', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0001999999999999000007............', NULL, '1000base-t', 0, NULL), ('a9a08741ebd94c95ba2b3e1de89e688b', '{}', '3dee57881e404c6f9f05a29000919b0c', '1/8', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0001999999999999000008............', NULL, '1000base-t', 0, NULL), ('347444c275e44360aa0f507bc5e386d7', '{}', '3dee57881e404c6f9f05a29000919b0c', '1/9', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0001999999999999000009............', NULL, '1000base-t', 0, NULL), ('bda445daf167449a8b87fa36bd4176a2', '{}', '3dee57881e404c6f9f05a29000919b0c', '1/10', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0001999999999999000010............', NULL, '1000base-t', 0, NULL), ('15693bbdbfe14630bf53641669868c86', '{}', '3dee57881e404c6f9f05a29000919b0c', '1/11', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0001999999999999000011............', NULL, '1000base-t', 0, NULL), ('0c80789a9d6b42a083de917349d6e054', '{}', '3dee57881e404c6f9f05a29000919b0c', '1/12', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0001999999999999000012............', NULL, '1000base-t', 0, NULL), ('e46707f53dd64bd7a10236d8dc2f15c7', '{}', '2863677820d9411897b280970c02a8df', '2/0', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0002999999999999000000............', NULL, '1000base-t', 0, NULL), ('21ba6dc8e3844091adb47f5b43e72407', '{}', '2863677820d9411897b280970c02a8df', '2/1', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0002999999999999000001............', NULL, '1000base-t', 0, NULL), ('a8c0eef139c94c9882ba072ed524d5b8', '{}', '2863677820d9411897b280970c02a8df', '2/2', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0002999999999999000002............', NULL, '1000base-t', 0, NULL), ('7f5fd3f8a65e479787a18eb0365b496a', '{}', '2863677820d9411897b280970c02a8df', '2/3', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0002999999999999000003............', NULL, '1000base-t', 0, NULL), ('01f6e2fbe08c462eb4a8d44a8a86f4f2', '{}', '2863677820d9411897b280970c02a8df', '2/4', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0002999999999999000004............', NULL, '1000base-t', 0, NULL), ('54bdd965e57c4225a86e4c14edc96fbd', '{}', '2863677820d9411897b280970c02a8df', '2/5', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0002999999999999000005............', NULL, '1000base-t', 0, NULL), ('8f0a020d831e4109bae98af8904f09c6', '{}', '2863677820d9411897b280970c02a8df', '2/6', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0002999999999999000006............', NULL, '1000base-t', 0, NULL), ('03475695b2f24a0ebd12a6647301038d', '{}', '2863677820d9411897b280970c02a8df', '2/7', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0002999999999999000007............', NULL, '1000base-t', 0, NULL), ('ef316e3e17ae402eb972a94951ad6747', '{}', '2863677820d9411897b280970c02a8df', '2/8', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0002999999999999000008............', NULL, '1000base-t', 0, NULL), ('5be6403ea3d34685ac3bf3f8619e6a0e', '{}', '2863677820d9411897b280970c02a8df', '2/9', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0002999999999999000009............', NULL, '1000base-t', 0, NULL), ('23f4be7856cc440db5ba6cc68b582392', '{}', '2863677820d9411897b280970c02a8df', '2/10', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0002999999999999000010............', NULL, '1000base-t', 0, NULL), ('e0a2037a4d0a4d5a8ea516dd70da74fc', '{}', '2863677820d9411897b280970c02a8df', '2/11', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0002999999999999000011............', NULL, '1000base-t', 0, NULL), ('da231d9c4e344140b1759313e820f91d', '{}', '2863677820d9411897b280970c02a8df', '2/12', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0002999999999999000012............', NULL, '1000base-t', 0, NULL), ('d3cd6653d0ad4057aa49d043de93e0e0', '{}', 'c02582598a6a4173b4e2142a2fa79136', '3/0', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0003999999999999000000............', NULL, '1000base-t', 0, NULL), ('ee27e1960dae4333a646d4568d3fd356', '{}', 'c02582598a6a4173b4e2142a2fa79136', '3/1', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0003999999999999000001............', NULL, '1000base-t', 0, NULL), ('2df21977e65e494e9cd137bee82c4074', '{}', 'c02582598a6a4173b4e2142a2fa79136', '3/2', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0003999999999999000002............', NULL, '1000base-t', 0, NULL), ('37e8cea17d794b39b40922694b6518b7', '{}', 'c02582598a6a4173b4e2142a2fa79136', '3/3', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0003999999999999000003............', NULL, '1000base-t', 0, NULL), ('79fda90580b54e16b8309d91299b7ee1', '{}', 'c02582598a6a4173b4e2142a2fa79136', '3/4', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0003999999999999000004............', NULL, '1000base-t', 0, NULL), ('9fd0b35a4b194f14ba8ca93d030ba32a', '{}', 'c02582598a6a4173b4e2142a2fa79136', '3/5', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0003999999999999000005............', NULL, '1000base-t', 0, NULL), ('cab9bc0d061d4a35bbd0926425da2bcf', '{}', 'c02582598a6a4173b4e2142a2fa79136', '3/6', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0003999999999999000006............', NULL, '1000base-t', 0, NULL), ('a7fcbe0c96f54698b171571d2083ad6c', '{}', 'c02582598a6a4173b4e2142a2fa79136', '3/7', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0003999999999999000007............', NULL, '1000base-t', 0, NULL), ('40bccaed4ee6449a9ebe2bd16d531d6b', '{}', 'c02582598a6a4173b4e2142a2fa79136', '3/8', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0003999999999999000008............', NULL, '1000base-t', 0, NULL), ('36d21ebf625745c495b091736e946ea3', '{}', 'c02582598a6a4173b4e2142a2fa79136', '3/9', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0003999999999999000009............', NULL, '1000base-t', 0, NULL), ('379645b037364d20a91e3af5ab651417', '{}', 'c02582598a6a4173b4e2142a2fa79136', '3/10', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0003999999999999000010............', NULL, '1000base-t', 0, NULL), ('187927c303e0453bb48b32409cedafee', '{}', 'c02582598a6a4173b4e2142a2fa79136', '3/11', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0003999999999999000011............', NULL, '1000base-t', 0, NULL), ('1c9f6ce2d50b4427a73c37f526f0d9c5', '{}', 'c02582598a6a4173b4e2142a2fa79136', '3/12', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0003999999999999000012............', NULL, '1000base-t', 0, NULL), ('5814838347bf4f9d98ac10c9b8070016', '{}', '1e4f1d8d82f5496ea1d106553290b100', '1/0', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0001999999999999000000............', NULL, '1000base-t', 0, NULL), ('a896177349ef47b0998a66e282132056', '{}', '1e4f1d8d82f5496ea1d106553290b100', '1/1', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0001999999999999000001............', NULL, '1000base-t', 0, NULL), ('70feb3d5b06e4a2ab9ca0df03f381871', '{}', '1e4f1d8d82f5496ea1d106553290b100', '1/2', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0001999999999999000002............', NULL, '1000base-t', 0, NULL), ('cc5aef6cc8954f61a04a3936d4c14190', '{}', '1e4f1d8d82f5496ea1d106553290b100', '1/3', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0001999999999999000003............', NULL, '1000base-t', 0, NULL), ('445f1687f91849f3ae0f6851d1711529', '{}', '1e4f1d8d82f5496ea1d106553290b100', '1/4', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0001999999999999000004............', NULL, '1000base-t', 0, NULL), ('e33b4031b6344a02a87b76bd016007f7', '{}', '1e4f1d8d82f5496ea1d106553290b100', '1/5', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0001999999999999000005............', NULL, '1000base-t', 0, NULL), ('11049e13703f4061b8f2630b0d1d0bc2', '{}', '1e4f1d8d82f5496ea1d106553290b100', '1/6', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0001999999999999000006............', NULL, '1000base-t', 0, NULL), ('5aa186a773e644b5bdfda3a8ea0bda3e', '{}', '1e4f1d8d82f5496ea1d106553290b100', '1/7', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0001999999999999000007............', NULL, '1000base-t', 0, NULL), ('44676df670e84a018706edbf8e374888', '{}', '1e4f1d8d82f5496ea1d106553290b100', '1/8', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0001999999999999000008............', NULL, '1000base-t', 0, NULL), ('ba60c4dd2f57469fb4aed07a60b87766', '{}', '1e4f1d8d82f5496ea1d106553290b100', '1/9', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0001999999999999000009............', NULL, '1000base-t', 0, NULL), ('e3adb29856db488f9eff24c38ed92a67', '{}', '1e4f1d8d82f5496ea1d106553290b100', '1/10', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0001999999999999000010............', NULL, '1000base-t', 0, NULL), ('da9931cb08404e43babcc64a2b20dd1d', '{}', '1e4f1d8d82f5496ea1d106553290b100', '1/11', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0001999999999999000011............', NULL, '1000base-t', 0, NULL), ('23c0c912e4464fa8a986b5c532f365e5', '{}', '1e4f1d8d82f5496ea1d106553290b100', '1/12', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0001999999999999000012............', NULL, '1000base-t', 0, NULL), ('d30b64611f2043e3a1f4dae0e640faa4', '{}', 'fb1bd899aa154fa38bb20eeed46f258d', '2/0', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0002999999999999000000............', NULL, '1000base-t', 0, NULL), ('68535793a9094a55917f4c841fa0de95', '{}', 'fb1bd899aa154fa38bb20eeed46f258d', '2/1', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0002999999999999000001............', NULL, '1000base-t', 0, NULL), ('cbf5ff5829cc4781bf5a605fd7a4cf18', '{}', 'fb1bd899aa154fa38bb20eeed46f258d', '2/2', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0002999999999999000002............', NULL, '1000base-t', 0, NULL), ('ee16fcd952b6459bb2572c650c4eb81d', '{}', 'fb1bd899aa154fa38bb20eeed46f258d', '2/3', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0002999999999999000003............', NULL, '1000base-t', 0, NULL), ('02e7337f6bdd452587ee67f0e98002ac', '{}', 'fb1bd899aa154fa38bb20eeed46f258d', '2/4', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0002999999999999000004............', NULL, '1000base-t', 0, NULL), ('4c52528e32d5435fb15057c423f7aaf1', '{}', 'fb1bd899aa154fa38bb20eeed46f258d', '2/5', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0002999999999999000005............', NULL, '1000base-t', 0, NULL), ('9a3cb3eac14948719b380a0702bbbbdd', '{}', 'fb1bd899aa154fa38bb20eeed46f258d', '2/6', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0002999999999999000006............', NULL, '1000base-t', 0, NULL), ('7345bbe099e040bfac1d4fe242493cc3', '{}', 'fb1bd899aa154fa38bb20eeed46f258d', '2/7', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0002999999999999000007............', NULL, '1000base-t', 0, NULL), ('966323ececf040eaa1d093ba61041548', '{}', 'fb1bd899aa154fa38bb20eeed46f258d', '2/8', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0002999999999999000008............', NULL, '1000base-t', 0, NULL), ('ae88dd914f8145b489b580479a230537', '{}', 'fb1bd899aa154fa38bb20eeed46f258d', '2/9', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0002999999999999000009............', NULL, '1000base-t', 0, NULL), ('f8f789aa8d1041f4a964280544f02b47', '{}', 'fb1bd899aa154fa38bb20eeed46f258d', '2/10', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0002999999999999000010............', NULL, '1000base-t', 0, NULL), ('b60f62b5b7d247e7a4a096b6ec1b5332', '{}', 'fb1bd899aa154fa38bb20eeed46f258d', '2/11', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0002999999999999000011............', NULL, '1000base-t', 0, NULL), ('bea79b7ff414489aa83b23095307b607', '{}', 'fb1bd899aa154fa38bb20eeed46f258d', '2/12', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0002999999999999000012............', NULL, '1000base-t', 0, NULL), ('d5e580fcb2d34e079927f4c9dc54556d', '{}', '8eef839fc2ac41639ae5fa38c7c47530', '3/0', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0003999999999999000000............', NULL, '1000base-t', 0, NULL), ('ea6a35a502d146e2ae7e7267c0ef8bb9', '{}', '8eef839fc2ac41639ae5fa38c7c47530', '3/1', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0003999999999999000001............', NULL, '1000base-t', 0, NULL), ('a966c10a37164a1488893979438ac281', '{}', '8eef839fc2ac41639ae5fa38c7c47530', '3/2', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0003999999999999000002............', NULL, '1000base-t', 0, NULL), ('6aa7e3b947a74257a26b0e769a34ed60', '{}', '8eef839fc2ac41639ae5fa38c7c47530', '3/3', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0003999999999999000003............', NULL, '1000base-t', 0, NULL), ('9407a89626e942e7988277f787ea2fb6', '{}', '8eef839fc2ac41639ae5fa38c7c47530', '3/4', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0003999999999999000004............', NULL, '1000base-t', 0, NULL), ('bffecb9911b143569545ad3a8e6a2016', '{}', '8eef839fc2ac41639ae5fa38c7c47530', '3/5', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0003999999999999000005............', NULL, '1000base-t', 0, NULL), ('67a5b01546cd4b61a11c728865b79da6', '{}', '8eef839fc2ac41639ae5fa38c7c47530', '3/6', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0003999999999999000006............', NULL, '1000base-t', 0, NULL), ('0bb0877c8434446f839ed6bf20f7392f', '{}', '8eef839fc2ac41639ae5fa38c7c47530', '3/7', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0003999999999999000007............', NULL, '1000base-t', 0, NULL), ('f333be856ada41daab3bb08af6af68c0', '{}', '8eef839fc2ac41639ae5fa38c7c47530', '3/8', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0003999999999999000008............', NULL, '1000base-t', 0, NULL), ('084deacd9d1d434589170caeeeebdd5b', '{}', '8eef839fc2ac41639ae5fa38c7c47530', '3/9', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0003999999999999000009............', NULL, '1000base-t', 0, NULL), ('d7c222ae40c14a819e33650f614b1c60', '{}', '8eef839fc2ac41639ae5fa38c7c47530', '3/10', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0003999999999999000010............', NULL, '1000base-t', 0, NULL), ('c2af33c8cb6e4aa3987631961498305e', '{}', '8eef839fc2ac41639ae5fa38c7c47530', '3/11', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0003999999999999000011............', NULL, '1000base-t', 0, NULL), ('1d947ed80ec3476e9e7d3fa239b202c7', '{}', '8eef839fc2ac41639ae5fa38c7c47530', '3/12', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0003999999999999000012............', NULL, '1000base-t', 0, NULL), ('e66d5d70311e461b8810e600c94f0200', '{}', '9b779fbd746f4ab8bffd202f0ced8d19', '1/0', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0001999999999999000000............', NULL, '1000base-t', 0, NULL), ('c91e9a53341f4338be05f323d35a650d', '{}', '9b779fbd746f4ab8bffd202f0ced8d19', '1/1', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0001999999999999000001............', NULL, '1000base-t', 0, NULL), ('3b5a833086a94b2989a9bc2315dd6492', '{}', '9b779fbd746f4ab8bffd202f0ced8d19', '1/2', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0001999999999999000002............', NULL, '1000base-t', 0, NULL), ('fe919205c4294a7d8c218d0c4bc5a8c6', '{}', '9b779fbd746f4ab8bffd202f0ced8d19', '1/3', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0001999999999999000003............', NULL, '1000base-t', 0, NULL), ('215b302d303349a3a3738290707e70ee', '{}', '9b779fbd746f4ab8bffd202f0ced8d19', '1/4', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0001999999999999000004............', NULL, '1000base-t', 0, NULL), ('1326ce2a7da640e6ba0b10a5fc4633d0', '{}', '9b779fbd746f4ab8bffd202f0ced8d19', '1/5', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0001999999999999000005............', NULL, '1000base-t', 0, NULL), ('4f42f1ccbe004e9898afa9ee586d8b33', '{}', '9b779fbd746f4ab8bffd202f0ced8d19', '1/6', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0001999999999999000006............', NULL, '1000base-t', 0, NULL), ('0e146c3b1b4e4d49a2e7fe402215b7ce', '{}', '9b779fbd746f4ab8bffd202f0ced8d19', '1/7', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0001999999999999000007............', NULL, '1000base-t', 0, NULL), ('362215840232480887fd2cb32f2a286e', '{}', '9b779fbd746f4ab8bffd202f0ced8d19', '1/8', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0001999999999999000008............', NULL, '1000base-t', 0, NULL), ('fbb1ade5dcb049d1990aa726d2055136', '{}', '9b779fbd746f4ab8bffd202f0ced8d19', '1/9', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0001999999999999000009............', NULL, '1000base-t', 0, NULL), ('0706877978544d5a9762d600bc54c965', '{}', '9b779fbd746f4ab8bffd202f0ced8d19', '1/10', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0001999999999999000010............', NULL, '1000base-t', 0, NULL), ('7a5e3ac983e84d1099b757818a5a1d89', '{}', '9b779fbd746f4ab8bffd202f0ced8d19', '1/11', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0001999999999999000011............', NULL, '1000base-t', 0, NULL), ('3530a26035634b648f3381a750558e94', '{}', '9b779fbd746f4ab8bffd202f0ced8d19', '1/12', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0001999999999999000012............', NULL, '1000base-t', 0, NULL), ('80e033fba5d641f28a801b895e233b2d', '{}', 'b289cfc65fcd4111a104c48d3bbce1b2', '2/0', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0002999999999999000000............', NULL, '1000base-t', 0, NULL), ('9b7080c4f13d4b5e8348e7670c088ea7', '{}', 'b289cfc65fcd4111a104c48d3bbce1b2', '2/1', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0002999999999999000001............', NULL, '1000base-t', 0, NULL), ('570cc6f4157c4a74a7fc380ac94f9bc6', '{}', 'b289cfc65fcd4111a104c48d3bbce1b2', '2/2', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0002999999999999000002............', NULL, '1000base-t', 0, NULL), ('ced3fe645c604871a60fad2544121729', '{}', 'b289cfc65fcd4111a104c48d3bbce1b2', '2/3', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0002999999999999000003............', NULL, '1000base-t', 0, NULL), ('e012acac7ff64e569e00f5c9fe54bce0', '{}', 'b289cfc65fcd4111a104c48d3bbce1b2', '2/4', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0002999999999999000004............', NULL, '1000base-t', 0, NULL), ('ed72f0bf417b474992b805e2fcb84892', '{}', 'b289cfc65fcd4111a104c48d3bbce1b2', '2/5', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0002999999999999000005............', NULL, '1000base-t', 0, NULL), ('e090dae7db544a3b9253aee14eeb74b2', '{}', 'b289cfc65fcd4111a104c48d3bbce1b2', '2/6', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0002999999999999000006............', NULL, '1000base-t', 0, NULL), ('5a9b54372f1241aaa8693dd5746eb256', '{}', 'b289cfc65fcd4111a104c48d3bbce1b2', '2/7', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0002999999999999000007............', NULL, '1000base-t', 0, NULL), ('354e06144ea3480bae9ceeaa43f2c7d3', '{}', 'b289cfc65fcd4111a104c48d3bbce1b2', '2/8', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0002999999999999000008............', NULL, '1000base-t', 0, NULL), ('6f788ba0a25d4aa6988e3b118b2c2b4a', '{}', 'b289cfc65fcd4111a104c48d3bbce1b2', '2/9', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0002999999999999000009............', NULL, '1000base-t', 0, NULL), ('fc10c539e4324d5f9b2ab58d0e18f7a3', '{}', 'b289cfc65fcd4111a104c48d3bbce1b2', '2/10', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0002999999999999000010............', NULL, '1000base-t', 0, NULL), ('f9b862c213a0494b9abbc2692a538bce', '{}', 'b289cfc65fcd4111a104c48d3bbce1b2', '2/11', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0002999999999999000011............', NULL, '1000base-t', 0, NULL), ('607827a9409e4a648c2e814991002256', '{}', 'b289cfc65fcd4111a104c48d3bbce1b2', '2/12', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0002999999999999000012............', NULL, '1000base-t', 0, NULL), ('4ab60aee21d74095b9a96608f2f9bcc0', '{}', '76c7aa720a0e45cdbed085aac602bd42', '3/0', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0003999999999999000000............', NULL, '1000base-t', 0, NULL), ('ca46d23604bb40d09ebe04869a4f3b3e', '{}', '76c7aa720a0e45cdbed085aac602bd42', '3/1', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0003999999999999000001............', NULL, '1000base-t', 0, NULL), ('1844ad332ae24979aa1f326833222e4f', '{}', '76c7aa720a0e45cdbed085aac602bd42', '3/2', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0003999999999999000002............', NULL, '1000base-t', 0, NULL), ('2cae5b761fb44f46b82f93f3df1f2b99', '{}', '76c7aa720a0e45cdbed085aac602bd42', '3/3', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0003999999999999000003............', NULL, '1000base-t', 0, NULL), ('bbc76eea059945db9af15a83d8f333ac', '{}', '76c7aa720a0e45cdbed085aac602bd42', '3/4', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0003999999999999000004............', NULL, '1000base-t', 0, NULL), ('95e245f51e8a40489bfbb891571de382', '{}', '76c7aa720a0e45cdbed085aac602bd42', '3/5', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0003999999999999000005............', NULL, '1000base-t', 0, NULL), ('bdb51b73b3764159a5dfa10b953a2f9c', '{}', '76c7aa720a0e45cdbed085aac602bd42', '3/6', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0003999999999999000006............', NULL, '1000base-t', 0, NULL), ('38a09d98ebcc4fe5b9a39139a9b6758a', '{}', '76c7aa720a0e45cdbed085aac602bd42', '3/7', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0003999999999999000007............', NULL, '1000base-t', 0, NULL), ('b3e5213e80bc4827bda29d725b08f269', '{}', '76c7aa720a0e45cdbed085aac602bd42', '3/8', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0003999999999999000008............', NULL, '1000base-t', 0, NULL), ('40f72d50972d40e6860e6a0ac04304c4', '{}', '76c7aa720a0e45cdbed085aac602bd42', '3/9', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0003999999999999000009............', NULL, '1000base-t', 0, NULL), ('3d12afa2b6164037b4afff1cd2e67855', '{}', '76c7aa720a0e45cdbed085aac602bd42', '3/10', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0003999999999999000010............', NULL, '1000base-t', 0, NULL), ('48faa5dc45064b41859e83ecc96a6df8', '{}', '76c7aa720a0e45cdbed085aac602bd42', '3/11', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0003999999999999000011............', NULL, '1000base-t', 0, NULL), ('02b252dfd7a94ba2825ad9fd53a7c6a7', '{}', '76c7aa720a0e45cdbed085aac602bd42', '3/12', '', '', NULL, NULL, NULL, NULL, 1, NULL, NULL, '', '0003999999999999000012............', NULL, '1000base-t', 0, NULL);`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `SELECT dcim_interface.id, dcim_interface._custom_field_data, dcim_interface.device_id, dcim_interface.name, dcim_interface.label, dcim_interface.description, dcim_interface.cable_id, dcim_interface._cable_peer_type_id, dcim_interface._cable_peer_id, dcim_interface._path_id, dcim_interface.enabled, dcim_interface.mac_address, dcim_interface.mtu, dcim_interface.mode, dcim_interface._name, dcim_interface.lag_id, dcim_interface.type, dcim_interface.mgmt_only, dcim_interface.untagged_vlan_id FROM dcim_interface INNER JOIN dcim_device ON (dcim_interface.device_id = dcim_device.id) INNER JOIN dcim_interface T3 ON (dcim_interface.lag_id = T3.id) WHERE (dcim_interface.device_id IN (SELECT U0.id FROM dcim_device U0 WHERE U0.virtual_chassis_id = '943f8560f24746d3af1e69189d431a34') AND dcim_interface.lag_id IS NOT NULL AND NOT (T3.device_id = dcim_interface.device_id)) ORDER BY dcim_device._name ASC, (dcim_interface._name) COLLATE utf8mb4_bin ASC;`,
				ExpectedErr: analyzer.ErrFieldMissing,
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
