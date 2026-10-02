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

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// WritesScriptTests contains self-contained writes script tests.
var WritesScriptTests = []ScriptTest{
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
				ExpectedErr: sql.ErrForeignKeyParentViolation,
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
				Dialect:     "mysql",
				ExpectedErr: sql.ErrForeignKeyParentViolation,
			},
			{
				Query:    "SELECT * FROM test;",
				Dialect:  "mysql",
				Expected: []sql.Row{{1, 1}, {4, 4}, {5, 5}},
			},
			{
				Query:    "SELECT * FROM test2;",
				Expected: []sql.Row{{4}},
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
				Expected: []sql.Row{{types.NewOkResult(2)}},
			},
		},
	},
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
	{
		Name: "empty table update",
		SetUpScript: []string{
			"create table t (i int primary key)",
			"insert into t values (1), (2), (3)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "update t set i = 0 where false",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 0, InsertID: 0, Info: plan.UpdateInfo{Matched: 0}}}},
			},
			{
				Query: "select * from t",
				Expected: []sql.Row{
					{1},
					{2},
					{3},
				},
			},
		},
	},
}
