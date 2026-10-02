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
)

// NameResolutionScriptTests contains self-contained name resolution script tests.
var NameResolutionScriptTests = []ScriptTest{
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
}
