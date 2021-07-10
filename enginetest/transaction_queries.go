// Copyright 2021 Dolthub, Inc.
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

import "github.com/dolthub/go-mysql-server/sql"

// TransactionTest is a script to test transaction correctness. It's similar to ScriptTest, but its assertions name
// clients that participate
type TransactionTest struct {
	// Name of the script test
	Name string
	// The sql statements to execute as setup, in order. Results are not checked, but statements must not error.
	// Setup scripts are run as a distinct client separate from the client used in any assertions.
	SetUpScript []string
	// The set of assertions to make after setup, in order
	// The transaction test runner augments the ScriptTest runner by allowing embedding of a client string in a query
	// comment to name the client running the query, like so:
	// /* client a */ select * from myTable
	Assertions []ScriptTestAssertion
}

var TransactionTests = []TransactionTest{
	{
		Name: "autocommit on",
		SetUpScript: []string{
			"create table t (x int primary key, y int)",
			"insert into t values (1, 1)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "/* client a */ insert into t values (2, 2)",
				Expected: []sql.Row{{sql.NewOkResult(1)}},
			},
			{
				Query:    "/* client b */ select * from t order by x",
				Expected: []sql.Row{{1, 1}, {2, 2}},
			},
			{
				Query:    "/* client b */ insert into t values (3, 3)",
				Expected: []sql.Row{{sql.NewOkResult(1)}},
			},
			{
				Query:    "/* client a */ select * from t order by x",
				Expected: []sql.Row{{1, 1}, {2, 2}, {3, 3}},
			},
		},
	},
	{
		Name: "autocommit off",
		SetUpScript: []string{
			"create table t (x int primary key, y int)",
			"insert into t values (1, 1)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "/* client a */ set autocommit = off",
				Expected: []sql.Row{{}},
			},
			{
				Query:    "/* client b */ set autocommit = off",
				Expected: []sql.Row{{}},
			},
			{
				Query:    "/* client b */ select * from t order by x",
				Expected: []sql.Row{{1, 1}},
			},
			{
				Query:    "/* client b */ insert into t values (2, 2)",
				Expected: []sql.Row{{sql.NewOkResult(1)}},
			},
			{
				Query: "/* client a */ select * from t order by x",
				Expected: []sql.Row{
					{1, 1},
				},
			},
			{
				Query:    "/* client a */ insert into t values (3,3)",
				Expected: []sql.Row{{sql.NewOkResult(1)}},
			},
			{
				Query:    "/* client b */ select * from t order by x",
				Expected: []sql.Row{{1, 1}, {2, 2}},
			},
			{
				Query:    "/* client b */ commit",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ select * from t order by x",
				Expected: []sql.Row{{1, 1}, {3, 3}},
			},
			{
				Query:    "/* client b */ select * from t order by x",
				Expected: []sql.Row{{1, 1}, {2, 2}},
			},
			{
				Query:    "/* client a */ commit",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ select * from t order by x",
				Expected: []sql.Row{{1, 1}, {2, 2}},
			},
			{
				Query:    "/* client b */ start transaction",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ select * from t order by x",
				Expected: []sql.Row{{1, 1}, {2, 2}, {3, 3}},
			},
			{
				Query:    "/* client a */ select * from t order by x",
				Expected: []sql.Row{{1, 1}, {2, 2}, {3, 3}},
			},
		},
	},
	{
		Name: "toggle autocommit",
		SetUpScript: []string{
			"create table t (x int primary key, y int)",
			"insert into t values (1, 1)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "/* client a */ set autocommit = off",
				Expected: []sql.Row{{}},
			},
			{
				Query:    "/* client b */ set autocommit = off",
				Expected: []sql.Row{{}},
			},
			{
				Query:    "/* client b */ insert into t values (2,2)",
				Expected: []sql.Row{{sql.NewOkResult(1)}},
			},
			{
				Query:    "/* client a */ select * from t order by x",
				Expected: []sql.Row{{1, 1}},
			},
			// should commit any pending transaction
			{
				Query:    "/* client b */ set autocommit = on",
				Expected: []sql.Row{{}},
			},
			{
				Query:    "/* client a */ select * from t order by x",
				Expected: []sql.Row{{1, 1}},
			},
			// client a sees the committed transaction from client b when it begins a new transaction
			{
				Query:    "/* client a */ set autocommit = on",
				Expected: []sql.Row{{}},
			},
			{
				Query:    "/* client a */ select * from t order by x",
				Expected: []sql.Row{{1, 1}, {2, 2}},
			},
		},
	},
	{
		Name: "autocommit on with explicit transactions",
		SetUpScript: []string{
			"create table t (x int primary key, y int)",
			"insert into t values (1, 1)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "/* client a */ start transaction",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ insert into t values (2, 2)",
				Expected: []sql.Row{{sql.NewOkResult(1)}},
			},
			{
				Query:    "/* client b */ select * from t order by x",
				Expected: []sql.Row{{1, 1}},
			},
			{
				Query:    "/* client a */ commit",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ select * from t order by x",
				Expected: []sql.Row{{1, 1}, {2, 2}},
			},
			// After commit, autocommit turns back on
			{
				Query:    "/* client a */ insert into t values (3, 3)",
				Expected: []sql.Row{{sql.NewOkResult(1)}},
			},
			{
				Query:    "/* client b */ select * from t order by x",
				Expected: []sql.Row{{1, 1}, {2, 2}, {3, 3}},
			},
		},
	},
	{
		Name: "rollback",
		SetUpScript: []string{
			"create table t (x int primary key, y int)",
			"insert into t values (1, 1)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "/* client a */ set autocommit = off",
				Expected: []sql.Row{{}},
			},
			{
				Query:    "/* client b */ set autocommit = off",
				Expected: []sql.Row{{}},
			},
			{
				Query:    "/* client a */ start transaction",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ start transaction",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ insert into t values (2, 2)",
				Expected: []sql.Row{{sql.NewOkResult(1)}},
			},
			{
				Query:    "/* client b */ insert into t values (3, 3)",
				Expected: []sql.Row{{sql.NewOkResult(1)}},
			},
			{
				Query:    "/* client a */ select * from t order by x",
				Expected: []sql.Row{{1, 1}, {2, 2}},
			},
			{
				Query:    "/* client b */ commit",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ select * from t order by x",
				Expected: []sql.Row{{1, 1}, {2, 2}},
			},
			{
				Query:    "/* client a */ rollback",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ select * from t order by x",
				Expected: []sql.Row{{1, 1}, {3, 3}},
			},
			{
				Query:    "/* client a */ insert into t values (2, 2)",
				Expected: []sql.Row{{sql.NewOkResult(1)}},
			},
			{
				Query:    "/* client b */ select * from t order by x",
				Expected: []sql.Row{{1, 1}, {3, 3}},
			},
			{
				Query:    "/* client a */ commit",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ select * from t order by x",
				Expected: []sql.Row{{1, 1}, {3, 3}},
			},
			{
				Query:    "/* client b */ rollback",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ select * from t order by x",
				Expected: []sql.Row{{1, 1}, {2, 2}, {3, 3}},
			},
		},
	},
	{
		Name: "rollback to savepoint",
		SetUpScript: []string{
			"create table t (x int primary key, y int)",
			"insert into t values (1, 1)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "/* client a */ set autocommit = off",
				Expected: []sql.Row{{}},
			},
			{
				Query:    "/* client b */ set autocommit = off",
				Expected: []sql.Row{{}},
			},
			{
				Query:    "/* client a */ start transaction",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ start transaction",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ insert into t values (2, 2)",
				Expected: []sql.Row{{sql.NewOkResult(1)}},
			},
			{
				Query:    "/* client b */ insert into t values (3, 3)",
				Expected: []sql.Row{{sql.NewOkResult(1)}},
			},
			{
				Query:    "/* client a */ savepoint spa1",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ savepoint spb1",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ insert into t values (4, 4)",
				Expected: []sql.Row{{sql.NewOkResult(1)}},
			},
			{
				Query:    "/* client b */ insert into t values (5, 5)",
				Expected: []sql.Row{{sql.NewOkResult(1)}},
			},
			{
				Query:    "/* client a */ savepoint spa2",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ savepoint spb2",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ insert into t values (6, 6)",
				Expected: []sql.Row{{sql.NewOkResult(1)}},
			},
			{
				Query:    "/* client b */ insert into t values (7, 7)",
				Expected: []sql.Row{{sql.NewOkResult(1)}},
			},
			{
				Query:    "/* client a */ select * from t order by x",
				Expected: []sql.Row{{1, 1}, {2, 2}, {4, 4}, {6, 6}},
			},
			{
				Query:    "/* client b */ select * from t order by x",
				Expected: []sql.Row{{1, 1}, {3, 3}, {5, 5}, {7, 7}},
			},
			{
				Query:    "/* client a */ rollback to SPA2",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ rollback to spB2",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ select * from t order by x",
				Expected: []sql.Row{{1, 1}, {2, 2}, {4, 4}},
			},
			{
				Query:    "/* client b */ select * from t order by x",
				Expected: []sql.Row{{1, 1}, {3, 3}, {5, 5}},
			},
			{
				Query:    "/* client a */ rollback to sPa2",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ rollback to Spb2",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ select * from t order by x",
				Expected: []sql.Row{{1, 1}, {2, 2}, {4, 4}},
			},
			{
				Query:    "/* client b */ select * from t order by x",
				Expected: []sql.Row{{1, 1}, {3, 3}, {5, 5}},
			},
			{
				Query:    "/* client a */ rollback to spA1",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ rollback to SPb1",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ select * from t order by x",
				Expected: []sql.Row{{1, 1}, {2, 2}},
			},
			{
				Query:    "/* client b */ select * from t order by x",
				Expected: []sql.Row{{1, 1}, {3, 3}},
			},
			{
				Query:       "/* client a */ rollback to spa2",
				ExpectedErr: sql.ErrSavepointDoesNotExist,
			},
			{
				Query:       "/* client b */ rollback to spb2",
				ExpectedErr: sql.ErrSavepointDoesNotExist,
			},
			{
				Query:    "/* client a */ rollback to Spa1",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ rollback to spB1",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ select * from t order by x",
				Expected: []sql.Row{{1, 1}, {2, 2}},
			},
			{
				Query:    "/* client b */ select * from t order by x",
				Expected: []sql.Row{{1, 1}, {3, 3}},
			},
			{
				Query:    "/* client a */ rollback",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ commit",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ select * from t order by x",
				Expected: []sql.Row{{1, 1}, {3, 3}},
			},
			{
				Query:    "/* client b */ select * from t order by x",
				Expected: []sql.Row{{1, 1}, {3, 3}},
			},
			{
				Query:       "/* client a */ rollback to spa1",
				ExpectedErr: sql.ErrSavepointDoesNotExist,
			},
			{
				Query:       "/* client b */ rollback to spb1",
				ExpectedErr: sql.ErrSavepointDoesNotExist,
			},
		},
	},
	{
		Name: "release savepoint",
		SetUpScript: []string{
			"create table t (x int primary key, y int)",
			"insert into t values (1, 1)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "/* client a */ set autocommit = off",
				Expected: []sql.Row{{}},
			},
			{
				Query:    "/* client b */ set autocommit = off",
				Expected: []sql.Row{{}},
			},
			{
				Query:    "/* client a */ start transaction",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ start transaction",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ insert into t values (2, 2)",
				Expected: []sql.Row{{sql.NewOkResult(1)}},
			},
			{
				Query:    "/* client b */ insert into t values (3, 3)",
				Expected: []sql.Row{{sql.NewOkResult(1)}},
			},
			{
				Query:    "/* client a */ savepoint spa1",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ savepoint spb1",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ release savepoint Spa1",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ release savepoint sPb1",
				Expected: []sql.Row{},
			},
			{
				Query:       "/* client a */ rollback to spa1",
				ExpectedErr: sql.ErrSavepointDoesNotExist,
			},
			{
				Query:       "/* client b */ rollback to spb1",
				ExpectedErr: sql.ErrSavepointDoesNotExist,
			},
			{
				Query:    "/* client a */ select * from t order by x",
				Expected: []sql.Row{{1, 1}, {2, 2}},
			},
			{
				Query:    "/* client b */ select * from t order by x",
				Expected: []sql.Row{{1, 1}, {3, 3}},
			},
		},
	},
	{
		Name: "overwrite savepoint",
		SetUpScript: []string{
			"create table t (x int primary key, y int)",
			"insert into t values (1, 1)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "/* client a */ start transaction",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ insert into t values (2, 2)",
				Expected: []sql.Row{{sql.NewOkResult(1)}},
			},
			{
				Query:    "/* client a */ savepoint spa1",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ insert into t values (3, 3)",
				Expected: []sql.Row{{sql.NewOkResult(1)}},
			},
			{
				Query:    "/* client a */ savepoint spa2",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ insert into t values (4, 4)",
				Expected: []sql.Row{{sql.NewOkResult(1)}},
			},
			{
				Query:    "/* client a */ savepoint SPA1",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ insert into t values (5, 5)",
				Expected: []sql.Row{{sql.NewOkResult(1)}},
			},
			{
				Query:    "/* client a */ select * from t order by x",
				Expected: []sql.Row{{1, 1}, {2, 2}, {3, 3}, {4, 4}, {5, 5}},
			},
			{
				Query:    "/* client a */ rollback to Spa1",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ select * from t order by x",
				Expected: []sql.Row{{1, 1}, {2, 2}, {3, 3}, {4, 4}},
			},
			{
				Query:    "/* client a */ rollback to spa2",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ select * from t order by x",
				Expected: []sql.Row{{1, 1}, {2, 2}, {3, 3}},
			},
			{
				Query:       "/* client a */ rollback to spa1",
				ExpectedErr: sql.ErrSavepointDoesNotExist,
			},
			{
				Query:       "/* client a */ release savepoint spa1",
				ExpectedErr: sql.ErrSavepointDoesNotExist,
			},
		},
	},
	{
		Name: "Test AUTO INCREMENT with no autocommit",
		SetUpScript: []string{
			"CREATE table t (x int PRIMARY KEY AUTO_INCREMENT, y int);",
			"CREATE table t2 (x int PRIMARY KEY AUTO_INCREMENT, y int);",
			"CREATE table t3 (x int PRIMARY KEY AUTO_INCREMENT, y int);",
			"insert into t (y) values (1);",
			"insert into t2 values (10, 10);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "/* client a */ set autocommit = off",
				Expected: []sql.Row{{}},
			},
			{
				Query:    "/* client b */ set autocommit = off",
				Expected: []sql.Row{{}},
			},
			{
				Query:    "/* client c */ set autocommit = off",
				Expected: []sql.Row{{}},
			},
			// Client a starts by insert into t
			{
				Query:    "/* client a */ insert into t (y) values (2)",
				Expected: []sql.Row{{sql.NewOkResult(1)}},
			},
			{
				Query:    "/* client b */ select * from t order by x",
				Expected: []sql.Row{{1, 1}},
			},
			{
				Query:    "/* client c*/ select * from t order by x",
				Expected: []sql.Row{{1, 1}},
			},
			// Client b inserts into t
			{
				Query:    "/* client b */ insert into t (y) values (3)",
				Expected: []sql.Row{{sql.NewOkResult(1)}},
			},
			{
				Query: "/* client a */ select * from t order by x",
				Expected: []sql.Row{
					{1, 1}, {2, 2},
				},
			},
			{
				Query: "/* client c */ select * from t order by x",
				Expected: []sql.Row{
					{1, 1},
				},
			},
			// Client c inserts into t2
			{
				Query:    "/* client c */ insert into t2 (y) values (11)",
				Expected: []sql.Row{{sql.NewOkResult(1)}},
			},
			{
				Query:    "/* client a */ select * from t2 order by x",
				Expected: []sql.Row{{10, 10}},
			},
			{
				Query:    "/* client b */ select * from t2 order by x",
				Expected: []sql.Row{{10, 10}},
			},
			{
				Query:    "/* client c */ select * from t2 order by x",
				Expected: []sql.Row{{10, 10}, {11, 11}},
			},
			// Client a inserts into t2
			{
				Query:    "/* client a */ insert into t2 (y) values (12)",
				Expected: []sql.Row{{sql.NewOkResult(1)}},
			},
			{
				Query:    "/* client a */ select * from t2 order by x",
				Expected: []sql.Row{{10, 10}, {12, 12}},
			},
			{
				Query:    "/* client b */ select * from t2 order by x",
				Expected: []sql.Row{{10, 10}},
			},
			{
				Query:    "/* client c */ select * from t2 order by x",
				Expected: []sql.Row{{10, 10}, {11, 11}},
			},
			// Client a commits
			{
				Query:    "/* client a */ commit",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ select * from t order by x",
				Expected: []sql.Row{{1, 1}, {3, 3}},
			},
			{
				Query:    "/* client b */ select * from t2 order by x",
				Expected: []sql.Row{{10, 10}},
			},
			{
				Query:    "/* client c */ select * from t2 order by x",
				Expected: []sql.Row{{10, 10}, {11, 11}},
			},
			{
				Query: "/* client c */ select * from t order by x",
				Expected: []sql.Row{
					{1, 1},
				},
			},
			// Client b commits
			{
				Query:    "/* client b */ commit",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ select * from t order by x",
				Expected: []sql.Row{{1, 1}, {2, 2}, {3, 3}},
			},
			{
				Query:    "/* client a */ select * from t2 order by x",
				Expected: []sql.Row{{10, 10}, {12, 12}},
			},
			{
				Query:    "/* client c */ select * from t2 order by x",
				Expected: []sql.Row{{10, 10}, {11, 11}},
			},
			{
				Query:    "/* client c */ select * from t order by x",
				Expected: []sql.Row{{1, 1}},
			},
			// Client c commits
			{
				Query:    "/* client c */ commit",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ select * from t order by x",
				Expected: []sql.Row{{1, 1}, {2, 2}, {3, 3}},
			},
			{
				Query:    "/* client a */ select * from t2 order by x",
				Expected: []sql.Row{{10, 10}, {12, 12}},
			},
			{
				Query:    "/* client b */ select * from t order by x",
				Expected: []sql.Row{{1, 1}, {2, 2}, {3, 3}},
			},
			{
				Query:    "/* client b */ select * from t2 order by x",
				Expected: []sql.Row{{10, 10}, {11, 11}, {12, 12}},
			},
			// Client a starts transactions
			{
				Query:    "/* client a */ start transaction",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ select * from t order by x",
				Expected: []sql.Row{{1, 1}, {2, 2}, {3, 3}},
			},
			{
				Query:    "/* client b */ select * from t order by x",
				Expected: []sql.Row{{1, 1}, {2, 2}, {3, 3}},
			},
			{
				Query:    "/* client c */ select * from t order by x",
				Expected: []sql.Row{{1, 1}, {2, 2}, {3, 3}},
			},
			{
				Query:    "/* client a */ select * from t2 order by x",
				Expected: []sql.Row{{10, 10}, {11, 11}, {12, 12}},
			},
			{
				Query:    "/* client b */ select * from t2 order by x",
				Expected: []sql.Row{{10, 10}, {11, 11}, {12, 12}},
			},
			{
				Query:    "/* client c */ select * from t2 order by x",
				Expected: []sql.Row{{10, 10}, {11, 11}, {12, 12}},
			},
			// Client a does a skip ahead
			{
				Query:    "/* client a */ insert into t values (10, 10)",
				Expected: []sql.Row{{sql.NewOkResult(1)}},
			},
			{
				Query:    "/* client b */ insert into t (y) values (11)",
				Expected: []sql.Row{{sql.NewOkResult(1)}},
			},
			// Client c skips ahead
			{
				Query:    "/* client c */ insert into t values (50, 50)",
				Expected: []sql.Row{{sql.NewOkResult(1)}},
			},
			{
				Query:    "/* client b */ insert into t (y) values (51)",
				Expected: []sql.Row{{sql.NewOkResult(1)}},
			},
			{
				Query:    "/* client a */ select * from t order by x",
				Expected: []sql.Row{{1, 1}, {2, 2}, {3, 3}, {10, 10}},
			},
			{
				Query:    "/* client b */ select * from t order by x",
				Expected: []sql.Row{{1, 1}, {2, 2}, {3, 3}, {11, 11}, {51, 51}},
			},
			{
				Query:    "/* client c */ select * from t order by x",
				Expected: []sql.Row{{1, 1}, {2, 2}, {3, 3}, {50, 50}},
			},
			{
				Query:    "/* client a */ commit",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ commit",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client c */ commit",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ select * from t order by x",
				Expected: []sql.Row{{1, 1}, {2, 2}, {3, 3}, {10, 10}, {11, 11}, {50, 50}, {51, 51}},
			},
			{
				Query:    "/* client b */ select * from t order by x",
				Expected: []sql.Row{{1, 1}, {2, 2}, {3, 3}, {10, 10}, {11, 11}, {50, 50}, {51, 51}},
			},
			{
				Query:    "/* client c */ select * from t order by x",
				Expected: []sql.Row{{1, 1}, {2, 2}, {3, 3}, {10, 10}, {11, 11}, {50, 50}, {51, 51}},
			},
			// Client a does a simple insert to ensure merging worked
			{
				Query:    "/* client a */ insert into t values (NULL, 52)",
				Expected: []sql.Row{{sql.NewOkResult(1)}},
			},
			{
				Query:    "/* client a */ select * from t order by x",
				Expected: []sql.Row{{1, 1}, {2, 2}, {3, 3}, {10, 10}, {11, 11}, {50, 50}, {51, 51}, {52, 52}},
			},
		},
	},
	{
		Name: "AUTO_INCREMENT transactions off",
		SetUpScript: []string{
			"CREATE table t2 (x int PRIMARY KEY AUTO_INCREMENT, y int);",
			"insert into t2 (y) values (1);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "/* client a */ insert into t2 (y) values (2)",
				Expected: []sql.Row{{sql.NewOkResult(1)}},
			},
			{
				Query:    "/* client b */ select * from t2 order by x",
				Expected: []sql.Row{{1, 1}, {2, 2}},
			},
			{
				Query:    "/* client b */ insert into t2 (y) values (3)",
				Expected: []sql.Row{{sql.NewOkResult(1)}},
			},
			{
				Query:    "/* client a */ select * from t2 order by x",
				Expected: []sql.Row{{1, 1}, {2, 2}, {3, 3}},
			},
		},
	},
}
