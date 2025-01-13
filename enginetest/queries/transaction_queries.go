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

package queries

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

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
		Name: "Changes from transactions are available before analyzing statements in other sessions (autocommit off)",
		Assertions: []ScriptTestAssertion{
			{
				Query:    "/* client a */ set @@autocommit = 0;",
				Expected: []sql.UntypedSqlRow{{}},
			},
			{
				Query:    "/* client b */ set @@autocommit = 0;",
				Expected: []sql.UntypedSqlRow{{}},
			},
			{
				Query:    "/* client a */ select @@autocommit;",
				Expected: []sql.UntypedSqlRow{{0}},
			},
			{
				Query:    "/* client b */ select @@autocommit;",
				Expected: []sql.UntypedSqlRow{{0}},
			},
			{
				Query:    "/* client a */ start transaction;",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:       "/* client a */ select * from t;",
				ExpectedErr: sql.ErrTableNotFound,
			},
			{
				Query:    "/* client a */ create table t(pk int primary key);",
				Expected: []sql.UntypedSqlRow{{types.OkResult{}}},
			},
			{
				// Trigger a query error to make sure explicit transaction is still
				// correctly configured in the session despite the error
				Query:       "/* client a */ select * from t2;",
				ExpectedErr: sql.ErrTableNotFound,
			},
			{
				Query:    "/* client a */ commit;",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "/* client b */ start transaction;",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "/* client b */ select count(*) from t;",
				Expected: []sql.UntypedSqlRow{{0}},
			},
		},
	},
	{
		Name: "autocommit on",
		SetUpScript: []string{
			"create table t (x int primary key, y int)",
			"insert into t values (1, 1)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "/* client a */ insert into t values (2, 2)",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
			},
			{
				Query:    "/* client b */ select * from t order by x",
				Expected: []sql.UntypedSqlRow{{1, 1}, {2, 2}},
			},
			{
				Query:    "/* client b */ insert into t values (3, 3)",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
			},
			{
				Query:    "/* client a */ select * from t order by x",
				Expected: []sql.UntypedSqlRow{{1, 1}, {2, 2}, {3, 3}},
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
				Expected: []sql.UntypedSqlRow{{}},
			},
			{
				Query:    "/* client b */ set autocommit = off",
				Expected: []sql.UntypedSqlRow{{}},
			},
			{
				Query:    "/* client b */ select * from t order by x",
				Expected: []sql.UntypedSqlRow{{1, 1}},
			},
			{
				Query:    "/* client b */ insert into t values (2, 2)",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
			},
			{
				Query: "/* client a */ select * from t order by x",
				Expected: []sql.UntypedSqlRow{
					{1, 1},
				},
			},
			{
				Query:    "/* client a */ insert into t values (3,3)",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
			},
			{
				Query:    "/* client b */ select * from t order by x",
				Expected: []sql.UntypedSqlRow{{1, 1}, {2, 2}},
			},
			{
				Query:    "/* client b */ commit",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "/* client a */ select * from t order by x",
				Expected: []sql.UntypedSqlRow{{1, 1}, {3, 3}},
			},
			{
				Query:    "/* client b */ select * from t order by x",
				Expected: []sql.UntypedSqlRow{{1, 1}, {2, 2}},
			},
			{
				Query:    "/* client a */ commit",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "/* client b */ select * from t order by x",
				Expected: []sql.UntypedSqlRow{{1, 1}, {2, 2}},
			},
			{
				Query:    "/* client b */ start transaction",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "/* client b */ select * from t order by x",
				Expected: []sql.UntypedSqlRow{{1, 1}, {2, 2}, {3, 3}},
			},
			{
				Query:    "/* client a */ select * from t order by x",
				Expected: []sql.UntypedSqlRow{{1, 1}, {2, 2}, {3, 3}},
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
				Expected: []sql.UntypedSqlRow{{}},
			},
			{
				Query:    "/* client b */ set autocommit = off",
				Expected: []sql.UntypedSqlRow{{}},
			},
			{
				Query:    "/* client b */ insert into t values (2,2)",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
			},
			{
				Query:    "/* client a */ select * from t order by x",
				Expected: []sql.UntypedSqlRow{{1, 1}},
			},
			// should commit any pending transaction
			{
				Query:    "/* client b */ set autocommit = on",
				Expected: []sql.UntypedSqlRow{{}},
			},
			{
				Query:    "/* client a */ select * from t order by x",
				Expected: []sql.UntypedSqlRow{{1, 1}},
			},
			// client a sees the committed transaction from client b when it begins a new transaction
			{
				Query:    "/* client a */ set autocommit = on",
				Expected: []sql.UntypedSqlRow{{}},
			},
			{
				Query:    "/* client a */ select * from t order by x",
				Expected: []sql.UntypedSqlRow{{1, 1}, {2, 2}},
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
				Expected: []sql.UntypedSqlRow{},
			},
			{
				// Trigger an analyzer error to make sure transaction state is managed correctly
				Query:       "/* client a */ select * from doesnotexist;",
				ExpectedErr: sql.ErrTableNotFound,
			},
			{
				Query:    "/* client a */ insert into t values (2, 2)",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
			},
			{
				Query:    "/* client b */ select * from t order by x",
				Expected: []sql.UntypedSqlRow{{1, 1}},
			},
			{
				// Trigger an analyzer error to make sure state for the explicitly started
				// transaction is managed correctly and not cleared
				Query:       "/* client a */ select * from doesnotexist;",
				ExpectedErr: sql.ErrTableNotFound,
			},
			{
				Query:    "/* client a */ commit",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "/* client b */ select * from t order by x",
				Expected: []sql.UntypedSqlRow{{1, 1}, {2, 2}},
			},
			// After commit, autocommit turns back on
			{
				Query:    "/* client a */ insert into t values (3, 3)",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
			},
			{
				Query:    "/* client b */ select * from t order by x",
				Expected: []sql.UntypedSqlRow{{1, 1}, {2, 2}, {3, 3}},
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
				Expected: []sql.UntypedSqlRow{{}},
			},
			{
				Query:    "/* client b */ set autocommit = off",
				Expected: []sql.UntypedSqlRow{{}},
			},
			{
				Query:    "/* client a */ start transaction",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "/* client b */ start transaction",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "/* client a */ insert into t values (2, 2)",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
			},
			{
				Query:    "/* client b */ insert into t values (3, 3)",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
			},
			{
				Query:    "/* client a */ select * from t order by x",
				Expected: []sql.UntypedSqlRow{{1, 1}, {2, 2}},
			},
			{
				Query:    "/* client b */ commit",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "/* client a */ select * from t order by x",
				Expected: []sql.UntypedSqlRow{{1, 1}, {2, 2}},
			},
			{
				Query:    "/* client a */ rollback",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "/* client a */ select * from t order by x",
				Expected: []sql.UntypedSqlRow{{1, 1}, {3, 3}},
			},
			{
				Query:    "/* client a */ insert into t values (2, 2)",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
			},
			{
				Query:    "/* client b */ select * from t order by x",
				Expected: []sql.UntypedSqlRow{{1, 1}, {3, 3}},
			},
			{
				Query:    "/* client a */ commit",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "/* client b */ select * from t order by x",
				Expected: []sql.UntypedSqlRow{{1, 1}, {3, 3}},
			},
			{
				Query:    "/* client b */ rollback",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "/* client b */ select * from t order by x",
				Expected: []sql.UntypedSqlRow{{1, 1}, {2, 2}, {3, 3}},
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
				Expected: []sql.UntypedSqlRow{{}},
			},
			{
				Query:    "/* client b */ set autocommit = off",
				Expected: []sql.UntypedSqlRow{{}},
			},
			{
				Query:    "/* client a */ start transaction",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "/* client b */ start transaction",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "/* client a */ insert into t values (2, 2)",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
			},
			{
				Query:    "/* client b */ insert into t values (3, 3)",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
			},
			{
				Query:    "/* client a */ savepoint spa1",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "/* client b */ savepoint spb1",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "/* client a */ insert into t values (4, 4)",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
			},
			{
				Query:    "/* client b */ insert into t values (5, 5)",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
			},
			{
				Query:    "/* client a */ savepoint spa2",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "/* client b */ savepoint spb2",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "/* client a */ insert into t values (6, 6)",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
			},
			{
				Query:    "/* client b */ insert into t values (7, 7)",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
			},
			{
				Query:    "/* client a */ select * from t order by x",
				Expected: []sql.UntypedSqlRow{{1, 1}, {2, 2}, {4, 4}, {6, 6}},
			},
			{
				Query:    "/* client b */ select * from t order by x",
				Expected: []sql.UntypedSqlRow{{1, 1}, {3, 3}, {5, 5}, {7, 7}},
			},
			{
				Query:    "/* client a */ rollback to SPA2",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "/* client b */ rollback to spB2",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "/* client a */ select * from t order by x",
				Expected: []sql.UntypedSqlRow{{1, 1}, {2, 2}, {4, 4}},
			},
			{
				Query:    "/* client b */ select * from t order by x",
				Expected: []sql.UntypedSqlRow{{1, 1}, {3, 3}, {5, 5}},
			},
			{
				Query:    "/* client a */ rollback to sPa2",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "/* client b */ rollback to Spb2",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "/* client a */ select * from t order by x",
				Expected: []sql.UntypedSqlRow{{1, 1}, {2, 2}, {4, 4}},
			},
			{
				Query:    "/* client b */ select * from t order by x",
				Expected: []sql.UntypedSqlRow{{1, 1}, {3, 3}, {5, 5}},
			},
			{
				Query:    "/* client a */ rollback to spA1",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "/* client b */ rollback to SPb1",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "/* client a */ select * from t order by x",
				Expected: []sql.UntypedSqlRow{{1, 1}, {2, 2}},
			},
			{
				Query:    "/* client b */ select * from t order by x",
				Expected: []sql.UntypedSqlRow{{1, 1}, {3, 3}},
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
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "/* client b */ rollback to spB1",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "/* client a */ select * from t order by x",
				Expected: []sql.UntypedSqlRow{{1, 1}, {2, 2}},
			},
			{
				Query:    "/* client b */ select * from t order by x",
				Expected: []sql.UntypedSqlRow{{1, 1}, {3, 3}},
			},
			{
				Query:    "/* client a */ rollback",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "/* client b */ commit",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "/* client a */ select * from t order by x",
				Expected: []sql.UntypedSqlRow{{1, 1}, {3, 3}},
			},
			{
				Query:    "/* client b */ select * from t order by x",
				Expected: []sql.UntypedSqlRow{{1, 1}, {3, 3}},
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
				Expected: []sql.UntypedSqlRow{{}},
			},
			{
				Query:    "/* client b */ set autocommit = off",
				Expected: []sql.UntypedSqlRow{{}},
			},
			{
				Query:    "/* client a */ start transaction",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "/* client b */ start transaction",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "/* client a */ insert into t values (2, 2)",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
			},
			{
				Query:    "/* client b */ insert into t values (3, 3)",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
			},
			{
				Query:    "/* client a */ savepoint spa1",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "/* client b */ savepoint spb1",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "/* client a */ release savepoint Spa1",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "/* client b */ release savepoint sPb1",
				Expected: []sql.UntypedSqlRow{},
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
				Expected: []sql.UntypedSqlRow{{1, 1}, {2, 2}},
			},
			{
				Query:    "/* client b */ select * from t order by x",
				Expected: []sql.UntypedSqlRow{{1, 1}, {3, 3}},
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
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "/* client a */ insert into t values (2, 2)",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
			},
			{
				Query:    "/* client a */ savepoint spa1",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "/* client a */ insert into t values (3, 3)",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
			},
			{
				Query:    "/* client a */ savepoint spa2",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "/* client a */ insert into t values (4, 4)",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
			},
			{
				Query:    "/* client a */ savepoint SPA1",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "/* client a */ insert into t values (5, 5)",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
			},
			{
				Query:    "/* client a */ select * from t order by x",
				Expected: []sql.UntypedSqlRow{{1, 1}, {2, 2}, {3, 3}, {4, 4}, {5, 5}},
			},
			{
				Query:    "/* client a */ rollback to Spa1",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "/* client a */ select * from t order by x",
				Expected: []sql.UntypedSqlRow{{1, 1}, {2, 2}, {3, 3}, {4, 4}},
			},
			{
				Query:    "/* client a */ rollback to spa2",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "/* client a */ select * from t order by x",
				Expected: []sql.UntypedSqlRow{{1, 1}, {2, 2}, {3, 3}},
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
				Expected: []sql.UntypedSqlRow{{}},
			},
			{
				Query:    "/* client b */ set autocommit = off",
				Expected: []sql.UntypedSqlRow{{}},
			},
			{
				Query:    "/* client c */ set autocommit = off",
				Expected: []sql.UntypedSqlRow{{}},
			},
			// Client a starts by insert into t
			{
				Query:    "/* client a */ insert into t (y) values (2)",
				Expected: []sql.UntypedSqlRow{{types.OkResult{RowsAffected: 1, InsertID: 2}}},
			},
			{
				Query:    "/* client b */ select * from t order by x",
				Expected: []sql.UntypedSqlRow{{1, 1}},
			},
			{
				Query:    "/* client c*/ select * from t order by x",
				Expected: []sql.UntypedSqlRow{{1, 1}},
			},
			// Client b inserts into t
			{
				Query:    "/* client b */ insert into t (y) values (3)",
				Expected: []sql.UntypedSqlRow{{types.OkResult{RowsAffected: 1, InsertID: 3}}},
			},
			{
				Query: "/* client a */ select * from t order by x",
				Expected: []sql.UntypedSqlRow{
					{1, 1}, {2, 2},
				},
			},
			{
				Query: "/* client c */ select * from t order by x",
				Expected: []sql.UntypedSqlRow{
					{1, 1},
				},
			},
			// Client c inserts into t2
			{
				Query:    "/* client c */ insert into t2 (y) values (11)",
				Expected: []sql.UntypedSqlRow{{types.OkResult{RowsAffected: 1, InsertID: 11}}},
			},
			{
				Query:    "/* client a */ select * from t2 order by x",
				Expected: []sql.UntypedSqlRow{{10, 10}},
			},
			{
				Query:    "/* client b */ select * from t2 order by x",
				Expected: []sql.UntypedSqlRow{{10, 10}},
			},
			{
				Query:    "/* client c */ select * from t2 order by x",
				Expected: []sql.UntypedSqlRow{{10, 10}, {11, 11}},
			},
			// Client a inserts into t2
			{
				Query:    "/* client a */ insert into t2 (y) values (12)",
				Expected: []sql.UntypedSqlRow{{types.OkResult{RowsAffected: 1, InsertID: 12}}},
			},
			{
				Query:    "/* client a */ select * from t2 order by x",
				Expected: []sql.UntypedSqlRow{{10, 10}, {12, 12}},
			},
			{
				Query:    "/* client b */ select * from t2 order by x",
				Expected: []sql.UntypedSqlRow{{10, 10}},
			},
			{
				Query:    "/* client c */ select * from t2 order by x",
				Expected: []sql.UntypedSqlRow{{10, 10}, {11, 11}},
			},
			// Client a commits
			{
				Query:    "/* client a */ commit",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "/* client b */ select * from t order by x",
				Expected: []sql.UntypedSqlRow{{1, 1}, {3, 3}},
			},
			{
				Query:    "/* client b */ select * from t2 order by x",
				Expected: []sql.UntypedSqlRow{{10, 10}},
			},
			{
				Query:    "/* client c */ select * from t2 order by x",
				Expected: []sql.UntypedSqlRow{{10, 10}, {11, 11}},
			},
			{
				Query: "/* client c */ select * from t order by x",
				Expected: []sql.UntypedSqlRow{
					{1, 1},
				},
			},
			// Client b commits
			{
				Query:    "/* client b */ commit",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "/* client a */ select * from t order by x",
				Expected: []sql.UntypedSqlRow{{1, 1}, {2, 2}, {3, 3}},
			},
			{
				Query:    "/* client a */ select * from t2 order by x",
				Expected: []sql.UntypedSqlRow{{10, 10}, {12, 12}},
			},
			{
				Query:    "/* client c */ select * from t2 order by x",
				Expected: []sql.UntypedSqlRow{{10, 10}, {11, 11}},
			},
			{
				Query:    "/* client c */ select * from t order by x",
				Expected: []sql.UntypedSqlRow{{1, 1}},
			},
			// Client c commits
			{
				Query:    "/* client c */ commit",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "/* client a */ select * from t order by x",
				Expected: []sql.UntypedSqlRow{{1, 1}, {2, 2}, {3, 3}},
			},
			{
				Query:    "/* client a */ select * from t2 order by x",
				Expected: []sql.UntypedSqlRow{{10, 10}, {12, 12}},
			},
			{
				Query:    "/* client b */ select * from t order by x",
				Expected: []sql.UntypedSqlRow{{1, 1}, {2, 2}, {3, 3}},
			},
			{
				Query:    "/* client b */ select * from t2 order by x",
				Expected: []sql.UntypedSqlRow{{10, 10}, {11, 11}, {12, 12}},
			},
			// Client a starts transactions
			{
				Query:    "/* client a */ start transaction",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "/* client a */ select * from t order by x",
				Expected: []sql.UntypedSqlRow{{1, 1}, {2, 2}, {3, 3}},
			},
			{
				Query:    "/* client b */ select * from t order by x",
				Expected: []sql.UntypedSqlRow{{1, 1}, {2, 2}, {3, 3}},
			},
			{
				Query:    "/* client c */ select * from t order by x",
				Expected: []sql.UntypedSqlRow{{1, 1}, {2, 2}, {3, 3}},
			},
			{
				Query:    "/* client a */ select * from t2 order by x",
				Expected: []sql.UntypedSqlRow{{10, 10}, {11, 11}, {12, 12}},
			},
			{
				Query:    "/* client b */ select * from t2 order by x",
				Expected: []sql.UntypedSqlRow{{10, 10}, {11, 11}, {12, 12}},
			},
			{
				Query:    "/* client c */ select * from t2 order by x",
				Expected: []sql.UntypedSqlRow{{10, 10}, {11, 11}, {12, 12}},
			},
			// Client a does a skip ahead
			{
				Query:    "/* client a */ insert into t values (10, 10)",
				Expected: []sql.UntypedSqlRow{{types.OkResult{RowsAffected: 1, InsertID: 12}}},
			},
			{
				Query:    "/* client b */ insert into t (y) values (11)",
				Expected: []sql.UntypedSqlRow{{types.OkResult{RowsAffected: 1, InsertID: 11}}},
			},
			// Client c skips ahead
			{
				Query:    "/* client c */ insert into t values (50, 50)",
				Expected: []sql.UntypedSqlRow{{types.OkResult{RowsAffected: 1, InsertID: 11}}},
			},
			{
				Query:    "/* client b */ insert into t (y) values (51)",
				Expected: []sql.UntypedSqlRow{{types.OkResult{RowsAffected: 1, InsertID: 51}}},
			},
			{
				Query:    "/* client a */ select * from t order by x",
				Expected: []sql.UntypedSqlRow{{1, 1}, {2, 2}, {3, 3}, {10, 10}},
			},
			{
				Query:    "/* client b */ select * from t order by x",
				Expected: []sql.UntypedSqlRow{{1, 1}, {2, 2}, {3, 3}, {11, 11}, {51, 51}},
			},
			{
				Query:    "/* client c */ select * from t order by x",
				Expected: []sql.UntypedSqlRow{{1, 1}, {2, 2}, {3, 3}, {50, 50}},
			},
			{
				Query:    "/* client a */ commit",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "/* client b */ commit",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "/* client c */ commit",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "/* client a */ select * from t order by x",
				Expected: []sql.UntypedSqlRow{{1, 1}, {2, 2}, {3, 3}, {10, 10}, {11, 11}, {50, 50}, {51, 51}},
			},
			{
				Query:    "/* client b */ select * from t order by x",
				Expected: []sql.UntypedSqlRow{{1, 1}, {2, 2}, {3, 3}, {10, 10}, {11, 11}, {50, 50}, {51, 51}},
			},
			{
				Query:    "/* client c */ select * from t order by x",
				Expected: []sql.UntypedSqlRow{{1, 1}, {2, 2}, {3, 3}, {10, 10}, {11, 11}, {50, 50}, {51, 51}},
			},
			// Client a does a simple insert to ensure merging worked
			{
				Query:    "/* client a */ insert into t values (NULL, 52)",
				Expected: []sql.UntypedSqlRow{{types.OkResult{RowsAffected: 1, InsertID: 52}}},
			},
			{
				Query:    "/* client a */ select * from t order by x",
				Expected: []sql.UntypedSqlRow{{1, 1}, {2, 2}, {3, 3}, {10, 10}, {11, 11}, {50, 50}, {51, 51}, {52, 52}},
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
				Expected: []sql.UntypedSqlRow{{types.OkResult{RowsAffected: 1, InsertID: 2}}},
			},
			{
				Query:    "/* client b */ select * from t2 order by x",
				Expected: []sql.UntypedSqlRow{{1, 1}, {2, 2}},
			},
			{
				Query:    "/* client b */ insert into t2 (y) values (3)",
				Expected: []sql.UntypedSqlRow{{types.OkResult{RowsAffected: 1, InsertID: 3}}},
			},
			{
				Query:    "/* client a */ select * from t2 order by x",
				Expected: []sql.UntypedSqlRow{{1, 1}, {2, 2}, {3, 3}},
			},
			{
				Query:    "/* client a */ alter table t2 modify column x int",
				Expected: []sql.UntypedSqlRow{{types.OkResult{RowsAffected: 0, InsertID: 0}}},
			},
			{
				Query:       "/* client a */ INSERT INTO t2 values (NULL, 3)",
				ExpectedErr: sql.ErrInsertIntoNonNullableProvidedNull,
			},
			{
				Query:    "/* client a */ DROP TABLE t2",
				Expected: []sql.UntypedSqlRow{{types.OkResult{RowsAffected: 0, InsertID: 0}}},
			},
			{
				Query:    "/* client a */ CREATE table t2 (x int PRIMARY KEY AUTO_INCREMENT, y int)",
				Expected: []sql.UntypedSqlRow{{types.OkResult{RowsAffected: 0, InsertID: 0}}},
			},
			{
				Query:    "/* client a */ insert into t2 (y) values (4)",
				Expected: []sql.UntypedSqlRow{{types.OkResult{RowsAffected: 1, InsertID: 1}}},
			},
			{
				Query:    "/* client a */ SELECT * FROM t2",
				Expected: []sql.UntypedSqlRow{{1, 4}},
			},
		},
	},
	{
		Name: "READ ONLY Transactions",
		SetUpScript: []string{
			"create table t2 (pk int primary key, val int)",
			"insert into t2 values (0,0)",
			"commit",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "/* client a */ set autocommit = off",
				Expected: []sql.UntypedSqlRow{{}},
			},
			{
				Query:    "/* client a */ create temporary table tmp(pk int primary key)",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query:    "/* client a */  START TRANSACTION READ ONLY",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "/* client a */ INSERT INTO tmp VALUES (1)",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
			},
			{
				Query:       "/* client a */ insert into t2 values (1, 1)",
				ExpectedErr: sql.ErrReadOnlyTransaction,
			},
			{
				Query:       "/* client a */ insert into t2 values (2, 2)",
				ExpectedErr: sql.ErrReadOnlyTransaction,
			},
			{
				Query:       "/* client a */ delete from t2 where pk = 0",
				ExpectedErr: sql.ErrReadOnlyTransaction,
			},
			{

				Query:    "/* client a */ alter table t2 add val2 int",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query:    "/* client a */ select * from t2",
				Expected: []sql.UntypedSqlRow{{0, 0, nil}},
			},
			{
				Query:       "/* client a */ create temporary table tmp2(pk int primary key)",
				ExpectedErr: sql.ErrReadOnlyTransaction,
			},
			{
				Query:    "/* client a */ COMMIT",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "/* client b */ START TRANSACTION READ ONLY",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "/* client b */ SELECT * FROM t2",
				Expected: []sql.UntypedSqlRow{{0, 0, nil}},
			},
		},
	},
	{
		Name: "Insert error with auto commit off",
		SetUpScript: []string{
			"create table t1 (pk int primary key, val int)",
			"insert into t1 values (0,0)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:            "/* client a */ set autocommit = off",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ set autocommit = off",
				SkipResultsCheck: true,
			},
			{
				Query:    "/* client a */ insert into t1 values (1, 1)",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
			},
			{
				Query:       "/* client a */ insert into t1 values (1, 2)",
				ExpectedErr: sql.ErrPrimaryKeyViolation,
			},
			{
				Query:    "/* client a */ insert into t1 values (2, 2)",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
			},
			{
				Query:    "/* client a */ select * from t1 order by pk",
				Expected: []sql.UntypedSqlRow{{0, 0}, {1, 1}, {2, 2}},
			},
			{
				Query:    "/* client b */ select * from t1 order by pk",
				Expected: []sql.UntypedSqlRow{{0, 0}},
			},
			{
				Query:            "/* client a */ commit",
				SkipResultsCheck: true,
			},
			{
				Query:            "/* client b */ start transaction",
				SkipResultsCheck: true,
			},
			{
				Query:    "/* client b */ select * from t1 order by pk",
				Expected: []sql.UntypedSqlRow{{0, 0}, {1, 1}, {2, 2}},
			},
			{
				Query:    "/* client a */ select * from t1 order by pk",
				Expected: []sql.UntypedSqlRow{{0, 0}, {1, 1}, {2, 2}},
			},
		},
	},
}
