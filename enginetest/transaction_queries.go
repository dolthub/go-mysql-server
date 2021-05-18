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
	Assertions []TransactionTestAssertion
}

type TransactionTestAssertion struct {
	// Client the name of the client executing the assertions. If the client name hasn't been seen before during this
	// script, a new session is created for them. Otherwise, the existing session for them will be reused.
	Client      string
	ScriptTestAssertion
}

var TransactionTests = []TransactionTest{
	{
		Name:        "autocommit on",
		SetUpScript: []string {
			"create table a (b int primary key, c int)",
			"insert into a values (1, 1)",
		},
		Assertions:  []TransactionTestAssertion{
			{
				Client:              "a",
				ScriptTestAssertion: ScriptTestAssertion{
					Query:           "insert into a values (2, 2)",
					Expected:        []sql.Row{{sql.NewOkResult(1)}},
				},
			},
			{
				Client:              "b",
				ScriptTestAssertion: ScriptTestAssertion{
					Query:           "select * from a",
					Expected:        []sql.Row{
						{1, 1},
						{2, 2},
					},
				},
			},
			{
				Client:              "b",
				ScriptTestAssertion: ScriptTestAssertion{
					Query:           "insert into a values (3, 3)",
					Expected:        []sql.Row{{sql.NewOkResult(1)}},
				},
			},
			{
				Client:              "a",
				ScriptTestAssertion: ScriptTestAssertion{
					Query:           "select * from a",
					Expected:        []sql.Row{
						{1, 1},
						{2, 2},
						{3, 3},
					},
				},
			},
		},
	},
	{
		Name:        "autocommit off",
		SetUpScript: []string {
			"create table a (b int primary key, c int)",
			"insert into a values (1, 1)",
		},
		Assertions:  []TransactionTestAssertion{
			{
				Client:              "a",
				ScriptTestAssertion: ScriptTestAssertion{
					Query:           "set autocommit = off",
					Expected:        []sql.Row{{}},
				},
			},
			{
				Client:              "b",
				ScriptTestAssertion: ScriptTestAssertion{
					Query:           "set autocommit = off",
					Expected:        []sql.Row{{}},
				},
			},
			{
				Client:              "b",
				ScriptTestAssertion: ScriptTestAssertion{
					Query:           "select * from a",
					Expected:        []sql.Row{
						{1, 1},
					},
				},
			},
			{
				Client:              "b",
				ScriptTestAssertion: ScriptTestAssertion{
					Query:           "insert into a values (2, 2)",
					Expected:        []sql.Row{{sql.NewOkResult(1)}},
				},
			},
			{
				Client:              "a",
				ScriptTestAssertion: ScriptTestAssertion{
					Query:           "select * from a",
					Expected:        []sql.Row{
						{1, 1},
					},
				},
			},
			{
				Client:              "a",
				ScriptTestAssertion: ScriptTestAssertion{
					Query:           "insert into a values (3,3)",
					Expected:        []sql.Row{{sql.NewOkResult(1)}},
				},
			},
			{
				Client:              "b",
				ScriptTestAssertion: ScriptTestAssertion{
					Query:           "select * from a",
					Expected:        []sql.Row{
						{1, 1},
						{2, 2},
					},
				},
			},
			{
				Client:              "b",
				ScriptTestAssertion: ScriptTestAssertion{
					Query:           "commit",
					Expected:        []sql.Row{},
				},
			},
			{
				Client:              "a",
				ScriptTestAssertion: ScriptTestAssertion{
					Query:           "select * from a",
					Expected:        []sql.Row{
						{1, 1},
						{3, 3},
					},
				},
			},
			{
				Client:              "b",
				ScriptTestAssertion: ScriptTestAssertion{
					Query:           "select * from a",
					Expected:        []sql.Row{
						{1, 1},
						{2, 2},
					},
				},
			},
			{
				Client:              "a",
				ScriptTestAssertion: ScriptTestAssertion{
					Query:           "commit",
					Expected:        []sql.Row{},
				},
			},
			{
				Client:              "b",
				ScriptTestAssertion: ScriptTestAssertion{
					Query:           "select * from a",
					Expected:        []sql.Row{
						{1, 1},
						{2, 2},
						{3, 3},
					},
				},
			},
			{
				Client:              "a",
				ScriptTestAssertion: ScriptTestAssertion{
					Query:           "select * from a",
					Expected:        []sql.Row{
						{1, 1},
						{2, 2},
						{3, 3},
					},
				},
			},
		},
	},
}
