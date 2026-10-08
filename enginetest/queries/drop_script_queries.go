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
	"github.com/dolthub/go-mysql-server/sql/types"
)

// DropDatabaseScriptTests contains self-contained script tests for DROP DATABASE and DROP SCHEMA.
var DropDatabaseScriptTests = []ScriptTest{
	{
		Name: "DROP DATABASE correctly works",
		Assertions: []ScriptTestAssertion{
			{
				Query:    "DROP DATABASE mydb",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1}}},
			},
			{
				Query:    "SELECT DATABASE()",
				Expected: []sql.Row{{nil}},
			},
			{
				// TODO: incorrect error returned because the currentdb is not set to empty
				Skip:        true,
				Query:       "SHOW TABLES",
				ExpectedErr: sql.ErrNoDatabaseSelected,
			},
		},
	},
	{
		Name: "DROP DATABASE works on newly created databases.",
		SetUpScript: []string{
			"CREATE DATABASE testdb",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "USE testdb",
				Expected: []sql.Row{},
			},
			{
				Query:    "DROP DATABASE testdb",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1}}},
			},
			{
				Query:       "USE testdb",
				ExpectedErr: sql.ErrDatabaseNotFound,
			},
		},
	},
	{
		Name: "DROP DATABASE works on current database and sets current database to empty.",
		SetUpScript: []string{
			"CREATE DATABASE testdb",
			"USE TESTdb",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "DROP DATABASE TESTDB",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1}}},
			},
			{
				Query:    "SELECT DATABASE()",
				Expected: []sql.Row{{nil}},
			},
			{
				Query:       "USE testdb",
				ExpectedErr: sql.ErrDatabaseNotFound,
			},
		},
	},
	{
		Name: "DROP SCHEMA works on newly created databases.",
		SetUpScript: []string{
			"CREATE SCHEMA testdb",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "DROP SCHEMA TESTDB",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1}}},
			},
			{
				Query:       "USE testdb",
				ExpectedErr: sql.ErrDatabaseNotFound,
			},
		},
	},
	{
		Name: "DROP DATABASE IF EXISTS correctly works.",
		SetUpScript: []string{
			"DROP DATABASE mydb",
			"CREATE DATABASE testdb",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "DROP DATABASE IF EXISTS mydb",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 0}}},
			},
			{
				Query:    "SHOW WARNINGS",
				Expected: []sql.Row{{"Note", 1008, "Can't drop database mydb; database doesn't exist "}},
			},
			{
				Query:    "DROP DATABASE IF EXISTS testdb",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1}}},
			},
			{
				Query:    "SHOW WARNINGS",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT DATABASE()",
				Expected: []sql.Row{{nil}},
			},
			{
				Query:       "USE testdb",
				ExpectedErr: sql.ErrDatabaseNotFound,
			},
			{
				Query:    "DROP DATABASE IF EXISTS testdb",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 0}}},
			},
			{
				Query:    "SHOW WARNINGS",
				Expected: []sql.Row{{"Note", 1008, "Can't drop database testdb; database doesn't exist "}},
			},
		},
	},
}

// DropTableScriptTests contains self-contained drop table script tests.
var DropTableScriptTests = []ScriptTest{
	{
		Name:    "drop table if exists on unknown table shows warning",
		Dialect: "mysql",
		Assertions: []ScriptTestAssertion{
			{
				Query:                           "DROP TABLE IF EXISTS non_existent_table;",
				ExpectedWarning:                 1051,
				ExpectedWarningsCount:           1,
				ExpectedWarningMessageSubstring: "Unknown table 'non_existent_table'",
				SkipResultsCheck:                true,
			},
		},
	},
}
