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

var CreateDatabaseScripts = []ScriptTest{
	{
		// https://github.com/dolthub/dolt/pull/9830
		Name: "CREATE SCHEMA without database selection falls back to CREATE DATABASE",
		SetUpScript: []string{
			"CREATE DATABASE tmp",
			"USE tmp",
		},
		Dialect: "mysql",
		Assertions: []ScriptTestAssertion{
			{
				Query: "DROP DATABASE tmp",
			},
			{
				Query:    "CREATE SCHEMA NewDatabase",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1}}},
			},
			{
				Query:    "SHOW DATABASES",
				Expected: []sql.Row{{"NewDatabase"}, {"information_schema"}, {"mydb"}, {"mysql"}},
			},
			{
				Query:    "USE NewDatabase",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT DATABASE()",
				Expected: []sql.Row{{"NewDatabase"}},
			},
			{
				Query:    "CREATE TABLE test_table (id INT PRIMARY KEY)",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 0}}},
			},
			{
				Query:    "SHOW TABLES",
				Expected: []sql.Row{{"test_table"}},
			},
			{
				Query:    "USE mydb",
				Expected: []sql.Row{},
			},
			{
				Query:    "DROP DATABASE NewDatabase",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1}}},
			},
		},
	},
	{
		Name: "CREATE DATABASE and create table",
		Assertions: []ScriptTestAssertion{
			{
				Query:    "CREATE DATABASE testdb",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1}}},
			},
			{
				Query:    "USE testdb",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT DATABASE()",
				Expected: []sql.Row{{"testdb"}},
			},
			{
				Query:    "CREATE TABLE test (pk int primary key)",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "SHOW TABLES",
				Expected: []sql.Row{{"test"}},
			},
		},
	},
	{
		Name: "CREATE DATABASE IF NOT EXISTS",
		Assertions: []ScriptTestAssertion{
			{
				Query:    "CREATE DATABASE IF NOT EXISTS testdb2",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1}}},
			},
			{
				Query:    "USE testdb2",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT DATABASE()",
				Expected: []sql.Row{{"testdb2"}},
			},
			{
				Query:    "CREATE TABLE test (pk int primary key)",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "SHOW TABLES",
				Expected: []sql.Row{{"test"}},
			},
		},
	},
	{
		Name: "CREATE SCHEMA",
		Assertions: []ScriptTestAssertion{
			{
				Query:    "CREATE SCHEMA testdb3",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1}}},
			},
			{
				Query:    "USE testdb3",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT DATABASE()",
				Expected: []sql.Row{{"testdb3"}},
			},
			{
				Query:    "CREATE TABLE test (pk int primary key)",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "SHOW TABLES",
				Expected: []sql.Row{{"test"}},
			},
		},
	},
	{
		Name: "CREATE DATABASE error handling",
		Assertions: []ScriptTestAssertion{
			{
				Query:    "CREATE DATABASE newtestdb CHARACTER SET utf8mb4 ENCRYPTION='N'",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "SHOW WARNINGS /* 1 */",
				Expected: []sql.Row{{"Warning", 1235, "Setting CHARACTER SET, COLLATION and ENCRYPTION are not supported yet"}},
			},
			{
				Query:    "CREATE DATABASE newtest1db DEFAULT COLLATE binary ENCRYPTION='Y'",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query: "SHOW WARNINGS /* 2 */",
				Expected: []sql.Row{
					{"Warning", 1235, "Setting CHARACTER SET, COLLATION and ENCRYPTION are not supported yet"},
				},
			},
			{
				Query:       "CREATE DATABASE mydb",
				ExpectedErr: sql.ErrDatabaseExists,
			},
			{
				Query:    "CREATE DATABASE IF NOT EXISTS mydb",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1}}},
			},
			{
				Query:    "SHOW WARNINGS /* 3 */",
				Expected: []sql.Row{{"Note", 1007, "Can't create database mydb; database exists "}},
			},
		},
	},
}
