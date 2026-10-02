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

// SchemaScriptTests contains self-contained schema script tests.
var SchemaScriptTests = []ScriptTest{
	{
		Name: "can't create view with same name as existing table",
		SetUpScript: []string{
			"create table t (i int);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "create view t as select 1",
				ExpectedErr: sql.ErrTableAlreadyExists,
			},
		},
	},
	{
		Name: "can't create table with same name as existing view",
		SetUpScript: []string{
			"create view t as select 1",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "create table t (i int);",
				ExpectedErr: sql.ErrTableAlreadyExists,
			},
		},
	},
	{
		Name:    "renaming views with RENAME TABLE ... TO .. statement",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table t1 (id int primary key, v1 int);",
			"create view v1 as select * from t1;",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "show tables;",
				Expected: []sql.Row{{"t1"}, {"v1"}},
			},
			{
				Query:    "rename table v1 to view1",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 0}}},
			},
			{
				Query:    "show tables;",
				Expected: []sql.Row{{"t1"}, {"view1"}},
			},
			{
				Query:    "rename table view1 to newViewName, t1 to newTableName",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 0}}},
			},
			{
				Query:    "show tables;",
				Expected: []sql.Row{{"newTableName"}, {"newViewName"}},
			},
		},
	},
	{
		Name:    "renaming views with ALTER TABLE ... RENAME .. statement should fail",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table t1 (id int primary key, v1 int);",
			"create view v1 as select * from t1;",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "show tables;",
				Expected: []sql.Row{{"t1"}, {"v1"}},
			},
			{
				Query:       "alter table v1 rename to view1",
				ExpectedErr: sql.ErrExpectedTableFoundView,
			},
			{
				Query:    "show tables;",
				Expected: []sql.Row{{"t1"}, {"v1"}},
			},
		},
	},

	{
		Name: "Querying existing view that references non-existing table",
		SetUpScript: []string{
			"CREATE TABLE a(id int primary key, col1 int);",
			"CREATE VIEW b AS SELECT * FROM a;",
			"CREATE VIEW f AS SELECT col1 AS npk FROM a;",
			"RENAME TABLE a TO d;",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "CREATE VIEW g AS SELECT * FROM nonexistenttable;",
				ExpectedErr: sql.ErrTableNotFound,
			},
			{
				// TODO: ALTER VIEWs are not supported
				Skip:        true,
				Query:       "ALTER VIEW b AS SELECT * FROM nonexistenttable;",
				ExpectedErr: sql.ErrTableNotFound,
			},
			{
				Query:       "SELECT * FROM b;",
				ExpectedErr: sql.ErrInvalidRefInView,
			},
			{
				Query:    "RENAME TABLE d TO a;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "SELECT * FROM b;",
				Expected: []sql.Row{},
			},
			{
				Query:    "ALTER TABLE a RENAME COLUMN col1 TO newcol;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				// TODO: View definition should have 'SELECT *' be expanded to each column of the referenced table
				Skip:        true,
				Query:       "SELECT * FROM b;",
				ExpectedErr: sql.ErrInvalidRefInView,
			},
			{
				Query:       "SELECT * FROM f;",
				ExpectedErr: sql.ErrInvalidRefInView,
			},
		},
	},
}
