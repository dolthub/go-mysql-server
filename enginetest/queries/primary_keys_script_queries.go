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

// PrimaryKeysScriptTests contains self-contained primary keys script tests.
var PrimaryKeysScriptTests = []ScriptTest{
	{
		Name: "recreate primary key rebuilds secondary indexes",
		SetUpScript: []string{
			"create table a (x int, y int, z int, primary key (x,y,z), index idx1 (y))",
			"insert into a values (1,2,3), (4,5,6), (7,8,9)",
			"alter table a drop primary key",
			"alter table a add primary key (y,z,x)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "delete from a where y = 2",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "delete from a where y = 2",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "select * from a where y = 2",
				Expected: []sql.Row{},
			},
			{
				Query:    "select * from a where y = 5",
				Expected: []sql.Row{{4, 5, 6}},
			},
		},
	},
	{
		Name:    "Multialter DDL with ADD/DROP Primary Key",
		Dialect: "mysql",
		SetUpScript: []string{
			"CREATE TABLE t(pk int primary key, v1 int)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "ALTER TABLE t ADD COLUMN (v2 int), drop primary key, add primary key (v2)",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query: "DESCRIBE t",
				Expected: []sql.Row{
					{"pk", "int", "NO", "", nil, ""},
					{"v1", "int", "YES", "", nil, ""},
					{"v2", "int", "NO", "PRI", nil, ""},
				},
			},
			{
				Query:       "ALTER TABLE t ADD COLUMN (v3 int), drop primary key, add primary key (notacolumn)",
				ExpectedErr: sql.ErrKeyColumnDoesNotExist,
			},
			{
				Query: "DESCRIBE t",
				Expected: []sql.Row{
					{"pk", "int", "NO", "", nil, ""},
					{"v1", "int", "YES", "", nil, ""},
					{"v2", "int", "NO", "PRI", nil, ""},
				},
			},
		},
	},
	{
		Name: "primary key order",
		SetUpScript: []string{
			"create table t1 (a varchar(5), b varchar(10), primary key(a, b));",
			"create table t2 (a varchar(5), b varchar(10), primary key(b, a));",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:          "insert into t1 (a, b) values ('1234567890', '12345')",
				ExpectedErrStr: "string '1234567890' is too large for column 'a'",
			},
			{
				Query: "insert into t1 (b, a) values ('1234567890', '12345')",
				Expected: []sql.Row{
					{types.OkResult{RowsAffected: 1}},
				},
			},
			{
				Query: "select a, b from t1",
				Expected: []sql.Row{
					{"12345", "1234567890"},
				},
			},
			{
				Query:          "insert into t2 (a, b) values ('1234567890', '12345')",
				ExpectedErrStr: "string '1234567890' is too large for column 'a'",
			},
			{
				Query: "insert into t2 (b, a) values ('1234567890', '12345')",
				Expected: []sql.Row{
					{types.OkResult{RowsAffected: 1}},
				},
			},
			{
				Query: "select a, b from t2",
				Expected: []sql.Row{
					{"12345", "1234567890"},
				},
			},
		},
	},
}
