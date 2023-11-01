// Copyright 2023 Dolthub, Inc.
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

var DropColumnScripts = []ScriptTest{
	{
		Name: "drop last column",
		SetUpScript: []string{
			"ALTER TABLE mytable DROP COLUMN s",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SHOW FULL COLUMNS FROM mytable",
				Expected: []sql.Row{{"i", "bigint", nil, "NO", "PRI", "NULL", "", "", ""}},
			},
			{
				Query:    "select * from mytable order by i",
				Expected: []sql.Row{{1}, {2}, {3}},
			},
		},
	},
	{
		Name: "drop first column",
		SetUpScript: []string{
			"CREATE TABLE t1 (a int, b varchar(10), c bigint, k bigint primary key)",
			"insert into t1 values (1, 'abc', 2, 3), (4, 'def', 5, 6)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "ALTER TABLE t1 DROP COLUMN a",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query: "SHOW FULL COLUMNS FROM t1",
				Expected: []sql.Row{
					{"b", "varchar(10)", "utf8mb4_0900_bin", "YES", "", "NULL", "", "", ""},
					{"c", "bigint", nil, "YES", "", "NULL", "", "", ""},
					{"k", "bigint", nil, "NO", "PRI", "NULL", "", "", ""},
				},
			},
			{
				Query: "SELECT * FROM t1 ORDER BY b",
				Expected: []sql.Row{
					{"abc", 2, 3},
					{"def", 5, 6},
				},
			},
		},
	},
	{
		Name: "drop middle column",
		SetUpScript: []string{
			"CREATE TABLE t2 (a int, b varchar(10), c bigint, k bigint primary key)",
			"insert into t2 values (1, 'abc', 2, 3), (4, 'def', 5, 6)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "ALTER TABLE t2 DROP COLUMN b",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query: "SHOW FULL COLUMNS FROM t2",
				Expected: []sql.Row{
					{"a", "int", nil, "YES", "", "NULL", "", "", ""},
					{"c", "bigint", nil, "YES", "", "NULL", "", "", ""},
					{"k", "bigint", nil, "NO", "PRI", "NULL", "", "", ""},
				},
			},
			{
				Query: "SELECT * FROM t2 ORDER BY c",
				Expected: []sql.Row{
					{1, 2, 3},
					{4, 5, 6},
				},
			},
		},
	},
	{
		// TODO: primary key column drops not well supported yet
		Name: "drop primary key column",
		SetUpScript: []string{
			"CREATE TABLE t3 (a int primary key, b varchar(10), c bigint)",
			"insert into t3 values (1, 'abc', 2), (3, 'def', 4)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Skip:     true,
				Query:    "ALTER TABLE t3 DROP COLUMN a",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Skip:  true,
				Query: "SHOW FULL COLUMNS FROM t3",
				Expected: []sql.Row{
					{"b", "varchar(10)", "utf8mb4_0900_bin", "YES", "", "NULL", "", "", ""},
					{"c", "bigint", nil, "YES", "", "NULL", "", "", ""},
				},
			},
			{
				Skip:  true,
				Query: "SELECT * FROM t3 ORDER BY b",
				Expected: []sql.Row{
					{"abc", 2},
					{"def", 4},
				},
			},
		},
	},
	{
		Name: "error cases",
		SetUpScript: []string{
			"create table t4 (a int primary key, b int, c int default (b+10))",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "ALTER TABLE not_exist DROP COLUMN s",
				ExpectedErr: sql.ErrTableNotFound,
			},
			{
				Query:       "ALTER TABLE mytable DROP COLUMN s",
				ExpectedErr: sql.ErrTableColumnNotFound,
			},
			{
				Query:       "ALTER TABLE t4 DROP COLUMN b",
				ExpectedErr: sql.ErrDropColumnReferencedInDefault,
			},
		},
	},
}

var DropColumnKeylessTablesScripts = []ScriptTest{
	{
		Name: "drop last column",
		SetUpScript: []string{
			"create table t0 (i bigint, s varchar(20))",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "ALTER TABLE t0 DROP COLUMN s",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "SHOW FULL COLUMNS FROM t0",
				Expected: []sql.Row{{"i", "bigint", nil, "YES", "", "NULL", "", "", ""}},
			},
		},
	},
	{
		Name: "drop first column",
		SetUpScript: []string{
			"CREATE TABLE t1 (a int, b varchar(10), c bigint)",
			"insert into t1 values (1, 'abc', 2), (4, 'def', 5)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "ALTER TABLE t1 DROP COLUMN a",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "SHOW FULL COLUMNS FROM t1",
				Expected: []sql.Row{{"b", "varchar(10)", "utf8mb4_0900_bin", "YES", "", "NULL", "", "", ""}, {"c", "bigint", nil, "YES", "", "NULL", "", "", ""}},
			},
			{
				Query: "SELECT * FROM t1 ORDER BY b",
				Expected: []sql.Row{
					{"abc", 2},
					{"def", 5},
				},
			},
		},
	},
	{
		Name: "drop middle column",
		SetUpScript: []string{
			"CREATE TABLE t2 (a int, b varchar(10), c bigint)",
			"insert into t2 values (1, 'abc', 2), (4, 'def', 5)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "ALTER TABLE t2 DROP COLUMN b",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "SHOW FULL COLUMNS FROM t2",
				Expected: []sql.Row{{"a", "int", nil, "YES", "", "NULL", "", "", ""}, {"c", "bigint", nil, "YES", "", "NULL", "", "", ""}},
			},
			{
				Query: "SELECT * FROM t2 ORDER BY c",
				Expected: []sql.Row{
					{1, 2},
					{4, 5},
				},
			},
		},
	},
	{
		Name:        "error cases",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "ALTER TABLE not_exist DROP COLUMN s",
				ExpectedErr: sql.ErrTableNotFound,
			},
			{
				Query:       "ALTER TABLE t0 DROP COLUMN s",
				ExpectedErr: sql.ErrTableColumnNotFound,
			},
			{
				Query: "SELECT * FROM t2 ORDER BY c",
				Expected: []sql.Row{
					{1, 2},
					{4, 5},
				},
			},
		},
	},
}
