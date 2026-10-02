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

// AlterTableScriptTests contains self-contained alter table script tests.
var AlterTableScriptTests = []ScriptTest{
	{
		Name:    "alter table out of range value error of column type change",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table t (i int primary key, i2 int, key(i2));",
			"insert into t values (0,-1)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       `alter table t modify column i2 int unsigned`,
				ExpectedErr: sql.ErrValueOutOfRange,
			},
		},
	},
	{
		Name:    "Multialter DDL with ADD/DROP INDEX",
		Dialect: "mysql",
		SetUpScript: []string{
			"CREATE TABLE t(pk int primary key, v1 int)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "ALTER TABLE t DROP COLUMN v1, ADD INDEX myidx (v1)",
				ExpectedErr: sql.ErrKeyColumnDoesNotExist,
			},
			{
				Query: "DESCRIBE t",
				Expected: []sql.Row{
					{"pk", "int", "NO", "PRI", nil, ""},
					{"v1", "int", "YES", "", nil, ""}, // should not be dropped
				},
			},
			{
				Query:    "ALTER TABLE t ADD COLUMN (v2 int), ADD INDEX myidx (v2)",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query: "DESCRIBE t",
				Expected: []sql.Row{
					{"pk", "int", "NO", "PRI", nil, ""},
					{"v1", "int", "YES", "", nil, ""},
					{"v2", "int", "YES", "MUL", nil, ""},
				},
			},
			{
				Query:       "ALTER TABLE t ADD COLUMN (v3 int), DROP INDEX notanindex",
				ExpectedErr: sql.ErrCantDropFieldOrKey,
			},
			{
				Query: "DESCRIBE t",
				Expected: []sql.Row{
					{"pk", "int", "NO", "PRI", nil, ""},
					{"v1", "int", "YES", "", nil, ""},
					{"v2", "int", "YES", "MUL", nil, ""},
				},
			},
			{
				Query:       "ALTER TABLE t ADD COLUMN (v4 int), ADD INDEX myidx (notacolumn)",
				ExpectedErr: sql.ErrKeyColumnDoesNotExist,
			},
			{
				Query: "DESCRIBE t",
				Expected: []sql.Row{
					{"pk", "int", "NO", "PRI", nil, ""},
					{"v1", "int", "YES", "", nil, ""},
					{"v2", "int", "YES", "MUL", nil, ""},
				},
			},
			{
				Query:       "ALTER TABLE t ADD COLUMN (v4 int), ADD INDEX myidx2 (v4), DROP INDEX notanindex;",
				ExpectedErr: sql.ErrCantDropFieldOrKey,
			},
			{
				Query: "DESCRIBE t",
				Expected: []sql.Row{
					{"pk", "int", "NO", "PRI", nil, ""},
					{"v1", "int", "YES", "", nil, ""},
					{"v2", "int", "YES", "MUL", nil, ""},
				},
			},
			{
				Query:    "ALTER TABLE t ADD COLUMN (v4 int), ADD INDEX myidx2 (v4)",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query: "DESCRIBE t",
				Expected: []sql.Row{
					{"pk", "int", "NO", "PRI", nil, ""},
					{"v1", "int", "YES", "", nil, ""},
					{"v2", "int", "YES", "MUL", nil, ""},
					{"v4", "int", "YES", "MUL", nil, ""},
				},
			},
			{
				Query:    "ALTER TABLE t ADD COLUMN (v5 int), RENAME INDEX myidx2 TO myidx3",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "ALTER TABLE t DROP INDEX myidx, ADD INDEX v5idx (v5)",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query: "DESCRIBE t",
				Expected: []sql.Row{
					{"pk", "int", "NO", "PRI", nil, ""},
					{"v1", "int", "YES", "", nil, ""},
					{"v2", "int", "YES", "", nil, ""},
					{"v4", "int", "YES", "MUL", nil, ""},
					{"v5", "int", "YES", "MUL", nil, ""},
				},
			},
		},
	},
	{
		Name:    "ALTER TABLE MULTI ADD/DROP COLUMN",
		Dialect: "mysql",
		SetUpScript: []string{
			"CREATE TABLE test (pk BIGINT PRIMARY KEY, v1 BIGINT NOT NULL DEFAULT 88);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "INSERT INTO test (pk) VALUES (1);",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "ALTER TABLE test DROP COLUMN v1, ADD COLUMN v2 INT NOT NULL DEFAULT 100",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query: "describe test",
				Expected: []sql.Row{
					{"pk", "bigint", "NO", "PRI", nil, ""},
					{"v2", "int", "NO", "", "100", ""},
				},
			},
			{
				Query:    "ALTER TABLE TEST MODIFY COLUMN pk BIGINT AUTO_INCREMENT, AUTO_INCREMENT = 100",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "INSERT INTO test (v2) values (11)",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1, InsertID: 100}}},
			},
			{
				Query:    "SELECT * from test where pk = 100",
				Expected: []sql.Row{{100, 11}},
			},
			{
				Query:       "ALTER TABLE test DROP COLUMN v2, ADD COLUMN v3 int NOT NULL after v2",
				ExpectedErr: sql.ErrTableColumnNotFound,
			},
			{
				Query: "describe test",
				Expected: []sql.Row{
					{"pk", "bigint", "NO", "PRI", nil, "auto_increment"},
					{"v2", "int", "NO", "", "100", ""},
				},
			},
			{
				Query:       "ALTER TABLE test DROP COLUMN v2, RENAME COLUMN v2 to v3",
				ExpectedErr: sql.ErrTableColumnNotFound,
			},
			{
				Query: "describe test",
				Expected: []sql.Row{
					{"pk", "bigint", "NO", "PRI", nil, "auto_increment"},
					{"v2", "int", "NO", "", "100", ""},
				},
			},
			{
				Query:       "ALTER TABLE test RENAME COLUMN v2 to v3, DROP COLUMN v2",
				ExpectedErr: sql.ErrTableColumnNotFound,
			},
			{
				Query: "describe test",
				Expected: []sql.Row{
					{"pk", "bigint", "NO", "PRI", nil, "auto_increment"},
					{"v2", "int", "NO", "", "100", ""},
				},
			},
			{
				Query:    "ALTER TABLE test ADD COLUMN (v3 int NOT NULL), add column (v4 int), drop column v2, add column (v5 int NOT NULL)",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query: "DESCRIBE test",
				Expected: []sql.Row{
					{"pk", "bigint", "NO", "PRI", nil, "auto_increment"},
					{"v3", "int", "NO", "", nil, ""},
					{"v4", "int", "YES", "", nil, ""},
					{"v5", "int", "NO", "", nil, ""},
				},
			},
			{
				Query:    "ALTER TABLE test ADD COLUMN (v6 int not null), RENAME COLUMN v5 TO mycol, DROP COLUMN v4, ADD COLUMN (v7 int);",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query: "describe test",
				Expected: []sql.Row{
					{"pk", "bigint", "NO", "PRI", nil, "auto_increment"},
					{"v3", "int", "NO", "", nil, ""},
					{"mycol", "int", "NO", "", nil, ""},
					{"v6", "int", "NO", "", nil, ""},
					{"v7", "int", "YES", "", nil, ""},
				},
			},
			// TODO: Does not include tests with column renames and defaults.
		},
	},
}
