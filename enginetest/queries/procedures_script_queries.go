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

// ProceduresScriptTests contains self-contained procedures script tests.
var ProceduresScriptTests = []ScriptTest{
	{
		// https://github.com/dolthub/dolt/issues/9865
		Name:    "Stored procedure containing a transaction does not return EOF",
		Dialect: "mysql",
		SetUpScript: []string{
			"CREATE TABLE test_table (id INT PRIMARY KEY, name TEXT)",
			`CREATE PROCEDURE my_proc()
BEGIN
    START TRANSACTION;
    INSERT INTO test_table VALUES (1, 'test');
    COMMIT;
END`,
			`CREATE PROCEDURE empty_procedure()
BEGIN
END`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "CALL my_proc()",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 0, InsertID: 0, Info: nil}}},
			},
			{
				Query:    "SELECT * FROM test_table",
				Expected: []sql.Row{{1, "test"}},
			},
			{
				Query:    "CALL empty_procedure()",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 0, InsertID: 0, Info: nil}}},
			},
		},
	},
	{
		// All DECLARE statements are only allowed under BEGIN/END blocks
		Name: "Top-level DECLARE statements",
		Assertions: []ScriptTestAssertion{
			{
				Query:       "DECLARE no_such_table CONDITION FOR SQLSTATE '42S02'",
				ExpectedErr: sql.ErrSyntaxError,
			},
			{
				Query:       "DECLARE no_such_table CONDITION FOR 1051",
				ExpectedErr: sql.ErrSyntaxError,
			},
			{
				Query:       "DECLARE a CHAR(16)",
				ExpectedErr: sql.ErrSyntaxError,
			},
			{
				Query:       "DECLARE cur2 CURSOR FOR SELECT i FROM test.t2",
				ExpectedErr: sql.ErrSyntaxError,
			},
			{
				Query:       "DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE",
				ExpectedErr: sql.ErrSyntaxError,
			},
		},
	},
}
