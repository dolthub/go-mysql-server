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
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"
)

var ProcedureDropTests = []ScriptTest{
	{
		Name: "DROP procedures",
		SetUpScript: []string{
			"CREATE PROCEDURE p1() SELECT 5",
			"CREATE PROCEDURE p2() SELECT 6",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "CALL p1",
				Expected: []sql.Row{
					{
						int64(5),
					},
				},
			},
			{
				Query: "CALL p2",
				Expected: []sql.Row{
					{
						int64(6),
					},
				},
			},
			{
				Query:    "DROP PROCEDURE p1",
				Expected: []sql.Row{{types.OkResult{}}},
			},
			{
				Query:       "CALL p1",
				ExpectedErr: sql.ErrStoredProcedureDoesNotExist,
			},
			{
				Query:    "DROP PROCEDURE IF EXISTS p2",
				Expected: []sql.Row{{types.OkResult{}}},
			},
			{
				Query:       "CALL p2",
				ExpectedErr: sql.ErrStoredProcedureDoesNotExist,
			},
			{
				Query:       "DROP PROCEDURE p3",
				ExpectedErr: sql.ErrStoredProcedureDoesNotExist,
			},
			{
				Query:    "DROP PROCEDURE IF EXISTS p4",
				Expected: []sql.Row{{types.OkResult{}}},
			},
		},
	},
}

var ProcedureShowStatus = []ScriptTest{
	{
		Name: "SHOW procedures",
		SetUpScript: []string{
			"CREATE PROCEDURE p1() COMMENT 'hi' DETERMINISTIC SELECT 6",
			"CREATE definer=`user` PROCEDURE p2() SQL SECURITY INVOKER SELECT 7",
			"CREATE PROCEDURE p21() SQL SECURITY DEFINER SELECT 8",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW PROCEDURE STATUS",
				Expected: []sql.Row{
					{
						"mydb",                // Db
						"p1",                  // Name
						"PROCEDURE",           // Type
						"",                    // Definer
						time.Unix(0, 0).UTC(), // Modified
						time.Unix(0, 0).UTC(), // Created
						"DEFINER",             // Security_type
						"hi",                  // Comment
						"utf8mb4",             // character_set_client
						"utf8mb4_0900_bin",    // collation_connection
						"utf8mb4_0900_bin",    // Database Collation
					},
					{
						"mydb",                // Db
						"p2",                  // Name
						"PROCEDURE",           // Type
						"user@%",              // Definer
						time.Unix(0, 0).UTC(), // Modified
						time.Unix(0, 0).UTC(), // Created
						"INVOKER",             // Security_type
						"",                    // Comment
						"utf8mb4",             // character_set_client
						"utf8mb4_0900_bin",    // collation_connection
						"utf8mb4_0900_bin",    // Database Collation
					},
					{
						"mydb",                // Db
						"p21",                 // Name
						"PROCEDURE",           // Type
						"",                    // Definer
						time.Unix(0, 0).UTC(), // Modified
						time.Unix(0, 0).UTC(), // Created
						"DEFINER",             // Security_type
						"",                    // Comment
						"utf8mb4",             // character_set_client
						"utf8mb4_0900_bin",    // collation_connection
						"utf8mb4_0900_bin",    // Database Collation
					},
				},
			},
			{
				Query: "SHOW PROCEDURE STATUS LIKE 'p2%'",
				Expected: []sql.Row{
					{
						"mydb",                // Db
						"p2",                  // Name
						"PROCEDURE",           // Type
						"user@%",              // Definer
						time.Unix(0, 0).UTC(), // Modified
						time.Unix(0, 0).UTC(), // Created
						"INVOKER",             // Security_type
						"",                    // Comment
						"utf8mb4",             // character_set_client
						"utf8mb4_0900_bin",    // collation_connection
						"utf8mb4_0900_bin",    // Database Collation
					},
					{
						"mydb",                // Db
						"p21",                 // Name
						"PROCEDURE",           // Type
						"",                    // Definer
						time.Unix(0, 0).UTC(), // Modified
						time.Unix(0, 0).UTC(), // Created
						"DEFINER",             // Security_type
						"",                    // Comment
						"utf8mb4",             // character_set_client
						"utf8mb4_0900_bin",    // collation_connection
						"utf8mb4_0900_bin",    // Database Collation
					},
				},
			},
			{
				Query:    "SHOW PROCEDURE STATUS LIKE 'p4'",
				Expected: []sql.Row{},
			},
			{
				Query: "SHOW PROCEDURE STATUS WHERE Db = 'mydb'",
				Expected: []sql.Row{
					{
						"mydb",                // Db
						"p1",                  // Name
						"PROCEDURE",           // Type
						"",                    // Definer
						time.Unix(0, 0).UTC(), // Modified
						time.Unix(0, 0).UTC(), // Created
						"DEFINER",             // Security_type
						"hi",                  // Comment
						"utf8mb4",             // character_set_client
						"utf8mb4_0900_bin",    // collation_connection
						"utf8mb4_0900_bin",    // Database Collation
					},
					{
						"mydb",                // Db
						"p2",                  // Name
						"PROCEDURE",           // Type
						"user@%",              // Definer
						time.Unix(0, 0).UTC(), // Modified
						time.Unix(0, 0).UTC(), // Created
						"INVOKER",             // Security_type
						"",                    // Comment
						"utf8mb4",             // character_set_client
						"utf8mb4_0900_bin",    // collation_connection
						"utf8mb4_0900_bin",    // Database Collation
					},
					{
						"mydb",                // Db
						"p21",                 // Name
						"PROCEDURE",           // Type
						"",                    // Definer
						time.Unix(0, 0).UTC(), // Modified
						time.Unix(0, 0).UTC(), // Created
						"DEFINER",             // Security_type
						"",                    // Comment
						"utf8mb4",             // character_set_client
						"utf8mb4_0900_bin",    // collation_connection
						"utf8mb4_0900_bin",    // Database Collation
					},
				},
			},
			{
				Query: "SHOW PROCEDURE STATUS WHERE Name LIKE '%1'",
				Expected: []sql.Row{
					{
						"mydb",                // Db
						"p1",                  // Name
						"PROCEDURE",           // Type
						"",                    // Definer
						time.Unix(0, 0).UTC(), // Modified
						time.Unix(0, 0).UTC(), // Created
						"DEFINER",             // Security_type
						"hi",                  // Comment
						"utf8mb4",             // character_set_client
						"utf8mb4_0900_bin",    // collation_connection
						"utf8mb4_0900_bin",    // Database Collation
					},
					{
						"mydb",                // Db
						"p21",                 // Name
						"PROCEDURE",           // Type
						"",                    // Definer
						time.Unix(0, 0).UTC(), // Modified
						time.Unix(0, 0).UTC(), // Created
						"DEFINER",             // Security_type
						"",                    // Comment
						"utf8mb4",             // character_set_client
						"utf8mb4_0900_bin",    // collation_connection
						"utf8mb4_0900_bin",    // Database Collation
					},
				},
			},
			{
				Query: "SHOW PROCEDURE STATUS WHERE Security_type = 'INVOKER'",
				Expected: []sql.Row{
					{
						"mydb",                // Db
						"p2",                  // Name
						"PROCEDURE",           // Type
						"user@%",              // Definer
						time.Unix(0, 0).UTC(), // Modified
						time.Unix(0, 0).UTC(), // Created
						"INVOKER",             // Security_type
						"",                    // Comment
						"utf8mb4",             // character_set_client
						"utf8mb4_0900_bin",    // collation_connection
						"utf8mb4_0900_bin",    // Database Collation
					},
				},
			},
			{
				Query: "SHOW PROCEDURE STATUS",
				Expected: []sql.Row{
					{
						"mydb",                // Db
						"p1",                  // Name
						"PROCEDURE",           // Type
						"",                    // Definer
						time.Unix(0, 0).UTC(), // Modified
						time.Unix(0, 0).UTC(), // Created
						"DEFINER",             // Security_type
						"hi",                  // Comment
						"utf8mb4",             // character_set_client
						"utf8mb4_0900_bin",    // collation_connection
						"utf8mb4_0900_bin",    // Database Collation
					},
					{
						"mydb",                // Db
						"p2",                  // Name
						"PROCEDURE",           // Type
						"user@%",              // Definer
						time.Unix(0, 0).UTC(), // Modified
						time.Unix(0, 0).UTC(), // Created
						"INVOKER",             // Security_type
						"",                    // Comment
						"utf8mb4",             // character_set_client
						"utf8mb4_0900_bin",    // collation_connection
						"utf8mb4_0900_bin",    // Database Collation
					},
					{
						"mydb",                // Db
						"p21",                 // Name
						"PROCEDURE",           // Type
						"",                    // Definer
						time.Unix(0, 0).UTC(), // Modified
						time.Unix(0, 0).UTC(), // Created
						"DEFINER",             // Security_type
						"",                    // Comment
						"utf8mb4",             // character_set_client
						"utf8mb4_0900_bin",    // collation_connection
						"utf8mb4_0900_bin",    // Database Collation
					},
				},
			},
		},
	},
}

var ProcedureShowCreate = []ScriptTest{
	{
		Name: "SHOW procedures",
		SetUpScript: []string{
			"CREATE PROCEDURE p1() COMMENT 'hi' DETERMINISTIC SELECT 6",
			"CREATE definer=`user` PROCEDURE p2() SQL SECURITY INVOKER SELECT 7",
			"CREATE PROCEDURE p21() SQL SECURITY DEFINER SELECT 8",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW CREATE PROCEDURE p1",
				Expected: []sql.Row{
					{
						"p1", // Procedure
						"",   // sql_mode
						"CREATE PROCEDURE p1() COMMENT 'hi' DETERMINISTIC SELECT 6", // Create Procedure
						"utf8mb4",          // character_set_client
						"utf8mb4_0900_bin", // collation_connection
						"utf8mb4_0900_bin", // Database Collation
					},
				},
			},
			{
				Query: "SHOW CREATE PROCEDURE p2",
				Expected: []sql.Row{
					{
						"p2", // Procedure
						"",   // sql_mode
						"CREATE definer=`user` PROCEDURE p2() SQL SECURITY INVOKER SELECT 7", // Create Procedure
						"utf8mb4",          // character_set_client
						"utf8mb4_0900_bin", // collation_connection
						"utf8mb4_0900_bin", // Database Collation
					},
				},
			},
			{
				Query: "SHOW CREATE PROCEDURE p21",
				Expected: []sql.Row{
					{
						"p21", // Procedure
						"",    // sql_mode
						"CREATE PROCEDURE p21() SQL SECURITY DEFINER SELECT 8", // Create Procedure
						"utf8mb4",          // character_set_client
						"utf8mb4_0900_bin", // collation_connection
						"utf8mb4_0900_bin", // Database Collation
					},
				},
			},
		},
	},
	{
		Name:        "SHOW non-existent procedures",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "SHOW CREATE PROCEDURE p1",
				ExpectedErr: sql.ErrStoredProcedureDoesNotExist,
			},
		},
	},
}

var ProcedureCreateInSubroutineTests = []ScriptTest{
	{
		Name: "event must not contain CREATE PROCEDURE",
		Assertions: []ScriptTestAssertion{
			{
				Query:          "CREATE EVENT foo ON SCHEDULE EVERY 1 YEAR DO CREATE PROCEDURE bar() SELECT 1;",
				ExpectedErrStr: "can't create a PROCEDURE from within another stored routine",
			},
		},
	},
	{
		Name: "trigger must not contain CREATE PROCEDURE",
		SetUpScript: []string{
			"CREATE TABLE t (pk INT PRIMARY KEY);",
		},
		Assertions: []ScriptTestAssertion{
			{
				// Skipped because MySQL errors here but we don't.
				Query:          "CREATE TRIGGER foo AFTER UPDATE ON t FOR EACH ROW BEGIN CREATE PROCEDURE bar() SELECT 1; END",
				ExpectedErrStr: "Can't create a PROCEDURE from within another stored routine",
				Skip:           true,
			},
		},
	},

	{
		Name: "table ddl statements in stored procedures",
		Assertions: []ScriptTestAssertion{
			{
				Query: "create procedure create_proc() create table t (i int primary key, j int);",
				Expected: []sql.Row{
					{types.NewOkResult(0)},
				},
			},
			{
				SkipResultCheckOnServerEngine: true,
				Query:                         "call create_proc()",
				Expected: []sql.Row{
					{types.NewOkResult(0)},
				},
			},
			{
				Query: "show create table t;",
				Expected: []sql.Row{
					{"t", "CREATE TABLE `t` (\n" +
						"  `i` int NOT NULL,\n" +
						"  `j` int,\n" +
						"  PRIMARY KEY (`i`)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
			{
				Query:          "call create_proc()",
				ExpectedErrStr: "table with name t already exists",
			},

			{
				Query: "create procedure insert_proc() insert into t values (1, 1), (2, 2), (3, 3);",
				Expected: []sql.Row{
					{types.NewOkResult(0)},
				},
			},
			{
				SkipResultCheckOnServerEngine: true,
				Query:                         "call insert_proc()",
				Expected: []sql.Row{
					{types.NewOkResult(3)},
				},
			},
			{
				Query: "select * from t",
				Expected: []sql.Row{
					{1, 1},
					{2, 2},
					{3, 3},
				},
			},
			{
				SkipResultCheckOnServerEngine: true,
				Query:                         "call insert_proc()",
				ExpectedErrStr:                "duplicate primary key given: [1]",
			},

			{
				Query: "create procedure update_proc() update t set j = 999 where i > 1;",
				Expected: []sql.Row{
					{types.NewOkResult(0)},
				},
			},
			{
				SkipResultCheckOnServerEngine: true,
				Query:                         "call update_proc()",
				Expected: []sql.Row{
					{types.OkResult{RowsAffected: 2, Info: plan.UpdateInfo{Matched: 2, Updated: 2}}},
				},
			},
			{
				Query: "select * from t",
				Expected: []sql.Row{
					{1, 1},
					{2, 999},
					{3, 999},
				},
			},
			{
				SkipResultCheckOnServerEngine: true,
				Query:                         "call update_proc()",
				Expected: []sql.Row{
					{types.OkResult{RowsAffected: 0, Info: plan.UpdateInfo{Matched: 2}}},
				},
			},

			{
				Query: "create procedure drop_proc() drop table t;",
				Expected: []sql.Row{
					{types.NewOkResult(0)},
				},
			},
			{
				SkipResultCheckOnServerEngine: true,
				Query:                         "call drop_proc()",
				Expected: []sql.Row{
					{types.NewOkResult(0)},
				},
			},
			{
				Query:    "show tables like 't'",
				Expected: []sql.Row{},
			},
			{
				Query:          "call drop_proc()",
				ExpectedErrStr: "table not found: t",
			},
		},
	},

	{
		Name: "procedure must not contain CREATE TRIGGER",
		SetUpScript: []string{
			"create table t (i int);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:          "create procedure p() create trigger trig before insert on t for each row begin select 1; end;",
				ExpectedErrStr: "can't create a TRIGGER from within another stored routine",
			},
			{
				Query:          "create procedure p() begin create trigger trig before insert on t for each row begin select 1; end; end;",
				ExpectedErrStr: "can't create a TRIGGER from within another stored routine",
			},
		},
	},
	{
		Name:        "procedure must not contain CREATE DB",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query:          "create procedure p() create database procdb;",
				ExpectedErrStr: "DBDDL in CREATE PROCEDURE not yet supported",
			},
			{
				Query:          "create procedure p() begin create database procdb; end;",
				ExpectedErrStr: "DBDDL in CREATE PROCEDURE not yet supported",
			},
		},
	},
	{
		Name:        "procedure can CREATE VIEW",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "create procedure p1() create view v as select 1;",
				Expected: []sql.Row{
					{types.NewOkResult(0)},
				},
			},
			{
				Query: "create procedure p() begin create view v as select 1; end;",
				Expected: []sql.Row{
					{types.NewOkResult(0)},
				},
			},
		},
	},
}
