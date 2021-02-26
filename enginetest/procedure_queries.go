// Copyright 2020-2021 Dolthub, Inc.
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

import (
	"time"

	"github.com/dolthub/go-mysql-server/sql"
)

var ProcedureTests = []ScriptTest{
	// CREATE PROCEDURE & CALL
	{
		Name: "Simple SELECT",
		SetUpScript: []string{
			"CREATE PROCEDURE testabc(x DOUBLE, y DOUBLE) SELECT x*y",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "CALL testabc(2, 3)",
				Expected: []sql.Row{
					{
						float64(6),
					},
				},
			},
			{
				Query: "CALL testabc(9, 9.5)",
				Expected: []sql.Row{
					{
						float64(85.5),
					},
				},
			},
		},
	},
	{
		Name: "OUT param with SET",
		SetUpScript: []string{
			"SET @outparam = 5",
			"CREATE PROCEDURE testabc(OUT x BIGINT) SET x = 9",
			"CALL testabc(@outparam)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT @outparam",
				Expected: []sql.Row{
					{
						int64(9),
					},
				},
			},
		},
	},
	{
		Name: "OUT param without SET",
		SetUpScript: []string{
			"SET @outparam = 5",
			"CREATE PROCEDURE testabc(OUT x BIGINT) SELECT x",
			"CALL testabc(@outparam)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT @outparam",
				Expected: []sql.Row{
					{
						nil,
					},
				},
			},
		},
	},
	{
		Name: "INOUT param with SET",
		SetUpScript: []string{
			"SET @outparam = 5",
			"CREATE PROCEDURE testabc(INOUT x BIGINT) BEGIN SET x = x + 1; SET x = x + 3; END;",
			"CALL testabc(@outparam)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT @outparam",
				Expected: []sql.Row{
					{
						int64(9),
					},
				},
			},
		},
	},
	{
		Name: "INOUT param without SET",
		SetUpScript: []string{
			"SET @outparam = 5",
			"CREATE PROCEDURE testabc(INOUT x BIGINT) SELECT x",
			"CALL testabc(@outparam)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT @outparam",
				Expected: []sql.Row{
					{
						int64(5),
					},
				},
			},
		},
	},
	{
		Name: "Nested CALL with INOUT param",
		SetUpScript: []string{
			"SET @outparam = 5",
			"CREATE PROCEDURE p3(INOUT z INT) BEGIN SET z = z * 111; END;",
			"CREATE PROCEDURE p2(INOUT y DOUBLE) BEGIN SET y = y + 4; CALL p3(y); END;",
			"CREATE PROCEDURE p1(INOUT x BIGINT) BEGIN SET x = 3; CALL p2(x); END;",
			"CALL p1(@outparam)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT @outparam",
				Expected: []sql.Row{
					{
						int64(777),
					},
				},
			},
		},
	},
	{
		Name: "IF/ELSE with OUT param",
		SetUpScript: []string{
			"SET @outparam = ''",
			`CREATE PROCEDURE p1(OUT s VARCHAR(200), n DOUBLE, m DOUBLE)
BEGIN
	SET s = '';
	IF n = m THEN SET s = 'equals';
	ELSE
		IF n > m THEN SET s = 'greater';
		ELSE SET s = 'less';
		END IF;
		SET s = CONCAT('is ', s, ' than');
	END IF;
	SET s = CONCAT(n, ' ', s, ' ', m, '.');
	SELECT s;
END;`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "CALL p1(@outparam, 1, 2)",
				Expected: []sql.Row{
					{
						"1 is less than 2.",
					},
				},
			},
			{
				Query: "SELECT @outparam",
				Expected: []sql.Row{
					{
						"1 is less than 2.",
					},
				},
			},
			{
				Query: "CALL p1(@outparam, 7, 4)",
				Expected: []sql.Row{
					{
						"7 is greater than 4.",
					},
				},
			},
			{
				Query: "SELECT @outparam",
				Expected: []sql.Row{
					{
						"7 is greater than 4.",
					},
				},
			},
			{
				Query: "CALL p1(@outparam, 5, 5)",
				Expected: []sql.Row{
					{
						"5 equals 5.",
					},
				},
			},
			{
				Query: "SELECT @outparam",
				Expected: []sql.Row{
					{
						"5 equals 5.",
					},
				},
			},
		},
	},
	// DROP PROCEDURE
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
				Expected: []sql.Row{},
			},
			{
				Query:       "CALL p1",
				ExpectedErr: sql.ErrStoredProcedureDoesNotExist,
			},
			{
				Query:    "DROP PROCEDURE IF EXISTS p2",
				Expected: []sql.Row{},
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
				Expected: []sql.Row{},
			},
		},
	},
	// SHOW PROCEDURE STATUS
	{
		Name: "SHOW procedures",
		SetUpScript: []string{
			"CREATE PROCEDURE p1() COMMENT 'hi' DETERMINISTIC SELECT 6",
			"CREATE definer=user PROCEDURE p2() SQL SECURITY INVOKER SELECT 7",
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
						"utf8mb4_0900_ai_ci",  // collation_connection
						"utf8mb4_0900_ai_ci",  // Database Collation
					},
					{
						"mydb",                // Db
						"p2",                  // Name
						"PROCEDURE",           // Type
						"user",                // Definer
						time.Unix(0, 0).UTC(), // Modified
						time.Unix(0, 0).UTC(), // Created
						"INVOKER",             // Security_type
						"",                    // Comment
						"utf8mb4",             // character_set_client
						"utf8mb4_0900_ai_ci",  // collation_connection
						"utf8mb4_0900_ai_ci",  // Database Collation
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
						"utf8mb4_0900_ai_ci",  // collation_connection
						"utf8mb4_0900_ai_ci",  // Database Collation
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
						"user",                // Definer
						time.Unix(0, 0).UTC(), // Modified
						time.Unix(0, 0).UTC(), // Created
						"INVOKER",             // Security_type
						"",                    // Comment
						"utf8mb4",             // character_set_client
						"utf8mb4_0900_ai_ci",  // collation_connection
						"utf8mb4_0900_ai_ci",  // Database Collation
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
						"utf8mb4_0900_ai_ci",  // collation_connection
						"utf8mb4_0900_ai_ci",  // Database Collation
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
						"utf8mb4_0900_ai_ci",  // collation_connection
						"utf8mb4_0900_ai_ci",  // Database Collation
					},
					{
						"mydb",                // Db
						"p2",                  // Name
						"PROCEDURE",           // Type
						"user",                // Definer
						time.Unix(0, 0).UTC(), // Modified
						time.Unix(0, 0).UTC(), // Created
						"INVOKER",             // Security_type
						"",                    // Comment
						"utf8mb4",             // character_set_client
						"utf8mb4_0900_ai_ci",  // collation_connection
						"utf8mb4_0900_ai_ci",  // Database Collation
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
						"utf8mb4_0900_ai_ci",  // collation_connection
						"utf8mb4_0900_ai_ci",  // Database Collation
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
						"utf8mb4_0900_ai_ci",  // collation_connection
						"utf8mb4_0900_ai_ci",  // Database Collation
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
						"utf8mb4_0900_ai_ci",  // collation_connection
						"utf8mb4_0900_ai_ci",  // Database Collation
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
						"user",                // Definer
						time.Unix(0, 0).UTC(), // Modified
						time.Unix(0, 0).UTC(), // Created
						"INVOKER",             // Security_type
						"",                    // Comment
						"utf8mb4",             // character_set_client
						"utf8mb4_0900_ai_ci",  // collation_connection
						"utf8mb4_0900_ai_ci",  // Database Collation
					},
				},
			},
		},
	},
}
