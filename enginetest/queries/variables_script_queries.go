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

// VariablesScriptTests contains self-contained variables script tests.
var VariablesScriptTests = []ScriptTest{
	{
		Name:    "Test cases on select into statement",
		Dialect: "mysql",
		SetUpScript: []string{
			"SELECT * FROM (VALUES ROW(22,44,88)) AS t INTO @x,@y,@z",
			"CREATE TABLE tab1 (id int primary key, v1 int)",
			"INSERT INTO tab1 VALUES (1, 1), (2, 3), (3, 6)",
			"SELECT id FROM tab1 ORDER BY id DESC LIMIT 1 INTO @myVar",
			"CREATE TABLE tab2 (i2 int primary key, s text)",
			"INSERT INTO tab2 VALUES (1, 'b'), (2, 'm'), (3, 'g')",
			"SELECT m.id, t.s FROM tab1 m JOIN tab2 t on m.id = t.i2 ORDER BY t.s DESC LIMIT 1 INTO @myId, @myText",
			// TODO: union statement does not handle order by and limit clauses
			// "SELECT id FROM tab1 UNION select s FROM tab2 LIMIT 1 INTO @myUnion",
			"SELECT id FROM tab1 WHERE id > 3 UNION select s FROM tab2 WHERE s < 'f' INTO @mustSingleVar",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:                         `SELECT 1 INTO @abc`,
				SkipResultCheckOnServerEngine: true,
				Expected:                      []sql.Row{{types.NewOkResult(1)}},
				ExpectedColumns:               nil,
			},
			{
				Query:    `SELECT @abc`,
				Expected: []sql.Row{{int8(1)}},
			},
			{
				Query:    `SELECT @z, @x, @y`,
				Expected: []sql.Row{{88, 22, 44}},
			},
			{
				Query:    `SELECT @myVar, @mustSingleVar`,
				Expected: []sql.Row{{3, "b"}},
			},
			{
				Query:    `SELECT @myId, @myText, @myUnion`,
				Expected: []sql.Row{{2, "m", nil}},
			},
			{
				Query:       `SELECT id FROM tab1 ORDER BY id DESC INTO @myvar`,
				ExpectedErr: sql.ErrMoreThanOneRow,
			},
			{
				Query:       `SELECT id INTO DUMPFILE 'baddump.out' FROM tab1 ORDER BY id DESC LIMIT 15`,
				ExpectedErr: sql.ErrMoreThanOneRow,
			},
			{
				Query:       `select 1, 2, 3 into @my1, @my2`,
				ExpectedErr: sql.ErrColumnNumberDoesNotMatch,
			},
			{
				Query:          `SELECT id, v1 INTO @myFirstVar FROM tab1 ORDER BY id DESC LIMIT 1 INTO @mySecondVar`,
				ExpectedErrStr: "Multiple INTO clauses in one query block at position 84 near '@mySecondVar'",
			},
			{
				Query:          `SELECT id FROM tab1 WHERE id > 3 UNION select s INTO @mustSingleVar FROM tab2 WHERE s < 'f' ORDER BY s DESC`,
				ExpectedErrStr: "INTO clause is not allowed at position 98 near 'ORDER'",
			},
		},
	},
	{
		Name: "validate_password_strength and validate_password.length",
		SetUpScript: []string{
			"set @orig = @@global.validate_password.length",
			"set @@global.validate_password.length = 0",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select validate_password_strength('')",
				Expected: []sql.Row{
					{0},
				},
			},
			{
				Query: "select validate_password_strength('123')",
				Expected: []sql.Row{
					{0},
				},
			},
			{
				Query: "select validate_password_strength('1234')",
				Expected: []sql.Row{
					{50},
				},
			},
			{
				SkipResultsCheck: true,
				Query:            "set @@global.validate_password.length = 1000",
			},
			{
				Query: "select validate_password_strength('ABCabc123!@!#')",
				Expected: []sql.Row{
					{25},
				},
			},
			{
				Query:          "set @@session.validate_password.length = 123",
				ExpectedErrStr: "Variable 'validate_password.length' is a GLOBAL variable and should be set with SET GLOBAL",
			},
			{
				SkipResultsCheck: true,
				Query:            "set @@global.validate_password.length = @orig",
			},
		},
	},
	{
		Name: "validate_password_strength and validate_password.number_count",
		SetUpScript: []string{
			"set @orig = @@global.validate_password.number_count",
			"set @@global.validate_password.number_count = 0",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select validate_password_strength('ABCabc!@#')",
				Expected: []sql.Row{
					{100},
				},
			},
			{
				SkipResultsCheck: true,
				Query:            "set @@global.validate_password.number_count = 1000",
			},
			{
				Query: "select validate_password_strength('ABCabc!!!!123456789012345678901234567890')",
				Expected: []sql.Row{
					{50},
				},
			},
			{
				Query:          "set @@session.validate_password.number_count = 123",
				ExpectedErrStr: "Variable 'validate_password.number_count' is a GLOBAL variable and should be set with SET GLOBAL",
			},
			{
				SkipResultsCheck: true,
				Query:            "set @@global.validate_password.number_count = @orig",
			},
		},
	},
	{
		Name: "validate_password_strength and validate_password.mixed_case_count",
		SetUpScript: []string{
			"set @orig = @@global.validate_password.mixed_case_count",
			"set @@global.validate_password.mixed_case_count = 0",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select validate_password_strength('abcabc!@#123')",
				Expected: []sql.Row{
					{100},
				},
			},
			{
				Query: "select validate_password_strength('ABCABC!@#123')",
				Expected: []sql.Row{
					{100},
				},
			},
			{
				SkipResultsCheck: true,
				Query:            "set @@global.validate_password.mixed_case_count = 1000",
			},
			{
				Query: "select validate_password_strength('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ123456!?!?!?')",
				Expected: []sql.Row{
					{50},
				},
			},
			{
				Query:          "set @@session.validate_password.mixed_case_count = 123",
				ExpectedErrStr: "Variable 'validate_password.mixed_case_count' is a GLOBAL variable and should be set with SET GLOBAL",
			},
			{
				SkipResultsCheck: true,
				Query:            "set @@global.validate_password.mixed_case_count = @orig",
			},
		},
	},
	{
		Name: "validate_password_strength and validate_password.special_char_count",
		SetUpScript: []string{
			"set @orig = @@global.validate_password.special_char_count",
			"set @@global.validate_password.special_char_count = 0",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select validate_password_strength('abcABC123')",
				Expected: []sql.Row{
					{100},
				},
			},
			{
				SkipResultsCheck: true,
				Query:            "set @@global.validate_password.special_char_count = 1000",
			},
			{
				Query: "select validate_password_strength('abcABC123!@#$%^&*()                            ')",
				Expected: []sql.Row{
					{50},
				},
			},
			{
				Query:          "set @@session.validate_password.special_char_count = 123",
				ExpectedErrStr: "Variable 'validate_password.special_char_count' is a GLOBAL variable and should be set with SET GLOBAL",
			},
			{
				SkipResultsCheck: true,
				Query:            "set @@global.validate_password.special_char_count = @orig",
			},
		},
	},
	{
		Name:        "MySQL default and strict SQL_MODE behavior",
		Dialect:     "mysql",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				// Disabling `NO_ZERO_IN_DATE` throws additional warning
				Query: "set @@sql_mode = '" +
					"STRICT_TRANS_TABLES," +
					"NO_ZERO_DATE," +
					"ERROR_FOR_DIVISION_BY_ZERO'",
				Expected: []sql.Row{
					{types.OkResult{}},
				},
				ExpectedWarningsCount:           2,
				ExpectedWarning:                 3135,
				ExpectedWarningMessageSubstring: "Removing NO_ZERO_IN_DATE mode is not supported",
			},
			{
				Query: "select @@sql_mode",
				Expected: []sql.Row{
					{"STRICT_TRANS_TABLES,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO"},
				},
			},
			{
				// Disabling `STRICT_TRANS_TABLES` throws strict mode warning
				Query: "set @@sql_mode = '" +
					"NO_ZERO_IN_DATE," +
					"NO_ZERO_DATE," +
					"ERROR_FOR_DIVISION_BY_ZERO'",
				Expected: []sql.Row{
					{types.OkResult{}},
				},
				ExpectedWarningsCount: 1,
				ExpectedWarning:       3135,
				ExpectedWarningMessageSubstring: "'NO_ZERO_DATE', 'NO_ZERO_IN_DATE' and 'ERROR_FOR_DIVISION_BY_ZERO' " +
					"sql modes should be used with strict mode. " +
					"They will be merged with strict mode in a future release",
			},
			{
				Query: "select @@sql_mode",
				Expected: []sql.Row{
					{"NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO"},
				},
			},
			{
				// Disabling `NO_ZERO_DATE` throws strict mode warning
				Query: "set @@sql_mode = '" +
					"STRICT_TRANS_TABLES," +
					"NO_ZERO_IN_DATE," +
					"ERROR_FOR_DIVISION_BY_ZERO'",
				Expected: []sql.Row{
					{types.OkResult{}},
				},
				ExpectedWarningsCount: 1,
				ExpectedWarning:       3135,
				ExpectedWarningMessageSubstring: "'NO_ZERO_DATE', 'NO_ZERO_IN_DATE' and 'ERROR_FOR_DIVISION_BY_ZERO' " +
					"sql modes should be used with strict mode. " +
					"They will be merged with strict mode in a future release",
			},
			{
				Query: "select @@sql_mode",
				Expected: []sql.Row{
					{"STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,ERROR_FOR_DIVISION_BY_ZERO"},
				},
			},
			{
				// Disabling `ERROR_FOR_DIVISION_BY_ZERO` throws strict mode warning
				Query: "set @@sql_mode = '" +
					"STRICT_TRANS_TABLES," +
					"NO_ZERO_IN_DATE," +
					"NO_ZERO_DATE'",
				Expected: []sql.Row{
					{types.OkResult{}},
				},
				ExpectedWarningsCount: 1,
				ExpectedWarning:       3135,
				ExpectedWarningMessageSubstring: "'NO_ZERO_DATE', 'NO_ZERO_IN_DATE' and 'ERROR_FOR_DIVISION_BY_ZERO' " +
					"sql modes should be used with strict mode. " +
					"They will be merged with strict mode in a future release",
			},
			{
				Query: "select @@sql_mode",
				Expected: []sql.Row{
					{"STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE"},
				},
			},
		},
	},
}
