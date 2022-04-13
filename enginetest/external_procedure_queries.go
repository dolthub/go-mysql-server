// Copyright 2022 Dolthub, Inc.
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

import "github.com/dolthub/go-mysql-server/sql"

var ExternalProcedureTests = []ScriptTest{
	{
		Name: "INOUT on first param, IN on second param",
		SetUpScript: []string{
			"SET @outparam = 5;",
			"CALL memory_inout_add(@outparam, 11);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT @outparam;",
				Expected: []sql.Row{{16}},
			},
		},
	},
	{
		Name: "Called from standard stored procedure",
		SetUpScript: []string{
			"CREATE PROCEDURE p1(x BIGINT) BEGIN CALL memory_inout_add(x, x); SELECT x; END;",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "CALL p1(11);",
				Expected: []sql.Row{{22}},
			},
		},
	},
	{
		Name: "Overloaded Name",
		Assertions: []ScriptTestAssertion{
			{
				Query:    "CALL memory_overloaded_mult(1);",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "CALL memory_overloaded_mult(2, 3);",
				Expected: []sql.Row{{6}},
			},
			{
				Query:    "CALL memory_overloaded_mult(4, 5, 6);",
				Expected: []sql.Row{{120}},
			},
		},
	},
	{
		Name: "Passing in all supported types",
		Assertions: []ScriptTestAssertion{
			{
				Query: "CALL memory_overloaded_type_test(1, 100, 10000, 1000000, 100000000, 3, 300," +
					"10, 1000, 100000, 10000000, 1000000000, 30, 3000);",
				Expected: []sql.Row{{1111114444}},
			},
			{
				Query: "CALL memory_overloaded_type_test(false, 'hi', 'A', '2020-02-20 12:00:00', 123.456," +
					"true, 'bye', 'B', '2022-02-02 12:00:00', 654.32);",
				Expected: []sql.Row{{`aa:false,ba:true,ab:"hi",bb:"bye",ac:[65],bc:[66],ad:2020-02-20,bd:2022-02-02,ae:123.456,be:654.32`}},
			},
		},
	},
	{
		Name: "BOOL and []BYTE INOUT conversions",
		SetUpScript: []string{
			"SET @outparam1 = 1;",
			"SET @outparam2 = 0;",
			"SET @outparam3 = 'A';",
			"SET @outparam4 = 'B';",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT @outparam1, @outparam2, @outparam3, @outparam4;",
				Expected: []sql.Row{{1, 0, "A", "B"}},
			},
			{
				Query:    "CALL memory_inout_bool_byte(@outparam1, @outparam2, @outparam3, @outparam4);",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT @outparam1, @outparam2, @outparam3, @outparam4;",
				Expected: []sql.Row{{1, 1, "A", "C"}},
			},
			{
				Query:    "CALL memory_inout_bool_byte(@outparam1, @outparam2, @outparam3, @outparam4);",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT @outparam1, @outparam2, @outparam3, @outparam4;",
				Expected: []sql.Row{{1, 0, "A", "D"}},
			},
		},
	},
	{
		Name: "Errors returned",
		Assertions: []ScriptTestAssertion{
			{
				Query:       "CALL memory_error_table_not_found();",
				ExpectedErr: sql.ErrTableNotFound,
			},
		},
	},
}
