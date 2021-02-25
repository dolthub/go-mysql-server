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

import "github.com/dolthub/go-mysql-server/sql"

var ProcedureTests = []ScriptTest{
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
}
