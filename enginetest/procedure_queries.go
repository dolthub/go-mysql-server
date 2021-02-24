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
		Name: "simple select",
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
}
