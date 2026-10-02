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

// DropTableScriptTests contains self-contained drop table script tests.
var DropTableScriptTests = []ScriptTest{
	{
		Name:    "drop table if exists on unknown table shows warning",
		Dialect: "mysql",
		Assertions: []ScriptTestAssertion{
			{
				Query:                           "DROP TABLE IF EXISTS non_existent_table;",
				ExpectedWarning:                 1051,
				ExpectedWarningsCount:           1,
				ExpectedWarningMessageSubstring: "Unknown table 'non_existent_table'",
				SkipResultsCheck:                true,
			},
		},
	},
}
