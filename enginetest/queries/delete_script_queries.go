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

// DeleteScriptTests contains self-contained delete script tests.
var DeleteScriptTests = []ScriptTest{
	{
		Name: "delete with in clause",
		SetUpScript: []string{
			"create table a (x int primary key)",
			"insert into a values (1), (3), (5)",
			"delete from a where x in (1, 3)",
		},
		Query: "select x from a order by 1",
		Expected: []sql.Row{
			{5},
		},
	},
	{
		Name: "delete from table with misordered pks",
		SetUpScript: []string{
			"create table a (x int, y int, z int, primary key (z,x))",
			"insert into a values (0,1,2), (3,4,5)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT count(*) FROM a where x = 0",
				Expected: []sql.Row{
					{1},
				},
			},
			{
				Query:    "delete from a where x = 0",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "SELECT * FROM a where x = 0",
				Expected: []sql.Row{},
			},
		},
	},
}
