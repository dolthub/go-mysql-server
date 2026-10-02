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
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// ForeignKeyResolutionScriptTests contains self-contained foreign key resolution script tests.
var ForeignKeyResolutionScriptTests = []ScriptTest{
	{
		Name:    "resolve foreign key on indexed update",
		Dialect: "mysql", // no way to disable foreign keys in doltgres yet
		SetUpScript: []string{
			"set foreign_key_checks=0;",
			"create table parent (i int primary key);",
			"create table child (i int primary key, foreign key (i) references parent(i));",
			"set foreign_key_checks=1;",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "update child set i = 1 where i = 1;",
				Expected: []sql.Row{
					{types.OkResult{RowsAffected: 0, Info: plan.UpdateInfo{Matched: 0, Updated: 0}}},
				},
			},
		},
	},
}
