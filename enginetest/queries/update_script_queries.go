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

// UpdateScriptTests contains self-contained update script tests.
var UpdateScriptTests = []ScriptTest{
	{
		Name: "empty table update",
		SetUpScript: []string{
			"create table t (i int primary key)",
			"insert into t values (1), (2), (3)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "update t set i = 0 where false",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 0, InsertID: 0, Info: plan.UpdateInfo{Matched: 0}}}},
			},
			{
				Query: "select * from t",
				Expected: []sql.Row{
					{1},
					{2},
					{3},
				},
			},
		},
	},
}
