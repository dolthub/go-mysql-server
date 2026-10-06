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

package queries

import (
	"github.com/dolthub/go-mysql-server/sql"
)

var OrderByGroupByScriptTests = []ScriptTest{
	{
		// https://github.com/dolthub/dolt/issues/9605
		Name: "Order by wrapped by parentheses",
		SetUpScript: []string{
			"create table t(i int, j int)",
			"insert into t values(2,4),(0,7),(9,10),(4,3)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "with cte(i) as (select i from t) select * from cte order by (i)",
				Expected: []sql.Row{{0}, {2}, {4}, {9}},
			},
			{
				Query:    "with cte(i) as (select i from t) select * from cte order by (((i)))",
				Expected: []sql.Row{{0}, {2}, {4}, {9}},
			},
			{
				Query:    "select * from t order by (i * 10 + j)",
				Expected: []sql.Row{{0, 7}, {2, 4}, {4, 3}, {9, 10}},
			},
		},
	},
}
