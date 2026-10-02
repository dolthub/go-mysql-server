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
)

// StatisticsScriptTests contains self-contained statistics script tests.
var StatisticsScriptTests = []ScriptTest{
	{
		Name: "histogram bucket merging error for implementor buckets",
		SetUpScript: []string{
			"CREATE TABLE xy (x int primary key, y varchar(10), key(y));",
			"insert into xy select x, 'x' from (with recursive inputs(x) as (select 1 union select x+1 from inputs where x < 5000) select * from inputs) dt",
			"analyze table xy",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select (select count(*) from information_schema.statistics) > 0",
				Expected: []sql.Row{{true}},
			},
			{
				Query:    "select a.y from xy a join xy b on a.y = b.y limit 1",
				Expected: []sql.Row{{"x"}},
			},
			{
				Query:    "select y from xy where y = 'x' limit 1",
				Expected: []sql.Row{{"x"}},
			},
		},
	},
}
