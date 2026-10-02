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
	"time"

	"github.com/dolthub/go-mysql-server/sql"
)

// JoinsAndSubqueriesScriptTests contains self-contained joins and subqueries script tests.
var JoinsAndSubqueriesScriptTests = []ScriptTest{
	{
		Name: "scalar subquery aggregate is scoped independently of outer aggregate",
		SetUpScript: []string{
			"CREATE TABLE subquery_aggregate_scope (id INT PRIMARY KEY, d DATETIME, tag VARCHAR(10));",
			"INSERT INTO subquery_aggregate_scope VALUES (1, '2020-01-01 00:00:00', 'keep'), (2, '2021-01-01 00:00:00', 'skip'), (3, '2030-01-01 00:00:00', 'skip');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT MAX(d), (SELECT MAX(d) FROM subquery_aggregate_scope WHERE tag = 'keep') FROM subquery_aggregate_scope;",
				Expected: []sql.Row{{time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)}},
			},
			{
				Query:    "SELECT SUM(id), (SELECT SUM(id) FROM subquery_aggregate_scope WHERE tag = 'keep') FROM subquery_aggregate_scope;",
				Expected: []sql.Row{{float64(6), float64(1)}},
			},
			{
				Query:    "SELECT COUNT(id), (SELECT COUNT(id) FROM subquery_aggregate_scope WHERE tag = 'keep') FROM subquery_aggregate_scope;",
				Expected: []sql.Row{{int64(3), int64(1)}},
			},
			{
				Query:    "SELECT AVG(id), (SELECT AVG(id) FROM subquery_aggregate_scope WHERE tag = 'keep') FROM subquery_aggregate_scope;",
				Expected: []sql.Row{{float64(2), float64(1)}},
			},
			{
				Query:    "SELECT MAX(id), (SELECT MAX(id) FROM subquery_aggregate_scope WHERE tag = 'keep') FROM subquery_aggregate_scope;",
				Expected: []sql.Row{{3, 1}},
			},
			{
				Query:    "SELECT MIN(id), (SELECT MIN(id) FROM subquery_aggregate_scope WHERE tag = 'skip') FROM subquery_aggregate_scope;",
				Expected: []sql.Row{{1, 2}},
			},
			{
				Query:    "SELECT MIN(d), (SELECT MIN(d) FROM subquery_aggregate_scope WHERE tag = 'skip') FROM subquery_aggregate_scope;",
				Expected: []sql.Row{{time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)}},
			},
			{
				Query:    "SELECT MIN(d) AS min_d, (SELECT MIN(d) AS min_d FROM subquery_aggregate_scope WHERE tag = 'skip') FROM subquery_aggregate_scope;",
				Expected: []sql.Row{{time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)}},
			},
		},
	},
}
