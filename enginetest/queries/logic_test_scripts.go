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

package queries

import (
	"github.com/dolthub/go-mysql-server/sql"
)

// SQLLogicJoinTests is a list of all the logic tests that are run against the sql engine.
var SQLLogicJoinTests = []ScriptTest{
	{
		Name: "Test cross join",
		SetUpScript: []string{
			"CREATE TABLE onecolumn (x INT);",
			"INSERT INTO onecolumn(x) VALUES (44), (null), (42);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM onecolumn AS a CROSS JOIN onecolumn AS b;",
				Expected: []sql.Row{
					{42, 44},
					{nil, 44},
					{44, 44},
					{42, nil},
					{nil, nil},
					{44, nil},
					{42, 42},
					{nil, 42},
					{44, 42},
				},
			},
		},
	},
}