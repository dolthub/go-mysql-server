// Copyright 2023 Dolthub, Inc.
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

var EventTests = []ScriptTest{
	{
		Name: "Simple CREATE EVENTs with ON SCHEDULE EVERY",
		SetUpScript: []string{
			"USE mydb;",
			"CREATE TABLE totals (num int);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "CREATE EVENT event1 ON SCHEDULE EVERY '1:2' MINUTE_SECOND DISABLE DO INSERT INTO totals VALUES (1);",
				Expected: []sql.Row{{types.OkResult{}}},
			},
			{
				Query:    "CREATE EVENT event2 ON SCHEDULE EVERY 3 DAY STARTS '2037-10-16 23:59:00 +0000 UTC' + INTERVAL 2 DAY ENDS '2037-11-16 23:59:00 +0000 UTC' + INTERVAL 1 MONTH DISABLE DO INSERT INTO totals VALUES (1000);",
				Expected: []sql.Row{{types.OkResult{}}},
			},
			{
				Query: "SHOW EVENTS LIKE 'event2';",
				Expected: []sql.Row{
					{"mydb", "event2", "", "SYSTEM", "RECURRING", nil, "3", "DAY", "2037-10-18 23:59:00 +0000 UTC", "2037-12-16 23:59:00 +0000 UTC", "DISABLE", 0, "utf8mb4", "utf8mb4_0900_bin", "utf8mb4_0900_bin"},
				},
			},
			{
				Query: "SHOW CREATE EVENT event1;",
				Expected: []sql.Row{
					// TODO: CREATE EVENT statements should not use the initial query but be generated to include STARTS timestamp set to current_timestamp of when the event was first created.
					{"event1", "", "SYSTEM", "CREATE EVENT event1 ON SCHEDULE EVERY '1:2' MINUTE_SECOND DISABLE DO INSERT INTO totals VALUES (1)", "utf8mb4", "utf8mb4_0900_bin", "utf8mb4_0900_bin"},
				},
			},
			{
				Query:          "CREATE EVENT event1 ON SCHEDULE EVERY 1 MINUTE ENDS '2006-02-10 23:59:00' DO INSERT INTO totals VALUES (1);",
				ExpectedErrStr: "ENDS is either invalid or before STARTS",
			},
		},
	},
	{
		Name: "Simple CREATE EVENTs with ON SCHEDULE AT",
		SetUpScript: []string{
			"USE mydb;",
			"CREATE TABLE totals (num int);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:                 "CREATE EVENT event1 ON SCHEDULE AT '2006-02-10 23:59:00' DISABLE DO INSERT INTO totals VALUES (100);",
				Expected:              []sql.Row{{types.OkResult{}}},
				ExpectedWarningsCount: 1,
			},
			{
				Query:    "SHOW WARNINGS;",
				Expected: []sql.Row{{"Note", 1588, "Event execution time is in the past and ON COMPLETION NOT PRESERVE is set. The event was dropped immediately after creation."}},
			},
			{
				Query:       "SHOW CREATE EVENT event1;",
				ExpectedErr: sql.ErrUnknownEvent,
			},
			{
				Query:    "CREATE EVENT event2 ON SCHEDULE AT '2038-01-16 23:59:00 +0000 UTC' + INTERVAL 1 DAY DISABLE DO INSERT INTO totals VALUES (100);",
				Expected: []sql.Row{{types.OkResult{}}},
			},
			{
				Query: "SHOW EVENTS;",
				Expected: []sql.Row{
					{"mydb", "event2", "", "SYSTEM", "ONE TIME", "2038-01-17 23:59:00 +0000 UTC", nil, nil, nil, nil, "DISABLE", 0, "utf8mb4", "utf8mb4_0900_bin", "utf8mb4_0900_bin"},
				},
			},
		},
	},
}
