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

// EventTests tests any EVENT related behavior. Events have at least one timestamp value (AT/STARTS/ENDS), so to test
// SHOW EVENTS and SHOW CREATE EVENTS statements, some tests have those timestamps defined in 2037.
var EventTests = []ScriptTest{
	{
		Name: "EVENTs with ON SCHEDULE EVERY",
		SetUpScript: []string{
			"USE mydb;",
			"CREATE TABLE totals (num int);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "CREATE EVENT event_with_starts_and_ends ON SCHEDULE EVERY '1:2' MINUTE_SECOND STARTS CURRENT_TIMESTAMP + INTERVAL 1 HOUR ENDS CURRENT_TIMESTAMP + INTERVAL 1 DAY DISABLE DO INSERT INTO totals VALUES (1);",
				Expected: []sql.Row{{types.OkResult{}}},
			},
			{
				Query:    "CREATE EVENT event_with_starts_only ON SCHEDULE EVERY '1:2' MINUTE_SECOND STARTS CURRENT_TIMESTAMP + INTERVAL 1 HOUR DISABLE DO INSERT INTO totals VALUES (1);",
				Expected: []sql.Row{{types.OkResult{}}},
			},
			{
				Query:    "CREATE EVENT event_with_ends_only ON SCHEDULE EVERY '1:2' MINUTE_SECOND ENDS CURRENT_TIMESTAMP + INTERVAL 1 DAY DISABLE DO INSERT INTO totals VALUES (1);",
				Expected: []sql.Row{{types.OkResult{}}},
			},
			{
				Query:    "CREATE EVENT event_without_starts_and_ends ON SCHEDULE EVERY '1:2' MINUTE_SECOND DISABLE DO INSERT INTO totals VALUES (1);",
				Expected: []sql.Row{{types.OkResult{}}},
			},
			{
				Query:    "CREATE EVENT event2 ON SCHEDULE EVERY 3 DAY STARTS '2037-10-16 23:59:00' + INTERVAL 2 DAY ENDS '2037-11-16 23:59:00' + INTERVAL 1 MONTH DO INSERT INTO totals VALUES (1000);",
				Expected: []sql.Row{{types.OkResult{}}},
			},
			{
				Query:    "SHOW EVENTS LIKE 'event2';",
				Expected: []sql.Row{{"mydb", "event2", "`root`@`localhost`", "SYSTEM", "RECURRING", nil, "3", "DAY", "2037-10-18 23:59:00", "2037-12-16 23:59:00", "ENABLED", 0, "utf8mb4", "utf8mb4_0900_bin", "utf8mb4_0900_bin"}},
			},
			{
				Query: "SHOW CREATE EVENT event2;",
				Expected: []sql.Row{
					{"event2", "", "SYSTEM", "CREATE DEFINER = `root`@`localhost` EVENT `event2` ON SCHEDULE EVERY 3 DAY STARTS '2037-10-18 23:59:00' ENDS '2037-12-16 23:59:00' ON COMPLETION NOT PRESERVE ENABLE DO INSERT INTO totals VALUES (1000)", "utf8mb4", "utf8mb4_0900_bin", "utf8mb4_0900_bin"},
				},
			},
		},
	},
	{
		Name: "EVENTs with ON SCHEDULE AT",
		SetUpScript: []string{
			"USE mydb;",
			"CREATE TABLE totals (num int);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "CREATE EVENT event2 ON SCHEDULE AT '2038-01-16 23:59:00 +0000 UTC' + INTERVAL 1 DAY ON COMPLETION PRESERVE DISABLE DO INSERT INTO totals VALUES (100);",
				Expected: []sql.Row{{types.OkResult{}}},
			},
			{
				Query:    "SHOW EVENTS;",
				Expected: []sql.Row{{"mydb", "event2", "`root`@`localhost`", "SYSTEM", "ONE TIME", "2038-01-17 23:59:00", nil, nil, nil, nil, "DISABLED", 0, "utf8mb4", "utf8mb4_0900_bin", "utf8mb4_0900_bin"}},
			},
		},
	},
	{
		Name: "DROP EVENTs",
		SetUpScript: []string{
			"USE mydb;",
			"CREATE TABLE totals (num int);",
			"CREATE EVENT event1 ON SCHEDULE AT '2038-01-15 23:59:00 +0000 UTC' + INTERVAL 2 DAY DISABLE DO INSERT INTO totals VALUES (100);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SHOW EVENTS;",
				Expected: []sql.Row{{"mydb", "event1", "`root`@`localhost`", "SYSTEM", "ONE TIME", "2038-01-17 23:59:00", nil, nil, nil, nil, "DISABLED", 0, "utf8mb4", "utf8mb4_0900_bin", "utf8mb4_0900_bin"}},
			},
			{
				Query:    "DROP EVENT event1",
				Expected: []sql.Row{{types.OkResult{}}},
			},
			{
				Query:    "SHOW EVENTS;",
				Expected: []sql.Row{},
			},
		},
	},
	{
		Name: "invalid events actions",
		SetUpScript: []string{
			"USE mydb;",
			"CREATE TABLE totals (num int);",
			"CREATE EVENT my_event1 ON SCHEDULE EVERY '1:2' MINUTE_SECOND DISABLE DO INSERT INTO totals VALUES (1);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "CREATE EVENT my_event1 ON SCHEDULE EVERY '1:2' MINUTE_SECOND DISABLE DO INSERT INTO totals VALUES (1);",
				ExpectedErr: sql.ErrEventAlreadyExists,
			},
			{
				Query:                 "CREATE EVENT IF NOT EXISTS my_event1 ON SCHEDULE EVERY '1:2' MINUTE_SECOND DISABLE DO INSERT INTO totals VALUES (1);",
				Expected:              []sql.Row{{types.OkResult{}}},
				ExpectedWarningsCount: 1,
			},
			{
				Query:    "SHOW WARNINGS;",
				Expected: []sql.Row{{"Note", 1537, "Event 'my_event1' already exists"}},
			},
			{
				Query:          "CREATE EVENT ends_before_starts ON SCHEDULE EVERY 1 MINUTE ENDS '2006-02-10 23:59:00' DO INSERT INTO totals VALUES (1);",
				ExpectedErrStr: "ENDS is either invalid or before STARTS",
			},
			{
				Query:       "SHOW CREATE EVENT non_existent_event;",
				ExpectedErr: sql.ErrUnknownEvent,
			},
			{
				Query:       "DROP EVENT non_existent_event",
				ExpectedErr: sql.ErrEventDoesNotExist,
			},
			{
				Query:                 "DROP EVENT IF EXISTS non_existent_event",
				Expected:              []sql.Row{{types.OkResult{}}},
				ExpectedWarningsCount: 1,
			},
			{
				Query:    "SHOW WARNINGS;",
				Expected: []sql.Row{{"Note", 1305, "Event non_existent_event does not exist"}},
			},
			{
				Query:                 "CREATE EVENT past_event1 ON SCHEDULE AT '2006-02-10 23:59:00' DISABLE DO INSERT INTO totals VALUES (100);",
				Expected:              []sql.Row{{types.OkResult{}}},
				ExpectedWarningsCount: 1,
			},
			{
				Query:    "SHOW WARNINGS;",
				Expected: []sql.Row{{"Note", 1588, "Event execution time is in the past and ON COMPLETION NOT PRESERVE is set. The event was dropped immediately after creation."}},
			},
			{
				Query:    "SHOW EVENTS LIKE 'past_event1';",
				Expected: []sql.Row{},
			},
			{
				Query:                 "CREATE EVENT past_event2 ON SCHEDULE AT '2006-02-10 23:59:00' ON COMPLETION PRESERVE DO INSERT INTO totals VALUES (100);",
				Expected:              []sql.Row{{types.OkResult{}}},
				ExpectedWarningsCount: 1,
			},
			{
				Query:    "SHOW WARNINGS;",
				Expected: []sql.Row{{"Note", 1544, "Event execution time is in the past. Event has been disabled"}},
			},
			{
				Query:    "SHOW EVENTS LIKE 'past_event2';",
				Expected: []sql.Row{{"mydb", "past_event2", "`root`@`localhost`", "SYSTEM", "ONE TIME", "2006-02-10 23:59:00", nil, nil, nil, nil, "DISABLED", 0, "utf8mb4", "utf8mb4_0900_bin", "utf8mb4_0900_bin"}},
			},
		},
	},
}
