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
	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// ServerEventTest is used to define a test on the user and privilege systems. These tests always have the root
// account available, and the root account is used with any queries in the SetUpScript.
type ServerEventTest struct {
	Name        string
	SetUpScript []string
	Assertions  []ServerEvenTestAssertion
}

// ServerEvenTestAssertion is within a ServerEventTest to assert functionality.
type ServerEvenTestAssertion struct {
	Query                           string
	Expected                        []sql.Row
	ExpectedErr                     *errors.Kind
	ExpectedErrStr                  string
	ExpectedWarning                 int
	ExpectedWarningsCount           int
	ExpectedWarningMessageSubstring string
}

// EventTests tests any EVENT related behavior. Events have at least one timestamp value (AT/STARTS/ENDS), so to test
// SHOW EVENTS and SHOW CREATE EVENTS statements, some tests have those timestamps defined in 2037.
var EventTests = []ServerEventTest{
	{
		Name: "Simple event definition with INSERT",
		SetUpScript: []string{
			"USE mydb;",
			"CREATE TABLE totals (num int);",
			"CREATE EVENT event1 ON SCHEDULE EVERY 5 SECOND DO INSERT INTO totals VALUES (1);",
			"SELECT SLEEP(2);",
		},
		Assertions: []ServerEvenTestAssertion{
			{
				Query:    "SELECT COUNT(*) FROM totals/* 1 */;",
				Expected: []sql.Row{{1}},
			},
			{
				Query: "SELECT SLEEP(7);",
			},
			{
				Query:    "SELECT COUNT(*) FROM totals/* 2 */;",
				Expected: []sql.Row{{2}},
			},
		},
	},
	{
		Name: "EVENTs with ON SCHEDULE EVERY",
		SetUpScript: []string{
			"USE mydb;",
			"CREATE TABLE totals (num int);",
		},
		Assertions: []ServerEvenTestAssertion{
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
		Assertions: []ServerEvenTestAssertion{
			{
				Query:    "CREATE EVENT event2 ON SCHEDULE AT '38-01-16 12:2:3.' + INTERVAL 1 DAY ON COMPLETION PRESERVE DISABLE DO INSERT INTO totals VALUES (100);",
				Expected: []sql.Row{{types.OkResult{}}},
			},
			{
				Query:    "SHOW EVENTS;",
				Expected: []sql.Row{{"mydb", "event2", "`root`@`localhost`", "SYSTEM", "ONE TIME", "2038-01-17 12:02:03", nil, nil, nil, nil, "DISABLED", 0, "utf8mb4", "utf8mb4_0900_bin", "utf8mb4_0900_bin"}},
			},
		},
	},
	{
		Name: "DROP EVENTs",
		SetUpScript: []string{
			"USE mydb;",
			"CREATE TABLE totals (num int);",
			"CREATE EVENT event1 ON SCHEDULE AT '2038-01-15 23:59:00' + INTERVAL 2 DAY DISABLE DO INSERT INTO totals VALUES (100);",
		},
		Assertions: []ServerEvenTestAssertion{
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
		Assertions: []ServerEvenTestAssertion{
			{
				Query:       "CREATE EVENT my_event1 ON SCHEDULE EVERY '1:2' MINUTE_SECOND DISABLE DO INSERT INTO totals VALUES (1);",
				ExpectedErr: sql.ErrEventAlreadyExists,
			},
			{
				Query:       "CREATE EVENT mY_EVENt1 ON SCHEDULE EVERY '1:2' HOUR_MINUTE DISABLE DO INSERT INTO totals VALUES (2);",
				ExpectedErr: sql.ErrEventAlreadyExists,
			},
			{
				Query:                           "CREATE EVENT IF NOT EXISTS my_event1 ON SCHEDULE EVERY '1:2' MINUTE_SECOND DISABLE DO INSERT INTO totals VALUES (1);",
				Expected:                        []sql.Row{{types.OkResult{}}},
				ExpectedWarning:                 1537,
				ExpectedWarningsCount:           1,
				ExpectedWarningMessageSubstring: "Event 'my_event1' already exists",
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
				Query:                           "DROP EVENT IF EXISTS non_existent_event",
				Expected:                        []sql.Row{{types.OkResult{}}},
				ExpectedWarning:                 1305,
				ExpectedWarningsCount:           1,
				ExpectedWarningMessageSubstring: "Event non_existent_event does not exist",
			},
			{
				Query:                           "CREATE EVENT past_event1 ON SCHEDULE AT '2006-02-10 23:59:00' DISABLE DO INSERT INTO totals VALUES (100);",
				Expected:                        []sql.Row{{types.OkResult{}}},
				ExpectedWarning:                 1588,
				ExpectedWarningMessageSubstring: "Event execution time is in the past and ON COMPLETION NOT PRESERVE is set. The event was dropped immediately after creation.",
				ExpectedWarningsCount:           1,
			},
			{
				Query:    "SHOW EVENTS LIKE 'past_event1';",
				Expected: []sql.Row{},
			},
			{
				Query:                           "CREATE EVENT past_event2 ON SCHEDULE AT '2006-02-10 23:59:00' ON COMPLETION PRESERVE DO INSERT INTO totals VALUES (100);",
				Expected:                        []sql.Row{{types.OkResult{}}},
				ExpectedWarning:                 1544,
				ExpectedWarningMessageSubstring: "Event execution time is in the past. Event has been disabled",
				ExpectedWarningsCount:           1,
			},
			{
				Query:    "SHOW EVENTS LIKE 'past_event2';",
				Expected: []sql.Row{{"mydb", "past_event2", "`root`@`localhost`", "SYSTEM", "ONE TIME", "2006-02-10 23:59:00", nil, nil, nil, nil, "DISABLED", 0, "utf8mb4", "utf8mb4_0900_bin", "utf8mb4_0900_bin"}},
			},
		},
	},
	{
		Name: "invalid ALTER EVENT actions",
		SetUpScript: []string{
			"USE mydb;",
			"CREATE TABLE totals (num int);",
			"CREATE EVENT my_event1 ON SCHEDULE EVERY '1:2' MINUTE_SECOND DISABLE DO INSERT INTO totals VALUES (1);",
		},
		Assertions: []ServerEvenTestAssertion{
			{
				Query:          "ALTER EVENT my_event1 ON SCHEDULE AT '2006-02-10 23:59:00' ON COMPLETION NOT PRESERVE;",
				ExpectedErrStr: "Event execution time is in the past and ON COMPLETION NOT PRESERVE is set. The event was not changed. Specify a time in the future.",
			},
			{
				Query:                           "ALTER EVENT my_event1 ON SCHEDULE AT '2006-02-10 23:59:00' ON COMPLETION PRESERVE;",
				Expected:                        []sql.Row{{types.OkResult{}}},
				ExpectedWarning:                 1544,
				ExpectedWarningsCount:           1,
				ExpectedWarningMessageSubstring: "Event execution time is in the past. Event has been disabled",
			},
			{
				Query:    "CREATE EVENT my_event2 ON SCHEDULE EVERY '1:2' MINUTE_SECOND DISABLE DO INSERT INTO totals VALUES (2);",
				Expected: []sql.Row{{types.OkResult{}}},
			},
			{
				Query:       "ALTER EVENT my_event2 RENAME TO my_event1;",
				ExpectedErr: sql.ErrEventAlreadyExists,
			},
		},
	},
	{
		Name: "enabling expired event stays disabled",
		SetUpScript: []string{
			"USE mydb;",
			"CREATE TABLE totals (num int);",
			"CREATE EVENT my_event1 ON SCHEDULE AT '2006-02-10 23:59:00' ON COMPLETION PRESERVE DISABLE DO INSERT INTO totals VALUES (1);",
		},
		Assertions: []ServerEvenTestAssertion{
			{
				Query:    "ALTER EVENT my_event1 ENABLE;",
				Expected: []sql.Row{{types.OkResult{}}},
			},
			{
				Query:    "SHOW CREATE EVENT my_event1;",
				Expected: []sql.Row{{"my_event1", "", "SYSTEM", "CREATE DEFINER = `root`@`localhost` EVENT `my_event1` ON SCHEDULE AT '2006-02-10 23:59:00' ON COMPLETION PRESERVE DISABLE DO INSERT INTO totals VALUES (1)", "utf8mb4", "utf8mb4_0900_bin", "utf8mb4_0900_bin"}},
			},
		},
	},
	{
		Name: "enabling expired event with ON COMPLETION NOT PRESERVE drops the event",
		SetUpScript: []string{
			"USE mydb;",
			"CREATE TABLE totals (num int);",
			"CREATE EVENT my_event1 ON SCHEDULE AT '2006-02-10 23:59:00' ON COMPLETION PRESERVE DISABLE DO INSERT INTO totals VALUES (1);",
		},
		Assertions: []ServerEvenTestAssertion{
			{
				Query:    "ALTER EVENT my_event1 ON COMPLETION NOT PRESERVE;",
				Expected: []sql.Row{{types.OkResult{}}},
			},
			{
				Query:    "SHOW CREATE EVENT my_event1;",
				Expected: []sql.Row{{"my_event1", "", "SYSTEM", "CREATE DEFINER = `root`@`localhost` EVENT `my_event1` ON SCHEDULE AT '2006-02-10 23:59:00' ON COMPLETION NOT PRESERVE DISABLE DO INSERT INTO totals VALUES (1)", "utf8mb4", "utf8mb4_0900_bin", "utf8mb4_0900_bin"}},
			},
			{
				Query:    "SHOW EVENTS;",
				Expected: []sql.Row{{"mydb", "my_event1", "`root`@`localhost`", "SYSTEM", "ONE TIME", "2006-02-10 23:59:00", nil, nil, nil, nil, "DISABLED", 0, "utf8mb4", "utf8mb4_0900_bin", "utf8mb4_0900_bin"}},
			},
			{
				Query:    "ALTER EVENT my_event1 ENABLE;",
				Expected: []sql.Row{{types.OkResult{}}},
			},
			{
				Query:    "SHOW EVENTS;",
				Expected: []sql.Row{},
			},
		},
	},
	{
		Name: "altering event schedule between AT and EVERY",
		SetUpScript: []string{
			"USE mydb;",
			"CREATE TABLE totals (num int);",
			"CREATE EVENT my_event1 ON SCHEDULE EVERY '1:2' MINUTE_SECOND DISABLE DO INSERT INTO totals VALUES (1);",
		},
		Assertions: []ServerEvenTestAssertion{
			{
				Query:    "ALTER EVENT my_event1 ON SCHEDULE AT '2006-02-10 23:59:00' ON COMPLETION PRESERVE;",
				Expected: []sql.Row{{types.OkResult{}}},
			},
			{
				Query:    "SHOW CREATE EVENT my_event1;",
				Expected: []sql.Row{{"my_event1", "", "SYSTEM", "CREATE DEFINER = `root`@`localhost` EVENT `my_event1` ON SCHEDULE AT '2006-02-10 23:59:00' ON COMPLETION PRESERVE DISABLE DO INSERT INTO totals VALUES (1)", "utf8mb4", "utf8mb4_0900_bin", "utf8mb4_0900_bin"}},
			},
			{
				Query:    "ALTER EVENT my_event1 ON SCHEDULE EVERY 5 HOUR STARTS '2037-10-16 23:59:00' COMMENT 'updating the event schedule from AT';",
				Expected: []sql.Row{{types.OkResult{}}},
			},
			{
				Query:    "SHOW CREATE EVENT my_event1;",
				Expected: []sql.Row{{"my_event1", "", "SYSTEM", "CREATE DEFINER = `root`@`localhost` EVENT `my_event1` ON SCHEDULE EVERY 5 HOUR STARTS '2037-10-16 23:59:00' ON COMPLETION PRESERVE DISABLE COMMENT 'updating the event schedule from AT' DO INSERT INTO totals VALUES (1)", "utf8mb4", "utf8mb4_0900_bin", "utf8mb4_0900_bin"}},
			},
		},
	},
	{
		Name: "altering event fields",
		SetUpScript: []string{
			"USE mydb;",
			"CREATE TABLE totals (num int);",
			"CREATE EVENT my_event1 ON SCHEDULE AT '2006-02-10 23:59:00' ON COMPLETION PRESERVE DISABLE DO INSERT INTO totals VALUES (1);",
		},
		Assertions: []ServerEvenTestAssertion{
			{
				Query:    "ALTER EVENT my_event1 RENAME TO newEventName;",
				Expected: []sql.Row{{types.OkResult{}}},
			},
			{
				Query:    "SHOW CREATE EVENT newEventName;",
				Expected: []sql.Row{{"newEventName", "", "SYSTEM", "CREATE DEFINER = `root`@`localhost` EVENT `newEventName` ON SCHEDULE AT '2006-02-10 23:59:00' ON COMPLETION PRESERVE DISABLE DO INSERT INTO totals VALUES (1)", "utf8mb4", "utf8mb4_0900_bin", "utf8mb4_0900_bin"}},
			},
			{
				Query:    "ALTER EVENT newEventName COMMENT 'insert 2 instead of 1' DO INSERT INTO totals VALUES (2);",
				Expected: []sql.Row{{types.OkResult{}}},
			},
			{
				Query:    "SHOW CREATE EVENT newEventName;",
				Expected: []sql.Row{{"newEventName", "", "SYSTEM", "CREATE DEFINER = `root`@`localhost` EVENT `newEventName` ON SCHEDULE AT '2006-02-10 23:59:00' ON COMPLETION PRESERVE DISABLE COMMENT 'insert 2 instead of 1' DO INSERT INTO totals VALUES (2)", "utf8mb4", "utf8mb4_0900_bin", "utf8mb4_0900_bin"}},
			},
		},
	},
}
