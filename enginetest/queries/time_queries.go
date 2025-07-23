package queries

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
	"time"
)

var TimeQueryTests = []ScriptTest{
	{
		// time zone tests the current time set as July 23, 2025 at 9:43am America/Phoenix (-7:00) (does not observe
		// daylight savings time so time zone does not change)
		Name:        "time zone tests",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "set time_zone='UTC'",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "select now()",
				Expected: []sql.Row{{time.Date(2025, time.July, 23, 16, 43, 0, 0, time.UTC)}},
			},
			{
				Query:    "set time_zone='-5:00'",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "select now()",
				Expected: []sql.Row{{time.Date(2025, time.July, 23, 11, 43, 0, 0, time.UTC)}},
			},
			{
				// doesn't observe daylight savings time
				Query:    "set time_zone='Pacific/Honolulu'",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "select now()",
				Expected: []sql.Row{{time.Date(2025, time.July, 23, 6, 43, 0, 0, time.UTC)}},
			},
			{
				// https://github.com/dolthub/dolt/issues/9559
				Skip:  true,
				Query: "set time_zone='invalid time zone",
				// update to actual error or error string
				ExpectedErrStr: "Unknown of incorrect time zone: 'invalid time zone'",
			},
		},
	},
}
