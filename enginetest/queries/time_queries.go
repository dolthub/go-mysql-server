// Copyright 2020-2025 Dolthub, Inc.
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
	"github.com/dolthub/go-mysql-server/sql/types"
)

var TimeQueryTests = []ScriptTest{
	{
		// time zone tests the current time set as July 23, 2025 at 9:43:21am America/Phoenix (-7:00) (does not observe
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
				Expected: []sql.Row{{time.Date(2025, time.July, 23, 16, 43, 21, 0, time.UTC)}},
			},
			{
				Query:    "set time_zone='-5:00'",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "select now()",
				Expected: []sql.Row{{time.Date(2025, time.July, 23, 11, 43, 21, 0, time.UTC)}},
			},
			{
				// doesn't observe daylight savings time
				Query:    "set time_zone='Pacific/Honolulu'",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "select now()",
				Expected: []sql.Row{{time.Date(2025, time.July, 23, 6, 43, 21, 0, time.UTC)}},
			},
			{
				Query:       "set time_zone='invalid time zone'",
				ExpectedErr: sql.ErrInvalidTimeZone,
			},
		},
	},
}
