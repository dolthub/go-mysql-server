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

package sql

import (
	"fmt"
	"time"
)

const EventTimeStampFormat = "2006-01-02 15:04:05"

// EventDefinition defines an event. Integrators are not expected to parse or understand the event definitions,
// but must store and return them when asked.
type EventDefinition struct {
	// The name of this event. Event names in a database are unique.
	Name string
	// The text of the statement to create this event.
	CreateStatement string
	// The time that the event was created.
	CreatedAt time.Time
	// The time that the event was last altered.
	LastAltered time.Time
	// True if this event definition relies on ANSI_QUOTES mode for parsing its SQL
	AnsiQuotes bool
}

// EventDetails are the details of the event.
type EventDetails struct {
	Name                 string
	Definer              string
	OnCompletionPreserve bool
	Status               string
	Comment              string
	Definition           string

	// events' ON SCHEDULE clause fields
	ExecuteAt    time.Time
	HasExecuteAt bool
	ExecuteEvery string
	Starts       time.Time // STARTS is always defined when EVERY is defined.
	Ends         time.Time
	HasEnds      bool

	// time values of event create/alter/execute metadata
	Created        time.Time
	LastAltered    time.Time
	LastExecuted   time.Time
	ExecutionCount uint64
	// TODO: add TimeZone
}

// CreateEventStatement returns a CREATE EVENT statement for this event.
func (e *EventDetails) CreateEventStatement() string {
	stmt := "CREATE"
	if e.Definer != "" {
		stmt = fmt.Sprintf("%s DEFINER = %s", stmt, e.Definer)
	}
	stmt = fmt.Sprintf("%s EVENT `%s`", stmt, e.Name)

	if e.HasExecuteAt {
		stmt = fmt.Sprintf("%s ON SCHEDULE AT '%s'", stmt, e.ExecuteAt.Format(EventTimeStampFormat))
	} else {
		// STARTS should be NOT null regardless of user definition
		stmt = fmt.Sprintf("%s ON SCHEDULE EVERY %s STARTS '%s'", stmt, e.ExecuteEvery, e.Starts.Format(EventTimeStampFormat))
		if e.HasEnds {
			stmt = fmt.Sprintf("%s ENDS '%s'", stmt, e.Ends.Format(EventTimeStampFormat))
		}
	}

	if e.OnCompletionPreserve {
		stmt = fmt.Sprintf("%s ON COMPLETION PRESERVE", stmt)
	} else {
		stmt = fmt.Sprintf("%s ON COMPLETION NOT PRESERVE", stmt)
	}

	stmt = fmt.Sprintf("%s %s", stmt, e.Status)

	if e.Comment != "" {
		stmt = fmt.Sprintf("%s COMMENT '%s'", stmt, e.Comment)
	}

	stmt = fmt.Sprintf("%s DO %s", stmt, e.Definition)
	return stmt
}
