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

import "time"

type EventScheduler struct {
	Name            string // The name of this event. Names must be unique within a database.
	CreateStatement string // The CREATE statement for this event.

	CreatedAt    time.Time // The time that the event was created.
	LastAltered  time.Time // The time of the last modification to the event.
	LastExecuted time.Time // The date and time when the event last executed. -- needs to be null/zero for never executed status

	ExecuteAt time.Time // The time that the event to be executed if defined with AT

	//ExecuteEvery    *expression.TimeDelta
	ExecuteStartsAt time.Time // The time that the event starts being executed if defined with EVERY
	ExecuteEndsAt   time.Time // The time that the event ends being executed if defined with EVERY

	Preserve bool
	// EventTimeZone   *time.Location // The time zone used for scheduling the event and that is in effect within the event as it executes. The default value is SYSTEM.
	// Definer			string    // This should be explicitly defined??? or not really???
	// SqlMode - is stored when the event is created or altered so, it does not depend on the current sql_mode
}
