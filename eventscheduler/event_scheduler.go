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

package eventscheduler

import (
	"errors"
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
)

// ErrEventSchedulerDisabled is returned when user tries to set `event_scheduler_notifier` global system variable to ON or OFF
// when the server started with either `--event-scheduler=DISABLED` or `--skip-grant-tables` configuration. Should have ERROR 1290 code.
var ErrEventSchedulerDisabled = errors.New("The server is running with the --event-scheduler=DISABLED or --skip-grant-tables option so it cannot executeEvent this statement")

// SchedulerStatus can be one of 'ON', 'OFF' or 'DISABLED'
// If --event-scheduler configuration variable is set to 'DISABLED'
// at the start of the server, it cannot be updated during runtime.
// If not defined, it defaults to 'ON', and it can be updated to 'OFF'
// during runtime.
// If --skip-grant-tables configuration flag is used, it defaults
// to 'DISABLED'.
type SchedulerStatus string

const (
	SchedulerOn       SchedulerStatus = "ON"
	SchedulerOff      SchedulerStatus = "OFF"
	SchedulerDisabled SchedulerStatus = "DISABLED"
)

var _ sql.EventSchedulerNotifier = (*EventScheduler)(nil)

// EventScheduler is responsible for SQL events execution.
type EventScheduler struct {
	status   SchedulerStatus
	executor *eventExecutor
}

// InitEventScheduler is called at the start of the server. This function returns EventScheduler object
// creating eventExecutor with empty events list. The enabled events will be loaded into the eventExecutor
// if the EventScheduler status is 'ON' or undefined. The runQueryFunc is used to run the event definition during
// event execution.
func InitEventScheduler(a *analyzer.Analyzer, bgt *sql.BackgroundThreads, ctx *sql.Context, status SchedulerStatus, runQueryFunc func(dbName, query, username, address string) error) (*EventScheduler, error) {
	var es = &EventScheduler{
		status:   status,
		executor: newEventExecutor(bgt, ctx, runQueryFunc),
	}

	// If the EventSchedulerStatus is set to ON, then load enabled
	// events and start executing events on schedule.
	if es.status == SchedulerOn {
		err := es.loadEventsAndStartEventExecutor(a, ctx)
		if err != nil {
			return nil, err
		}
	}

	return es, nil
}

// Close closes the EventScheduler.
func (es *EventScheduler) Close() {
	es.status = SchedulerOff
	es.executor.shutdown()
}

// TurnOnEventScheduler is called when user sets --event-scheduler system variable to ON or 1.
// This function requires valid analyzer and sql context to evaluate all events in all databases
// to load enabled events to the EventScheduler.
func (es *EventScheduler) TurnOnEventScheduler(a *analyzer.Analyzer, ctx *sql.Context) error {
	if es.status == SchedulerDisabled {
		return ErrEventSchedulerDisabled
	} else if es.status == SchedulerOn {
		return nil
	}

	es.status = SchedulerOn
	return es.loadEventsAndStartEventExecutor(a, ctx)
}

// TurnOffEventScheduler is called when user sets --event-scheduler system variable to OFF or 0.
func (es *EventScheduler) TurnOffEventScheduler() error {
	if es.status == SchedulerDisabled {
		return ErrEventSchedulerDisabled
	} else if es.status == SchedulerOff {
		return nil
	}

	es.status = SchedulerOff
	es.executor.shutdown()

	return nil
}

// loadEventsAndStartEventExecutor evaluates all events of all databases and retrieves the enabled events
// with valid schedule to load into the eventExecutor. Then, it starts the eventExecutor.
func (es *EventScheduler) loadEventsAndStartEventExecutor(a *analyzer.Analyzer, ctx *sql.Context) error {
	enabledEvents, err := es.evaluateAllEventsAndLoadEnabledEvents(a, ctx)
	if err != nil {
		return err
	}
	es.executor.loadEvents(enabledEvents)
	go es.executor.start()
	return nil
}

// evaluateAllEventsAndLoadEnabledEvents is called only when sql server starts with --event-scheduler
// configuration variable set to 'ON' or undefined, or it is set to 'ON' at runtime only when it was not
// set to DISABLED when server started. This function retrieves all events evaluating them by dropping
// events that are expired or updating the appropriate events metadata in the databases.
// This function returns list of events that are enabled and have valid schedule.
func (es *EventScheduler) evaluateAllEventsAndLoadEnabledEvents(a *analyzer.Analyzer, ctx *sql.Context) ([]*enabledEvent, error) {
	dbs := a.Catalog.AllDatabases(ctx)
	events := make([]*enabledEvent, 0)
	for _, db := range dbs {
		if edb, ok := db.(sql.EventDatabase); ok {
			eDefs, err := edb.GetEvents(ctx)
			if err != nil {
				return nil, err
			}
			// need to set the current database to get parsed plan
			ctx.SetCurrentDatabase(edb.Name())
			for _, eDef := range eDefs {
				ed, err := analyzer.GetEventDetailsFromEventDefinition(ctx, eDef)
				if err != nil {
					return nil, err
				}
				newEnabledEvent, created, err := newEnabledEventFromEventDetails(ctx, edb, ed)
				if err != nil {
					return nil, err
				} else if created {
					events = append(events, newEnabledEvent)
				}
			}
		}
	}
	return events, nil
}

// AddEvent implements sql.EventSchedulerNotifier interface.
// This function is called when there is an event created at runtime.
func (es *EventScheduler) AddEvent(edb sql.EventDatabase, details sql.EventDetails) {
	if es.status == SchedulerDisabled || es.status == SchedulerOff {
		return
	}
	es.executor.add(edb, details)
}

// UpdateEvent implements sql.EventSchedulerNotifier interface.
// This function is called when there is an event altered at runtime.
func (es *EventScheduler) UpdateEvent(edb sql.EventDatabase, orgEventName string, details sql.EventDetails) {
	if es.status == SchedulerDisabled || es.status == SchedulerOff {
		return
	}
	es.executor.update(edb, orgEventName, details)
}

// RemoveEvent implements sql.EventSchedulerNotifier interface.
// This function is called when there is an event dropped at runtime. This function
// removes the given event if it exists in the enabled events list of the EventScheduler.
func (es *EventScheduler) RemoveEvent(dbName, eventName string) {
	if es.status == SchedulerDisabled || es.status == SchedulerOff {
		return
	}
	es.executor.remove(fmt.Sprintf("%s.%s", dbName, eventName))
}

// RemoveSchemaEvents implements sql.EventSchedulerNotifier interface.
// This function is called when there is a database dropped at runtime. This function
// removes all events of given database that exist in the enabled events list of the EventScheduler.
func (es *EventScheduler) RemoveSchemaEvents(dbName string) {
	if es.status == SchedulerDisabled || es.status == SchedulerOff {
		return
	}
	es.executor.removeSchemaEvents(dbName)
}
