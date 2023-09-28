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
var ErrEventSchedulerDisabled = errors.New("The server is running with the --event-scheduler=DISABLED or --skip-grant-tables option and the event scheduler cannot be enabled")

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

var _ sql.EventScheduler = (*EventScheduler)(nil)

// EventScheduler is responsible for SQL events execution.
type EventScheduler struct {
	status        SchedulerStatus
	executor      *eventExecutor
	ctxGetterFunc func() (*sql.Context, func() error, error)
}

// InitEventScheduler is called at the start of the server. This function returns EventScheduler object
// creating eventExecutor with empty events list. The enabled events will be loaded into the eventExecutor
// if the EventScheduler status is 'ON'. The runQueryFunc is used to run an event definition during
// event execution. If the |period| parameter is 1 or greater, then that value will be used as the period
// (in seconds) at which the event scheduler will wake up and see if events need to be executed.
func InitEventScheduler(
	a *analyzer.Analyzer,
	bgt *sql.BackgroundThreads,
	getSqlCtxFunc func() (*sql.Context, func() error, error),
	status SchedulerStatus,
	runQueryFunc func(ctx *sql.Context, dbName, query, username, address string) error,
	period int,
) (*EventScheduler, error) {
	var es = &EventScheduler{
		status:        status,
		executor:      newEventExecutor(bgt, getSqlCtxFunc, runQueryFunc, period),
		ctxGetterFunc: getSqlCtxFunc,
	}

	// If the EventSchedulerStatus is ON, then load enabled
	// events and start executing events on schedule.
	if es.status == SchedulerOn {
		ctx, commit, err := getSqlCtxFunc()
		if err != nil {
			return nil, err
		}
		err = es.loadEventsAndStartEventExecutor(ctx, a)
		if err != nil {
			return nil, err
		}
		err = commit()
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
func (es *EventScheduler) TurnOnEventScheduler(a *analyzer.Analyzer) error {
	if es.status == SchedulerDisabled {
		return ErrEventSchedulerDisabled
	} else if es.status == SchedulerOn {
		return nil
	}

	es.status = SchedulerOn

	ctx, commit, err := es.ctxGetterFunc()
	if err != nil {
		return err
	}
	err = es.loadEventsAndStartEventExecutor(ctx, a)
	if err != nil {
		return err
	}
	return commit()
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

// loadEventsAndStartEventExecutor evaluates all events in all databases and evaluates the enabled events
// with valid schedule to load into the eventExecutor. Then, it starts the eventExecutor.
func (es *EventScheduler) loadEventsAndStartEventExecutor(ctx *sql.Context, a *analyzer.Analyzer) error {
	es.executor.catalog = a.Catalog
	es.executor.loadAllEvents(ctx)
	go es.executor.start()
	return nil
}

// AddEvent implements sql.EventScheduler interface.
// This function is called when there is an event created at runtime.
func (es *EventScheduler) AddEvent(ctx *sql.Context, edb sql.EventDatabase, details sql.EventDefinition) {
	if es.status == SchedulerDisabled || es.status == SchedulerOff {
		return
	}
	es.executor.addEvent(ctx, edb, details)
}

// UpdateEvent implements sql.EventScheduler interface.
// This function is called when there is an event altered at runtime.
func (es *EventScheduler) UpdateEvent(ctx *sql.Context, edb sql.EventDatabase, orgEventName string, details sql.EventDefinition) {
	if es.status == SchedulerDisabled || es.status == SchedulerOff {
		return
	}
	es.executor.updateEvent(ctx, edb, orgEventName, details)
}

// RemoveEvent implements sql.EventScheduler interface.
// This function is called when there is an event dropped at runtime. This function
// removes the given event if it exists in the enabled events list of the EventScheduler.
func (es *EventScheduler) RemoveEvent(dbName, eventName string) {
	if es.status == SchedulerDisabled || es.status == SchedulerOff {
		return
	}
	es.executor.removeEvent(fmt.Sprintf("%s.%s", dbName, eventName))
}

// RemoveSchemaEvents implements sql.EventScheduler interface.
// This function is called when there is a database dropped at runtime. This function
// removes all events of given database that exist in the enabled events list of the EventScheduler.
func (es *EventScheduler) RemoveSchemaEvents(dbName string) {
	if es.status == SchedulerDisabled || es.status == SchedulerOff {
		return
	}
	es.executor.removeSchemaEvents(dbName)
}
