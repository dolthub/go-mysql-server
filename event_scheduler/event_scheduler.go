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

package event_scheduler

import (
	"errors"
	"fmt"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	"strings"
)

// ErrEventSchedulerDisabled is returned when user tries to set `event_scheduler_notifier` global system variable to ON or OFF
// when the server started with either `--event-scheduler=DISABLED` or `--skip-grant-tables`. Should have ERROR 1290 code.
var ErrEventSchedulerDisabled = errors.New("The server is running with the --event-scheduler=DISABLED or --skip-grant-tables option so it cannot execute this statement")

type EventSchedulerStatus string

const (
	EventSchedulerOn       EventSchedulerStatus = "ON"
	EventSchedulerOff      EventSchedulerStatus = "OFF"
	EventSchedulerDisabled EventSchedulerStatus = "DISABLED"
)

var _ sql.EventSchedulerNotifier = (*EventScheduler)(nil)

// EventScheduler is equivalent to MySQL server event scheduler.
// If --event-scheduler configuration variable is set to DISABLED at the start of the server,
// it cannot be updated during runtime.
// If not defined, it defaults to ON, and it can be updated to OFF during runtime.
type EventScheduler struct {
	analyzer          *analyzer.Analyzer
	bgt               *sql.BackgroundThreads
	ctx               *sql.Context
	status            EventSchedulerStatus // ON (true) or OFF (false), redundant if DISABLED is true
	eventExecutioner *EventExecutioner
}

// InitEventScheduler will be called after engine is built with required fields including
// analyzer, background thread, sql.Context/session builder and the status of the event scheduler.
// The event scheduler status will be ON by default if user did not define `--event-scheduler` or
// `--skip-grant-tables` config variables at the start of the sql server.
// TODO: need callback instead? like: sessBuilder func(ctx context.Context) (*sql.Context, error)
func InitEventScheduler(analyzer *analyzer.Analyzer, bgt *sql.BackgroundThreads, ctx *sql.Context, status string) (*EventScheduler, error) {
	var s EventSchedulerStatus
	switch strings.ToLower(status) {
	case "on", "1":
		s = EventSchedulerOn
	case "off", "0":
		s = EventSchedulerOff
	case "disabled":
		s = EventSchedulerDisabled
	default:
		// if empty or anything else, ON by default
		s = EventSchedulerOn
	}

	var es = &EventScheduler{
		analyzer:         analyzer,
		bgt:              bgt,
		ctx:              ctx,
		status:           s,
		eventExecutioner: &EventExecutioner{
			analyzer:            analyzer,
			bThreads:            bgt,
			ctx:                 ctx,
			enabledEventsList:   make([]*EnabledEvent, 0),
			eventsRunningStatus: make(map[string]runningStatus),
			stop: false,
		},
	}

	// If the EventScheduler is set to ON, then load enabled events and
	// start the background thread to run and execute events
	if es.status == EventSchedulerOn {
		err := es.evaluateAllEventsAndLoadEnabledEvents()
		if err != nil {
			return nil, err
		}
		go es.eventExecutioner.start()
	}

	analyzer.EventSchedulerNotifier = es
	return es, nil
}

// TurnOnEventScheduler is called when user sets --event-scheduler system variable to ON or 1
func (es *EventScheduler) TurnOnEventScheduler() error {
	if es.status == EventSchedulerDisabled {
		return ErrEventSchedulerDisabled
	} else if es.status == EventSchedulerOn {
		return nil
	}

	es.status = EventSchedulerOn
	err := es.evaluateAllEventsAndLoadEnabledEvents()
	if err != nil {
		return err
	}

	return nil
}

// TurnOffEventScheduler is called when user sets --event-scheduler system variable to OFF or 0
func (es *EventScheduler) TurnOffEventScheduler() error {
	if es.status == EventSchedulerDisabled {
		return ErrEventSchedulerDisabled
	} else if es.status == EventSchedulerOff {
		return nil
	}

	es.status = EventSchedulerOff
	es.eventExecutioner.shutdown()

	return nil
}

// evaluateAllEventsAndLoadEnabledEvents is called only when sql server starts or
// EventScheduler is turned ON at runtime. This function validates all events by updating
// or dropping events that are expired and updates the appropriate events metadata
// in the databases. This function STARTS the EventExecutioner after initializing
// its enabledEventsList.
func (es *EventScheduler) evaluateAllEventsAndLoadEnabledEvents() error {
	dbs := es.eventExecutioner.analyzer.Catalog.AllDatabases(es.ctx)
	events := make([]*EnabledEvent, 0)
	for _, db := range dbs {
		if edb, ok := db.(sql.EventDatabase); ok {
			eDefs, err := edb.GetEvents(es.ctx)
			if err != nil {
				return err
			}
			for _, eDef := range eDefs {
				ed, err := analyzer.GetEventDetailsFromEventDefinition(es.ctx, eDef)
				if err != nil {
					return err
				}
				newEnabledEvent, created, err := NewEnabledEventFromEventDetails(es.ctx, edb, ed)
				if err != nil {
					return err
				} else if created {
					events = append(events, newEnabledEvent)
				}
			}
		}
	}

	es.eventExecutioner.enabledEventsList = events
	go es.eventExecutioner.start()

	return nil
}

// All these functions are only called by events related query statements.
// The events stored in the database are handled in the appropriate plan Nodes.
// Other cases such as dropping the event that has been completed after executing
// will be updated by eventExecutioner.

// AddEvent implements sql.EventSchedulerNotifier interface.
// This function is called when there is an event created at runtime.
func (es *EventScheduler) AddEvent(ctx *sql.Context, edb sql.EventDatabase, details sql.EventDetails) {
	if es.status == EventSchedulerDisabled || es.status == EventSchedulerOff {
		return
	}
	es.eventExecutioner.add(ctx, edb, details)
}

// UpdateEvent implements sql.EventSchedulerNotifier interface.
// This function is called when there is an event altered at runtime.
func (es *EventScheduler) UpdateEvent(ctx *sql.Context, edb sql.EventDatabase, orgEventName string, details sql.EventDetails) {
	if es.status == EventSchedulerDisabled || es.status == EventSchedulerOff {
		return
	}
	es.eventExecutioner.update(ctx, edb, orgEventName, details)
}

// RemoveEvent implements sql.EventSchedulerNotifier interface.
// This function is called when there is an event dropped at runtime. This function
// removes the given event if it exists in the enabled events list of the EventScheduler.
func (es *EventScheduler) RemoveEvent(ctx *sql.Context, dbName, eventName string) {
	if es.status == EventSchedulerDisabled || es.status == EventSchedulerOff {
		return
	}
	es.eventExecutioner.remove(fmt.Sprintf("%s.%s", dbName, eventName))
}

// RemoveSchemaEvents implements sql.EventSchedulerNotifier interface.
// This function is called when there is a database dropped at runtime. This function
// removes all events of given database that exist in the enabled events list of the EventScheduler.
func (es *EventScheduler) RemoveSchemaEvents(dbName string) {
	if es.status == EventSchedulerDisabled || es.status == EventSchedulerOff {
		return
	}
	es.eventExecutioner.removeSchemaEvents(dbName)
}
