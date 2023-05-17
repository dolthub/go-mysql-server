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
	"context"
	"fmt"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
)

// eventExecutioner handles any action regarding events and executing each of events in the list.
// This includes handling any events related queries including CREATE/ALTER/DROP EVENT and DROP DATABASE.
// These queries notify the EventScheduler to update the enabled events list in the eventExecutioner.
// eventExecutioner also handles updating the event metadata in the database or dropping them after
// event execution.
type eventExecutioner struct {
	bThreads            *sql.BackgroundThreads
	ctx                 *sql.Context
	list                *enabledEventsList
	runningEventsStatus *runningEventsStatus
	stop                bool
	runQueryFunc        func(query string) error
}

// newEventExecutioner returns eventExecutioner object with empty enabled events list.
// The enabled events list is loaded only when the EventScheduler status is ENABLED.
func newEventExecutioner(bgt *sql.BackgroundThreads, ctx *sql.Context, runQueryFunc func(query string) error) *eventExecutioner {
	return &eventExecutioner{
		bThreads:            bgt,
		ctx:                 ctx,
		list:                newEnabledEventsList([]*enabledEvent{}),
		runningEventsStatus: newRunningEventsStatus(),
		stop:                false,
		runQueryFunc:        runQueryFunc,
	}
}

// loadEvents loads the events list for the eventExecutioner.
func (ee *eventExecutioner) loadEvents(l []*enabledEvent) {
	ee.list = newEnabledEventsList(l)
}

// TODO: this function needs to start a background process to
//  check for events to execute on given schedule.
func (ee *eventExecutioner) start() {
	timer := time.NewTimer(time.Duration(1) * time.Second)
	defer timer.Stop()

	// TODO: use the same context from the initialization?
	for {
		select {
		case <-ee.ctx.Done():
			return
		case <-timer.C:
			if ee.stop {
				return
			} else if ee.list.len() > 1 {
				timeNow := time.Now()
				if ee.list.getNextExecutionTime().Sub(timeNow).Abs().Seconds() < 1 {
					curEvent := ee.list.pop()
					ee.executeEventAndUpdateListIfApplicable(ee.ctx, curEvent, timeNow)
				}
			}
		}
	}
}

// shutdown stops the eventExecutioner.
func (ee *eventExecutioner) shutdown() {
	ee.stop = true
	// TODO: do i need this?
	//  this bgThread shutdown gets called at engine.Close()
	//err := ee.bThreads.Shutdown()
	//if err != nil {
	//	// TODO: log error
	//}
	ee.list.clear()
	ee.runningEventsStatus.clear()
}

// executeEventAndUpdateListIfApplicable executes the given event and updates the event's last executed time
// in the database. If the event is still valid (not ended), then it updates the enabled event and re-adds it back
// to the list.
func (ee *eventExecutioner) executeEventAndUpdateListIfApplicable(ctx *sql.Context, event *enabledEvent, executionTime time.Time) {
	reAdd, qErr, tErr := ee.executeEvent(event)
	if tErr != nil {
		// TODO: log background thread error
	}
	if qErr != nil {
		// TODO: log query error
	}

	if !reAdd {
		return
	}

	ended, err := event.updateEventAfterExecution(ctx, event.edb, executionTime)
	if err != nil {
		// TODO: log update error
	} else if !ended {
		ee.list.add(event)
	}
}

// executeEvent adds another background thread to run the given event's definition query.
// This function returns whether the event needs to be added back into the enabled events list,
// error returned from executing the event definition and thread error.
func (ee *eventExecutioner) executeEvent(event *enabledEvent) (reAdd bool, queryErr error, threadErr error) {
	ee.runningEventsStatus.update(event.name(), true, true)
	defer ee.runningEventsStatus.remove(event.name())

	threadErr = ee.bThreads.Add(fmt.Sprintf("executing %s", event.name()), func(ctx context.Context) {
		select {
		case <-ctx.Done():
			ee.stop = true
			return
		default:
			queryErr = ee.runQueryFunc(event.eventDetails.Definition)
		}
	})

	reAdd, ok := ee.runningEventsStatus.getReAdd(event.name())
	if !ok {
		// TODO: should not happen
	}
	return
}

// add creates new enabledEvent only if the event being created is at ENABLE status
// with valid schedule. If the updated event's schedule is starting at the same time
// as created time, it executes immediately.
func (ee *eventExecutioner) add(ctx *sql.Context, edb sql.EventDatabase, details sql.EventDetails) {
	// if the updated event status is not ENABLE, do not add it to the list.
	if details.Status != sql.EventStatus_Enable.String() {
		return
	}

	newEvent, created, err := NewEnabledEventFromEventDetails(ctx, edb, details)
	if err != nil {
		// TODO: log error
	} else if !created {
		return
	} else {
		// If Starts is set to current_timestamp or not set, then executeEvent the event once and update last executed At.
		if newEvent.eventDetails.Created.Sub(newEvent.eventDetails.Starts).Abs().Seconds() <= 1 {
			// after execution, the event is added to the list if applicable (if the event is not ended)
			go ee.executeEventAndUpdateListIfApplicable(ctx, newEvent, newEvent.eventDetails.Created)
		} else {
			ee.list.add(newEvent)
		}
	}
}

// update removes the event from enabled events list if it exists. If the updated event status
// is ENABLE, then it creates new enabled event and adds to the enabled events list.
// This function make sure the events that are disabled or expired do not get added to the
// enabled events list. If the new event's schedule is starting at the same time as last altered
// time, it executes immediately.
func (ee *eventExecutioner) update(ctx *sql.Context, edb sql.EventDatabase, origEventName string, details sql.EventDetails) {
	var origEventKeyName = fmt.Sprintf("%s.%s", edb.Name(), origEventName)
	// remove the original event if exists.
	ee.list.remove(origEventKeyName)

	// if the updated event status is not ENABLE, do not add it to the list.
	if details.Status != sql.EventStatus_Enable.String() {
		return
	}

	// add the updated event as new event
	newUpdatedEvent, created, err := NewEnabledEventFromEventDetails(ctx, edb, details)
	if err != nil {
		// TODO: log error
	} else if created {
		// if the event being updated is currently running,
		// then do not re-add the event to the list after execution
		if s, ok := ee.runningEventsStatus.getStatus(origEventKeyName); ok && s {
			ee.runningEventsStatus.update(origEventKeyName, s, false)
		}

		// if STARTS is set to current_timestamp or not set,
		// then executeEvent the event once and update last executed At.
		if details.LastAltered.Sub(details.Starts).Abs().Seconds() <= 1 {
			go ee.executeEventAndUpdateListIfApplicable(ctx, newUpdatedEvent, newUpdatedEvent.eventDetails.LastAltered)
		} else {
			ee.list.add(newUpdatedEvent)
		}
	}
}

// remove removes the event if it exists in the enabled events list.
// If the event is currently executing, it will not be in the list,
// so it updates the running events status to not re-add this event
// after its execution.
func (ee *eventExecutioner) remove(eventIdName string) {
	ee.list.remove(eventIdName)

	// if not found, it might have been removed as it's currently executing
	if s, ok := ee.runningEventsStatus.getStatus(eventIdName); ok && s {
		ee.runningEventsStatus.update(eventIdName, s, false)
	}
}

// remove removes all events of given database if any exists
// in the enabled events list. If any events of this database
// is currently executing, it will not be in the list,
// so it updates the running events status to not re-add those
// events after their execution.
func (ee *eventExecutioner) removeSchemaEvents(dbName string) {
	ee.list.removeSchemaEvents(dbName)

	// if not found, it might be currently executing
	ee.runningEventsStatus.removeSchemaEvents(dbName)
}
