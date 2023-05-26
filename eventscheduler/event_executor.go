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
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
)

// eventExecutor handles any action regarding events and executing each of events in the list.
// This includes handling any events related queries including CREATE/ALTER/DROP EVENT and DROP DATABASE.
// These queries notify the EventScheduler to update the enabled events list in the eventExecutor.
// eventExecutor also handles updating the event metadata in the database or dropping them after
// event execution.
type eventExecutor struct {
	bThreads            *sql.BackgroundThreads
	ctx                 *sql.Context
	list                *enabledEventsList
	runningEventsStatus *runningEventsStatus
	stop                atomic.Bool
	runQueryFunc        func(dbName, query, username, address string) error
}

// newEventExecutor returns eventExecutor object with empty enabled events list.
// The enabled events list is loaded only when the EventScheduler status is ENABLED.
func newEventExecutor(bgt *sql.BackgroundThreads, ctx *sql.Context, runQueryFunc func(dbName, query, username, address string) error) *eventExecutor {
	return &eventExecutor{
		bThreads:            bgt,
		ctx:                 ctx,
		list:                newEnabledEventsList([]*enabledEvent{}),
		runningEventsStatus: newRunningEventsStatus(),
		stop:                atomic.Bool{},
		runQueryFunc:        runQueryFunc,
	}
}

// loadEvents loads the events list for the eventExecutor.
func (ee *eventExecutor) loadEvents(l []*enabledEvent) {
	ee.list = newEnabledEventsList(l)
}

// start starts the eventExecutor checking and executing
// enabled events and updates necessary events' metadata.
func (ee *eventExecutor) start() {
	ee.stop.Store(false)

	for {
		timeNow := time.Now()
		select {
		case <-ee.ctx.Done():
			return
		default:
			if ee.stop.Load() {
				return
			} else if ee.list.len() > 1 {
				if ee.list.getNextExecutionTime().Sub(timeNow).Abs().Seconds() < 1 {
					curEvent := ee.list.pop()
					ee.executeEventAndUpdateListIfApplicable(curEvent, timeNow)
				}
			}
		}
	}
}

// shutdown stops the eventExecutor.
func (ee *eventExecutor) shutdown() {
	ee.stop.Store(true)
	ee.list.clear()
	ee.runningEventsStatus.clear()
}

// executeEventAndUpdateListIfApplicable executes the given event and updates the event's last executed time
// in the database. If the event is still valid (not ended), then it updates the enabled event and re-adds it back
// to the list.
func (ee *eventExecutor) executeEventAndUpdateListIfApplicable(event *enabledEvent, executionTime time.Time) {
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

	ended, err := event.updateEventAfterExecution(ee.ctx, event.edb, executionTime)
	if err != nil {
		// TODO: log update error
	} else if !ended {
		ee.list.add(event)
	}
}

// executeEvent adds another background thread to run the given event's definition query.
// This function returns whether the event needs to be added back into the enabled events list,
// error returned from executing the event definition and thread error.
func (ee *eventExecutor) executeEvent(event *enabledEvent) (bool, error, error) {
	ee.runningEventsStatus.update(event.name(), true, true)
	defer ee.runningEventsStatus.remove(event.name())

	reAdd, ok := ee.runningEventsStatus.getReAdd(event.name())
	if !ok {
		// should not happen, but sanity check
		reAdd = false
	}

	var queryErr error
	threadErr := ee.bThreads.Add(fmt.Sprintf("executing %s", event.name()), func(ctx context.Context) {
		select {
		case <-ctx.Done():
			ee.stop.Store(true)
			return
		default:
			queryErr = ee.runQueryFunc(event.edb.Name(), event.eventDetails.Definition, event.username, event.address)
			if queryErr != nil {
				return
			}
		}
	})

	return reAdd, queryErr, threadErr
}

// add creates new enabledEvent only if the event being created is at ENABLE status
// with valid schedule. If the updated event's schedule is starting at the same time
// as created time, it executes immediately.
func (ee *eventExecutor) add(edb sql.EventDatabase, details sql.EventDetails) {
	// if the updated event status is not ENABLE, do not add it to the list.
	if details.Status != sql.EventStatus_Enable.String() {
		return
	}

	newEvent, created, err := newEnabledEventFromEventDetails(ee.ctx, edb, details)
	if err != nil {
		// TODO: log error
	} else if !created {
		return
	} else {
		// TODO: created and starts value should be converted from event TZ to system TZ
		//  for checking event should be executed.
		// If Starts is set to current_timestamp or not set, then executeEvent the event once and update last executed At.
		if newEvent.eventDetails.Created.Sub(newEvent.eventDetails.Starts).Abs().Seconds() <= 1 {
			// after execution, the event is added to the list if applicable (if the event is not ended)
			ee.executeEventAndUpdateListIfApplicable(newEvent, newEvent.eventDetails.Created)
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
func (ee *eventExecutor) update(edb sql.EventDatabase, origEventName string, details sql.EventDetails) {
	var origEventKeyName = fmt.Sprintf("%s.%s", edb.Name(), origEventName)
	// remove the original event if exists.
	ee.list.remove(origEventKeyName)

	// if the updated event status is not ENABLE, do not add it to the list.
	if details.Status != sql.EventStatus_Enable.String() {
		return
	}

	// add the updated event as new event
	newUpdatedEvent, created, err := newEnabledEventFromEventDetails(ee.ctx, edb, details)
	if err != nil {
		// TODO: log error
	} else if created {
		// if the event being updated is currently running,
		// then do not re-add the event to the list after execution
		if s, ok := ee.runningEventsStatus.getStatus(origEventKeyName); ok && s {
			ee.runningEventsStatus.update(origEventKeyName, s, false)
		}

		// TODO: lastAltered and starts value should be converted from event TZ to system TZ
		//  for checking event should be executed.
		// if STARTS is set to current_timestamp or not set,
		// then executeEvent the event once and update last executed At.
		if details.LastAltered.Sub(details.Starts).Abs().Seconds() <= 1 {
			go ee.executeEventAndUpdateListIfApplicable(newUpdatedEvent, newUpdatedEvent.eventDetails.LastAltered)
		} else {
			ee.list.add(newUpdatedEvent)
		}
	}
}

// remove removes the event if it exists in the enabled events list.
// If the event is currently executing, it will not be in the list,
// so it updates the running events status to not re-add this event
// after its execution.
func (ee *eventExecutor) remove(eventIdName string) {
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
func (ee *eventExecutor) removeSchemaEvents(dbName string) {
	ee.list.removeSchemaEvents(dbName)
	// if not found, it might be currently executing
	ee.runningEventsStatus.removeSchemaEvents(dbName)
}
