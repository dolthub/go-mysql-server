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

// eventExecutor handles execution of each enabled events and any events related queries
// including CREATE/ALTER/DROP EVENT and DROP DATABASE. These queries notify the EventScheduler
// to update the enabled events list in the eventExecutor. It also handles updating the event
// metadata in the database or dropping it from the database after its execution.
type eventExecutor struct {
	bThreads            *sql.BackgroundThreads
	list                *enabledEventsList
	runningEventsStatus *runningEventsStatus
	ctxGetterFunc       func() (*sql.Context, func() error, error)
	queryRunFunc        func(ctx *sql.Context, dbName, query, username, address string) error
	stop                atomic.Bool
}

// newEventExecutor returns eventExecutor object with empty enabled events list.
// The enabled events list is loaded only when the EventScheduler status is ENABLED.
func newEventExecutor(bgt *sql.BackgroundThreads, ctxFunc func() (*sql.Context, func() error, error), runQueryFunc func(ctx *sql.Context, dbName, query, username, address string) error) *eventExecutor {
	return &eventExecutor{
		bThreads:            bgt,
		list:                newEnabledEventsList([]*enabledEvent{}),
		runningEventsStatus: newRunningEventsStatus(),
		ctxGetterFunc:       ctxFunc,
		queryRunFunc:        runQueryFunc,
		stop:                atomic.Bool{},
	}
}

// loadEvents loads the enabled events list to the eventExecutor.
func (ee *eventExecutor) loadEvents(l []*enabledEvent) {
	ee.list = newEnabledEventsList(l)
}

// start starts the eventExecutor. It checks and executes
// enabled events and updates necessary events' metadata.
func (ee *eventExecutor) start() {
	ee.stop.Store(false)

	for {
		time.Sleep(1 * time.Second)
		timeNow := time.Now()
		if ee.stop.Load() {
			return
		} else if ee.list.len() > 0 {
			// safeguard list entry getting removed while in check
			nextAt, ok := ee.list.getNextExecutionTime()
			if ok {
				diff := nextAt.Sub(timeNow).Seconds()
				if diff <= -1.0000001 {
					// in case the execution time is past, re-evaluate it ( TODO: should not happen )
					curEvent := ee.list.pop()
					if curEvent != nil {
						ee.reevaluateEvent(curEvent.edb, curEvent.event)
					}
				} else if diff <= 0.0000001 {
					curEvent := ee.list.pop()
					if curEvent != nil {
						ctx, commit, err := ee.ctxGetterFunc()
						if err != nil {
							ctx.GetLogger().Errorf("Received error '%s' getting ctx in event scheduler", err)
						}
						err = ee.executeEventAndUpdateList(ctx, curEvent, timeNow)
						if err != nil {
							ctx.GetLogger().Errorf("Received error '%s' executing event: %s", err, curEvent.event.Name)
						}
						err = commit()
						if err != nil {
							ctx.GetLogger().Errorf("Received error '%s' executing event: %s", err, curEvent.event.Name)
						}
					}
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

// executeEventAndUpdateList executes the given event and updates the event's last executed time in the database.
// If the event is not ended, then it updates the enabled event and re-adds it back to the list.
func (ee *eventExecutor) executeEventAndUpdateList(ctx *sql.Context, event *enabledEvent, executionTime time.Time) error {
	reAdd, qErr, tErr := ee.executeEvent(event)
	if tErr != nil {
		return fmt.Errorf("error from background thread: %s", tErr)
	}
	if qErr != nil {
		return fmt.Errorf("error from event definition query: %s", qErr)
	}

	ended, err := event.updateEventAfterExecution(ctx, event.edb, executionTime)
	if err != nil {
		return err
	} else if !reAdd {
		return nil
	} else if !ended {
		ee.list.add(event)
	}
	return nil
}

// executeEvent executes given event by adding a thread to background threads to run the given event's definition.
// This function returns whether the event needs to be added back into the enabled events list, error returned from
// executing the event definition and thread error.
func (ee *eventExecutor) executeEvent(event *enabledEvent) (bool, error, error) {
	ee.runningEventsStatus.update(event.name(), true, true)
	defer ee.runningEventsStatus.remove(event.name())

	reAdd, ok := ee.runningEventsStatus.getReAdd(event.name())
	if !ok {
		// should not happen, but sanity check
		reAdd = false
	}
	// if event is ONE TIME, then do not re-add
	if event.event.HasExecuteAt {
		reAdd = false
	}

	var queryErr error
	threadErr := ee.bThreads.Add(fmt.Sprintf("executing %s", event.name()), func(ctx context.Context) {
		select {
		case <-ctx.Done():
			ee.stop.Store(true)
			return
		default:
			// get a new session sql.Context for each event definition execution
			sqlCtx, commit, err := ee.ctxGetterFunc()
			if err != nil {
				queryErr = err
				return
			}

			queryErr = ee.queryRunFunc(sqlCtx, event.edb.Name(), event.event.EventBody, event.username, event.address)
			if queryErr != nil {
				queryErr = err
				return
			}

			// must commit after done using the sql.Context
			err = commit()
			if err != nil {
				queryErr = err
				return
			}
		}
	})

	return reAdd, queryErr, threadErr
}

// reevaluateEvent evaluates an event from enabled events list, but its execution time passed the current time.
// It creates new enabledEvent if the event being created is at ENABLE status with valid schedule.
// This function is used when the event misses the execution time check of the event.
func (ee *eventExecutor) reevaluateEvent(edb sql.EventDatabase, details sql.EventDefinition) {
	// if the updated event status is not ENABLE, do not add it to the list.
	if details.Status != sql.EventStatus_Enable.String() {
		return
	}

	ctx, commit, err := ee.ctxGetterFunc()
	if err != nil {
		ctx.GetLogger().Errorf("Received error '%s' getting ctx in event scheduler", err)
	}

	newEvent, created, err := newEnabledEventFromEventDetails(ctx, edb, details, time.Now())
	if err != nil {
		ctx.GetLogger().Errorf("Received error '%s' re-evaluating event to scheduler: %s", err, details.Name)
	} else if created {
		ee.list.add(newEvent)
	}

	err = commit()
	if err != nil {
		ctx.GetLogger().Errorf("Received error '%s' re-evaluating event to scheduler: %s", err, details.Name)
	}
	return
}

// addEvent creates new enabledEvent if the event being created is at ENABLE status with valid schedule.
// If the updated event's schedule is starting at the same time as created time, it executes immediately.
func (ee *eventExecutor) addEvent(ctx *sql.Context, edb sql.EventDatabase, details sql.EventDefinition) {
	// if the updated event status is not ENABLE, do not add it to the list.
	if details.Status != sql.EventStatus_Enable.String() {
		return
	}

	newEvent, created, err := newEnabledEventFromEventDetails(ctx, edb, details, details.CreatedAt)
	if err != nil {
		ctx.GetLogger().Errorf("Received error '%s' executing event: %s", err, details.Name)
	} else if created {
		newDetails := newEvent.event
		// if STARTS is set to current_timestamp or not set,
		// then executeEvent the event once and update lastExecuted.
		var firstExecutionTime time.Time
		if newDetails.HasExecuteAt {
			firstExecutionTime = newDetails.ExecuteAt
		} else {
			firstExecutionTime = newDetails.Starts
		}
		if firstExecutionTime.Sub(newDetails.CreatedAt).Abs().Seconds() <= 1 {
			// after execution, the event is added to the list if applicable (if the event is not ended)
			err = ee.executeEventAndUpdateList(ctx, newEvent, newDetails.CreatedAt)
			if err != nil {
				ctx.GetLogger().Errorf("Received error '%s' executing event: %s", err, details.Name)
				return
			}
		} else {
			ee.list.add(newEvent)
		}
	}
	return
}

// updateEvent removes the event from enabled events list if it exists and adds new enabledEvent if the event status
// is ENABLE and event schedule is not expired. If the new event's schedule is starting at the same time as
// last altered time, it executes immediately.
func (ee *eventExecutor) updateEvent(ctx *sql.Context, edb sql.EventDatabase, origEventName string, details sql.EventDefinition) {
	var origEventKeyName = fmt.Sprintf("%s.%s", edb.Name(), origEventName)
	// remove the original event if exists.
	ee.list.remove(origEventKeyName)

	// if the updated event status is not ENABLE, do not add it to the list.
	if details.Status != sql.EventStatus_Enable.String() {
		return
	}

	// add the updated event as new event
	newUpdatedEvent, created, err := newEnabledEventFromEventDetails(ctx, edb, details, details.LastAltered)
	if err != nil {
		return
	} else if created {
		newDetails := newUpdatedEvent.event
		// if the event being updated is currently running,
		// then do not re-add the event to the list after execution
		if s, ok := ee.runningEventsStatus.getStatus(origEventKeyName); ok && s {
			ee.runningEventsStatus.update(origEventKeyName, s, false)
		}

		if newDetails.Starts.Sub(newDetails.LastAltered).Abs().Seconds() <= 1 {
			err = ee.executeEventAndUpdateList(ctx, newUpdatedEvent, newDetails.LastAltered)
			if err != nil {
				ctx.GetLogger().Errorf("Received error '%s' executing event: %s", err, newDetails.Name)
				return
			}
		} else {
			ee.list.add(newUpdatedEvent)
		}
	}
	return
}

// removeEvent removes the event if it exists in the enabled events list.
// If the event is currently executing, it will not be in the list,
// so it updates the running events status to not re-add this event
// after its execution.
func (ee *eventExecutor) removeEvent(eventIdName string) {
	ee.list.remove(eventIdName)
	// if not found, it might have been removed as it's currently executing
	if s, ok := ee.runningEventsStatus.getStatus(eventIdName); ok && s {
		ee.runningEventsStatus.update(eventIdName, s, false)
	}
}

// removeSchemaEvents removes all events from a given database if any exist
// in the enabled events list. If any events of this database
// are currently executing, they will not be in the list,
// so it updates the running events status to not re-add those
// events after their execution.
func (ee *eventExecutor) removeSchemaEvents(dbName string) {
	ee.list.removeSchemaEvents(dbName)
	// if not found, it might be currently executing
	ee.runningEventsStatus.cancelEventsForDatabase(dbName)
}
