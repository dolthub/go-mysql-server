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
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
)

// EnabledEvent is used for the events list stored in EventScheduler
type EnabledEvent struct {
	DbName        string
	EventName     string
	EventDetails  sql.EventDetails
	NextExecuteAt time.Time
}

// NewEnabledEventFromEventDetails returns new EnabledEvent and whether it is created successfully.
// The event with ENABLE status can be NOT created if the event SCHEDULE is ended/expired.
func NewEnabledEventFromEventDetails(ctx *sql.Context, edb sql.EventDatabase, ed sql.EventDetails) (*EnabledEvent, bool, error) {
	if ed.Status == sql.EventStatus_Enable.String() {
		// evaluating each event schedules by updating/dropping events if applicable
		nextExecution, eventEnded, err := ed.GetNextExecutionTime(time.Now())
		if err != nil {
			return nil, false, err
		} else if !eventEnded {
			return &EnabledEvent{
				DbName:        edb.Name(),
				EventName:     ed.Name,
				EventDetails:  ed,
				NextExecuteAt: nextExecution,
			}, true, nil
		} else {
			ed.Status = sql.EventStatus_Disable.String()
			if ed.OnCompletionPreserve {
				// update status to DISABLE
				err = edb.UpdateEvent(ctx, ed.Name, ed.GetEventStorageDefinition())
				if err != nil {
					return nil, false, err
				}
			} else {
				err = edb.DropEvent(ctx, ed.Name)
				if err != nil {
					return nil, false, err
				}
			}
		}
	}
	return nil, false, nil
}

// Name returns 'database_name.event_name' used as a key for mapping unique events.
func (e *EnabledEvent) Name() string {
	return fmt.Sprintf("%s.%s", e.DbName, e.EventName)
}

// updateEventAfterExecution updates the event's LastExecuted metadata and
// returns whether the event SCHEDULE is ended/expired. If the event SCHEDULE is not ended,
// this function updates the EnabledEvent with the next execution time.
func (e *EnabledEvent) updateEventAfterExecution(ctx *sql.Context, edb sql.EventDatabase) (bool, error) {
	e.EventDetails.LastExecuted = e.NextExecuteAt

	nextExecutionAt, ended, err := e.EventDetails.GetNextExecutionTime(time.Now())
	if err != nil {
		return ended, err
	} else if ended {
		if e.EventDetails.OnCompletionPreserve {
			e.EventDetails.Status = sql.EventStatus_Disable.String()
		} else {
			err = edb.DropEvent(ctx, e.EventName)
			if err != nil {
				return ended, err
			}
			return true, nil
		}
	} else {
		e.NextExecuteAt = nextExecutionAt
	}

	// update the database stored event with LastExecuted and Status metadata update if applicable.
	err = edb.UpdateEvent(ctx, e.EventName, e.EventDetails.GetEventStorageDefinition())
	if err != nil {
		return ended, err
	}

	return ended, nil
}

// runningStatus stores whether the event is currently running and needs to be re-added after execution.
// When currently running event is updated or dropped, it should not be re-added to the enabledEventsList
// after execution.
type runningStatus struct {
	status bool
	reAdd  bool
}

// EventExecutioner handles any action regarding enabledEventsList and running each of events in the list.
// This includes handling any events related queries updating the enabledEventsList. If any enabled event
// is executed or its scheduler is ended, this is responsible for updating the event metadata in the database.
type EventExecutioner struct {
	analyzer            *analyzer.Analyzer
	bThreads            *sql.BackgroundThreads
	ctx                 *sql.Context
	enabledEventsList   []*EnabledEvent
	eventsRunningStatus map[string]runningStatus
	stop                bool
}

func (ee *EventExecutioner) start() {
	ee.reorderEnabledEventsList()
	for {
		if ee.stop {
			break
		} else if len(ee.enabledEventsList) > 1 && ee.enabledEventsList[0].NextExecuteAt.Sub(time.Now()).Abs().Seconds() < 1 {
			// if there is at least one event and the event's execute at is equal to time.Now, then execute it
			ee.execute(ee.enabledEventsList[0])
			ee.enabledEventsList = ee.enabledEventsList[1:]
		} else {
			time.Sleep(time.Duration(1)*time.Second)
		}
	}
}

// close needs to be something similar with context handling...
func (ee *EventExecutioner) shutdown() {
	ee.stop = true
	err := ee.bThreads.Shutdown()
	if err != nil {
		// TODO: log error
	}
	for e := range ee.eventsRunningStatus {
		delete(ee.eventsRunningStatus, e)
	}
}

// execute adds another background thread to run the given event definition query.
// This function also updates the event after execution by evaluating it whether to
// drop the event or update event status or update next execution time. If the event
// is not ended, the event with updated nextExecuteAt value will be added to the
// enabledEventsList.
func (ee *EventExecutioner) execute(event *EnabledEvent) {
	ee.eventsRunningStatus[event.Name()] = runningStatus{true, true}
	defer func() {
		delete(ee.eventsRunningStatus, event.Name())
	}()
	time.Sleep(time.Duration(10)*time.Second)
	//err := ee.bThreads.Add(fmt.Sprintf("executing %s", event.Name()), func(ctx context.Context) {
	//	var qerr error
	//	// TODO: how to pass query to run to the engine??
	//	//_, _, qerr := es.engine.Query(es.ctx, es.curEvent.eventDetails.Definition)
	//	err := ee.updateCurrentEventAfterExecution(qerr, event)
	//	if err != nil {
	//		// TODO: log error
	//	}
	//})
	//if err != nil {
	//	// TODO: log error
	//}
}

// updateCurrentEventAfterExecution updates the current event after execution by either updating
// the event status or dropping it if ended or updating the LastExecuted field and getting
// the next execution time and update it on the database. It also updates the enabled events
// list of the EventScheduler by updating the next execution time or removing the entry.
func (ee *EventExecutioner) updateCurrentEventAfterExecution(errReturned error, e *EnabledEvent) error {
	// TODO: any error here, add more explanation
	if errReturned != nil {
		// TODO: log error somewhere
	}

	db, err := ee.analyzer.Catalog.Database(ee.ctx, e.DbName)
	if err != nil {
		return err
	}
	edb, ok := db.(sql.EventDatabase)
	if !ok {
		// this should not happen, but sanity check
		return sql.ErrEventsNotSupported.New(db.Name())
	}

	// ee.eventsRunningStatus[e.Name()] must exist here.
	if ee.eventsRunningStatus[e.Name()].reAdd {
		ended, err := e.updateEventAfterExecution(ee.ctx, edb)
		if err != nil {
			return err
		} else if !ended {
			ee.enabledEventsList = append(ee.enabledEventsList, e)
		}
	}

	return nil
}

func (ee *EventExecutioner) reorderEnabledEventsList() {
	sort.SliceStable(ee.enabledEventsList, func(i, j int) bool {
		return ee.enabledEventsList[i].NextExecuteAt.Sub(ee.enabledEventsList[j].NextExecuteAt).Seconds() < 1
	})
}

// add creates new EnabledEvent only if the event being created is at ENABLE status,
// or its schedule is not ended or in the past. If the updated event has
// SCHEDULE starting at the same time as created time, it executes immediately.
func (ee *EventExecutioner) add(ctx *sql.Context, edb sql.EventDatabase, details sql.EventDetails) {
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
		// If Starts is set to current_timestamp or not set, then execute the event once and update last executed At.
		if newEvent.EventDetails.Created.Sub(newEvent.EventDetails.Starts).Abs().Seconds() <= 1 {
			// after execution, the event is added to the list if applicable (if the event is not ended)
			ee.execute(newEvent)
			err := edb.UpdateLastExecuted(ctx, newEvent.EventDetails.Name, newEvent.EventDetails.Created)
			if err != nil {
				// TODO: log error
			}
		} else {
			ee.enabledEventsList = append(ee.enabledEventsList, newEvent)
			ee.reorderEnabledEventsList()
		}
	}
}

// remove is called when there is DROP EVENT on enabled event.
func (ee *EventExecutioner) remove(eventIdName string) {
	for i, e := range ee.enabledEventsList {
		if e.Name() == eventIdName {
			ee.enabledEventsList = append(ee.enabledEventsList[:i],ee.enabledEventsList[i+1:]...)
			ee.reorderEnabledEventsList()
		}
	}

	// if not found, it might have been removed as it's currently executing
	rs, ok := ee.eventsRunningStatus[eventIdName]
	if ok && rs.status {
		ee.eventsRunningStatus[eventIdName] = runningStatus{rs.status, false}
	}
}

// remove is called when there is DROP DATABASE.
func (ee *EventExecutioner) removeSchemaEvents(dbName string) {
	for i, e := range ee.enabledEventsList {
		if e.DbName == dbName {
			ee.enabledEventsList = append(ee.enabledEventsList[:i],ee.enabledEventsList[i+1:]...)
		}
	}

	ee.reorderEnabledEventsList()

	// if not found, it might have been removed as it's currently executing
	for evId := range ee.eventsRunningStatus {
		if strings.HasPrefix(evId, fmt.Sprintf("%s.", dbName)) {
			if ee.eventsRunningStatus[evId].status {
				ee.eventsRunningStatus[evId] = runningStatus{ee.eventsRunningStatus[evId].status, false}
			}
		}
	}
}

// update removes the event from enabledEventsList if it exists. It checks if the updated event status
// is ENABLE-d, if so it created new enabled event for the updated event to add to the enabled events list.
// This function make sure the events that are disabled or ended/expired do not get added to the
// enabled events list. If the updated event has SCHEDULE starts at the same time as lastAltered
// time, it executes immediately.
func (ee *EventExecutioner) update(ctx *sql.Context, edb sql.EventDatabase, origEventName string, details sql.EventDetails) {
	var origEventIdName = fmt.Sprintf("%s.%s", edb.Name(), origEventName)
	// remove the original event if exists.
	for i, e := range ee.enabledEventsList {
		if e.Name() == origEventIdName {
			ee.enabledEventsList = append(ee.enabledEventsList[:i],ee.enabledEventsList[i+1:]...)
		}
	}

	// if the updated event status is not ENABLE, do not add it to the list.
	if details.Status != sql.EventStatus_Enable.String() {
		return
	}

	// add the updated event as new event
	newUpdatedEvent, created, err := NewEnabledEventFromEventDetails(ctx, edb, details)
	if err != nil {
		// TODO: log error
	} else if created {
		// if the event being updated is currently running, then do not re-add the event after execution
		rs, ok := ee.eventsRunningStatus[origEventIdName]
		if ok && rs.status {
			ee.eventsRunningStatus[origEventIdName] = runningStatus{rs.status, false}
		}

		// If Starts is set to current_timestamp or not set, then execute the event once and update last executed At.
		if details.LastAltered.Sub(details.Starts).Abs().Seconds() <= 1 {
			// TODO: execute the event once and update 'LastExecuted' and 'ExecutionCount'
			// after execution, the event is added to the list if applicable (if the event is not ended)
			ee.execute(newUpdatedEvent)
			err = edb.UpdateLastExecuted(ctx, origEventName, details.LastAltered)
			if err != nil {
				// TODO: log error
			}
		} else {
			ee.enabledEventsList = append(ee.enabledEventsList, newUpdatedEvent)
			ee.reorderEnabledEventsList()
		}
	}
}
