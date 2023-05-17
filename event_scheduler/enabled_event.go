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
	"sync"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
)

// enabledEvent is used for the events list stored in EventScheduler
type enabledEvent struct {
	edb             sql.EventDatabase
	eventDetails    sql.EventDetails
	nextExecutionAt time.Time
}

// NewEnabledEventFromEventDetails returns new enabledEvent and whether it is created successfully.
// An event with ENABLE status might NOT be created if the event SCHEDULE is ended/expired. If the
// event is expired, then this function either updates its status in the database or drops it from
// the database.
func NewEnabledEventFromEventDetails(ctx *sql.Context, edb sql.EventDatabase, ed sql.EventDetails) (*enabledEvent, bool, error) {
	if ed.Status == sql.EventStatus_Enable.String() {
		// evaluating each event schedules by updating/dropping events if applicable
		nextExecution, eventEnded, err := ed.GetNextExecutionTime(time.Now())
		if err != nil {
			return nil, false, err
		} else if !eventEnded {
			return &enabledEvent{
				edb:             edb,
				eventDetails:    ed,
				nextExecutionAt: nextExecution,
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
func (e *enabledEvent) name() string {
	return fmt.Sprintf("%s.%s", e.edb.Name(), e.eventDetails.Name)
}

// updateEventAfterExecution updates the event's LastExecuted metadata with given execution time
// and returns whether the event SCHEDULE is ended/expired. If the event SCHEDULE is not ended,
// this function updates the enabledEvent with the next execution time. It also updates the event
// metadata in the database.
func (e *enabledEvent) updateEventAfterExecution(ctx *sql.Context, edb sql.EventDatabase, executionTime time.Time) (bool, error) {
	e.eventDetails.LastExecuted = executionTime
	nextExecutionAt, ended, err := e.eventDetails.GetNextExecutionTime(executionTime)
	if err != nil {
		return ended, err
	} else if ended {
		if e.eventDetails.OnCompletionPreserve {
			e.eventDetails.Status = sql.EventStatus_Disable.String()
		} else {
			err = edb.DropEvent(ctx, e.eventDetails.Name)
			if err != nil {
				return ended, err
			}
			return true, nil
		}
	} else {
		e.nextExecutionAt = nextExecutionAt
	}

	// update the database stored event with LastExecuted and Status metadata update if applicable.
	err = edb.UpdateEvent(ctx, e.eventDetails.Name, e.eventDetails.GetEventStorageDefinition())
	if err != nil {
		return ended, err
	}

	return ended, nil
}

// enabledEventsList is a list of enabled events of all databases that the eventExecutioner
// uses to executeEvent them at the scheduled time.
type enabledEventsList struct {
	mu         *sync.Mutex
	eventsList []*enabledEvent
}

// newEnabledEventsList returns new enabledEventsList object
// with the enabledEvent list sorted by the nextExecutionAt time.
func newEnabledEventsList(list []*enabledEvent) *enabledEventsList {
	newList := &enabledEventsList{
		mu:         &sync.Mutex{},
		eventsList: list,
	}
	newList.sort()
	return newList
}

func (l *enabledEventsList) clear() {
	l.mu.Lock()
	defer l.mu.Unlock()
	// TODO: do I need to set it to an empty list?
	l.eventsList = []*enabledEvent{}
}

func (l *enabledEventsList) sort() {
	l.mu.Lock()
	defer l.mu.Unlock()
	sort.SliceStable(l.eventsList, func(i, j int) bool {
		return l.eventsList[i].nextExecutionAt.Sub(l.eventsList[j].nextExecutionAt).Seconds() < 1
	})
}

func (l *enabledEventsList) len() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.eventsList)
}

func (l *enabledEventsList) getNextExecutionTime() time.Time {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.eventsList) == 0 {
		return time.Time{}
	}
	return l.eventsList[0].nextExecutionAt
}

func (l *enabledEventsList) pop() *enabledEvent {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.eventsList) == 0 {
		return nil
	}
	return l.eventsList[0]
}

func (l *enabledEventsList) add(event *enabledEvent) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.eventsList = append(l.eventsList, event)
	l.sort()
}

func (l *enabledEventsList) remove(key string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for i, e := range l.eventsList {
		if e.name() == key {
			l.eventsList = append(l.eventsList[:i], l.eventsList[i+1:]...)
			l.sort()
			return
		}
	}
}

func (l *enabledEventsList) removeSchemaEvents(dbName string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for i, e := range l.eventsList {
		if e.edb.Name() == dbName {
			l.eventsList = append(l.eventsList[:i], l.eventsList[i+1:]...)
		}
	}
	l.sort()
}

// runningEventsStatus stores whether the event is currently running and
// needs to be re-added after execution. When currently running event is
// updated or dropped, it should not be re-added to the enabledEventsList
// after execution.
type runningEventsStatus struct {
	mu     *sync.Mutex
	status map[string]bool
	reAdd  map[string]bool
}

func newRunningEventsStatus() *runningEventsStatus {
	return &runningEventsStatus{
		mu:     &sync.Mutex{},
		status: make(map[string]bool),
		reAdd:  make(map[string]bool),
	}
}

func (r *runningEventsStatus) clear() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for k := range r.status {
		delete(r.status, k)
	}
	for k := range r.reAdd {
		delete(r.status, k)
	}
}

func (r *runningEventsStatus) update(key string, status, reAdd bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.status[key] = status
	r.reAdd[key] = reAdd
}

func (r *runningEventsStatus) remove(key string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.status, key)
}

func (r *runningEventsStatus) getStatus(key string) (bool, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	s, ok := r.status[key]
	return s, ok
}

func (r *runningEventsStatus) getReAdd(key string) (bool, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	ra, ok := r.reAdd[key]
	return ra, ok
}

func (r *runningEventsStatus) removeSchemaEvents(dbName string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	// if there are any running events of given database, then set reAdd to false
	for evId := range r.status {
		if strings.HasPrefix(evId, fmt.Sprintf("%s.", dbName)) {
			r.update(evId, r.status[evId], false)
		}
	}
}
