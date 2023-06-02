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
	username        string
	address         string
}

// newEnabledEventFromEventDetails returns new enabledEvent and whether it is created successfully.
// An event with ENABLE status might NOT be created if the event SCHEDULE is ended/expired. If the
// event is expired, then this function either updates its status in the database or drops it from
// the database.
func newEnabledEventFromEventDetails(ctx *sql.Context, edb sql.EventDatabase, ed sql.EventDetails) (*enabledEvent, bool, error) {
	if ed.Status == sql.EventStatus_Enable.String() {
		// evaluating each event schedules by updating/dropping events if applicable
		nextExecution, eventEnded, err := ed.GetNextExecutionTime()
		if err != nil {
			return nil, false, err
		} else if !eventEnded {
			username, address, err := getUsernameAndAddressFromDefiner(ed.Definer)
			if err != nil {
				return nil, false, err
			}
			return &enabledEvent{
				edb:             edb,
				eventDetails:    ed,
				nextExecutionAt: nextExecution,
				username:        username,
				address:         address,
			}, true, nil
		} else {
			ed.Status = sql.EventStatus_Disable.String()
			if ed.OnCompletionPreserve {
				// update status to DISABLE
				err = edb.UpdateEvent(ctx, ed.Name, ed)
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

// getUsernameAndAddressFromDefiner returns username and address parsed from given definer value of EventDetails.
func getUsernameAndAddressFromDefiner(definer string) (string, string, error) {
	// make sure definer has username and address information here
	ua := strings.Split(definer, "@")
	if len(ua) != 2 {
		// log error
	}

	username := strings.TrimSuffix(strings.TrimPrefix(ua[0], "`"), "`")
	username = strings.TrimSuffix(strings.TrimPrefix(username, "'"), "'")

	address := strings.TrimSuffix(strings.TrimPrefix(ua[1], "`"), "`")
	address = strings.TrimSuffix(strings.TrimPrefix(address, "'"), "'")

	return username, address, nil
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
	// TODO: the lastExecuted value should be stored in the event's timezone
	//  (currently, it is always SYSTEM, so conversion not needed for now)
	nextExecutionAt, ended, err := e.eventDetails.GetNextExecutionTime()
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

	e.eventDetails.LastExecuted = executionTime
	// update the database stored event with LastExecuted and Status metadata update if applicable.
	err = edb.UpdateEvent(ctx, e.eventDetails.Name, e.eventDetails)
	if err != nil {
		// TODO: getting event doesn't exist error because different ctx is used
		return ended, err
	}

	return ended, nil
}

// enabledEventsList is a list of enabled events of all databases that the eventExecutor
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
	sort.SliceStable(newList.eventsList, func(i, j int) bool {
		return list[i].nextExecutionAt.Sub(list[j].nextExecutionAt).Seconds() < 1
	})
	return newList
}

func (l *enabledEventsList) clear() {
	l.mu.Lock()
	defer l.mu.Unlock()
	// TODO: do I need to set it to an empty list?
	l.eventsList = []*enabledEvent{}
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

// pop returns the first element and removes it from the list.
func (l *enabledEventsList) pop() *enabledEvent {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.eventsList) == 0 {
		return nil
	}
	firstInList := l.eventsList[0]
	l.eventsList = l.eventsList[1:]
	return firstInList
}

// add adds the event to the list and sorts the list.
func (l *enabledEventsList) add(event *enabledEvent) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.eventsList = append(l.eventsList, event)
	sort.SliceStable(l.eventsList, func(i, j int) bool {
		return l.eventsList[i].nextExecutionAt.Sub(l.eventsList[j].nextExecutionAt).Seconds() < 1
	})
}

// remove removes the event from the list,
// the list order stays the same.
func (l *enabledEventsList) remove(key string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for i, e := range l.eventsList {
		if e.name() == key {
			l.eventsList = append(l.eventsList[:i], l.eventsList[i+1:]...)
			return
		}
	}
}

// remove removes all events of the given database from the list,
// the list order stays the same.
func (l *enabledEventsList) removeSchemaEvents(dbName string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for i, e := range l.eventsList {
		if e.edb.Name() == dbName {
			l.eventsList = append(l.eventsList[:i], l.eventsList[i+1:]...)
		}
	}
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
			r.status[evId] = r.status[evId]
			r.reAdd[evId] = false
		}
	}
}
