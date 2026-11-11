// Copyright 2026 Dolthub, Inc.
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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
)

// quiescableEventDatabase is an EventDatabase whose stored events and reload token are set directly by the test, the
// way an integrator's stored events change when a transaction commits.
type quiescableEventDatabase struct {
	*memory.Database
	mu     sync.Mutex
	events []sql.EventDefinition
	token  int
	// loaded receives a value each time the events are loaded.
	loaded chan struct{}
}

var _ sql.QuiescableEventDatabase = (*quiescableEventDatabase)(nil)

func (db *quiescableEventDatabase) setEvents(token int, events ...sql.EventDefinition) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.events = events
	db.token = token
}

func (db *quiescableEventDatabase) GetEvents(*sql.Context) ([]sql.EventDefinition, interface{}, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	select {
	case db.loaded <- struct{}{}:
	default:
	}
	return append([]sql.EventDefinition(nil), db.events...), db.token, nil
}

func (db *quiescableEventDatabase) NeedsToReloadEvents(_ *sql.Context, token interface{}) (bool, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	loadedToken, _ := token.(int)
	return loadedToken != db.token, nil
}

func (db *quiescableEventDatabase) UpdateLastExecuted(*sql.Context, string, time.Time) error {
	return nil
}

func (db *quiescableEventDatabase) QuiescableEvents() bool {
	return true
}

// singleDatabaseCatalog is a catalog of a single database, which is all the event executor needs from it.
type singleDatabaseCatalog struct {
	sql.Catalog
	db sql.Database
}

func (c singleDatabaseCatalog) AllDatabases(*sql.Context) []sql.Database {
	return []sql.Database{c.db}
}

// TestEventExecutorLoadsEventEnabledBeforeCommit verifies that a quiesced event executor runs an event that is enabled
// through the EventScheduler, even when the executor reloads events before the transaction enabling it commits.
func TestEventExecutorLoadsEventEnabledBeforeCommit(t *testing.T) {
	db := &quiescableEventDatabase{
		Database: memory.NewDatabase("db"),
		loaded:   make(chan struct{}, 1),
	}
	newCtx := func() (*sql.Context, error) {
		return sql.NewContext(context.Background(), sql.WithSession(sql.NewBaseSession())), nil
	}
	executed := make(chan struct{}, 1)
	runQuery := func(*sql.Context, string, string, string, string) error {
		select {
		case executed <- struct{}{}:
		default:
		}
		return nil
	}
	bThreads := sql.NewBackgroundThreads()
	defer bThreads.Shutdown()
	ee := newEventExecutor(bThreads, newCtx, runQuery, 1)
	ee.catalog = singleDatabaseCatalog{db: db}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go ee.start(ctx)

	now := time.Now()
	event := sql.EventDefinition{
		Name:         "event1",
		EventBody:    "SELECT 1",
		Status:       sql.EventStatus_Disable.String(),
		Definer:      "`root`@`localhost`",
		CreatedAt:    now,
		LastAltered:  now,
		Starts:       now.Add(-time.Hour),
		ExecuteEvery: "1 SECOND",
	}
	enabled := event
	enabled.Status = sql.EventStatus_Enable.String()

	// Enable the event through the executor as EventScheduler.UpdateEvent does, while the stored event is still
	// disabled because the transaction enabling it hasn't committed.
	db.setEvents(1, event)
	sqlCtx, err := newCtx()
	require.NoError(t, err)
	ee.updateEvent(sqlCtx, db, event.Name, enabled)
	ee.notify()

	select {
	case <-db.loaded:
	case <-time.After(5 * time.Second):
		t.Fatal("the event executor did not reload events after it was notified")
	}

	// The transaction commits, which changes the stored event without notifying the executor.
	db.setEvents(2, enabled)

	select {
	case <-executed:
	case <-time.After(5 * time.Second):
		t.Fatal("the enabled event was never executed")
	}
}
