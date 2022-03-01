// Copyright 2022 Dolthub, Inc.
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

package locks

import (
	"fmt"
	"github.com/dolthub/go-mysql-server/sql"
	"sync"
)

type ExclusiveLock struct {
	clientId uint32
	mu       *sync.Mutex
}

type LockManager interface {
	LockTable(ctx *sql.Context, databaseName, tableName string) error
	UnlockTable(ctx *sql.Context, databaseName, tableName string) error
	ReleaseLocksHeldByClient(ctx *sql.Context, id uint32) error
}

type lockManagerImpl struct {
	locksMap map[string]map[string]*ExclusiveLock // we will need to update this
	catalog  sql.Catalog
	mu       sync.RWMutex
}

var _ LockManager = &lockManagerImpl{}

// TODO: Need to update internal state when tables are added and dropped
func NewLockManager(c sql.Catalog) *lockManagerImpl {
	dbMap := make(map[string]map[string]*ExclusiveLock)

	return &lockManagerImpl{locksMap: dbMap, catalog: c}
}

func (l *lockManagerImpl) LockTable(ctx *sql.Context, databaseName, tableName string) error {
	// Validate that the table exists in the catalog
	l.mu.Lock()
	defer l.mu.Unlock()
	_, _, err := l.catalog.Table(ctx, databaseName, tableName)
	if err != nil {
		return err
	}

	clientId := ctx.ID()

	tableMap, ok := l.locksMap[databaseName]
	if !ok {
		tableMap = make(map[string]*ExclusiveLock)
		l.locksMap[databaseName] = tableMap
	}

	lock, ok := tableMap[tableName]
	if !ok {
		lock = &ExclusiveLock{clientId: 0, mu: &sync.Mutex{}}
		tableMap[tableName] = lock
	}

	// Don't lock the node again if the current client already has access to it. Otherwise, we will have a deadlock.
	if lock.clientId == clientId {
		return nil
	}

	lock.mu.Lock()
	lock.clientId = clientId

	return nil
}

func (l *lockManagerImpl) UnlockTable(ctx *sql.Context, databaseName, tableName string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	// Validate that the table exists in the catalog
	_, _, err := l.catalog.Table(ctx, databaseName, tableName)
	if err != nil {
		return err
	}

	tableMap, ok := l.locksMap[databaseName]
	if !ok {
		return fmt.Errorf("database not found")
	}

	lock, ok := tableMap[tableName]
	if !ok {
		return fmt.Errorf("table not found")
	}
	lock.mu.Unlock()
	lock.clientId = 0

	return nil
}

func (l *lockManagerImpl) ReleaseLocksHeldByClient(ctx *sql.Context, id uint32) error {
	for _, tableMap := range l.locksMap {
		for _, lock := range tableMap {
			if lock.clientId == id {
				lock.mu.Unlock()
				lock.clientId = 0 // reset the client id
			}
		}
	}

	return nil
}
