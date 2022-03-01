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
	"sync"

	"github.com/dolthub/go-mysql-server/sql"
)

type ExclusiveLock struct {
	clientId uint32
	mu       *sync.Mutex
}

type LockManager interface {
	// HoldTableLock informs the LockManager to reserve a table lock by the called client.
	HoldTableLock(ctx *sql.Context, databaseName, tableName string) error

	// PollTableLock polls the status of a table and exits when the table lock is not held by any other client except the
	// current client.
	PollTableLock(ctx *sql.Context, databaseName, tableName string) error

	// ReleaseTableLocksHeldByClient release all held locks by the given client id.
	ReleaseTableLocksHeldByClient(ctx *sql.Context, id uint32) error
}

type lockManagerImpl struct {
	locksMap map[string]map[string]*ExclusiveLock // we will need to update this
	catalog  sql.Catalog
	mu       sync.RWMutex
}

var _ LockManager = &lockManagerImpl{}

func NewLockManager(c sql.Catalog) *lockManagerImpl {
	dbMap := make(map[string]map[string]*ExclusiveLock)

	return &lockManagerImpl{locksMap: dbMap, catalog: c}
}

func (l *lockManagerImpl) HoldTableLock(ctx *sql.Context, databaseName, tableName string) error {
	// Validate that the table exists in the catalog
	l.mu.Lock()
	defer l.mu.Unlock()
	_, _, err := l.catalog.Table(ctx, databaseName, tableName)
	if err != nil {
		return err
	}

	clientId := ctx.ID()

	if _, ok := l.locksMap[databaseName]; !ok {
		l.locksMap[databaseName] = make(map[string]*ExclusiveLock)
	}

	lock, ok := l.locksMap[databaseName][tableName]
	if !ok {
		lock = &ExclusiveLock{clientId: 0, mu: &sync.Mutex{}}
	}

	// Don't lock the node again if the current client already has access to it. Otherwise, we will have a deadlock.
	if lock.clientId == clientId {
		return nil
	}

	lock.mu.Lock() // block on the lock
	lock.clientId = clientId
	l.locksMap[databaseName][tableName] = lock

	return nil
}

func (l *lockManagerImpl) PollTableLock(ctx *sql.Context, databaseName, tableName string) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	_, _, err := l.catalog.Table(ctx, databaseName, tableName)
	if err != nil {
		return err
	}

	_, ok := l.locksMap[databaseName]
	if !ok {
		return nil
	}

	lock, ok := l.locksMap[databaseName][tableName]
	if !ok {
		return nil
	}

	if lock.clientId == ctx.ID() {
		return nil
	}

	lock.mu.Lock() // TODO: Probably not the right thing to do here. We should be intentional about who can ever actually
	// lock the mu.
	lock.mu.Unlock()

	return nil
}

func (l *lockManagerImpl) ReleaseTableLocksHeldByClient(ctx *sql.Context, id uint32) error {
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
