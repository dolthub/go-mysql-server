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
	LockTable(ctx *sql.Context, tableName string) error
	UnlockTable(ctx *sql.Context, tableNames string) error
}

// TODO: Need this to work for muliple databases
type lockManagerImpl struct {
	tables map[string]*ExclusiveLock // we will need to update this
}

var _ LockManager = &lockManagerImpl{}

// TODO: Need to update internal state when tables are added and dropped
func NewLockManager(ctx *sql.Context, c sql.Catalog) (*lockManagerImpl, error) {
	dbName := ctx.GetCurrentDatabase()
	db, err := c.Database(ctx, dbName)
	if err != nil {
		return nil, err
	}

	tables, err := db.GetTableNames(ctx)
	if err != nil {
		return nil, err
	}

	tableMap := make(map[string]*ExclusiveLock)
	for _, table := range tables {
		tableMap[table] = &ExclusiveLock{clientId: ctx.ID(), mu: &sync.Mutex{}}
	}

	return &lockManagerImpl{tables: tableMap}, nil
}

func (l *lockManagerImpl) LockTable(ctx *sql.Context, tableName string) error {
	clientId := ctx.ID()

	lock, ok := l.tables[tableName]
	if !ok {
		return fmt.Errorf("Lock called on table not found")
	}

	lock.mu.Lock()
	lock.clientId = clientId

	return nil
}

func (l *lockManagerImpl) UnlockTable(ctx *sql.Context, tableName string) error {
	lock, ok := l.tables[tableName]
	if !ok {
		return fmt.Errorf("Unlock called on table that does not exist")
	}

	lock.mu.Unlock()
	lock.clientId = 0

	return nil
}
