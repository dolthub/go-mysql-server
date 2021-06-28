// Copyright 2020-2021 Dolthub, Inc.
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

package sql

import (
	"fmt"
	"strings"
	"sync"

	"github.com/dolthub/go-mysql-server/internal/similartext"

	"gopkg.in/src-d/go-errors.v1"
)

// ErrDatabaseNotFound is thrown when a database is not found
var ErrDatabaseNotFound = errors.NewKind("database not found: %s")

// ErrNoDatabaseSelected is thrown when a database is not selected and the query requires one
var ErrNoDatabaseSelected = errors.NewKind("no database selected")

// ErrAsOfNotSupported is thrown when an AS OF query is run on a database that can't support it
var ErrAsOfNotSupported = errors.NewKind("AS OF not supported for database %s")

// ErrIncompatibleAsOf is thrown when an AS OF clause is used in an incompatible manner, such as when using an AS OF
// expression with a view when the view definition has its own AS OF expressions.
var ErrIncompatibleAsOf = errors.NewKind("incompatible use of AS OF: %s")

// Catalog holds databases, tables and functions.
type Catalog struct {
	FunctionRegistry
	*ProcessList
	*MemoryManager

	mu       sync.RWMutex
	provider MutableDatabaseProvider
	locks    sessionLocks
}

type tableLocks map[string]struct{}

type dbLocks map[string]tableLocks

type sessionLocks map[uint32]dbLocks

// NewCatalog returns a new empty Catalog.
func NewCatalog() *Catalog {
	return NewCatalogWithDbProvider(&sliceDBProvider{})
}

// NewCatalogWithDbProvider returns a new empty Catalog.
func NewCatalogWithDbProvider(provider MutableDatabaseProvider) *Catalog {
	return &Catalog{
		FunctionRegistry: NewFunctionRegistry(),
		MemoryManager:    NewMemoryManager(ProcessMemory),
		ProcessList:      NewProcessList(),
		provider:         provider,
		locks:            make(sessionLocks),
	}
}

// AllDatabases returns all sliceDBProvider in the catalog.
func (c *Catalog) AllDatabases() []Database {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.provider.AllDatabases()
}

// AddDatabase adds a new database to the catalog.
func (c *Catalog) AddDatabase(db Database) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.provider.AddDatabase(db)
}

// RemoveDatabase removes a database from the catalog.
func (c *Catalog) RemoveDatabase(dbName string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.provider.DropDatabase(dbName)
}

func (c *Catalog) HasDB(db string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.provider.HasDatabase(db)
}

// Database returns the database with the given name.
func (c *Catalog) Database(db string) (Database, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.provider.Database(db)
}

// LockTable adds a lock for the given table and session client. It is assumed
// the database is the current database in use.
func (c *Catalog) LockTable(ctx *Context, table string) {
	id := ctx.ID()
	db := ctx.GetCurrentDatabase()

	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.locks[id]; !ok {
		c.locks[id] = make(dbLocks)
	}

	if _, ok := c.locks[id][db]; !ok {
		c.locks[id][db] = make(tableLocks)
	}

	c.locks[id][db][table] = struct{}{}
}

// UnlockTables unlocks all tables for which the given session client has a
// lock.
func (c *Catalog) UnlockTables(ctx *Context, id uint32) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var errors []string
	for db, tables := range c.locks[id] {
		for t := range tables {
			database, err := c.provider.Database(db)
			if err != nil {
				return err
			}

			table, _, err := database.GetTableInsensitive(ctx, t)
			if err == nil {
				if lockable, ok := table.(Lockable); ok {
					if e := lockable.Unlock(ctx, id); e != nil {
						errors = append(errors, e.Error())
					}
				}
			} else {
				errors = append(errors, err.Error())
			}
		}
	}

	delete(c.locks, id)
	if len(errors) > 0 {
		return fmt.Errorf("error unlocking tables for %d: %s", id, strings.Join(errors, ", "))
	}

	return nil
}

// Table returns the table in the given database with the given name.
func (c *Catalog) Table(ctx *Context, dbName, tableName string) (Table, Database, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	db, err := c.Database(dbName)
	if err != nil {
		return nil, nil, err
	}

	tbl, ok, err := db.GetTableInsensitive(ctx, tableName)
	if err != nil {
		return nil, nil, err
	} else if !ok {
		return nil, nil, suggestSimilarTables(db, ctx, tableName)
	}

	return tbl, db, nil
}

// TableAsOf returns the table in the given database with the given name, as it existed at the time given. The database
// named must support timed queries.
func (c *Catalog) TableAsOf(ctx *Context, dbName, tableName string, asOf interface{}) (Table, Database, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	db, err := c.Database(dbName)
	if err != nil {
		return nil, nil, err
	}

	versionedDb, ok := db.(VersionedDatabase)
	if !ok {
		return nil, nil, ErrAsOfNotSupported.New(tableName)
	}

	tbl, ok, err := versionedDb.GetTableInsensitiveAsOf(ctx, tableName, asOf)

	if err != nil {
		return nil, nil, err
	} else if !ok {
		return nil, nil, suggestSimilarTablesAsOf(versionedDb, ctx, tableName, asOf)
	}

	return tbl, versionedDb, nil
}

func suggestSimilarTables(db Database, ctx *Context, tableName string) error {
	tableNames, err := db.GetTableNames(ctx)
	if err != nil {
		return err
	}

	similar := similartext.Find(tableNames, tableName)
	return ErrTableNotFound.New(tableName + similar)
}

func suggestSimilarTablesAsOf(db VersionedDatabase, ctx *Context, tableName string, time interface{}) error {
	tableNames, err := db.GetTableNamesAsOf(ctx, time)
	if err != nil {
		return err
	}

	similar := similartext.Find(tableNames, tableName)
	return ErrTableNotFound.New(tableName + similar)
}

// sliceDBProvider is a collection of Database.
type sliceDBProvider []Database

var _ DatabaseProvider = &sliceDBProvider{}

// Database returns the Database with the given name if it exists.
func (d *sliceDBProvider) Database(name string) (Database, error) {
	if len(*d) == 0 {
		return nil, ErrDatabaseNotFound.New(name)
	}

	name = strings.ToLower(name)
	var dbNames []string
	for _, db := range *d {
		if strings.ToLower(db.Name()) == name {
			return db, nil
		}
		dbNames = append(dbNames, db.Name())
	}
	similar := similartext.Find(dbNames, name)
	return nil, ErrDatabaseNotFound.New(name + similar)
}

// HasDatabase returns the Database with the given name if it exists.
func (d *sliceDBProvider) HasDatabase(name string) bool {
	name = strings.ToLower(name)
	for _, db := range *d {
		if strings.ToLower(db.Name()) == name {
			return true
		}
	}
	return false
}

// AllDatabases returns the Database with the given name if it exists.
func (d *sliceDBProvider) AllDatabases() []Database {
	return *d
}

// AddDatabase adds a new database.
func (d *sliceDBProvider) AddDatabase(db Database) {
	*d = append(*d, db)
}

// DropDatabase removes a database.
func (d *sliceDBProvider) DropDatabase(dbName string) {
	idx := -1
	for i, db := range *d {
		if db.Name() == dbName {
			idx = i
			break
		}
	}

	if idx != -1 {
		*d = append((*d)[:idx], (*d)[idx+1:]...)
	}
}
