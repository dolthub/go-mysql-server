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
	*ProcessList
	*MemoryManager

	provider DatabaseProvider
	builtInFunctions FunctionRegistry
	mu       sync.RWMutex
	locks    sessionLocks
}

type tableLocks map[string]struct{}

type dbLocks map[string]tableLocks

type sessionLocks map[uint32]dbLocks

// NewCatalog returns a new empty Catalog with the given provider
func NewCatalog(provider DatabaseProvider) *Catalog {
	return &Catalog{
		MemoryManager:    NewMemoryManager(ProcessMemory),
		ProcessList:      NewProcessList(),
		provider:         provider,
		builtInFunctions: NewFunctionRegistry(),
		locks:            make(sessionLocks),
	}
}

var _ FunctionProvider = (*Catalog)(nil)

// AllDatabases returns all sliceDBProvider in the catalog.
func (c *Catalog) AllDatabases() []Database {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.provider.AllDatabases()
}

// CreateDatabase creates a new Database and adds it to the catalog.
func (c *Catalog) CreateDatabase(ctx *Context, dbName string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	mut, ok := c.provider.(MutableDatabaseProvider)
	if ok {
		return mut.CreateDatabase(ctx, dbName)
	} else {
		return ErrImmutableDatabaseProvider.New()
	}
}

// RemoveDatabase removes a database from the catalog.
func (c *Catalog) RemoveDatabase(ctx *Context, dbName string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	mut, ok := c.provider.(MutableDatabaseProvider)
	if ok {
		return mut.DropDatabase(ctx, dbName)
	} else {
		return ErrImmutableDatabaseProvider.New()
	}
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

// RegisterFunction registers the functions given, adding them to the built-in functions.
// Integrators with custom functions should typically use the FunctionProvider interface instead.
func (c *Catalog) RegisterFunction(fns ...Function) {
	for _, fn := range fns {
		c.builtInFunctions.MustRegister(fn)
	}
}

// Function returns the function with the name given, or sql.ErrFunctionNotFound if it doesn't exist
func (c *Catalog) Function(name string) (Function, error) {
	if fp, ok := c.provider.(FunctionProvider); ok {
		f, err := fp.Function(name)
		if err != nil && !ErrFunctionNotFound.Is(err) {
			return nil, err
		}
		return f, nil
	}

	return c.builtInFunctions.Function(name)
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
