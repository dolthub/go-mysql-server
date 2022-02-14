// Copyright 2021 Dolthub, Inc.
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

package analyzer

import (
	"fmt"
	"strings"
	"sync"

	"github.com/dolthub/go-mysql-server/internal/similartext"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression/function"
	"github.com/dolthub/go-mysql-server/sql/grant_tables"
)

type Catalog struct {
	GrantTables *grant_tables.GrantTables

	provider         sql.DatabaseProvider
	builtInFunctions function.Registry
	mu               sync.RWMutex
	locks            sessionLocks
}

var _ sql.Catalog = (*Catalog)(nil)
var _ sql.FunctionProvider = (*Catalog)(nil)

type tableLocks map[string]struct{}

type dbLocks map[string]tableLocks

type sessionLocks map[uint32]dbLocks

// NewCatalog returns a new empty Catalog with the given provider
func NewCatalog(provider sql.DatabaseProvider) *Catalog {
	return &Catalog{
		GrantTables:      grant_tables.CreateEmptyGrantTables(),
		provider:         provider,
		builtInFunctions: function.NewRegistry(),
		locks:            make(sessionLocks),
	}
}

func NewDatabaseProvider(dbs ...sql.Database) sql.DatabaseProvider {
	return sql.NewDatabaseProvider(dbs...)
}

func (c *Catalog) AllDatabases(ctx *sql.Context) []sql.Database {
	if c.GrantTables.Enabled {
		return grant_tables.NewPrivilegedDatabaseProvider(c.GrantTables, c.provider).AllDatabases(ctx)
	} else {
		return c.provider.AllDatabases(ctx)
	}
}

// CreateDatabase creates a new Database and adds it to the catalog.
func (c *Catalog) CreateDatabase(ctx *sql.Context, dbName string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	mut, ok := c.provider.(sql.MutableDatabaseProvider)
	if ok {
		return mut.CreateDatabase(ctx, dbName)
	} else {
		return sql.ErrImmutableDatabaseProvider.New()
	}
}

// RemoveDatabase removes a database from the catalog.
func (c *Catalog) RemoveDatabase(ctx *sql.Context, dbName string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	mut, ok := c.provider.(sql.MutableDatabaseProvider)
	if ok {
		return mut.DropDatabase(ctx, dbName)
	} else {
		return sql.ErrImmutableDatabaseProvider.New()
	}
}

func (c *Catalog) HasDB(ctx *sql.Context, db string) bool {
	if c.GrantTables.Enabled {
		return grant_tables.NewPrivilegedDatabaseProvider(c.GrantTables, c.provider).HasDatabase(ctx, db)
	} else {
		return c.provider.HasDatabase(ctx, db)
	}
}

// Database returns the database with the given name.
func (c *Catalog) Database(ctx *sql.Context, db string) (sql.Database, error) {
	if c.GrantTables.Enabled {
		return grant_tables.NewPrivilegedDatabaseProvider(c.GrantTables, c.provider).Database(ctx, db)
	} else {
		return c.provider.Database(ctx, db)
	}
}

// LockTable adds a lock for the given table and session client. It is assumed
// the database is the current database in use.
func (c *Catalog) LockTable(ctx *sql.Context, table string) {
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
func (c *Catalog) UnlockTables(ctx *sql.Context, id uint32) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var errors []string
	for db, tables := range c.locks[id] {
		for t := range tables {
			database, err := c.provider.Database(ctx, db)
			if err != nil {
				return err
			}

			table, _, err := database.GetTableInsensitive(ctx, t)
			if err == nil {
				if lockable, ok := table.(sql.Lockable); ok {
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
func (c *Catalog) Table(ctx *sql.Context, dbName, tableName string) (sql.Table, sql.Database, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	db, err := c.Database(ctx, dbName)
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
func (c *Catalog) TableAsOf(ctx *sql.Context, dbName, tableName string, asOf interface{}) (sql.Table, sql.Database, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	db, err := c.Database(ctx, dbName)
	if err != nil {
		return nil, nil, err
	}

	versionedDb, ok := db.(sql.VersionedDatabase)
	if !ok {
		return nil, nil, sql.ErrAsOfNotSupported.New(tableName)
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
func (c *Catalog) RegisterFunction(ctx *sql.Context, fns ...sql.Function) {
	for _, fn := range fns {
		err := c.builtInFunctions.Register(fn)
		if err != nil {
			panic(err)
		}
	}
}

// Function returns the function with the name given, or sql.ErrFunctionNotFound if it doesn't exist
func (c *Catalog) Function(ctx *sql.Context, name string) (sql.Function, error) {
	if fp, ok := c.provider.(sql.FunctionProvider); ok {
		f, err := fp.Function(ctx, name)
		if err != nil && !sql.ErrFunctionNotFound.Is(err) {
			return nil, err
		} else if f != nil {
			return f, nil
		}
	}

	return c.builtInFunctions.Function(ctx, name)
}

func suggestSimilarTables(db sql.Database, ctx *sql.Context, tableName string) error {
	tableNames, err := db.GetTableNames(ctx)
	if err != nil {
		return err
	}

	similar := similartext.Find(tableNames, tableName)
	return sql.ErrTableNotFound.New(tableName + similar)
}

func suggestSimilarTablesAsOf(db sql.VersionedDatabase, ctx *sql.Context, tableName string, time interface{}) error {
	tableNames, err := db.GetTableNamesAsOf(ctx, time)
	if err != nil {
		return err
	}

	similar := similartext.Find(tableNames, tableName)
	return sql.ErrTableNotFound.New(tableName + similar)
}
