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
	"github.com/dolthub/go-mysql-server/sql/mysql_db"
)

type Catalog struct {
	MySQLDb *mysql_db.MySQLDb

	provider         sql.DatabaseProvider
	builtInFunctions function.Registry
	mu               sync.RWMutex
	locks            sessionLocks
}

var _ sql.Catalog = (*Catalog)(nil)
var _ sql.FunctionProvider = (*Catalog)(nil)
var _ sql.TableFunctionProvider = (*Catalog)(nil)
var _ sql.ExternalStoredProcedureProvider = (*Catalog)(nil)

type tableLocks map[string]struct{}

type dbLocks map[string]tableLocks

type sessionLocks map[uint32]dbLocks

// NewCatalog returns a new empty Catalog with the given provider
func NewCatalog(provider sql.DatabaseProvider) *Catalog {
	return &Catalog{
		MySQLDb:          mysql_db.CreateEmptyMySQLDb(),
		provider:         provider,
		builtInFunctions: function.NewRegistry(),
		locks:            make(sessionLocks),
	}
}

func NewDatabaseProvider(dbs ...sql.Database) sql.DatabaseProvider {
	return sql.NewDatabaseProvider(dbs...)
}

func (c *Catalog) AllDatabases(ctx *sql.Context) []sql.Database {
	if c.MySQLDb.Enabled {
		return mysql_db.NewPrivilegedDatabaseProvider(c.MySQLDb, c.provider).AllDatabases(ctx)
	} else {
		return c.provider.AllDatabases(ctx)
	}
}

// CreateDatabase creates a new Database and adds it to the catalog.
func (c *Catalog) CreateDatabase(ctx *sql.Context, dbName string, collation sql.CollationID) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if collatedDbProvider, ok := c.provider.(sql.CollatedDatabaseProvider); ok {
		// If the database provider supports creation with a collation, then we call that function directly
		return collatedDbProvider.CreateCollatedDatabase(ctx, dbName, collation)
	} else if mut, ok := c.provider.(sql.MutableDatabaseProvider); ok {
		err := mut.CreateDatabase(ctx, dbName)
		if err != nil {
			return err
		}
		// It's possible that the db provider doesn't support creation with a collation, in which case we create the
		// database and then set the collation. If the database doesn't support collations at all, then we ignore the
		// provided collation rather than erroring.
		if db, err := c.Database(ctx, dbName); err == nil {
			if collatedDb, ok := db.(sql.CollatedDatabase); ok {
				return collatedDb.SetCollation(ctx, collation)
			}
		}
		return nil
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
	if c.MySQLDb.Enabled {
		return mysql_db.NewPrivilegedDatabaseProvider(c.MySQLDb, c.provider).HasDatabase(ctx, db)
	} else {
		return c.provider.HasDatabase(ctx, db)
	}
}
// UseDatabase returns the database with the given name, and switches the working set.
func (c *Catalog) UseDatabase(ctx *sql.Context, db string) (sql.Database, error) {
	if c.MySQLDb.Enabled {
		return mysql_db.NewPrivilegedDatabaseProvider(c.MySQLDb, c.provider).UseDatabase(ctx, db)
	} else {
		return c.provider.UseDatabase(ctx, db)
	}
}


// Database returns the database with the given name.
func (c *Catalog) Database(ctx *sql.Context, db string) (sql.Database, error) {
	if c.MySQLDb.Enabled {
		return mysql_db.NewPrivilegedDatabaseProvider(c.MySQLDb, c.provider).Database(ctx, db)
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

// ExternalStoredProcedure implements sql.ExternalStoredProcedureProvider
func (c *Catalog) ExternalStoredProcedure(ctx *sql.Context, name string, numOfParams int) (*sql.ExternalStoredProcedureDetails, error) {
	if espp, ok := c.provider.(sql.ExternalStoredProcedureProvider); ok {
		esp, err := espp.ExternalStoredProcedure(ctx, name, numOfParams)
		if err != nil {
			return nil, err
		} else if esp != nil {
			return esp, nil
		}
	}

	return nil, nil
}

// ExternalStoredProcedures implements sql.ExternalStoredProcedureProvider
func (c *Catalog) ExternalStoredProcedures(ctx *sql.Context, name string) ([]sql.ExternalStoredProcedureDetails, error) {
	if espp, ok := c.provider.(sql.ExternalStoredProcedureProvider); ok {
		esps, err := espp.ExternalStoredProcedures(ctx, name)
		if err != nil {
			return nil, err
		} else if esps != nil {
			return esps, nil
		}
	}

	return nil, nil
}

// TableFunction implements the TableFunctionProvider interface
func (c *Catalog) TableFunction(ctx *sql.Context, name string) (sql.TableFunction, error) {
	if fp, ok := c.provider.(sql.TableFunctionProvider); ok {
		tf, err := fp.TableFunction(ctx, name)
		if err != nil {
			return nil, err
		} else if tf != nil {
			return tf, nil
		}
	}

	return nil, sql.ErrTableFunctionNotFound.New(name)
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
