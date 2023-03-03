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

package test

import (
	"github.com/dolthub/go-mysql-server/sql"
)

type Catalog struct {
	provider sql.DatabaseProvider
}

// NewCatalog returns a new empty Catalog with the given provider
func NewCatalog(provider sql.DatabaseProvider) sql.Catalog {
	return &Catalog{
		provider: provider,
	}
}

var _ sql.FunctionProvider = (*Catalog)(nil)
var _ sql.Catalog = (*Catalog)(nil)

// AllDatabases returns all sliceDBProvider in the catalog.
func (c *Catalog) AllDatabases(ctx *sql.Context) []sql.Database {
	return c.provider.AllDatabases(ctx)
}

// CreateDatabase creates a new Database and adds it to the catalog.
func (c *Catalog) CreateDatabase(ctx *sql.Context, dbName string, collation sql.CollationID) error {
	if collatedDb, ok := c.provider.(sql.CollatedDatabaseProvider); ok {
		return collatedDb.CreateCollatedDatabase(ctx, dbName, collation)
	} else if mut, ok := c.provider.(sql.MutableDatabaseProvider); ok {
		err := mut.CreateDatabase(ctx, dbName)
		if err != nil {
			return err
		}
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
	mut, ok := c.provider.(sql.MutableDatabaseProvider)
	if ok {
		return mut.DropDatabase(ctx, dbName)
	} else {
		return sql.ErrImmutableDatabaseProvider.New()
	}
}

func (c *Catalog) HasDB(ctx *sql.Context, db string) bool {
	return c.provider.HasDatabase(ctx, db)
}

// Database returns the database with the given name.
func (c *Catalog) Database(ctx *sql.Context, db string) (sql.Database, error) {
	return c.provider.Database(ctx, db)
}

// Table returns the table in the given database with the given name.
func (c *Catalog) Table(ctx *sql.Context, dbName, tableName string) (sql.Table, sql.Database, error) {
	db, err := c.Database(ctx, dbName)
	if err != nil {
		return nil, nil, err
	}

	tbl, ok, err := db.GetTableInsensitive(ctx, tableName)
	if err != nil {
		return nil, nil, err
	} else if !ok {
		return nil, nil, sql.ErrTableNotFound.New(tableName)
	}

	return tbl, db, nil
}

func (c *Catalog) TableAsOf(ctx *sql.Context, dbName, tableName string, asOf interface{}) (sql.Table, sql.Database, error) {
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
		return nil, nil, sql.ErrTableNotFound.New(tableName)
	}

	return tbl, versionedDb, nil
}

func (c *Catalog) RegisterFunction(ctx *sql.Context, fns ...sql.Function) {}

func (c *Catalog) Function(ctx *sql.Context, name string) (sql.Function, error) {
	return nil, sql.ErrFunctionNotFound.New(name)
}

func (c *Catalog) LockTable(ctx *sql.Context, table string) {}

func (c *Catalog) UnlockTables(ctx *sql.Context, id uint32) error {
	return nil
}

func (c *Catalog) Statistics(ctx *sql.Context) (sql.StatsReadWriter, error) {
	return nil, nil
}
