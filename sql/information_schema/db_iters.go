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

package information_schema

import (
	"github.com/dolthub/go-mysql-server/sql"
)

// information_schema row iterators enumerate every database in the catalog and then visit each one. A database can be
// dropped by another session between those two steps. MySQL's information_schema is a non-transactional view of the
// catalog, so a database that is dropped mid-enumeration contributes no rows instead of failing the whole query. The
// functions in this file are meant for catalog enumeration only: statements that address a database directly (SHOW
// TABLES, SHOW CREATE, ...) must keep reporting the error.

// iterDBTables wraps sql.DBTableIter, but ignores sql.ErrDatabaseNotFound errors from concurrent drops of |db|.
func iterDBTables(ctx *sql.Context, db sql.Database, cb func(sql.Table) (cont bool, err error)) error {
	err := sql.DBTableIter(ctx, db, cb)
	if sql.ErrDatabaseNotFound.Is(err) {
		return nil
	}
	return err
}

// dbTableNames wraps sql.Database.GetTableNames, but ignores sql.ErrDatabaseNotFound errors from concurrent drops of
// |db|.
func dbTableNames(ctx *sql.Context, db sql.Database) ([]string, error) {
	names, err := db.GetTableNames(ctx)
	if sql.ErrDatabaseNotFound.Is(err) {
		return nil, nil
	}
	return names, err
}

// forEachDBTable wraps sql.Database.GetTableNames and sql.Catalog.DatabaseTable, calling |cb| for each table of |db|,
// but ignores sql.ErrDatabaseNotFound errors from concurrent drops of |db|. If |db| is dropped at any step, including
// inside |cb|, the remaining tables are skipped and nil is returned. Rows |cb| already produced for earlier tables are
// kept. |cb| must handle sql.ErrDatabaseNotFound itself when it consults a different database, so that it does not
// truncate this one.
func forEachDBTable(ctx *sql.Context, c sql.Catalog, db sql.Database, cb func(sql.Table) error) error {
	names, err := dbTableNames(ctx, db)
	if err != nil {
		return err
	}

	for _, name := range names {
		tbl, _, err := c.DatabaseTable(ctx, db, name)
		if err == nil {
			err = cb(tbl)
		}
		if sql.ErrDatabaseNotFound.Is(err) {
			return nil
		}
		if err != nil {
			return err
		}
	}

	return nil
}

// dbViews wraps ViewsInDatabase, but ignores sql.ErrDatabaseNotFound errors from concurrent drops of |db|.
func dbViews(ctx *sql.Context, db sql.Database) ([]sql.ViewDefinition, error) {
	views, err := ViewsInDatabase(ctx, db)
	if sql.ErrDatabaseNotFound.Is(err) {
		return nil, nil
	}
	return views, err
}

// dbTriggers wraps sql.TriggerDatabase.GetTriggers, but ignores sql.ErrDatabaseNotFound errors from concurrent drops of
// |db|.
func dbTriggers(ctx *sql.Context, db sql.TriggerDatabase) ([]sql.TriggerDefinition, error) {
	triggers, err := db.GetTriggers(ctx)
	if sql.ErrDatabaseNotFound.Is(err) {
		return nil, nil
	}
	return triggers, err
}

// dbEvents wraps sql.EventDatabase.GetEvents, but ignores sql.ErrDatabaseNotFound errors from concurrent drops of |db|.
func dbEvents(ctx *sql.Context, db sql.EventDatabase) ([]sql.EventDefinition, error) {
	events, _, err := db.GetEvents(ctx)
	if sql.ErrDatabaseNotFound.Is(err) {
		return nil, nil
	}
	return events, err
}
