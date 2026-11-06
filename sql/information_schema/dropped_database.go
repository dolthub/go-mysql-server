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
	. "github.com/dolthub/go-mysql-server/sql"
)

// information_schema row iterators enumerate every database in the catalog and then visit each one. A database can be
// dropped by another session between those two steps. MySQL's information_schema is a non-transactional view of the
// catalog, so a database that vanished mid-enumeration simply contributes no rows; it must not fail the whole query.
// The helpers below implement that for the access patterns the iterators use. Only the typed ErrDatabaseNotFound is
// treated this way; every other error is still returned. They are meant for catalog enumeration only: statements that
// address a database directly (SHOW TABLES, SHOW CREATE, ...) must keep reporting the error.

// iterDBTables is DBTableIter for catalog enumeration: a database dropped concurrently yields no tables.
func iterDBTables(ctx *Context, db Database, cb func(Table) (cont bool, err error)) error {
	err := DBTableIter(ctx, db, cb)
	if ErrDatabaseNotFound.Is(err) {
		return nil
	}
	return err
}

// dbTableNames is db.GetTableNames for catalog enumeration: a database dropped concurrently yields no table names.
func dbTableNames(ctx *Context, db Database) ([]string, error) {
	names, err := db.GetTableNames(ctx)
	if ErrDatabaseNotFound.Is(err) {
		return nil, nil
	}
	return names, err
}

// forEachDBTable calls cb for every table of db, resolving each name through the catalog. If the database is dropped
// at any step, including inside cb (for example when cb reads the table's indexes or constraints), the remaining
// tables of that database are skipped and nil is returned. Rows cb already produced for earlier tables are kept; a
// partial listing is acceptable because information_schema is not transactional. cb must handle
// ErrDatabaseNotFound itself when it consults a different database, so that it does not truncate this one.
func forEachDBTable(ctx *Context, c Catalog, db Database, cb func(Table) error) error {
	names, err := dbTableNames(ctx, db)
	if err != nil {
		return err
	}
	for _, name := range names {
		tbl, _, err := c.DatabaseTable(ctx, db, name)
		if err == nil {
			err = cb(tbl)
		}
		if ErrDatabaseNotFound.Is(err) {
			return nil
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// dbViews is ViewsInDatabase for catalog enumeration: a database dropped concurrently yields no views.
func dbViews(ctx *Context, db Database) ([]ViewDefinition, error) {
	views, err := ViewsInDatabase(ctx, db)
	if ErrDatabaseNotFound.Is(err) {
		return nil, nil
	}
	return views, err
}

// dbTriggers is TriggerDatabase.GetTriggers for catalog enumeration: a database dropped concurrently yields no triggers.
func dbTriggers(ctx *Context, db TriggerDatabase) ([]TriggerDefinition, error) {
	triggers, err := db.GetTriggers(ctx)
	if ErrDatabaseNotFound.Is(err) {
		return nil, nil
	}
	return triggers, err
}

// dbEvents is EventDatabase.GetEvents for catalog enumeration: a database dropped concurrently yields no events.
func dbEvents(ctx *Context, db EventDatabase) ([]EventDefinition, error) {
	events, _, err := db.GetEvents(ctx)
	if ErrDatabaseNotFound.Is(err) {
		return nil, nil
	}
	return events, err
}
