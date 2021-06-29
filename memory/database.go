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

package memory

import (
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
)

// Database is an in-memory database.
type Database struct {
	name              string
	tables            map[string]sql.Table
	triggers          []sql.TriggerDefinition
	storedProcedures  []sql.StoredProcedureDetails
	primaryKeyIndexes bool
}

var _ sql.Database = (*Database)(nil)
var _ sql.TableCreator = (*Database)(nil)
var _ sql.TableDropper = (*Database)(nil)
var _ sql.TableRenamer = (*Database)(nil)
var _ sql.TriggerDatabase = (*Database)(nil)
var _ sql.StoredProcedureDatabase = (*Database)(nil)

// NewDatabase creates a new database with the given name.
func NewDatabase(name string) *Database {
	return &Database{
		name:   name,
		tables: map[string]sql.Table{},
	}
}

// EnablePrimaryKeyIndexes causes every table created in this database to use an index on its primary keys
func (d *Database) EnablePrimaryKeyIndexes() {
	d.primaryKeyIndexes = true
}

// Name returns the database name.
func (d *Database) Name() string {
	return d.name
}

// Tables returns all tables in the database.
func (d *Database) Tables() map[string]sql.Table {
	return d.tables
}

func (d *Database) GetTableInsensitive(ctx *sql.Context, tblName string) (sql.Table, bool, error) {
	tbl, ok := sql.GetTableInsensitive(tblName, d.tables)
	return tbl, ok, nil
}

func (d *Database) GetTableNames(ctx *sql.Context) ([]string, error) {
	tblNames := make([]string, 0, len(d.tables))
	for k := range d.tables {
		tblNames = append(tblNames, k)
	}

	return tblNames, nil
}

// HistoryDatabase is a test-only VersionedDatabase implementation. It only supports exact lookups, not AS OF queries
// between two revisions. It's constructed just like its non-versioned sibling, but it can receive updates to particular
// tables via the AddTableAsOf method. Consecutive calls to AddTableAsOf with the same table must install new versions
// of the named table each time, with ascending version identifiers, for this to work.
type HistoryDatabase struct {
	Database
	Revisions    map[string]map[interface{}]sql.Table
	currRevision interface{}
}

var _ sql.VersionedDatabase = (*HistoryDatabase)(nil)

func (db *HistoryDatabase) GetTableInsensitiveAsOf(ctx *sql.Context, tblName string, time interface{}) (sql.Table, bool, error) {
	table, ok := db.Revisions[strings.ToLower(tblName)][time]
	if ok {
		return table, true, nil
	}

	// If we have revisions for the named table, but not the named revision, consider it not found.
	if _, ok := db.Revisions[strings.ToLower(tblName)]; ok {
		return nil, false, sql.ErrTableNotFound.New(tblName)
	}

	// Otherwise (this table has no revisions), return it as an unversioned lookup
	return db.GetTableInsensitive(ctx, tblName)
}

func (db *HistoryDatabase) GetTableNamesAsOf(ctx *sql.Context, time interface{}) ([]string, error) {
	// TODO: this can't make any queries fail (only used for error messages on table lookup failure), but would be nice
	//  to support better.
	return db.GetTableNames(ctx)
}

func NewHistoryDatabase(name string) *HistoryDatabase {
	return &HistoryDatabase{
		Database:  *NewDatabase(name),
		Revisions: make(map[string]map[interface{}]sql.Table),
	}
}

// Adds a table with an asOf revision key. The table given becomes the current version for the name given.
func (db *HistoryDatabase) AddTableAsOf(name string, t sql.Table, asOf interface{}) {
	// TODO: this won't handle table names that vary only in case
	if _, ok := db.Revisions[strings.ToLower(name)]; !ok {
		db.Revisions[strings.ToLower(name)] = make(map[interface{}]sql.Table)
	}

	db.Revisions[strings.ToLower(name)][asOf] = t
	db.tables[name] = t
}

// AddTable adds a new table to the database.
func (d *Database) AddTable(name string, t sql.Table) {
	d.tables[name] = t
}

// CreateTable creates a table with the given name and schema
func (d *Database) CreateTable(ctx *sql.Context, name string, schema sql.Schema) error {
	_, ok := d.tables[name]
	if ok {
		return sql.ErrTableAlreadyExists.New(name)
	}

	table := NewTable(name, schema)
	if d.primaryKeyIndexes {
		table.EnablePrimaryKeyIndexes()
	}
	d.tables[name] = table
	return nil
}

// DropTable drops the table with the given name
func (d *Database) DropTable(ctx *sql.Context, name string) error {
	_, ok := d.tables[name]
	if !ok {
		return sql.ErrTableNotFound.New(name)
	}

	delete(d.tables, name)
	return nil
}

func (d *Database) RenameTable(ctx *sql.Context, oldName, newName string) error {
	tbl, ok := d.tables[oldName]
	if !ok {
		// Should be impossible (engine already checks this condition)
		return sql.ErrTableNotFound.New(oldName)
	}

	_, ok = d.tables[newName]
	if ok {
		return sql.ErrTableAlreadyExists.New(newName)
	}

	tbl.(*Table).name = newName
	d.tables[newName] = tbl
	delete(d.tables, oldName)

	return nil
}

func (d *Database) GetTriggers(ctx *sql.Context) ([]sql.TriggerDefinition, error) {
	var triggers []sql.TriggerDefinition
	for _, def := range d.triggers {
		triggers = append(triggers, def)
	}
	return triggers, nil
}

func (d *Database) CreateTrigger(ctx *sql.Context, definition sql.TriggerDefinition) error {
	d.triggers = append(d.triggers, definition)
	return nil
}

func (d *Database) DropTrigger(ctx *sql.Context, name string) error {
	found := false
	for i, trigger := range d.triggers {
		if trigger.Name == name {
			d.triggers = append(d.triggers[:i], d.triggers[i+1:]...)
			found = true
			break
		}
	}
	if !found {
		return sql.ErrTriggerDoesNotExist.New(name)
	}
	return nil
}

// GetStoredProcedures implements sql.StoredProcedureDatabase
func (d *Database) GetStoredProcedures(ctx *sql.Context) ([]sql.StoredProcedureDetails, error) {
	var spds []sql.StoredProcedureDetails
	for _, spd := range d.storedProcedures {
		spds = append(spds, spd)
	}
	return spds, nil
}

// SaveStoredProcedure implements sql.StoredProcedureDatabase
func (d *Database) SaveStoredProcedure(ctx *sql.Context, spd sql.StoredProcedureDetails) error {
	loweredName := strings.ToLower(spd.Name)
	for _, existingSpd := range d.storedProcedures {
		if strings.ToLower(existingSpd.Name) == loweredName {
			return sql.ErrStoredProcedureAlreadyExists.New(spd.Name)
		}
	}
	d.storedProcedures = append(d.storedProcedures, spd)
	return nil
}

// DropStoredProcedure implements sql.StoredProcedureDatabase
func (d *Database) DropStoredProcedure(ctx *sql.Context, name string) error {
	loweredName := strings.ToLower(name)
	found := false
	for i, spd := range d.storedProcedures {
		if strings.ToLower(spd.Name) == loweredName {
			d.storedProcedures = append(d.storedProcedures[:i], d.storedProcedures[i+1:]...)
			found = true
			break
		}
	}
	if !found {
		return sql.ErrStoredProcedureDoesNotExist.New(name)
	}
	return nil
}

type ReadOnlyDatabase struct {
	*HistoryDatabase
}

var _ sql.ReadOnlyDatabase = ReadOnlyDatabase{}

func NewReadOnlyDatabase(name string) ReadOnlyDatabase {
	h := NewHistoryDatabase(name)
	return ReadOnlyDatabase{h}
}

func (d ReadOnlyDatabase) IsReadOnly() bool {
	return true
}
