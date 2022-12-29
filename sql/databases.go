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

package sql

import "strings"

// Databaser is a node that contains a reference to a database.
type Databaser interface {
	// Database the current database.
	Database() Database
	// WithDatabase returns a new node instance with the database replaced with
	// the one given as parameter.
	WithDatabase(Database) (Node, error)
}

// Databaseable is a node with a string reference to a database
type Databaseable interface {
	Database() string
}

// MultiDatabaser is a node that contains a reference to a database provider. This interface is intended for very
// specific nodes that must resolve databases during execution time rather than during analysis, such as block
// statements where the execution of a nested statement in the block may affect future statements within that same block.
type MultiDatabaser interface {
	// DatabaseProvider returns the current DatabaseProvider.
	DatabaseProvider() DatabaseProvider
	// WithDatabaseProvider returns a new node instance with the database provider replaced with the one given as parameter.
	WithDatabaseProvider(DatabaseProvider) (Node, error)
}

// Database represents the database.
type Database interface {
	Nameable

	// GetTableInsensitive retrieves a table by its case insensitive name.  Implementations should look for exact
	// (case-sensitive matches) first.  If no exact matches are found then any table matching the name case insensitively
	// should be returned.  If there is more than one table that matches a case insensitive comparison the resolution
	// strategy is not defined.
	GetTableInsensitive(ctx *Context, tblName string) (Table, bool, error)

	// GetTableNames returns the table names of every table in the database. It does not return the names of temporary
	// tables
	GetTableNames(ctx *Context) ([]string, error)
}

type ReadOnlyDatabase interface {
	Database

	// IsReadOnly returns whether this database is read-only.
	IsReadOnly() bool
}

// VersionedDatabase is a Database that can return tables as they existed at different points in time. The engine
// supports queries on historical table data via the AS OF construct introduced in SQL 2011.
type VersionedDatabase interface {
	Database

	// GetTableInsensitiveAsOf retrieves a table by its case-insensitive name with the same semantics as
	// Database.GetTableInsensitive, but at a particular revision of the database. Implementors must choose which types
	// of expressions to accept as revision names.
	GetTableInsensitiveAsOf(ctx *Context, tblName string, asOf interface{}) (Table, bool, error)

	// GetTableNamesAsOf returns the table names of every table in the database as of the revision given. Implementors
	// must choose which types of expressions to accept as revision names.
	GetTableNamesAsOf(ctx *Context, asOf interface{}) ([]string, error)
}

// CollatedDatabase is a Database that may store and update its collation.
type CollatedDatabase interface {
	Database

	// GetCollation returns this database's collation.
	GetCollation(ctx *Context) CollationID

	// SetCollation updates this database's collation.
	SetCollation(ctx *Context, collation CollationID) error
}

// TriggerDatabase is a Database that supports the creation and execution of triggers. The engine handles all parsing
// and execution logic for triggers. Integrators are not expected to parse or understand the trigger definitions, but
// must store and return them when asked.
type TriggerDatabase interface {
	Database

	// GetTriggers returns all trigger definitions for the database
	GetTriggers(ctx *Context) ([]TriggerDefinition, error)

	// CreateTrigger is called when an integrator is asked to create a trigger. The create trigger statement string is
	// provided to store, along with the name of the trigger.
	CreateTrigger(ctx *Context, definition TriggerDefinition) error

	// DropTrigger is called when a trigger should no longer be stored. The name has already been validated.
	// Returns ErrTriggerDoesNotExist if the trigger was not found.
	DropTrigger(ctx *Context, name string) error
}

// TemporaryTableDatabase is a database that can query the session (which manages the temporary table state) to
// retrieve the name of all temporary tables.
type TemporaryTableDatabase interface {
	GetAllTemporaryTables(ctx *Context) ([]Table, error)
}

// TableCopierDatabase is a database that can copy a source table's data (without preserving indexed, fks, etc.) into
// another destination table.
type TableCopierDatabase interface {
	// CopyTableData copies the sourceTable data to the destinationTable and returns the number of rows copied.
	CopyTableData(ctx *Context, sourceTable string, destinationTable string) (uint64, error)
}

// GetTableInsensitive implements a case insensitive map lookup for tables keyed off of the table name.
// Looks for exact matches first.  If no exact matches are found then any table matching the name case insensitively
// should be returned.  If there is more than one table that matches a case insensitive comparison the resolution
// strategy is not defined.
func GetTableInsensitive(tblName string, tables map[string]Table) (Table, bool) {
	if tbl, ok := tables[tblName]; ok {
		return tbl, true
	}

	lwrName := strings.ToLower(tblName)

	for k, tbl := range tables {
		if lwrName == strings.ToLower(k) {
			return tbl, true
		}
	}

	return nil, false
}

// GetTableNameInsensitive implements a case insensitive search of a slice of table names. It looks for exact matches
// first.  If no exact matches are found then any table matching the name case insensitively should be returned.  If
// there is more than one table that matches a case insensitive comparison the resolution strategy is not defined.
func GetTableNameInsensitive(tblName string, tableNames []string) (string, bool) {
	for _, name := range tableNames {
		if tblName == name {
			return name, true
		}
	}

	lwrName := strings.ToLower(tblName)

	for _, name := range tableNames {
		if lwrName == strings.ToLower(name) {
			return name, true
		}
	}

	return "", false
}

// DBTableIter iterates over all tables returned by db.GetTableNames() calling cb for each one until all tables have
// been processed, or an error is returned from the callback, or the cont flag is false when returned from the callback.
func DBTableIter(ctx *Context, db Database, cb func(Table) (cont bool, err error)) error {
	names, err := db.GetTableNames(ctx)

	if err != nil {
		return err
	}

	for _, name := range names {
		tbl, ok, err := db.GetTableInsensitive(ctx, name)

		if err != nil {
			return err
		} else if !ok {
			return ErrTableNotFound.New(name)
		}

		cont, err := cb(tbl)

		if err != nil {
			return err
		}

		if !cont {
			break
		}
	}

	return nil
}

// TableCreator should be implemented by databases that can create new tables.
type TableCreator interface {
	// CreateTable creates the table with the given name and schema. If a table with that name already exists, must return
	// sql.ErrTableAlreadyExists.
	CreateTable(ctx *Context, name string, schema PrimaryKeySchema, collation CollationID) error
}

// IndexedTableCreator should be implemented by databases that create new tables where they have a Primary Key that has columns that have prefix lengths.
type IndexedTableCreator interface {
	CreateIndexedTable(ctx *Context, name string, schema PrimaryKeySchema, idxDef IndexDef, collation CollationID) error
}

// TemporaryTableCreator is a database that can create temporary tables that persist only as long as the session.
// Note that temporary tables with the same name as persisted tables take precedence in most SQL operations.
type TemporaryTableCreator interface {
	Database
	// CreateTemporaryTable creates the table with the given name and schema. If a temporary table with that name already exists, must
	// return sql.ErrTableAlreadyExists
	CreateTemporaryTable(ctx *Context, name string, schema PrimaryKeySchema, collation CollationID) error
}

// TableDropper should be implemented by databases that can drop tables.
type TableDropper interface {
	DropTable(ctx *Context, name string) error
}

// TableRenamer should be implemented by databases that can rename tables.
type TableRenamer interface {
	// RenameTable renames a table from oldName to newName as given. If a table with newName already exists, must return
	// sql.ErrTableAlreadyExists.
	RenameTable(ctx *Context, oldName, newName string) error
}

// StoredProcedureDatabase is a database that supports the creation and execution of stored procedures. The engine will
// handle all parsing and execution logic for stored procedures. Integrators only need to store and retrieve
// StoredProcedureDetails, while verifying that all stored procedures have a unique name without regard to
// case-sensitivity.
type StoredProcedureDatabase interface {
	Database

	// GetStoredProcedures returns all StoredProcedureDetails for the database.
	GetStoredProcedures(ctx *Context) ([]StoredProcedureDetails, error)

	// SaveStoredProcedure stores the given StoredProcedureDetails to the database. The integrator should verify that
	// the name of the new stored procedure is unique amongst existing stored procedures.
	SaveStoredProcedure(ctx *Context, spd StoredProcedureDetails) error

	// DropStoredProcedure removes the StoredProcedureDetails with the matching name from the database.
	DropStoredProcedure(ctx *Context, name string) error
}

// DatabaseProvider is a collection of Database.
type DatabaseProvider interface {
	// Database gets a Database from the provider.
	Database(ctx *Context, name string) (Database, error)

	// HasDatabase checks if the Database exists in the provider.
	HasDatabase(ctx *Context, name string) bool

	// AllDatabases returns a slice of all Databases in the provider.
	AllDatabases(ctx *Context) []Database
}

// MutableDatabaseProvider is a DatabaseProvider that can create and drop databases.
type MutableDatabaseProvider interface {
	DatabaseProvider

	// CreateDatabase creates a database and adds it to the provider's collection.
	CreateDatabase(ctx *Context, name string) error

	// DropDatabase removes a database from the provider's collection.
	DropDatabase(ctx *Context, name string) error
}

// CollatedDatabaseProvider is a DatabaseProvider that can create a Database with a specific collation.
type CollatedDatabaseProvider interface {
	MutableDatabaseProvider

	// CreateCollatedDatabase creates a collated database and adds it to the provider's collection.
	CreateCollatedDatabase(ctx *Context, name string, collation CollationID) error
}

