package sql

import (
	"strings"

	"gopkg.in/src-d/go-errors.v1"
)

// ErrDatabaseNotFound is thrown when a database is not found
var ErrDatabaseNotFound = errors.NewKind("database not found: %s")

// Catalog holds databases, tables and functions.
type Catalog struct {
	Databases
	FunctionRegistry
	*IndexRegistry
	*ProcessList
}

// NewCatalog returns a new empty Catalog.
func NewCatalog() *Catalog {
	return &Catalog{
		Databases:        Databases{},
		FunctionRegistry: NewFunctionRegistry(),
		IndexRegistry:    NewIndexRegistry(),
		ProcessList:      NewProcessList(),
	}
}

// Databases is a collection of Database.
type Databases []Database

// Database returns the Database with the given name if it exists.
func (d Databases) Database(name string) (Database, error) {
	for _, db := range d {
		if db.Name() == name {
			return db, nil
		}
	}

	return nil, ErrDatabaseNotFound.New(name)
}

// AddDatabase adds a new database.
func (d *Databases) AddDatabase(db Database) {
	*d = append(*d, db)
}

// Table returns the Table with the given name if it exists.
func (d Databases) Table(dbName string, tableName string) (Table, error) {
	db, err := d.Database(dbName)
	if err != nil {
		return nil, err
	}

	tableName = strings.ToLower(tableName)

	tables := db.Tables()
	// Try to get the table by key, but if the name is not the same,
	// then use the slow path and iterate over all tables comparing
	// the name.
	table, ok := tables[tableName]
	if !ok {
		for name, table := range tables {
			if strings.ToLower(name) == tableName {
				return table, nil
			}
		}

		return nil, ErrTableNotFound.New(tableName)
	}

	return table, nil
}
