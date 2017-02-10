package sql

import (
	"fmt"
)

// Catalog holds databases, tables and functions.
type Catalog struct {
	Databases
	FunctionRegistry
}

// NewCatalog returns a new empty Catalog.
func NewCatalog() *Catalog {
	return &Catalog{
		Databases:        Databases{},
		FunctionRegistry: NewFunctionRegistry(),
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

	return nil, fmt.Errorf("database not found: %s", name)
}

// Table returns the Table with the given name if it exists.
func (d Databases) Table(dbName string, tableName string) (Table, error) {
	db, err := d.Database(dbName)
	if err != nil {
		return nil, err
	}

	tables := db.Tables()
	table, found := tables[tableName]
	if !found {
		return nil, fmt.Errorf("table not found: %s", tableName)
	}

	return table, nil
}
