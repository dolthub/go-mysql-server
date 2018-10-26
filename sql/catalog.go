package sql

import (
	"fmt"
	"strings"
	"sync"

	"gopkg.in/src-d/go-errors.v1"
)

// ErrDatabaseNotFound is thrown when a database is not found
var ErrDatabaseNotFound = errors.NewKind("database not found: %s")

// Catalog holds databases, tables and functions.
type Catalog struct {
	FunctionRegistry
	*IndexRegistry
	*ProcessList

	mu              sync.RWMutex
	currentDatabase string
	dbs             Databases
	locks           sessionLocks
}

type (
	sessionLocks map[uint32]dbLocks
	dbLocks      map[string]tableLocks
	tableLocks   map[string]struct{}
)

// NewCatalog returns a new empty Catalog.
func NewCatalog() *Catalog {
	return &Catalog{
		FunctionRegistry: NewFunctionRegistry(),
		IndexRegistry:    NewIndexRegistry(),
		ProcessList:      NewProcessList(),
		locks:            make(sessionLocks),
	}
}

// CurrentDatabase returns the current database.
func (c *Catalog) CurrentDatabase() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.currentDatabase
}

// SetCurrentDatabase changes the current database.
func (c *Catalog) SetCurrentDatabase(db string) {
	c.mu.Lock()
	c.currentDatabase = db
	c.mu.Unlock()
}

// AllDatabases returns all databases in the catalog.
func (c *Catalog) AllDatabases() Databases {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var result = make(Databases, len(c.dbs))
	copy(result, c.dbs)
	return result
}

// AddDatabase adds a new database to the catalog.
func (c *Catalog) AddDatabase(db Database) {
	c.mu.Lock()
	if c.currentDatabase == "" {
		c.currentDatabase = db.Name()
	}

	c.dbs.Add(db)
	c.mu.Unlock()
}

// Database returns the database with the given name.
func (c *Catalog) Database(db string) (Database, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.dbs.Database(db)
}

// Table returns the table in the given database with the given name.
func (c *Catalog) Table(db, table string) (Table, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.dbs.Table(db, table)
}

// Databases is a collection of Database.
type Databases []Database

// Database returns the Database with the given name if it exists.
func (d Databases) Database(name string) (Database, error) {
	name = strings.ToLower(name)
	for _, db := range d {
		if strings.ToLower(db.Name()) == name {
			return db, nil
		}
	}

	return nil, ErrDatabaseNotFound.New(name)
}

// Add adds a new database.
func (d *Databases) Add(db Database) {
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

// LockTable adds a lock for the given table and session client. It is assumed
// the database is the current database in use.
func (c *Catalog) LockTable(id uint32, table string) {
	db := c.CurrentDatabase()
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
			table, err := c.dbs.Table(db, t)
			if err == nil {
				if lockable, ok := table.(Lockable); ok {
					if err := lockable.Unlock(ctx, id); err != nil {
						errors = append(errors, err.Error())
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
