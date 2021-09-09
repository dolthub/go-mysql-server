package memory

import (
	"github.com/dolthub/go-mysql-server/internal/similartext"
	"github.com/dolthub/go-mysql-server/sql"
	"strings"
)

// memoryDBProvider is a collection of Database.
type memoryDBProvider struct {
	dbs []sql.Database
}

var _ sql.DatabaseProvider = memoryDBProvider{}

func NewMemoryDBProvider(dbs ...sql.Database) sql.DatabaseProvider {
	return memoryDBProvider{dbs: dbs}
}

// Database returns the Database with the given name if it exists.
func (d memoryDBProvider) Database(name string) (sql.Database, error) {
	if len(d.dbs) == 0 {
		return nil, sql.ErrDatabaseNotFound.New(name)
	}

	name = strings.ToLower(name)
	var dbNames []string
	for _, db := range d.dbs {
		if strings.ToLower(db.Name()) == name {
			return db, nil
		}
		dbNames = append(dbNames, db.Name())
	}
	similar := similartext.Find(dbNames, name)
	return nil, sql.ErrDatabaseNotFound.New(name + similar)
}

// HasDatabase returns the Database with the given name if it exists.
func (d memoryDBProvider) HasDatabase(name string) bool {
	name = strings.ToLower(name)
	for _, db := range d.dbs {
		if strings.ToLower(db.Name()) == name {
			return true
		}
	}
	return false
}

// AllDatabases returns the Database with the given name if it exists.
func (d memoryDBProvider) AllDatabases() []sql.Database {
	return d.dbs
}

// DropDatabase removes a database.
func (d memoryDBProvider) DropDatabase(dbName string) {
	idx := -1
	for i, db := range d.dbs {
		if db.Name() == dbName {
			idx = i
			break
		}
	}

	if idx != -1 {
		d.dbs = append(d.dbs[:idx], d.dbs[idx+1:]...)
	}
}
