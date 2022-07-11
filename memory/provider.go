package memory

import (
	"sort"
	"strings"
	"sync"

	"github.com/dolthub/go-mysql-server/internal/similartext"
	"github.com/dolthub/go-mysql-server/sql"
)

var _ sql.DatabaseProvider = memoryDBProvider{}
var _ sql.MutableDatabaseProvider = memoryDBProvider{}
var _ sql.TableFunctionProvider = memoryDBProvider{}

// memoryDBProvider is a collection of Database.
type memoryDBProvider struct {
	dbs            map[string]sql.Database
	mu             *sync.RWMutex
	tableFunctions map[string]sql.TableFunction
}

func NewMemoryDBProvider(dbs ...sql.Database) sql.MutableDatabaseProvider {
	dbMap := make(map[string]sql.Database, len(dbs))
	for _, db := range dbs {
		dbMap[strings.ToLower(db.Name())] = db
	}
	return memoryDBProvider{
		dbs:            dbMap,
		mu:             &sync.RWMutex{},
		tableFunctions: make(map[string]sql.TableFunction),
	}
}

// Database returns the Database with the given name if it exists.
func (d memoryDBProvider) Database(_ *sql.Context, name string) (sql.Database, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	db, ok := d.dbs[strings.ToLower(name)]
	if ok {
		return db, nil
	}

	names := make([]string, 0, len(d.dbs))
	for n := range d.dbs {
		names = append(names, n)
	}

	similar := similartext.Find(names, name)
	return nil, sql.ErrDatabaseNotFound.New(name + similar)
}

// HasDatabase returns the Database with the given name if it exists.
func (d memoryDBProvider) HasDatabase(_ *sql.Context, name string) bool {
	d.mu.RLock()
	defer d.mu.RUnlock()

	_, ok := d.dbs[strings.ToLower(name)]
	return ok
}

// AllDatabases returns the Database with the given name if it exists.
func (d memoryDBProvider) AllDatabases(*sql.Context) []sql.Database {
	d.mu.RLock()
	defer d.mu.RUnlock()

	all := make([]sql.Database, 0, len(d.dbs))
	for _, db := range d.dbs {
		all = append(all, db)
	}

	sort.Slice(all, func(i, j int) bool {
		return all[i].Name() < all[j].Name()
	})

	return all
}

// CreateDatabase implements MutableDatabaseProvider.
func (d memoryDBProvider) CreateDatabase(_ *sql.Context, name string) (err error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	db := NewDatabase(name)
	db.EnablePrimaryKeyIndexes()
	d.dbs[strings.ToLower(db.Name())] = db
	return
}

// DropDatabase implements MutableDatabaseProvider.
func (d memoryDBProvider) DropDatabase(_ *sql.Context, name string) (err error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	delete(d.dbs, strings.ToLower(name))
	return
}

// TableFunction implements sql.TableFunctionProvider
func (mdb memoryDBProvider) TableFunction(_ *sql.Context, name string) (sql.TableFunction, error) {
	if tableFunction, ok := mdb.tableFunctions[name]; ok {
		return tableFunction, nil
	}

	return nil, sql.ErrTableFunctionNotFound.New(name)
}
