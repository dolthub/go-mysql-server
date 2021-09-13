package memory

import (
	"sort"
	"strings"
	"sync"

	"github.com/dolthub/go-mysql-server/internal/similartext"
	"github.com/dolthub/go-mysql-server/sql"
)

// memoryDBProvider is a collection of Database.
type memoryDBProvider struct {
	dbs map[string]sql.Database
	mu  *sync.RWMutex
}

var _ sql.DatabaseProvider = memoryDBProvider{}
var _ sql.MutableDatabaseProvider = memoryDBProvider{}

func NewMemoryDBProvider(dbs ...sql.Database) sql.MutableDatabaseProvider {
	dbMap := make(map[string]sql.Database, len(dbs))
	for _, db := range dbs {
		dbMap[strings.ToLower(db.Name())] = db
	}
	return memoryDBProvider{
		dbs: dbMap,
		mu:  &sync.RWMutex{},
	}
}

// Database returns the Database with the given name if it exists.
func (d memoryDBProvider) Database(name string) (sql.Database, error) {
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
func (d memoryDBProvider) HasDatabase(name string) bool {
	d.mu.RLock()
	defer d.mu.RUnlock()

	_, ok := d.dbs[strings.ToLower(name)]
	return ok
}

// AllDatabases returns the Database with the given name if it exists.
func (d memoryDBProvider) AllDatabases() []sql.Database {
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
func (d memoryDBProvider) CreateDatabase(ctx *sql.Context, name string) (err error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	db := NewDatabase(name)
	d.dbs[strings.ToLower(db.Name())] = db
	return
}

// DropDatabase implements MutableDatabaseProvider.
func (d memoryDBProvider) DropDatabase(ctx *sql.Context, name string) (err error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	delete(d.dbs, strings.ToLower(name))
	return
}
