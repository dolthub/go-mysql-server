package memory

import (
	"github.com/src-d/go-mysql-server/sql"
)

const IndexDriverId = "MemoryIndexDriver"

// TestIndexDriver is a non-performant index driver meant to aid in verification of engine correctness. It can not
// create or delete indexes, but will use the index types defined in this package to alter how queries are executed,
// retrieving values from the indexes rather than from the tables directly.
type TestIndexDriver struct {
	db string
	indexes map[string][]sql.Index
}

// NewIndexDriver returns a new index driver for database and the indexes given, keyed by the table name.
func NewIndexDriver(db string, indexes map[string][]sql.Index) *TestIndexDriver {
	return &TestIndexDriver{db: db, indexes: indexes}
}

func (d *TestIndexDriver) ID() string {
	return IndexDriverId
}

func (d *TestIndexDriver) LoadAll(ctx *sql.Context, db, table string) ([]sql.Index, error) {
	if d.db != db {
		return nil, nil
	}
	return d.indexes[table], nil
}

func (d *TestIndexDriver) Save(*sql.Context, sql.Index, sql.PartitionIndexKeyValueIter) error {
	panic("not implemented")
}

func (d *TestIndexDriver) Delete(sql.Index, sql.PartitionIter) error {
	panic("not implemented")
}

func (d *TestIndexDriver) Create(db, table, id string, expressions []sql.Expression, config map[string]string) (sql.Index, error) {
	panic("not implemented")
}