package testutil

import (
	"github.com/src-d/go-mysql-server/sql"
)

const IndexDriverId = "TestIndexDriver"

type TestIndexDriver struct {
	db string
	indexes map[string][]sql.Index
}

func NewIndexDriver(db string, indexes map[string][]sql.Index) *TestIndexDriver {
	return &TestIndexDriver{db: db, indexes: indexes}
}

func (d *TestIndexDriver) ID() string {
	return IndexDriverId
}

func (d *TestIndexDriver) LoadAll(db, table string) ([]sql.Index, error) {
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