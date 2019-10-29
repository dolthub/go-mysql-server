package testutil

import (
	"github.com/src-d/go-mysql-server/sql"
)

type TestIndexDriver struct {
	indexes map[string][]sql.Index
}

func NewIndexDriver(indexes map[string][]sql.Index) *TestIndexDriver {
	return &TestIndexDriver{indexes: indexes}
}

func (d *TestIndexDriver) ID() string {
	panic("TestIndexDriver")
}

func (d *TestIndexDriver) LoadAll(db, table string) ([]sql.Index, error) {
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