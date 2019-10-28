package testutil

import (
	"github.com/src-d/go-mysql-server/sql"
)

type TestIndexDriver struct {

}

func (d *TestIndexDriver) ID() string {
	panic("TestIndexDriver")
}

func (d *TestIndexDriver) LoadAll(db, table string) ([]sql.Index, error) {
	panic("implement me")
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