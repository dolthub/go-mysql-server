package information_schema

import (
	"bytes"
	"fmt"

	. "github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

type routineTable struct {
	name       string
	schema     Schema
	catalog    Catalog
	procedures []*plan.Procedure
	// functions
	rowIter func(*Context, Catalog, []*plan.Procedure) (RowIter, error)
}

var (
	_ Table = (*routineTable)(nil)
)

func (t *routineTable) AssignCatalog(cat Catalog) Table {
	t.catalog = cat
	return t
}

func (r *routineTable) AssignProcedures(p []*plan.Procedure) Table {
	// TODO: should also assign functions
	r.procedures = p
	return r
}

// Name implements the sql.Table interface.
func (r *routineTable) Name() string {
	return r.name
}

// Schema implements the sql.Table interface.
func (r *routineTable) Schema() Schema {
	return r.schema
}

func (r *routineTable) String() string {
	return printTable(r.Name(), r.Schema())
}

func (r *routineTable) Partitions(context *Context) (PartitionIter, error) {
	return &informationSchemaPartitionIter{informationSchemaPartition: informationSchemaPartition{partitionKey(r.Name())}}, nil
}

func (r *routineTable) PartitionRows(context *Context, partition Partition) (RowIter, error) {
	if !bytes.Equal(partition.Key(), partitionKey(r.Name())) {
		return nil, ErrPartitionNotFound.New(partition.Key())
	}
	if r.rowIter == nil {
		return RowsToRowIter(), nil
	}
	if r.catalog == nil {
		return nil, fmt.Errorf("nil catalog for info schema table %s", r.name)
	}

	return r.rowIter(context, r.catalog, r.procedures)
}
