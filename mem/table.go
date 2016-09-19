package mem

import (
	"io"

	"github.com/mvader/gitql/sql"
)

type Table struct {
	name string
	schema sql.Schema
	data [][]interface{}
}

func NewTable(name string, schema sql.Schema) *Table {
	return &Table{
		name: name,
		schema: schema,
		data: [][]interface{}{},
	}
}

func (t *Table) Name() string {
	return t.name
}

func (t *Table) Schema() sql.Schema {
	return t.schema
}

func (t *Table) Children() []sql.Node {
	return []sql.Node{}
}

func (t *Table) RowIter() (sql.RowIter, error) {
	return &iter{data: t.data}, nil
}

func (t *Table) Insert(values ...interface{}) {
	t.data = append(t.data, values)
}

type iter struct {
	idx int
	data [][]interface{}
}

func (i *iter) Next() (sql.Row, error) {
	if i.idx >= len(i.data) {
		return nil, io.EOF
	}
	row := sql.NewMemoryRow(i.data[i.idx]...)
	i.idx++
	return row, nil
}
