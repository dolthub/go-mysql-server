package mem

import (
	"fmt"
	"io"

	"github.com/gitql/gitql/sql"
)

type Table struct {
	name   string
	schema sql.Schema
	data   []sql.Row
}

func NewTable(name string, schema sql.Schema) *Table {
	return &Table{
		name:   name,
		schema: schema,
	}
}

func (Table) Resolved() bool {
	return true
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
	return &iter{rows: t.data}, nil
}

func (t *Table) TransformUp(f func(sql.Node) sql.Node) sql.Node {
	return f(t)
}

func (t *Table) TransformExpressionsUp(f func(sql.Expression) sql.Expression) sql.Node {
	return t
}

func (t *Table) Insert(row sql.Row) error {
	if len(row) != len(t.schema) {
		return fmt.Errorf("insert expected %d values, got %d", len(t.schema), len(row))
	}

	for idx, value := range row {
		f := t.schema[idx]
		if !f.Type.Check(value) {
			return sql.ErrInvalidType
		}
	}

	t.data = append(t.data, row.Copy())
	return nil
}

type iter struct {
	idx  int
	rows []sql.Row
}

func (i *iter) Next() (sql.Row, error) {
	if i.idx >= len(i.rows) {
		return nil, io.EOF
	}

	row := i.rows[i.idx]
	i.idx++
	return row.Copy(), nil
}
