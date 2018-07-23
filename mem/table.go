package mem

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	errors "gopkg.in/src-d/go-errors.v1"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

// Table represents an in-memory database table.
type Table struct {
	name   string
	schema sql.Schema
	data   []sql.Row
}

// NewTable creates a new Table with the given name and schema.
func NewTable(name string, schema sql.Schema) *Table {
	return &Table{
		name:   name,
		schema: schema,
	}
}

// Resolved implements the Resolvable interface.
func (Table) Resolved() bool {
	return true
}

// Name returns the table name.
func (t *Table) Name() string {
	return t.name
}

// Schema implements the Node interface.
func (t *Table) Schema() sql.Schema {
	return t.schema
}

// Children implements the Node interface.
func (t *Table) Children() []sql.Node {
	return nil
}

// RowIter implements the Node interface.
func (t *Table) RowIter(ctx *sql.Context) (sql.RowIter, error) {
	return sql.RowsToRowIter(t.data...), nil
}

// TransformUp implements the Transformer interface.
func (t *Table) TransformUp(f sql.TransformNodeFunc) (sql.Node, error) {
	return f(t)
}

// TransformExpressionsUp implements the Transformer interface.
func (t *Table) TransformExpressionsUp(f sql.TransformExprFunc) (sql.Node, error) {
	return t, nil
}

// Insert a new row into the table.
func (t *Table) Insert(ctx *sql.Context, row sql.Row) error {
	if len(row) != len(t.schema) {
		return sql.ErrUnexpectedRowLength.New(len(t.schema), len(row))
	}

	for idx, value := range row {
		c := t.schema[idx]
		if !c.Check(value) {
			return sql.ErrInvalidType.New(value)
		}
	}

	t.data = append(t.data, row.Copy())
	return nil
}

func (t Table) String() string {
	p := sql.NewTreePrinter()
	_ = p.WriteNode("Table(%s)", t.name)
	var schema = make([]string, len(t.schema))
	for i, col := range t.schema {
		schema[i] = fmt.Sprintf(
			"Column(%s, %s, nullable=%v)",
			col.Name,
			col.Type.Type().String(),
			col.Nullable,
		)
	}
	_ = p.WriteChildren(schema...)
	return p.String()
}

var _ sql.Indexable = (*Table)(nil)

var errColumnNotFound = errors.NewKind("could not find column %s")

// IndexKeyValueIter implements the Indexable interface.
func (t *Table) IndexKeyValueIter(ctx *sql.Context, colNames []string) (sql.IndexKeyValueIter, error) {
	var columns = make([]int, len(colNames))
	for i, name := range colNames {
		var found bool
		for j, col := range t.schema {
			if col.Name == name {
				columns[i] = j
				found = true
				break
			}
		}

		if !found {
			return nil, errColumnNotFound.New(name)
		}
	}

	return &keyValueIter{t.data, columns, 0}, nil
}

// HandledFilters implements the PushdownProjectionAndFiltersTable interface.
func (t *Table) HandledFilters([]sql.Expression) []sql.Expression {
	return nil
}

// WithProjectAndFilters implements the PushdownProjectionAndFiltersTable interface.
func (t *Table) WithProjectAndFilters(
	ctx *sql.Context,
	columns, filters []sql.Expression,
) (sql.RowIter, error) {
	return t.RowIter(ctx)
}

// WithProjectFiltersAndIndex implements the Indexable interface.
func (t *Table) WithProjectFiltersAndIndex(
	ctx *sql.Context,
	columns, filters []sql.Expression,
	index sql.IndexValueIter,
) (sql.RowIter, error) {
	return &indexIter{t.data, index}, nil
}

type keyValueIter struct {
	data    []sql.Row
	columns []int
	pos     int
}

func (i *keyValueIter) Next() ([]interface{}, []byte, error) {
	if i.pos >= len(i.data) {
		return nil, nil, io.EOF
	}

	var buf bytes.Buffer
	if err := binary.Write(&buf, binary.LittleEndian, int64(i.pos)); err != nil {
		return nil, nil, err
	}

	var values = make([]interface{}, len(i.columns))
	for j, col := range i.columns {
		values[j] = i.data[i.pos][col]
	}

	i.pos++

	return values, buf.Bytes(), nil
}

func (i *keyValueIter) Close() error {
	i.pos = len(i.data)
	return nil
}

type indexIter struct {
	data  []sql.Row
	index sql.IndexValueIter
}

func (i *indexIter) Next() (sql.Row, error) {
	data, err := i.index.Next()
	if err != nil {
		return nil, err
	}

	var pos int64
	if err := binary.Read(bytes.NewBuffer(data), binary.LittleEndian, &pos); err != nil {
		return nil, err
	}

	return i.data[int(pos)], nil
}

func (i *indexIter) Close() error { return i.index.Close() }
