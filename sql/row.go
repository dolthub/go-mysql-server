package sql

import (
	"io"
)

// Row is a tuple of values.
type Row []interface{}

// NewRow creates a row from the given values.
func NewRow(values ...interface{}) Row {
	row := make([]interface{}, len(values))
	copy(row, values)
	return row
}

// Copy creates a new row with the same values as the current one.
func (r Row) Copy() Row {
	return NewRow(r...)
}

// Equals checks whether two rows are equal given a schema.
func (r Row) Equals(row Row, schema Schema) (bool, error) {
	if len(row) != len(r) || len(row) != len(schema) {
		return false, nil
	}

	for i, colLeft := range r {
		colRight := row[i]
		cmp, err := schema[i].Type.Compare(colLeft, colRight)
		if err != nil {
			return false, err
		}
		if cmp != 0 {
			return false, nil
		}
	}

	return true, nil
}

// RowIter is an iterator that produces rows.
type RowIter interface {
	// Next retrieves the next row. It will return io.EOF if it's the last row.
	// After retrieving the last row, Close will be automatically closed.
	Next() (Row, error)
	// Close the iterator.
	Close() error
}

// RowIterToRows converts a row iterator to a slice of rows.
func RowIterToRows(i RowIter) ([]Row, error) {
	var rows []Row
	for {
		row, err := i.Next()
		if err == io.EOF {
			break
		}

		if err != nil {
			return nil, err
		}

		rows = append(rows, row)
	}

	return rows, i.Close()
}

// NodeToRows converts a node to a slice of rows.
func NodeToRows(ctx *Context, n Node) ([]Row, error) {
	i, err := n.RowIter(ctx)
	if err != nil {
		return nil, err
	}

	return RowIterToRows(i)
}

// RowsToRowIter creates a RowIter that iterates over the given rows.
func RowsToRowIter(rows ...Row) RowIter {
	return &sliceRowIter{rows: rows}
}

type sliceRowIter struct {
	rows []Row
	idx  int
}

func (i *sliceRowIter) Next() (Row, error) {
	if i.idx >= len(i.rows) {
		return nil, io.EOF
	}

	r := i.rows[i.idx]
	i.idx++
	return r.Copy(), nil
}

func (i *sliceRowIter) Close() error {
	i.rows = nil
	return nil
}
