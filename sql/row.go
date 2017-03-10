package sql

import (
	"io"
)

type Row []Value

func NewRow(values ...interface{}) Row {
	row := make([]Value, len(values))
	for i := 0; i < len(values); i++ {
		row[i] = Value(values[i])
	}

	return row
}

func (r Row) Copy() Row {
	crow := make([]Value, len(r))
	copy(crow, r)
	return r
}

type Value interface{}

type RowIter interface {
	Next() (Row, error)
	Close() error
}

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

func NodeToRows(n Node) ([]Row, error) {
	i, err := n.RowIter()
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
