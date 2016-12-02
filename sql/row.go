package sql

import "io"

type Row interface {
	Fields() []interface{}
}

type RowIter interface {
	Next() (Row, error)
}

type MemoryRow []interface{}

func NewMemoryRow(fields ...interface{}) MemoryRow {
	return MemoryRow(fields)
}

func (r MemoryRow) Fields() []interface{} {
	return []interface{}(r)
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

	return rows, nil
}

func NodeToRows(n Node) ([]Row, error) {
	i, err := n.RowIter()
	if err != nil {
		return nil, err
	}

	return RowIterToRows(i)
}
