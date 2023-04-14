// Copyright 2020-2021 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sql

import (
	"fmt"
	"io"
	"strings"

	"github.com/dolthub/vitess/go/vt/proto/query"

	"github.com/dolthub/go-mysql-server/sql/values"
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

// Append appends all the values in r2 to this row and returns the result
func (r Row) Append(r2 Row) Row {
	row := make(Row, len(r)+len(r2))
	for i := range r {
		row[i] = r[i]
	}
	for i := range r2 {
		row[i+len(r)] = r2[i]
	}
	return row
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

// FormatRow returns a formatted string representing this row's values
func FormatRow(row Row) string {
	var sb strings.Builder
	sb.WriteRune('[')
	for i, v := range row {
		if i > 0 {
			sb.WriteRune(',')
		}
		sb.WriteString(fmt.Sprintf("%v", v))
	}
	sb.WriteRune(']')
	return sb.String()
}

// RowIter is an iterator that produces rows.
// TODO: most row iters need to be Disposable for CachedResult safety
type RowIter interface {
	// Next retrieves the next row. It will return io.EOF if it's the last row.
	// After retrieving the last row, Close will be automatically closed.
	Next(ctx *Context) (Row, error)
	Closer
}

// RowIterToRows converts a row iterator to a slice of rows.
func RowIterToRows(ctx *Context, sch Schema, i RowIter) ([]Row, error) {
	var rows []Row
	for {
		row, err := i.Next(ctx)
		if err == io.EOF {
			break
		}

		if err != nil {
			i.Close(ctx)
			return nil, err
		}

		rows = append(rows, row)
	}

	return rows, i.Close(ctx)
}

func rowFromRow2(sch Schema, r Row2) Row {
	row := make(Row, len(sch))
	for i, col := range sch {
		switch col.Type.Type() {
		case query.Type_INT8:
			row[i] = values.ReadInt8(r.GetField(i).Val)
		case query.Type_UINT8:
			row[i] = values.ReadUint8(r.GetField(i).Val)
		case query.Type_INT16:
			row[i] = values.ReadInt16(r.GetField(i).Val)
		case query.Type_UINT16:
			row[i] = values.ReadUint16(r.GetField(i).Val)
		case query.Type_INT32:
			row[i] = values.ReadInt32(r.GetField(i).Val)
		case query.Type_UINT32:
			row[i] = values.ReadUint32(r.GetField(i).Val)
		case query.Type_INT64:
			row[i] = values.ReadInt64(r.GetField(i).Val)
		case query.Type_UINT64:
			row[i] = values.ReadUint64(r.GetField(i).Val)
		case query.Type_FLOAT32:
			row[i] = values.ReadFloat32(r.GetField(i).Val)
		case query.Type_FLOAT64:
			row[i] = values.ReadFloat64(r.GetField(i).Val)
		case query.Type_TEXT, query.Type_VARCHAR, query.Type_CHAR:
			row[i] = values.ReadString(r.GetField(i).Val, values.ByteOrderCollation)
		case query.Type_BLOB, query.Type_VARBINARY, query.Type_BINARY:
			row[i] = values.ReadBytes(r.GetField(i).Val, values.ByteOrderCollation)
		case query.Type_BIT:
			fallthrough
		case query.Type_ENUM:
			fallthrough
		case query.Type_SET:
			fallthrough
		case query.Type_TUPLE:
			fallthrough
		case query.Type_GEOMETRY:
			fallthrough
		case query.Type_JSON:
			fallthrough
		case query.Type_EXPRESSION:
			fallthrough
		case query.Type_INT24:
			fallthrough
		case query.Type_UINT24:
			fallthrough
		case query.Type_TIMESTAMP:
			fallthrough
		case query.Type_DATE:
			fallthrough
		case query.Type_TIME:
			fallthrough
		case query.Type_DATETIME:
			fallthrough
		case query.Type_YEAR:
			fallthrough
		case query.Type_DECIMAL:
			panic(fmt.Sprintf("Unimplemented type conversion: %T", col.Type))
		default:
			panic(fmt.Sprintf("unknown type %T", col.Type))
		}
	}
	return row
}

// RowsToRowIter creates a RowIter that iterates over the given rows.
func RowsToRowIter(rows ...Row) RowIter {
	return &sliceRowIter{rows: rows}
}

type sliceRowIter struct {
	rows []Row
	idx  int
}

func (i *sliceRowIter) Next(*Context) (Row, error) {
	if i.idx >= len(i.rows) {
		return nil, io.EOF
	}

	r := i.rows[i.idx]
	i.idx++
	return r.Copy(), nil
}

func (i *sliceRowIter) Close(*Context) error {
	i.rows = nil
	return nil
}
