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
type Row interface {
	GetValue(i int) interface{}
	GetBytes() []byte
	SetValue(i int, v interface{})
	SetBytes(i int, v []byte)
	GetType(i int)
	Values() []interface{}
	Copy() Row
	Len() int
	Subslice(i, j int) Row
	Append(Row) Row
	Equals(row Row, schema Schema) (bool, error)
}

type SqlRow struct {
	values []interface{}
	types  []Type
}

func NewSqlRowWithLen(l int) Row {
	return make(UntypedSqlRow, l)
}

func NewSqlRow(values ...interface{}) *SqlRow {
	row := make([]interface{}, len(values))
	copy(row, values)
	return &SqlRow{values: row}
}

var _ Row = (*SqlRow)(nil)

func (r *SqlRow) Subslice(i, j int) Row {
	return &SqlRow{values: r.values[i:j]}
}

func (r *SqlRow) Len() int {
	return len(r.values)
}

func (r *SqlRow) Copy() Row {
	return NewSqlRow(r.values...)
}

func (r *SqlRow) Append(r2 Row) Row {
	row := make([]interface{}, r.Len()+r2.Len())
	copy(row, r.values)
	for i := range r2.Values() {
		row[i+r.Len()] = r2.GetValue(i)
	}
	return &SqlRow{values: row}
}

func (r *SqlRow) Equals(row Row, schema Schema) (bool, error) {
	if row.Len() != r.Len() || row.Len() != len(schema) {
		return false, nil
	}

	for i, colLeft := range r.Values() {
		colRight := row.GetValue(i)
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

func (r *SqlRow) GetValue(i int) interface{} {
	return r.values[i]
}

func (r *SqlRow) GetBytes() []byte {
	//TODO implement me
	panic("implement me")
}

func (r *SqlRow) SetValue(i int, v interface{}) {
	r.values[i] = v
}

func (r *SqlRow) SetBytes(i int, v []byte) {
	//TODO implement me
	panic("implement me")
}

func (r *SqlRow) GetType(i int) {
	//TODO implement me
	panic("implement me")
}

func (r *SqlRow) Values() []interface{} {
	return r.values
}

// NewRow creates a row from the given values.
func NewRow(values ...interface{}) Row {
	return NewSqlRow(values...)
}

func NewUntypedRow(v ...interface{}) Row {
	return UntypedSqlRow(v)
}

type UntypedSqlRow []interface{}

func (r UntypedSqlRow) Append(row Row) Row {
	if row == nil || row.Len() == 0 {
		return r
	}
	return append(r, row.Values()...)
}

func (r UntypedSqlRow) GetValue(i int) interface{} {
	return r[i]
}

func (r UntypedSqlRow) GetBytes() []byte {
	//TODO implement me
	panic("implement me")
}

func (r UntypedSqlRow) SetValue(i int, v interface{}) {
	r[i] = v
}

func (r UntypedSqlRow) SetBytes(i int, v []byte) {
	//TODO implement me
	panic("implement me")
}

func (r UntypedSqlRow) GetType(i int) {
	//TODO implement me
	panic("implement me")
}

func (r UntypedSqlRow) Values() []interface{} {
	return r
}

func (r UntypedSqlRow) Copy() Row {
	cp := make([]interface{}, len(r))
	copy(cp, r)
	return UntypedSqlRow(cp)
}

func (r UntypedSqlRow) Len() int {
	return len(r)
}

func (r UntypedSqlRow) Subslice(i, j int) Row {
	return r[i:j]
}

func (r UntypedSqlRow) Equals(row Row, schema Schema) (bool, error) {
	if row.Len() != r.Len() || row.Len() != len(schema) {
		return false, nil
	}

	for i, colLeft := range r.Values() {
		colRight := row.GetValue(i)
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

var _ Row = UntypedSqlRow{}

// FormatRow returns a formatted string representing this row's values
func FormatRow(row Row) string {
	var sb strings.Builder
	sb.WriteRune('[')
	for i, v := range row.Values() {
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
func RowIterToRows(ctx *Context, i RowIter) ([]Row, error) {
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
	row := make([]interface{}, len(sch))
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
	return NewSqlRow(row...)
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

// MutableRowIter is an extension of RowIter for integrators that wrap RowIters.
// It allows for analysis rules to inspect the underlying RowIters.
type MutableRowIter interface {
	RowIter
	GetChildIter() RowIter
	WithChildIter(childIter RowIter) RowIter
}

func RowsToUntyped(rows []Row) []UntypedSqlRow {
	var dest []UntypedSqlRow
	for _, r := range rows {
		if ur, ok := r.(UntypedSqlRow); ok {
			dest = append(dest, ur)
		} else {
			dest = append(dest, NewUntypedRow(r.Values()...).(UntypedSqlRow))
		}
	}
	return dest
}
