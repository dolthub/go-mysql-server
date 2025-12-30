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
	"io"
)

// TableRowIter is an iterator over the partitions in a table.
type TableRowIter struct {
	rowIters      []RowIter
	valueRowIters []ValueRowIter
	idx           int
}

var _ RowIter = (*TableRowIter)(nil)

// NewTableRowIter returns a new iterator over the rows in the partitions of the table given.
func NewTableRowIter(ctx *Context, table Table, partitions PartitionIter) *TableRowIter {
	var rowIters []RowIter
	var valueRowIters []ValueRowIter
	for {
		part, err := partitions.Next(ctx)
		if err != nil {
			if err != io.EOF {
				panic(err)
			}
			err = partitions.Close(ctx)
			if err != nil {
				panic(err)
			}
			break
		}
		rowIter, err := table.PartitionRows(ctx, part)
		if err != nil {
			panic(err)
		}
		rowIters = append(rowIters, rowIter)
		if valRowIter, ok := rowIter.(ValueRowIter); ok {
			valueRowIters = append(valueRowIters, valRowIter)
		}
	}
	return &TableRowIter{
		rowIters:      rowIters,
		valueRowIters: valueRowIters,
	}
}

func (i *TableRowIter) Next(ctx *Context) (Row, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	if i.idx == len(i.rowIters) {
		return nil, io.EOF
	}
	rowIter := i.rowIters[i.idx]
	row, err := rowIter.Next(ctx)
	if err != nil {
		if err != io.EOF {
			return nil, err
		}
		err = rowIter.Close(ctx)
		if err != nil {
			return nil, err
		}
		i.idx++
		return i.Next(ctx)
	}
	return row, nil
}

// NextValueRow implements the sql.ValueRowIter interface
func (i *TableRowIter) NextValueRow(ctx *Context) (ValueRow, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	if i.idx == len(i.valueRowIters) {
		return nil, io.EOF
	}
	rowIter := i.valueRowIters[i.idx]
	row, err := rowIter.NextValueRow(ctx)
	if err != nil {
		if err != io.EOF {
			return nil, err
		}
		err = rowIter.Close(ctx)
		if err != nil {
			return nil, err
		}
		i.idx++
		return i.NextValueRow(ctx)
	}
	return row, nil
}

// IsValueRowIter implements the sql.ValueRowIter interface.
func (i *TableRowIter) IsValueRowIter(ctx *Context) bool {
	if len(i.valueRowIters) == 0 {
		return false
	}
	for _, iter := range i.valueRowIters {
		if !iter.IsValueRowIter(ctx) {
			return false
		}
	}
	return true
}

func (i *TableRowIter) Close(ctx *Context) error {
	return nil
}
