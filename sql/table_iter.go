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
	rowChans      []chan Row
	valueRowIters []ValueRowIter
	valueRowChans []chan ValueRow
	errChan       chan error
	idx           int
	hasInit       bool
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

	var rowChans = make([]chan Row, len(rowIters))
	for i := 0; i < len(rowIters); i++ {
		rowChans[i] = make(chan Row, 512)
	}

	var valueRowChans []chan ValueRow
	if len(valueRowIters) > 0 {
		valueRowChans = make([]chan ValueRow, len(valueRowIters))
		for i := 0; i < len(valueRowIters); i++ {
			valueRowChans[i] = make(chan ValueRow, 512)
		}
	}

	return &TableRowIter{
		rowIters:      rowIters,
		rowChans:      rowChans,
		valueRowIters: valueRowIters,
		valueRowChans: valueRowChans,
		errChan:       make(chan error),
	}
}

func (i *TableRowIter) exhaustIter(ctx *Context, rowIter RowIter, rowChan chan Row) {
	defer close(rowChan)
	for {
		row, err := rowIter.Next(ctx)
		if err != nil {
			if err != io.EOF {
				i.errChan <- err
			}
			err = rowIter.Close(ctx)
			if err != nil {
				i.errChan <- err
			}
			break
		}
		rowChan <- row
	}
}

func (i *TableRowIter) initRowQueue(ctx *Context) {
	for idx := 0; idx < len(i.rowIters); idx++ {
		go i.exhaustIter(ctx, i.rowIters[idx], i.rowChans[idx])
	}
}

func (i *TableRowIter) Next(ctx *Context) (Row, error) {
	if i.idx == len(i.rowIters) {
		return nil, io.EOF
	}
	if !i.hasInit {
		i.initRowQueue(ctx)
		i.hasInit = true
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case err := <-i.errChan:
		return nil, err
	case row, ok := <-i.rowChans[i.idx]:
		if !ok {
			i.idx++
			return i.Next(ctx)
		}
		return row, nil
	}
}

func (i *TableRowIter) initValueRowQueue(ctx *Context) {
	for idx := 0; idx < len(i.valueRowIters); idx++ {
		go i.exhaustValueIter(ctx, i.valueRowIters[idx], i.valueRowChans[idx])
	}
}

func (i *TableRowIter) exhaustValueIter(ctx *Context, rowIter ValueRowIter, rowChan chan ValueRow) {
	defer close(rowChan)
	for {
		row, err := rowIter.NextValueRow(ctx)
		if err != nil {
			if err != io.EOF {
				i.errChan <- err
			}
			err = rowIter.Close(ctx)
			if err != nil {
				i.errChan <- err
			}
			break
		}
		rowChan <- row
	}
}

// NextValueRow implements the sql.ValueRowIter interface
func (i *TableRowIter) NextValueRow(ctx *Context) (ValueRow, error) {
	if i.idx == len(i.valueRowChans) {
		return nil, io.EOF
	}
	if !i.hasInit {
		i.initRowQueue(ctx)
		i.hasInit = true
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case err := <-i.errChan:
		return nil, err
	case row, ok := <-i.valueRowChans[i.idx]:
		if !ok {
			i.idx++
			return i.NextValueRow(ctx)
		}
		return row, nil
	}
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
