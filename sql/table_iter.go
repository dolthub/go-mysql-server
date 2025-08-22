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
	"sync"
)

// TableRowIter is an iterator over the partitions in a table.
type TableRowIter struct {
	table      Table
	partitions PartitionIter
	partition  Partition
	rows       RowIter

	parts    []Partition
	currPart int

	rowChan chan Row
	errChan chan error
	once    sync.Once
}

var _ RowIter = (*TableRowIter)(nil)

// NewTableRowIter returns a new iterator over the rows in the partitions of the table given.
func NewTableRowIter(ctx *Context, table Table, partitions PartitionIter) *TableRowIter {
	return &TableRowIter{table: table, partitions: partitions}
}

func (i *TableRowIter) start(ctx *Context) {
	i.once.Do(func() {
		i.rowChan = make(chan Row, 128)
		i.errChan = make(chan error)
		i.parts = make([]Partition, 0)

		for {
			partition, err := i.partitions.Next(ctx)
			if err != nil {
				if err == io.EOF {
					if err = i.partitions.Close(ctx); err != nil {
						panic(err)
					}
					break
				}
				panic(err)
				return
			}
			i.parts = append(i.parts, partition)
		}

		// TODO: if very few partitions, just read sequentially
		if len(i.parts) < 2 {
			return
		}

		go func() {
			defer close(i.rowChan)
			for idx := 0; idx < len(i.parts); idx++ {
				rowIter, riErr := i.table.PartitionRows(ctx, i.parts[idx])
				if riErr != nil {
					i.errChan <- riErr
					return
				}
				for {
					row, rErr := rowIter.Next(ctx)
					if rErr != nil {
						if rErr == io.EOF {
							if rErr = rowIter.Close(ctx); rErr != nil {
								i.errChan <- rErr
								return
							}
							break
						}
						i.errChan <- rErr
						return
					}
					select {
					case i.rowChan <- row:
					case <-ctx.Done():
						return
					}
				}
			}
		}()
	})
}

func (i *TableRowIter) Next(ctx *Context) (Row, error) {
	i.start(ctx)

	if len(i.parts) < 2 {
		for i.currPart < len(i.parts) {
			if i.partition == nil {
				i.partition = i.parts[i.currPart]
			}
			if i.rows == nil {
				rows, err := i.table.PartitionRows(ctx, i.partition)
				if err != nil {
					return nil, err
				}
				i.rows = rows
			}
			row, err := i.rows.Next(ctx)
			if err != nil && err == io.EOF {
				if err = i.rows.Close(ctx); err != nil {
					return nil, err
				}
				i.partition = nil
				i.rows = nil
				i.currPart++
				continue
			}
			return row, err
		}
		return nil, io.EOF
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case err := <-i.errChan:
		return nil, err
	case row, ok := <-i.rowChan:
		if !ok {
			return nil, io.EOF
		}
		return row, nil
	}
}

func (i *TableRowIter) Close(ctx *Context) error {
	return nil
	if i.rows != nil {
		if err := i.rows.Close(ctx); err != nil {
			_ = i.partitions.Close(ctx)
			return err
		}
	}
	return i.partitions.Close(ctx)
}
