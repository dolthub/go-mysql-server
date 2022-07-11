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
)

// TableRowIter is an iterator over the partitions in a table.
type TableRowIter struct {
	table      Table
	partitions PartitionIter
	partition  Partition
	rows       RowIter
	rows2      RowIter2
}

var _ RowIter = (*TableRowIter)(nil)
var _ RowIter2 = (*TableRowIter)(nil)

// NewTableRowIter returns a new iterator over the rows in the partitions of the table given.
func NewTableRowIter(ctx *Context, table Table, partitions PartitionIter) *TableRowIter {
	return &TableRowIter{table: table, partitions: partitions}
}

func (i *TableRowIter) Next(ctx *Context) (Row, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	if i.partition == nil {
		partition, err := i.partitions.Next(ctx)
		if err != nil {
			if err == io.EOF {
				if e := i.partitions.Close(ctx); e != nil {
					return nil, e
				}
			}

			return nil, err
		}

		i.partition = partition
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
		row, err = i.Next(ctx)
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	return row, err
}

func (i *TableRowIter) Next2(ctx *Context, frame *RowFrame) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	if i.partition == nil {
		partition, err := i.partitions.Next(ctx)
		if err != nil {
			if err == io.EOF {
				if e := i.partitions.Close(ctx); e != nil {
					return e
				}
			}

			return err
		}

		i.partition = partition
	}

	if i.rows2 == nil {
		t2, ok := i.table.(Table2)
		if !ok {
			return fmt.Errorf("table does not implement Table2: %s (%T)", i.table.Name(), i.table)
		}

		rows, err := t2.PartitionRows2(ctx, i.partition)
		if err != nil {
			return err
		}

		i.rows2 = rows
	}

	err := i.rows2.Next2(ctx, frame)
	if err != nil && err == io.EOF {
		if err = i.rows2.Close(ctx); err != nil {
			return err
		}

		i.partition = nil
		i.rows2 = nil
		return i.Next2(ctx, frame)
	}

	return err
}

func (i *TableRowIter) Close(ctx *Context) error {
	if i.rows != nil {
		if err := i.rows.Close(ctx); err != nil {
			_ = i.partitions.Close(ctx)
			return err
		}
	}
	return i.partitions.Close(ctx)
}
