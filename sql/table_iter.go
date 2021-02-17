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
	ctx        *Context
	table      Table
	partitions PartitionIter
	partition  Partition
	rows       RowIter
}

// NewTableRowIter returns a new iterator over the rows in the partitions of the table given.
func NewTableRowIter(ctx *Context, table Table, partitions PartitionIter) *TableRowIter {
	return &TableRowIter{ctx: ctx, table: table, partitions: partitions}
}

func (i *TableRowIter) Next() (Row, error) {
	if i.ctx.Err() != nil {
		return nil, i.ctx.Err()
	}

	if i.partition == nil {
		partition, err := i.partitions.Next()
		if err != nil {
			if err == io.EOF {
				if e := i.partitions.Close(i.ctx); e != nil {
					return nil, e
				}
			}

			return nil, err
		}

		i.partition = partition
	}

	if i.rows == nil {
		rows, err := i.table.PartitionRows(i.ctx, i.partition)
		if err != nil {
			return nil, err
		}

		i.rows = rows
	}

	row, err := i.rows.Next()
	if err != nil && err == io.EOF {
		if err = i.rows.Close(i.ctx); err != nil {
			return nil, err
		}

		i.partition = nil
		i.rows = nil
		return i.Next()
	}

	return row, err
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
