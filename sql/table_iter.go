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
	table      Table
	partitions PartitionIter
	partition  Partition
	rows       RowIter
	valueRows  ValueRowWriter
	colSet     ColSet
}

var _ RowIter = (*TableRowIter)(nil)

// NewTableRowIter returns a new iterator over the rows in the partitions of the table given.
func NewTableRowIter(ctx *Context, table Table, partitions PartitionIter) *TableRowIter {
	return &TableRowIter{table: table, partitions: partitions}
}

// NewTableValueRowIter returns a new iterator over the rows in the partitions of the table given,
// when the table has specific ColumnIds in a plan.
func NewTableValueRowIter(ctx *Context, table Table, cols ColSet, partitions PartitionIter) RowIter {
	return &TableRowIter{table: table, partitions: partitions, colSet: cols}
}

func (i *TableRowIter) Next(ctx *Context) (Row, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
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
	if err == io.EOF {
		if err = i.rows.Close(ctx); err != nil {
			return nil, err
		}

		i.partition = nil
		i.rows = nil
		row, err = i.Next(ctx)
	}
	return row, err
}

// WriteNextValueRow implements the ValueRowWriter interface
func (i *TableRowIter) WriteNextValueRow(ctx *Context, row ValueRow) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
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

	if i.valueRows == nil {
		rows, err := i.table.PartitionRows(ctx, i.partition)
		if err != nil {
			return err
		}
		i.valueRows = NewValueRowWriter(i.colSet, rows.(ValueRowIter))
	}

	err := i.valueRows.WriteNextValueRow(ctx, row)

	if err == io.EOF {
		if err = i.valueRows.Close(ctx); err != nil {
			return err
		}
		i.partition = nil
		i.valueRows = nil
		return i.WriteNextValueRow(ctx, row)
	}

	return err
}

// IsValueRowWriter implements the ValueRowWriter interface.
func (i *TableRowIter) IsValueRowWriter(ctx *Context) bool {
	if i.colSet.Empty() {
		return false
	}
	if i.partition == nil {
		partition, err := i.partitions.Next(ctx)
		if err != nil {
			return false
		}
		i.partition = partition
	}
	if i.valueRows == nil {
		rows, err := i.table.PartitionRows(ctx, i.partition)
		if err != nil {
			return false
		}
		i.rows = rows
		valRowIter, ok := rows.(ValueRowIter)
		if !ok || !valRowIter.IsValueRowIter(ctx) {
			return false
		}
		i.valueRows = NewValueRowWriter(i.colSet, valRowIter)
	}
	return true
}

// Columns implements the ValueRowWriter interface
func (i *TableRowIter) Columns() ColSet {
	return i.colSet
}

func (i *TableRowIter) Close(ctx *Context) error {
	if i.rows != nil {
		if err := i.rows.Close(ctx); err != nil {
			_ = i.partitions.Close(ctx)
			return err
		}
	}
	if i.valueRows != nil {
		if err := i.valueRows.Close(ctx); err != nil {
			_ = i.partitions.Close(ctx)
			return err
		}
	}
	return i.partitions.Close(ctx)
}
