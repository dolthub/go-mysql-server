package sql

import (
	"context"
	"io"
)

type TableRowIter struct {
	ctx        *Context
	table      Table
	partitions PartitionIter
	partition  Partition
	rows       RowIter
}

func NewTableIter(ctx *Context, table Table, partitions PartitionIter) *TableRowIter {
	return &TableRowIter{ctx: ctx, table: table, partitions: partitions}
}

func (i *TableRowIter) Next() (Row, error) {
	select {
	case <-i.ctx.Done():
		return nil, context.Canceled
	default:
	}

	if i.partition == nil {
		partition, err := i.partitions.Next()
		if err != nil {
			if err == io.EOF {
				if e := i.partitions.Close(); e != nil {
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
		if err = i.rows.Close(); err != nil {
			return nil, err
		}

		i.partition = nil
		i.rows = nil
		return i.Next()
	}

	return row, err
}

func (i *TableRowIter) Close() error {
	if i.rows != nil {
		if err := i.rows.Close(); err != nil {
			_ = i.partitions.Close()
			return err
		}
	}
	return i.partitions.Close()
}
