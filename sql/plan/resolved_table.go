package plan

import (
	"io"

	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

// ResolvedTable represents a resolved SQL Table.
type ResolvedTable struct {
	sql.Table
}

var _ sql.Node = (*ResolvedTable)(nil)

// NewResolvedTable creates a new instance of ResolvedTable.
func NewResolvedTable(table sql.Table) *ResolvedTable {
	return &ResolvedTable{table}
}

// Resolved implements the Resolvable interface.
func (*ResolvedTable) Resolved() bool {
	return true
}

// Children implements the Node interface.
func (*ResolvedTable) Children() []sql.Node { return nil }

// RowIter implements the RowIter interface.
func (t *ResolvedTable) RowIter(ctx *sql.Context) (sql.RowIter, error) {
	span, ctx := ctx.Span("plan.ResolvedTable")

	partitions, err := t.Table.Partitions(ctx)
	if err != nil {
		span.Finish()
		return nil, err
	}

	return sql.NewSpanIter(span, &tableIter{
		ctx:        ctx,
		table:      t.Table,
		partitions: partitions,
	}), nil
}

// TransformUp implements the Transformable interface.
func (t *ResolvedTable) TransformUp(f sql.TransformNodeFunc) (sql.Node, error) {
	return f(NewResolvedTable(t.Table))
}

// TransformExpressionsUp implements the Transformable interface.
func (t *ResolvedTable) TransformExpressionsUp(f sql.TransformExprFunc) (sql.Node, error) {
	return t, nil
}

type tableIter struct {
	ctx        *sql.Context
	table      sql.Table
	partitions sql.PartitionIter
	partition  sql.Partition
	rows       sql.RowIter
}

func (i *tableIter) Next() (sql.Row, error) {
	if i.partition == nil {
		partition, err := i.partitions.Next()
		if err != nil {
			if err == io.EOF {
				if err := i.partitions.Close(); err != nil {
					return nil, err
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
		if err := i.rows.Close(); err != nil {
			return nil, err
		}

		i.partition = nil
		i.rows = nil
		return i.Next()
	}

	return row, err
}

func (i *tableIter) Close() error {
	if i.rows != nil {
		if err := i.rows.Close(); err != nil {
			_ = i.partitions.Close()
			return err
		}
	}
	return i.partitions.Close()
}
