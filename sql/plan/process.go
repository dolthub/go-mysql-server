package plan

import (
	"io"

	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

// QueryProcess represents a running query process node. It will use a callback
// to notify when it has finished running.
type QueryProcess struct {
	UnaryNode
	Notify NotifyFunc
}

// NotifyFunc is a function to notify about some event.
type NotifyFunc func()

// NewQueryProcess creates a new QueryProcess node.
func NewQueryProcess(node sql.Node, notify NotifyFunc) *QueryProcess {
	return &QueryProcess{UnaryNode{Child: node}, notify}
}

// TransformUp implements the sql.Node interface.
func (p *QueryProcess) TransformUp(f sql.TransformNodeFunc) (sql.Node, error) {
	n, err := p.Child.TransformUp(f)
	if err != nil {
		return nil, err
	}

	np := *p
	np.Child = n
	return &np, nil
}

// TransformExpressionsUp implements the sql.Node interface.
func (p *QueryProcess) TransformExpressionsUp(f sql.TransformExprFunc) (sql.Node, error) {
	n, err := p.Child.TransformExpressionsUp(f)
	if err != nil {
		return nil, err
	}

	np := *p
	np.Child = n
	return &np, nil
}

// RowIter implements the sql.Node interface.
func (p *QueryProcess) RowIter(ctx *sql.Context) (sql.RowIter, error) {
	iter, err := p.Child.RowIter(ctx)
	if err != nil {
		return nil, err
	}

	return &trackedRowIter{iter, p.Notify}, nil
}

func (p *QueryProcess) String() string { return p.Child.String() }

// ProcessIndexableTable is a wrapper for sql.Tables inside a query process
// that support indexing.
// It notifies the process manager about the status of a query when a
// partition is processed.
type ProcessIndexableTable struct {
	sql.IndexableTable
	Notify NotifyFunc
}

// NewProcessIndexableTable returns a new ProcessIndexableTable.
func NewProcessIndexableTable(t sql.IndexableTable, notify NotifyFunc) *ProcessIndexableTable {
	return &ProcessIndexableTable{t, notify}
}

// Underlying implements sql.TableWrapper interface.
func (t *ProcessIndexableTable) Underlying() sql.Table {
	return t.IndexableTable
}

// IndexKeyValues implements the sql.IndexableTable interface.
func (t *ProcessIndexableTable) IndexKeyValues(
	ctx *sql.Context,
	columns []string,
) (sql.PartitionIndexKeyValueIter, error) {
	iter, err := t.IndexableTable.IndexKeyValues(ctx, columns)
	if err != nil {
		return nil, err
	}

	return &trackedPartitionIndexKeyValueIter{iter, t.Notify}, nil
}

// PartitionRows implements the sql.Table interface.
func (t *ProcessIndexableTable) PartitionRows(ctx *sql.Context, p sql.Partition) (sql.RowIter, error) {
	iter, err := t.IndexableTable.PartitionRows(ctx, p)
	if err != nil {
		return nil, err
	}

	return &trackedRowIter{iter, t.Notify}, nil
}

var _ sql.IndexableTable = (*ProcessIndexableTable)(nil)

// ProcessTable is a wrapper for sql.Tables inside a query process. It
// notifies the process manager about the status of a query when a partition
// is processed.
type ProcessTable struct {
	sql.Table
	Notify NotifyFunc
}

// NewProcessTable returns a new ProcessTable.
func NewProcessTable(t sql.Table, notify NotifyFunc) *ProcessTable {
	return &ProcessTable{t, notify}
}

// Underlying implements sql.TableWrapper interface.
func (t *ProcessTable) Underlying() sql.Table {
	return t.Table
}

// PartitionRows implements the sql.Table interface.
func (t *ProcessTable) PartitionRows(ctx *sql.Context, p sql.Partition) (sql.RowIter, error) {
	iter, err := t.Table.PartitionRows(ctx, p)
	if err != nil {
		return nil, err
	}

	return &trackedRowIter{iter, t.Notify}, nil
}

type trackedRowIter struct {
	iter   sql.RowIter
	notify NotifyFunc
}

func (i *trackedRowIter) done() {
	if i.notify != nil {
		i.notify()
		i.notify = nil
	}
}

func (i *trackedRowIter) Next() (sql.Row, error) {
	row, err := i.iter.Next()
	if err != nil {
		if err == io.EOF {
			i.done()
		}
		return nil, err
	}
	return row, nil
}

func (i *trackedRowIter) Close() error {
	i.done()
	return i.iter.Close()
}

type trackedPartitionIndexKeyValueIter struct {
	sql.PartitionIndexKeyValueIter
	notify NotifyFunc
}

func (i *trackedPartitionIndexKeyValueIter) Next() (sql.Partition, sql.IndexKeyValueIter, error) {
	p, iter, err := i.PartitionIndexKeyValueIter.Next()
	if err != nil {
		return nil, nil, err
	}

	return p, &trackedIndexKeyValueIter{iter, i.notify}, nil
}

type trackedIndexKeyValueIter struct {
	iter   sql.IndexKeyValueIter
	notify NotifyFunc
}

func (i *trackedIndexKeyValueIter) done() {
	if i.notify != nil {
		i.notify()
		i.notify = nil
	}
}

func (i *trackedIndexKeyValueIter) Close() error {
	i.done()
	return nil
}

func (i *trackedIndexKeyValueIter) Next() ([]interface{}, []byte, error) {
	v, k, err := i.iter.Next()
	if err != nil {
		if err == io.EOF {
			i.done()
		}
		return nil, nil, err
	}

	return v, k, nil
}
