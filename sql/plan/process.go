package plan

import (
	"io"

	"github.com/liquidata-inc/go-mysql-server/sql"
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

// WithChildren implements the Node interface.
func (p *QueryProcess) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(p, len(children), 1)
	}

	return NewQueryProcess(children[0], p.Notify), nil
}

// RowIter implements the sql.Node interface.
func (p *QueryProcess) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	iter, err := p.Child.RowIter(ctx, row)
	if err != nil {
		return nil, err
	}

	return &trackedRowIter{iter: iter, onDone: p.Notify}, nil
}

func (p *QueryProcess) String() string { return p.Child.String() }

func (p *QueryProcess) DebugString() string { return sql.DebugString(p.Child) }

// ProcessIndexableTable is a wrapper for sql.Tables inside a query process
// that support indexing.
// It notifies the process manager about the status of a query when a
// partition is processed.
type ProcessIndexableTable struct {
	sql.DriverIndexableTable
	OnPartitionDone  NamedNotifyFunc
	OnPartitionStart NamedNotifyFunc
	OnRowNext        NamedNotifyFunc
}

// NewProcessIndexableTable returns a new ProcessIndexableTable.
func NewProcessIndexableTable(t sql.DriverIndexableTable, onPartitionDone, onPartitionStart, OnRowNext NamedNotifyFunc) *ProcessIndexableTable {
	return &ProcessIndexableTable{t, onPartitionDone, onPartitionStart, OnRowNext}
}

// Underlying implements sql.TableWrapper interface.
func (t *ProcessIndexableTable) Underlying() sql.Table {
	return t.DriverIndexableTable
}

// IndexKeyValues implements the sql.IndexableTable interface.
func (t *ProcessIndexableTable) IndexKeyValues(
	ctx *sql.Context,
	columns []string,
) (sql.PartitionIndexKeyValueIter, error) {
	iter, err := t.DriverIndexableTable.IndexKeyValues(ctx, columns)
	if err != nil {
		return nil, err
	}

	return &trackedPartitionIndexKeyValueIter{iter, t.OnPartitionDone, t.OnPartitionStart, t.OnRowNext}, nil
}

// PartitionRows implements the sql.Table interface.
func (t *ProcessIndexableTable) PartitionRows(ctx *sql.Context, p sql.Partition) (sql.RowIter, error) {
	iter, err := t.DriverIndexableTable.PartitionRows(ctx, p)
	if err != nil {
		return nil, err
	}

	partitionName := partitionName(p)
	if t.OnPartitionStart != nil {
		t.OnPartitionStart(partitionName)
	}

	var onDone NotifyFunc
	if t.OnPartitionDone != nil {
		onDone = func() {
			t.OnPartitionDone(partitionName)
		}
	}

	var onNext NotifyFunc
	if t.OnRowNext != nil {
		onNext = func() {
			t.OnRowNext(partitionName)
		}
	}

	return &trackedRowIter{iter: iter, onNext: onNext, onDone: onDone}, nil
}

var _ sql.DriverIndexableTable = (*ProcessIndexableTable)(nil)

// NamedNotifyFunc is a function to notify about some event with a string argument.
type NamedNotifyFunc func(name string)

// ProcessTable is a wrapper for sql.Tables inside a query process. It
// notifies the process manager about the status of a query when a partition
// is processed.
type ProcessTable struct {
	sql.Table
	OnPartitionDone  NamedNotifyFunc
	OnPartitionStart NamedNotifyFunc
	OnRowNext        NamedNotifyFunc
}

// NewProcessTable returns a new ProcessTable.
func NewProcessTable(t sql.Table, onPartitionDone, onPartitionStart, OnRowNext NamedNotifyFunc) *ProcessTable {
	return &ProcessTable{t, onPartitionDone, onPartitionStart, OnRowNext}
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

	partitionName := partitionName(p)
	if t.OnPartitionStart != nil {
		t.OnPartitionStart(partitionName)
	}

	var onDone NotifyFunc
	if t.OnPartitionDone != nil {
		onDone = func() {
			t.OnPartitionDone(partitionName)
		}
	}

	var onNext NotifyFunc
	if t.OnRowNext != nil {
		onNext = func() {
			t.OnRowNext(partitionName)
		}
	}

	return &trackedRowIter{iter: iter, onNext: onNext, onDone: onDone}, nil
}

type trackedRowIter struct {
	iter   sql.RowIter
	onDone NotifyFunc
	onNext NotifyFunc
}

func (i *trackedRowIter) done() {
	if i.onDone != nil {
		i.onDone()
		i.onDone = nil
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

	if i.onNext != nil {
		i.onNext()
	}

	return row, nil
}

func (i *trackedRowIter) Close() error {
	i.done()
	return i.iter.Close()
}

type trackedPartitionIndexKeyValueIter struct {
	sql.PartitionIndexKeyValueIter
	OnPartitionDone  NamedNotifyFunc
	OnPartitionStart NamedNotifyFunc
	OnRowNext        NamedNotifyFunc
}

func (i *trackedPartitionIndexKeyValueIter) Next() (sql.Partition, sql.IndexKeyValueIter, error) {
	p, iter, err := i.PartitionIndexKeyValueIter.Next()
	if err != nil {
		return nil, nil, err
	}

	partitionName := partitionName(p)
	if i.OnPartitionStart != nil {
		i.OnPartitionStart(partitionName)
	}

	var onDone NotifyFunc
	if i.OnPartitionDone != nil {
		onDone = func() {
			i.OnPartitionDone(partitionName)
		}
	}

	var onNext NotifyFunc
	if i.OnRowNext != nil {
		onNext = func() {
			i.OnRowNext(partitionName)
		}
	}

	return p, &trackedIndexKeyValueIter{iter, onDone, onNext}, nil
}

type trackedIndexKeyValueIter struct {
	iter   sql.IndexKeyValueIter
	onDone NotifyFunc
	onNext NotifyFunc
}

func (i *trackedIndexKeyValueIter) done() {
	if i.onDone != nil {
		i.onDone()
		i.onDone = nil
	}
}

func (i *trackedIndexKeyValueIter) Close() (err error) {
	i.done()
	if i.iter != nil {
		err = i.iter.Close()
	}
	return err
}

func (i *trackedIndexKeyValueIter) Next() ([]interface{}, []byte, error) {
	v, k, err := i.iter.Next()
	if err != nil {
		if err == io.EOF {
			i.done()
		}
		return nil, nil, err
	}

	if i.onNext != nil {
		i.onNext()
	}

	return v, k, nil
}

func partitionName(p sql.Partition) string {
	if n, ok := p.(sql.Nameable); ok {
		return n.Name()
	}
	return string(p.Key())
}
