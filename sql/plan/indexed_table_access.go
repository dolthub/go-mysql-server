package plan

import (
	"github.com/src-d/go-mysql-server/sql"
	"gopkg.in/src-d/go-errors.v1"
)

var ErrNoIndexableTable = errors.NewKind("expected an IndexableTable, couldn't find one in %v")
var ErrNoIndexedTableAccess = errors.NewKind("expected an IndexedTableAccess, couldn't find one in %v")
var ErrIndexedTableAccessNotInitialized = errors.NewKind("IndexedTableAccess must be initialized before RowIter is called")

// IndexedTableAccess represents an indexed lookup of a particular ResolvedTable. Unlike other kinds of UnaryNodes,
// this node supports being repeatedly initialized and being iterated over multiple times, potentially with different
// values returned every iteration. Used during analysis as part of the process of optimizing joins, replacing
// (wrapping) a ResolvedTable.
type IndexedTableAccess struct {
	*ResolvedTable
	indexedTable sql.Table
}

func (i *IndexedTableAccess) SetIndexLookup(ctx *sql.Context, lookup sql.IndexLookup) error {
	resolvedTable, ok := i.ResolvedTable.Table.(sql.IndexableTable)
	if !ok {
		return ErrNoIndexableTable.New(i.ResolvedTable)
	}

	i.indexedTable = resolvedTable.WithIndexLookup(lookup)
	return nil
}

func (i *IndexedTableAccess) RowIter(ctx *sql.Context) (sql.RowIter, error) {
	if i.indexedTable == nil {
		return nil, ErrIndexedTableAccessNotInitialized.New()
	}

	partIter, err := i.indexedTable.Partitions(ctx)
	if err != nil {
		return nil, err
	}

	return sql.NewTableIter(ctx, i.indexedTable, partIter), nil
}

func (i *IndexedTableAccess) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(i, len(children), 1)
	}

	resolvedTable, ok := children[0].(*ResolvedTable)
	if !ok {
		return nil, sql.ErrInvalidChildType.New(i, children[0], (*ResolvedTable)(nil))
	}

	return NewIndexedTable(resolvedTable), nil
}

func NewIndexedTable(resolvedTable *ResolvedTable) *IndexedTableAccess {
	return &IndexedTableAccess{
		ResolvedTable: resolvedTable,
	}
}

var _ sql.Node = (*IndexedTableAccess)(nil)