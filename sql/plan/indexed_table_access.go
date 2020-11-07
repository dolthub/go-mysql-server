package plan

import (
	"fmt"

	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/sql"
)

var ErrNoIndexableTable = errors.NewKind("expected an IndexableTable, couldn't find one in %v")
var ErrNoIndexedTableAccess = errors.NewKind("expected an IndexedTableAccess, couldn't find one in %v")

// IndexedTableAccess represents an indexed lookup of a particular ResolvedTable. The key used to access the indexed
// table is provided in RowIter().
type IndexedTableAccess struct {
	*ResolvedTable
	index    sql.Index
	keyExprs []sql.Expression
}

var _ sql.Node = (*IndexedTableAccess)(nil)

func (i *IndexedTableAccess) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	resolvedTable, ok := i.ResolvedTable.Table.(sql.IndexAddressableTable)
	if !ok {
		return nil, ErrNoIndexableTable.New(i.ResolvedTable)
	}

	// evaluate the key expressions against the row given to obtain the key for an index lookup
	key := make([]interface{}, len(i.keyExprs))
	for i, keyExpr := range i.keyExprs {
		var err error
		key[i], err = keyExpr.Eval(ctx, row)
		if err != nil {
			return nil, err
		}
	}

	lookup, err := i.index.Get(key...)
	if err != nil {
		return nil, err
	}

	indexedTable := resolvedTable.WithIndexLookup(lookup)

	partIter, err := indexedTable.Partitions(ctx)
	if err != nil {
		return nil, err
	}

	return sql.NewTableRowIter(ctx, indexedTable, partIter), nil
}

func (i *IndexedTableAccess) DebugString() string {
	return fmt.Sprintf("IndexedTableAccess(%s)", i.Name())
}

func (i *IndexedTableAccess) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(i, len(children), 1)
	}

	resolvedTable, ok := children[0].(*ResolvedTable)
	if !ok {
		return nil, sql.ErrInvalidChildType.New(i, children[0], (*ResolvedTable)(nil))
	}

	return NewIndexedTable(resolvedTable, i.index, i.keyExprs), nil
}

func NewIndexedTable(resolvedTable *ResolvedTable, index sql.Index, keyExprs []sql.Expression) *IndexedTableAccess {
	return &IndexedTableAccess{
		ResolvedTable: resolvedTable,
		index:         index,
		keyExprs:      keyExprs,
	}
}
