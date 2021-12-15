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

package plan

import (
	"fmt"
	"strings"

	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/sql"
)

var ErrNoIndexableTable = errors.NewKind("expected an IndexableTable, couldn't find one in %v")
var ErrNoIndexedTableAccess = errors.NewKind("expected an IndexedTableAccess, couldn't find one in %v")

// IndexedTableAccess represents an indexed lookup of a particular ResolvedTable. The values for the key used to access
// the indexed table is provided in RowIter(), or during static analysis.
type IndexedTableAccess struct {
	*ResolvedTable
	index    sql.Index
	keyExprs []sql.Expression
	lookup   sql.IndexLookup
}

var _ sql.Node = (*IndexedTableAccess)(nil)
var _ sql.Expressioner = (*IndexedTableAccess)(nil)

// NewIndexedTableAccess returns a new IndexedTableAccess node with the index and key expressions given. An index
// lookup will be calculated and applied for the row given in RowIter().
func NewIndexedTableAccess(resolvedTable *ResolvedTable, index sql.Index, keyExprs []sql.Expression) *IndexedTableAccess {
	return &IndexedTableAccess{
		ResolvedTable: resolvedTable,
		index:         index,
		keyExprs:      keyExprs,
	}
}

// NewStaticIndexedTableAccess returns a new IndexedTableAccess node with the indexlookup given. It will be applied in
// RowIter() without consideration of the row given. The key expression should faithfully represent this lookup, but is
// only for display purposes.
func NewStaticIndexedTableAccess(resolvedTable *ResolvedTable, lookup sql.IndexLookup, index sql.Index, keyExprs []sql.Expression) *IndexedTableAccess {
	return &IndexedTableAccess{
		ResolvedTable: resolvedTable,
		index:         index,
		keyExprs:      keyExprs,
		lookup:        lookup,
	}
}

func (i *IndexedTableAccess) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	resolvedTable, ok := i.ResolvedTable.Table.(sql.IndexAddressableTable)
	if !ok {
		return nil, ErrNoIndexableTable.New(i.ResolvedTable)
	}

	lookup, err := i.getLookup(ctx, row)
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

func (i *IndexedTableAccess) CanBuildIndex(ctx *sql.Context) (bool, error) {
	// If the lookup was provided at analysis time (static evaluation), then an index was already built
	if i.lookup != nil {
		return true, nil
	}

	// Otherwise, grab the return type of the expressions and use the zero value
	key := make([]interface{}, len(i.keyExprs))
	for i, keyExpr := range i.keyExprs {
		key[i] = keyExpr.Type().Zero()
	}

	idxBuilder := sql.NewIndexBuilder(ctx, i.index)
	for keyIndex := 0; keyIndex < len(key); keyIndex++ {
		idxBuilder = idxBuilder.Equals(ctx, i.keyExprs[keyIndex].String(), key[keyIndex])
	}
	lookup, err := idxBuilder.Build(ctx)
	if err != nil {
		return false, err
	}

	return lookup != nil, nil
}

func (i *IndexedTableAccess) getLookup(ctx *sql.Context, row sql.Row) (sql.IndexLookup, error) {
	// if the lookup was provided at analysis time (static evaluation), use it.
	if i.lookup != nil {
		return i.lookup, nil
	}

	// otherwise, evaluate the key expressions against the row given to obtain the key for an index lookup
	key := make([]interface{}, len(i.keyExprs))
	for i, keyExpr := range i.keyExprs {
		var err error
		key[i], err = keyExpr.Eval(ctx, row)
		if err != nil {
			return nil, err
		}
	}

	idxExpressions := i.index.Expressions()
	idxBuilder := sql.NewIndexBuilder(ctx, i.index)
	for keyIndex := 0; keyIndex < len(key); keyIndex++ {
		idxBuilder = idxBuilder.Equals(ctx, idxExpressions[keyIndex], key[keyIndex])
	}
	lookup, err := idxBuilder.Build(ctx)
	if err != nil {
		return nil, err
	}

	return lookup, nil
}

func (i *IndexedTableAccess) String() string {
	return fmt.Sprintf("IndexedTableAccess(%s on %s)", i.Name(), formatIndexDecoratorString(i.index))
}

func formatIndexDecoratorString(idx sql.Index) string {
	var expStrs []string
	for _, e := range idx.Expressions() {
		expStrs = append(expStrs, e)
	}
	return fmt.Sprintf("[%s]", strings.Join(expStrs, ","))
}

func (i *IndexedTableAccess) DebugString() string {
	if i.lookup != nil {
		return fmt.Sprintf("IndexedTableAccess(%s on %s, using fields %s)", i.Name(), formatIndexDecoratorString(i.index), "STATIC LOOKUP("+sql.DebugString(i.lookup)+")")
	}
	keyExprs := make([]string, len(i.keyExprs))
	for j := range i.keyExprs {
		keyExprs[j] = sql.DebugString(i.keyExprs[j])
	}
	return fmt.Sprintf("IndexedTableAccess(%s on %s, using fields %s)", i.Name(), formatIndexDecoratorString(i.index), strings.Join(keyExprs, ", "))
}

// Expressions implements sql.Expressioner
func (i *IndexedTableAccess) Expressions() []sql.Expression {
	if i.lookup != nil {
		return nil
	}
	return i.keyExprs
}

// WithExpressions implements sql.Expressioner
func (i *IndexedTableAccess) WithExpressions(exprs ...sql.Expression) (sql.Node, error) {
	if i.lookup != nil {
		if len(exprs) != 0 {
			return nil, sql.ErrInvalidChildrenNumber.New(i, 0, len(i.keyExprs))
		}
		n := *i
		return &n, nil
	}
	if len(exprs) != len(i.keyExprs) {
		return nil, sql.ErrInvalidChildrenNumber.New(i, len(exprs), len(i.keyExprs))
	}
	return &IndexedTableAccess{
		ResolvedTable: i.ResolvedTable,
		index:         i.index,
		keyExprs:      exprs,
		lookup:        i.lookup,
	}, nil
}

// GetIndexLookup returns the sql.IndexLookup from an IndexedTableAccess.
// This method is exported for use in integration tests.
func GetIndexLookup(ita *IndexedTableAccess) sql.IndexLookup {
	return ita.lookup
}
