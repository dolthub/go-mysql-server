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
	ResolvedTable *ResolvedTable
	lb            LookupBuilder
	lookup        sql.IndexLookup
}

var _ sql.Node = (*IndexedTableAccess)(nil)
var _ sql.Nameable = (*IndexedTableAccess)(nil)
var _ sql.Node2 = (*IndexedTableAccess)(nil)
var _ sql.Expressioner = (*IndexedTableAccess)(nil)

// NewIndexedTableAccess returns a new IndexedTableAccess node that will use
// the LookupBuilder to build lookups. An index lookup will be calculated and
// applied for the row given in RowIter().
func NewIndexedTableAccess(resolvedTable *ResolvedTable, lb LookupBuilder) *IndexedTableAccess {
	return &IndexedTableAccess{
		ResolvedTable: resolvedTable,
		lb:            lb,
	}
}

// NewStaticIndexedTableAccess returns a new IndexedTableAccess node with the indexlookup given. It will be applied in
// RowIter() without consideration of the row given. The key expression should faithfully represent this lookup, but is
// only for display purposes.
func NewStaticIndexedTableAccess(resolvedTable *ResolvedTable, lookup sql.IndexLookup) *IndexedTableAccess {
	return &IndexedTableAccess{
		ResolvedTable: resolvedTable,
		lookup:        lookup,
	}
}

func (i *IndexedTableAccess) Resolved() bool {
	return i.ResolvedTable.Resolved()
}

func (i *IndexedTableAccess) Schema() sql.Schema {
	return i.ResolvedTable.Schema()
}

func (i *IndexedTableAccess) Children() []sql.Node {
	return nil
}

func (i *IndexedTableAccess) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(i, len(children), 0)
	}

	return i, nil
}

func (i *IndexedTableAccess) Name() string {
	return i.ResolvedTable.Name()
}

func (i *IndexedTableAccess) Database() sql.Database {
	return i.ResolvedTable.Database
}

func (i *IndexedTableAccess) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return i.ResolvedTable.CheckPrivileges(ctx, opChecker)
}

func (i *IndexedTableAccess) Index() sql.Index {
	if i.lookup != nil {
		return i.lookup.Index()
	}
	return i.lb.index
}

func (i *IndexedTableAccess) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	span, ctx := ctx.Span("plan.IndexedTableAccess")

	// child is ProcessTable, so get underlying
	t := i.ResolvedTable.Table
	if wrapperTable, ok := i.ResolvedTable.Table.(sql.TableWrapper); ok {
		t = wrapperTable.Underlying()
	}

	indexAddressableTable, ok := t.(sql.IndexAddressableTable)
	if !ok {
		return nil, ErrNoIndexableTable.New(i.ResolvedTable)
	}

	lookup, err := i.getLookup(ctx, row)
	if err != nil {
		return nil, err
	}

	indexedTable := indexAddressableTable.WithIndexLookup(lookup)
	partIter, err := indexedTable.Partitions(ctx)
	if err != nil {
		return nil, err
	}

	return sql.NewSpanIter(span, sql.NewTableRowIter(ctx, indexedTable, partIter)), nil
}

func (i *IndexedTableAccess) RowIter2(ctx *sql.Context, f *sql.RowFrame) (sql.RowIter2, error) {
	resolvedTable, ok := i.ResolvedTable.Table.(sql.IndexAddressableTable)
	if !ok {
		return nil, ErrNoIndexableTable.New(i.ResolvedTable)
	}

	lookup, err := i.getLookup2(ctx, f.Row2())
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

// CanBuildIndex returns whether an index lookup on this table can be successfully built for a zero-valued key. For a
// static lookup, no lookup needs to be built, so returns true.
func (i *IndexedTableAccess) CanBuildIndex(ctx *sql.Context) (bool, error) {
	// If the lookup was provided at analysis time (static evaluation), then an index was already built
	if i.lookup != nil {
		return true, nil
	}

	key := i.lb.GetZeroKey()
	lookup, err := i.lb.GetLookup(ctx, key)
	return err == nil && lookup != nil, nil
}

func (i *IndexedTableAccess) getLookup(ctx *sql.Context, row sql.Row) (sql.IndexLookup, error) {
	// if the lookup was provided at analysis time (static evaluation), use it.
	if i.lookup != nil {
		return i.lookup, nil
	}

	key, err := i.lb.GetKey(ctx, row)
	if err != nil {
		return nil, err
	}
	return i.lb.GetLookup(ctx, key)
}

func (i *IndexedTableAccess) getLookup2(ctx *sql.Context, row sql.Row2) (sql.IndexLookup, error) {
	// if the lookup was provided at analysis time (static evaluation), use it.
	if i.lookup != nil {
		return i.lookup, nil
	}

	key, err := i.lb.GetKey2(ctx, row)
	if err != nil {
		return nil, err
	}
	return i.lb.GetLookup(ctx, key)
}

func (i *IndexedTableAccess) String() string {
	pr := sql.NewTreePrinter()
	pr.WriteNode("IndexedTableAccess(%s)", i.ResolvedTable.Name())
	var children []string
	children = append(children, fmt.Sprintf("index: %s", formatIndexDecoratorString(i.Index())))
	if i.lookup != nil {
		children = append(children, fmt.Sprintf("filters: %s", i.lookup.Ranges().DebugString()))
	}
	if pt, ok := seethroughTableWrapper(i.ResolvedTable).(sql.ProjectedTable); ok {
		if len(pt.Projections()) > 0 {
			children = append(children, fmt.Sprintf("columns: %v", pt.Projections()))
		}
	}
	pr.WriteChildren(children...)
	return pr.String()
}

func formatIndexDecoratorString(idx sql.Index) string {
	var expStrs []string
	for _, e := range idx.Expressions() {
		expStrs = append(expStrs, e)
	}
	return fmt.Sprintf("[%s]", strings.Join(expStrs, ","))
}

func (i *IndexedTableAccess) DebugString() string {
	pr := sql.NewTreePrinter()
	pr.WriteNode("IndexedTableAccess(%s)", sql.DebugString(i.ResolvedTable))
	var children []string
	children = append(children, fmt.Sprintf("index: %s", formatIndexDecoratorString(i.Index())))
	if i.lookup != nil {
		children = append(children, fmt.Sprintf("filters: %s", i.lookup.Ranges().DebugString()))
		children = append(children, fmt.Sprintf("lookup: STATIC LOOKUP(%s)", sql.DebugString(i.lookup)))
	} else {
		children = append(children, fmt.Sprintf("lookup: %s", sql.DebugString(i.lb)))
	}
	if pt, ok := seethroughTableWrapper(i.ResolvedTable).(sql.ProjectedTable); ok {
		if len(pt.Projections()) > 0 {
			children = append(children, fmt.Sprintf("columns: %v", pt.Projections()))
		}
	}
	pr.WriteChildren(children...)
	return pr.String()
}

// Expressions implements sql.Expressioner
func (i *IndexedTableAccess) Expressions() []sql.Expression {
	if i.lookup != nil {
		return nil
	}
	return i.lb.Expressions()
}

// WithExpressions implements sql.Expressioner
func (i *IndexedTableAccess) WithExpressions(exprs ...sql.Expression) (sql.Node, error) {
	if i.lookup != nil {
		if len(exprs) != 0 {
			return nil, sql.ErrInvalidChildrenNumber.New(i, len(exprs), 0)
		}
		n := *i
		return &n, nil
	}
	lb, err := i.lb.WithExpressions(i, exprs...)
	if err != nil {
		return nil, err
	}
	return &IndexedTableAccess{
		ResolvedTable: i.ResolvedTable,
		lb:            lb,
	}, nil
}

// WithTable replaces the underlying ResolvedTable with the one given.
func (i IndexedTableAccess) WithTable(table sql.Table) (*IndexedTableAccess, error) {
	nrt, err := i.ResolvedTable.WithTable(table)
	if err != nil {
		return nil, err
	}
	i.ResolvedTable = nrt
	return &i, nil
}

// Partitions implements sql.Table
func (i *IndexedTableAccess) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	if i.lookup == nil {
		return i.ResolvedTable.Partitions(ctx)
	}

	table := i.baseTable()
	return table.Partitions(ctx)
}

// baseTable returns the underlying sql.Table with any static index lookup applied
func (i *IndexedTableAccess) baseTable() sql.Table {
	table := i.ResolvedTable.Table
	// This won't work if we add another layer of wrapping on top
	if tw, ok := table.(sql.TableWrapper); ok {
		table = tw.Underlying()
	}

	if indexAddressableTable, ok := table.(sql.IndexAddressable); ok {
		table = indexAddressableTable.WithIndexLookup(i.lookup)
	}
	return table
}

// PartitionRows implements sql.Table
func (i *IndexedTableAccess) PartitionRows(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	return i.baseTable().PartitionRows(ctx, partition)
}

// GetIndexLookup returns the sql.IndexLookup from an IndexedTableAccess.
// This method is exported for use in integration tests.
func GetIndexLookup(ita *IndexedTableAccess) sql.IndexLookup {
	return ita.lookup
}

// LookupBuilder abstracts getting equality index lookups for something like an
// indexed join, where a Row that has been sourced from somewhere is turned
// into an IndexLookup against a particular index based on some expressions
// that extract values out of the Row.
//
// Currently the analyzer constructs one of these and uses it for the
// IndexedTableAccess nodes below an indexed join, for example. This struct is
// also used to implement Expressioner on the IndexedTableAccess node.
type LookupBuilder struct {
	keyExprs  []sql.Expression
	keyExprs2 []sql.Expression2

	// When building the lookup, we will use an IndexBuilder. If the
	// extracted lookup value is NULL, but we have a non-NULL safe
	// comparison, then the lookup should return no values. But if the
	// comparison is NULL-safe, then the lookup should returns indexed
	// values having that value <=> NULL. For each |keyExpr|, this field
	// contains |true| if the lookup should also match NULLs, and |false|
	// otherwise.
	matchesNullMask []bool

	index sql.Index

	template, ranges *sql.RangeCollection
}

func NewLookupBuilder(index sql.Index, keyExprs []sql.Expression, matchesNullMask []bool) LookupBuilder {
	return LookupBuilder{
		index:           index,
		keyExprs:        keyExprs,
		matchesNullMask: matchesNullMask,
		ranges:          &sql.RangeCollection{Ranges: make([]sql.Range, 0, len(index.Expressions()))},
	}
}

func (lb LookupBuilder) NewTemplate(ctx *sql.Context, key sql.LookupBuilderKey) *sql.RangeCollection {
	// A range collection is naturally N x N, where N = number of index
	// columns. A specific lookup might only use a subset M (prefix) of those
	// columns for the filter. The range for a given lookup, then, will
	// be M x N. Non-diagonal range expressions will be default, filtering
	// no rows.
	template := &sql.RangeCollection{Ranges: make([]sql.Range, 0, len(key))}
	cets := lb.index.ColumnExpressionTypes(ctx)
	for i := range key {
		def := sql.RangeColumnExpr{LowerBound: sql.BelowNull{}, UpperBound: sql.AboveAll{}, Typ: cets[i].Type}
		rang := make([]sql.RangeColumnExpr, len(cets))
		for i := range rang {
			rang[i] = def
		}
		if key[i] == nil && lb.matchesNullMask[i] {
			rang[i] = sql.RangeColumnExpr{
				LowerBound: sql.BelowNull{},
				UpperBound: sql.AboveNull{},
				Typ:        cets[i].Type,
			}
		} else if lb.matchesNullMask[i] {
			rang[i] = sql.RangeColumnExpr{
				LowerBound: sql.BelowNull{},
				UpperBound: sql.AboveNull{},
				Typ:        cets[i].Type,
			}
		} else {
			rang[i] = sql.RangeColumnExpr{
				LowerBound: sql.Below{Key: sql.LookupPlaceholder(i)},
				UpperBound: sql.Above{Key: sql.LookupPlaceholder(i)},
				Typ:        cets[i].Type,
			}
		}
		template.Ranges = append(template.Ranges, rang)
	}
	return template
}

func (lb LookupBuilder) GetLookup(ctx *sql.Context, key sql.LookupBuilderKey) (sql.IndexLookup, error) {
	if lb.template == nil {
		lb.template = lb.NewTemplate(ctx, key)
		if lb.ranges == nil {
			lb.ranges = &sql.RangeCollection{}
		}
		lb.ranges.Template = lb.template.Ranges
	}
	lb.ranges.Key = key
	lb.ranges.NullMask = lb.matchesNullMask
	lb.ranges.Ranges = lb.ranges.Ranges[:0]
	for i := range key {
		if lb.matchesNullMask[i] {
			if key[i] == nil {
				lb.ranges.Ranges = append(lb.ranges.Ranges, lb.template.Ranges[i])
			}
		} else {
			// non-default filters lie on diagonal
			lb.template.Ranges[i][i].LowerBound = sql.Below{Key: key[i]}
			lb.template.Ranges[i][i].UpperBound = sql.Above{Key: key[i]}
			lb.ranges.Ranges = append(lb.ranges.Ranges, lb.template.Ranges[i])
		}
	}
	return lb.index.NewLookup(ctx, lb.ranges)
	//iexprs := lb.index.Expressions()
	//for i := range key {
	//	if key[i] == nil && lb.matchesNullMask[i] {
	//ib = ib.IsNull(ctx, iexprs[i])
	//	} else {
	//		ib = ib.Equals(ctx, iexprs[i], key[i])
	//	}
	//}
	//lookup, err := ib.Build(ctx)
	//return lookup, err
}

func (lb LookupBuilder) GetKey(ctx *sql.Context, row sql.Row) (sql.LookupBuilderKey, error) {
	key := make([]interface{}, len(lb.keyExprs))
	for i := range lb.keyExprs {
		var err error
		key[i], err = lb.keyExprs[i].Eval(ctx, row)
		if err != nil {
			return nil, err
		}
	}
	return key, nil
}

func (lb LookupBuilder) GetKey2(ctx *sql.Context, row sql.Row2) (sql.LookupBuilderKey, error) {
	key := make([]interface{}, len(lb.keyExprs))
	for i := range lb.keyExprs {
		var err error
		key[i], err = lb.keyExprs2[i].Eval2(ctx, row)
		if err != nil {
			return nil, err
		}
	}
	return key, nil
}

func (lb LookupBuilder) GetZeroKey() sql.LookupBuilderKey {
	key := make(sql.LookupBuilderKey, len(lb.keyExprs))
	for i, keyExpr := range lb.keyExprs {
		key[i] = keyExpr.Type().Zero()
	}
	return key
}

func (lb LookupBuilder) Index() sql.Index {
	return lb.index
}

func (lb LookupBuilder) Expressions() []sql.Expression {
	return lb.keyExprs
}

func (lb LookupBuilder) DebugString() string {
	keyExprs := make([]string, len(lb.keyExprs))
	for i := range lb.keyExprs {
		keyExprs[i] = sql.DebugString(lb.keyExprs[i])
	}
	return fmt.Sprintf("on %s, using fields %s", formatIndexDecoratorString(lb.Index()), strings.Join(keyExprs, ", "))
}

func (lb LookupBuilder) WithExpressions(node sql.Node, exprs ...sql.Expression) (LookupBuilder, error) {
	if len(exprs) != len(lb.keyExprs) {
		return LookupBuilder{}, sql.ErrInvalidChildrenNumber.New(node, len(exprs), len(lb.keyExprs))
	}
	return LookupBuilder{
		keyExprs:        exprs,
		index:           lb.index,
		matchesNullMask: lb.matchesNullMask,
	}, nil
}
