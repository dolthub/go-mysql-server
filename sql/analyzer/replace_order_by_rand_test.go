// Copyright 2026 Dolthub, Inc.
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

package analyzer

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/expression/function"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/rowexec"
	"github.com/dolthub/go-mysql-server/sql/transform"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestReplaceIdxOrderByRand(t *testing.T) {
	ctx := sql.NewEmptyContext()
	db := memory.NewDatabase("testdb")
	sch := sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "id", Type: types.Int64, Nullable: false, Source: "tbl", PrimaryKey: true},
		{Name: "val", Type: types.Text, Nullable: true, Source: "tbl"},
	})

	tableRows := make([]sql.Row, 100)
	for i := range tableRows {
		tableRows[i] = sql.NewRow(int64(i), fmt.Sprintf("row-%d", i))
	}

	idx := &mockOrdinalIndex{
		id:       "PRIMARY",
		table:    "tbl",
		count:    100,
		maxLimit: 10,
	}

	tbl := &mockOrdinalTable{
		name:     "tbl",
		schema:   sch.Schema,
		indexes:  []sql.Index{idx},
		allRows:  tableRows,
		ordIndex: idx,
	}
	resolvedTable := plan.NewResolvedTable(tbl, db, nil)

	randFn, err := function.NewRand(ctx)
	require.NoError(t, err)

	seededRandFn, err := function.NewRand(
		ctx,
		expression.NewLiteral(int64(42), types.Int64),
	)
	require.NoError(t, err)

	tests := []struct {
		name      string
		inputPlan sql.Node
		rewritten bool
	}{
		{
			name: "simple_order_by_rand",
			inputPlan: plan.NewTopN(
				[]sql.SortCondition{{Expr: randFn, Order: sql.Ascending}},
				expression.NewLiteral(int64(5), types.Int64),
				resolvedTable,
			),
			rewritten: true,
		},
		{
			name: "reject_seeded_rand",
			inputPlan: plan.NewTopN(
				[]sql.SortCondition{{Expr: seededRandFn, Order: sql.Ascending}},
				expression.NewLiteral(int64(5), types.Int64),
				resolvedTable,
			),
			rewritten: false,
		},
		{
			name: "reject_multiple_sort_conditions",
			inputPlan: plan.NewTopN(
				[]sql.SortCondition{
					{Expr: randFn, Order: sql.Ascending},
					{
						Expr:  expression.NewGetField(0, types.Int64, "id", false),
						Order: sql.Ascending,
					},
				},
				expression.NewLiteral(int64(5), types.Int64),
				resolvedTable,
			),
			rewritten: false,
		},
		{
			name: "reject_zero_limit",
			inputPlan: plan.NewTopN(
				[]sql.SortCondition{{Expr: randFn, Order: sql.Ascending}},
				expression.NewLiteral(int64(0), types.Int64),
				resolvedTable,
			),
			rewritten: false,
		},
		{
			name: "reject_negative_limit",
			inputPlan: plan.NewTopN(
				[]sql.SortCondition{{Expr: randFn, Order: sql.Ascending}},
				expression.NewLiteral(int64(-1), types.Int64),
				resolvedTable,
			),
			rewritten: false,
		},
		{
			name: "reject_limit_exceeding_max_limit",
			inputPlan: plan.NewTopN(
				[]sql.SortCondition{{Expr: randFn, Order: sql.Ascending}},
				expression.NewLiteral(int64(15), types.Int64),
				resolvedTable,
			),
			rewritten: false,
		},
		{
			name: "reject_filter",
			inputPlan: plan.NewTopN(
				[]sql.SortCondition{{Expr: randFn, Order: sql.Ascending}},
				expression.NewLiteral(int64(5), types.Int64),
				plan.NewFilter(
					ctx,
					expression.NewLiteral(true, types.Boolean),
					resolvedTable,
				),
			),
			rewritten: false,
		},
		{
			name: "reject_calc_found_rows",
			inputPlan: plan.NewTopN(
				[]sql.SortCondition{{Expr: randFn, Order: sql.Ascending}},
				expression.NewLiteral(int64(5), types.Int64),
				resolvedTable,
			).WithCalcFoundRows(true),
			rewritten: false,
		},
		{
			name: "reject_non_addressable_table",
			inputPlan: plan.NewTopN(
				[]sql.SortCondition{{Expr: randFn, Order: sql.Ascending}},
				expression.NewLiteral(int64(5), types.Int64),
				plan.NewResolvedTable(&mockNonAddressableTable{
					name:   "non_addr",
					schema: sch.Schema,
				}, db, nil),
			),
			rewritten: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			res, same, err := replaceIdxOrderByRand(
				ctx,
				nil,
				tc.inputPlan,
				nil,
				nil,
				nil,
			)
			require.NoError(t, err)
			require.Equal(t, tc.rewritten, same == transform.NewTree)
			if tc.rewritten {
				_, isRandomSample := res.(*plan.RandomSample)
				require.True(t, isRandomSample)
			}
		})
	}
}

func TestRandomSampleExecution(t *testing.T) {
	ctx := sql.NewEmptyContext()
	sch := sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "id", Type: types.Int64, Nullable: false, Source: "tbl", PrimaryKey: true},
		{Name: "val", Type: types.Text, Nullable: true, Source: "tbl"},
	})

	tableRows := make([]sql.Row, 100)
	for i := range tableRows {
		tableRows[i] = sql.NewRow(int64(i), fmt.Sprintf("val-%d", i))
	}

	idx := &mockOrdinalIndex{
		id:       "PRIMARY",
		table:    "tbl",
		count:    100,
		maxLimit: 20,
	}

	tbl := &mockOrdinalTable{
		name:     "tbl",
		schema:   sch.Schema,
		indexes:  []sql.Index{idx},
		allRows:  tableRows,
		ordIndex: idx,
	}
	db := memory.NewDatabase("testdb")
	resolvedTable := plan.NewResolvedTable(tbl, db, nil)

	randSampleNode := plan.NewRandomSample(
		resolvedTable,
		tbl,
		idx,
		expression.NewLiteral(int64(5), types.Int64),
	)

	builder := rowexec.NewBuilder(nil, sql.EngineOverrides{})
	iter, err := builder.Build(ctx, randSampleNode, nil)
	require.NoError(t, err)
	defer iter.Close(ctx)

	rows, err := sql.RowIterToRows(ctx, iter)
	require.NoError(t, err)
	require.Len(t, rows, 5)

	seen := make(map[int64]bool)
	for _, r := range rows {
		id := r[0].(int64)
		require.False(t, seen[id], "duplicate row sampled: %d", id)
		seen[id] = true
	}
}

type mockOrdinalIndex struct {
	id       string
	table    string
	count    uint64
	maxLimit int64
}

var _ sql.OrdinalAddressableIndex = (*mockOrdinalIndex)(nil)

func (m *mockOrdinalIndex) ID() string                              { return m.id }
func (m *mockOrdinalIndex) Database() string                        { return "testdb" }
func (m *mockOrdinalIndex) Table() string                           { return m.table }
func (m *mockOrdinalIndex) Expressions() []string                   { return []string{"id"} }
func (m *mockOrdinalIndex) IsUnique() bool                          { return true }
func (m *mockOrdinalIndex) IsSpatial() bool                         { return false }
func (m *mockOrdinalIndex) IsFullText() bool                        { return false }
func (m *mockOrdinalIndex) IsVector() bool                          { return false }
func (m *mockOrdinalIndex) Comment() string                         { return "" }
func (m *mockOrdinalIndex) IndexType() string                       { return "BTREE" }
func (m *mockOrdinalIndex) IsGenerated() bool                       { return false }
func (m *mockOrdinalIndex) CanSupport(*sql.Context, ...sql.Range) bool { return true }
func (m *mockOrdinalIndex) CanSupportOrderBy(sql.Expression) bool   { return false }
func (m *mockOrdinalIndex) CoversColumns([]string) bool             { return true }
func (m *mockOrdinalIndex) PrefixLengths() []uint16                 { return nil }
func (m *mockOrdinalIndex) ColumnExpressionTypes(*sql.Context) []sql.ColumnExpressionType {
	return []sql.ColumnExpressionType{{Type: types.Int64, Expression: "id"}}
}
func (m *mockOrdinalIndex) Count(*sql.Context) (uint64, error) {
	return m.count, nil
}
func (m *mockOrdinalIndex) MaxOrdinalSampleLimit(*sql.Context, uint64) int64 {
	return m.maxLimit
}

type mockOrdinalTable struct {
	name     string
	schema   sql.Schema
	indexes  []sql.Index
	allRows  []sql.Row
	ordIndex *mockOrdinalIndex
}

var _ sql.Table = (*mockOrdinalTable)(nil)
var _ sql.IndexAddressableTable = (*mockOrdinalTable)(nil)

func (m *mockOrdinalTable) Name() string                   { return m.name }
func (m *mockOrdinalTable) String() string                 { return m.name }
func (m *mockOrdinalTable) Schema(*sql.Context) sql.Schema { return m.schema }
func (m *mockOrdinalTable) Collation() sql.CollationID     { return sql.Collation_binary }
func (m *mockOrdinalTable) Partitions(*sql.Context) (sql.PartitionIter, error) {
	return sql.PartitionsToPartitionIter(&mockPartition{}), nil
}
func (m *mockOrdinalTable) PartitionRows(*sql.Context, sql.Partition) (sql.RowIter, error) {
	return sql.RowsToRowIter(m.allRows...), nil
}
func (m *mockOrdinalTable) GetIndexes(*sql.Context) ([]sql.Index, error) {
	return m.indexes, nil
}
func (m *mockOrdinalTable) IndexedAccess(*sql.Context, sql.IndexLookup) sql.IndexedTable {
	return &mockIndexedTable{parent: m}
}
func (m *mockOrdinalTable) PreciseMatch() bool {
	return true
}

type mockIndexedTable struct {
	parent *mockOrdinalTable
}

var _ sql.IndexedTable = (*mockIndexedTable)(nil)

func (m *mockIndexedTable) Name() string                   { return m.parent.name }
func (m *mockIndexedTable) String() string                 { return m.parent.name }
func (m *mockIndexedTable) Schema(*sql.Context) sql.Schema { return m.parent.schema }
func (m *mockIndexedTable) Collation() sql.CollationID     { return sql.Collation_binary }
func (m *mockIndexedTable) Partitions(*sql.Context) (sql.PartitionIter, error) {
	return sql.PartitionsToPartitionIter(&mockPartition{}), nil
}
func (m *mockIndexedTable) PartitionRows(ctx *sql.Context, part sql.Partition) (sql.RowIter, error) {
	op, ok := part.(*mockOrdinalPartition)
	if !ok {
		return sql.RowsToRowIter(m.parent.allRows...), nil
	}
	rows := make([]sql.Row, 0, len(op.ordinals))
	for _, ord := range op.ordinals {
		if ord < uint64(len(m.parent.allRows)) {
			rows = append(rows, m.parent.allRows[ord])
		}
	}
	return sql.RowsToRowIter(rows...), nil
}
func (m *mockIndexedTable) LookupPartitions(ctx *sql.Context, lookup sql.IndexLookup) (sql.PartitionIter, error) {
	return sql.PartitionsToPartitionIter(&mockOrdinalPartition{ordinals: lookup.Ordinals}), nil
}

type mockPartition struct{}

func (m *mockPartition) Key() []byte { return []byte("part") }

type mockOrdinalPartition struct {
	ordinals []uint64
}

func (m *mockOrdinalPartition) Key() []byte { return []byte("ordinal_part") }

type mockNonAddressableTable struct {
	name   string
	schema sql.Schema
}

var _ sql.Table = (*mockNonAddressableTable)(nil)

func (m *mockNonAddressableTable) Name() string                   { return m.name }
func (m *mockNonAddressableTable) String() string                 { return m.name }
func (m *mockNonAddressableTable) Schema(*sql.Context) sql.Schema { return m.schema }
func (m *mockNonAddressableTable) Collation() sql.CollationID     { return sql.Collation_binary }
func (m *mockNonAddressableTable) Partitions(*sql.Context) (sql.PartitionIter, error) {
	return sql.PartitionsToPartitionIter(&mockPartition{}), nil
}
func (m *mockNonAddressableTable) PartitionRows(*sql.Context, sql.Partition) (sql.RowIter, error) {
	return sql.RowsToRowIter(), nil
}
