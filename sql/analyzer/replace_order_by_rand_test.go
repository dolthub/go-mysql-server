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
	tbl, resolvedTable, _ := newMockOrdinalTable(ctx, db, 100, 10)

	randFn, err := function.NewRand(ctx)
	require.NoError(t, err)

	seededRandFn, err := function.NewRand(ctx, expression.NewLiteral(int64(42), types.Int64))
	require.NoError(t, err)

	topN := func(conds []sql.SortCondition, limit int64, child sql.Node) sql.Node {
		return plan.NewTopN(conds, expression.NewLiteral(limit, types.Int64), child)
	}
	randCond := []sql.SortCondition{{Expr: randFn, Order: sql.Ascending}}

	tests := []struct {
		name      string
		inputPlan sql.Node
		rewritten bool
	}{
		{name: "simple_order_by_rand", inputPlan: topN(randCond, 5, resolvedTable), rewritten: true},
		{name: "reject_seeded_rand", inputPlan: topN([]sql.SortCondition{{Expr: seededRandFn, Order: sql.Ascending}}, 5, resolvedTable)},
		{name: "reject_multiple_sort_conditions", inputPlan: plan.NewTopN(
			[]sql.SortCondition{randCond[0], {Expr: expression.NewGetField(0, types.Int64, "id", false), Order: sql.Ascending}},
			expression.NewLiteral(int64(5), types.Int64), resolvedTable,
		)},
		{name: "reject_zero_limit", inputPlan: topN(randCond, 0, resolvedTable)},
		{name: "reject_negative_limit", inputPlan: topN(randCond, -1, resolvedTable)},
		{name: "reject_limit_exceeding_max_limit", inputPlan: topN(randCond, 15, resolvedTable)},
		{name: "reject_filter", inputPlan: topN(randCond, 5, plan.NewFilter(ctx, expression.NewLiteral(true, types.Boolean), resolvedTable))},
		{name: "reject_calc_found_rows", inputPlan: topN(randCond, 5, resolvedTable).(*plan.TopN).WithCalcFoundRows(true)},
		{name: "reject_non_addressable_table", inputPlan: topN(randCond, 5, plan.NewResolvedTable(&nonAddressableTable{tbl.Table}, db, nil))},
		{
			name: "separate_limit_and_sort",
			inputPlan: plan.NewLimit(
				expression.NewLiteral(int64(5), types.Int64),
				plan.NewSort(randCond, resolvedTable),
			),
			rewritten: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			res, same, err := replaceIdxOrderByRand(ctx, nil, tc.inputPlan, nil, nil, nil)
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
	db := memory.NewDatabase("testdb")
	tbl, resolvedTable, idx := newMockOrdinalTable(ctx, db, 100, 20)

	randSampleNode := plan.NewRandomSample(resolvedTable, tbl, idx, expression.NewLiteral(int64(5), types.Int64))

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

func newMockOrdinalTable(
	ctx *sql.Context,
	db *memory.Database,
	count uint64,
	maxLimit int64,
) (*mockOrdinalTable, *plan.ResolvedTable, *mockOrdinalIndex) {
	sch := sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "id", Type: types.Int64, Nullable: false, Source: "tbl", PrimaryKey: true},
		{Name: "val", Type: types.Text, Nullable: true, Source: "tbl"},
	})
	idx := &mockOrdinalIndex{
		Index: &memory.Index{
			DB:        db.Name(),
			TableName: "tbl",
			Name:      "PRIMARY",
			Exprs:     []sql.Expression{expression.NewGetField(0, types.Int64, "id", false)},
		},
		count:    count,
		maxLimit: maxLimit,
	}
	tbl := &mockOrdinalTable{Table: memory.NewTable(ctx, db, "tbl", sch, nil), idx: idx}
	return tbl, plan.NewResolvedTable(tbl, db, nil), idx
}

type mockOrdinalIndex struct {
	*memory.Index
	count    uint64
	maxLimit int64
}

var _ sql.OrdinalAddressableIndex = (*mockOrdinalIndex)(nil)

func (m *mockOrdinalIndex) Count(*sql.Context) (uint64, error) { return m.count, nil }
func (m *mockOrdinalIndex) MaxOrdinalSampleLimit(*sql.Context, uint64) int64 { return m.maxLimit }

type mockOrdinalTable struct {
	*memory.Table
	idx sql.Index
}

var _ sql.IndexAddressableTable = (*mockOrdinalTable)(nil)

func (m *mockOrdinalTable) GetIndexes(*sql.Context) ([]sql.Index, error) { return []sql.Index{m.idx}, nil }
func (m *mockOrdinalTable) IndexedAccess(*sql.Context, sql.IndexLookup) sql.IndexedTable {
	return &mockIndexedTable{m}
}

type mockIndexedTable struct {
	*mockOrdinalTable
}

var _ sql.IndexedTable = (*mockIndexedTable)(nil)

func (m *mockIndexedTable) LookupPartitions(ctx *sql.Context, lookup sql.IndexLookup) (sql.PartitionIter, error) {
	rows := make([]sql.Row, len(lookup.Ordinals))
	for i, ord := range lookup.Ordinals {
		rows[i] = sql.NewRow(int64(ord), fmt.Sprintf("val-%d", ord))
	}
	return sql.PartitionsToPartitionIter(&mockPartition{rows: rows}), nil
}

func (m *mockIndexedTable) PartitionRows(ctx *sql.Context, part sql.Partition) (sql.RowIter, error) {
	return sql.RowsToRowIter(part.(*mockPartition).rows...), nil
}

type mockPartition struct {
	rows []sql.Row
}

func (m *mockPartition) Key() []byte { return nil }

type nonAddressableTable struct {
	sql.Table
}
