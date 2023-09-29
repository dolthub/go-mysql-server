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

package rowexec

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestJoinSchema(t *testing.T) {
	db := memory.NewDatabase("test")
	t1 := plan.NewResolvedTable(memory.NewTable(db.BaseDatabase, "foo", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "a", Source: "foo", Type: types.Int64},
	}), nil), nil, nil)

	t2 := plan.NewResolvedTable(memory.NewTable(db.BaseDatabase, "bar", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "b", Source: "bar", Type: types.Int64},
	}), nil), nil, nil)

	t.Run("inner", func(t *testing.T) {
		j := plan.NewInnerJoin(t1, t2, nil)
		result := j.Schema()

		require.Equal(t, sql.Schema{
			{Name: "a", Source: "foo", Type: types.Int64},
			{Name: "b", Source: "bar", Type: types.Int64},
		}, result)
	})

	t.Run("left", func(t *testing.T) {
		j := plan.NewLeftOuterJoin(t1, t2, nil)
		result := j.Schema()

		require.Equal(t, sql.Schema{
			{Name: "a", Source: "foo", Type: types.Int64},
			{Name: "b", Source: "bar", Type: types.Int64, Nullable: true},
		}, result)
	})

	t.Run("right", func(t *testing.T) {
		j := plan.NewRightOuterJoin(t1, t2, nil)
		result := j.Schema()

		require.Equal(t, sql.Schema{
			{Name: "a", Source: "foo", Type: types.Int64, Nullable: true},
			{Name: "b", Source: "bar", Type: types.Int64},
		}, result)
	})
}

func TestInnerJoin(t *testing.T) {
	db := memory.NewDatabase("test")
	pro := memory.NewDBProvider(db)
	ctx := newContext(pro)
	testInnerJoin(t, db, ctx)
}

func TestMultiPassInnerJoin(t *testing.T) {
	db := memory.NewDatabase("test")
	pro := memory.NewDBProvider(db)
	ctx := sql.NewContext(context.TODO(), sql.WithMemoryManager(
		sql.NewMemoryManager(mockReporter{2, 1}),
	), sql.WithSession(memory.NewSession(sql.NewBaseSession(), pro)))
	testInnerJoin(t, db, ctx)
}

func testInnerJoin(t *testing.T, db *memory.Database, ctx *sql.Context) {
	t.Helper()

	require := require.New(t)

	ltable := memory.NewTable(db.BaseDatabase, "left", lSchema, nil)
	rtable := memory.NewTable(db.BaseDatabase, "right", rSchema, nil)
	insertData(t, ctx, ltable)
	insertData(t, ctx, rtable)

	j := plan.NewInnerJoin(
		plan.NewResolvedTable(ltable, nil, nil),
		plan.NewResolvedTable(rtable, nil, nil),
		expression.NewEquals(
			expression.NewGetField(0, types.Text, "lcol1", false),
			expression.NewGetField(4, types.Text, "rcol1", false),
		))

	rows := collectRows(t, ctx, j)
	require.Len(rows, 2)

	require.Equal([]sql.Row{
		{"col1_1", "col2_1", int32(1), int64(2), "col1_1", "col2_1", int32(1), int64(2)},
		{"col1_2", "col2_2", int32(3), int64(4), "col1_2", "col2_2", int32(3), int64(4)},
	}, rows)
}

func TestInnerJoinEmpty(t *testing.T) {
	require := require.New(t)

	db := memory.NewDatabase("test")
	pro := memory.NewDBProvider(db)
	ctx := newContext(pro)

	ltable := memory.NewTable(db.BaseDatabase, "left", lSchema, nil)
	rtable := memory.NewTable(db.BaseDatabase, "right", rSchema, nil)

	j := plan.NewInnerJoin(
		plan.NewResolvedTable(ltable, nil, nil),
		plan.NewResolvedTable(rtable, nil, nil),
		expression.NewEquals(
			expression.NewGetField(0, types.Text, "lcol1", false),
			expression.NewGetField(4, types.Text, "rcol1", false),
		))

	iter, err := DefaultBuilder.Build(ctx, j, nil)
	require.NoError(err)

	assertRows(t, ctx, iter, 0)
}

func BenchmarkInnerJoin(b *testing.B) {
	db := memory.NewDatabase("test")
	pro := memory.NewDBProvider(db)

	t1 := memory.NewTable(db.BaseDatabase, "foo", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "a", Source: "foo", Type: types.Int64},
		{Name: "b", Source: "foo", Type: types.Text},
	}), nil)

	t2 := memory.NewTable(db.BaseDatabase, "bar", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "a", Source: "bar", Type: types.Int64},
		{Name: "b", Source: "bar", Type: types.Text},
	}), nil)

	for i := 0; i < 5; i++ {
		t1.Insert(sql.NewEmptyContext(), sql.NewRow(int64(i), fmt.Sprintf("t1_%d", i)))
		t2.Insert(sql.NewEmptyContext(), sql.NewRow(int64(i), fmt.Sprintf("t2_%d", i)))
	}

	n1 := plan.NewInnerJoin(
		plan.NewResolvedTable(t1, nil, nil),
		plan.NewResolvedTable(t2, nil, nil),
		expression.NewEquals(
			expression.NewGetField(0, types.Int64, "a", false),
			expression.NewGetField(2, types.Int64, "a", false),
		),
	)

	n2 := plan.NewFilter(
		expression.NewEquals(
			expression.NewGetField(0, types.Int64, "a", false),
			expression.NewGetField(2, types.Int64, "a", false),
		),
		plan.NewCrossJoin(
			plan.NewResolvedTable(t1, nil, nil),
			plan.NewResolvedTable(t2, nil, nil),
		),
	)

	expected := []sql.Row{
		{int64(0), "t1_0", int64(0), "t2_0"},
		{int64(1), "t1_1", int64(1), "t2_1"},
		{int64(2), "t1_2", int64(2), "t2_2"},
		{int64(3), "t1_3", int64(3), "t2_3"},
		{int64(4), "t1_4", int64(4), "t2_4"},
	}

	ctx := sql.NewContext(context.Background(), sql.WithMemoryManager(
		sql.NewMemoryManager(mockReporter{1, 5}),
	), sql.WithSession(memory.NewSession(sql.NewBaseSession(), pro)))

	b.Run("inner join", func(b *testing.B) {
		require := require.New(b)

		for i := 0; i < b.N; i++ {
			iter, err := DefaultBuilder.Build(ctx, n1, nil)
			require.NoError(err)

			rows, err := sql.RowIterToRows(ctx, nil, iter)
			require.NoError(err)

			require.Equal(expected, rows)
		}
	})

	b.Run("within memory threshold", func(b *testing.B) {
		require := require.New(b)

		for i := 0; i < b.N; i++ {
			iter, err := DefaultBuilder.Build(ctx, n1, nil)
			require.NoError(err)

			rows, err := sql.RowIterToRows(ctx, nil, iter)
			require.NoError(err)

			require.Equal(expected, rows)
		}
	})

	b.Run("cross join with filter", func(b *testing.B) {
		require := require.New(b)

		for i := 0; i < b.N; i++ {
			iter, err := DefaultBuilder.Build(ctx, n2, nil)
			require.NoError(err)

			rows, err := sql.RowIterToRows(ctx, nil, iter)
			require.NoError(err)

			require.Equal(expected, rows)
		}
	})
}

func TestLeftJoin(t *testing.T) {
	require := require.New(t)

	db := memory.NewDatabase("test")
	pro := memory.NewDBProvider(db)
	ctx := newContext(pro)

	ltable := memory.NewTable(db.BaseDatabase, "left", lSchema, nil)
	rtable := memory.NewTable(db.BaseDatabase, "right", rSchema, nil)

	insertData(t, newContext(pro), ltable)
	insertData(t, newContext(pro), rtable)

	j := plan.NewLeftOuterJoin(
		plan.NewResolvedTable(ltable, nil, nil),
		plan.NewResolvedTable(rtable, nil, nil),
		expression.NewEquals(
			expression.NewPlus(
				expression.NewGetField(2, types.Text, "lcol3", false),
				expression.NewLiteral(int32(2), types.Int32),
			),
			expression.NewGetField(6, types.Text, "rcol3", false),
		))

	iter, err := DefaultBuilder.Build(ctx, j, nil)
	require.NoError(err)
	rows, err := sql.RowIterToRows(ctx, nil, iter)
	require.NoError(err)
	require.ElementsMatch([]sql.Row{
		{"col1_1", "col2_1", int32(1), int64(2), "col1_2", "col2_2", int32(3), int64(4)},
		{"col1_2", "col2_2", int32(3), int64(4), nil, nil, nil, nil},
	}, rows)
}

type mockReporter struct {
	val uint64
	max uint64
}

func (m mockReporter) UsedMemory() uint64 { return m.val }
func (m mockReporter) MaxMemory() uint64  { return m.max }
