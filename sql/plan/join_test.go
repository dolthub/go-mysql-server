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
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

func TestJoinSchema(t *testing.T) {
	t1 := NewResolvedTable(memory.NewTable("foo", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "a", Source: "foo", Type: sql.Int64},
	}), nil), nil, nil)

	t2 := NewResolvedTable(memory.NewTable("bar", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "b", Source: "bar", Type: sql.Int64},
	}), nil), nil, nil)

	t.Run("inner", func(t *testing.T) {
		j := NewInnerJoin(t1, t2, nil)
		result := j.Schema()

		require.Equal(t, sql.Schema{
			{Name: "a", Source: "foo", Type: sql.Int64},
			{Name: "b", Source: "bar", Type: sql.Int64},
		}, result)
	})

	t.Run("left", func(t *testing.T) {
		j := NewLeftJoin(t1, t2, nil)
		result := j.Schema()

		require.Equal(t, sql.Schema{
			{Name: "a", Source: "foo", Type: sql.Int64},
			{Name: "b", Source: "bar", Type: sql.Int64, Nullable: true},
		}, result)
	})

	t.Run("right", func(t *testing.T) {
		j := NewRightJoin(t1, t2, nil)
		result := j.Schema()

		require.Equal(t, sql.Schema{
			{Name: "a", Source: "foo", Type: sql.Int64, Nullable: true},
			{Name: "b", Source: "bar", Type: sql.Int64},
		}, result)
	})
}

func TestInnerJoin(t *testing.T) {
	testInnerJoin(t, sql.NewEmptyContext())
}

func TestInMemoryInnerJoin(t *testing.T) {
	ctx := sql.NewEmptyContext()
	err := ctx.SetSessionVariable(ctx, inMemoryJoinSessionVar, true)
	require.NoError(t, err)
	testInnerJoin(t, ctx)
}

func TestMultiPassInnerJoin(t *testing.T) {
	ctx := sql.NewContext(context.TODO(), sql.WithMemoryManager(
		sql.NewMemoryManager(mockReporter{2, 1}),
	))
	testInnerJoin(t, ctx)
}

func testInnerJoin(t *testing.T, ctx *sql.Context) {
	t.Helper()

	require := require.New(t)
	ltable := memory.NewTable("left", lSchema, nil)
	rtable := memory.NewTable("right", rSchema, nil)
	insertData(t, ltable)
	insertData(t, rtable)

	j := NewInnerJoin(
		NewResolvedTable(ltable, nil, nil),
		NewResolvedTable(rtable, nil, nil),
		expression.NewEquals(
			expression.NewGetField(0, sql.Text, "lcol1", false),
			expression.NewGetField(4, sql.Text, "rcol1", false),
		))

	rows := collectRows(t, j)
	require.Len(rows, 2)

	require.Equal([]sql.Row{
		{"col1_1", "col2_1", int32(1), int64(2), "col1_1", "col2_1", int32(1), int64(2)},
		{"col1_2", "col2_2", int32(3), int64(4), "col1_2", "col2_2", int32(3), int64(4)},
	}, rows)
}
func TestInnerJoinEmpty(t *testing.T) {
	require := require.New(t)
	ctx := sql.NewEmptyContext()

	ltable := memory.NewTable("left", lSchema, nil)
	rtable := memory.NewTable("right", rSchema, nil)

	j := NewInnerJoin(
		NewResolvedTable(ltable, nil, nil),
		NewResolvedTable(rtable, nil, nil),
		expression.NewEquals(
			expression.NewGetField(0, sql.Text, "lcol1", false),
			expression.NewGetField(4, sql.Text, "rcol1", false),
		))

	iter, err := j.RowIter(ctx, nil)
	require.NoError(err)

	assertRows(t, ctx, iter, 0)
}

func BenchmarkInnerJoin(b *testing.B) {
	t1 := memory.NewTable("foo", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "a", Source: "foo", Type: sql.Int64},
		{Name: "b", Source: "foo", Type: sql.Text},
	}), nil)

	t2 := memory.NewTable("bar", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "a", Source: "bar", Type: sql.Int64},
		{Name: "b", Source: "bar", Type: sql.Text},
	}), nil)

	for i := 0; i < 5; i++ {
		t1.Insert(sql.NewEmptyContext(), sql.NewRow(int64(i), fmt.Sprintf("t1_%d", i)))
		t2.Insert(sql.NewEmptyContext(), sql.NewRow(int64(i), fmt.Sprintf("t2_%d", i)))
	}

	n1 := NewInnerJoin(
		NewResolvedTable(t1, nil, nil),
		NewResolvedTable(t2, nil, nil),
		expression.NewEquals(
			expression.NewGetField(0, sql.Int64, "a", false),
			expression.NewGetField(2, sql.Int64, "a", false),
		),
	)

	n2 := NewFilter(
		expression.NewEquals(
			expression.NewGetField(0, sql.Int64, "a", false),
			expression.NewGetField(2, sql.Int64, "a", false),
		),
		NewCrossJoin(
			NewResolvedTable(t1, nil, nil),
			NewResolvedTable(t2, nil, nil),
		),
	)

	expected := []sql.Row{
		{int64(0), "t1_0", int64(0), "t2_0"},
		{int64(1), "t1_1", int64(1), "t2_1"},
		{int64(2), "t1_2", int64(2), "t2_2"},
		{int64(3), "t1_3", int64(3), "t2_3"},
		{int64(4), "t1_4", int64(4), "t2_4"},
	}

	ctx := sql.NewContext(context.TODO(), sql.WithMemoryManager(
		sql.NewMemoryManager(mockReporter{1, 5}),
	))
	b.Run("inner join", func(b *testing.B) {
		require := require.New(b)

		for i := 0; i < b.N; i++ {
			iter, err := n1.RowIter(ctx, nil)
			require.NoError(err)

			rows, err := sql.RowIterToRows(ctx, nil, iter)
			require.NoError(err)

			require.Equal(expected, rows)
		}
	})

	b.Run("in memory inner join", func(b *testing.B) {
		useInMemoryJoins = true
		require := require.New(b)

		for i := 0; i < b.N; i++ {
			iter, err := n1.RowIter(ctx, nil)
			require.NoError(err)

			rows, err := sql.RowIterToRows(ctx, nil, iter)
			require.NoError(err)

			require.Equal(expected, rows)
		}

		useInMemoryJoins = false
	})

	b.Run("within memory threshold", func(b *testing.B) {
		require := require.New(b)

		for i := 0; i < b.N; i++ {
			iter, err := n1.RowIter(ctx, nil)
			require.NoError(err)

			rows, err := sql.RowIterToRows(ctx, nil, iter)
			require.NoError(err)

			require.Equal(expected, rows)
		}
	})

	b.Run("cross join with filter", func(b *testing.B) {
		require := require.New(b)

		for i := 0; i < b.N; i++ {
			iter, err := n2.RowIter(ctx, nil)
			require.NoError(err)

			rows, err := sql.RowIterToRows(ctx, nil, iter)
			require.NoError(err)

			require.Equal(expected, rows)
		}
	})
}

func TestLeftJoin(t *testing.T) {
	require := require.New(t)

	ltable := memory.NewTable("left", lSchema, nil)
	rtable := memory.NewTable("right", rSchema, nil)
	insertData(t, ltable)
	insertData(t, rtable)

	j := NewLeftJoin(
		NewResolvedTable(ltable, nil, nil),
		NewResolvedTable(rtable, nil, nil),
		expression.NewEquals(
			expression.NewPlus(
				expression.NewGetField(2, sql.Text, "lcol3", false),
				expression.NewLiteral(int32(2), sql.Int32),
			),
			expression.NewGetField(6, sql.Text, "rcol3", false),
		))

	ctx := sql.NewEmptyContext()
	iter, err := j.RowIter(ctx, nil)
	require.NoError(err)
	rows, err := sql.RowIterToRows(ctx, nil, iter)
	require.NoError(err)
	require.ElementsMatch([]sql.Row{
		{"col1_1", "col2_1", int32(1), int64(2), "col1_2", "col2_2", int32(3), int64(4)},
		{"col1_2", "col2_2", int32(3), int64(4), nil, nil, nil, nil},
	}, rows)
}

func TestRightJoin(t *testing.T) {
	require := require.New(t)

	ltable := memory.NewTable("left", lSchema, nil)
	rtable := memory.NewTable("right", rSchema, nil)
	insertData(t, ltable)
	insertData(t, rtable)

	j := NewRightJoin(
		NewResolvedTable(ltable, nil, nil),
		NewResolvedTable(rtable, nil, nil),
		expression.NewEquals(
			expression.NewPlus(
				expression.NewGetField(2, sql.Text, "lcol3", false),
				expression.NewLiteral(int32(2), sql.Int32),
			),
			expression.NewGetField(6, sql.Text, "rcol3", false),
		))

	ctx := sql.NewEmptyContext()
	iter, err := j.RowIter(ctx, nil)
	require.NoError(err)
	rows, err := sql.RowIterToRows(ctx, nil, iter)
	require.NoError(err)
	require.ElementsMatch([]sql.Row{
		{nil, nil, nil, nil, "col1_1", "col2_1", int32(1), int64(2)},
		{"col1_1", "col2_1", int32(1), int64(2), "col1_2", "col2_2", int32(3), int64(4)},
	}, rows)
}

type mockReporter struct {
	val uint64
	max uint64
}

func (m mockReporter) UsedMemory() uint64 { return m.val }
func (m mockReporter) MaxMemory() uint64  { return m.max }
