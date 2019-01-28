package plan

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/mem"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
)

func TestInnerJoin(t *testing.T) {
	require := require.New(t)
	finalSchema := append(lSchema, rSchema...)

	ltable := mem.NewTable("left", lSchema)
	rtable := mem.NewTable("right", rSchema)
	insertData(t, ltable)
	insertData(t, rtable)

	j := NewInnerJoin(
		NewResolvedTable(ltable),
		NewResolvedTable(rtable),
		expression.NewEquals(
			expression.NewGetField(0, sql.Text, "lcol1", false),
			expression.NewGetField(4, sql.Text, "rcol1", false),
		))

	require.Equal(finalSchema, j.Schema())

	rows := collectRows(t, j)
	require.Len(rows, 2)

	require.Equal([]sql.Row{
		{"col1_1", "col2_1", int32(1111), int64(2222), "col1_1", "col2_1", int32(1111), int64(2222)},
		{"col1_2", "col2_2", int32(3333), int64(4444), "col1_2", "col2_2", int32(3333), int64(4444)},
	}, rows)
}

func TestInMemoryInnerJoin(t *testing.T) {
	require := require.New(t)
	finalSchema := append(lSchema, rSchema...)

	ltable := mem.NewTable("left", lSchema)
	rtable := mem.NewTable("right", rSchema)
	insertData(t, ltable)
	insertData(t, rtable)

	j := NewInnerJoin(
		NewResolvedTable(ltable),
		NewResolvedTable(rtable),
		expression.NewEquals(
			expression.NewGetField(0, sql.Text, "lcol1", false),
			expression.NewGetField(4, sql.Text, "rcol1", false),
		))

	require.Equal(finalSchema, j.Schema())

	ctx := sql.NewEmptyContext()
	ctx.Set(inMemoryJoinSessionVar, sql.Text, "true")

	iter, err := j.RowIter(ctx)
	require.NoError(err)

	rows, err := sql.RowIterToRows(iter)
	require.NoError(err)
	require.Len(rows, 2)

	require.Equal([]sql.Row{
		{"col1_1", "col2_1", int32(1111), int64(2222), "col1_1", "col2_1", int32(1111), int64(2222)},
		{"col1_2", "col2_2", int32(3333), int64(4444), "col1_2", "col2_2", int32(3333), int64(4444)},
	}, rows)
}

func TestInnerJoinEmpty(t *testing.T) {
	require := require.New(t)
	ctx := sql.NewEmptyContext()

	ltable := mem.NewTable("left", lSchema)
	rtable := mem.NewTable("right", rSchema)

	j := NewInnerJoin(
		NewResolvedTable(ltable),
		NewResolvedTable(rtable),
		expression.NewEquals(
			expression.NewGetField(0, sql.Text, "lcol1", false),
			expression.NewGetField(4, sql.Text, "rcol1", false),
		))

	iter, err := j.RowIter(ctx)
	require.NoError(err)

	assertRows(t, iter, 0)
}

func BenchmarkInnerJoin(b *testing.B) {
	t1 := mem.NewTable("foo", sql.Schema{
		{Name: "a", Source: "foo", Type: sql.Int64},
		{Name: "b", Source: "foo", Type: sql.Text},
	})

	t2 := mem.NewTable("bar", sql.Schema{
		{Name: "a", Source: "bar", Type: sql.Int64},
		{Name: "b", Source: "bar", Type: sql.Text},
	})

	for i := 0; i < 5; i++ {
		t1.Insert(sql.NewEmptyContext(), sql.NewRow(int64(i), fmt.Sprintf("t1_%d", i)))
		t2.Insert(sql.NewEmptyContext(), sql.NewRow(int64(i), fmt.Sprintf("t2_%d", i)))
	}

	n1 := NewInnerJoin(
		NewResolvedTable(t1),
		NewResolvedTable(t2),
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
			NewResolvedTable(t1),
			NewResolvedTable(t2),
		),
	)

	expected := []sql.Row{
		{int64(0), "t1_0", int64(0), "t2_0"},
		{int64(1), "t1_1", int64(1), "t2_1"},
		{int64(2), "t1_2", int64(2), "t2_2"},
		{int64(3), "t1_3", int64(3), "t2_3"},
		{int64(4), "t1_4", int64(4), "t2_4"},
	}

	ctx := sql.NewEmptyContext()
	b.Run("inner join", func(b *testing.B) {
		require := require.New(b)

		for i := 0; i < b.N; i++ {
			iter, err := n1.RowIter(ctx)
			require.NoError(err)

			rows, err := sql.RowIterToRows(iter)
			require.NoError(err)

			require.Equal(expected, rows)
		}
	})

	b.Run("in memory inner join", func(b *testing.B) {
		useInMemoryJoins = true
		require := require.New(b)

		for i := 0; i < b.N; i++ {
			iter, err := n1.RowIter(ctx)
			require.NoError(err)

			rows, err := sql.RowIterToRows(iter)
			require.NoError(err)

			require.Equal(expected, rows)
		}

		useInMemoryJoins = false
	})

	b.Run("cross join with filter", func(b *testing.B) {
		require := require.New(b)

		for i := 0; i < b.N; i++ {
			iter, err := n2.RowIter(ctx)
			require.NoError(err)

			rows, err := sql.RowIterToRows(iter)
			require.NoError(err)

			require.Equal(expected, rows)
		}
	})
}
