package plan

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/liquidata-inc/go-mysql-server/memory"
	"github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/liquidata-inc/go-mysql-server/sql/expression"
	"github.com/liquidata-inc/go-mysql-server/sql/expression/function/aggregation"
)

func TestGroupBySchema(t *testing.T) {
	require := require.New(t)

	child := memory.NewTable("test", nil)
	agg := []sql.Expression{
		expression.NewAlias(expression.NewLiteral("s", sql.LongText), "c1"),
		expression.NewAlias(aggregation.NewCount(expression.NewStar()), "c2"),
	}
	gb := NewGroupBy(agg, nil, NewResolvedTable(child))
	require.Equal(sql.Schema{
		{Name: "c1", Type: sql.LongText},
		{Name: "c2", Type: sql.Int64},
	}, gb.Schema())
}

func TestGroupByResolved(t *testing.T) {
	require := require.New(t)

	child := memory.NewTable("test", nil)
	agg := []sql.Expression{
		expression.NewAlias(aggregation.NewCount(expression.NewStar()), "c2"),
	}
	gb := NewGroupBy(agg, nil, NewResolvedTable(child))
	require.True(gb.Resolved())

	agg = []sql.Expression{
		expression.NewStar(),
	}
	gb = NewGroupBy(agg, nil, NewResolvedTable(child))
	require.False(gb.Resolved())
}

func TestGroupByRowIter(t *testing.T) {
	require := require.New(t)
	ctx := sql.NewEmptyContext()

	childSchema := sql.Schema{
		{Name: "col1", Type: sql.LongText},
		{Name: "col2", Type: sql.Int64},
	}
	child := memory.NewTable("test", childSchema)

	rows := []sql.Row{
		sql.NewRow("col1_1", int64(1111)),
		sql.NewRow("col1_1", int64(1111)),
		sql.NewRow("col1_2", int64(4444)),
		sql.NewRow("col1_1", int64(1111)),
		sql.NewRow("col1_2", int64(4444)),
	}

	for _, r := range rows {
		require.NoError(child.Insert(sql.NewEmptyContext(), r))
	}

	p := NewSort(
		[]SortField{
			{
				Column: expression.NewGetField(0, sql.LongText, "col1", true),
				Order:  Ascending,
			}, {
				Column: expression.NewGetField(1, sql.Int64, "col2", true),
				Order:  Ascending,
			},
		},
		NewGroupBy(
			[]sql.Expression{
				expression.NewGetField(0, sql.LongText, "col1", true),
				expression.NewGetField(1, sql.Int64, "col2", true),
			},
			[]sql.Expression{
				expression.NewGetField(0, sql.LongText, "col1", true),
				expression.NewGetField(1, sql.Int64, "col2", true),
			},
			NewResolvedTable(child),
		))

	require.Equal(1, len(p.Children()))

	rows, err := sql.NodeToRows(ctx, p)
	require.NoError(err)
	require.Len(rows, 2)

	require.Equal(sql.NewRow("col1_1", int64(1111)), rows[0])
	require.Equal(sql.NewRow("col1_2", int64(4444)), rows[1])
}

func TestGroupByEvalEmptyBuffer(t *testing.T) {
	require := require.New(t)
	ctx := sql.NewEmptyContext()

	r, err := evalBuffer(ctx, expression.NewGetField(0, sql.LongText, "col1", true), sql.Row{})
	require.NoError(err)
	require.Nil(r)
}

func TestGroupByAggregationGrouping(t *testing.T) {
	require := require.New(t)
	ctx := sql.NewEmptyContext()

	childSchema := sql.Schema{
		{Name: "col1", Type: sql.LongText},
		{Name: "col2", Type: sql.Int64},
	}

	child := memory.NewTable("test", childSchema)

	rows := []sql.Row{
		sql.NewRow("col1_1", int64(1111)),
		sql.NewRow("col1_1", int64(1111)),
		sql.NewRow("col1_2", int64(4444)),
		sql.NewRow("col1_1", int64(1111)),
		sql.NewRow("col1_2", int64(4444)),
	}

	for _, r := range rows {
		require.NoError(child.Insert(sql.NewEmptyContext(), r))
	}

	p := NewGroupBy(
		[]sql.Expression{
			aggregation.NewCount(expression.NewGetField(0, sql.LongText, "col1", true)),
			expression.NewIsNull(expression.NewGetField(1, sql.Int64, "col2", true)),
		},
		[]sql.Expression{
			aggregation.NewCount(expression.NewGetField(0, sql.LongText, "col1", true)),
			expression.NewGetField(1, sql.Int64, "col2", true),
		},
		NewResolvedTable(child),
	)

	rows, err := sql.NodeToRows(ctx, p)
	require.NoError(err)

	expected := []sql.Row{
		{int64(3), false},
		{int64(2), false},
	}

	require.Equal(expected, rows)
}

func BenchmarkGroupBy(b *testing.B) {
	table := benchmarkTable(b)

	node := NewGroupBy(
		[]sql.Expression{
			aggregation.NewMax(
				expression.NewGetField(1, sql.Int64, "b", false),
			),
		},
		nil,
		NewResolvedTable(table),
	)

	expected := []sql.Row{{int64(200)}}

	bench := func(node sql.Node, expected []sql.Row) func(*testing.B) {
		return func(b *testing.B) {
			require := require.New(b)

			for i := 0; i < b.N; i++ {
				iter, err := node.RowIter(sql.NewEmptyContext())
				require.NoError(err)

				rows, err := sql.RowIterToRows(iter)
				require.NoError(err)
				require.ElementsMatch(expected, rows)
			}
		}
	}

	b.Run("no grouping", bench(node, expected))

	node = NewGroupBy(
		[]sql.Expression{
			expression.NewGetField(0, sql.Int64, "a", false),
			aggregation.NewMax(
				expression.NewGetField(1, sql.Int64, "b", false),
			),
		},
		[]sql.Expression{
			expression.NewGetField(0, sql.Int64, "a", false),
		},
		NewResolvedTable(table),
	)

	expected = []sql.Row{}
	for i := int64(0); i < 50; i++ {
		expected = append(expected, sql.NewRow(i, int64(200)))
	}

	b.Run("grouping", bench(node, expected))
}

func benchmarkTable(t testing.TB) sql.Table {
	t.Helper()
	require := require.New(t)

	table := memory.NewTable("test", sql.Schema{
		{Name: "a", Type: sql.Int64},
		{Name: "b", Type: sql.Int64},
	})

	for i := int64(0); i < 50; i++ {
		for j := int64(200); j > 0; j-- {
			row := sql.NewRow(i, j)
			require.NoError(table.Insert(sql.NewEmptyContext(), row))
		}
	}

	return table
}
