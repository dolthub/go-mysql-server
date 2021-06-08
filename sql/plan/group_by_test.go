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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/expression/function/aggregation"
)

func TestGroupBySchema(t *testing.T) {
	require := require.New(t)

	child := memory.NewTable("test", nil)
	agg := []sql.Expression{
		expression.NewAlias("c1", expression.NewLiteral("s", sql.LongText)),
		expression.NewAlias("c2", aggregation.NewCount(sql.NewEmptyContext(), expression.NewStar())),
	}
	gb := NewGroupBy(agg, nil, NewResolvedTable(child, nil, nil))
	require.Equal(sql.Schema{
		{Name: "c1", Type: sql.LongText},
		{Name: "c2", Type: sql.Int64},
	}, gb.Schema())
}

func TestGroupByResolved(t *testing.T) {
	require := require.New(t)

	child := memory.NewTable("test", nil)
	agg := []sql.Expression{
		expression.NewAlias("c2", aggregation.NewCount(sql.NewEmptyContext(), expression.NewStar())),
	}
	gb := NewGroupBy(agg, nil, NewResolvedTable(child, nil, nil))
	require.True(gb.Resolved())

	agg = []sql.Expression{
		expression.NewStar(),
	}
	gb = NewGroupBy(agg, nil, NewResolvedTable(child, nil, nil))
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
		[]sql.SortField{
			{
				Column: expression.NewGetField(0, sql.LongText, "col1", true),
				Order:  sql.Ascending,
			}, {
				Column: expression.NewGetField(1, sql.Int64, "col2", true),
				Order:  sql.Ascending,
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
			NewResolvedTable(child, nil, nil),
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
			aggregation.NewCount(sql.NewEmptyContext(), expression.NewGetField(0, sql.LongText, "col1", true)),
			expression.NewIsNull(expression.NewGetField(1, sql.Int64, "col2", true)),
		},
		[]sql.Expression{
			aggregation.NewCount(sql.NewEmptyContext(), expression.NewGetField(0, sql.LongText, "col1", true)),
			expression.NewGetField(1, sql.Int64, "col2", true),
		},
		NewResolvedTable(child, nil, nil),
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
				sql.NewEmptyContext(),
				expression.NewGetField(1, sql.Int64, "b", false),
			),
		},
		nil,
		NewResolvedTable(table, nil, nil),
	)

	expected := []sql.Row{{int64(200)}}

	bench := func(node sql.Node, expected []sql.Row) func(*testing.B) {
		return func(b *testing.B) {
			require := require.New(b)

			for i := 0; i < b.N; i++ {
				ctx := sql.NewEmptyContext()
				iter, err := node.RowIter(ctx, nil)
				require.NoError(err)

				rows, err := sql.RowIterToRows(ctx, iter)
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
				sql.NewEmptyContext(),
				expression.NewGetField(1, sql.Int64, "b", false),
			),
		},
		[]sql.Expression{
			expression.NewGetField(0, sql.Int64, "a", false),
		},
		NewResolvedTable(table, nil, nil),
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
