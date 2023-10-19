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
	"testing"

	"github.com/dolthub/vitess/go/vt/proto/query"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/expression/function/aggregation"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestGroupBySchema(t *testing.T) {
	require := require.New(t)

	db := memory.NewDatabase("test")
	child := memory.NewTable(db.BaseDatabase, "test", sql.PrimaryKeySchema{}, nil)
	agg := []sql.Expression{
		expression.NewAlias("c1", expression.NewLiteral("s", types.LongText)),
		expression.NewAlias("c2", aggregation.NewCount(expression.NewStar())),
	}
	gb := plan.NewGroupBy(agg, nil, plan.NewResolvedTable(child, nil, nil))
	require.Equal(sql.Schema{
		{Name: "c1", Type: types.LongText},
		{Name: "c2", Type: types.Int64},
	}, gb.Schema())
}

func TestGroupByResolved(t *testing.T) {
	require := require.New(t)

	db := memory.NewDatabase("test")
	child := memory.NewTable(db.BaseDatabase, "test", sql.PrimaryKeySchema{}, nil)
	agg := []sql.Expression{
		expression.NewAlias("c2", aggregation.NewCount(expression.NewStar())),
	}
	gb := plan.NewGroupBy(agg, nil, plan.NewResolvedTable(child, nil, nil))
	require.True(gb.Resolved())

	agg = []sql.Expression{
		expression.NewStar(),
	}
	gb = plan.NewGroupBy(agg, nil, plan.NewResolvedTable(child, nil, nil))
	require.False(gb.Resolved())
}

func TestGroupByRowIter(t *testing.T) {
	require := require.New(t)

	db := memory.NewDatabase("test")
	pro := memory.NewDBProvider(db)
	ctx := newContext(pro)

	childSchema := sql.Schema{
		{Name: "col1", Type: types.LongText},
		{Name: "col2", Type: types.Int64},
	}
	child := memory.NewTable(db.BaseDatabase, "test", sql.NewPrimaryKeySchema(childSchema), nil)

	rows := []sql.Row{
		sql.NewRow("col1_1", int64(1111)),
		sql.NewRow("col1_1", int64(1111)),
		sql.NewRow("col1_2", int64(4444)),
		sql.NewRow("col1_1", int64(1111)),
		sql.NewRow("col1_2", int64(4444)),
	}

	for _, r := range rows {
		require.NoError(child.Insert(ctx, r))
	}

	p := plan.NewSort(
		[]sql.SortField{
			{
				Column: expression.NewGetField(0, types.LongText, "col1", true),
				Order:  sql.Ascending,
			}, {
				Column: expression.NewGetField(1, types.Int64, "col2", true),
				Order:  sql.Ascending,
			},
		},
		plan.NewGroupBy(
			[]sql.Expression{
				expression.NewGetField(0, types.LongText, "col1", true),
				expression.NewGetField(1, types.Int64, "col2", true),
			},
			[]sql.Expression{
				expression.NewGetField(0, types.LongText, "col1", true),
				expression.NewGetField(1, types.Int64, "col2", true),
			},
			plan.NewResolvedTable(child, nil, nil),
		))

	require.Equal(1, len(p.Children()))

	rows, err := NodeToRows(ctx, p)
	require.NoError(err)
	require.Len(rows, 2)

	require.Equal(sql.NewRow("col1_1", int64(1111)), rows[0])
	require.Equal(sql.NewRow("col1_2", int64(4444)), rows[1])
}

func TestGroupByAggregationGrouping(t *testing.T) {
	require := require.New(t)

	db := memory.NewDatabase("test")
	pro := memory.NewDBProvider(db)
	ctx := newContext(pro)

	childSchema := sql.Schema{
		{Name: "col1", Type: types.LongText},
		{Name: "col2", Type: types.Int64},
	}

	child := memory.NewTable(db.BaseDatabase, "test", sql.NewPrimaryKeySchema(childSchema), nil)

	rows := []sql.Row{
		sql.NewRow("col1_1", int64(1111)),
		sql.NewRow("col1_1", int64(1111)),
		sql.NewRow("col1_2", int64(4444)),
		sql.NewRow("col1_1", int64(1111)),
		sql.NewRow("col1_2", int64(4444)),
	}

	for _, r := range rows {
		require.NoError(child.Insert(ctx, r))
	}

	p := plan.NewGroupBy(
		[]sql.Expression{
			aggregation.NewCount(expression.NewGetField(0, types.LongText, "col1", true)),
			expression.NewIsNull(expression.NewGetField(1, types.Int64, "col2", true)),
		},
		[]sql.Expression{
			expression.NewGetField(0, types.LongText, "col1", true),
			expression.NewIsNull(expression.NewGetField(1, types.Int64, "col2", true)),
		},
		plan.NewResolvedTable(child, nil, nil),
	)

	rows, err := NodeToRows(ctx, p)
	require.NoError(err)

	expected := []sql.Row{
		{int64(3), false},
		{int64(2), false},
	}

	require.Equal(expected, rows)
}

func TestGroupByCollations(t *testing.T) {
	tString := types.MustCreateString(query.Type_VARCHAR, 255, sql.Collation_utf8mb4_0900_ai_ci)
	tEnum := types.MustCreateEnumType([]string{"col1_1", "col1_2"}, sql.Collation_utf8mb4_0900_ai_ci)
	tSet := types.MustCreateSetType([]string{"col1_1", "col1_2"}, sql.Collation_utf8mb4_0900_ai_ci)

	var testCases = []struct {
		Type  sql.Type
		Value func(t *testing.T, v string) any
	}{
		{
			Type:  tString,
			Value: func(t *testing.T, v string) any { return v },
		},
		{
			Type: tEnum,
			Value: func(t *testing.T, v string) any {
				conv, _, err := tEnum.Convert(v)
				require.NoError(t, err)
				return conv
			},
		},
		{
			Type: tSet,
			Value: func(t *testing.T, v string) any {
				conv, _, err := tSet.Convert(v)
				require.NoError(t, err)
				return conv
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.Type.String(), func(t *testing.T) {
			require := require.New(t)

			childSchema := sql.Schema{
				{Name: "col1", Type: tc.Type},
				{Name: "col2", Type: types.Int64},
			}

			db := memory.NewDatabase("test")
			pro := memory.NewDBProvider(db)
			ctx := newContext(pro)

			child := memory.NewTable(db.BaseDatabase, "test", sql.NewPrimaryKeySchema(childSchema), nil)

			rows := []sql.Row{
				sql.NewRow(tc.Value(t, "col1_1"), int64(1111)),
				sql.NewRow(tc.Value(t, "Col1_1"), int64(1111)),
				sql.NewRow(tc.Value(t, "col1_2"), int64(4444)),
				sql.NewRow(tc.Value(t, "col1_1"), int64(1111)),
				sql.NewRow(tc.Value(t, "Col1_2"), int64(4444)),
			}

			for _, r := range rows {
				require.NoError(child.Insert(ctx, r))
			}

			p := plan.NewGroupBy(
				[]sql.Expression{
					aggregation.NewSum(
						expression.NewGetFieldWithTable(1, types.Int64, "db", "test", "col2", false),
					),
				},
				[]sql.Expression{
					expression.NewGetFieldWithTable(0, tc.Type, "db", "test", "col1", false),
				},
				plan.NewResolvedTable(child, nil, nil),
			)

			rows, err := NodeToRows(ctx, p)
			require.NoError(err)

			expected := []sql.Row{
				{float64(3333)},
				{float64(8888)},
			}

			require.Equal(expected, rows)
		})
	}
}

func BenchmarkGroupBy(b *testing.B) {
	table := benchmarkTable(b)

	node := plan.NewGroupBy(
		[]sql.Expression{
			aggregation.NewMax(
				expression.NewGetField(1, types.Int64, "b", false),
			),
		},
		nil,
		plan.NewResolvedTable(table, nil, nil),
	)

	expected := []sql.Row{{int64(200)}}

	bench := func(node sql.Node, expected []sql.Row) func(*testing.B) {
		return func(b *testing.B) {
			require := require.New(b)

			for i := 0; i < b.N; i++ {
				ctx := sql.NewEmptyContext()
				iter, err := DefaultBuilder.Build(ctx, node, nil)
				require.NoError(err)

				rows, err := sql.RowIterToRows(ctx, nil, iter)
				require.NoError(err)
				require.ElementsMatch(expected, rows)
			}
		}
	}

	b.Run("no grouping", bench(node, expected))

	node = plan.NewGroupBy(
		[]sql.Expression{
			expression.NewGetField(0, types.Int64, "a", false),
			aggregation.NewMax(
				expression.NewGetField(1, types.Int64, "b", false),
			),
		},
		[]sql.Expression{
			expression.NewGetField(0, types.Int64, "a", false),
		},
		plan.NewResolvedTable(table, nil, nil),
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

	db := memory.NewDatabase("test")
	table := memory.NewTable(db.BaseDatabase, "test", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "a", Type: types.Int64},
		{Name: "b", Type: types.Int64},
	}), nil)

	for i := int64(0); i < 50; i++ {
		for j := int64(200); j > 0; j-- {
			row := sql.NewRow(i, j)
			require.NoError(table.Insert(sql.NewEmptyContext(), row))
		}
	}

	return table
}

// NodeToRows converts a node to a slice of rows.
func NodeToRows(ctx *sql.Context, n sql.Node) ([]sql.Row, error) {
	// TODO can't have sql depend on rowexec
	// move execution tests to rowexec
	i, err := DefaultBuilder.Build(ctx, n, nil)
	if err != nil {
		return nil, err
	}

	return sql.RowIterToRows(ctx, nil, i)
}
