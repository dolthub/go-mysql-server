// Copyright 2022 DoltHub, Inc.
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

package aggregation

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

func TestWindowBlockIter(t *testing.T) {

	tests := []struct {
		Name     string
		Iter     windowBlockIter
		Expected []sql.Row
	}{
		{
			Name: "group by",
			Iter: windowBlockIter{
				partitionBy: []sql.Expression{
					expression.NewGetFieldWithTable(1, sql.Text, "a", "x", false),
				},
				sortBy: sql.SortFields{
					{
						Column: expression.NewGetFieldWithTable(0, sql.Int64, "a", "w", false),
					},
				},
				aggs: []*Aggregation{
					NewAggregation(
						NewLastAgg(expression.NewGetField(1, sql.Text, "x", true)),
						NewGroupByFramer(),
					),
					NewAggregation(
						NewFirstAgg(expression.NewGetField(2, sql.Text, "y", true)),
						NewGroupByFramer(),
					),
					NewAggregation(
						NewSumAgg(expression.NewGetField(3, sql.Int64, "z", true)),
						NewGroupByFramer(),
					),
				},
				partitionIdx: -1,
			},
			Expected: []sql.Row{
				{"forest", "leaf", float64(27)},
				{"desert", "sand", float64(23)},
			},
		},
		{
			Name: "partition level desc",
			Iter: windowBlockIter{
				partitionBy: []sql.Expression{
					expression.NewGetFieldWithTable(1, sql.Text, "a", "x", false),
				},
				sortBy: sql.SortFields{
					{
						Column: expression.NewGetFieldWithTable(0, sql.Int64, "a", "w", false),
						Order:  sql.Descending,
					},
				},
				aggs: []*Aggregation{
					NewAggregation(
						NewLastAgg(expression.NewGetField(1, sql.Text, "x", true)),
						NewPartitionFramer(),
					),
					NewAggregation(
						NewFirstAgg(expression.NewGetField(2, sql.Text, "y", true)),
						NewPartitionFramer(),
					),
					NewAggregation(
						NewSumAgg(expression.NewGetField(3, sql.Int64, "z", true)),
						NewPartitionFramer(),
					),
				},
				partitionIdx: -1,
			},
			Expected: []sql.Row{
				{"forest", "wildflower", float64(27)},
				{"forest", "wildflower", float64(27)},
				{"forest", "wildflower", float64(27)},
				{"forest", "wildflower", float64(27)},
				{"forest", "wildflower", float64(27)},
				{"desert", "mummy", float64(23)},
				{"desert", "mummy", float64(23)},
				{"desert", "mummy", float64(23)},
				{"desert", "mummy", float64(23)},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			ctx := sql.NewEmptyContext()
			tt.Iter.child = newRowIter(t, ctx)
			res, err := sql.RowIterToRows(ctx, &tt.Iter)
			require.NoError(t, err)
			require.Equal(t, tt.Expected, res)
		})
	}
}

func newRowIter(t *testing.T, ctx *sql.Context) sql.RowIter {
	childSchema := sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "w", Type: sql.Int64, Nullable: true},
		{Name: "x", Type: sql.Text, Nullable: true},
		{Name: "y", Type: sql.Text, Nullable: true},
		{Name: "z", Type: sql.Int32, Nullable: true},
	})
	table := memory.NewTable("test", childSchema)

	rows := []sql.Row{
		{int64(1), "forest", "leaf", 4},
		{int64(2), "forest", "bark", 4},
		{int64(3), "forest", "canopy", 6},
		{int64(4), "forest", "bug", 3},
		{int64(5), "forest", "wildflower", 10},
		{int64(6), "desert", "sand", 4},
		{int64(7), "desert", "cactus", 6},
		{int64(8), "desert", "scorpion", 8},
		{int64(9), "desert", "mummy", 5},
	}

	for _, r := range rows {
		assert.NoError(t, table.Insert(sql.NewEmptyContext(), r))
	}

	pIter, err := table.Partitions(ctx)
	require.NoError(t, err)

	p, err := pIter.Next(ctx)
	require.NoError(t, err)

	child, err := table.PartitionRows(ctx, p)
	require.NoError(t, err)
	return child
}
