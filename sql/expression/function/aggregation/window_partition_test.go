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
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

func TestWindowPartitionIter(t *testing.T) {
	tests := []struct {
		Name     string
		Iter     *WindowPartitionIter
		Expected []sql.Row
	}{
		{
			Name: "group by",
			Iter: NewWindowPartitionIter(
				&WindowPartition{
					PartitionBy: partitionByX,
					SortBy:      sortByW,
					Aggs: []*Aggregation{
						NewAggregation(lastX, NewGroupByFramer()),
						NewAggregation(firstY, NewGroupByFramer()),
						NewAggregation(sumZ, NewGroupByFramer()),
					},
				},
			),
			Expected: []sql.Row{
				{"forest", "leaf", float64(27)},
				{"desert", "sand", float64(23)},
			},
		},
		{
			Name: "partition level desc",
			Iter: NewWindowPartitionIter(
				&WindowPartition{
					PartitionBy: partitionByX,
					SortBy:      sortByWDesc,
					Aggs: []*Aggregation{
						NewAggregation(lastX, NewPartitionFramer()),
						NewAggregation(firstY, NewPartitionFramer()),
						NewAggregation(sumZ, NewPartitionFramer()),
					},
				}),
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
			tt.Iter.child = mustNewRowIter(t, ctx)
			res, err := sql.RowIterToRows(ctx, nil, tt.Iter)
			require.NoError(t, err)
			require.Equal(t, tt.Expected, res)
		})
	}
}

func mustNewRowIter(t *testing.T, ctx *sql.Context) sql.RowIter {
	childSchema := sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "w", Type: sql.Int64, Nullable: true},
		{Name: "x", Type: sql.Text, Nullable: true},
		{Name: "y", Type: sql.Text, Nullable: true},
		{Name: "z", Type: sql.Int32, Nullable: true},
	})
	table := memory.NewTable("test", childSchema, nil)

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
		require.NoError(t, table.Insert(sql.NewEmptyContext(), r))
	}

	pIter, err := table.Partitions(ctx)
	require.NoError(t, err)

	p, err := pIter.Next(ctx)
	require.NoError(t, err)

	child, err := table.PartitionRows(ctx, p)
	require.NoError(t, err)
	return child
}

func TestWindowPartition_MaterializeInput(t *testing.T) {
	ctx := sql.NewEmptyContext()
	i := WindowPartitionIter{
		w: &WindowPartition{
			PartitionBy: partitionByX,
			SortBy:      sortByW,
		},
		child: mustNewRowIter(t, ctx),
	}

	buf, ordering, err := i.materializeInput(ctx)
	require.NoError(t, err)
	expBuf := []sql.Row{
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
	require.ElementsMatch(t, expBuf, buf)
	expOrd := []int{0, 1, 2, 3, 4, 5, 6, 7, 8}
	require.ElementsMatch(t, expOrd, ordering)
}

func TestWindowPartition_InitializePartitions(t *testing.T) {
	ctx := sql.NewEmptyContext()
	i := NewWindowPartitionIter(
		&WindowPartition{
			PartitionBy: partitionByX,
		})
	i.input = []sql.Row{
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
	partitions, err := i.initializePartitions(ctx)
	require.NoError(t, err)
	expPartitions := []sql.WindowInterval{
		{Start: 0, End: 5},
		{Start: 5, End: 9},
	}
	require.ElementsMatch(t, expPartitions, partitions)
}

func TestWindowPartition_MaterializeOutput(t *testing.T) {
	t.Run("non nil input", func(t *testing.T) {
		ctx := sql.NewEmptyContext()
		i := NewWindowPartitionIter(
			&WindowPartition{
				PartitionBy: partitionByX,
				Aggs: []*Aggregation{
					NewAggregation(sumZ, NewGroupByFramer()),
				},
			})
		i.input = []sql.Row{
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
		i.partitions = []sql.WindowInterval{{0, 5}, {5, 9}}
		i.outputOrdering = []int{0, 1, 2, 3, 4, 5, 6, 7, 8}
		output, err := i.materializeOutput(ctx)
		require.NoError(t, err)
		expOutput := []sql.Row{
			{float64(27), 0},
			{float64(23), 5},
		}
		require.ElementsMatch(t, expOutput, output)
	})

	t.Run("nil input with partition by", func(t *testing.T) {
		ctx := sql.NewEmptyContext()
		i := NewWindowPartitionIter(
			&WindowPartition{
				PartitionBy: partitionByX,
				Aggs: []*Aggregation{
					NewAggregation(sumZ, NewGroupByFramer()),
				},
			})
		i.input = []sql.Row{}
		i.partitions = []sql.WindowInterval{{0, 0}}
		i.outputOrdering = []int{}
		output, err := i.materializeOutput(ctx)
		require.Equal(t, io.EOF, err)
		require.ElementsMatch(t, nil, output)
	})

	t.Run("nil input no partition by", func(t *testing.T) {
		ctx := sql.NewEmptyContext()
		i := NewWindowPartitionIter(
			&WindowPartition{
				PartitionBy: nil,
				Aggs: []*Aggregation{
					NewAggregation(NewCountAgg(expression.NewGetField(0, sql.Int64, "z", true)), NewGroupByFramer()),
				},
			})
		i.input = []sql.Row{}
		i.partitions = []sql.WindowInterval{{0, 0}}
		i.outputOrdering = nil
		output, err := i.materializeOutput(ctx)
		require.NoError(t, err)
		expOutput := []sql.Row{{int64(0), nil}}
		require.ElementsMatch(t, expOutput, output)
	})
}

func TestWindowPartition_SortAndFilterOutput(t *testing.T) {
	tests := []struct {
		Name     string
		Output   []sql.Row
		Expected []sql.Row
	}{
		{
			Name: "no input rows filtered before output, contiguous output indexes",
			Output: []sql.Row{
				{0, 0},
				{3, 3},
				{2, 2},
				{1, 1},
			},
			Expected: []sql.Row{
				{0},
				{1},
				{2},
				{3},
			},
		},
		{
			Name: "input rows filtered before output, non contiguous output indexes",
			Output: []sql.Row{
				{0, 0},
				{3, 3},
				{2, 7},
				{1, 1},
			},
			Expected: []sql.Row{
				{0},
				{1},
				{3},
				{2},
			},
		},
		{
			Name:     "empty output",
			Output:   []sql.Row{},
			Expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			i := NewWindowPartitionIter(&WindowPartition{})
			i.output = tt.Output
			err := i.sortAndFilterOutput()
			require.NoError(t, err)
			require.ElementsMatch(t, tt.Expected, i.output)
		})
	}
}
