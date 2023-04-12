// Copyright 2021-2022 Dolthub, Inc.
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
	"github.com/dolthub/go-mysql-server/sql/plan"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/expression/function/aggregation"
	"github.com/dolthub/go-mysql-server/sql/expression/function/aggregation/window"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestWindowPlanToIter(t *testing.T) {
	n1, err := window.NewRowNumber().(sql.WindowAggregation).WithWindow(
		&sql.WindowDefinition{
			PartitionBy: []sql.Expression{
				expression.NewGetField(2, types.Int64, "c", false)},
			OrderBy: nil,
		})
	require.NoError(t, err)

	n2, err := aggregation.NewMax(
		expression.NewGetField(0, types.Int64, "a", false),
	).WithWindow(
		&sql.WindowDefinition{
			PartitionBy: []sql.Expression{
				expression.NewGetField(1, types.Int64, "b", false)},
			OrderBy: nil,
		})
	require.NoError(t, err)
	n3 := expression.NewGetField(0, types.Int64, "a", false)
	n4, err := aggregation.NewMin(
		expression.NewGetField(0, types.Int64, "a", false),
	).WithWindow(
		&sql.WindowDefinition{
			PartitionBy: []sql.Expression{
				expression.NewGetField(1, types.Int64, "b", false)},
			OrderBy: nil,
		})
	require.NoError(t, err)

	fn1, err := n1.NewWindowFunction()
	require.NoError(t, err)
	fn2, err := n2.NewWindowFunction()
	require.NoError(t, err)
	fn3, err := aggregation.NewLast(n3).NewWindowFunction()
	fn4, err := n4.NewWindowFunction()
	require.NoError(t, err)

	agg1 := aggregation.NewAggregation(fn1, fn1.DefaultFramer())
	agg2 := aggregation.NewAggregation(fn2, fn2.DefaultFramer())
	agg3 := aggregation.NewAggregation(fn3, fn3.DefaultFramer())
	agg4 := aggregation.NewAggregation(fn4, fn4.DefaultFramer())

	window := plan.NewWindow([]sql.Expression{n1, n2, n3, n4}, nil)
	outputIters, outputOrdinals, err := windowToIter(window)
	require.NoError(t, err)

	require.Equal(t, len(outputIters), 3)
	require.Equal(t, len(outputOrdinals), 3)
	accOrdinals := make([]int, 0)
	for _, p := range outputOrdinals {
		for _, v := range p {
			accOrdinals = append(accOrdinals, v)
		}
	}
	require.ElementsMatch(t, accOrdinals, []int{0, 1, 2, 3})

	// check aggs
	allOutputAggs := make([]*aggregation.Aggregation, 0)
	for _, i := range outputIters {
		allOutputAggs = append(allOutputAggs, i.WindowBlock().Aggs...)
	}
	require.ElementsMatch(t, allOutputAggs, []*aggregation.Aggregation{agg1, agg2, agg3, agg4})

	// check partitionBy
	allPartitionBy := make([][]sql.Expression, 0)
	for _, i := range outputIters {
		allPartitionBy = append(allPartitionBy, i.WindowBlock().PartitionBy)
	}
	require.ElementsMatch(t, allPartitionBy, [][]sql.Expression{
		nil,
		{
			expression.NewGetField(1, types.Int64, "b", false),
		}, {
			expression.NewGetField(2, types.Int64, "c", false),
		}})
}
