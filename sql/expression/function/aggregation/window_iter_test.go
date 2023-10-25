// Copyright 2022 Dolthub, Inc.
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
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

var (
	partitionByX = []sql.Expression{
		expression.NewGetFieldWithTable(1, types.Text, "db", "a", "x", false),
	}
	sortByW = sql.SortFields{{
		Column: expression.NewGetFieldWithTable(0, types.Int64, "db", "a", "w", false),
	}}
	sortByWDesc = sql.SortFields{
		{
			Column: expression.NewGetFieldWithTable(0, types.Int64, "db", "a", "w", false),
			Order:  sql.Descending,
		},
	}
	lastX  = NewLastAgg(expression.NewGetField(1, types.Text, "x", true))
	firstY = NewFirstAgg(expression.NewGetField(2, types.Text, "y", true))
	sumZ   = NewSumAgg(expression.NewGetField(3, types.Int64, "z", true))
)

func TestWindowIter(t *testing.T) {
	tests := []struct {
		Name           string
		PartitionIters []*WindowPartitionIter
		OutputOrdinals [][]int
		Expected       []sql.Row
	}{
		{
			Name: "unbounded preceding to current row",
			PartitionIters: []*WindowPartitionIter{
				NewWindowPartitionIter(
					&WindowPartition{
						PartitionBy: partitionByX,
						SortBy:      sortByW,
						Aggs: []*Aggregation{
							NewAggregation(lastX, NewUnboundedPrecedingToCurrentRowFramer()),
							NewAggregation(sumZ, NewUnboundedPrecedingToCurrentRowFramer()),
						},
					}),
				NewWindowPartitionIter(
					&WindowPartition{
						PartitionBy: partitionByX,
						SortBy:      sortByWDesc,
						Aggs: []*Aggregation{
							NewAggregation(firstY, NewUnboundedPrecedingToCurrentRowFramer()),
						},
					}),
			},
			OutputOrdinals: [][]int{{0, 1}, {2}},
			Expected: []sql.Row{
				{"forest", float64(4), "wildflower"},
				{"forest", float64(8), "wildflower"},
				{"forest", float64(14), "wildflower"},
				{"forest", float64(17), "wildflower"},
				{"forest", float64(27), "wildflower"},
				{"desert", float64(4), "mummy"},
				{"desert", float64(10), "mummy"},
				{"desert", float64(18), "mummy"},
				{"desert", float64(23), "mummy"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			db := memory.NewDatabase("test")
			pro := memory.NewDBProvider(db)
			ctx := sql.NewContext(context.Background(), sql.WithSession(memory.NewSession(sql.NewBaseSession(), pro)))

			i := NewWindowIter(tt.PartitionIters, tt.OutputOrdinals, mustNewRowIter(t, db, ctx))
			res, err := sql.RowIterToRows(ctx, nil, i)
			require.NoError(t, err)
			require.Equal(t, tt.Expected, res)
		})
	}
}
