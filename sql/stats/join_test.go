// Copyright 2023 Dolthub, Inc.
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

package stats

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestBinMerge(t *testing.T) {
	tests := []struct {
		inp []sql.HistogramBucket
		exp []sql.HistogramBucket
	}{
		{
			inp: sql.Histogram{
				&Bucket{RowCnt: 5, DistinctCnt: 5, BoundVal: sql.Row{2}, BoundCnt: 5},
				&Bucket{RowCnt: 5, DistinctCnt: 1, BoundVal: sql.Row{2}, BoundCnt: 5},
				&Bucket{RowCnt: 5, DistinctCnt: 1, BoundVal: sql.Row{3}, BoundCnt: 5},
				&Bucket{RowCnt: 5, DistinctCnt: 1, BoundVal: sql.Row{4}, BoundCnt: 5},
			},
			exp: sql.Histogram{
				&Bucket{RowCnt: 10, DistinctCnt: 5, BoundVal: sql.Row{2}, BoundCnt: 5},
				&Bucket{RowCnt: 5, DistinctCnt: 1, BoundVal: sql.Row{3}, BoundCnt: 5},
				&Bucket{RowCnt: 5, DistinctCnt: 1, BoundVal: sql.Row{4}, BoundCnt: 5},
			},
		},
		{
			inp: sql.Histogram{
				&Bucket{RowCnt: 5, DistinctCnt: 10, BoundVal: sql.Row{2}, BoundCnt: 5},
				&Bucket{RowCnt: 5, DistinctCnt: 1, BoundVal: sql.Row{3}, BoundCnt: 5},
				&Bucket{RowCnt: 5, DistinctCnt: 1, BoundVal: sql.Row{3}, BoundCnt: 5},
				&Bucket{RowCnt: 5, DistinctCnt: 1, BoundVal: sql.Row{4}, BoundCnt: 5},
			},
			exp: sql.Histogram{
				&Bucket{RowCnt: 5, DistinctCnt: 10, BoundVal: sql.Row{2}, BoundCnt: 5},
				&Bucket{RowCnt: 10, DistinctCnt: 1, BoundVal: sql.Row{3}, BoundCnt: 10},
				&Bucket{RowCnt: 5, DistinctCnt: 1, BoundVal: sql.Row{4}, BoundCnt: 5},
			},
		},
		{
			inp: sql.Histogram{
				&Bucket{RowCnt: 5, DistinctCnt: 10, BoundVal: sql.Row{2}, BoundCnt: 5},
				&Bucket{RowCnt: 5, DistinctCnt: 1, BoundVal: sql.Row{2}, BoundCnt: 5},
				&Bucket{RowCnt: 5, DistinctCnt: 5, BoundVal: sql.Row{4}, BoundCnt: 5},
				&Bucket{RowCnt: 5, DistinctCnt: 1, BoundVal: sql.Row{4}, BoundCnt: 5},
			},
			exp: sql.Histogram{
				&Bucket{RowCnt: 10, DistinctCnt: 10, BoundVal: sql.Row{2}, BoundCnt: 10},
				&Bucket{RowCnt: 10, DistinctCnt: 5, BoundVal: sql.Row{4}, BoundCnt: 10},
			},
		},
	}
	for i, tt := range tests {
		t.Run(fmt.Sprintf("bin merge %d", i), func(t *testing.T) {
			cmp, err := mergeOverlappingBuckets(tt.inp, []sql.Type{types.Int64})
			require.NoError(t, err)
			compareHist(t, tt.exp, cmp)
		})
	}
}

func TestEuclideanDistance(t *testing.T) {
	tests := []struct {
		x, y sql.Row
		dist float64
	}{
		{
			x:    sql.Row{0, 3},
			y:    sql.Row{4, 0},
			dist: 5,
		},
		{
			x:    sql.Row{5, 0, 0},
			y:    sql.Row{0, 12, 0},
			dist: 13,
		},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("%v x %v = %.2f", tt.x, tt.y, tt.dist), func(t *testing.T) {
			cmp, err := euclideanDistance(tt.x, tt.y, len(tt.x))
			require.NoError(t, err)
			require.Equal(t, tt.dist, cmp)
		})
	}
}

func TestBinAlignment(t *testing.T) {
	tests := []struct {
		left     []sql.HistogramBucket
		right    []sql.HistogramBucket
		expLeft  []sql.HistogramBucket
		expRight []sql.HistogramBucket
	}{
		{
			left: []sql.HistogramBucket{
				&Bucket{RowCnt: 10, DistinctCnt: 10, BoundVal: sql.Row{0}, BoundCnt: 1},
				&Bucket{RowCnt: 10, DistinctCnt: 10, BoundVal: sql.Row{10}, BoundCnt: 1},
				&Bucket{RowCnt: 10, DistinctCnt: 10, BoundVal: sql.Row{30}, BoundCnt: 1},
			},
			right: []sql.HistogramBucket{
				&Bucket{RowCnt: 10, DistinctCnt: 10, BoundVal: sql.Row{0}, BoundCnt: 1},
				&Bucket{RowCnt: 10, DistinctCnt: 10, BoundVal: sql.Row{20}, BoundCnt: 1},
				&Bucket{RowCnt: 10, DistinctCnt: 10, BoundVal: sql.Row{30}, BoundCnt: 1},
			},
			expLeft: []sql.HistogramBucket{
				&Bucket{RowCnt: 10, DistinctCnt: 10, BoundVal: sql.Row{0}, BoundCnt: 1},
				&Bucket{RowCnt: 10, DistinctCnt: 10, BoundVal: sql.Row{10}, BoundCnt: 1},
				&Bucket{RowCnt: 5, DistinctCnt: 5, BoundVal: sql.Row{20}, BoundCnt: 1},
				&Bucket{RowCnt: 5, DistinctCnt: 5, BoundVal: sql.Row{30}, BoundCnt: 1},
			},
			expRight: []sql.HistogramBucket{
				&Bucket{RowCnt: 10, DistinctCnt: 10, BoundVal: sql.Row{0}, BoundCnt: 1},
				&Bucket{RowCnt: 5, DistinctCnt: 5, BoundVal: sql.Row{10}, BoundCnt: 1},
				&Bucket{RowCnt: 5, DistinctCnt: 5, BoundVal: sql.Row{20}, BoundCnt: 1},
				&Bucket{RowCnt: 10, DistinctCnt: 10, BoundVal: sql.Row{30}, BoundCnt: 1},
			},
		},
		{
			left: []sql.HistogramBucket{
				&Bucket{RowCnt: 10, DistinctCnt: 10, BoundVal: sql.Row{0}, BoundCnt: 1},
				&Bucket{RowCnt: 10, DistinctCnt: 10, BoundVal: sql.Row{50}, BoundCnt: 1},
			},
			right: []sql.HistogramBucket{
				&Bucket{RowCnt: 10, DistinctCnt: 10, BoundVal: sql.Row{10}, BoundCnt: 1},
				&Bucket{RowCnt: 10, DistinctCnt: 10, BoundVal: sql.Row{20}, BoundCnt: 1},
				&Bucket{RowCnt: 10, DistinctCnt: 10, BoundVal: sql.Row{30}, BoundCnt: 1},
			},
			expLeft: []sql.HistogramBucket{
				&Bucket{RowCnt: 12, DistinctCnt: 12, BoundVal: sql.Row{10}, BoundCnt: 1},
				&Bucket{RowCnt: 2, DistinctCnt: 2, BoundVal: sql.Row{20}, BoundCnt: 1},
				&Bucket{RowCnt: 6, DistinctCnt: 6, BoundVal: sql.Row{50}, BoundCnt: 1},
			},
			expRight: []sql.HistogramBucket{
				&Bucket{RowCnt: 10, DistinctCnt: 10, BoundVal: sql.Row{10}, BoundCnt: 1},
				&Bucket{RowCnt: 10, DistinctCnt: 10, BoundVal: sql.Row{20}, BoundCnt: 1},
				&Bucket{RowCnt: 10, DistinctCnt: 10, BoundVal: sql.Row{30}, BoundCnt: 1},
			},
		},
		{
			left: []sql.HistogramBucket{
				&Bucket{RowCnt: 10, DistinctCnt: 10, BoundVal: sql.Row{0}, BoundCnt: 1},
				&Bucket{RowCnt: 10, DistinctCnt: 10, BoundVal: sql.Row{10}, BoundCnt: 1},
				&Bucket{RowCnt: 10, DistinctCnt: 10, BoundVal: sql.Row{40}, BoundCnt: 1},
			},
			right: []sql.HistogramBucket{
				&Bucket{RowCnt: 10, DistinctCnt: 10, BoundVal: sql.Row{20}, BoundCnt: 1},
				&Bucket{RowCnt: 10, DistinctCnt: 10, BoundVal: sql.Row{30}, BoundCnt: 1},
				&Bucket{RowCnt: 10, DistinctCnt: 10, BoundVal: sql.Row{50}, BoundCnt: 1},
			},
			expLeft: []sql.HistogramBucket{
				&Bucket{RowCnt: 23, DistinctCnt: 23, BoundVal: sql.Row{20}, BoundCnt: 1},
				&Bucket{RowCnt: 3, DistinctCnt: 3, BoundVal: sql.Row{30}, BoundCnt: 1},
				&Bucket{RowCnt: 3, DistinctCnt: 3, BoundVal: sql.Row{40}, BoundCnt: 1},
			},
			expRight: []sql.HistogramBucket{
				&Bucket{RowCnt: 10, DistinctCnt: 10, BoundVal: sql.Row{20}, BoundCnt: 1},
				&Bucket{RowCnt: 10, DistinctCnt: 10, BoundVal: sql.Row{30}, BoundCnt: 1},
				&Bucket{RowCnt: 10, DistinctCnt: 10, BoundVal: sql.Row{50}, BoundCnt: 1},
			},
		},
		{
			left: []sql.HistogramBucket{
				&Bucket{RowCnt: 10, DistinctCnt: 10, BoundVal: sql.Row{0}, BoundCnt: 1},
				&Bucket{RowCnt: 10, DistinctCnt: 10, BoundVal: sql.Row{10}, BoundCnt: 1},
				&Bucket{RowCnt: 10, DistinctCnt: 10, BoundVal: sql.Row{40}, BoundCnt: 1},
			},
			right: []sql.HistogramBucket{
				&Bucket{RowCnt: 10, DistinctCnt: 10, BoundVal: sql.Row{30}, BoundCnt: 1},
				&Bucket{RowCnt: 10, DistinctCnt: 10, BoundVal: sql.Row{50}, BoundCnt: 1},
				&Bucket{RowCnt: 10, DistinctCnt: 10, BoundVal: sql.Row{60}, BoundCnt: 1},
			},
			expLeft: []sql.HistogramBucket{
				&Bucket{RowCnt: 26, DistinctCnt: 26, BoundVal: sql.Row{30}, BoundCnt: 1},
				&Bucket{RowCnt: 3, DistinctCnt: 3, BoundVal: sql.Row{40}, BoundCnt: 1},
			},
			expRight: []sql.HistogramBucket{
				&Bucket{RowCnt: 10, DistinctCnt: 10, BoundVal: sql.Row{30}, BoundCnt: 1},
				&Bucket{RowCnt: 10, DistinctCnt: 10, BoundVal: sql.Row{50}, BoundCnt: 1},
			},
		},
		{
			left: []sql.HistogramBucket{
				&Bucket{RowCnt: 10, DistinctCnt: 10, BoundVal: sql.Row{0}, BoundCnt: 1},
				&Bucket{RowCnt: 10, DistinctCnt: 10, BoundVal: sql.Row{10}, BoundCnt: 1},
			},
			right: []sql.HistogramBucket{
				&Bucket{RowCnt: 10, DistinctCnt: 10, BoundVal: sql.Row{30}, BoundCnt: 1},
				&Bucket{RowCnt: 10, DistinctCnt: 10, BoundVal: sql.Row{50}, BoundCnt: 1},
				&Bucket{RowCnt: 10, DistinctCnt: 10, BoundVal: sql.Row{60}, BoundCnt: 1},
			},
			expLeft: []sql.HistogramBucket{
				&Bucket{RowCnt: 20, DistinctCnt: 20, BoundVal: sql.Row{10}, BoundCnt: 1},
			},
			expRight: []sql.HistogramBucket{
				&Bucket{RowCnt: 10, DistinctCnt: 10, BoundVal: sql.Row{30}, BoundCnt: 1},
			},
		},
		{
			left: []sql.HistogramBucket{
				&Bucket{RowCnt: 10, DistinctCnt: 10, BoundVal: sql.Row{0}, BoundCnt: 1},
				&Bucket{RowCnt: 10, DistinctCnt: 10, BoundVal: sql.Row{10}, BoundCnt: 1},
				&Bucket{RowCnt: 10, DistinctCnt: 10, BoundVal: sql.Row{30}, BoundCnt: 1},
			},
			right: []sql.HistogramBucket{
				&Bucket{RowCnt: 10, DistinctCnt: 10, BoundVal: sql.Row{30}, BoundCnt: 1},
				&Bucket{RowCnt: 10, DistinctCnt: 10, BoundVal: sql.Row{50}, BoundCnt: 1},
				&Bucket{RowCnt: 10, DistinctCnt: 10, BoundVal: sql.Row{60}, BoundCnt: 1},
			},
			expLeft: []sql.HistogramBucket{
				&Bucket{RowCnt: 30, DistinctCnt: 30, BoundVal: sql.Row{30}, BoundCnt: 1},
			},
			expRight: []sql.HistogramBucket{
				&Bucket{RowCnt: 10, DistinctCnt: 10, BoundVal: sql.Row{30}, BoundCnt: 1},
			},
		},
		{
			left: []sql.HistogramBucket{
				&Bucket{RowCnt: 10, DistinctCnt: 10, BoundVal: sql.Row{0}, BoundCnt: 1},
				&Bucket{RowCnt: 10, DistinctCnt: 10, BoundVal: sql.Row{10}, BoundCnt: 1},
				&Bucket{RowCnt: 10, DistinctCnt: 10, BoundVal: sql.Row{30}, BoundCnt: 1},
				&Bucket{RowCnt: 10, DistinctCnt: 10, BoundVal: sql.Row{50}, BoundCnt: 1},
			},
			right: []sql.HistogramBucket{
				&Bucket{RowCnt: 10, DistinctCnt: 10, BoundVal: sql.Row{30}, BoundCnt: 1},
				&Bucket{RowCnt: 10, DistinctCnt: 10, BoundVal: sql.Row{50}, BoundCnt: 1},
				&Bucket{RowCnt: 10, DistinctCnt: 10, BoundVal: sql.Row{60}, BoundCnt: 1},
			},
			expLeft: []sql.HistogramBucket{
				&Bucket{RowCnt: 30, DistinctCnt: 30, BoundVal: sql.Row{30}, BoundCnt: 1},
				&Bucket{RowCnt: 10, DistinctCnt: 10, BoundVal: sql.Row{50}, BoundCnt: 1},
			},
			expRight: []sql.HistogramBucket{
				&Bucket{RowCnt: 10, DistinctCnt: 10, BoundVal: sql.Row{30}, BoundCnt: 1},
				&Bucket{RowCnt: 10, DistinctCnt: 10, BoundVal: sql.Row{50}, BoundCnt: 1},
			},
		},
		{
			left: []sql.HistogramBucket{
				&Bucket{RowCnt: 10, DistinctCnt: 10, BoundVal: sql.Row{0}, BoundCnt: 1},
				&Bucket{RowCnt: 10, DistinctCnt: 10, BoundVal: sql.Row{10}, BoundCnt: 1},
				&Bucket{RowCnt: 10, DistinctCnt: 10, BoundVal: sql.Row{20}, BoundCnt: 1},
			},
			right: []sql.HistogramBucket{
				&Bucket{RowCnt: 10, DistinctCnt: 10, BoundVal: sql.Row{20}, BoundCnt: 1},
				&Bucket{RowCnt: 10, DistinctCnt: 10, BoundVal: sql.Row{30}, BoundCnt: 1},
			},
			expLeft: []sql.HistogramBucket{
				&Bucket{RowCnt: 30, DistinctCnt: 30, BoundVal: sql.Row{20}, BoundCnt: 1},
			},
			expRight: []sql.HistogramBucket{
				&Bucket{RowCnt: 10, DistinctCnt: 10, BoundVal: sql.Row{20}, BoundCnt: 1},
			},
		},
		{
			left: []sql.HistogramBucket{
				&Bucket{RowCnt: 10, DistinctCnt: 10, BoundVal: sql.Row{0}, BoundCnt: 1},
				&Bucket{RowCnt: 10, DistinctCnt: 10, BoundVal: sql.Row{10}, BoundCnt: 1},
			},
			right: []sql.HistogramBucket{
				&Bucket{RowCnt: 10, DistinctCnt: 10, BoundVal: sql.Row{20}, BoundCnt: 1},
				&Bucket{RowCnt: 10, DistinctCnt: 10, BoundVal: sql.Row{30}, BoundCnt: 1},
			},
			expLeft: []sql.HistogramBucket{
				&Bucket{RowCnt: 20, DistinctCnt: 20, BoundVal: sql.Row{10}, BoundCnt: 1},
			},
			expRight: []sql.HistogramBucket{
				&Bucket{RowCnt: 10, DistinctCnt: 10, BoundVal: sql.Row{20}, BoundCnt: 1},
			},
		},
	}

	cmp := func(i, j sql.Row) (int, error) {
		return types.Int64.Compare(i[0], j[0])
	}

	for i, tt := range tests {
		t.Run(fmt.Sprintf("alignment test %d", i), func(t *testing.T) {
			lCmp, rCmp, err := AlignBuckets(tt.left, tt.right, nil, nil, []sql.Type{types.Int64}, []sql.Type{types.Int64}, cmp)
			require.NoError(t, err)
			compareHist(t, tt.expLeft, lCmp)
			compareHist(t, tt.expRight, rCmp)
		})
	}
}

func TestJoin(t *testing.T) {
	tests := []struct {
		left  sql.Histogram
		right sql.Histogram
		exp   sql.Histogram
	}{
		{
			left: []sql.HistogramBucket{
				&Bucket{RowCnt: 20, DistinctCnt: 20, BoundVal: sql.Row{10}, BoundCnt: 1},
			},
			right: []sql.HistogramBucket{
				&Bucket{RowCnt: 10, DistinctCnt: 10, BoundVal: sql.Row{20}, BoundCnt: 1},
			},
			exp: []sql.HistogramBucket{
				&Bucket{RowCnt: 10, DistinctCnt: 10, BoundVal: sql.Row{10}, BoundCnt: 1},
			},
		},
		{
			left: []sql.HistogramBucket{
				&Bucket{RowCnt: 20, DistinctCnt: 10, BoundVal: sql.Row{10}, BoundCnt: 1},
			},
			right: []sql.HistogramBucket{
				&Bucket{RowCnt: 10, DistinctCnt: 10, BoundVal: sql.Row{10}, BoundCnt: 1},
			},
			exp: []sql.HistogramBucket{
				&Bucket{RowCnt: 20, DistinctCnt: 10, BoundVal: sql.Row{10}, BoundCnt: 1},
			},
		},
		{
			left: []sql.HistogramBucket{
				&Bucket{RowCnt: 20, DistinctCnt: 11, BoundVal: sql.Row{10}, McvVals: []sql.Row{{1}, {2}}, McvsCnt: []uint64{5, 5}, BoundCnt: 1},
			},
			right: []sql.HistogramBucket{
				&Bucket{RowCnt: 10, DistinctCnt: 6, BoundVal: sql.Row{10}, McvVals: []sql.Row{{2}}, McvsCnt: []uint64{4}, BoundCnt: 1},
			},
			exp: []sql.HistogramBucket{
				&Bucket{RowCnt: 29, DistinctCnt: 6, BoundVal: sql.Row{10}, BoundCnt: 1},
			},
		},
		{
			left: []sql.HistogramBucket{
				&Bucket{RowCnt: 20, DistinctCnt: 10, BoundVal: sql.Row{10}, McvVals: []sql.Row{{1}, {2}}, McvsCnt: []uint64{5, 5}, BoundCnt: 1},
			},
			right: []sql.HistogramBucket{
				&Bucket{RowCnt: 10, DistinctCnt: 6, BoundVal: sql.Row{10}, McvVals: []sql.Row{{3}}, McvsCnt: []uint64{4}, BoundCnt: 1},
			},
			exp: []sql.HistogramBucket{
				&Bucket{RowCnt: 20, DistinctCnt: 6, BoundVal: sql.Row{10}, BoundCnt: 1},
			},
		},
		{
			left: []sql.HistogramBucket{
				&Bucket{RowCnt: 20, DistinctCnt: 10, BoundVal: sql.Row{10}, McvVals: []sql.Row{{1}, {2}}, McvsCnt: []uint64{5, 5}, BoundCnt: 1},
			},
			right: []sql.HistogramBucket{
				&Bucket{RowCnt: 10, DistinctCnt: 10, BoundVal: sql.Row{10}, McvVals: []sql.Row{{3}}, McvsCnt: []uint64{4}, BoundCnt: 1},
			},
			exp: []sql.HistogramBucket{
				&Bucket{RowCnt: 20, DistinctCnt: 10, BoundVal: sql.Row{10}, BoundCnt: 1},
			},
		},
		{
			left: []sql.HistogramBucket{
				&Bucket{RowCnt: 23, DistinctCnt: 23, BoundVal: sql.Row{20}, BoundCnt: 1},
				&Bucket{RowCnt: 3, DistinctCnt: 3, BoundVal: sql.Row{30}, BoundCnt: 1},
				&Bucket{RowCnt: 3, DistinctCnt: 3, BoundVal: sql.Row{40}, BoundCnt: 1},
			},
			right: []sql.HistogramBucket{
				&Bucket{RowCnt: 10, DistinctCnt: 10, BoundVal: sql.Row{20}, BoundCnt: 1},
				&Bucket{RowCnt: 10, DistinctCnt: 10, BoundVal: sql.Row{30}, BoundCnt: 1},
				&Bucket{RowCnt: 10, DistinctCnt: 10, BoundVal: sql.Row{50}, BoundCnt: 1},
			},
			exp: []sql.HistogramBucket{
				&Bucket{RowCnt: 10, DistinctCnt: 10, BoundVal: sql.Row{20}, BoundCnt: 1},
				&Bucket{RowCnt: 3, DistinctCnt: 3, BoundVal: sql.Row{30}, BoundCnt: 1},
				&Bucket{RowCnt: 3, DistinctCnt: 3, BoundVal: sql.Row{40}, BoundCnt: 1},
			},
		},
		{
			left: []sql.HistogramBucket{
				&Bucket{RowCnt: 10, DistinctCnt: 10, BoundVal: sql.Row{0}, BoundCnt: 1},
				&Bucket{RowCnt: 10, DistinctCnt: 10, BoundVal: sql.Row{10}, BoundCnt: 1},
				&Bucket{RowCnt: 5, DistinctCnt: 5, BoundVal: sql.Row{20}, BoundCnt: 1},
				&Bucket{RowCnt: 5, DistinctCnt: 5, BoundVal: sql.Row{30}, BoundCnt: 1},
			},
			right: []sql.HistogramBucket{
				&Bucket{RowCnt: 10, DistinctCnt: 10, BoundVal: sql.Row{0}, BoundCnt: 1},
				&Bucket{RowCnt: 5, DistinctCnt: 5, BoundVal: sql.Row{10}, BoundCnt: 1},
				&Bucket{RowCnt: 5, DistinctCnt: 5, BoundVal: sql.Row{20}, BoundCnt: 1},
				&Bucket{RowCnt: 10, DistinctCnt: 10, BoundVal: sql.Row{30}, BoundCnt: 1},
			},
			exp: []sql.HistogramBucket{
				&Bucket{RowCnt: 10, DistinctCnt: 10, BoundVal: sql.Row{0}, BoundCnt: 1},
				&Bucket{RowCnt: 5, DistinctCnt: 5, BoundVal: sql.Row{10}, BoundCnt: 1},
				&Bucket{RowCnt: 5, DistinctCnt: 5, BoundVal: sql.Row{20}, BoundCnt: 1},
				&Bucket{RowCnt: 5, DistinctCnt: 5, BoundVal: sql.Row{30}, BoundCnt: 1},
			},
		},
		{
			left: []sql.HistogramBucket{
				&Bucket{RowCnt: 12, DistinctCnt: 12, BoundVal: sql.Row{10}, BoundCnt: 1},
				&Bucket{RowCnt: 6, DistinctCnt: 6, BoundVal: sql.Row{20}, BoundCnt: 1},
				&Bucket{RowCnt: 1, DistinctCnt: 1, BoundVal: sql.Row{50}, BoundCnt: 1},
			},
			right: []sql.HistogramBucket{
				&Bucket{RowCnt: 10, DistinctCnt: 10, BoundVal: sql.Row{10}, BoundCnt: 1},
				&Bucket{RowCnt: 10, DistinctCnt: 10, BoundVal: sql.Row{20}, BoundCnt: 1},
				&Bucket{RowCnt: 10, DistinctCnt: 10, BoundVal: sql.Row{30}, BoundCnt: 1},
			},
			exp: []sql.HistogramBucket{
				&Bucket{RowCnt: 10, DistinctCnt: 10, BoundVal: sql.Row{10}, BoundCnt: 1},
				&Bucket{RowCnt: 6, DistinctCnt: 6, BoundVal: sql.Row{20}, BoundCnt: 1},
				&Bucket{RowCnt: 1, DistinctCnt: 1, BoundVal: sql.Row{50}, BoundCnt: 1},
			},
		},
		{
			left: []sql.HistogramBucket{
				&Bucket{RowCnt: 10, DistinctCnt: 3, BoundVal: sql.Row{0}, BoundCnt: 1},
				&Bucket{RowCnt: 10, DistinctCnt: 3, BoundVal: sql.Row{10}, BoundCnt: 1},
				&Bucket{RowCnt: 5, DistinctCnt: 2, BoundVal: sql.Row{20}, BoundCnt: 1},
				&Bucket{RowCnt: 5, DistinctCnt: 2, BoundVal: sql.Row{30}, BoundCnt: 1},
			},
			right: []sql.HistogramBucket{
				&Bucket{RowCnt: 10, DistinctCnt: 3, BoundVal: sql.Row{0}, BoundCnt: 1},
				&Bucket{RowCnt: 5, DistinctCnt: 2, BoundVal: sql.Row{10}, BoundCnt: 1},
				&Bucket{RowCnt: 5, DistinctCnt: 2, BoundVal: sql.Row{20}, BoundCnt: 1},
				&Bucket{RowCnt: 10, DistinctCnt: 3, BoundVal: sql.Row{30}, BoundCnt: 1},
			},
			exp: []sql.HistogramBucket{
				&Bucket{RowCnt: 33, DistinctCnt: 3, BoundVal: sql.Row{0}, BoundCnt: 1},
				&Bucket{RowCnt: 16, DistinctCnt: 2, BoundVal: sql.Row{10}, BoundCnt: 1},
				&Bucket{RowCnt: 12, DistinctCnt: 2, BoundVal: sql.Row{20}, BoundCnt: 1},
				&Bucket{RowCnt: 16, DistinctCnt: 2, BoundVal: sql.Row{30}, BoundCnt: 1},
			},
		},
	}

	cmp := func(i, j sql.Row) (int, error) {
		return types.Int64.Compare(i[0], j[0])
	}

	for i, tt := range tests {
		t.Run(fmt.Sprintf("join test %d", i), func(t *testing.T) {
			cmp, err := joinAlignedStats(tt.left, tt.right, cmp)
			require.NoError(t, err)
			cmpHist := make(sql.Histogram, len(cmp))
			for i, v := range cmp {
				cmpHist[i] = v
			}
			compareHist(t, tt.exp, cmpHist)
		})
	}
}

func compareHist(t *testing.T, exp, cmp sql.Histogram) {
	if len(exp) != len(cmp) {
		t.Errorf("histograms not same length: %d != %d\n%s\n%s\n", len(exp), len(cmp), exp.DebugString(), cmp.DebugString())
	}
	for i, b := range exp {
		require.Equalf(t, b.UpperBound(), cmp[i].UpperBound(), "bound not equal: %v != %v\n%s\n%s", b.UpperBound(), cmp[i].UpperBound(), exp.DebugString(), cmp.DebugString())
		if b.RowCount() != cmp[i].RowCount() {
			t.Errorf("histograms row count not equal: %d != %d\n%s\n%s", b.RowCount(), cmp[i].RowCount(), exp.DebugString(), cmp.DebugString())
		}
		if b.DistinctCount() != cmp[i].DistinctCount() {
			t.Errorf("histograms distinct not equal: %d != %d\n%s\n%s", b.DistinctCount(), cmp[i].DistinctCount(), exp.DebugString(), cmp.DebugString())
		}
		if b.NullCount() != cmp[i].NullCount() {
			t.Errorf("histograms null not equal: %d != %d\n%s\n%s", b.NullCount(), cmp[i].NullCount(), exp.DebugString(), cmp.DebugString())
		}
	}
}
