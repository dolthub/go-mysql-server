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
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

func TestGroupedAggFuncs(t *testing.T) {
	tests := []struct {
		Name     string
		Agg      sql.WindowFunction
		Expected sql.Row
	}{
		{
			Name:     "count star",
			Agg:      NewCountAgg(expression.NewStar()),
			Expected: sql.Row{int64(4), int64(4), int64(6)},
		},
		{
			Name:     "count without nulls",
			Agg:      NewCountAgg(expression.NewGetField(1, sql.LongText, "x", true)),
			Expected: sql.Row{int64(4), int64(4), int64(6)},
		},
		{
			Name:     "count with nulls",
			Agg:      NewCountAgg(expression.NewGetField(0, sql.LongText, "x", true)),
			Expected: sql.Row{int64(3), int64(3), int64(4)},
		},
		{
			Name:     "max ints",
			Agg:      NewMaxAgg(expression.NewGetField(1, sql.LongText, "x", true)),
			Expected: sql.Row{4, 4, 6},
		},
		{
			Name:     "max int64",
			Agg:      NewMaxAgg(expression.NewGetField(2, sql.LongText, "x", true)),
			Expected: sql.Row{int64(3), int64(3), int64(5)},
		},
		{
			Name:     "max w/ nulls",
			Agg:      NewMaxAgg(expression.NewGetField(0, sql.LongText, "x", true)),
			Expected: sql.Row{4, 4, 6},
		},
		{
			Name:     "max w/ float",
			Agg:      NewMaxAgg(expression.NewGetField(3, sql.LongText, "x", true)),
			Expected: sql.Row{float64(4), float64(4), float64(6)},
		},
		{
			Name:     "min ints",
			Agg:      NewMinAgg(expression.NewGetField(1, sql.LongText, "x", true)),
			Expected: sql.Row{1, 1, 1},
		},
		{
			Name:     "min int64",
			Agg:      NewMinAgg(expression.NewGetField(2, sql.LongText, "x", true)),
			Expected: sql.Row{int64(1), int64(1), int64(1)},
		},
		{
			Name:     "min w/ nulls",
			Agg:      NewMinAgg(expression.NewGetField(0, sql.LongText, "x", true)),
			Expected: sql.Row{1, 1, 1},
		},
		{
			Name:     "min w/ float",
			Agg:      NewMinAgg(expression.NewGetField(3, sql.LongText, "x", true)),
			Expected: sql.Row{float64(1), float64(1), float64(1)},
		},
		{
			Name:     "avg nulls",
			Agg:      NewAvgAgg(expression.NewGetField(0, sql.LongText, "x", true)),
			Expected: sql.Row{float64(8) / float64(3), float64(8) / float64(3), float64(14) / float64(4)},
		},
		{
			Name:     "avg int",
			Agg:      NewAvgAgg(expression.NewGetField(1, sql.LongText, "x", true)),
			Expected: sql.Row{float64(10) / float64(4), float64(10) / float64(4), float64(21) / float64(6)},
		},
		{
			Name:     "avg int64",
			Agg:      NewAvgAgg(expression.NewGetField(2, sql.LongText, "x", true)),
			Expected: sql.Row{float64(8) / float64(4), float64(8) / float64(4), float64(17) / float64(6)},
		},
		{
			Name:     "avg float",
			Agg:      NewAvgAgg(expression.NewGetField(3, sql.LongText, "x", true)),
			Expected: sql.Row{float64(10) / float64(4), float64(10) / float64(4), float64(21) / float64(6)},
		},
		{
			Name:     "sum nulls",
			Agg:      NewSumAgg(expression.NewGetField(0, sql.LongText, "x", true)),
			Expected: sql.Row{float64(8), float64(8), float64(14)},
		},
		{
			Name:     "sum ints",
			Agg:      NewSumAgg(expression.NewGetField(1, sql.LongText, "x", true)),
			Expected: sql.Row{float64(10), float64(10), float64(21)},
		},
		{
			Name:     "sum int64",
			Agg:      NewSumAgg(expression.NewGetField(2, sql.LongText, "x", true)),
			Expected: sql.Row{float64(8), float64(8), float64(17)},
		},
		{
			Name:     "sum float64",
			Agg:      NewSumAgg(expression.NewGetField(3, sql.LongText, "x", true)),
			Expected: sql.Row{float64(10), float64(10), float64(21)},
		},
		{
			Name:     "first",
			Agg:      NewFirstAgg(expression.NewGetField(0, sql.LongText, "x", true)),
			Expected: sql.Row{1, 1, 1},
		},
		{
			Name:     "last",
			Agg:      NewLastAgg(expression.NewGetField(0, sql.LongText, "x", true)),
			Expected: sql.Row{4, 4, 6},
		},
		// list aggregations
		{
			Name:     "group concat null",
			Agg:      NewGroupConcatAgg(mustNewGroupByConcat("", nil, ",", []sql.Expression{expression.NewGetField(0, sql.LongText, "x", true)}, 1042)),
			Expected: sql.Row{"1,3,4", "1,3,4", "1,2,5,6"},
		},
		{
			Name:     "group concat int",
			Agg:      NewGroupConcatAgg(mustNewGroupByConcat("", nil, ",", []sql.Expression{expression.NewGetField(1, sql.LongText, "x", true)}, 1042)),
			Expected: sql.Row{"1,2,3,4", "1,2,3,4", "1,2,3,4,5,6"},
		},
		{
			Name:     "group concat float",
			Agg:      NewGroupConcatAgg(mustNewGroupByConcat("", nil, ",", []sql.Expression{expression.NewGetField(3, sql.LongText, "x", true)}, 1042)),
			Expected: sql.Row{"1,2,3,4", "1,2,3,4", "1,2,3,4,5,6"},
		},
		{
			Name: "json array null",
			Agg:  NewJsonArrayAgg(expression.NewGetField(0, sql.LongText, "x", true)),
			Expected: sql.Row{
				sql.JSONDocument{Val: []interface{}{1, nil, 3, 4}},
				sql.JSONDocument{Val: []interface{}{1, nil, 3, 4}},
				sql.JSONDocument{Val: []interface{}{1, 2, nil, nil, 5, 6}},
			},
		},
		{
			Name: "json array int",
			Agg:  NewJsonArrayAgg(expression.NewGetField(1, sql.LongText, "x", true)),
			Expected: sql.Row{
				sql.JSONDocument{Val: []interface{}{1, 2, 3, 4}},
				sql.JSONDocument{Val: []interface{}{1, 2, 3, 4}},
				sql.JSONDocument{Val: []interface{}{1, 2, 3, 4, 5, 6}},
			},
		},
		{
			Name: "json array float",
			Agg:  NewJsonArrayAgg(expression.NewGetField(3, sql.LongText, "x", true)),
			Expected: sql.Row{
				sql.JSONDocument{Val: []interface{}{float64(1), float64(2), float64(3), float64(4)}},
				sql.JSONDocument{Val: []interface{}{float64(1), float64(2), float64(3), float64(4)}},
				sql.JSONDocument{Val: []interface{}{float64(1), float64(2), float64(3), float64(4), float64(5), float64(6)}},
			},
		},
		{
			Name: "json object null",
			Agg: NewWindowedJSONObjectAgg(
				NewJSONObjectAgg(
					expression.NewGetField(1, sql.LongText, "x", true),
					expression.NewGetField(0, sql.LongText, "y", true),
				).(*JSONObjectAgg),
			),
			Expected: sql.Row{
				sql.JSONDocument{Val: map[string]interface{}{"1": 1, "2": nil, "3": 3, "4": 4}},
				sql.JSONDocument{Val: map[string]interface{}{"1": 1, "2": nil, "3": 3, "4": 4}},
				sql.JSONDocument{Val: map[string]interface{}{"1": 1, "2": 2, "3": nil, "4": nil, "5": 5, "6": 6}},
			},
		},
		{
			Name: "json object int",
			Agg: NewWindowedJSONObjectAgg(
				NewJSONObjectAgg(
					expression.NewGetField(1, sql.LongText, "x", true),
					expression.NewGetField(0, sql.LongText, "x", true),
				).(*JSONObjectAgg),
			),
			Expected: sql.Row{
				sql.JSONDocument{Val: map[string]interface{}{"1": 1, "2": nil, "3": 3, "4": 4}},
				sql.JSONDocument{Val: map[string]interface{}{"1": 1, "2": nil, "3": 3, "4": 4}},
				sql.JSONDocument{Val: map[string]interface{}{"1": 1, "2": 2, "3": nil, "4": nil, "5": 5, "6": 6}},
			},
		},
		{
			Name: "json object float",
			Agg: NewWindowedJSONObjectAgg(
				NewJSONObjectAgg(
					expression.NewGetField(1, sql.LongText, "x", true),
					expression.NewGetField(3, sql.LongText, "x", true),
				).(*JSONObjectAgg),
			),
			Expected: sql.Row{
				sql.JSONDocument{Val: map[string]interface{}{"1": float64(1), "2": float64(2), "3": float64(3), "4": float64(4)}},
				sql.JSONDocument{Val: map[string]interface{}{"1": float64(1), "2": float64(2), "3": float64(3), "4": float64(4)}},
				sql.JSONDocument{Val: map[string]interface{}{"1": float64(1), "2": float64(2), "3": float64(3), "4": float64(4), "5": float64(5), "6": float64(6)}},
			},
		},
		{
			Name: "json object float",
			Agg: NewWindowedJSONObjectAgg(
				NewJSONObjectAgg(
					expression.NewGetField(1, sql.LongText, "x", true),
					expression.NewGetField(3, sql.LongText, "x", true),
				).(*JSONObjectAgg),
			),
			Expected: sql.Row{
				sql.JSONDocument{Val: map[string]interface{}{"1": float64(1), "2": float64(2), "3": float64(3), "4": float64(4)}},
				sql.JSONDocument{Val: map[string]interface{}{"1": float64(1), "2": float64(2), "3": float64(3), "4": float64(4)}},
				sql.JSONDocument{Val: map[string]interface{}{"1": float64(1), "2": float64(2), "3": float64(3), "4": float64(4), "5": float64(5), "6": float64(6)}},
			},
		},
	}

	buf := []sql.Row{
		{1, 1, int64(1), float64(1), 1, 1},
		{nil, 2, int64(2), float64(2), 1, 1},
		{3, 3, int64(3), float64(3), 1, 2},
		{4, 4, int64(2), float64(4), 1, 3},
		{1, 1, int64(1), float64(1), 2, 1},
		{nil, 2, int64(2), float64(2), 2, 2},
		{3, 3, int64(3), float64(3), 2, 2},
		{4, 4, int64(2), float64(4), 3},
		{1, 1, int64(1), float64(1), 1},
		{2, 2, int64(2), float64(2), 2},
		{nil, 3, int64(3), float64(3), 2},
		{nil, 4, int64(4), float64(4), 3},
		{5, 5, int64(5), float64(5), 3},
		{6, 6, int64(2), float64(6), 4},
	}

	partitions := []sql.WindowInterval{
		{Start: 0, End: 4},
		{Start: 4, End: 8},
		{Start: 8, End: 14},
	}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			ctx := sql.NewEmptyContext()
			res := make(sql.Row, len(partitions))
			for i, p := range partitions {
				err := tt.Agg.StartPartition(ctx, p, buf)
				require.NoError(t, err)
				res[i] = tt.Agg.Compute(ctx, p, buf)
			}
			require.Equal(t, tt.Expected, res)
		})
	}
}

func TestWindowedAggFuncs(t *testing.T) {
	tests := []struct {
		Name     string
		Agg      sql.WindowFunction
		Expected sql.Row
	}{
		{
			Name:     "lag",
			Agg:      NewLag(expression.NewGetField(1, sql.LongText, "x", true), nil, 2),
			Expected: sql.Row{nil, nil, 1, 2, nil, nil, 1, 2, nil, nil, 1, 2, 3, 4},
		},
		{
			Name: "lag w/ default",
			Agg: NewLag(
				expression.NewGetField(1, sql.LongText, "x", true),
				expression.NewGetField(1, sql.LongText, "x", true),
				2,
			),
			Expected: sql.Row{1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 3, 4},
		},
		{
			Name: "lag nil",
			Agg: NewLag(
				expression.NewGetField(0, sql.LongText, "x", true),
				nil,
				1,
			),
			Expected: sql.Row{nil, 1, nil, 3, nil, 1, nil, 3, nil, 1, 2, nil, nil, 5},
		},
		{
			Name:     "lead",
			Agg:      NewLead(expression.NewGetField(1, sql.LongText, "x", true), nil, 2),
			Expected: sql.Row{3, 4, nil, nil, 3, 4, nil, nil, 3, 4, 5, 6, nil, nil},
		},
		{
			Name: "lead w/ default",
			Agg: NewLead(
				expression.NewGetField(1, sql.LongText, "x", true),
				expression.NewGetField(1, sql.LongText, "x", true),
				2,
			),
			Expected: sql.Row{3, 4, 3, 4, 3, 4, 3, 4, 3, 4, 5, 6, 5, 6},
		},
		{
			Name:     "row number",
			Agg:      NewRowNumber(),
			Expected: sql.Row{1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4, 5, 6},
		},
		{
			Name: "percent rank no peers",
			Agg:  NewPercentRank([]sql.Expression{}),
			Expected: sql.Row{
				float64(0), float64(0), float64(0), float64(0),
				float64(0), float64(0), float64(0), float64(0),
				float64(0), float64(0), float64(0), float64(0), float64(0), float64(0),
			},
		},
		{
			Name: "percent rank peer groups",
			Agg:  NewPercentRank([]sql.Expression{expression.NewGetField(5, sql.LongText, "x", true)}),
			Expected: sql.Row{
				float64(0), float64(0) / float64(3), float64(2) / float64(3), float64(3) / float64(3),
				float64(0), float64(1) / float64(3), float64(1) / float64(3), float64(3) / float64(3),
				float64(0), float64(1) / float64(5), float64(1) / float64(5), float64(3) / float64(5), float64(3) / float64(5), float64(1),
			},
		},
	}

	buf := []sql.Row{
		{1, 1, int64(1), float64(1), 1, 1},
		{nil, 2, int64(2), float64(2), 1, 1},
		{3, 3, int64(3), float64(3), 1, 2},
		{4, 4, int64(2), float64(4), 1, 3},
		{1, 1, int64(1), float64(1), 2, 1},
		{nil, 2, int64(2), float64(2), 2, 2},
		{3, 3, int64(3), float64(3), 2, 2},
		{4, 4, int64(2), float64(4), 2, 3},
		{1, 1, int64(1), float64(1), 3, 3},
		{2, 2, int64(2), float64(2), 3, 4},
		{nil, 3, int64(3), float64(3), 3, 4},
		{nil, 4, int64(4), float64(4), 3, 5},
		{5, 5, int64(5), float64(5), 3, 5},
		{6, 6, int64(2), float64(6), 3, 6},
	}

	partitions := []sql.WindowInterval{
		{Start: 0, End: 4},
		{Start: 4, End: 8},
		{Start: 8, End: 14},
	}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			ctx := sql.NewEmptyContext()
			res := make(sql.Row, len(buf))
			i := 0
			for _, p := range partitions {
				err := tt.Agg.StartPartition(ctx, p, buf)
				require.NoError(t, err)
				var framer sql.WindowFramer = NewUnboundedPrecedingToCurrentRowFramer()
				framer, err = tt.Agg.DefaultFramer().NewFramer(p)
				require.NoError(t, err)
				for {
					interval, err := framer.Next(ctx, buf)
					if errors.Is(err, io.EOF) {
						break
					}
					res[i] = tt.Agg.Compute(ctx, interval, buf)
					i++
				}
			}
			require.Equal(t, tt.Expected, res)
		})
	}

}

func mustNewGroupByConcat(distinct string, orderBy sql.SortFields, separator string, selectExprs []sql.Expression, maxLen int) *GroupConcat {
	gc, err := NewGroupConcat(distinct, orderBy, separator, selectExprs, maxLen)
	if err != nil {
		panic(err)
	}
	return gc
}
