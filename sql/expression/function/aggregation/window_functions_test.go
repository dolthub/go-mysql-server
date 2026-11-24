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
	"github.com/dolthub/go-mysql-server/sql/types"
)

// windowCountingExpr counts evaluations while returning the selected row value.
type windowCountingExpr struct {
	sql.Expression
	count *int
}

// Eval implements sql.Expression and records each underlying evaluation.
func (e *windowCountingExpr) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	*e.count++
	return e.Expression.Eval(ctx, row)
}

// TestFirstAndLastValueCacheSourceRows verifies that source-row values are evaluated once per partition.
func TestFirstAndLastValueCacheSourceRows(t *testing.T) {
	ctx := sql.NewEmptyContext()
	buffer := sql.WindowBuffer{{nil}, {1}, {2}}

	firstCount := 0
	first := NewFirstAgg(&windowCountingExpr{
		Expression: expression.NewGetField(0, types.Int64, "value", true),
		count:      &firstCount,
	})
	require.NoError(t, first.StartPartition(ctx, sql.WindowInterval{Start: 0, End: 3}, buffer))
	for _, test := range []struct {
		interval sql.WindowInterval
		expected interface{}
	}{
		{sql.WindowInterval{Start: 0, End: 1}, nil},
		{sql.WindowInterval{Start: 0, End: 2}, nil},
		{sql.WindowInterval{Start: 1, End: 2}, 1},
		{sql.WindowInterval{Start: 1, End: 3}, 1},
	} {
		actual, err := first.Compute(ctx, test.interval, buffer)
		require.NoError(t, err)
		require.Equal(t, test.expected, actual)
	}
	require.Equal(t, 2, firstCount)
	require.NoError(t, first.StartPartition(ctx, sql.WindowInterval{Start: 0, End: 3}, buffer))
	_, err := first.Compute(ctx, sql.WindowInterval{Start: 0, End: 1}, buffer)
	require.NoError(t, err)
	require.Equal(t, 3, firstCount)

	lastCount := 0
	last := NewLastAgg(&windowCountingExpr{
		Expression: expression.NewGetField(0, types.Int64, "value", true),
		count:      &lastCount,
	})
	require.NoError(t, last.StartPartition(ctx, sql.WindowInterval{Start: 0, End: 3}, buffer))
	for _, test := range []struct {
		interval sql.WindowInterval
		expected interface{}
	}{
		{sql.WindowInterval{Start: 0, End: 3}, 2},
		{sql.WindowInterval{Start: 1, End: 3}, 2},
		{sql.WindowInterval{Start: 0, End: 2}, 1},
		{sql.WindowInterval{Start: 1, End: 2}, 1},
		{sql.WindowInterval{Start: 0, End: 1}, nil},
		{sql.WindowInterval{Start: 0, End: 1}, nil},
	} {
		actual, err := last.Compute(ctx, test.interval, buffer)
		require.NoError(t, err)
		require.Equal(t, test.expected, actual)
	}
	require.Equal(t, 3, lastCount)
	require.NoError(t, last.StartPartition(ctx, sql.WindowInterval{Start: 0, End: 3}, buffer))
	_, err = last.Compute(ctx, sql.WindowInterval{Start: 0, End: 3}, buffer)
	require.NoError(t, err)
	require.Equal(t, 4, lastCount)
}

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
			Agg:      NewCountAgg(expression.NewGetField(1, types.LongText, "x", true)),
			Expected: sql.Row{int64(4), int64(4), int64(6)},
		},
		{
			Name:     "count with nulls",
			Agg:      NewCountAgg(expression.NewGetField(0, types.LongText, "x", true)),
			Expected: sql.Row{int64(3), int64(3), int64(4)},
		},
		{
			Name:     "max ints",
			Agg:      NewMaxAgg(expression.NewGetField(1, types.LongText, "x", true)),
			Expected: sql.Row{4, 4, 6},
		},
		{
			Name:     "max int64",
			Agg:      NewMaxAgg(expression.NewGetField(2, types.LongText, "x", true)),
			Expected: sql.Row{int64(3), int64(3), int64(5)},
		},
		{
			Name:     "max w/ nulls",
			Agg:      NewMaxAgg(expression.NewGetField(0, types.LongText, "x", true)),
			Expected: sql.Row{4, 4, 6},
		},
		{
			Name:     "max w/ float",
			Agg:      NewMaxAgg(expression.NewGetField(3, types.LongText, "x", true)),
			Expected: sql.Row{float64(4), float64(4), float64(6)},
		},
		{
			Name:     "min ints",
			Agg:      NewMinAgg(expression.NewGetField(1, types.LongText, "x", true)),
			Expected: sql.Row{1, 1, 1},
		},
		{
			Name:     "min int64",
			Agg:      NewMinAgg(expression.NewGetField(2, types.LongText, "x", true)),
			Expected: sql.Row{int64(1), int64(1), int64(1)},
		},
		{
			Name:     "min w/ nulls",
			Agg:      NewMinAgg(expression.NewGetField(0, types.LongText, "x", true)),
			Expected: sql.Row{1, 1, 1},
		},
		{
			Name:     "min w/ float",
			Agg:      NewMinAgg(expression.NewGetField(3, types.LongText, "x", true)),
			Expected: sql.Row{float64(1), float64(1), float64(1)},
		},
		{
			Name:     "avg nulls",
			Agg:      NewAvgAgg(expression.NewGetField(0, types.LongText, "x", true)),
			Expected: sql.Row{float64(8) / float64(3), float64(8) / float64(3), float64(14) / float64(4)},
		},
		{
			Name:     "avg int",
			Agg:      NewAvgAgg(expression.NewGetField(1, types.LongText, "x", true)),
			Expected: sql.Row{float64(10) / float64(4), float64(10) / float64(4), float64(21) / float64(6)},
		},
		{
			Name:     "avg int64",
			Agg:      NewAvgAgg(expression.NewGetField(2, types.LongText, "x", true)),
			Expected: sql.Row{float64(8) / float64(4), float64(8) / float64(4), float64(17) / float64(6)},
		},
		{
			Name:     "avg float",
			Agg:      NewAvgAgg(expression.NewGetField(3, types.LongText, "x", true)),
			Expected: sql.Row{float64(10) / float64(4), float64(10) / float64(4), float64(21) / float64(6)},
		},
		{
			Name:     "sum nulls",
			Agg:      NewSumAgg(expression.NewGetField(0, types.LongText, "x", true)),
			Expected: sql.Row{float64(8), float64(8), float64(14)},
		},
		{
			Name:     "sum ints",
			Agg:      NewSumAgg(expression.NewGetField(1, types.LongText, "x", true)),
			Expected: sql.Row{float64(10), float64(10), float64(21)},
		},
		{
			Name:     "sum int64",
			Agg:      NewSumAgg(expression.NewGetField(2, types.LongText, "x", true)),
			Expected: sql.Row{float64(8), float64(8), float64(17)},
		},
		{
			Name:     "sum float64",
			Agg:      NewSumAgg(expression.NewGetField(3, types.LongText, "x", true)),
			Expected: sql.Row{float64(10), float64(10), float64(21)},
		},
		{
			Name:     "first",
			Agg:      NewFirstAgg(expression.NewGetField(0, types.LongText, "x", true)),
			Expected: sql.Row{1, 1, 1},
		},
		{
			Name:     "last",
			Agg:      NewLastAgg(expression.NewGetField(0, types.LongText, "x", true)),
			Expected: sql.Row{4, 4, 6},
		},
		// list aggregations
		{
			Name:     "group concat null",
			Agg:      NewGroupConcatAgg(NewGroupConcat("", nil, ",", []sql.Expression{expression.NewGetField(0, types.LongText, "x", true)}, 1042)),
			Expected: sql.Row{"1,3,4", "1,3,4", "1,2,5,6"},
		},
		{
			Name:     "group concat int",
			Agg:      NewGroupConcatAgg(NewGroupConcat("", nil, ",", []sql.Expression{expression.NewGetField(1, types.LongText, "x", true)}, 1042)),
			Expected: sql.Row{"1,2,3,4", "1,2,3,4", "1,2,3,4,5,6"},
		},
		{
			Name:     "group concat float",
			Agg:      NewGroupConcatAgg(NewGroupConcat("", nil, ",", []sql.Expression{expression.NewGetField(3, types.LongText, "x", true)}, 1042)),
			Expected: sql.Row{"1,2,3,4", "1,2,3,4", "1,2,3,4,5,6"},
		},
		{
			Name: "group concat multiple expressions",
			Agg: NewGroupConcatAgg(NewGroupConcat("", nil, ",", []sql.Expression{
				expression.NewGetField(0, types.LongText, "x", true),
				expression.NewLiteral("-", types.LongText),
				expression.NewGetField(1, types.LongText, "y", true),
			}, 1042)),
			Expected: sql.Row{"1-1,3-3,4-4", "1-1,3-3,4-4", "1-1,2-2,5-5,6-6"},
		},
		{
			Name: "group concat multiple expressions with later null",
			Agg: NewGroupConcatAgg(NewGroupConcat("", nil, ",", []sql.Expression{
				expression.NewGetField(1, types.LongText, "x", true),
				expression.NewLiteral("-", types.LongText),
				expression.NewGetField(0, types.LongText, "y", true),
			}, 1042)),
			Expected: sql.Row{"1-1,3-3,4-4", "1-1,3-3,4-4", "1-1,2-2,5-5,6-6"},
		},
		{
			Name: "json array null",
			Agg:  NewJsonArrayAgg(expression.NewGetField(0, types.LongText, "x", true)),
			Expected: sql.Row{
				types.JSONDocument{Val: []interface{}{1, nil, 3, 4}},
				types.JSONDocument{Val: []interface{}{1, nil, 3, 4}},
				types.JSONDocument{Val: []interface{}{1, 2, nil, nil, 5, 6}},
			},
		},
		{
			Name: "json array int",
			Agg:  NewJsonArrayAgg(expression.NewGetField(1, types.LongText, "x", true)),
			Expected: sql.Row{
				types.JSONDocument{Val: []interface{}{1, 2, 3, 4}},
				types.JSONDocument{Val: []interface{}{1, 2, 3, 4}},
				types.JSONDocument{Val: []interface{}{1, 2, 3, 4, 5, 6}},
			},
		},
		{
			Name: "json array float",
			Agg:  NewJsonArrayAgg(expression.NewGetField(3, types.LongText, "x", true)),
			Expected: sql.Row{
				types.JSONDocument{Val: []interface{}{float64(1), float64(2), float64(3), float64(4)}},
				types.JSONDocument{Val: []interface{}{float64(1), float64(2), float64(3), float64(4)}},
				types.JSONDocument{Val: []interface{}{float64(1), float64(2), float64(3), float64(4), float64(5), float64(6)}},
			},
		},
		{
			Name: "json object null",
			Agg: NewWindowedJSONObjectAgg(
				NewJSONObjectAgg(
					sql.NewEmptyContext(),
					expression.NewGetField(1, types.LongText, "x", true),
					expression.NewGetField(0, types.LongText, "y", true),
				).(*JSONObjectAgg),
			),
			Expected: sql.Row{
				types.JSONDocument{Val: map[string]interface{}{"1": 1, "2": nil, "3": 3, "4": 4}},
				types.JSONDocument{Val: map[string]interface{}{"1": 1, "2": nil, "3": 3, "4": 4}},
				types.JSONDocument{Val: map[string]interface{}{"1": 1, "2": 2, "3": nil, "4": nil, "5": 5, "6": 6}},
			},
		},
		{
			Name: "json object int",
			Agg: NewWindowedJSONObjectAgg(
				NewJSONObjectAgg(
					sql.NewEmptyContext(),
					expression.NewGetField(1, types.LongText, "x", true),
					expression.NewGetField(0, types.LongText, "x", true),
				).(*JSONObjectAgg),
			),
			Expected: sql.Row{
				types.JSONDocument{Val: map[string]interface{}{"1": 1, "2": nil, "3": 3, "4": 4}},
				types.JSONDocument{Val: map[string]interface{}{"1": 1, "2": nil, "3": 3, "4": 4}},
				types.JSONDocument{Val: map[string]interface{}{"1": 1, "2": 2, "3": nil, "4": nil, "5": 5, "6": 6}},
			},
		},
		{
			Name: "json object float",
			Agg: NewWindowedJSONObjectAgg(
				NewJSONObjectAgg(
					sql.NewEmptyContext(),
					expression.NewGetField(1, types.LongText, "x", true),
					expression.NewGetField(3, types.LongText, "x", true),
				).(*JSONObjectAgg),
			),
			Expected: sql.Row{
				types.JSONDocument{Val: map[string]interface{}{"1": float64(1), "2": float64(2), "3": float64(3), "4": float64(4)}},
				types.JSONDocument{Val: map[string]interface{}{"1": float64(1), "2": float64(2), "3": float64(3), "4": float64(4)}},
				types.JSONDocument{Val: map[string]interface{}{"1": float64(1), "2": float64(2), "3": float64(3), "4": float64(4), "5": float64(5), "6": float64(6)}},
			},
		},
		{
			Name: "json object float",
			Agg: NewWindowedJSONObjectAgg(
				NewJSONObjectAgg(
					sql.NewEmptyContext(),
					expression.NewGetField(1, types.LongText, "x", true),
					expression.NewGetField(3, types.LongText, "x", true),
				).(*JSONObjectAgg),
			),
			Expected: sql.Row{
				types.JSONDocument{Val: map[string]interface{}{"1": float64(1), "2": float64(2), "3": float64(3), "4": float64(4)}},
				types.JSONDocument{Val: map[string]interface{}{"1": float64(1), "2": float64(2), "3": float64(3), "4": float64(4)}},
				types.JSONDocument{Val: map[string]interface{}{"1": float64(1), "2": float64(2), "3": float64(3), "4": float64(4), "5": float64(5), "6": float64(6)}},
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
				res[i], err = tt.Agg.Compute(ctx, p, buf)
				require.NoError(t, err)
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
			Agg:      NewLag(expression.NewGetField(1, types.LongText, "x", true), nil, 2),
			Expected: sql.Row{nil, nil, 1, 2, nil, nil, 1, 2, nil, nil, 1, 2, 3, 4},
		},
		{
			Name: "lag w/ default",
			Agg: NewLag(
				expression.NewGetField(1, types.LongText, "x", true),
				expression.NewGetField(1, types.LongText, "x", true),
				2,
			),
			Expected: sql.Row{1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 3, 4},
		},
		{
			Name: "lag nil",
			Agg: NewLag(
				expression.NewGetField(0, types.LongText, "x", true),
				nil,
				1,
			),
			Expected: sql.Row{nil, 1, nil, 3, nil, 1, nil, 3, nil, 1, 2, nil, nil, 5},
		},
		{
			Name:     "lead",
			Agg:      NewLead(expression.NewGetField(1, types.LongText, "x", true), nil, 2),
			Expected: sql.Row{3, 4, nil, nil, 3, 4, nil, nil, 3, 4, 5, 6, nil, nil},
		},
		{
			Name: "lead w/ default",
			Agg: NewLead(
				expression.NewGetField(1, types.LongText, "x", true),
				expression.NewGetField(1, types.LongText, "x", true),
				2,
			),
			Expected: sql.Row{3, 4, 3, 4, 3, 4, 3, 4, 3, 4, 5, 6, 5, 6},
		},
		{
			Name:     "row number",
			Agg:      NewRowNumber(),
			Expected: sql.Row{int64(1), int64(2), int64(3), int64(4), int64(1), int64(2), int64(3), int64(4), int64(1), int64(2), int64(3), int64(4), int64(5), int64(6)},
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
			Agg:  NewPercentRank([]sql.Expression{expression.NewGetField(5, types.LongText, "x", true)}),
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
					res[i], err = tt.Agg.Compute(ctx, interval, buf)
					require.NoError(t, err)
					i++
				}
			}
			require.Equal(t, tt.Expected, res)
		})
	}
}

// TestWindowedAggFuncsWithExclusion checks that frame-respecting window functions skip the rows removed from their
// frames by an EXCLUDE clause. Expected values are from Postgres, for the frame
// (PARTITION BY grp ORDER BY k ROWS BETWEEN 1 PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE ...).
func TestWindowedAggFuncsWithExclusion(t *testing.T) {
	x := expression.NewGetField(2, types.Int64, "x", true)
	tests := []struct {
		Name      string
		Agg       sql.WindowFunction
		Exclusion sql.WindowFrameExclusion
		Expected  sql.Row
	}{
		{
			Name:      "sum exclude current row",
			Agg:       NewSumAgg(x),
			Exclusion: sql.WindowFrameExcludeCurrentRow,
			Expected:  sql.Row{float64(100), float64(90), float64(70), float64(80), nil, float64(27), float64(25), float64(18), float64(9)},
		},
		{
			Name:      "count exclude current row",
			Agg:       NewCountAgg(x),
			Exclusion: sql.WindowFrameExcludeCurrentRow,
			Expected:  sql.Row{int64(3), int64(3), int64(2), int64(2), int64(0), int64(3), int64(3), int64(2), int64(1)},
		},
		{
			Name:      "avg exclude current row",
			Agg:       NewAvgAgg(x),
			Exclusion: sql.WindowFrameExcludeCurrentRow,
			Expected:  sql.Row{float64(33.333333333333336), float64(30), float64(35), float64(40), nil, float64(9), float64(8.333333333333334), float64(9), float64(9)},
		},
		{
			Name:      "min exclude current row",
			Agg:       NewMinAgg(x),
			Exclusion: sql.WindowFrameExcludeCurrentRow,
			Expected:  sql.Row{int64(20), int64(10), int64(20), int64(30), nil, int64(7), int64(5), int64(7), int64(9)},
		},
		{
			Name:      "max exclude current row",
			Agg:       NewMaxAgg(x),
			Exclusion: sql.WindowFrameExcludeCurrentRow,
			Expected:  sql.Row{int64(50), int64(50), int64(50), int64(50), nil, int64(11), int64(11), int64(11), int64(9)},
		},
		{
			Name:      "first_value exclude current row",
			Agg:       NewFirstAgg(x),
			Exclusion: sql.WindowFrameExcludeCurrentRow,
			Expected:  sql.Row{int64(20), int64(10), int64(20), int64(30), nil, int64(7), int64(5), int64(7), int64(9)},
		},
		{
			Name:      "last_value exclude current row",
			Agg:       NewLastAgg(x),
			Exclusion: sql.WindowFrameExcludeCurrentRow,
			Expected:  sql.Row{int64(50), int64(50), int64(50), int64(50), nil, int64(11), int64(11), int64(11), int64(9)},
		},
		{
			Name:      "stddev_samp exclude current row",
			Agg:       NewStdDevSampAgg(x),
			Exclusion: sql.WindowFrameExcludeCurrentRow,
			Expected:  sql.Row{float64(15.275252316519467), float64(20), float64(21.213203435596427), float64(14.142135623730951), nil, float64(2), float64(3.0550504633038935), float64(2.8284271247461903), nil},
		},
		{
			Name:      "var_pop exclude current row",
			Agg:       NewVarPopAgg(x),
			Exclusion: sql.WindowFrameExcludeCurrentRow,
			Expected:  sql.Row{float64(155.55555555555554), float64(266.6666666666667), float64(225), float64(100), nil, float64(2.6666666666666665), float64(6.222222222222222), float64(4), float64(0)},
		},
		{
			Name:      "sum exclude group",
			Agg:       NewSumAgg(x),
			Exclusion: sql.WindowFrameExcludeGroup,
			Expected:  sql.Row{float64(100), float64(60), float64(50), float64(80), nil, float64(11), float64(11), float64(11), float64(9)},
		},
		{
			Name:      "count exclude group",
			Agg:       NewCountAgg(x),
			Exclusion: sql.WindowFrameExcludeGroup,
			Expected:  sql.Row{int64(3), int64(2), int64(1), int64(2), int64(0), int64(1), int64(1), int64(1), int64(1)},
		},
		{
			Name:      "avg exclude group",
			Agg:       NewAvgAgg(x),
			Exclusion: sql.WindowFrameExcludeGroup,
			Expected:  sql.Row{float64(33.333333333333336), float64(30), float64(50), float64(40), nil, float64(11), float64(11), float64(11), float64(9)},
		},
		{
			Name:      "min exclude group",
			Agg:       NewMinAgg(x),
			Exclusion: sql.WindowFrameExcludeGroup,
			Expected:  sql.Row{int64(20), int64(10), int64(50), int64(30), nil, int64(11), int64(11), int64(11), int64(9)},
		},
		{
			Name:      "max exclude group",
			Agg:       NewMaxAgg(x),
			Exclusion: sql.WindowFrameExcludeGroup,
			Expected:  sql.Row{int64(50), int64(50), int64(50), int64(50), nil, int64(11), int64(11), int64(11), int64(9)},
		},
		{
			Name:      "first_value exclude group",
			Agg:       NewFirstAgg(x),
			Exclusion: sql.WindowFrameExcludeGroup,
			Expected:  sql.Row{int64(20), int64(10), nil, int64(30), nil, int64(11), int64(11), int64(11), int64(9)},
		},
		{
			Name:      "last_value exclude group",
			Agg:       NewLastAgg(x),
			Exclusion: sql.WindowFrameExcludeGroup,
			Expected:  sql.Row{int64(50), int64(50), int64(50), int64(50), nil, int64(11), int64(11), int64(11), int64(9)},
		},
		{
			Name:      "stddev_samp exclude group",
			Agg:       NewStdDevSampAgg(x),
			Exclusion: sql.WindowFrameExcludeGroup,
			Expected:  sql.Row{float64(15.275252316519467), float64(28.284271247461902), nil, float64(14.142135623730951), nil, nil, nil, nil, nil},
		},
		{
			Name:      "var_pop exclude group",
			Agg:       NewVarPopAgg(x),
			Exclusion: sql.WindowFrameExcludeGroup,
			Expected:  sql.Row{float64(155.55555555555554), float64(400), float64(0), float64(100), nil, float64(0), float64(0), float64(0), float64(0)},
		},
		{
			Name:      "sum exclude ties",
			Agg:       NewSumAgg(x),
			Exclusion: sql.WindowFrameExcludeTies,
			Expected:  sql.Row{float64(110), float64(80), float64(80), float64(80), float64(50), float64(16), float64(18), float64(20), float64(20)},
		},
		{
			Name:      "count exclude ties",
			Agg:       NewCountAgg(x),
			Exclusion: sql.WindowFrameExcludeTies,
			Expected:  sql.Row{int64(4), int64(3), int64(2), int64(2), int64(1), int64(2), int64(2), int64(2), int64(2)},
		},
		{
			Name:      "avg exclude ties",
			Agg:       NewAvgAgg(x),
			Exclusion: sql.WindowFrameExcludeTies,
			Expected:  sql.Row{float64(27.5), float64(26.666666666666668), float64(40), float64(40), float64(50), float64(8), float64(9), float64(10), float64(10)},
		},
		{
			Name:      "min exclude ties",
			Agg:       NewMinAgg(x),
			Exclusion: sql.WindowFrameExcludeTies,
			Expected:  sql.Row{int64(10), int64(10), int64(30), int64(30), int64(50), int64(5), int64(7), int64(9), int64(9)},
		},
		{
			Name:      "max exclude ties",
			Agg:       NewMaxAgg(x),
			Exclusion: sql.WindowFrameExcludeTies,
			Expected:  sql.Row{int64(50), int64(50), int64(50), int64(50), int64(50), int64(11), int64(11), int64(11), int64(11)},
		},
		{
			Name:      "first_value exclude ties",
			Agg:       NewFirstAgg(x),
			Exclusion: sql.WindowFrameExcludeTies,
			Expected:  sql.Row{int64(10), int64(10), int64(30), int64(30), nil, int64(5), int64(7), int64(9), int64(9)},
		},
		{
			Name:      "last_value exclude ties",
			Agg:       NewLastAgg(x),
			Exclusion: sql.WindowFrameExcludeTies,
			Expected:  sql.Row{int64(50), int64(50), int64(50), int64(50), int64(50), int64(11), int64(11), int64(11), int64(11)},
		},
		{
			Name:      "stddev_samp exclude ties",
			Agg:       NewStdDevSampAgg(x),
			Exclusion: sql.WindowFrameExcludeTies,
			Expected:  sql.Row{float64(17.07825127659933), float64(20.81665999466133), float64(14.142135623730951), float64(14.142135623730951), nil, float64(4.242640687119285), float64(2.8284271247461903), float64(1.414213562373095), float64(1.414213562373095)},
		},
		{
			Name:      "var_pop exclude ties",
			Agg:       NewVarPopAgg(x),
			Exclusion: sql.WindowFrameExcludeTies,
			Expected:  sql.Row{float64(218.75), float64(288.8888888888889), float64(100), float64(100), float64(0), float64(9), float64(4), float64(1), float64(1)},
		},
	}

	// columns are (grp, k, x), sorted by grp and k
	buf := []sql.Row{
		{1, 1, int64(10)},
		{1, 2, int64(20)},
		{1, 2, int64(30)},
		{1, 3, nil},
		{1, 5, int64(50)},
		{2, 1, int64(5)},
		{2, 1, int64(7)},
		{2, 1, int64(9)},
		{2, 4, int64(11)},
	}
	partitions := []sql.WindowInterval{
		{Start: 0, End: 5},
		{Start: 5, End: 9},
	}
	orderBy := []sql.Expression{expression.NewGetField(1, types.Int64, "k", false)}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			ctx := sql.NewEmptyContext()
			var framer sql.WindowFramer = NewExcludingFramer(&rowFramerBase{startNPreceding: 1, unboundedFollowing: true}, tt.Exclusion, orderBy)
			res := make(sql.Row, 0, len(buf))
			for _, p := range partitions {
				err := tt.Agg.StartPartition(ctx, p, buf)
				require.NoError(t, err)

				framer, err = framer.NewFramer(p)
				require.NoError(t, err)

				for {
					interval, err := framer.Next(ctx, buf)
					if errors.Is(err, io.EOF) {
						break
					}

					require.NoError(t, err)
					v, err := tt.Agg.Compute(ctx, interval, buf)
					require.NoError(t, err)
					res = append(res, v)
				}
			}

			require.Len(t, res, len(tt.Expected))
			for i, expected := range tt.Expected {
				if f, ok := expected.(float64); ok {
					require.InDelta(t, f, res[i], 1e-9, "row %d", i)
				} else {
					require.Equal(t, expected, res[i], "row %d", i)
				}
			}
		})
	}
}

func TestAvgAgg_NonNumericVarchar(t *testing.T) {
	ctx := sql.NewEmptyContext()
	buf := []sql.Row{{"aa"}, {nil}, {"10"}}
	p := sql.WindowInterval{Start: 0, End: len(buf)}

	avg := NewAvgAgg(expression.NewGetField(0, types.LongText, "x", true))
	require.NoError(t, avg.StartPartition(ctx, p, buf))
	res, err := avg.Compute(ctx, sql.WindowInterval{Start: 0, End: 1}, buf)
	require.NoError(t, err)
	require.Equal(t, float64(0), res)

	res, err = avg.Compute(ctx, sql.WindowInterval{Start: 1, End: 2}, buf)
	require.NoError(t, err)
	require.Nil(t, res)

	res, err = avg.Compute(ctx, sql.WindowInterval{Start: 0, End: 3}, buf)
	require.NoError(t, err)
	require.Equal(t, float64(5), res)
}
