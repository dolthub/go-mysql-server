// Copyright 2026 Dolthub, Inc.
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

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// filterTestRows have a nullable value in the first column, and a nullable FILTER condition in the second.
var filterTestRows = []sql.Row{
	{int64(1), true},
	{nil, true},
	{int64(3), false},
	{int64(4), nil},
	{int64(5), true},
}

func filterTestValue() sql.Expression {
	return expression.NewGetField(0, types.Int64, "x", true)
}

func filterTestCondition() sql.Expression {
	return expression.NewGetField(1, types.Boolean, "keep", true)
}

func requireFilterResult(t *testing.T, ctx *sql.Context, expected, actual interface{}, msgAndArgs ...interface{}) {
	// compare JSON results by their string form, since JSON numbers are not comparable by type in all cases.
	if expectedJSON, ok := expected.(sql.JSONWrapper); ok {
		require.IsType(t, expectedJSON, actual, msgAndArgs...)
		expectedString, err := types.JsonToMySqlString(ctx, expectedJSON)
		require.NoError(t, err)
		actualString, err := types.JsonToMySqlString(ctx, actual.(sql.JSONWrapper))
		require.NoError(t, err)
		require.Equal(t, expectedString, actualString, msgAndArgs...)
		return
	}
	require.Equal(t, expected, actual, msgAndArgs...)
}

func TestFilterBuffer(t *testing.T) {
	ctx := sql.NewEmptyContext()
	tests := []struct {
		name     string
		agg      sql.Aggregation
		expected interface{}
	}{
		{
			name:     "count",
			agg:      NewCount(filterTestValue()),
			expected: int64(2),
		},
		{
			name:     "sum",
			agg:      NewSum(filterTestValue()),
			expected: float64(6),
		},
		{
			// JSON_ARRAYAGG keeps NULL inputs, so only rows that do not match the condition may be dropped.
			name:     "json_arrayagg",
			agg:      NewJsonArray(filterTestValue()),
			expected: types.MustJSON(`[1, null, 5]`),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			buf, err := NewFilter(test.agg, filterTestCondition()).NewBuffer(ctx)
			require.NoError(t, err)
			for _, row := range filterTestRows {
				require.NoError(t, buf.Update(ctx, row))
			}

			actual, err := buf.Eval(ctx)
			require.NoError(t, err)
			requireFilterResult(t, ctx, test.expected, actual)
		})
	}

	t.Run("no matching rows", func(t *testing.T) {
		buf, err := NewFilter(NewCount(filterTestValue()), expression.NewLiteral(false, types.Boolean)).NewBuffer(ctx)
		require.NoError(t, err)
		for _, row := range filterTestRows {
			require.NoError(t, buf.Update(ctx, row))
		}

		actual, err := buf.Eval(ctx)
		require.NoError(t, err)
		require.Equal(t, int64(0), actual)
	})
}

func TestFilterWindowFunction(t *testing.T) {
	ctx := sql.NewEmptyContext()
	// The partition starts after a row of another partition, so frames are not relative to the buffer's start.
	buf := append(sql.WindowBuffer{{int64(100), true}}, filterTestRows...)
	partition := sql.WindowInterval{Start: 1, End: len(buf)}

	tests := []struct {
		name     string
		agg      sql.Aggregation
		frames   []sql.WindowInterval
		expected []interface{}
	}{
		{
			name: "count",
			agg:  NewCount(filterTestValue()),
			frames: []sql.WindowInterval{
				{Start: 1, End: 6},
				{Start: 1, End: 3},
				{Start: 3, End: 5},
				{Start: 4, End: 6},
				{Start: 2, End: 2},
			},
			expected: []interface{}{int64(2), int64(1), int64(0), int64(1), int64(0)},
		},
		{
			name: "sum",
			agg:  NewSum(filterTestValue()),
			frames: []sql.WindowInterval{
				{Start: 1, End: 6},
				{Start: 1, End: 2},
				{Start: 3, End: 5},
				{Start: 2, End: 6},
			},
			expected: []interface{}{float64(6), float64(1), nil, float64(5)},
		},
		{
			name: "json_arrayagg",
			agg:  NewJsonArray(filterTestValue()),
			frames: []sql.WindowInterval{
				{Start: 1, End: 6},
				{Start: 2, End: 5},
			},
			expected: []interface{}{types.MustJSON(`[1, null, 5]`), types.MustJSON(`[null]`)},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			window := sql.NewWindowDefinition(nil, nil, nil, "", "")
			fn, err := NewFilter(test.agg, filterTestCondition()).WithWindow(ctx, window).NewWindowFunction(ctx)
			require.NoError(t, err)
			require.NoError(t, fn.StartPartition(ctx, partition, buf))
			for i, frame := range test.frames {
				actual, err := fn.Compute(ctx, frame, buf)
				require.NoError(t, err)
				requireFilterResult(t, ctx, test.expected[i], actual, "frame %v", frame)
			}
		})
	}
}

func TestFilterExpression(t *testing.T) {
	ctx := sql.NewEmptyContext()
	cond := filterTestCondition()

	count := NewFilter(NewCount(filterTestValue()), cond)
	_, ordered := count.(sql.OrderedAggregation)
	require.False(t, ordered)
	require.Equal(t, "COUNT(x) FILTER (WHERE keep)", count.String())

	groupConcat := NewFilter(NewGroupConcat("", nil, ",", []sql.Expression{filterTestValue()}, 1024), cond)
	_, ordered = groupConcat.(sql.OrderedAggregation)
	require.True(t, ordered)

	withId := groupConcat.WithId(7).(*orderedFilter)
	require.Equal(t, sql.ColumnId(7), withId.Id())
	require.Equal(t, cond, withId.Condition())

	children := groupConcat.Children()
	require.Len(t, children, 2)
	rebuilt, err := groupConcat.WithChildren(ctx, children...)
	require.NoError(t, err)
	require.Equal(t, groupConcat.String(), rebuilt.String())
	_, ordered = rebuilt.(sql.OrderedAggregation)
	require.True(t, ordered)
}
