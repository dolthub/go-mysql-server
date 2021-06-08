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

package aggregation

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

func TestAvg_String(t *testing.T) {
	require := require.New(t)

	avg := NewAvg(sql.NewEmptyContext(), expression.NewGetField(0, sql.Int32, "col1", true))
	require.Equal("AVG(col1)", avg.String())
}

func TestAvg_Float64(t *testing.T) {
	require := require.New(t)
	ctx := sql.NewEmptyContext()

	avg := NewAvg(sql.NewEmptyContext(), expression.NewGetField(0, sql.Float64, "col1", true))
	buffer := avg.NewBuffer()
	avg.Update(ctx, buffer, sql.NewRow(float64(23.2220000)))

	require.Equal(float64(23.222), eval(t, avg, buffer))
}

func TestAvg_Eval_INT32(t *testing.T) {
	require := require.New(t)
	ctx := sql.NewEmptyContext()

	avgNode := NewAvg(sql.NewEmptyContext(), expression.NewGetField(0, sql.Int32, "col1", true))
	buffer := avgNode.NewBuffer()
	require.Equal(nil, eval(t, avgNode, buffer))

	avgNode.Update(ctx, buffer, sql.NewRow(int32(1)))
	require.Equal(float64(1), eval(t, avgNode, buffer))

	avgNode.Update(ctx, buffer, sql.NewRow(int32(2)))
	require.Equal(float64(1.5), eval(t, avgNode, buffer))
}

func TestAvg_Eval_UINT64(t *testing.T) {
	require := require.New(t)
	ctx := sql.NewEmptyContext()

	avgNode := NewAvg(sql.NewEmptyContext(), expression.NewGetField(0, sql.Uint64, "col1", true))
	buffer := avgNode.NewBuffer()
	require.Equal(nil, eval(t, avgNode, buffer))

	err := avgNode.Update(ctx, buffer, sql.NewRow(uint64(1)))
	require.NoError(err)
	require.Equal(float64(1), eval(t, avgNode, buffer))

	err = avgNode.Update(ctx, buffer, sql.NewRow(uint64(2)))
	require.NoError(err)
	require.Equal(float64(1.5), eval(t, avgNode, buffer))
}

func TestAvg_Eval_String(t *testing.T) {
	require := require.New(t)
	ctx := sql.NewEmptyContext()

	avgNode := NewAvg(sql.NewEmptyContext(), expression.NewGetField(0, sql.Text, "col1", true))
	buffer := avgNode.NewBuffer()
	require.Equal(nil, eval(t, avgNode, buffer))

	err := avgNode.Update(ctx, buffer, sql.NewRow("foo"))
	require.NoError(err)
	require.Equal(float64(0), eval(t, avgNode, buffer))

	err = avgNode.Update(ctx, buffer, sql.NewRow("2"))
	require.NoError(err)
	require.Equal(float64(1), eval(t, avgNode, buffer))
}

func TestAvg_Merge(t *testing.T) {
	require := require.New(t)
	ctx := sql.NewEmptyContext()

	avgNode := NewAvg(sql.NewEmptyContext(), expression.NewGetField(0, sql.Float32, "col1", true))
	require.NotNil(avgNode)

	buffer1 := avgNode.NewBuffer()
	require.Zero(avgNode.Eval(ctx, buffer1))
	err := avgNode.Update(ctx, buffer1, sql.NewRow(float32(1)))
	require.NoError(err)
	err = avgNode.Update(ctx, buffer1, sql.NewRow(float32(4)))
	require.NoError(err)
	require.Equal(float64(2.5), eval(t, avgNode, buffer1))

	buffer2 := avgNode.NewBuffer()
	require.Zero(avgNode.Eval(ctx, buffer2))
	err = avgNode.Update(ctx, buffer2, sql.NewRow(float32(2)))
	require.NoError(err)
	err = avgNode.Update(ctx, buffer2, sql.NewRow(float32(7)))
	require.NoError(err)
	err = avgNode.Update(ctx, buffer2, sql.NewRow(float32(12)))
	require.NoError(err)
	require.Equal(float64(7), eval(t, avgNode, buffer2))

	err = avgNode.Merge(ctx, buffer1, buffer2)
	require.NoError(err)
	require.Equal(float64(5.2), eval(t, avgNode, buffer1))
}

func TestAvg_NULL(t *testing.T) {
	require := require.New(t)
	ctx := sql.NewEmptyContext()

	avgNode := NewAvg(sql.NewEmptyContext(), expression.NewGetField(0, sql.Uint64, "col1", true))
	buffer := avgNode.NewBuffer()
	require.Zero(avgNode.Eval(ctx, buffer))

	err := avgNode.Update(ctx, buffer, sql.NewRow(nil))
	require.NoError(err)
	require.Equal(nil, eval(t, avgNode, buffer))
}

func TestAvg_NUMS_AND_NULLS(t *testing.T) {
	require := require.New(t)
	ctx := sql.NewEmptyContext()

	avgNode := NewAvg(sql.NewEmptyContext(), expression.NewGetField(0, sql.Uint64, "col1", true))

	testCases := []struct {
		name     string
		rows     []sql.Row
		expected interface{}
	}{
		{
			"float values with nil",
			[]sql.Row{{2.0}, {2.0}, {3.}, {4.}, {nil}},
			float64(2.75),
		},
		{
			"float values with nil",
			[]sql.Row{{1}, {2}, {3}, {nil}, {nil}},
			float64(2.0),
		},
		{
			"no rows",
			[]sql.Row{},
			nil,
		},
		{
			"nil values",
			[]sql.Row{{nil}, {nil}},
			nil,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			buf := avgNode.NewBuffer()
			for _, row := range tt.rows {
				require.NoError(avgNode.Update(ctx, buf, row))
			}

			result, err := avgNode.Eval(ctx, buf)
			require.NoError(err)
			require.Equal(tt.expected, result)
		})
	}
}

func TestAvg_Distinct(t *testing.T) {
	require := require.New(t)
	ctx := sql.NewEmptyContext()

	ad := expression.NewDistinctExpression(expression.NewGetField(0, nil, "myfield", false))
	avg := NewAvg(sql.NewEmptyContext(), ad)

	// first validate that the expression's name is correct
	require.Equal("AVG(DISTINCT myfield)", avg.String())

	testCases := []struct {
		name     string
		rows     []sql.Row
		expected interface{}
	}{
		{
			"string int values",
			[]sql.Row{{"1"}, {"1"}, {"2"}, {"2"}, {"3"}, {"3"}, {"4"}, {"4"}},
			float64(2.5),
		},
		{
			"string float values",
			[]sql.Row{{"2.0"}, {"2.0"}, {"3.0"}, {"4.0"}, {"4.0"}},
			float64(3.0),
		},
		{
			"string float values",
			[]sql.Row{{"2.0"}, {"2.0"}, {"3.0"}, {"4.0"}, {"4.0"}},
			float64(3.0),
		},
		{
			"float values",
			[]sql.Row{{2.0}, {2.0}, {3.}, {4.}},
			float64(3.0),
		},
		{
			"float values with nil",
			[]sql.Row{{2.0}, {2.0}, {3.}, {4.}, {nil}},
			float64(3.0),
		},
		{
			"no rows",
			[]sql.Row{},
			nil,
		},
		{
			"nil values",
			[]sql.Row{{nil}, {nil}},
			nil,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			ad.Dispose()

			buf := avg.NewBuffer()
			for _, row := range tt.rows {
				require.NoError(avg.Update(ctx, buf, row))
			}

			result, err := avg.Eval(ctx, buf)
			require.NoError(err)
			require.Equal(tt.expected, result)
		})
	}
}
