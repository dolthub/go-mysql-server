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

	"github.com/gabereiser/go-mysql-server/sql"
	"github.com/gabereiser/go-mysql-server/sql/expression"
	"github.com/gabereiser/go-mysql-server/sql/types"
	_ "github.com/gabereiser/go-mysql-server/sql/variables"
)

func TestAvg_String(t *testing.T) {
	require := require.New(t)

	avg := NewAvg(expression.NewGetField(0, types.Int32, "col1", true))
	require.Equal("AVG(col1)", avg.String())
}

func TestAvg_Float64(t *testing.T) {
	require := require.New(t)
	ctx := sql.NewEmptyContext()

	avg := NewAvg(expression.NewGetField(0, types.Float64, "col1", true))
	buffer, _ := avg.NewBuffer()
	buffer.Update(ctx, sql.NewRow(float64(23.2220000)))

	require.Equal(float64(23.222), evalBuffer(t, buffer))
}

func TestAvg_Eval_INT32(t *testing.T) {
	require := require.New(t)
	ctx := sql.NewEmptyContext()

	avgNode := NewAvg(expression.NewGetField(0, types.Int32, "col1", true))
	buffer, _ := avgNode.NewBuffer()
	require.Equal(nil, evalBuffer(t, buffer))

	buffer.Update(ctx, sql.NewRow(int32(1)))
	require.Equal(float64(1), evalBuffer(t, buffer))

	buffer.Update(ctx, sql.NewRow(int32(2)))
	require.Equal(float64(1.5), evalBuffer(t, buffer))
}

func TestAvg_Eval_UINT64(t *testing.T) {
	require := require.New(t)
	ctx := sql.NewEmptyContext()

	avgNode := NewAvg(expression.NewGetField(0, types.Uint64, "col1", true))
	buffer, _ := avgNode.NewBuffer()
	require.Equal(nil, evalBuffer(t, buffer))

	err := buffer.Update(ctx, sql.NewRow(uint64(1)))
	require.NoError(err)
	require.Equal(float64(1), evalBuffer(t, buffer))

	err = buffer.Update(ctx, sql.NewRow(uint64(2)))
	require.NoError(err)
	require.Equal(float64(1.5), evalBuffer(t, buffer))
}

func TestAvg_Eval_String(t *testing.T) {
	require := require.New(t)
	ctx := sql.NewEmptyContext()

	avgNode := NewAvg(expression.NewGetField(0, types.Text, "col1", true))
	buffer, _ := avgNode.NewBuffer()
	require.Equal(nil, evalBuffer(t, buffer))

	err := buffer.Update(ctx, sql.NewRow("foo"))
	require.NoError(err)
	require.Equal(float64(0), evalBuffer(t, buffer))

	err = buffer.Update(ctx, sql.NewRow("2"))
	require.NoError(err)
	require.Equal(float64(1), evalBuffer(t, buffer))
}

func TestAvg_NULL(t *testing.T) {
	require := require.New(t)
	ctx := sql.NewEmptyContext()

	avgNode := NewAvg(expression.NewGetField(0, types.Uint64, "col1", true))
	buffer, _ := avgNode.NewBuffer()
	require.Zero(evalBuffer(t, buffer))

	err := buffer.Update(ctx, sql.NewRow(nil))
	require.NoError(err)
	require.Equal(nil, evalBuffer(t, buffer))
}

func TestAvg_NUMS_AND_NULLS(t *testing.T) {
	require := require.New(t)
	ctx := sql.NewEmptyContext()

	avgNode := NewAvg(expression.NewGetField(0, types.Uint64, "col1", true))

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
			buf, _ := avgNode.NewBuffer()
			for _, row := range tt.rows {
				require.NoError(buf.Update(ctx, row))
			}

			require.Equal(tt.expected, evalBuffer(t, buf))
		})
	}
}

func TestAvg_Distinct(t *testing.T) {
	require := require.New(t)
	ctx := sql.NewEmptyContext()

	ad := expression.NewDistinctExpression(expression.NewGetField(0, nil, "myfield", false))
	avg := NewAvg(ad)

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
			buf, _ := avg.NewBuffer()
			for _, row := range tt.rows {
				require.NoError(buf.Update(ctx, row))
			}

			require.Equal(tt.expected, evalBuffer(t, buf))
		})
	}
}
