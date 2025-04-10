// Copyright 2025 Dolthub, Inc.
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
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

func isFloatEqual(a, b float64) bool {
	return math.Abs(a-b) < 1e-9
}

func TestStd(t *testing.T) {
	sum := NewStdDevPop(expression.NewGetField(0, nil, "", false))

	testCases := []struct {
		name     string
		rows     []sql.Row
		expected interface{}
	}{
		{
			"string int values",
			[]sql.Row{{"1"}, {"2"}, {"3"}, {"4"}},
			1.118033988749895,
		},
		{
			"string float values",
			[]sql.Row{{"1.5"}, {"2"}, {"3"}, {"4"}},
			0.9601432184835761,
		},
		{
			"string non-int values",
			[]sql.Row{{"a"}, {"b"}, {"c"}, {"d"}},
			float64(0),
		},
		{
			"float values",
			[]sql.Row{{1.}, {2.5}, {3.}, {4.}},
			1.0825317547305484,
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
		{
			"int64 values",
			[]sql.Row{{int64(1)}, {int64(3)}},
			1.0,
		},
		{
			"int32 values",
			[]sql.Row{{int32(1)}, {int32(3)}},
			1.0,
		},
		{
			"int32 and nil values",
			[]sql.Row{{int32(1)}, {int32(3)}, {nil}},
			1.0,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)

			ctx := sql.NewEmptyContext()
			buf, _ := sum.NewBuffer()
			for _, row := range tt.rows {
				require.NoError(buf.Update(ctx, row))
			}

			result, err := buf.Eval(sql.NewEmptyContext())
			require.NoError(err)
			if tt.expected == nil {
				require.Equal(tt.expected, nil)
				return
			}
			require.True(isFloatEqual(tt.expected.(float64), result.(float64)))
		})
	}
}

func TestStdSamp(t *testing.T) {
	sum := NewStdDevSamp(expression.NewGetField(0, nil, "", false))

	testCases := []struct {
		name     string
		rows     []sql.Row
		expected interface{}
	}{
		{
			"string int values",
			[]sql.Row{{"1"}, {"2"}, {"3"}, {"4"}},
			1.2909944487358056,
		},
		{
			"string float values",
			[]sql.Row{{"1.5"}, {"2"}, {"3"}, {"4"}},
			1.1086778913041726,
		},
		{
			"string non-int values",
			[]sql.Row{{"a"}, {"b"}, {"c"}, {"d"}},
			float64(0),
		},
		{
			"float values",
			[]sql.Row{{1.}, {2.5}, {3.}, {4.}},
			1.25,
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
		{
			"int64 values",
			[]sql.Row{{int64(1)}, {int64(3)}},
			1.4142135623730951,
		},
		{
			"int32 values",
			[]sql.Row{{int32(1)}, {int32(3)}},
			1.4142135623730951,
		},
		{
			"int32 and nil values",
			[]sql.Row{{int32(1)}, {int32(3)}, {nil}},
			1.4142135623730951,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)

			ctx := sql.NewEmptyContext()
			buf, _ := sum.NewBuffer()
			for _, row := range tt.rows {
				require.NoError(buf.Update(ctx, row))
			}

			result, err := buf.Eval(sql.NewEmptyContext())
			require.NoError(err)
			if tt.expected == nil {
				require.Equal(tt.expected, nil)
				return
			}
			require.True(isFloatEqual(tt.expected.(float64), result.(float64)))
		})
	}
}

func TestVariance(t *testing.T) {
	sum := NewVarPop(expression.NewGetField(0, nil, "", false))

	testCases := []struct {
		name     string
		rows     []sql.Row
		expected interface{}
	}{
		{
			"string int values",
			[]sql.Row{{"1"}, {"2"}, {"3"}, {"4"}},
			1.25,
		},
		{
			"string float values",
			[]sql.Row{{"1.5"}, {"2"}, {"3"}, {"4"}},
			0.9218750000000001,
		},
		{
			"string non-int values",
			[]sql.Row{{"a"}, {"b"}, {"c"}, {"d"}},
			float64(0),
		},
		{
			"float values",
			[]sql.Row{{1.}, {2.5}, {3.}, {4.}},
			1.171875,
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
		{
			"int64 values",
			[]sql.Row{{int64(1)}, {int64(3)}},
			1.0,
		},
		{
			"int32 values",
			[]sql.Row{{int32(1)}, {int32(3)}},
			1.0,
		},
		{
			"int32 and nil values",
			[]sql.Row{{int32(1)}, {int32(3)}, {nil}},
			1.0,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)

			ctx := sql.NewEmptyContext()
			buf, _ := sum.NewBuffer()
			for _, row := range tt.rows {
				require.NoError(buf.Update(ctx, row))
			}

			result, err := buf.Eval(sql.NewEmptyContext())
			require.NoError(err)
			if tt.expected == nil {
				require.Equal(tt.expected, nil)
				return
			}
			require.True(isFloatEqual(tt.expected.(float64), result.(float64)))
		})
	}
}

func TestVarSamp(t *testing.T) {
	sum := NewVarSamp(expression.NewGetField(0, nil, "", false))

	testCases := []struct {
		name     string
		rows     []sql.Row
		expected interface{}
	}{
		{
			"string int values",
			[]sql.Row{{"1"}, {"2"}, {"3"}, {"4"}},
			1.6666666666666667,
		},
		{
			"string float values",
			[]sql.Row{{"1.5"}, {"2"}, {"3"}, {"4"}},
			1.2291666666666667,
		},
		{
			"string non-int values",
			[]sql.Row{{"a"}, {"b"}, {"c"}, {"d"}},
			float64(0),
		},
		{
			"float values",
			[]sql.Row{{1.}, {2.5}, {3.}, {4.}},
			1.5625,
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
		{
			"int64 values",
			[]sql.Row{{int64(1)}, {int64(3)}},
			2.0,
		},
		{
			"int32 values",
			[]sql.Row{{int32(1)}, {int32(3)}},
			2.0,
		},
		{
			"int32 and nil values",
			[]sql.Row{{int32(1)}, {int32(3)}, {nil}},
			2.0,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)

			ctx := sql.NewEmptyContext()
			buf, _ := sum.NewBuffer()
			for _, row := range tt.rows {
				require.NoError(buf.Update(ctx, row))
			}

			result, err := buf.Eval(sql.NewEmptyContext())
			require.NoError(err)
			if tt.expected == nil {
				require.Equal(tt.expected, nil)
				return
			}
			require.True(isFloatEqual(tt.expected.(float64), result.(float64)))
		})
	}
}
