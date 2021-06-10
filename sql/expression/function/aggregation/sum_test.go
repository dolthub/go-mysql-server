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

func TestSum(t *testing.T) {
	sum := NewSum(sql.NewEmptyContext(), expression.NewGetField(0, nil, "", false))

	testCases := []struct {
		name     string
		rows     []sql.Row
		expected interface{}
	}{
		{
			"string int values",
			[]sql.Row{{"1"}, {"2"}, {"3"}, {"4"}},
			float64(10),
		},
		{
			"string float values",
			[]sql.Row{{"1.5"}, {"2"}, {"3"}, {"4"}},
			float64(10.5),
		},
		{
			"string non-int values",
			[]sql.Row{{"a"}, {"b"}, {"c"}, {"d"}},
			float64(0),
		},
		{
			"float values",
			[]sql.Row{{1.}, {2.5}, {3.}, {4.}},
			float64(10.5),
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
			float64(4),
		},
		{
			"int32 values",
			[]sql.Row{{int32(1)}, {int32(3)}},
			float64(4),
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)

			buf := sum.NewBuffer()
			for _, row := range tt.rows {
				require.NoError(sum.Update(sql.NewEmptyContext(), buf, row))
			}

			result, err := sum.Eval(sql.NewEmptyContext(), buf)
			require.NoError(err)
			require.Equal(tt.expected, result)
		})
	}
}

func TestSumWithDistinct(t *testing.T) {
	require := require.New(t)

	ad := expression.NewDistinctExpression(expression.NewGetField(0, nil, "myfield", false))
	sum := NewSum(sql.NewEmptyContext(), ad)

	// first validate that the expression's name is correct
	require.Equal("SUM(DISTINCT myfield)", sum.String())

	testCases := []struct {
		name     string
		rows     []sql.Row
		expected interface{}
	}{
		{
			"string int values",
			[]sql.Row{{"1"}, {"1"}, {"2"}, {"2"}, {"3"}, {"3"}, {"4"}, {"4"}},
			float64(10),
		},
		{
			"string float values",
			[]sql.Row{{"1.5"}, {"1.5"}, {"1.5"}, {"1.5"}, {"2"}, {"3"}, {"4"}},
			float64(10.5),
		},
		{
			"string non-int values",
			[]sql.Row{{"a"}, {"b"}, {"b"}, {"c"}, {"c"}, {"d"}},
			float64(0),
		},
		{
			"float values",
			[]sql.Row{{1.}, {2.5}, {3.}, {4.}},
			float64(10.5),
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
			[]sql.Row{{int64(1)}, {int64(3)}, {int64(3)}, {int64(3)}},
			float64(4),
		},
		{
			"int32 values",
			[]sql.Row{{int32(1)}, {int32(1)}, {int32(1)}, {int32(3)}},
			float64(4),
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			ad.Dispose()

			buf := sum.NewBuffer()
			for _, row := range tt.rows {
				require.NoError(sum.Update(sql.NewEmptyContext(), buf, row))
			}

			result, err := sum.Eval(sql.NewEmptyContext(), buf)
			require.NoError(err)
			require.Equal(tt.expected, result)
		})
	}
}
