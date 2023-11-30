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

package function

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestSqrt(t *testing.T) {
	f := NewSqrt(
		expression.NewGetField(0, types.Float64, "n", false),
	)
	testCases := []struct {
		name     string
		row      sql.Row
		expected interface{}
		err      bool
	}{
		{"null input", sql.NewRow(nil), nil, false},
		{"invalid string", sql.NewRow("foo"), nil, true},
		{"valid string", sql.NewRow("9"), float64(3), false},
		{"number is zero", sql.NewRow(0), float64(0), false},
		{"positive number", sql.NewRow(8), float64(2.8284271247461903), false},
		{"negative number", sql.NewRow(-1), nil, false},
	}
	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			t.Helper()
			require := require.New(t)
			ctx := sql.NewEmptyContext()

			v, err := f.Eval(ctx, tt.row)
			if tt.err {
				require.Error(err)
			} else {
				require.NoError(err)
				require.Equal(tt.expected, v)
			}
		})
	}
}

func TestPower(t *testing.T) {
	testCases := []struct {
		name     string
		rowType  sql.Type
		row      sql.Row
		expected interface{}
		err      bool
	}{
		{"Base and exp are nil", types.Float64, sql.NewRow(nil, nil), nil, false},
		{"Base is nil", types.Float64, sql.NewRow(2, nil), nil, false},
		{"Exp is nil", types.Float64, sql.NewRow(nil, 2), nil, false},

		{"Base is 0", types.Float64, sql.NewRow(0, 2), float64(0), false},
		{"Base and exp is 0", types.Float64, sql.NewRow(0, 0), float64(1), false},
		{"Exp is 0", types.Float64, sql.NewRow(2, 0), float64(1), false},
		{"Base is negative", types.Float64, sql.NewRow(-2, 2), float64(4), false},
		{"Exp is negative", types.Float64, sql.NewRow(2, -2), float64(0.25), false},
		{"Base and exp are invalid strings", types.Float64, sql.NewRow("a", "b"), nil, true},
		{"Base and exp are valid strings", types.Float64, sql.NewRow("2", "2"), float64(4), false},
		{"positive inf", types.Float64, sql.NewRow(2, math.Inf(1)), nil, false},
		{"negative inf", types.Float64, sql.NewRow(2, math.Inf(1)), nil, false},
	}
	for _, tt := range testCases {
		f := NewPower(
			expression.NewGetField(0, tt.rowType, "", false),
			expression.NewGetField(1, tt.rowType, "", false),
		)
		t.Run(tt.name, func(t *testing.T) {
			t.Helper()
			require := require.New(t)
			ctx := sql.NewEmptyContext()

			v, err := f.Eval(ctx, tt.row)
			if tt.err {
				require.Error(err)
			} else {
				require.NoError(err)
				require.Equal(tt.expected, v)
			}
		})
	}
}
