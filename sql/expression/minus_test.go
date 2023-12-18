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

package expression

import (
	"testing"
	"time"

	"github.com/dolthub/vitess/go/vt/sqlparser"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
	_ "github.com/dolthub/go-mysql-server/sql/variables"
)

func TestMinus(t *testing.T) {
	var testCases = []struct {
		name        string
		left, right float64
		expected    string
	}{
		{"1 - 1", 1, 1, "0"},
		{"1 - -1", 1, -1, "2"},
		{"0 - 0", 0, 0, "0"},
		{"3.14159 - 3.0", 3.14159, 3.0, "0.14159"},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			result, err := NewMinus(
				NewLiteral(tt.left, types.Float64),
				NewLiteral(tt.right, types.Float64),
			).Eval(sql.NewEmptyContext(), sql.NewRow())
			require.NoError(err)
			r, ok := result.(decimal.Decimal)
			assert.True(t, ok)
			assert.Equal(t, tt.expected, r.StringFixed(r.Exponent()*-1))
		})
	}

	require := require.New(t)
	result, err := NewMinus(NewLiteral("10", types.LongText), NewLiteral(10, types.Int64)).
		Eval(sql.NewEmptyContext(), sql.NewRow())
	require.NoError(err)
	require.Equal(0.0, result)
}

func TestMinusInterval(t *testing.T) {
	require := require.New(t)

	expected := time.Date(2018, time.May, 1, 0, 0, 0, 0, time.UTC)
	op := NewMinus(
		NewLiteral("2018-05-02", types.LongText),
		NewInterval(NewLiteral(int64(1), types.Int64), "DAY"),
	)

	result, err := op.Eval(sql.NewEmptyContext(), nil)
	require.NoError(err)
	require.Equal(expected, result)
}

func TestAllFloat64(t *testing.T) {
	var testCases = []struct {
		op       string
		value    float64
		expected string
	}{
		// The value here are given with decimal place to force the value type to float, but the interpreted values
		// will not have 0 scale, so the mult is 3.0000 * 0 = 0.0000 instead of 3.0000 * 0.0 = 0.00000
		{"+", 1.0, "1"},
		{"-", -8.0, "9"},
		{"/", 3.0, "3.0000"},
		{"*", 4.0, "12.0000"},
		{"%", 11, "1.0000"},
	}

	// ((((0 + 1) - (-8)) / 3) * 4) % 11 == 1
	lval := NewLiteral(float64(0.0), types.Float64)
	for _, tt := range testCases {
		t.Run(tt.op, func(t *testing.T) {
			require := require.New(t)
			var result interface{}
			var err error
			switch tt.op {
			case sqlparser.DivStr:
				result, err = NewDiv(lval,
					NewLiteral(tt.value, types.Float64),
				).Eval(sql.NewEmptyContext(), sql.NewRow())
			case sqlparser.ModStr:
				result, err = NewMod(lval,
					NewLiteral(tt.value, types.Float64),
				).Eval(sql.NewEmptyContext(), sql.NewRow())
			case sqlparser.MinusStr:
				result, err = NewMinus(lval,
					NewLiteral(tt.value, types.Float64),
				).Eval(sql.NewEmptyContext(), sql.NewRow())
			case sqlparser.PlusStr:
				result, err = NewPlus(lval,
					NewLiteral(tt.value, types.Float64),
				).Eval(sql.NewEmptyContext(), sql.NewRow())
			case sqlparser.MultStr:
				result, err = NewMult(lval,
					NewLiteral(tt.value, types.Float64),
				).Eval(sql.NewEmptyContext(), sql.NewRow())
			}
			require.NoError(err)
			if r, ok := result.(decimal.Decimal); ok {
				assert.Equal(t, tt.expected, r.StringFixed(r.Exponent()*-1))
			} else {
				assert.Equal(t, tt.expected, result)
			}

			lval = NewLiteral(result, types.Float64)
		})
	}
}

func TestUnaryMinus(t *testing.T) {
	testCases := []struct {
		name     string
		input    interface{}
		typ      sql.Type
		expected interface{}
	}{
		{"int32", int32(1), types.Int32, int32(-1)},
		{"uint32", uint32(1), types.Uint32, int32(-1)},
		{"int64", int64(1), types.Int64, int64(-1)},
		{"uint64", uint64(1), types.Uint64, int64(-1)},
		{"float32", float32(1), types.Float32, float32(-1)},
		{"float64", float64(1), types.Float64, float64(-1)},
		{"int text", "1", types.LongText, "-1"},
		{"float text", "1.2", types.LongText, "-1.2"},
		{"nil", nil, types.LongText, nil},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			f := NewUnaryMinus(NewLiteral(tt.input, tt.typ))
			result, err := f.Eval(sql.NewEmptyContext(), nil)
			require.NoError(t, err)
			if dt, ok := result.(decimal.Decimal); ok {
				require.Equal(t, tt.expected, dt.StringFixed(dt.Exponent()*-1))
			} else {
				require.Equal(t, tt.expected, result)
			}
		})
	}
}
