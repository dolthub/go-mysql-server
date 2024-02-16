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

package expression

import (
	"testing"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestDiv(t *testing.T) {
	var floatTestCases = []struct {
		name        string
		left, right float64
		expected    string
		null        bool
	}{
		{"1 / 1", 1, 1, "1.0000", false},
		{"1 / 2", 1, 2, "0.5000", false},
		{"-1 / 1.0", -1, 1, "-1.0000", false},
		{"0 / 1234567890", 0, 12345677890, "0.0000", false},
		{"3.14159 / 3.0", 3.14159, 3.0, "1.047196667", false},
		{"1/0", 1, 0, "", true},
		{"-1/0", -1, 0, "", true},
		{"0/0", 0, 0, "", true},
	}

	for _, tt := range floatTestCases {
		t.Run(tt.name, func(t *testing.T) {
			result, err := NewDiv(
				// The numbers are interpreted as Float64 without going through parser, so we lose precision here for 1.0
				NewLiteral(tt.left, types.Float64),
				NewLiteral(tt.right, types.Float64),
			).Eval(sql.NewEmptyContext(), sql.NewRow())
			require.NoError(t, err)
			if tt.null {
				assert.Equal(t, nil, result)
			} else {
				r, ok := result.(decimal.Decimal)
				assert.True(t, ok)
				assert.Equal(t, tt.expected, r.StringFixed(r.Exponent()*-1))
			}
		})
	}

	var intTestCases = []struct {
		name        string
		left, right int64
		expected    string
		null        bool
	}{
		{"1 / 1", 1, 1, "1.0000", false},
		{"-1 / 1", -1, 1, "-1.0000", false},
		{"0 / 1234567890", 0, 12345677890, "0.0000", false},
		{"1/0", 1, 0, "", true},
		{"0/0", 1, 0, "", true},
	}
	for _, tt := range intTestCases {
		t.Run(tt.name, func(t *testing.T) {
			result, err := NewDiv(
				NewLiteral(tt.left, types.Int64),
				NewLiteral(tt.right, types.Int64),
			).Eval(sql.NewEmptyContext(), sql.NewRow())
			require.NoError(t, err)
			if tt.null {
				assert.Equal(t, nil, result)
			} else {
				r, ok := result.(decimal.Decimal)
				assert.True(t, ok)
				assert.Equal(t, tt.expected, r.StringFixed(r.Exponent()*-1))
			}
		})
	}

	var uintTestCases = []struct {
		name        string
		left, right uint64
		expected    string
		null        bool
	}{
		{"1 / 1", 1, 1, "1.0000", false},
		{"0 / 1234567890", 0, 12345677890, "0.0000", false},
		{"1/0", 1, 0, "", true},
		{"0/0", 1, 0, "", true},
	}
	for _, tt := range uintTestCases {
		t.Run(tt.name, func(t *testing.T) {
			result, err := NewDiv(
				NewLiteral(tt.left, types.Uint64),
				NewLiteral(tt.right, types.Uint64),
			).Eval(sql.NewEmptyContext(), sql.NewRow())
			require.NoError(t, err)
			if tt.null {
				assert.Equal(t, nil, result)
			} else {
				r, ok := result.(decimal.Decimal)
				assert.True(t, ok)
				assert.Equal(t, tt.expected, r.StringFixed(r.Exponent()*-1))
			}
		})
	}
}

// TestDivUsesFloatsInternally tests that division expression trees internally use floating point types when operating
// on integers, but when returning the final result from the expression tree, it is returned as a Decimal.
func TestDivUsesFloatsInternally(t *testing.T) {
	bottomDiv := NewDiv(
		NewGetField(0, types.Int32, "", false),
		NewGetField(1, types.Int64, "", false))
	middleDiv := NewDiv(bottomDiv,
		NewGetField(2, types.Int64, "", false))
	topDiv := NewDiv(middleDiv,
		NewGetField(3, types.Int64, "", false))

	result, err := topDiv.Eval(sql.NewEmptyContext(), sql.NewRow(250, 2, 5, 2))
	require.NoError(t, err)
	dec, isDecimal := result.(decimal.Decimal)
	require.True(t, isDecimal)
	require.Equal(t, "12.5", dec.String())

	// Internal nodes should use floats for division with integers (for performance reasons), but the top node
	// should return a Decimal (to match MySQL's behavior).
	require.Equal(t, types.Float64, bottomDiv.Type())
	require.Equal(t, types.Float64, middleDiv.Type())
	require.True(t, types.IsDecimal(topDiv.Type()))
}

func TestIntDiv(t *testing.T) {
	var testCases = []struct {
		name                string
		left, right         interface{}
		leftType, rightType sql.Type
		expected            int64
		null                bool
	}{
		{"1 div 1", 1, 1, types.Int64, types.Int64, 1, false},
		{"8 div 3", 8, 3, types.Int64, types.Int64, 2, false},
		{"1 div 3", 1, 3, types.Int64, types.Int64, 0, false},
		{"0 div -1024", 0, -1024, types.Int64, types.Int64, 0, false},
		{"1 div 0", 1, 0, types.Int64, types.Int64, 0, true},
		{"0 div 0", 1, 0, types.Int64, types.Int64, 0, true},
		{"10.24 div 0.6", 10.24, 0.6, types.Float64, types.Float64, 17, false},
		{"-10.24 div 0.6", -10.24, 0.6, types.Float64, types.Float64, -17, false},
		{"-10.24 div -0.6", -10.24, -0.6, types.Float64, types.Float64, 17, false},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			result, err := NewIntDiv(
				NewLiteral(tt.left, tt.leftType),
				NewLiteral(tt.right, tt.rightType),
			).Eval(sql.NewEmptyContext(), sql.NewRow())
			require.NoError(err)
			if tt.null {
				assert.Equal(t, nil, result)
			} else {
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

// Results:
// BenchmarkDivInt-16         10000            695805 ns/op
func BenchmarkDivInt(b *testing.B) {
	require := require.New(b)
	ctx := sql.NewEmptyContext()
	div := NewDiv(
		NewLiteral(1, types.Int64),
		NewLiteral(3, types.Int64),
	)
	var res interface{}
	var err error
	for i := 0; i < b.N; i++ {
		res, err = div.Eval(ctx, nil)
		require.NoError(err)
	}
	if dec, ok := res.(decimal.Decimal); ok {
		res = dec.StringFixed(dec.Exponent() * -1)
	}
	exp := "0.3333"
	if res != exp {
		b.Logf("Expected %v, got %v", exp, res)
	}
}

// Results:
// BenchmarkDivFloat-16               10000            695044 ns/op
func BenchmarkDivFloat(b *testing.B) {
	require := require.New(b)
	ctx := sql.NewEmptyContext()
	div := NewDiv(
		NewLiteral(1.0, types.Float64),
		NewLiteral(3.0, types.Float64),
	)
	var res interface{}
	var err error
	for i := 0; i < b.N; i++ {
		res, err = div.Eval(ctx, nil)
		require.NoError(err)
	}
	exp := 1.0 / 3.0
	if res != exp {
		b.Logf("Expected %v, got %v", exp, res)
	}
}

// Results:
// BenchmarkDivHighScaleDecimals-16           10000            694577 ns/op
func BenchmarkDivHighScaleDecimals(b *testing.B) {
	require := require.New(b)
	ctx := sql.NewEmptyContext()
	div := NewDiv(
		NewLiteral(decimal.NewFromFloat(0.123456789), types.MustCreateDecimalType(types.DecimalTypeMaxPrecision, types.DecimalTypeMaxScale)),
		NewLiteral(decimal.NewFromFloat(0.987654321), types.MustCreateDecimalType(types.DecimalTypeMaxPrecision, types.DecimalTypeMaxScale)),
	)
	var res interface{}
	var err error
	for i := 0; i < b.N; i++ {
		res, err = div.Eval(ctx, nil)
		require.NoError(err)
	}
	if dec, ok := res.(decimal.Decimal); ok {
		res = dec.StringFixed(dec.Exponent() * -1)
	}
	exp := "0.124999998860937500014238281250"
	if res != exp {
		b.Logf("Expected %v, got %v", exp, res)
	}
}

// Results:
// BenchmarkDivManyInts-16            10000           1151316 ns/op
func BenchmarkDivManyInts(b *testing.B) {
	require := require.New(b)
	var div sql.Expression = NewLiteral(1, types.Int64)
	for i := 2; i < 10; i++ {
		div = NewDiv(div, NewLiteral(int64(i), types.Int64))
	}
	ctx := sql.NewEmptyContext()
	var res interface{}
	var err error
	for i := 0; i < b.N; i++ {
		res, err = div.Eval(ctx, nil)
		require.NoError(err)
	}
	if dec, ok := res.(decimal.Decimal); ok {
		res = dec.StringFixed(dec.Exponent() * -1)
	}
	exp := "0.000002755731922398589054232804"
	if res != exp {
		b.Logf("Expected %v, got %v", exp, res)
	}
}

// Results:
// BenchmarkManyFloats-16              4322            618849 ns/op
func BenchmarkManyFloats(b *testing.B) {
	require := require.New(b)
	ctx := sql.NewEmptyContext()
	var div sql.Expression = NewLiteral(1.0, types.Float64)
	for i := 2; i < 10; i++ {
		div = NewDiv(div, NewLiteral(float64(i), types.Float64))
	}
	var res interface{}
	var err error
	for i := 0; i < b.N; i++ {
		res, err = div.Eval(ctx, nil)
		require.NoError(err)
	}
	exp := 1.0 / 2.0 / 3.0 / 4.0 / 5.0 / 6.0 / 7.0 / 8.0 / 9.0
	if res != exp {
		b.Logf("Expected %v, got %v", exp, res)
	}
}

// Results:
// BenchmarkDivManyDecimals-16         5721            699095 ns/op
func BenchmarkDivManyDecimals(b *testing.B) {
	require := require.New(b)
	var div sql.Expression = NewLiteral(decimal.NewFromInt(int64(1)), types.DecimalType_{})
	for i := 2; i < 10; i++ {
		div = NewDiv(div, NewLiteral(decimal.NewFromInt(int64(i)), types.DecimalType_{}))
	}
	ctx := sql.NewEmptyContext()
	var res interface{}
	var err error
	for i := 0; i < b.N; i++ {
		res, err = div.Eval(ctx, nil)
		require.NoError(err)
	}
	if dec, ok := res.(decimal.Decimal); ok {
		res = dec.StringFixed(dec.Exponent() * -1)
	}
	exp := "0.000002755731922398589054232804"
	if res != exp {
		b.Logf("Expected %v, got %v", exp, res)
	}
}
