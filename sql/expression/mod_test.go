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

package expression

import (
	"math"
	"testing"

	"github.com/cockroachdb/apd/v3"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// The expected values and types were checked against MySQL 8.4
func TestMod(t *testing.T) {
	dec := func(s string) *apd.Decimal {
		d, _, err := apd.NewFromString(s)
		require.NoError(t, err)
		return d
	}

	testCases := []struct {
		name         string
		left         any
		leftType     sql.Type
		right        any
		rightType    sql.Type
		expected     any
		expectedType sql.Type
	}{
		{"1 % 1", int64(1), types.Int64, int64(1), types.Int64, int64(0), types.Int64},
		{"8 % 3", int64(8), types.Int64, int64(3), types.Int64, int64(2), types.Int64},
		{"1 % 3", int64(1), types.Int64, int64(3), types.Int64, int64(1), types.Int64},
		{"0 % -1024", int64(0), types.Int64, int64(-1024), types.Int64, int64(0), types.Int64},
		{"-1 % 2", int64(-1), types.Int64, int64(2), types.Int64, int64(-1), types.Int64},
		{"1 % -2", int64(1), types.Int64, int64(-2), types.Int64, int64(1), types.Int64},
		{"-1 % -2", int64(-1), types.Int64, int64(-2), types.Int64, int64(-1), types.Int64},
		{"-7 % 2 tinyint", int8(-7), types.Int8, int8(2), types.Int8, int64(-1), types.Int64},
		{"-9223372036854775808 % -1", int64(math.MinInt64), types.Int64, int64(-1), types.Int64, int64(0), types.Int64},
		{"-9223372036854775808 % 3", int64(math.MinInt64), types.Int64, int64(3), types.Int64, int64(-2), types.Int64},
		{"1 % 0", int64(1), types.Int64, int64(0), types.Int64, nil, types.Int64},
		{"0 % 0", int64(0), types.Int64, int64(0), types.Int64, nil, types.Int64},

		// The result is unsigned when the dividend is unsigned
		{"unsigned 5 % unsigned 3", uint64(5), types.Uint64, uint64(3), types.Uint64, uint64(2), types.Uint64},
		{"unsigned 7 % -2", uint64(7), types.Uint64, int64(-2), types.Int64, uint64(1), types.Uint64},
		{"unsigned 7 % -2 int", uint32(7), types.Uint32, int32(-2), types.Int32, uint64(1), types.Uint64},
		{"unsigned 18446744073709551615 % 10", uint64(math.MaxUint64), types.Uint64, int64(10), types.Int64, uint64(5), types.Uint64},
		{"unsigned 18446744073709551615 % -10", uint64(math.MaxUint64), types.Uint64, int64(-10), types.Int64, uint64(5), types.Uint64},
		{"unsigned 7 % 0", uint64(7), types.Uint64, int64(0), types.Int64, nil, types.Uint64},

		// The result is signed when the dividend is signed, even if the divisor is unsigned
		{"-7 % unsigned 7", int64(-7), types.Int64, uint64(7), types.Uint64, int64(0), types.Int64},
		{"-7 % unsigned 4", int64(-7), types.Int64, uint64(4), types.Uint64, int64(-3), types.Int64},
		{"-7 % unsigned 4 tinyint", int8(-7), types.Int8, uint32(4), types.Uint32, int64(-3), types.Int64},
		{"-9223372036854775808 % unsigned 18446744073709551615", int64(math.MinInt64), types.Int64, uint64(math.MaxUint64), types.Uint64, int64(math.MinInt64), types.Int64},
		{"9223372036854775807 % unsigned 18446744073709551615", int64(math.MaxInt64), types.Int64, uint64(math.MaxUint64), types.Uint64, int64(math.MaxInt64), types.Int64},

		// Decimal and float operands do not return integers
		{"0.5 % 0.24", dec("0.5"), types.MustCreateDecimalType(1, 1), dec("0.24"), types.MustCreateDecimalType(2, 2), "0.02", types.MustCreateDecimalType(3, 2)},
		{"-0.5 % 0.24", dec("-0.5"), types.MustCreateDecimalType(1, 1), dec("0.24"), types.MustCreateDecimalType(2, 2), "-0.02", types.MustCreateDecimalType(3, 2)},
		// TODO: MySQL returns decimal(24,2) for this one
		{"12385865823658963784136.5 % 24852.24", dec("12385865823658963784136.5"), types.MustCreateDecimalType(24, 1), dec("24852.24"), types.MustCreateDecimalType(7, 2), "24264.50", types.MustCreateDecimalType(25, 2)},
		{"24852.5 % 12385865823658963784136.24", dec("24852.5"), types.MustCreateDecimalType(6, 1), dec("12385865823658963784136.24"), types.MustCreateDecimalType(25, 2), "24852.50", types.MustCreateDecimalType(25, 2)},
		{"0.5e0 % 0.24e0", 0.5, types.Float64, 0.24, types.Float64, 0.020000000000000018, types.Float64},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			ctx := sql.NewEmptyContext()
			m := NewMod(NewLiteral(tt.left, tt.leftType), NewLiteral(tt.right, tt.rightType))
			require.Equal(t, tt.expectedType, m.Type(ctx))
			result, err := m.Eval(ctx, sql.NewRow())
			require.NoError(t, err)
			if d, ok := result.(*apd.Decimal); ok {
				require.Equal(t, tt.expected, d.Text('f'))
			} else {
				require.Equal(t, tt.expected, result)
			}
		})
	}
}
