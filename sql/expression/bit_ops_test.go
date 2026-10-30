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
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// TestBitNot checks numeric conversion, NULLs, and nonmutating binary evaluation.
func TestBitNot(t *testing.T) {
	tests := []struct {
		name     string
		value    any
		typ      sql.Type
		expected any
	}{
		{"signed", int64(64), types.Int64, uint64(18446744073709551551)},
		{"negative", int64(-64), types.Int64, uint64(63)},
		{"maximum unsigned", uint64(18446744073709551615), types.Uint64, uint64(0)},
		{"NULL", nil, types.Null, nil},
		{"double truncates", 1.6, types.Float64, uint64(18446744073709551614)},
		{"negative double truncates", -1.6, types.Float64, uint64(0)},
		{"text truncates", "1.6", types.LongText, uint64(18446744073709551614)},
		{"binary", []byte{0, 64, 255}, types.LongBlob, []byte{255, 191, 0}},
		{"empty binary", []byte{}, types.LongBlob, []byte{}},
		{"binary character set", "A", types.CreateLongText(sql.Collation_binary), []byte{190}},
	}
	ctx := sql.NewEmptyContext()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			row := sql.Row{tt.value}
			var original []byte
			if value, ok := tt.value.([]byte); ok {
				original = bytes.Clone(value)
			}
			expr := NewBitNot(NewGetField(0, tt.typ, "value", tt.value == nil))
			actual, err := expr.Eval(ctx, row)
			require.NoError(t, err)
			require.Equal(t, tt.expected, actual)
			require.Equal(t, tt.value == nil, expr.IsNullable(ctx))
			if bytes, ok := actual.([]byte); ok {
				require.Equal(t, types.LongBlob, expr.Type(ctx))
				if len(bytes) > 0 {
					bytes[0] = 42
					if _, ok := tt.value.([]byte); ok {
						require.Equal(t, original, row[0])
					} else {
						require.Equal(t, tt.value, row[0])
					}
				}
			} else {
				require.Equal(t, types.Uint64, expr.Type(ctx))
			}
		})
	}
}

func TestBitAnd(t *testing.T) {
	var testCases = []struct {
		name                string
		left, right         interface{}
		leftType, rightType sql.Type
		expected            uint64
	}{
		{"1 & 1", 1, 1, types.Uint64, types.Uint64, 1},
		{"8 & 1", 8, 1, types.Uint64, types.Uint64, 0},
		{"3 & 1", 3, 1, types.Uint64, types.Uint64, 1},
		{"1024 & 0", 1024, 0, types.Uint64, types.Uint64, 0},
		{"0 & 1024", 0, 1024, types.Uint64, types.Uint64, 0},
		{"-1 & -12", -1, -12, types.Int64, types.Int64, 18446744073709551604},
		{"signed & unsigned mask", int64(127), uint64(18446744073709551551), types.Int64, types.Uint64, 63},
		{"negative & unsigned mask", int64(-1), uint64(18446744073709551551), types.Int64, types.Uint64, 18446744073709551551},
		{"unsigned mask & signed", uint64(18446744073709551551), int64(9223372036854775807), types.Uint64, types.Int64, 9223372036854775743},
		{"0.6 & 10.24", 0.6, 10.24, types.Float64, types.Float64, 0},
		{"0.6 & -10.24", 0.6, -10.24, types.Float64, types.Float64, 0},
		{"-0.6 & -10.24", -0.6, -10.24, types.Float64, types.Float64, 18446744073709551606},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			result, err := NewBitAnd(
				NewLiteral(tt.left, tt.leftType),
				NewLiteral(tt.right, tt.rightType),
			).Eval(sql.NewEmptyContext(), sql.NewRow())
			require.NoError(err)
			require.Equal(tt.expected, result)
		})
	}
}

func TestBitOr(t *testing.T) {
	var testCases = []struct {
		name                string
		left, right         interface{}
		leftType, rightType sql.Type
		expected            uint64
	}{
		{"1 | 1", 1, 1, types.Uint64, types.Uint64, 1},
		{"8 | 1", 8, 1, types.Uint64, types.Uint64, 9},
		{"3 | 1", 3, 1, types.Uint64, types.Uint64, 3},
		{"1024 | 0", 1024, 0, types.Uint64, types.Uint64, 1024},
		{"0 | 1024", 0, 1024, types.Uint64, types.Uint64, 1024},
		{"-1 | -12", -1, -12, types.Int64, types.Int64, 18446744073709551615},
		{"0.6 | 10.24", 0.6, 10.24, types.Float64, types.Float64, 11},
		{"0.6 | -10.24", 0.6, -10.24, types.Float64, types.Float64, 18446744073709551607},
		{"-0.6 | -10.24", -0.6, -10.24, types.Float64, types.Float64, 18446744073709551615},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			result, err := NewBitOr(
				NewLiteral(tt.left, tt.leftType),
				NewLiteral(tt.right, tt.rightType),
			).Eval(sql.NewEmptyContext(), sql.NewRow())
			require.NoError(err)
			require.Equal(tt.expected, result)
		})
	}
}

func TestBitXor(t *testing.T) {
	var testCases = []struct {
		name                string
		left, right         interface{}
		leftType, rightType sql.Type
		expected            uint64
	}{
		{"1 ^ 1", 1, 1, types.Uint64, types.Uint64, 0},
		{"8 ^ 1", 8, 1, types.Uint64, types.Uint64, 9},
		{"3 ^ 1", 3, 1, types.Uint64, types.Uint64, 2},
		{"1024 ^ 0", 1024, 0, types.Uint64, types.Uint64, 1024},
		{"0 ^ -1024", 0, -1024, types.Int64, types.Int64, 18446744073709550592},
		{"-1 ^ -12", -1, -12, types.Int64, types.Int64, 11},
		{"0.6 ^ 10.24", 0.6, 10.24, types.Float64, types.Float64, 11},
		{"0.6 ^ -10.24", 0.6, -10.24, types.Float64, types.Float64, 18446744073709551607},
		{"-0.6 ^ -10.24", -0.6, -10.24, types.Float64, types.Float64, 9},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			result, err := NewBitXor(
				NewLiteral(tt.left, tt.leftType),
				NewLiteral(tt.right, tt.rightType),
			).Eval(sql.NewEmptyContext(), sql.NewRow())
			require.NoError(err)
			require.Equal(tt.expected, result)
		})
	}
}

func TestShiftLeft(t *testing.T) {
	var testCases = []struct {
		name        string
		left, right uint64
		expected    uint64
	}{
		{"1 << 1", 1, 1, 2},
		{"1 << 3", 1, 3, 8},
		{"1024 << 0", 1024, 0, 1024},
		{"0 << 1024", 0, 1024, 0},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			result, err := NewShiftLeft(
				NewLiteral(tt.left, types.Uint64),
				NewLiteral(tt.right, types.Uint64),
			).Eval(sql.NewEmptyContext(), sql.NewRow())
			require.NoError(err)
			require.Equal(tt.expected, result)
		})
	}
}

func TestShiftRight(t *testing.T) {
	var testCases = []struct {
		name        string
		left, right uint64
		expected    uint64
	}{
		{"1 >> 1", 1, 1, 0},
		{"8 >> 1", 8, 1, 4},
		{"3 >> 1", 3, 1, 1},
		{"1024 >> 0", 1024, 0, 1024},
		{"0 >> 1024", 0, 1024, 0},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			result, err := NewShiftRight(
				NewLiteral(tt.left, types.Uint64),
				NewLiteral(tt.right, types.Uint64),
			).Eval(sql.NewEmptyContext(), sql.NewRow())
			require.NoError(err)
			require.Equal(tt.expected, result)
		})
	}
}

func TestAllUint64(t *testing.T) {
	var testCases = []struct {
		op        string
		value     interface{}
		valueType sql.Type
		expected  uint64
	}{
		{"|", 1, types.Uint64, 1},
		{"&", 3.4, types.Float64, 1},
		{"^", -1024, types.Int64, 18446744073709550593},
		{"<<", 50, types.Uint64, 17294948469009547264},
		{">>", 50, types.Uint64, 15361},
	}

	// (((((0 | 1) & 3.4) ^ -1024) << 50) >> 50) == 15361
	lval := NewLiteral(int64(0), types.Uint64)
	for _, tt := range testCases {
		t.Run(tt.op, func(t *testing.T) {
			require := require.New(t)
			result, err := NewBitOp(lval,
				NewLiteral(tt.value, tt.valueType), tt.op,
			).Eval(sql.NewEmptyContext(), sql.NewRow())
			require.NoError(err)
			require.Equal(tt.expected, result)

			lval = NewLiteral(result, types.Uint64)
		})
	}
}

func TestBitOpType(t *testing.T) {
	testCases := []struct {
		name         string
		leftType     sql.Type
		rightType    sql.Type
		expectedType sql.Type
		description  string
	}{
		{"unsigned & unsigned", types.Uint64, types.Uint64, types.Uint64, "Both operands are unsigned"},
		{"signed & signed", types.Int64, types.Int64, types.Uint64, "Both operands are signed - should return Uint64"},
		{"mixed signed & unsigned", types.Int64, types.Uint64, types.Uint64, "Mixed signed/unsigned operands"},
		{"text & text", types.Text, types.Text, types.Uint64, "Text operands should return Float64"},
		{"text & int", types.Text, types.Int64, types.Uint64, "Mixed text and numeric operands"},
		{"float & float", types.Float64, types.Float64, types.Uint64, "Float operands should return Uint64"},
	}

	operations := []struct {
		name string
		op   func(left, right sql.Expression) *BitOp
	}{
		{"BitAnd", NewBitAnd},
		{"BitOr", NewBitOr},
		{"BitXor", NewBitXor},
		{"ShiftLeft", NewShiftLeft},
		{"ShiftRight", NewShiftRight},
	}

	for _, tt := range testCases {
		for _, op := range operations {
			t.Run(tt.name+"_"+op.name, func(t *testing.T) {
				require := require.New(t)
				bitOp := op.op(NewLiteral(1, tt.leftType), NewLiteral(1, tt.rightType))
				actualType := bitOp.Type(sql.NewEmptyContext())
				require.Equal(tt.expectedType, actualType,
					"BitOp.Type(ctx) should return %v for %s: %s",
					tt.expectedType, tt.name, tt.description)
			})
		}
	}
}
