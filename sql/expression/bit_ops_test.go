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

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

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
