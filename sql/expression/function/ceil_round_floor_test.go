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
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestCeil(t *testing.T) {
	testCases := []struct {
		name     string
		rowType  sql.Type
		row      sql.Row
		expected interface{}
		err      *errors.Kind
	}{
		{"float64 is nil", types.Float64, sql.NewRow(nil), nil, nil},
		{"float64 is ok", types.Float64, sql.NewRow(5.8), float64(6), nil},
		{"float32 is nil", types.Float32, sql.NewRow(nil), nil, nil},
		{"float32 is ok", types.Float32, sql.NewRow(float32(5.8)), float32(6), nil},
		{"int32 is nil", types.Int32, sql.NewRow(nil), nil, nil},
		{"int32 is ok", types.Int32, sql.NewRow(int32(6)), int32(6), nil},
		{"int64 is nil", types.Int64, sql.NewRow(nil), nil, nil},
		{"int64 is ok", types.Int64, sql.NewRow(int64(6)), int64(6), nil},
		{"blob is nil", types.Blob, sql.NewRow(nil), nil, nil},
		{"blob is ok", types.Blob, sql.NewRow([]byte{1, 2, 3}), int32(66051), nil},
		{"string int is ok", types.Text, sql.NewRow("1"), int32(1), nil},
		{"string float is ok", types.Text, sql.NewRow("1.2"), int32(2), nil},
	}

	for _, tt := range testCases {
		f := NewCeil(expression.NewGetField(0, tt.rowType, "", false))

		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			exprs := f.Children()
			require.True(len(exprs) > 0 && len(exprs) < 3)
			require.NotNil(exprs[0])

			result, err := f.Eval(sql.NewEmptyContext(), tt.row)
			if tt.err != nil {
				require.Error(err)
				require.True(tt.err.Is(err))
			} else {
				require.NoError(err)
				require.Equal(tt.expected, result)
			}

			require.True(types.IsInteger(f.Type()))
			require.False(f.IsNullable())
		})
	}
}

func TestFloor(t *testing.T) {
	testCases := []struct {
		name     string
		rowType  sql.Type
		row      sql.Row
		expected interface{}
		err      *errors.Kind
	}{
		{"float64 is nil", types.Float64, sql.NewRow(nil), nil, nil},
		{"float64 is ok", types.Float64, sql.NewRow(5.8), float64(5), nil},
		{"float32 is nil", types.Float32, sql.NewRow(nil), nil, nil},
		{"float32 is ok", types.Float32, sql.NewRow(float32(5.8)), float32(5), nil},
		{"int32 is nil", types.Int32, sql.NewRow(nil), nil, nil},
		{"int32 is ok", types.Int32, sql.NewRow(int32(6)), int32(6), nil},
		{"int64 is nil", types.Int64, sql.NewRow(nil), nil, nil},
		{"int64 is ok", types.Int64, sql.NewRow(int64(6)), int64(6), nil},
		{"blob is nil", types.Blob, sql.NewRow(nil), nil, nil},
		{"blob is ok", types.Blob, sql.NewRow([]byte{1, 2, 3}), int32(66051), nil},
		{"string int is ok", types.Text, sql.NewRow("1"), int32(1), nil},
		{"string float is ok", types.Text, sql.NewRow("1.2"), int32(1), nil},
	}

	for _, tt := range testCases {
		f := NewFloor(expression.NewGetField(0, tt.rowType, "", false))

		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			exprs := f.Children()
			require.True(len(exprs) > 0 && len(exprs) < 3)
			require.NotNil(exprs[0])

			result, err := f.Eval(sql.NewEmptyContext(), tt.row)
			if tt.err != nil {
				require.Error(err)
				require.True(tt.err.Is(err))
			} else {
				require.NoError(err)
				require.Equal(tt.expected, result)
			}

			require.True(types.IsInteger(f.Type()))
			require.False(f.IsNullable())
		})
	}
}

func TestRound(t *testing.T) {
	testCases := []struct {
		name  string
		xExpr sql.Expression
		dExpr sql.Expression
		exp   interface{}
		err   *errors.Kind
	}{
		{
			name:  "float64 is nil",
			xExpr: expression.NewLiteral(nil, types.Null),
			exp:   nil,
		},
		{
			name:  "float64 without d",
			xExpr: expression.NewLiteral(5.8, types.Float64),
			exp:   6.0,
		},
		{
			name:  "float64 with nil d",
			xExpr: expression.NewLiteral(5.855, types.Float64),
			dExpr: expression.NewLiteral(nil, types.Null),
			exp:   nil,
		},
		{
			name:  "float64 with d",
			xExpr: expression.NewLiteral(5.855, types.Float64),
			dExpr: expression.NewLiteral(2, types.Int32),
			exp:   5.86,
		},
		{
			name:  "float64 with negative d",
			xExpr: expression.NewLiteral(52.855, types.Float64),
			dExpr: expression.NewLiteral(-1, types.Int32),
			exp:   50.0,
		},
		{
			name:  "float64 with negative d",
			xExpr: expression.NewLiteral(52.855, types.Float64),
			dExpr: expression.NewLiteral(-2, types.Int32),
			exp:   100.0,
		},
		{
			name:  "float64 with large d",
			xExpr: expression.NewLiteral(1234567890.0987654321, types.Float64),
			dExpr: expression.NewLiteral(999_999_999, types.Int32),
			exp:   1234567890.0987654321,
		},
		{
			name:  "float64 with large negative d",
			xExpr: expression.NewLiteral(52.855, types.Float64),
			dExpr: expression.NewLiteral(-999_999_999, types.Int32),
			exp:   0.0,
		},
		{
			name:  "float64 with float d",
			xExpr: expression.NewLiteral(5.855, types.Float64),
			dExpr: expression.NewLiteral(2.123, types.Float64),
			exp:   5.86,
		},
		{
			name:  "float64 with float negative d",
			xExpr: expression.NewLiteral(52.855, types.Float64),
			dExpr: expression.NewLiteral(-1.0, types.Float64),
			exp:   50.0,
		},
		{
			name:  "float64 with blob d",
			xExpr: expression.NewLiteral(5.855, types.Float64),
			dExpr: expression.NewLiteral([]byte{'1', '2', '3'}, types.Blob),
			exp:   5.855,
		},

		{
			name:  "float32 without d",
			xExpr: expression.NewLiteral(5.8, types.Float32),
			exp:   6.0,
		},
		{
			name:  "float32 with d",
			xExpr: expression.NewLiteral(5.855, types.Float32),
			dExpr: expression.NewLiteral(2, types.Int32),
			exp:   5.86,
		},
		{
			name:  "float32 with negative d",
			xExpr: expression.NewLiteral(52.855, types.Float32),
			dExpr: expression.NewLiteral(-1, types.Int32),
			exp:   50.0,
		},
		{
			name:  "float32 with float d",
			xExpr: expression.NewLiteral(5.855, types.Float32),
			dExpr: expression.NewLiteral(2.123, types.Float64),
			exp:   5.86,
		},
		{
			name:  "float32 with float negative d",
			xExpr: expression.NewLiteral(52.855, types.Float32),
			dExpr: expression.NewLiteral(-1.0, types.Float32),
			exp:   50.0,
		},
		{
			name:  "float32 with blob d",
			xExpr: expression.NewLiteral(5.855, types.Float32),
			dExpr: expression.NewLiteral([]byte{'1', '2', '3'}, types.Blob),
			exp:   5.855,
		},

		{
			name:  "int64 without d",
			xExpr: expression.NewLiteral(5, types.Int64),
			exp:   int64(5),
		},
		{
			name:  "int64 with d",
			xExpr: expression.NewLiteral(5, types.Int64),
			dExpr: expression.NewLiteral(2, types.Int32),
			exp:   int64(5),
		},
		{
			name:  "int64 with negative d",
			xExpr: expression.NewLiteral(52, types.Int64),
			dExpr: expression.NewLiteral(-1, types.Int32),
			exp:   int64(50),
		},
		{
			name:  "int64 with float d",
			xExpr: expression.NewLiteral(5, types.Int64),
			dExpr: expression.NewLiteral(2.123, types.Float64),
			exp:   int64(5),
		},
		{
			name:  "int64 with float negative d",
			xExpr: expression.NewLiteral(52, types.Int64),
			dExpr: expression.NewLiteral(-1.0, types.Float32),
			exp:   int64(50),
		},
		{
			name:  "int64 with blob d",
			xExpr: expression.NewLiteral(5, types.Int64),
			dExpr: expression.NewLiteral([]byte{'1', '2', '3'}, types.Blob),
			exp:   int64(5),
		},

		{
			name:  "int32 without d",
			xExpr: expression.NewLiteral(5, types.Int32),
			exp:   int64(5),
		},
		{
			name:  "int32 with d",
			xExpr: expression.NewLiteral(5, types.Int32),
			dExpr: expression.NewLiteral(2, types.Int32),
			exp:   int64(5),
		},
		{
			name:  "int32 with negative d",
			xExpr: expression.NewLiteral(52, types.Int32),
			dExpr: expression.NewLiteral(-1, types.Int32),
			exp:   int64(50),
		},
		{
			name:  "int32 with float d",
			xExpr: expression.NewLiteral(5, types.Int32),
			dExpr: expression.NewLiteral(2.123, types.Float64),
			exp:   int64(5),
		},
		{
			name:  "int32 with float negative d",
			xExpr: expression.NewLiteral(52, types.Int32),
			dExpr: expression.NewLiteral(-1.0, types.Float32),
			exp:   int64(50),
		},
		{
			name:  "int32 with blob d",
			xExpr: expression.NewLiteral(5, types.Int32),
			dExpr: expression.NewLiteral([]byte{'1', '2', '3'}, types.Blob),
			exp:   int64(5),
		},

		{
			name:  "int16 without d",
			xExpr: expression.NewLiteral(5, types.Int16),
			exp:   int64(5),
		},
		{
			name:  "int16 with d",
			xExpr: expression.NewLiteral(5, types.Int16),
			dExpr: expression.NewLiteral(2, types.Int32),
			exp:   int64(5),
		},
		{
			name:  "int16 with negative d",
			xExpr: expression.NewLiteral(52, types.Int16),
			dExpr: expression.NewLiteral(-1, types.Int32),
			exp:   int64(50),
		},
		{
			name:  "int16 with float d",
			xExpr: expression.NewLiteral(5, types.Int16),
			dExpr: expression.NewLiteral(2.123, types.Float64),
			exp:   int64(5),
		},
		{
			name:  "int16 with float negative d",
			xExpr: expression.NewLiteral(52, types.Int16),
			dExpr: expression.NewLiteral(-1.0, types.Float32),
			exp:   int64(50),
		},
		{
			name:  "int16 with blob d",
			xExpr: expression.NewLiteral(5, types.Int16),
			dExpr: expression.NewLiteral([]byte{'1', '2', '3'}, types.Blob),
			exp:   int64(5),
		},

		{
			name:  "int8 without d",
			xExpr: expression.NewLiteral(5, types.Int16),
			exp:   int64(5),
		},
		{
			name:  "int8 with d",
			xExpr: expression.NewLiteral(5, types.Int8),
			dExpr: expression.NewLiteral(2, types.Int32),
			exp:   int64(5),
		},
		{
			name:  "int8 with negative d",
			xExpr: expression.NewLiteral(52, types.Int8),
			dExpr: expression.NewLiteral(-1, types.Int32),
			exp:   int64(50),
		},
		{
			name:  "int8 with float d",
			xExpr: expression.NewLiteral(5, types.Int8),
			dExpr: expression.NewLiteral(2.123, types.Float64),
			exp:   int64(5),
		},
		{
			name:  "int8 with float negative d",
			xExpr: expression.NewLiteral(52, types.Int8),
			dExpr: expression.NewLiteral(-1.0, types.Float32),
			exp:   int64(50),
		},
		{
			name:  "int8 with blob d",
			xExpr: expression.NewLiteral(5, types.Int8),
			dExpr: expression.NewLiteral([]byte{'1', '2', '3'}, types.Blob),
			exp:   int64(5),
		},

		{
			name:  "uint64 without d",
			xExpr: expression.NewLiteral(5, types.Uint64),
			exp:   uint64(5),
		},
		{
			name:  "uint64 with d",
			xExpr: expression.NewLiteral(5, types.Uint64),
			dExpr: expression.NewLiteral(2, types.Int32),
			exp:   uint64(5),
		},
		{
			name:  "uint64 with negative d",
			xExpr: expression.NewLiteral(52, types.Uint64),
			dExpr: expression.NewLiteral(-1, types.Int32),
			exp:   uint64(50),
		},
		{
			name:  "uint64 with float d",
			xExpr: expression.NewLiteral(5, types.Uint64),
			dExpr: expression.NewLiteral(2.123, types.Float64),
			exp:   uint64(5),
		},
		{
			name:  "uint64 with float negative d",
			xExpr: expression.NewLiteral(52, types.Uint64),
			dExpr: expression.NewLiteral(-1.0, types.Float32),
			exp:   uint64(50),
		},
		{
			name:  "uint64 with blob d",
			xExpr: expression.NewLiteral(5, types.Uint64),
			dExpr: expression.NewLiteral([]byte{'1', '2', '3'}, types.Blob),
			exp:   uint64(5),
		},

		{
			name:  "uint32 without d",
			xExpr: expression.NewLiteral(5, types.Uint32),
			exp:   uint64(5),
		},
		{
			name:  "uint32 with d",
			xExpr: expression.NewLiteral(5, types.Uint32),
			dExpr: expression.NewLiteral(2, types.Int32),
			exp:   uint64(5),
		},
		{
			name:  "uint32 with negative d",
			xExpr: expression.NewLiteral(52, types.Uint32),
			dExpr: expression.NewLiteral(-1, types.Int32),
			exp:   uint64(50),
		},
		{
			name:  "uint32 with float d",
			xExpr: expression.NewLiteral(5, types.Uint32),
			dExpr: expression.NewLiteral(2.123, types.Float64),
			exp:   uint64(5),
		},
		{
			name:  "uint32 with float negative d",
			xExpr: expression.NewLiteral(52, types.Uint32),
			dExpr: expression.NewLiteral(-1.0, types.Float32),
			exp:   uint64(50),
		},
		{
			name:  "uint32 with blob d",
			xExpr: expression.NewLiteral(5, types.Uint32),
			dExpr: expression.NewLiteral([]byte{'1', '2', '3'}, types.Blob),
			exp:   uint64(5),
		},

		{
			name:  "uint16 without d",
			xExpr: expression.NewLiteral(5, types.Uint16),
			exp:   uint64(5),
		},
		{
			name:  "uint16 with d",
			xExpr: expression.NewLiteral(5, types.Uint16),
			dExpr: expression.NewLiteral(2, types.Int32),
			exp:   uint64(5),
		},
		{
			name:  "uint16 with negative d",
			xExpr: expression.NewLiteral(52, types.Uint16),
			dExpr: expression.NewLiteral(-1, types.Int32),
			exp:   uint64(50),
		},
		{
			name:  "uint16 with float d",
			xExpr: expression.NewLiteral(5, types.Uint16),
			dExpr: expression.NewLiteral(2.123, types.Float64),
			exp:   uint64(5),
		},
		{
			name:  "uint16 with float negative d",
			xExpr: expression.NewLiteral(52, types.Uint16),
			dExpr: expression.NewLiteral(-1.0, types.Float32),
			exp:   uint64(50),
		},
		{
			name:  "uint16 with blob d",
			xExpr: expression.NewLiteral(5, types.Uint16),
			dExpr: expression.NewLiteral([]byte{'1', '2', '3'}, types.Blob),
			exp:   uint64(5),
		},

		{
			name:  "int8 without d",
			xExpr: expression.NewLiteral(5, types.Uint8),
			exp:   uint64(5),
		},
		{
			name:  "int8 with d",
			xExpr: expression.NewLiteral(5, types.Uint8),
			dExpr: expression.NewLiteral(2, types.Int32),
			exp:   uint64(5),
		},
		{
			name:  "int8 with negative d",
			xExpr: expression.NewLiteral(52, types.Uint8),
			dExpr: expression.NewLiteral(-1, types.Int32),
			exp:   uint64(50),
		},
		{
			name:  "int8 with float d",
			xExpr: expression.NewLiteral(5, types.Uint8),
			dExpr: expression.NewLiteral(2.123, types.Float64),
			exp:   uint64(5),
		},
		{
			name:  "int8 with float negative d",
			xExpr: expression.NewLiteral(52, types.Uint8),
			dExpr: expression.NewLiteral(-1.0, types.Float32),
			exp:   uint64(50),
		},
		{
			name:  "int8 with blob d",
			xExpr: expression.NewLiteral(5, types.Uint8),
			dExpr: expression.NewLiteral([]byte{'1', '2', '3'}, types.Blob),
			exp:   uint64(5),
		},

		{
			name:  "text int without d",
			xExpr: expression.NewLiteral("5", types.Text),
			exp:   5.0,
		},
		{
			name:  "text int with d",
			xExpr: expression.NewLiteral("5", types.Text),
			dExpr: expression.NewLiteral(2, types.Int32),
			exp:   5.0,
		},
		{
			name:  "text int with negative d",
			xExpr: expression.NewLiteral("52", types.Text),
			dExpr: expression.NewLiteral(-1, types.Int32),
			exp:   50.0,
		},
		{
			name:  "text int with float d",
			xExpr: expression.NewLiteral("5", types.Text),
			dExpr: expression.NewLiteral(2.123, types.Float64),
			exp:   5.0,
		},
		{
			name:  "text int with float negative d",
			xExpr: expression.NewLiteral("52", types.Text),
			dExpr: expression.NewLiteral(-1.0, types.Float32),
			exp:   50.0,
		},
		{
			name:  "text int with blob d",
			xExpr: expression.NewLiteral("5", types.Text),
			dExpr: expression.NewLiteral([]byte{'1', '2', '3'}, types.Blob),
			exp:   5.0,
		},

		{
			name:  "text float without d",
			xExpr: expression.NewLiteral("5.8", types.Text),
			exp:   6.0,
		},
		{
			name:  "text float with d",
			xExpr: expression.NewLiteral("5.855", types.Text),
			dExpr: expression.NewLiteral(2, types.Int32),
			exp:   5.86,
		},
		{
			name:  "text float with negative d",
			xExpr: expression.NewLiteral("52.855", types.Text),
			dExpr: expression.NewLiteral(-1, types.Int32),
			exp:   50.0,
		},
		{
			name:  "text float with float d",
			xExpr: expression.NewLiteral("5.855", types.Text),
			dExpr: expression.NewLiteral(2.123, types.Float64),
			exp:   5.86,
		},
		{
			name:  "text float with float negative d",
			xExpr: expression.NewLiteral("52.855", types.Text),
			dExpr: expression.NewLiteral(-1.0, types.Float32),
			exp:   50.0,
		},
		{
			name:  "text float with blob d",
			xExpr: expression.NewLiteral(5, types.Text),
			dExpr: expression.NewLiteral([]byte{'1', '2', '3'}, types.Blob),
			exp:   5.0,
		},

		{
			name:  "blob is nil",
			xExpr: expression.NewLiteral(nil, types.Blob),
			dExpr: expression.NewLiteral(nil, types.Int32),
			exp:   nil,
		},
		{
			name:  "blob is ok",
			xExpr: expression.NewLiteral([]byte{'1', '2', '3'}, types.Blob),
			exp:   123.0,
		},

		// TODO: tests truncated strings
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			f, err := NewRound(tt.xExpr, tt.dExpr)
			require.NoError(t, err)

			res, err := f.Eval(sql.NewEmptyContext(), nil)
			if tt.err != nil {
				require.Error(t, err)
				require.True(t, tt.err.Is(err))
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.exp, res)
		})
	}
}
