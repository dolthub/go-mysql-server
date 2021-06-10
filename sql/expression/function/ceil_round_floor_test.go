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
)

func TestCeil(t *testing.T) {
	testCases := []struct {
		name     string
		rowType  sql.Type
		row      sql.Row
		expected interface{}
		err      *errors.Kind
	}{
		{"float64 is nil", sql.Float64, sql.NewRow(nil), nil, nil},
		{"float64 is ok", sql.Float64, sql.NewRow(5.8), float64(6), nil},
		{"float32 is nil", sql.Float32, sql.NewRow(nil), nil, nil},
		{"float32 is ok", sql.Float32, sql.NewRow(float32(5.8)), float32(6), nil},
		{"int32 is nil", sql.Int32, sql.NewRow(nil), nil, nil},
		{"int32 is ok", sql.Int32, sql.NewRow(int32(6)), int32(6), nil},
		{"int64 is nil", sql.Int64, sql.NewRow(nil), nil, nil},
		{"int64 is ok", sql.Int64, sql.NewRow(int64(6)), int64(6), nil},
		{"blob is nil", sql.Blob, sql.NewRow(nil), nil, nil},
		{"blob is ok", sql.Blob, sql.NewRow([]byte{1, 2, 3}), int32(0), nil},
		{"string int is ok", sql.Text, sql.NewRow("1"), int32(1), nil},
		{"string float is ok", sql.Text, sql.NewRow("1.2"), int32(2), nil},
	}

	for _, tt := range testCases {
		f := NewCeil(sql.NewEmptyContext(), expression.NewGetField(0, tt.rowType, "", false))

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

			switch {
			case sql.IsFloat(tt.rowType):
				require.True(sql.IsFloat(f.Type()))
				require.False(f.IsNullable())
			case sql.IsInteger(tt.rowType):
				require.True(sql.IsInteger(f.Type()))
				require.False(f.IsNullable())
			default:
				require.True(sql.IsInteger(f.Type()))
				require.False(f.IsNullable())
			}
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
		{"float64 is nil", sql.Float64, sql.NewRow(nil), nil, nil},
		{"float64 is ok", sql.Float64, sql.NewRow(5.8), float64(5), nil},
		{"float32 is nil", sql.Float32, sql.NewRow(nil), nil, nil},
		{"float32 is ok", sql.Float32, sql.NewRow(float32(5.8)), float32(5), nil},
		{"int32 is nil", sql.Int32, sql.NewRow(nil), nil, nil},
		{"int32 is ok", sql.Int32, sql.NewRow(int32(6)), int32(6), nil},
		{"int64 is nil", sql.Int64, sql.NewRow(nil), nil, nil},
		{"int64 is ok", sql.Int64, sql.NewRow(int64(6)), int64(6), nil},
		{"blob is nil", sql.Blob, sql.NewRow(nil), nil, nil},
		{"blob is ok", sql.Blob, sql.NewRow([]byte{1, 2, 3}), int32(0), nil},
		{"string int is ok", sql.Text, sql.NewRow("1"), int32(1), nil},
		{"string float is ok", sql.Text, sql.NewRow("1.2"), int32(1), nil},
	}

	for _, tt := range testCases {
		f := NewFloor(sql.NewEmptyContext(), expression.NewGetField(0, tt.rowType, "", false))

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

			switch {
			case sql.IsFloat(tt.rowType):
				require.True(sql.IsFloat(f.Type()))
				require.False(f.IsNullable())
			case sql.IsInteger(tt.rowType):
				require.True(sql.IsInteger(f.Type()))
				require.False(f.IsNullable())
			default:
				require.True(sql.IsInteger(f.Type()))
				require.False(f.IsNullable())
			}
		})
	}
}

func TestRound(t *testing.T) {
	testCases := []struct {
		name     string
		xType    sql.Type
		dType    sql.Type
		row      sql.Row
		expected interface{}
		err      *errors.Kind
	}{
		{"float64 is nil", sql.Float64, sql.Int32, sql.NewRow(nil, nil), nil, nil},
		{"float64 without d", sql.Float64, sql.Int32, sql.NewRow(5.8, nil), float64(6), nil},
		{"float64 with d", sql.Float64, sql.Int32, sql.NewRow(5.855, 2), float64(5.86), nil},
		{"float64 with negative d", sql.Float64, sql.Int32, sql.NewRow(52.855, -1), float64(50), nil},
		{"float64 with float d", sql.Float64, sql.Float64, sql.NewRow(5.855, float64(2.123)), float64(5.86), nil},
		{"float64 with float negative d", sql.Float64, sql.Float64, sql.NewRow(52.855, float64(-1)), float64(50), nil},
		{"float64 with blob d", sql.Float64, sql.Blob, sql.NewRow(5.855, []byte{1, 2, 3}), float64(6), nil},
		{"float32 is nil", sql.Float32, sql.Int32, sql.NewRow(nil, nil), nil, nil},
		{"float32 without d", sql.Float32, sql.Int32, sql.NewRow(float32(5.8), nil), float32(6), nil},
		{"float32 with d", sql.Float32, sql.Int32, sql.NewRow(float32(5.855), 2), float32(5.86), nil},
		{"float32 with negative d", sql.Float32, sql.Int32, sql.NewRow(float32(52.855), -1), float32(50), nil},
		{"float32 with float d", sql.Float32, sql.Float64, sql.NewRow(float32(5.855), float32(2.123)), float32(5.86), nil},
		{"float32 with float negative d", sql.Float32, sql.Float64, sql.NewRow(float32(52.855), float32(-1)), float32(50), nil},
		{"float32 with blob d", sql.Float32, sql.Blob, sql.NewRow(float32(5.855), []byte{1, 2, 3}), float32(6), nil},
		{"int64 is nil", sql.Int64, sql.Int32, sql.NewRow(nil, nil), nil, nil},
		{"int64 without d", sql.Int64, sql.Int32, sql.NewRow(int64(5), nil), int64(5), nil},
		{"int64 with d", sql.Int64, sql.Int32, sql.NewRow(int64(5), 2), int64(5), nil},
		{"int64 with negative d", sql.Int64, sql.Int32, sql.NewRow(int64(52), -1), int64(50), nil},
		{"int64 with float d", sql.Int64, sql.Float64, sql.NewRow(int64(5), float32(2.123)), int64(5), nil},
		{"int64 with float negative d", sql.Int64, sql.Float64, sql.NewRow(int64(52), float32(-1)), int64(50), nil},
		{"int32 with blob d", sql.Int32, sql.Blob, sql.NewRow(int32(5), []byte{1, 2, 3}), int32(5), nil},
		{"int32 is nil", sql.Int32, sql.Int32, sql.NewRow(nil, nil), nil, nil},
		{"int32 without d", sql.Int32, sql.Int32, sql.NewRow(int32(5), nil), int32(5), nil},
		{"int32 with d", sql.Int32, sql.Int32, sql.NewRow(int32(5), 2), int32(5), nil},
		{"int32 with negative d", sql.Int32, sql.Int32, sql.NewRow(int32(52), -1), int32(50), nil},
		{"int32 with float d", sql.Int32, sql.Float64, sql.NewRow(int32(5), float32(2.123)), int32(5), nil},
		{"int32 with float negative d", sql.Int32, sql.Float64, sql.NewRow(int32(52), float32(-1)), int32(50), nil},
		{"int32 with blob d", sql.Int32, sql.Blob, sql.NewRow(int32(5), []byte{1, 2, 3}), int32(5), nil},
		{"int16 is nil", sql.Int16, sql.Int16, sql.NewRow(nil, nil), nil, nil},
		{"int16 without d", sql.Int16, sql.Int16, sql.NewRow(int16(5), nil), int16(5), nil},
		{"int16 with d", sql.Int16, sql.Int16, sql.NewRow(int16(5), 2), int16(5), nil},
		{"int16 with negative d", sql.Int16, sql.Int16, sql.NewRow(int16(52), -1), int16(50), nil},
		{"int16 with float d", sql.Int16, sql.Float64, sql.NewRow(int16(5), float32(2.123)), int16(5), nil},
		{"int16 with float negative d", sql.Int16, sql.Float64, sql.NewRow(int16(52), float32(-1)), int16(50), nil},
		{"int16 with blob d", sql.Int16, sql.Blob, sql.NewRow(int16(5), []byte{1, 2, 3}), int16(5), nil},
		{"int8 is nil", sql.Int8, sql.Int8, sql.NewRow(nil, nil), nil, nil},
		{"int8 without d", sql.Int8, sql.Int8, sql.NewRow(int8(5), nil), int8(5), nil},
		{"int8 with d", sql.Int8, sql.Int8, sql.NewRow(int8(5), 2), int8(5), nil},
		{"int8 with negative d", sql.Int8, sql.Int8, sql.NewRow(int8(52), -1), int8(50), nil},
		{"int8 with float d", sql.Int8, sql.Float64, sql.NewRow(int8(5), float32(2.123)), int8(5), nil},
		{"int8 with float negative d", sql.Int8, sql.Float64, sql.NewRow(int8(52), float32(-1)), int8(50), nil},
		{"int8 with blob d", sql.Int8, sql.Blob, sql.NewRow(int8(5), []byte{1, 2, 3}), int8(5), nil},
		{"uint64 is nil", sql.Uint64, sql.Int32, sql.NewRow(nil, nil), nil, nil},
		{"uint64 without d", sql.Uint64, sql.Int32, sql.NewRow(uint64(5), nil), uint64(5), nil},
		{"uint64 with d", sql.Uint64, sql.Int32, sql.NewRow(uint64(5), 2), uint64(5), nil},
		{"uint64 with negative d", sql.Uint64, sql.Int32, sql.NewRow(uint64(52), -1), uint64(50), nil},
		{"uint64 with float d", sql.Uint64, sql.Float64, sql.NewRow(uint64(5), float32(2.123)), uint64(5), nil},
		{"uint64 with float negative d", sql.Uint64, sql.Float64, sql.NewRow(uint64(52), float32(-1)), uint64(50), nil},
		{"uint32 with blob d", sql.Uint32, sql.Blob, sql.NewRow(uint32(5), []byte{1, 2, 3}), uint32(5), nil},
		{"uint32 is nil", sql.Uint32, sql.Int32, sql.NewRow(nil, nil), nil, nil},
		{"uint32 without d", sql.Uint32, sql.Int32, sql.NewRow(uint32(5), nil), uint32(5), nil},
		{"uint32 with d", sql.Uint32, sql.Int32, sql.NewRow(uint32(5), 2), uint32(5), nil},
		{"uint32 with negative d", sql.Uint32, sql.Int32, sql.NewRow(uint32(52), -1), uint32(50), nil},
		{"uint32 with float d", sql.Uint32, sql.Float64, sql.NewRow(uint32(5), float32(2.123)), uint32(5), nil},
		{"uint32 with float negative d", sql.Uint32, sql.Float64, sql.NewRow(uint32(52), float32(-1)), uint32(50), nil},
		{"uint32 with blob d", sql.Uint32, sql.Blob, sql.NewRow(uint32(5), []byte{1, 2, 3}), uint32(5), nil},
		{"uint16 with blob d", sql.Uint16, sql.Blob, sql.NewRow(uint16(5), []byte{1, 2, 3}), uint16(5), nil},
		{"uint16 is nil", sql.Uint16, sql.Int16, sql.NewRow(nil, nil), nil, nil},
		{"uint16 without d", sql.Uint16, sql.Int16, sql.NewRow(uint16(5), nil), uint16(5), nil},
		{"uint16 with d", sql.Uint16, sql.Int16, sql.NewRow(uint16(5), 2), uint16(5), nil},
		{"uint16 with negative d", sql.Uint16, sql.Int16, sql.NewRow(uint16(52), -1), uint16(50), nil},
		{"uint16 with float d", sql.Uint16, sql.Float64, sql.NewRow(uint16(5), float32(2.123)), uint16(5), nil},
		{"uint16 with float negative d", sql.Uint16, sql.Float64, sql.NewRow(uint16(52), float32(-1)), uint16(50), nil},
		{"uint16 with blob d", sql.Uint16, sql.Blob, sql.NewRow(uint16(5), []byte{1, 2, 3}), uint16(5), nil},
		{"uint8 with blob d", sql.Uint8, sql.Blob, sql.NewRow(uint8(5), []byte{1, 2, 3}), uint8(5), nil},
		{"uint8 is nil", sql.Uint8, sql.Int8, sql.NewRow(nil, nil), nil, nil},
		{"uint8 without d", sql.Uint8, sql.Int8, sql.NewRow(uint8(5), nil), uint8(5), nil},
		{"uint8 with d", sql.Uint8, sql.Int8, sql.NewRow(uint8(5), 2), uint8(5), nil},
		{"uint8 with negative d", sql.Uint8, sql.Int8, sql.NewRow(uint8(52), -1), uint8(50), nil},
		{"uint8 with float d", sql.Uint8, sql.Float64, sql.NewRow(uint8(5), float32(2.123)), uint8(5), nil},
		{"uint8 with float negative d", sql.Uint8, sql.Float64, sql.NewRow(uint8(52), float32(-1)), uint8(50), nil},
		{"uint8 with blob d", sql.Uint8, sql.Blob, sql.NewRow(uint8(5), []byte{1, 2, 3}), uint8(5), nil},
		{"blob is nil", sql.Blob, sql.Int32, sql.NewRow(nil, nil), nil, nil},
		{"blob is ok", sql.Blob, sql.Int32, sql.NewRow([]byte{1, 2, 3}, nil), int32(0), nil},
		{"text int without d", sql.Text, sql.Int32, sql.NewRow("5", nil), int32(5), nil},
		{"text int with d", sql.Text, sql.Int32, sql.NewRow("5", 2), int32(5), nil},
		{"text int with negative d", sql.Text, sql.Int32, sql.NewRow("52", -1), int32(50), nil},
		{"text int with float d", sql.Text, sql.Float64, sql.NewRow("5", float32(2.123)), int32(5), nil},
		{"text int with float negative d", sql.Text, sql.Float64, sql.NewRow("52", float32(-1)), int32(50), nil},
		{"text float without d", sql.Text, sql.Int32, sql.NewRow("5.8", nil), int32(6), nil},
		{"text float with d", sql.Text, sql.Int32, sql.NewRow("5.855", 2), int32(5), nil},
		{"text float with negative d", sql.Text, sql.Int32, sql.NewRow("52.855", -1), int32(50), nil},
		{"text float with float d", sql.Text, sql.Float64, sql.NewRow("5.855", float64(2.123)), int32(5), nil},
		{"text float with float negative d", sql.Text, sql.Float64, sql.NewRow("52.855", float64(-1)), int32(50), nil},
		{"text float with blob d", sql.Text, sql.Blob, sql.NewRow("5.855", []byte{1, 2, 3}), int32(6), nil},
	}

	for _, tt := range testCases {
		var args = make([]sql.Expression, 2)
		args[0] = expression.NewGetField(0, tt.xType, "", false)
		args[1] = expression.NewGetField(1, tt.dType, "", false)
		f, err := NewRound(sql.NewEmptyContext(), args...)

		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)

			require.Nil(err)

			result, err := f.Eval(sql.NewEmptyContext(), tt.row)
			if tt.err != nil {
				require.Error(err)
				require.True(tt.err.Is(err))
			} else {
				require.NoError(err)
				require.Equal(tt.expected, result)
			}

			switch {
			case sql.IsFloat(tt.xType):
				require.True(sql.IsFloat(f.Type()))
				require.False(f.IsNullable())
			case sql.IsInteger(tt.xType):
				require.True(sql.IsInteger(f.Type()))
				require.False(f.IsNullable())
			default:
				require.True(sql.IsInteger(f.Type()))
				require.False(f.IsNullable())
			}
		})
	}

	// Test on invalid type return 0
	var args = make([]sql.Expression, 2)
	args[0] = expression.NewGetField(0, sql.Blob, "", false)
	args[1] = expression.NewGetField(1, sql.Int32, "", false)

	f, err := NewRound(sql.NewEmptyContext(), args...)
	req := require.New(t)
	req.Nil(err)

	exprs := f.Children()
	req.True(len(exprs) > 0 && len(exprs) < 3)
	req.NotNil(exprs[0])

	result, err := f.Eval(sql.NewEmptyContext(), sql.NewRow([]byte{1, 2, 3}, 2))
	req.NoError(err)
	req.Equal(int32(0), result)
}
