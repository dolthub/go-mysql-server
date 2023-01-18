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

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
	_ "github.com/dolthub/go-mysql-server/sql/variables"
)

func TestAbsValue(t *testing.T) {
	type toTypeFunc func(float64) interface{}

	decimal1616 := types.MustCreateDecimalType(16, 16)

	toInt64 := func(x float64) interface{} { return int64(x) }
	toInt32 := func(x float64) interface{} { return int32(x) }
	toInt := func(x float64) interface{} { return int(x) }
	toInt16 := func(x float64) interface{} { return int16(x) }
	toInt8 := func(x float64) interface{} { return int8(x) }
	toUint64 := func(x float64) interface{} { return uint64(x) }
	toUint32 := func(x float64) interface{} { return uint32(x) }
	toUint := func(x float64) interface{} { return uint(x) }
	toUint16 := func(x float64) interface{} { return uint16(x) }
	toUint8 := func(x float64) interface{} { return uint8(x) }
	toFloat64 := func(x float64) interface{} { return x }
	toFloat32 := func(x float64) interface{} { return float32(x) }
	toDecimal1616 := func(x float64) interface{} { return decimal.NewFromFloat(x) }

	signedTypes := map[sql.Type]toTypeFunc{
		types.Int64: toInt64,
		types.Int32: toInt32,
		types.Int24: toInt,
		types.Int16: toInt16,
		types.Int8:  toInt8}
	unsignedTypes := map[sql.Type]toTypeFunc{
		types.Uint64: toUint64,
		types.Uint32: toUint32,
		types.Uint24: toUint,
		types.Uint16: toUint16,
		types.Uint8:  toUint8}
	floatTypes := map[sql.Type]toTypeFunc{
		types.Float64: toFloat64,
		types.Float32: toFloat32,
		decimal1616:   toDecimal1616,
	}

	testCases := []struct {
		name       string
		typeToConv map[sql.Type]toTypeFunc
		val        float64
		expected   float64
		err        error
	}{
		{
			"signed types positive int",
			signedTypes,
			5.0,
			5.0,
			nil,
		}, {
			"signed types negative int",
			signedTypes,
			-5.0,
			5.0,
			nil,
		},
		{
			"unsigned types positive int",
			unsignedTypes,
			5.0,
			5.0,
			nil,
		},
		{
			"float positive int",
			floatTypes,
			5.0,
			5.0,
			nil,
		}, {
			"float negative int",
			floatTypes,
			-5.0,
			5.0,
			nil,
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			for sqlType, conv := range test.typeToConv {
				f := NewAbsVal(expression.NewGetField(0, sqlType, "blob", true))

				row := sql.NewRow(conv(test.val))
				res, err := f.Eval(sql.NewEmptyContext(), row)

				if test.err == nil {
					require.NoError(t, err)
					require.Equal(t, conv(test.expected), res)
				} else {
					require.Error(t, err)
				}
			}
		})
	}
}
