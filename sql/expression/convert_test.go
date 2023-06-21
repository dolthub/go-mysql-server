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
	"math"
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestConvert(t *testing.T) {
	tests := []struct {
		name        string
		row         sql.Row
		expression  sql.Expression
		castTo      string
		typeLength  int
		typeScale   int
		expected    interface{}
		expectedErr bool
	}{
		{
			name:        "convert int32 to signed",
			row:         nil,
			expression:  NewLiteral(int32(1), types.Int32),
			castTo:      ConvertToSigned,
			expected:    int64(1),
			expectedErr: false,
		},
		{
			name:        "convert int32 to unsigned",
			row:         nil,
			expression:  NewLiteral(int32(-5), types.Int32),
			castTo:      ConvertToUnsigned,
			expected:    uint64(math.MaxUint64 - 4),
			expectedErr: false,
		},
		{
			name:        "convert int32 to float",
			row:         nil,
			expression:  NewLiteral(int32(-5), types.Int32),
			castTo:      ConvertToFloat,
			expected:    float32(-5.0),
			expectedErr: false,
		},
		{
			name:        "convert int32 to double",
			row:         nil,
			expression:  NewLiteral(int32(-5), types.Int32),
			castTo:      ConvertToDouble,
			expected:    -5.0,
			expectedErr: false,
		},
		{
			name:        "convert string to signed",
			row:         nil,
			expression:  NewLiteral("-3", types.LongText),
			castTo:      ConvertToSigned,
			expected:    int64(-3),
			expectedErr: false,
		},
		{
			name:        "convert string to unsigned",
			row:         nil,
			expression:  NewLiteral("-3", types.LongText),
			castTo:      ConvertToUnsigned,
			expected:    uint64(18446744073709551613),
			expectedErr: false,
		},
		{
			name:        "convert string to double",
			row:         nil,
			expression:  NewLiteral("-3.123", types.LongText),
			castTo:      ConvertToDouble,
			expected:    -3.123,
			expectedErr: false,
		},
		{
			name:        "convert string to decimal with precision/scale constraints",
			row:         nil,
			expression:  NewLiteral("10.123456", types.LongText),
			typeLength:  4,
			typeScale:   2,
			castTo:      ConvertToDecimal,
			expected:    "10.12",
			expectedErr: false,
		},
		{
			name:        "convert int to string",
			row:         nil,
			expression:  NewLiteral(-3, types.Int32),
			castTo:      ConvertToChar,
			expected:    "-3",
			expectedErr: false,
		},
		{
			name:        "convert int to string with length constraint",
			row:         nil,
			expression:  NewLiteral(-3, types.Int32),
			castTo:      ConvertToChar,
			typeLength:  1,
			expected:    "-",
			expectedErr: false,
		},
		{
			name:        "impossible conversion string to unsigned",
			row:         nil,
			expression:  NewLiteral("hello", types.LongText),
			castTo:      ConvertToUnsigned,
			expected:    uint64(0),
			expectedErr: false,
		},
		{
			name:        "impossible conversion string to signed",
			row:         nil,
			castTo:      ConvertToSigned,
			expression:  NewLiteral("A", types.LongText),
			expected:    int64(0),
			expectedErr: false,
		},
		{
			name:        "impossible conversion string to double",
			row:         nil,
			castTo:      ConvertToDouble,
			expression:  NewLiteral("A", types.LongText),
			expected:    0.0,
			expectedErr: false,
		},
		{
			name:        "string to datetime",
			row:         nil,
			expression:  NewLiteral("2017-12-12", types.LongText),
			castTo:      ConvertToDatetime,
			expected:    time.Date(2017, time.December, 12, 0, 0, 0, 0, time.UTC),
			expectedErr: false,
		},
		{
			name:        "impossible conversion string to datetime",
			row:         nil,
			castTo:      ConvertToDatetime,
			expression:  NewLiteral(1, types.Int32),
			expected:    nil,
			expectedErr: false,
		},
		{
			name:        "string to date",
			row:         nil,
			castTo:      ConvertToDate,
			expression:  NewLiteral("2017-12-12 11:12:13", types.Int32),
			expected:    time.Date(2017, time.December, 12, 0, 0, 0, 0, time.UTC),
			expectedErr: false,
		},
		{
			name:        "impossible conversion string to date",
			row:         nil,
			castTo:      ConvertToDate,
			expression:  NewLiteral(1, types.Int32),
			expected:    nil,
			expectedErr: false,
		},
		{
			name:        "float to binary",
			row:         nil,
			castTo:      ConvertToBinary,
			expression:  NewLiteral(float64(-2.3), types.Float64),
			expected:    []byte("-2.3"),
			expectedErr: false,
		},
		{
			name:        "float to binary with length restriction",
			row:         nil,
			castTo:      ConvertToBinary,
			typeLength:  3,
			expression:  NewLiteral(float64(-2.3), types.Float64),
			expected:    []byte("-2."),
			expectedErr: false,
		},
		{
			name:        "float to char with length restriction",
			row:         nil,
			castTo:      ConvertToChar,
			typeLength:  4,
			expression:  NewLiteral(10.56789, types.Float32),
			expected:    "10.5",
			expectedErr: false,
		},
		{
			name:        "string to json",
			row:         nil,
			castTo:      ConvertToJSON,
			expression:  NewLiteral(`{"a":2}`, types.LongText),
			expected:    types.MustJSON(`{"a":2}`),
			expectedErr: false,
		},
		{
			name:        "int to json",
			row:         nil,
			castTo:      ConvertToJSON,
			expression:  NewLiteral(2, types.Int32),
			expected:    types.JSONDocument{Val: float64(2)},
			expectedErr: false,
		},
		{
			name:        "impossible conversion string to json",
			row:         nil,
			castTo:      ConvertToJSON,
			expression:  NewLiteral("3>2", types.LongText),
			expected:    nil,
			expectedErr: true,
		},
		{
			name:        "bool to signed",
			row:         nil,
			castTo:      ConvertToSigned,
			expression:  NewLiteral(true, types.Boolean),
			expected:    int64(1),
			expectedErr: false,
		},
		{
			name:        "bool to datetime",
			row:         nil,
			castTo:      ConvertToDatetime,
			expression:  NewLiteral(true, types.Boolean),
			expected:    nil,
			expectedErr: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require := require.New(t)
			convert := NewConvertWithLengthAndScale(test.expression, test.castTo, test.typeLength, test.typeScale)
			val, err := convert.Eval(sql.NewEmptyContext(), test.row)
			if test.expectedErr {
				require.Error(err)
			} else {
				require.NoError(err)
			}

			// Convert any Decimal values to strings for easier comparison (same as we do for engine tests)
			if d, ok := val.(decimal.Decimal); ok {
				val = d.String()
			}

			require.Equal(test.expected, val)
		})
	}
}
