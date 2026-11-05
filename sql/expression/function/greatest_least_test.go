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

	"github.com/cockroachdb/apd/v3"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestGreatest(t *testing.T) {
	testCases := []struct {
		name string
		args []sql.Expression
		exp  any
	}{
		{
			"null",
			[]sql.Expression{
				expression.NewLiteral(nil, types.Null),
				expression.NewLiteral(5, types.Int64),
				expression.NewLiteral(1, types.Int64),
			},
			nil,
		},
		{
			name: "all signed ints",
			args: []sql.Expression{
				expression.NewLiteral(int64(1), types.Int8),
				expression.NewLiteral(int64(2), types.Int16),
				expression.NewLiteral(int64(3), types.Int32),
				expression.NewLiteral(int64(4), types.Int64),
			},
			exp: int64(4),
		},
		{
			name: "all unsigned ints",
			args: []sql.Expression{
				expression.NewLiteral(int64(1), types.Uint8),
				expression.NewLiteral(int64(2), types.Uint16),
				expression.NewLiteral(int64(3), types.Uint32),
				expression.NewLiteral(int64(4), types.Uint64),
			},
			exp: uint64(4),
		},
		{
			name: "int64 limit",
			args: []sql.Expression{
				expression.NewLiteral(int64(1), types.Int64),
				expression.NewLiteral(int64(9223372036854775807), types.Int64),
			},
			exp: int64(9223372036854775807),
		},
		{
			name: "uint64 limit",
			args: []sql.Expression{
				expression.NewLiteral(uint64(1), types.Uint64),
				expression.NewLiteral(uint64(9223372036854775807), types.Uint64),
				expression.NewLiteral(uint64(18446744073709551615), types.Uint64),
			},
			exp: uint64(18446744073709551615),
		},
		{
			name: "all floats",
			args: []sql.Expression{
				expression.NewLiteral(float32(1.23), types.Float32),
				expression.NewLiteral(float32(34.5), types.Float32),
				expression.NewLiteral(float32(678), types.Float32),
			},
			exp: float32(678),
		},
		{
			name: "all doubles",
			args: []sql.Expression{
				expression.NewLiteral(float64(1.23), types.Float64),
				expression.NewLiteral(float64(34.5), types.Float64),
				expression.NewLiteral(float64(678), types.Float64),
			},
			exp: float64(678),
		},
		{
			name: "all decimals",
			args: []sql.Expression{
				expression.NewLiteral(apd.New(123, 0), types.MustCreateDecimalType(10, 1)),
				expression.NewLiteral(apd.New(456, 0), types.MustCreateDecimalType(10, 2)),
				expression.NewLiteral(apd.New(789, 0), types.MustCreateDecimalType(10, 3)),
			},
			exp: apd.New(789000, -3),
		},
		{
			"all strings",
			[]sql.Expression{
				expression.NewLiteral("aaa", types.LongText),
				expression.NewLiteral("bbb", types.LongText),
				expression.NewLiteral("9999", types.LongText),
				expression.NewLiteral("", types.LongText),
			},
			"bbb",
		},
		{
			name: "mix of ints",
			args: []sql.Expression{
				expression.NewLiteral(int64(1), types.Int8),
				expression.NewLiteral(int64(12), types.Uint16),
				expression.NewLiteral(int64(123), types.Int32),
				expression.NewLiteral(int64(1234), types.Uint64),
			},
			exp: apd.New(1234, 0),
		},
		{
			name: "signed int and float",
			args: []sql.Expression{
				expression.NewLiteral(int64(1), types.Int64),
				expression.NewLiteral(float32(2), types.Float32),
			},
			exp: float32(2),
		},
		{
			name: "signed int and double",
			args: []sql.Expression{
				expression.NewLiteral(int64(1), types.Int64),
				expression.NewLiteral(float64(2), types.Float64),
			},
			exp: float64(2),
		},
		{
			name: "signed int and decimal",
			args: []sql.Expression{
				expression.NewLiteral(int64(1), types.Int64),
				expression.NewLiteral(int64(2), types.MustCreateDecimalType(10, 2)),
			},
			exp: apd.New(200, -2),
		},
		{
			name: "signed int and string",
			args: []sql.Expression{
				expression.NewLiteral(int64(1), types.Int64),
				expression.NewLiteral("999", types.Text),
			},
			exp: "999",
		},

		{
			name: "unsigned int and float",
			args: []sql.Expression{
				expression.NewLiteral(int64(1), types.Uint64),
				expression.NewLiteral(float32(2), types.Float32),
			},
			exp: float32(2),
		},
		{
			name: "unsigned int and double",
			args: []sql.Expression{
				expression.NewLiteral(int64(1), types.Uint64),
				expression.NewLiteral(int64(2), types.Float64),
			},
			exp: float64(2),
		},
		{
			name: "unsigned int and decimal",
			args: []sql.Expression{
				expression.NewLiteral(int64(1), types.Uint64),
				expression.NewLiteral(int64(2), types.MustCreateDecimalType(10, 2)),
			},
			exp: apd.New(200, -2),
		},
		{
			name: "unsigned int and string",
			args: []sql.Expression{
				expression.NewLiteral(uint64(1), types.Uint64),
				expression.NewLiteral("999", types.Text),
			},
			exp: "999",
		},
		{
			name: "float and double",
			args: []sql.Expression{
				expression.NewLiteral(float32(1), types.Float32),
				expression.NewLiteral(float64(2), types.Float64),
			},
			exp: float64(2),
		},
		{
			name: "float and decimal",
			args: []sql.Expression{
				expression.NewLiteral(float32(1), types.Float32),
				expression.NewLiteral(apd.New(2, 0), types.MustCreateDecimalType(10, 2)),
			},
			exp: float64(2),
		},
		{
			name: "float and string",
			args: []sql.Expression{
				expression.NewLiteral(float32(999), types.Float32),
				expression.NewLiteral("123", types.LongText),
			},
			exp: "999",
		},
		{
			name: "double and decimal",
			args: []sql.Expression{
				expression.NewLiteral(float64(1), types.Float64),
				expression.NewLiteral(apd.New(2, 0), types.MustCreateDecimalType(10, 2)),
			},
			exp: float64(2),
		},
		{
			name: "double and string",
			args: []sql.Expression{
				expression.NewLiteral(float64(999), types.Float64),
				expression.NewLiteral("123", types.LongText),
			},
			exp: "999",
		},
		{
			name: "decimal and string",
			args: []sql.Expression{
				expression.NewLiteral(apd.New(999, 0), types.MustCreateDecimalType(10, 2)),
				expression.NewLiteral("123", types.LongText),
			},
			exp: "999",
		},

		{
			"string mixed",
			[]sql.Expression{
				expression.NewLiteral("9", types.LongText),
				expression.NewLiteral(int64(5), types.Int64),
				expression.NewLiteral(int64(1), types.Int64),
			},
			"9",
		},
		{
			name: "unconvertible strings and signed int mixed",
			args: []sql.Expression{
				expression.NewLiteral("10.5", types.LongText),
				expression.NewLiteral("foobar", types.Int64),
				expression.NewLiteral(int64(5), types.Int64),
				expression.NewLiteral(int64(1), types.Int64),
			},
			exp: "foobar",
		},
		{
			name: "nulls of a non-null type, char",
			args: []sql.Expression{
				expression.NewConvert(expression.NewLiteral("aaa", types.LongText), types.LongText, expression.ConvertToChar),
				expression.NewConvert(expression.NewLiteral(nil, types.Null), types.LongText, expression.ConvertToChar),
			},
			exp: nil,
		},
		{
			name: "nulls of a non-null type, signed",
			args: []sql.Expression{
				expression.NewConvert(expression.NewLiteral(3.14159265359, types.Float64), types.Int64, expression.ConvertToSigned),
				expression.NewConvert(expression.NewLiteral(nil, types.Null), types.Int64, expression.ConvertToSigned),
			},
			exp: nil,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)

			ctx := sql.NewEmptyContext()
			f, err := NewGreatest(sql.NewEmptyContext(), tt.args...)
			require.NoError(err)

			res, err := f.Eval(ctx, nil)
			require.NoError(err)
			require.Equal(tt.exp, res)
		})
	}
}

func TestGreatestLeastExactDecimalDirection(t *testing.T) {
	ctx := sql.NewEmptyContext()
	dt := types.MustCreateDecimalType(20, 19)
	low, err := types.InternalDecimalType.ConvertToDecimal("2.0000000000000000001")
	require.NoError(t, err)
	high, err := types.InternalDecimalType.ConvertToDecimal("2.0000000000000000002")
	require.NoError(t, err)

	for _, test := range []struct {
		name        string
		constructor func(*sql.Context, ...sql.Expression) (sql.Expression, error)
		expected    string
	}{
		{
			name:        "greatest",
			constructor: NewGreatest,
			expected:    high.String(),
		},
		{
			name:        "least",
			constructor: NewLeast,
			expected:    low.String(),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			args := []sql.Expression{expression.NewLiteral(low, dt), expression.NewLiteral(high, dt)}
			for range 2 {
				f, err := test.constructor(ctx, args...)
				require.NoError(t, err)
				value, err := f.Eval(ctx, nil)
				require.NoError(t, err)
				decimal, err := types.InternalDecimalType.ConvertToDecimal(value)
				require.NoError(t, err)
				require.Equal(t, test.expected, decimal.String())

				args[0], args[1] = args[1], args[0]
			}
		})
	}
}

func TestLeast(t *testing.T) {
	testCases := []struct {
		name     string
		args     []sql.Expression
		expected interface{}
	}{
		{
			"null",
			[]sql.Expression{
				expression.NewLiteral(nil, types.Null),
				expression.NewLiteral(5, types.Int64),
				expression.NewLiteral(1, types.Int64),
			},
			nil,
		},
		{
			"negative and all ints",
			[]sql.Expression{
				expression.NewLiteral(int64(-1), types.Int64),
				expression.NewLiteral(int64(5), types.Int64),
				expression.NewLiteral(int64(1), types.Int64),
			},
			int64(-1),
		},
		{
			"string mixed",
			[]sql.Expression{
				expression.NewLiteral(string("10"), types.LongText),
				expression.NewLiteral(int64(5), types.Int64),
				expression.NewLiteral(int64(1), types.Int64),
			},
			"1",
		},
		{
			"unconvertible string mixed ignored",
			[]sql.Expression{
				expression.NewLiteral(string("10.5"), types.LongText),
				expression.NewLiteral(string("foobar"), types.Int64),
				expression.NewLiteral(int64(5), types.Int64),
				expression.NewLiteral(int64(1), types.Int64),
			},
			"1",
		},
		{
			"float mixed",
			[]sql.Expression{
				expression.NewLiteral(float64(10.0), types.Float64),
				expression.NewLiteral(int(5), types.Int64),
				expression.NewLiteral(int(1), types.Int64),
			},
			float64(1.0),
		},
		{
			"all strings",
			[]sql.Expression{
				expression.NewLiteral("aaa", types.LongText),
				expression.NewLiteral("bbb", types.LongText),
				expression.NewLiteral("9999", types.LongText),
			},
			"9999",
		},
		{
			"all strings and empty",
			[]sql.Expression{
				expression.NewLiteral("aaa", types.LongText),
				expression.NewLiteral("bbb", types.LongText),
				expression.NewLiteral("9999", types.LongText),
				expression.NewLiteral("", types.LongText),
			},
			"",
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			ctx := sql.NewEmptyContext()
			require := require.New(t)

			f, err := NewLeast(ctx, tt.args...)
			require.NoError(err)

			output, err := f.Eval(ctx, nil)
			require.NoError(err)
			require.Equal(tt.expected, output)
		})
	}
}
