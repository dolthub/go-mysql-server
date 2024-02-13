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
	"fmt"
	"testing"
	"time"

	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
	_ "github.com/dolthub/go-mysql-server/sql/variables"
)

func TestPlus(t *testing.T) {
	var testCases = []struct {
		name  string
		left  sql.Expression
		right sql.Expression
		exp   interface{}
		skip  bool
	}{
		{
			left:  NewLiteral(1, types.Uint32),
			right: NewLiteral(1, types.Uint32),
			exp:   uint64(2),
		},
		{
			left:  NewLiteral(1, types.Uint64),
			right: NewLiteral(1, types.Uint64),
			exp:   uint64(2),
		},
		{
			left:  NewLiteral(1, types.Int32),
			right: NewLiteral(1, types.Int32),
			exp:   int64(2),
		},
		{
			left:  NewLiteral(1, types.Int64),
			right: NewLiteral(1, types.Int64),
			exp:   int64(2),
		},
		{
			left:  NewLiteral(0, types.Int64),
			right: NewLiteral(0, types.Int64),
			exp:   int64(0),
		},
		{
			left:  NewLiteral(-1, types.Int64),
			right: NewLiteral(1, types.Int64),
			exp:   int64(0),
		},
		{
			left:  NewLiteral(1, types.Float32),
			right: NewLiteral(1, types.Float32),
			exp:   float64(2),
		},
		{
			left:  NewLiteral(1, types.Float64),
			right: NewLiteral(1, types.Float64),
			exp:   float64(2),
		},
		{
			left:  NewLiteral(0.1459, types.Float64),
			right: NewLiteral(3.0, types.Float64),
			exp:   3.1459,
		},
		{
			left:  NewLiteral(decimal.New(1, 0), types.MustCreateDecimalType(10, 0)),
			right: NewLiteral(decimal.New(1, 0), types.MustCreateDecimalType(10, 0)),
			exp:   "2",
		},
		{
			left:  NewLiteral(decimal.New(1000, -3), types.MustCreateDecimalType(10, 3)), // 1.000
			right: NewLiteral(decimal.New(1, 0), types.MustCreateDecimalType(10, 0)),
			exp:   "2.000",
		},
		{
			left:  NewLiteral(decimal.New(1000, -3), types.MustCreateDecimalType(10, 3)),   // 1.000
			right: NewLiteral(decimal.New(100000, -5), types.MustCreateDecimalType(10, 5)), // 1.00000
			exp:   "2.00000",
		},
		{
			left:  NewLiteral(decimal.New(1459, -4), types.MustCreateDecimalType(10, 4)), // 0.1459
			right: NewLiteral(decimal.New(3, 0), types.MustCreateDecimalType(10, 0)),     // 3
			exp:   "3.1459",
		},
		{
			left:  NewLiteral(2001, types.Year),
			right: NewLiteral(2002, types.Year),
			exp:   uint64(4003),
		},
		{
			left:  NewLiteral("2001-01-01", types.Date),
			right: NewLiteral("2001-01-01", types.Date),
			exp:   int64(40020202),
		},
		{
			skip:  true, // need to trim just the date portion
			left:  NewLiteral("2001-01-01 12:00:00", types.Date),
			right: NewLiteral("2001-01-01 12:00:00", types.Date),
			exp:   int64(40020202),
		},
		{
			skip:  true, // need to trim just the date portion
			left:  NewLiteral("2001-01-01 12:00:00.123456", types.Date),
			right: NewLiteral("2001-01-01 12:00:00.123456", types.Date),
			exp:   int64(40020202),
		},
		{
			left:  NewLiteral("2001-01-01 12:00:00", types.Datetime),
			right: NewLiteral("2001-01-01 12:00:00", types.Datetime),
			exp:   int64(40020202240000),
		},
		{
			skip:  true, // need to trim just the datetime portion according to precision
			left:  NewLiteral("2001-01-01 12:00:00.123456", types.Datetime),
			right: NewLiteral("2001-01-01 12:00:00.123456", types.Datetime),
			exp:   int64(40020202240000),
		},
		{
			skip:  true, // need to trim just the datetime portion according to precision and use as exponent
			left:  NewLiteral("2001-01-01 12:00:00.123456", types.MustCreateDatetimeType(sqltypes.Datetime, 3)),
			right: NewLiteral("2001-01-01 12:00:00.123456", types.MustCreateDatetimeType(sqltypes.Datetime, 3)),
			exp:   "40020202240000.246",
		},
		{
			skip:  true, // need to use precision as exponent
			left:  NewLiteral("2001-01-01 12:00:00.123456", types.DatetimeMaxPrecision),
			right: NewLiteral("2001-01-01 12:00:00.123456", types.DatetimeMaxPrecision),
			exp:   "40020202240000.246912",
		},
		{
			left:  NewLiteral("1", types.Text),
			right: NewLiteral("1", types.Text),
			exp:   float64(2),
		},
		{
			left:  NewLiteral("1", types.Text),
			right: NewLiteral(1.0, types.Float64),
			exp:   float64(2),
		},
		{
			left:  NewLiteral(1, types.MustCreateBitType(1)),
			right: NewLiteral(0, types.MustCreateBitType(1)),
			exp:   int64(1),
		},
		{
			left:  NewLiteral("2018-05-01", types.LongText),
			right: NewInterval(NewLiteral(int64(1), types.Int64), "DAY"),
			exp:   time.Date(2018, time.May, 2, 0, 0, 0, 0, time.UTC),
		},
		{
			left:  NewInterval(NewLiteral(int64(1), types.Int64), "DAY"),
			right: NewLiteral("2018-05-01", types.LongText),
			exp:   time.Date(2018, time.May, 2, 0, 0, 0, 0, time.UTC),
		},
	}

	for _, tt := range testCases {
		name := fmt.Sprintf("%s(%v)+%s(%v)", tt.left.Type(), tt.left, tt.right.Type(), tt.right)
		t.Run(name, func(t *testing.T) {
			require := require.New(t)
			if tt.skip {
				t.Skip()
			}
			f := NewPlus(tt.left, tt.right)
			result, err := f.Eval(sql.NewEmptyContext(), nil)
			require.NoError(err)
			if dec, ok := result.(decimal.Decimal); ok {
				result = dec.StringFixed(dec.Exponent() * -1)
			}
			assert.Equal(t, tt.exp, result)
		})
	}
}

func TestMinus(t *testing.T) {
	var testCases = []struct {
		name  string
		left  sql.Expression
		right sql.Expression
		exp   interface{}
		skip  bool
	}{
		{
			left:  NewLiteral(1, types.Uint32),
			right: NewLiteral(1, types.Uint32),
			exp:   uint64(0),
		},
		{
			left:  NewLiteral(1, types.Uint64),
			right: NewLiteral(1, types.Uint64),
			exp:   uint64(0),
		},
		{
			left:  NewLiteral(1, types.Int32),
			right: NewLiteral(1, types.Int32),
			exp:   int64(0),
		},
		{
			left:  NewLiteral(1, types.Int64),
			right: NewLiteral(1, types.Int64),
			exp:   int64(0),
		},
		{
			left:  NewLiteral(0, types.Int64),
			right: NewLiteral(0, types.Int64),
			exp:   int64(0),
		},
		{
			left:  NewLiteral(-1, types.Int64),
			right: NewLiteral(1, types.Int64),
			exp:   int64(-2),
		},
		{
			left:  NewLiteral(1, types.Float32),
			right: NewLiteral(1, types.Float32),
			exp:   float64(0),
		},
		{
			left:  NewLiteral(1, types.Float64),
			right: NewLiteral(1, types.Float64),
			exp:   float64(0),
		},
		{
			left:  NewLiteral(0.1459, types.Float64),
			right: NewLiteral(3.0, types.Float64),
			exp:   -2.8541,
		},
		{
			left:  NewLiteral(decimal.New(1, 0), types.MustCreateDecimalType(10, 0)),
			right: NewLiteral(decimal.New(1, 0), types.MustCreateDecimalType(10, 0)),
			exp:   "0",
		},
		{
			left:  NewLiteral(decimal.New(1000, -3), types.MustCreateDecimalType(10, 3)), // 1.000
			right: NewLiteral(decimal.New(1, 0), types.MustCreateDecimalType(10, 0)),
			exp:   "0.000",
		},
		{
			left:  NewLiteral(decimal.New(1000, -3), types.MustCreateDecimalType(10, 3)),   // 1.000
			right: NewLiteral(decimal.New(100000, -5), types.MustCreateDecimalType(10, 5)), // 1.00000
			exp:   "0.00000",
		},
		{
			left:  NewLiteral(decimal.New(1459, -4), types.MustCreateDecimalType(10, 4)), // 0.1459
			right: NewLiteral(decimal.New(3, 0), types.MustCreateDecimalType(10, 0)),     // 3
			exp:   "-2.8541",
		},
		{
			left:  NewLiteral(2002, types.Year),
			right: NewLiteral(2001, types.Year),
			exp:   uint64(1),
		},
		{
			left:  NewLiteral("2001-01-01", types.Date),
			right: NewLiteral("2001-01-01", types.Date),
			exp:   int64(0),
		},
		{
			skip:  true, // need to trim just the date portion
			left:  NewLiteral("2001-01-01 12:00:00", types.Date),
			right: NewLiteral("2001-01-01 12:00:00", types.Date),
			exp:   int64(0),
		},
		{
			skip:  true, // need to trim just the date portion
			left:  NewLiteral("2001-01-01 12:00:00.123456", types.Date),
			right: NewLiteral("2001-01-01 12:00:00.123456", types.Date),
			exp:   int64(0),
		},
		{
			left:  NewLiteral("2001-01-01 12:00:00", types.Datetime),
			right: NewLiteral("2001-01-01 12:00:00", types.Datetime),
			exp:   int64(0),
		},
		{
			skip:  true, // need to trim just the datetime portion according to precision
			left:  NewLiteral("2001-01-01 12:00:00.123456", types.Datetime),
			right: NewLiteral("2001-01-01 12:00:00.123456", types.Datetime),
			exp:   int64(0),
		},
		{
			skip:  true, // need to trim just the datetime portion according to precision and use as exponent
			left:  NewLiteral("2001-01-01 12:00:00.123456", types.MustCreateDatetimeType(sqltypes.Datetime, 3)),
			right: NewLiteral("2001-01-01 12:00:00.123456", types.MustCreateDatetimeType(sqltypes.Datetime, 3)),
			exp:   "0.000",
		},
		{
			skip:  true, // need to use precision as exponent
			left:  NewLiteral("2001-01-01 12:00:00.123456", types.DatetimeMaxPrecision),
			right: NewLiteral("2001-01-01 12:00:00.123456", types.DatetimeMaxPrecision),
			exp:   "0.000000",
		},
		{
			left:  NewLiteral("1", types.Text),
			right: NewLiteral("1", types.Text),
			exp:   float64(0),
		},
		{
			left:  NewLiteral("1", types.Text),
			right: NewLiteral(1.0, types.Float64),
			exp:   float64(0),
		},
		{
			left:  NewLiteral(1, types.MustCreateBitType(1)),
			right: NewLiteral(0, types.MustCreateBitType(1)),
			exp:   int64(1),
		},
		{
			left:  NewLiteral("2018-05-01", types.LongText),
			right: NewInterval(NewLiteral(int64(1), types.Int64), "DAY"),
			exp:   time.Date(2018, time.April, 30, 0, 0, 0, 0, time.UTC),
		},
	}

	for _, tt := range testCases {
		name := fmt.Sprintf("%s(%v)-%s(%v)", tt.left.Type(), tt.left, tt.right.Type(), tt.right)
		t.Run(name, func(t *testing.T) {
			require := require.New(t)
			if tt.skip {
				t.Skip()
			}
			f := NewMinus(tt.left, tt.right)
			result, err := f.Eval(sql.NewEmptyContext(), nil)
			require.NoError(err)
			if dec, ok := result.(decimal.Decimal); ok {
				result = dec.StringFixed(dec.Exponent() * -1)
			}
			assert.Equal(t, tt.exp, result)
		})
	}
}

func TestMult(t *testing.T) {
	var testCases = []struct {
		name  string
		left  sql.Expression
		right sql.Expression
		exp   interface{}
		err   *errors.Kind
		skip  bool
	}{
		{
			left:  NewLiteral(1, types.Uint32),
			right: NewLiteral(1, types.Uint32),
			exp:   uint64(1),
		},
		{
			left:  NewLiteral(1, types.Uint64),
			right: NewLiteral(1, types.Uint64),
			exp:   uint64(1),
		},
		{
			left:  NewLiteral(1, types.Int32),
			right: NewLiteral(1, types.Int32),
			exp:   int64(1),
		},
		{
			left:  NewLiteral(1, types.Int64),
			right: NewLiteral(1, types.Int64),
			exp:   int64(1),
		},
		{
			left:  NewLiteral(0, types.Int64),
			right: NewLiteral(0, types.Int64),
			exp:   int64(0),
		},
		{
			left:  NewLiteral(-1, types.Int64),
			right: NewLiteral(1, types.Int64),
			exp:   int64(-1),
		},
		{
			left:  NewLiteral(1, types.Float32),
			right: NewLiteral(1, types.Float32),
			exp:   float64(1),
		},
		{
			left:  NewLiteral(1, types.Float64),
			right: NewLiteral(1, types.Float64),
			exp:   float64(1),
		},
		{
			left:  NewLiteral(0.1459, types.Float64),
			right: NewLiteral(3.0, types.Float64),
			exp:   0.4377,
		},
		{
			left:  NewLiteral(3.1459, types.Float64),
			right: NewLiteral(3.0, types.Float64),
			exp:   9.4377,
		},
		{
			left:  NewLiteral(decimal.New(1, 0), types.MustCreateDecimalType(10, 0)),
			right: NewLiteral(decimal.New(1, 0), types.MustCreateDecimalType(10, 0)),
			exp:   "1",
		},
		{
			left:  NewLiteral(decimal.New(1000, -3), types.MustCreateDecimalType(10, 3)), // 1.000
			right: NewLiteral(decimal.New(1, 0), types.MustCreateDecimalType(10, 0)),
			exp:   "1.000",
		},
		{
			left:  NewLiteral(decimal.New(1000, -3), types.MustCreateDecimalType(10, 3)),   // 1.000
			right: NewLiteral(decimal.New(100000, -5), types.MustCreateDecimalType(10, 5)), // 1.00000
			exp:   "1.00000000",
		},
		{
			left:  NewLiteral(decimal.New(1459, -4), types.MustCreateDecimalType(10, 4)), // 0.1459
			right: NewLiteral(decimal.New(3, 0), types.MustCreateDecimalType(10, 0)),     // 3
			exp:   "0.4377",
		},
		{
			left:  NewLiteral(decimal.New(31459, -4), types.MustCreateDecimalType(10, 4)), // 3.1459
			right: NewLiteral(decimal.New(3, 0), types.MustCreateDecimalType(10, 0)),      // 3
			exp:   "9.4377",
		},
		{
			left:  NewLiteral(2002, types.Year),
			right: NewLiteral(2001, types.Year),
			exp:   uint64(4006002),
		},
		{
			left:  NewLiteral("2001-01-01", types.Date),
			right: NewLiteral("2001-01-01", types.Date),
			exp:   int64(400404142030201),
		},
		{
			skip:  true, // need to trim just the date portion
			left:  NewLiteral("2001-01-01 12:00:00", types.Date),
			right: NewLiteral("2001-01-01 12:00:00", types.Date),
			exp:   int64(400404142030201),
		},
		{
			skip:  true, // need to trim just the date portion
			left:  NewLiteral("2001-01-01 12:00:00.123456", types.Date),
			right: NewLiteral("2001-01-01 12:00:00.123456", types.Date),
			exp:   int64(400404142030201),
		},
		{
			// MySQL throws out of range
			skip:  true,
			left:  NewLiteral("2001-01-01 12:00:00", types.Datetime),
			right: NewLiteral("2001-01-01 12:00:00", types.Datetime),
			err:   sql.ErrValueOutOfRange,
		},
		{
			skip:  true, // need to trim just the datetime portion according to precision
			left:  NewLiteral("2001-01-01 12:00:00.123456", types.Datetime),
			right: NewLiteral("2001-01-01 12:00:00.123456", types.Datetime),
			err:   sql.ErrValueOutOfRange,
		},
		{
			skip:  true, // need to trim just the datetime portion according to precision and use as exponent
			left:  NewLiteral("2001-01-01 12:00:00.123456", types.MustCreateDatetimeType(sqltypes.Datetime, 3)),
			right: NewLiteral("2001-01-01 12:00:00.123456", types.MustCreateDatetimeType(sqltypes.Datetime, 3)),
			exp:   "400404146832630176884875520.015129",
		},
		{
			skip:  true, // need to use precision as exponent
			left:  NewLiteral("2001-01-01 12:00:00.123456", types.DatetimeMaxPrecision),
			right: NewLiteral("2001-01-01 12:00:00.123456", types.DatetimeMaxPrecision),
			exp:   "400404146832630195134087741.455241383936",
		},
		{
			left:  NewLiteral("10", types.Text),
			right: NewLiteral("10", types.Text),
			exp:   float64(100),
		},
		{
			left:  NewLiteral("10", types.Text),
			right: NewLiteral(10.0, types.Float64),
			exp:   float64(100),
		},
		{
			left:  NewLiteral(1, types.MustCreateBitType(1)),
			right: NewLiteral(0, types.MustCreateBitType(1)),
			exp:   int64(0),
		},
	}

	for _, tt := range testCases {
		name := fmt.Sprintf("%s(%v)*%s(%v)", tt.left.Type(), tt.left, tt.right.Type(), tt.right)
		t.Run(name, func(t *testing.T) {
			require := require.New(t)
			if tt.skip {
				t.Skip()
			}
			f := NewMult(tt.left, tt.right)
			result, err := f.Eval(sql.NewEmptyContext(), nil)
			if tt.err != nil {
				require.Error(err)
				require.True(tt.err.Is(err), err.Error())
				return
			}
			require.NoError(err)
			if dec, ok := result.(decimal.Decimal); ok {
				result = dec.StringFixed(dec.Exponent() * -1)
			}
			assert.Equal(t, tt.exp, result)
		})
	}
}

func TestMod(t *testing.T) {
	// TODO: make this match the others
	var testCases = []struct {
		name        string
		left, right int64
		expected    string
		null        bool
	}{
		{"1 % 1", 1, 1, "0", false},
		{"8 % 3", 8, 3, "2", false},
		{"1 % 3", 1, 3, "1", false},
		{"0 % -1024", 0, -1024, "0", false},
		{"-1 % 2", -1, 2, "-1", false},
		{"1 % -2", 1, -2, "1", false},
		{"-1 % -2", -1, -2, "-1", false},
		{"1 % 0", 1, 0, "0", true},
		{"0 % 0", 0, 0, "0", true},
		{"0.5 % 0.24", 0, 0, "0.02", true},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			result, err := NewMod(
				NewLiteral(tt.left, types.Int64),
				NewLiteral(tt.right, types.Int64),
			).Eval(sql.NewEmptyContext(), sql.NewRow())
			require.NoError(err)
			if tt.null {
				require.Nil(result)
			} else {
				r, ok := result.(decimal.Decimal)
				require.True(ok)
				require.Equal(tt.expected, r.StringFixed(r.Exponent()*-1))
			}
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
