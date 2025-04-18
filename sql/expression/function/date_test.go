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
	"time"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestUnixTimestamp(t *testing.T) {
	currTime := time.Date(1999, 11, 5, 12, 34, 56, 123456000, time.UTC)
	tests := []struct {
		name string
		args []sql.Expression
		typ  sql.Type
		exp  interface{}
		err  bool
		skip bool

		warnCode int
		warnMsg  string
	}{
		{
			name: "too many args",
			args: []sql.Expression{
				expression.NewLiteral("2018-05-02", types.LongText),
				expression.NewLiteral("2018-05-02", types.LongText),
			},
			err: true,
		},
		{
			name:     "invalid types give warning",
			args:     []sql.Expression{expression.NewLiteral(123456, types.Int64)},
			typ:      types.Int64,
			exp:      int64(0),
			warnCode: 1292,
			warnMsg:  "Incorrect datetime value: 123456",
		},
		{
			name:     "invalid types give warning",
			args:     []sql.Expression{expression.NewLiteral("d0lthub", types.Text)},
			typ:      types.Int64,
			exp:      int64(0),
			warnCode: 1292,
			warnMsg:  "Incorrect datetime value: 'd0lthub'",
		},

		{
			name: "no args uses current time",
			typ:  types.Int64,
			exp:  currTime.Unix(),
		},
		{
			name: "2018-05-02",
			args: []sql.Expression{expression.NewLiteral("2018-05-02", types.LongText)},
			typ:  types.Int64,
			exp:  time.Date(2018, 5, 2, 0, 0, 0, 0, time.UTC).Unix(),
		},
		{
			name: "2018-05-02 12:34:56",
			args: []sql.Expression{expression.NewLiteral("2018-05-02 12:34:56", types.LongText)},
			typ:  types.Int64,
			exp:  time.Date(2018, 5, 2, 12, 34, 56, 0, time.UTC).Unix(),
		},
		{
			name: "2018-05-02 12:34:56.1",
			args: []sql.Expression{expression.NewLiteral("2018-05-02 12:34:56.1", types.LongText)},
			typ:  types.MustCreateDecimalType(19, 1),
			exp:  decimal.New(15252644961, -1),
		},
		{
			name: "2018-05-02 12:34:56.12",
			args: []sql.Expression{expression.NewLiteral("2018-05-02 12:34:56.12", types.LongText)},
			typ:  types.MustCreateDecimalType(19, 2),
			exp:  decimal.New(152526449612, -2),
		},
		{
			name: "2018-05-02 12:34:56.123",
			args: []sql.Expression{expression.NewLiteral("2018-05-02 12:34:56.123", types.LongText)},
			typ:  types.MustCreateDecimalType(19, 3),
			exp:  decimal.New(1525264496123, -3),
		},
		{
			name: "2018-05-02 12:34:56.1234",
			args: []sql.Expression{expression.NewLiteral("2018-05-02 12:34:56.1234", types.LongText)},
			typ:  types.MustCreateDecimalType(19, 4),
			exp:  decimal.New(15252644961234, -4),
		},
		{
			name: "2018-05-02 12:34:56.12345",
			args: []sql.Expression{expression.NewLiteral("2018-05-02 12:34:56.12345", types.LongText)},
			typ:  types.MustCreateDecimalType(19, 5),
			exp:  decimal.New(152526449612345, -5),
		},
		{
			name: "2018-05-02 12:34:56.123456",
			args: []sql.Expression{expression.NewLiteral("2018-05-02 12:34:56.123456", types.LongText)},
			typ:  types.MustCreateDecimalType(19, 6),
			exp:  decimal.New(1525264496123456, -6),
		},
		{
			skip: true, // we can't tell if trailing zeros are from string or rounding
			name: "2018-05-02 12:34:56.123456",
			args: []sql.Expression{expression.NewLiteral("2018-05-02 12:34:56.123000", types.LongText)},
			typ:  types.MustCreateDecimalType(19, 6),
			exp:  decimal.New(1525264496123000, -6),
		},

		{
			name: "1970-01-01 00:00:01",
			args: []sql.Expression{expression.NewLiteral("1970-01-01 00:00:01", types.LongText)},
			typ:  types.Int64,
			exp:  int64(1),
		},
		{
			name: "1970-01-01 00:00:01.123",
			args: []sql.Expression{expression.NewLiteral("1970-01-01 00:00:01.123", types.LongText)},
			typ:  types.MustCreateDecimalType(19, 3),
			exp:  decimal.New(1123, -3),
		},
		{
			name: "1970-01-01 00:00:01.123456",
			args: []sql.Expression{expression.NewLiteral("1970-01-01 00:00:01.123456", types.LongText)},
			typ:  types.MustCreateDecimalType(19, 6),
			exp:  decimal.New(1123456, -6),
		},
		{
			name: "3001-01-18 23:59:59.123",
			args: []sql.Expression{expression.NewLiteral("3001-01-18 23:59:59.123", types.LongText)},
			typ:  types.MustCreateDecimalType(19, 3),
			exp:  decimal.New(32536771199123, -3),
		},
		{
			name: "3001-01-18 23:59:59.999999",
			args: []sql.Expression{expression.NewLiteral("3001-01-18 23:59:59.999999", types.LongText)},
			typ:  types.MustCreateDecimalType(19, 6),
			exp:  decimal.New(32536771199999999, -6),
		},

		{
			name: "microseconds after epoch are still 0, but contribute to precision result",
			args: []sql.Expression{expression.NewLiteral("1970-01-01 00:00:00.123", types.LongText)},
			typ:  types.MustCreateDecimalType(19, 3),
			exp:  decimal.New(0, -3),
		},
		{
			name: "microseconds after epoch are still 0, but contribute to precision result",
			args: []sql.Expression{expression.NewLiteral("1970-01-01 00:00:00.123456", types.LongText)},
			typ:  types.MustCreateDecimalType(19, 6),
			exp:  decimal.New(0, -6),
		},
		{
			name: "unix time after valid time range is 0",
			args: []sql.Expression{expression.NewLiteral("3001-01-19 00:00:00", types.LongText)},
			typ:  types.Int64,
			exp:  int64(0),
		},
		{
			name: "unix time after valid time range is 0.000",
			args: []sql.Expression{expression.NewLiteral("3001-01-19 00:00:00.123", types.LongText)},
			typ:  types.MustCreateDecimalType(19, 3),
			exp:  int64(0),
		},
		{
			name: "unix time after valid time range is 0.000000",
			args: []sql.Expression{expression.NewLiteral("3001-01-19 00:00:00.123456", types.LongText)},
			typ:  types.MustCreateDecimalType(19, 6),
			exp:  int64(0),
		},

		{
			skip: true, // there are timezone conversion issues
			name: "now()",
			args: []sql.Expression{&Now{}},
			typ:  types.Int64,
			exp:  currTime.Unix(),
		},
		{
			skip: true, // there are timezone conversion issues
			name: "now(3)",
			args: []sql.Expression{&Now{prec: expression.NewLiteral(int64(3), types.Int64)}},
			typ:  types.MustCreateDecimalType(19, 3),
			exp:  decimal.New(941805296123, -3),
		},
		{
			skip: true, // there are timezone conversion issues
			name: "now(6)",
			args: []sql.Expression{&Now{prec: expression.NewLiteral(int64(6), types.Int64)}},
			typ:  types.MustCreateDecimalType(19, 6),
			exp:  decimal.New(941805296123456, -6),
		},
	}

	for _, test := range tests {
		require := require.New(t)
		ctx := sql.NewEmptyContext()
		ctx.SetQueryTime(currTime)
		ctx.SetSessionVariable(ctx, "time_zone", "UTC")
		t.Run(test.name, func(t *testing.T) {
			if test.skip {
				t.Skip()
			}

			f, err := NewUnixTimestamp(test.args...)
			if test.err {
				require.Error(err)
				return
			}
			require.NoError(err)
			require.Equal(test.typ, f.Type())

			result, err := f.Eval(ctx, nil)
			require.NoError(err)
			require.Equal(test.exp, result)
			require.Equal(test.typ, f.Type())

			if test.warnCode != 0 {
				require.Equal(uint16(1), ctx.WarningCount())
				require.Equal(test.warnCode, ctx.Warnings()[0].Code)
				require.Equal(test.warnMsg, ctx.Warnings()[0].Message)
			}
		})
	}
}

func TestFromUnixtime(t *testing.T) {
	require := require.New(t)

	_, err := NewUnixTimestamp(expression.NewLiteral(0, types.Int64))
	require.NoError(err)

	_, err = NewUnixTimestamp(expression.NewLiteral(1447430881, types.Int64))
	require.NoError(err)
}
