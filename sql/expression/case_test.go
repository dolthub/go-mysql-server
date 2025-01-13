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
	"testing"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestCase(t *testing.T) {
	f1 := NewCase(
		NewGetField(0, types.Int64, "foo", false),
		[]CaseBranch{
			{Cond: NewLiteral(int64(1), types.Int64), Value: NewLiteral(int64(2), types.Int64)},
			{Cond: NewLiteral(int64(3), types.Int64), Value: NewLiteral(int64(4), types.Int64)},
			{Cond: NewLiteral(int64(5), types.Int64), Value: NewLiteral(int64(6), types.Int64)},
		},
		NewLiteral(int64(7), types.Int64),
	)

	f2 := NewCase(
		nil,
		[]CaseBranch{
			{
				Cond: NewEquals(
					NewGetField(0, types.Int64, "foo", false),
					NewLiteral(int64(1), types.Int64),
				),
				Value: NewLiteral(int64(2), types.Int64),
			},
			{
				Cond: NewEquals(
					NewGetField(0, types.Int64, "foo", false),
					NewLiteral(int64(3), types.Int64),
				),
				Value: NewLiteral(int64(4), types.Int64),
			},
			{
				Cond: NewEquals(
					NewGetField(0, types.Int64, "foo", false),
					NewLiteral(int64(5), types.Int64),
				),
				Value: NewLiteral(int64(6), types.Int64),
			},
		},
		NewLiteral(int64(7), types.Int64),
	)

	f3 := NewCase(
		NewGetField(0, types.Int64, "foo", false),
		[]CaseBranch{
			{Cond: NewLiteral(int64(1), types.Int64), Value: NewLiteral(int64(2), types.Int64)},
			{Cond: NewLiteral(int64(3), types.Int64), Value: NewLiteral(int64(4), types.Int64)},
			{Cond: NewLiteral(int64(5), types.Int64), Value: NewLiteral(int64(6), types.Int64)},
		},
		nil,
	)

	testCases := []struct {
		name     string
		f        *Case
		row      sql.Row
		expected interface{}
	}{
		{
			"with expr and else branch 1",
			f1,
			sql.UntypedSqlRow{int64(1)},
			int64(2),
		},
		{
			"with expr and else branch 2",
			f1,
			sql.UntypedSqlRow{int64(3)},
			int64(4),
		},
		{
			"with expr and else branch 3",
			f1,
			sql.UntypedSqlRow{int64(5)},
			int64(6),
		},
		{
			"with expr and else, else branch",
			f1,
			sql.UntypedSqlRow{int64(9)},
			int64(7),
		},
		{
			"without expr and else branch 1",
			f2,
			sql.UntypedSqlRow{int64(1)},
			int64(2),
		},
		{
			"without expr and else branch 2",
			f2,
			sql.UntypedSqlRow{int64(3)},
			int64(4),
		},
		{
			"without expr and else branch 3",
			f2,
			sql.UntypedSqlRow{int64(5)},
			int64(6),
		},
		{
			"without expr and else, else branch",
			f2,
			sql.UntypedSqlRow{int64(9)},
			int64(7),
		},
		{
			"without else, else branch",
			f3,
			sql.UntypedSqlRow{int64(9)},
			nil,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			result, err := tt.f.Eval(sql.NewEmptyContext(), tt.row)
			require.NoError(err)
			require.Equal(tt.expected, result)
		})
	}
}

func TestCaseType(t *testing.T) {
	caseExpr := func(values ...sql.Expression) *Case {
		var branches []CaseBranch
		for i := 0; i < len(values)-1; i++ {
			branches = append(branches, CaseBranch{
				Cond:  NewLiteral(int64(i), types.Int64),
				Value: values[i],
			})
		}
		return &Case{
			nil,
			branches,
			values[len(values)-1],
		}
	}

	decimalType := types.MustCreateDecimalType(65, 10)

	testCases := []struct {
		name string
		c    *Case
		t    sql.Type
	}{
		{
			"standalone else clause",
			caseExpr(NewLiteral(int64(0), types.Int64)),
			types.Int64,
		},
		{
			"unsigned promoted and unsigned",
			caseExpr(NewLiteral(uint32(0), types.Uint32), NewLiteral(uint32(1), types.Uint32)),
			types.Uint64,
		},
		{
			"signed promoted and signed",
			caseExpr(NewLiteral(int8(0), types.Int8), NewLiteral(int32(1), types.Int32)),
			types.Int64,
		},
		{
			"int and float to float",
			caseExpr(NewLiteral(int64(0), types.Int64), NewLiteral(float64(1.0), types.Float64)),
			types.Float64,
		},
		{
			"float and int to float",
			caseExpr(NewLiteral(float64(1.0), types.Float64), NewLiteral(int64(0), types.Int64)),
			types.Float64,
		},
		{
			"int and text to text",
			caseExpr(NewLiteral(int64(0), types.Int64), NewLiteral("Hello, world!", types.Text)),
			types.LongText,
		},
		{
			"text and blob to blob",
			caseExpr(NewLiteral("Hello, world!", types.Text), NewLiteral([]byte("0x480x650x6c0x6c0x6f"), types.Blob)),
			types.LongBlob,
		},
		{
			"int and null to int",
			caseExpr(NewLiteral(int64(10), types.Int64), NewLiteral(nil, types.Null)),
			types.Int64,
		},
		{
			"null and int to int",
			caseExpr(NewLiteral(nil, types.Null), NewLiteral(int64(10), types.Int64)),
			types.Int64,
		},
		{
			"uint64 and int8 to decimal",
			caseExpr(NewLiteral(uint64(10), types.Uint64), NewLiteral(int8(0), types.Int8)),
			decimalType,
		},
		{
			"int and text to text",
			caseExpr(NewLiteral(uint64(10), types.Uint64), NewLiteral("Hello, world!", types.LongText)),
			types.LongText,
		},
		{
			"uint and decimal to decimal",
			caseExpr(NewLiteral(uint64(10), types.Uint64), NewLiteral("Hello, world!", types.LongText)),
			types.LongText,
		},
		{
			"int and decimal to decimal",
			caseExpr(NewLiteral(int32(10), types.Int32), NewLiteral(decimal.NewFromInt(1), decimalType)),
			decimalType,
		},
		{
			"date and date stays date",
			caseExpr(NewLiteral("2020-04-07", types.Date), NewLiteral("2020-04-07", types.Date)),
			types.Date,
		},
		{
			"date and timestamp becomes datetime",
			caseExpr(NewLiteral("2020-04-07", types.Date), NewLiteral("2020-04-07T00:00:00Z", types.Timestamp)),
			types.DatetimeMaxPrecision,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.t, tt.c.Type())
		})
	}
}

func TestCaseNullBranch(t *testing.T) {
	require := require.New(t)
	f := NewCase(
		NewGetField(0, types.Int64, "x", false),
		[]CaseBranch{
			{
				Cond:  NewLiteral(int64(1), types.Int64),
				Value: NewLiteral(nil, types.Null),
			},
		},
		nil,
	)
	result, err := f.Eval(sql.NewEmptyContext(), sql.UntypedSqlRow{int64(1)})
	require.NoError(err)
	require.Nil(result)
}
