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
)

func TestCase(t *testing.T) {
	f1 := NewCase(
		NewGetField(0, sql.Int64, "foo", false),
		[]CaseBranch{
			{Cond: NewLiteral(int64(1), sql.Int64), Value: NewLiteral(int64(2), sql.Int64)},
			{Cond: NewLiteral(int64(3), sql.Int64), Value: NewLiteral(int64(4), sql.Int64)},
			{Cond: NewLiteral(int64(5), sql.Int64), Value: NewLiteral(int64(6), sql.Int64)},
		},
		NewLiteral(int64(7), sql.Int64),
	)

	f2 := NewCase(
		nil,
		[]CaseBranch{
			{
				Cond: NewEquals(
					NewGetField(0, sql.Int64, "foo", false),
					NewLiteral(int64(1), sql.Int64),
				),
				Value: NewLiteral(int64(2), sql.Int64),
			},
			{
				Cond: NewEquals(
					NewGetField(0, sql.Int64, "foo", false),
					NewLiteral(int64(3), sql.Int64),
				),
				Value: NewLiteral(int64(4), sql.Int64),
			},
			{
				Cond: NewEquals(
					NewGetField(0, sql.Int64, "foo", false),
					NewLiteral(int64(5), sql.Int64),
				),
				Value: NewLiteral(int64(6), sql.Int64),
			},
		},
		NewLiteral(int64(7), sql.Int64),
	)

	f3 := NewCase(
		NewGetField(0, sql.Int64, "foo", false),
		[]CaseBranch{
			{Cond: NewLiteral(int64(1), sql.Int64), Value: NewLiteral(int64(2), sql.Int64)},
			{Cond: NewLiteral(int64(3), sql.Int64), Value: NewLiteral(int64(4), sql.Int64)},
			{Cond: NewLiteral(int64(5), sql.Int64), Value: NewLiteral(int64(6), sql.Int64)},
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
			sql.Row{int64(1)},
			int64(2),
		},
		{
			"with expr and else branch 2",
			f1,
			sql.Row{int64(3)},
			int64(4),
		},
		{
			"with expr and else branch 3",
			f1,
			sql.Row{int64(5)},
			int64(6),
		},
		{
			"with expr and else, else branch",
			f1,
			sql.Row{int64(9)},
			int64(7),
		},
		{
			"without expr and else branch 1",
			f2,
			sql.Row{int64(1)},
			int64(2),
		},
		{
			"without expr and else branch 2",
			f2,
			sql.Row{int64(3)},
			int64(4),
		},
		{
			"without expr and else branch 3",
			f2,
			sql.Row{int64(5)},
			int64(6),
		},
		{
			"without expr and else, else branch",
			f2,
			sql.Row{int64(9)},
			int64(7),
		},
		{
			"without else, else branch",
			f3,
			sql.Row{int64(9)},
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
				Cond:  NewLiteral(int64(i), sql.Int64),
				Value: values[i],
			})
		}
		return &Case{
			nil,
			branches,
			values[len(values)-1],
		}
	}

	decimalType := sql.MustCreateDecimalType(65, 10)

	testCases := []struct {
		name string
		c    *Case
		t    sql.Type
	}{
		{
			"standalone else clause",
			caseExpr(NewLiteral(int64(0), sql.Int64)),
			sql.Int64,
		},
		{
			"unsigned promoted and unsigned",
			caseExpr(NewLiteral(uint32(0), sql.Uint32), NewLiteral(uint32(1), sql.Uint32)),
			sql.Uint64,
		},
		{
			"signed promoted and signed",
			caseExpr(NewLiteral(int8(0), sql.Int8), NewLiteral(int32(1), sql.Int32)),
			sql.Int64,
		},
		{
			"int and float to float",
			caseExpr(NewLiteral(int64(0), sql.Int64), NewLiteral(float64(1.0), sql.Float64)),
			sql.Float64,
		},
		{
			"float and int to float",
			caseExpr(NewLiteral(float64(1.0), sql.Float64), NewLiteral(int64(0), sql.Int64)),
			sql.Float64,
		},
		{
			"int and text to text",
			caseExpr(NewLiteral(int64(0), sql.Int64), NewLiteral("Hello, world!", sql.Text)),
			sql.LongText,
		},
		{
			"text and blob to blob",
			caseExpr(NewLiteral("Hello, world!", sql.Text), NewLiteral([]byte("0x480x650x6c0x6c0x6f"), sql.Blob)),
			sql.LongBlob,
		},
		{
			"int and null to int",
			caseExpr(NewLiteral(int64(10), sql.Int64), NewLiteral(nil, sql.Null)),
			sql.Int64,
		},
		{
			"null and int to int",
			caseExpr(NewLiteral(nil, sql.Null), NewLiteral(int64(10), sql.Int64)),
			sql.Int64,
		},
		{
			"uint64 and int8 to decimal",
			caseExpr(NewLiteral(uint64(10), sql.Uint64), NewLiteral(int8(0), sql.Int8)),
			decimalType,
		},
		{
			"int and text to text",
			caseExpr(NewLiteral(uint64(10), sql.Uint64), NewLiteral("Hello, world!", sql.LongText)),
			sql.LongText,
		},
		{
			"uint and decimal to decimal",
			caseExpr(NewLiteral(uint64(10), sql.Uint64), NewLiteral("Hello, world!", sql.LongText)),
			sql.LongText,
		},
		{
			"int and decimal to decimal",
			caseExpr(NewLiteral(int32(10), sql.Int32), NewLiteral(decimal.NewFromInt(1), decimalType)),
			decimalType,
		},
		{
			"date and date stays date",
			caseExpr(NewLiteral("2020-04-07", sql.Date), NewLiteral("2020-04-07", sql.Date)),
			sql.Date,
		},
		{
			"date and timestamp becomes datetime",
			caseExpr(NewLiteral("2020-04-07", sql.Date), NewLiteral("2020-04-07T00:00:00Z", sql.Timestamp)),
			sql.Datetime,
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
		NewGetField(0, sql.Int64, "x", false),
		[]CaseBranch{
			{
				Cond:  NewLiteral(int64(1), sql.Int64),
				Value: NewLiteral(nil, sql.Null),
			},
		},
		nil,
	)
	result, err := f.Eval(sql.NewEmptyContext(), sql.Row{int64(1)})
	require.NoError(err)
	require.Nil(result)
}
