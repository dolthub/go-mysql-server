// Copyright 2021 Dolthub, Inc.
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

package sql_test

import (
	"fmt"
	"testing"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

var rangeType = types.Uint8

func TestRangeOverlapTwoColumns(t *testing.T) {
	ctx := sql.NewEmptyContext()
	x, y, _, values2, _, _ := setup()

	tests := []struct {
		reference sql.Expression
		ranges    sql.MySQLRangeCollection
	}{
		{
			or(
				and(lt(x, 2), gt(y, 5)),
				and(gt(x, 8), gt(y, 5)),
				and(gt(x, 5), gt(y, 8)),
			),
			sql.MySQLRangeCollection{
				r(rlt(2), rgt(5)),
				r(rgt(8), rgt(5)),
				r(rgt(5), rgt(8)),
			},
		},
		{
			or(
				and(lt(x, 2), gt(y, 5)),
				and(gt(x, 8), gt(y, 5)),
				and(gt(x, 5), lt(y, 8)),
			),
			sql.MySQLRangeCollection{
				r(rlt(2), rgt(5)),
				r(rgt(8), rgt(5)),
				r(rgt(5), rlt(8)),
			},
		},
		{
			or(
				and(gt(x, 1), gt(y, 1)),
				and(gt(x, 2), gt(y, 2)),
				and(gt(x, 3), gt(y, 3)),
				and(gt(x, 4), gt(y, 4)),
				and(gt(x, 5), gt(y, 5)),
				and(gt(x, 6), gt(y, 6)),
				and(gt(x, 7), gt(y, 7)),
				and(gt(x, 8), gt(y, 8)),
				and(gt(x, 9), gt(y, 9)),
				and(lt(x, 1), lt(y, 1)),
				and(lt(x, 2), lt(y, 2)),
				and(lt(x, 3), lt(y, 3)),
				and(lt(x, 4), lt(y, 4)),
				and(lt(x, 5), lt(y, 5)),
				and(lt(x, 6), lt(y, 6)),
				and(lt(x, 7), lt(y, 7)),
				and(lt(x, 8), lt(y, 8)),
				and(lt(x, 9), lt(y, 9)),
			),
			sql.MySQLRangeCollection{
				r(rgt(1), rgt(1)),
				r(rgt(2), rgt(2)),
				r(rgt(3), rgt(3)),
				r(rgt(4), rgt(4)),
				r(rgt(5), rgt(5)),
				r(rgt(6), rgt(6)),
				r(rgt(7), rgt(7)),
				r(rgt(8), rgt(8)),
				r(rgt(9), rgt(9)),
				r(rlt(1), rlt(1)),
				r(rlt(2), rlt(2)),
				r(rlt(3), rlt(3)),
				r(rlt(4), rlt(4)),
				r(rlt(5), rlt(5)),
				r(rlt(6), rlt(6)),
				r(rlt(7), rlt(7)),
				r(rlt(8), rlt(8)),
				r(rlt(9), rlt(9)),
			},
		},
		{
			or(
				and(gt(x, 2), gt(y, 2)),
				and(eq(x, 4), eq(y, 4)),
				and(lt(x, 9), lt(y, 9)),
				and(eq(x, 6), eq(y, 6)),
				and(eq(x, 8), eq(y, 8)),
			),
			sql.MySQLRangeCollection{
				r(rgt(2), rgt(2)),
				r(req(4), req(4)),
				r(rlt(9), rlt(9)),
				r(req(6), req(6)),
				r(req(8), req(8)),
			},
		},
		{
			or(
				and(gt(x, 2), gt(y, 2)),
				and(eq(x, 4), eq(y, 4)),
				and(lt(x, 9), lt(y, 9)),
				and(eq(x, 6), eq(y, 6)),
				and(eq(x, 8), eq(y, 8)),
			),
			sql.MySQLRangeCollection{
				r(rgt(2), rgt(2)),
				r(req(4), req(4)),
				r(rlt(9), rlt(9)),
				r(req(6), req(6)),
				r(req(8), req(8)),
			},
		},
		{
			or(
				and(cc(x, 2, 6), cc(y, 5, 10)),
				and(cc(x, 3, 7), cc(y, 1, 4)),
				and(oo(x, 4, 8), oo(y, 2, 5)),
				and(oc(x, 5, 10), oc(y, 4, 7)),
			),
			sql.MySQLRangeCollection{
				r(rcc(2, 6), rcc(5, 10)),
				r(rcc(3, 7), rcc(1, 4)),
				r(roo(4, 8), roo(2, 5)),
				r(roc(5, 10), roc(4, 7)),
			},
		},
		{
			or(
				and(cc(x, 1, 6), cc(y, 1, 3)),
				and(cc(x, 1, 2), cc(y, 1, 3)),
				and(cc(x, 3, 4), cc(y, 1, 3)),
				and(cc(x, 5, 6), cc(y, 1, 3)),
				and(cc(x, 2, 3), cc(y, 1, 2)),
			),
			sql.MySQLRangeCollection{
				r(rcc(1, 6), rcc(1, 3)),
				r(rcc(1, 2), rcc(1, 3)),
				r(rcc(3, 4), rcc(1, 3)),
				r(rcc(5, 6), rcc(1, 3)),
			},
		},
		{
			or(
				and(cc(x, 1, 6), cc(y, 4, 7)),
				and(cc(x, 4, 5), cc(y, 3, 8)),
				and(cc(x, 3, 8), cc(y, 1, 6)),
			),
			sql.MySQLRangeCollection{
				r(rcc(1, 6), rcc(4, 7)),
				r(rcc(4, 5), rcc(3, 8)),
				r(rcc(3, 8), rcc(1, 6)),
			},
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("Expr:  %s\nRange: %s", test.reference.String(), test.ranges.DebugString()), func(t *testing.T) {
			discreteRanges, err := sql.RemoveOverlappingRanges(ctx, test.ranges...)
			require.NoError(t, err)
			verificationRanges, err := removeOverlappingRangesVerification(ctx, test.ranges...)
			require.NoError(t, err)
			for _, row := range values2 {
				referenceBool, err := test.reference.Eval(ctx, row)
				require.NoError(t, err)
				rangeBool := evalRanges(t, ctx, discreteRanges, row)
				assert.Equal(t, referenceBool, rangeBool, fmt.Sprintf("%v: DiscreteRanges: %s", row, discreteRanges.DebugString()))
			}
			discreteRanges, err = sql.SortRanges(ctx, discreteRanges...)
			require.NoError(t, err)
			verificationRanges, err = sql.SortRanges(ctx, verificationRanges...)
			require.NoError(t, err)
			ok, err := discreteRanges.Equals(ctx, verificationRanges)
			require.NoError(t, err)
			assert.True(t, ok)
		})
	}
}

func TestRangeOverlapThreeColumns(t *testing.T) {
	ctx := sql.NewEmptyContext()
	x, y, z, _, values3, _ := setup()

	tests := []struct {
		reference sql.Expression
		ranges    sql.MySQLRangeCollection
	}{
		{
			or(
				and(gt(x, 2), gt(y, 2), gt(z, 2)),
				and(lt(x, 8), lt(y, 8), lt(z, 8)),
			),
			sql.MySQLRangeCollection{
				r(rgt(2), rgt(2), rgt(2)),
				r(rlt(8), rlt(8), rlt(8)),
			},
		},
		{
			or(
				and(gte(x, 3), gte(y, 3), gte(z, 3)),
				and(lte(x, 5), lte(y, 5), lte(z, 5)),
			),
			sql.MySQLRangeCollection{
				r(rgte(3), rgte(3), rgte(3)),
				r(rlte(5), rlte(5), rlte(5)),
			},
		},
		{
			or(
				and(gte(x, 3), gte(y, 4), gt(z, 5)),
				and(lte(x, 6), lt(y, 7), lte(z, 8)),
			),
			sql.MySQLRangeCollection{
				r(rgte(3), rgte(4), rgt(5)),
				r(rlte(6), rlt(7), rlte(8)),
			},
		},
		{
			or(
				and(gte(x, 4), gte(y, 4), gte(z, 3)),
				and(lte(x, 4), lte(y, 4), lte(z, 5)),
			),
			sql.MySQLRangeCollection{
				r(rgte(4), rgte(4), rgte(3)),
				r(rlte(4), rlte(4), rlte(5)),
			},
		},
		{
			or(
				and(gte(x, 4), gte(y, 3), gte(z, 4)),
				and(lte(x, 4), lte(y, 5), lte(z, 4)),
			),
			sql.MySQLRangeCollection{
				r(rgte(4), rgte(3), rgte(4)),
				r(rlte(4), rlte(5), rlte(4)),
			},
		},
		{
			or(
				and(gte(x, 3), gte(y, 4), gte(z, 4)),
				and(lte(x, 5), lte(y, 4), lte(z, 4)),
			),
			sql.MySQLRangeCollection{
				r(rgte(3), rgte(4), rgte(4)),
				r(rlte(5), rlte(4), rlte(4)),
			},
		},
		{
			or(
				and(lt(x, 4), gte(y, 3), lt(z, 4)),
				and(gt(x, 4), lte(y, 5), gt(z, 4)),
			),
			sql.MySQLRangeCollection{
				r(rlt(4), rgte(3), rlt(4)),
				r(rgt(4), rlte(5), rgt(4)),
			},
		},
		{
			or(
				and(gt(x, 3), gt(y, 2), eq(z, 2)),
				and(lt(x, 4), gte(y, 7), lte(z, 2)),
				and(lte(x, 9), gt(y, 5), gt(z, 1)),
			),
			sql.MySQLRangeCollection{
				r(rgt(3), rgt(2), req(2)),
				r(rlt(4), rgte(7), rlte(2)),
				r(rlte(9), rgt(5), rgt(1)),
			},
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("Expr:  %s\nRange: %s", test.reference.String(), test.ranges.DebugString()), func(t *testing.T) {
			discreteRanges, err := sql.RemoveOverlappingRanges(ctx, test.ranges...)
			require.NoError(t, err)
			verificationRanges, err := removeOverlappingRangesVerification(ctx, test.ranges...)
			require.NoError(t, err)
			for _, row := range values3 {
				referenceBool, err := test.reference.Eval(ctx, row)
				require.NoError(t, err)
				rangeBool := evalRanges(t, ctx, discreteRanges, row)
				assert.Equal(t, referenceBool, rangeBool, fmt.Sprintf("%v: DiscreteRanges: %s", row, discreteRanges.DebugString()))
			}
			discreteRanges, err = sql.SortRanges(ctx, discreteRanges...)
			require.NoError(t, err)
			verificationRanges, err = sql.SortRanges(ctx, verificationRanges...)
			require.NoError(t, err)
			ok, err := discreteRanges.Equals(ctx, verificationRanges)
			require.NoError(t, err)
			assert.True(t, ok)
		})
	}
}

func TestRangeOverlapNulls(t *testing.T) {
	ctx := sql.NewEmptyContext()
	x, y, _, _, _, valuesNull := setup()

	tests := []struct {
		reference sql.Expression
		ranges    sql.MySQLRangeCollection
	}{
		{
			reference: or(
				and(isNull(x), gt(y, 5)),
			),
			ranges: sql.MySQLRangeCollection{
				r(null(), rgt(5)),
			},
		},
		{
			reference: or(
				and(isNull(x), isNotNull(y)),
			),
			ranges: sql.MySQLRangeCollection{
				r(null(), notNull()),
			},
		},
		{
			reference: or(
				and(isNull(x), lt(y, 5)),
			),
			ranges: sql.MySQLRangeCollection{
				r(null(), rlt(5)),
			},
		},
		{
			reference: or(
				and(isNull(x), gte(y, 5)),
			),
			ranges: sql.MySQLRangeCollection{
				r(null(), rgte(5)),
			},
		},
		{
			reference: or(
				and(isNull(x), lte(y, 5)),
			),
			ranges: sql.MySQLRangeCollection{
				r(null(), rlte(5)),
			},
		},
		{
			reference: or(
				and(isNull(x), lte(y, 5)),
			),
			ranges: sql.MySQLRangeCollection{
				r(null(), rlte(5)),
			},
		},
		{
			reference: or(
				and(isNull(x), eq(y, 1)),
			),
			ranges: sql.MySQLRangeCollection{
				r(null(), req(1)),
			},
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("Expr:  %s\nRange: %s", test.reference.String(), test.ranges.DebugString()), func(t *testing.T) {
			discreteRanges, err := sql.RemoveOverlappingRanges(ctx, test.ranges...)
			require.NoError(t, err)
			verificationRanges, err := removeOverlappingRangesVerification(ctx, test.ranges...)
			require.NoError(t, err)
			for _, row := range valuesNull {
				referenceBool, err := test.reference.Eval(ctx, row)
				require.NoError(t, err)
				rangeBool := evalRanges(t, ctx, discreteRanges, row)
				assert.Equal(t, referenceBool, rangeBool, fmt.Sprintf("%v: DiscreteRanges: %s", row, discreteRanges.DebugString()))
			}
			discreteRanges, err = sql.SortRanges(ctx, discreteRanges...)
			require.NoError(t, err)
			verificationRanges, err = sql.SortRanges(ctx, verificationRanges...)
			require.NoError(t, err)
			ok, err := discreteRanges.Equals(ctx, verificationRanges)
			require.NoError(t, err)
			assert.True(t, ok)
		})
	}
}

func TestComplexRange(t *testing.T) {
	tests := []struct {
		skip   bool
		ranges sql.MySQLRangeCollection
	}{
		{
			// derived from sqllogictest/index/in/100/slt_good_1.test:12655
			ranges: sql.MySQLRangeCollection{
				r(
					sql.MySQLRangeColumnExpr{LowerBound: sql.NewBelowNullBound(), UpperBound: sql.NewAboveAllBound(), Typ: types.Int16},
					sql.MySQLRangeColumnExpr{LowerBound: sql.NewBelowNullBound(), UpperBound: sql.NewAboveAllBound(), Typ: types.Int16},
					sql.MySQLRangeColumnExpr{LowerBound: sql.NewBelowNullBound(), UpperBound: sql.NewAboveNullBound(), Typ: types.Float32},
				),
				r(
					sql.MySQLRangeColumnExpr{LowerBound: sql.NewAboveNullBound(), UpperBound: sql.NewBound(int16(848), sql.Below), Typ: types.Int16},
					sql.MySQLRangeColumnExpr{LowerBound: sql.NewBound(int16(560), sql.Above), UpperBound: sql.NewAboveAllBound(), Typ: types.Int16},
					sql.MySQLRangeColumnExpr{LowerBound: sql.NewBelowNullBound(), UpperBound: sql.NewAboveAllBound(), Typ: types.Float32},
				),
				r(
					sql.MySQLRangeColumnExpr{LowerBound: sql.NewAboveNullBound(), UpperBound: sql.NewBound(953, sql.Above), Typ: types.Int16},
					sql.MySQLRangeColumnExpr{LowerBound: sql.NewBelowNullBound(), UpperBound: sql.NewAboveAllBound(), Typ: types.Int16},
					sql.MySQLRangeColumnExpr{LowerBound: sql.NewBound(decimal.New(53978, -2), sql.Below), UpperBound: sql.NewAboveAllBound(), Typ: types.Float32},
				),
				r(
					sql.MySQLRangeColumnExpr{LowerBound: sql.NewBound(int16(234), sql.Below), UpperBound: sql.NewBound(int16(234), sql.Above), Typ: types.Int16},
					sql.MySQLRangeColumnExpr{LowerBound: sql.NewBelowNullBound(), UpperBound: sql.NewAboveAllBound(), Typ: types.Int16},
					sql.MySQLRangeColumnExpr{LowerBound: sql.NewBound(decimal.New(48843, -2), sql.Below), UpperBound: sql.NewAboveAllBound(), Typ: types.Float32},
				),
				r(
					sql.MySQLRangeColumnExpr{LowerBound: sql.NewBound(int16(258), sql.Below), UpperBound: sql.NewBound(int16(258), sql.Above), Typ: types.Int16},
					sql.MySQLRangeColumnExpr{LowerBound: sql.NewBelowNullBound(), UpperBound: sql.NewAboveAllBound(), Typ: types.Int16},
					sql.MySQLRangeColumnExpr{LowerBound: sql.NewBelowNullBound(), UpperBound: sql.NewAboveAllBound(), Typ: types.Float32},
				),
				r(
					sql.MySQLRangeColumnExpr{LowerBound: sql.NewBound(int16(372), sql.Below), UpperBound: sql.NewBound(int16(372), sql.Above), Typ: types.Int16},
					sql.MySQLRangeColumnExpr{LowerBound: sql.NewBelowNullBound(), UpperBound: sql.NewAboveAllBound(), Typ: types.Int16},
					sql.MySQLRangeColumnExpr{LowerBound: sql.NewBound(decimal.New(48843, -2), sql.Below), UpperBound: sql.NewAboveAllBound(), Typ: types.Float32},
				),
				r(
					sql.MySQLRangeColumnExpr{LowerBound: sql.NewBound(int16(583), sql.Below), UpperBound: sql.NewBound(int16(583), sql.Above), Typ: types.Int16},
					sql.MySQLRangeColumnExpr{LowerBound: sql.NewBelowNullBound(), UpperBound: sql.NewAboveAllBound(), Typ: types.Int16},
					sql.MySQLRangeColumnExpr{LowerBound: sql.NewBound(decimal.New(48843, -2), sql.Below), UpperBound: sql.NewAboveAllBound(), Typ: types.Float32},
				),
				r(
					sql.MySQLRangeColumnExpr{LowerBound: sql.NewBound(int16(883), sql.Above), UpperBound: sql.NewAboveAllBound(), Typ: types.Int16},
					sql.MySQLRangeColumnExpr{LowerBound: sql.NewBelowNullBound(), UpperBound: sql.NewAboveAllBound(), Typ: types.Int16},
					sql.MySQLRangeColumnExpr{LowerBound: sql.NewBelowNullBound(), UpperBound: sql.NewAboveAllBound(), Typ: types.Float32},
				),
			},
		},
		{
			// derived from index query plan test
			// `SELECT * FROM comp_index_t2 WHERE (((v1>25 AND v2 BETWEEN 23 AND 54) OR (v1<>40 AND v3>90)) OR (v1<>7 AND v4<=78));`
			ranges: sql.MySQLRangeCollection{
				r(
					sql.MySQLRangeColumnExpr{LowerBound: sql.NewBound(25, sql.Above), UpperBound: sql.NewAboveAllBound(), Typ: types.Int32},
					sql.MySQLRangeColumnExpr{LowerBound: sql.NewBound(23, sql.Below), UpperBound: sql.NewBound(54, sql.Above), Typ: types.Int32},
					sql.MySQLRangeColumnExpr{LowerBound: sql.NewBelowNullBound(), UpperBound: sql.NewAboveAllBound(), Typ: types.Int32},
					sql.MySQLRangeColumnExpr{LowerBound: sql.NewBelowNullBound(), UpperBound: sql.NewAboveAllBound(), Typ: types.Int32},
				),
				r(
					sql.MySQLRangeColumnExpr{LowerBound: sql.NewBound(40, sql.Above), UpperBound: sql.NewAboveAllBound(), Typ: types.Int32},
					sql.MySQLRangeColumnExpr{LowerBound: sql.NewBelowNullBound(), UpperBound: sql.NewAboveAllBound(), Typ: types.Int32},
					sql.MySQLRangeColumnExpr{LowerBound: sql.NewBound(90, sql.Above), UpperBound: sql.NewAboveAllBound(), Typ: types.Int32},
					sql.MySQLRangeColumnExpr{LowerBound: sql.NewBelowNullBound(), UpperBound: sql.NewAboveAllBound(), Typ: types.Int32},
				),
				r(
					sql.MySQLRangeColumnExpr{LowerBound: sql.NewAboveNullBound(), UpperBound: sql.NewBound(40, sql.Below), Typ: types.Int32},
					sql.MySQLRangeColumnExpr{LowerBound: sql.NewBelowNullBound(), UpperBound: sql.NewAboveAllBound(), Typ: types.Int32},
					sql.MySQLRangeColumnExpr{LowerBound: sql.NewBound(90, sql.Above), UpperBound: sql.NewAboveAllBound(), Typ: types.Int32},
					sql.MySQLRangeColumnExpr{LowerBound: sql.NewBelowNullBound(), UpperBound: sql.NewAboveAllBound(), Typ: types.Int32},
				),
				r(
					sql.MySQLRangeColumnExpr{LowerBound: sql.NewBound(7, sql.Above), UpperBound: sql.NewAboveAllBound(), Typ: types.Int32},
					sql.MySQLRangeColumnExpr{LowerBound: sql.NewBelowNullBound(), UpperBound: sql.NewAboveAllBound(), Typ: types.Int32},
					sql.MySQLRangeColumnExpr{LowerBound: sql.NewBelowNullBound(), UpperBound: sql.NewAboveAllBound(), Typ: types.Int32},
					sql.MySQLRangeColumnExpr{LowerBound: sql.NewAboveNullBound(), UpperBound: sql.NewBound(78, sql.Above), Typ: types.Int32},
				),
				r(
					sql.MySQLRangeColumnExpr{LowerBound: sql.NewAboveNullBound(), UpperBound: sql.NewBound(7, sql.Below), Typ: types.Int32},
					sql.MySQLRangeColumnExpr{LowerBound: sql.NewBelowNullBound(), UpperBound: sql.NewAboveAllBound(), Typ: types.Int32},
					sql.MySQLRangeColumnExpr{LowerBound: sql.NewBelowNullBound(), UpperBound: sql.NewAboveAllBound(), Typ: types.Int32},
					sql.MySQLRangeColumnExpr{LowerBound: sql.NewAboveNullBound(), UpperBound: sql.NewBound(78, sql.Above), Typ: types.Int32},
				),
			},
		},
		{
			ranges: sql.MySQLRangeCollection{
				r(
					sql.MySQLRangeColumnExpr{LowerBound: sql.NewBound(0, sql.Below), UpperBound: sql.NewBound(6, sql.Above), Typ: types.Int16},
					sql.MySQLRangeColumnExpr{LowerBound: sql.NewBound(0, sql.Below), UpperBound: sql.NewBound(6, sql.Above), Typ: types.Int16},
					sql.MySQLRangeColumnExpr{LowerBound: sql.NewBound(0, sql.Below), UpperBound: sql.NewBound(0, sql.Above), Typ: types.Int16},
				),
				r(
					sql.MySQLRangeColumnExpr{LowerBound: sql.NewBound(0, sql.Above), UpperBound: sql.NewBound(5, sql.Below), Typ: types.Int16},
					sql.MySQLRangeColumnExpr{LowerBound: sql.NewBound(3, sql.Above), UpperBound: sql.NewBound(6, sql.Above), Typ: types.Int16},
					sql.MySQLRangeColumnExpr{LowerBound: sql.NewBound(0, sql.Below), UpperBound: sql.NewBound(6, sql.Above), Typ: types.Int16},
				),
				r(
					sql.MySQLRangeColumnExpr{LowerBound: sql.NewBound(1, sql.Below), UpperBound: sql.NewBound(1, sql.Above), Typ: types.Int16},
					sql.MySQLRangeColumnExpr{LowerBound: sql.NewBound(0, sql.Below), UpperBound: sql.NewBound(6, sql.Above), Typ: types.Int16},
					sql.MySQLRangeColumnExpr{LowerBound: sql.NewBound(0, sql.Below), UpperBound: sql.NewBound(6, sql.Above), Typ: types.Int16},
				),
				r(
					sql.MySQLRangeColumnExpr{LowerBound: sql.NewBound(2, sql.Below), UpperBound: sql.NewBound(2, sql.Above), Typ: types.Int16},
					sql.MySQLRangeColumnExpr{LowerBound: sql.NewBound(0, sql.Below), UpperBound: sql.NewBound(6, sql.Above), Typ: types.Int16},
					sql.MySQLRangeColumnExpr{LowerBound: sql.NewBound(1, sql.Below), UpperBound: sql.NewBound(6, sql.Above), Typ: types.Int16},
				),
				r(
					sql.MySQLRangeColumnExpr{LowerBound: sql.NewBound(4, sql.Below), UpperBound: sql.NewBound(4, sql.Above), Typ: types.Int16},
					sql.MySQLRangeColumnExpr{LowerBound: sql.NewBound(0, sql.Below), UpperBound: sql.NewBound(6, sql.Above), Typ: types.Int16},
					sql.MySQLRangeColumnExpr{LowerBound: sql.NewBound(1, sql.Below), UpperBound: sql.NewBound(6, sql.Above), Typ: types.Int16},
				),
			},
		},
		{
			ranges: sql.MySQLRangeCollection{
				r(
					sql.MySQLRangeColumnExpr{LowerBound: sql.NewBound(69, sql.Below), UpperBound: sql.NewBound(69, sql.Above), Typ: types.Int32},
					sql.MySQLRangeColumnExpr{LowerBound: sql.NewBelowNullBound(), UpperBound: sql.NewAboveAllBound(), Typ: types.Float32},
				),
				r(
					sql.MySQLRangeColumnExpr{LowerBound: sql.NewBound(73, sql.Below), UpperBound: sql.NewBound(73, sql.Above), Typ: types.Int32},
					sql.MySQLRangeColumnExpr{LowerBound: sql.NewBelowNullBound(), UpperBound: sql.NewAboveAllBound(), Typ: types.Float32},
				),
				r(
					sql.MySQLRangeColumnExpr{LowerBound: sql.NewBound(12, sql.Below), UpperBound: sql.NewBound(12, sql.Above), Typ: types.Int32},
					sql.MySQLRangeColumnExpr{LowerBound: sql.NewBelowNullBound(), UpperBound: sql.NewAboveAllBound(), Typ: types.Float32},
				),
				r(
					sql.MySQLRangeColumnExpr{LowerBound: sql.NewBound(3, sql.Below), UpperBound: sql.NewBound(3, sql.Above), Typ: types.Int32},
					sql.MySQLRangeColumnExpr{LowerBound: sql.NewBelowNullBound(), UpperBound: sql.NewAboveAllBound(), Typ: types.Float32},
				),
				r(
					sql.MySQLRangeColumnExpr{LowerBound: sql.NewBound(17, sql.Below), UpperBound: sql.NewBound(17, sql.Above), Typ: types.Int32},
					sql.MySQLRangeColumnExpr{LowerBound: sql.NewBelowNullBound(), UpperBound: sql.NewAboveAllBound(), Typ: types.Float32},
				),
				r(
					sql.MySQLRangeColumnExpr{LowerBound: sql.NewBound(70, sql.Below), UpperBound: sql.NewBound(70, sql.Above), Typ: types.Int32},
					sql.MySQLRangeColumnExpr{LowerBound: sql.NewBelowNullBound(), UpperBound: sql.NewAboveAllBound(), Typ: types.Float32},
				),
				r(
					sql.MySQLRangeColumnExpr{LowerBound: sql.NewBound(20, sql.Below), UpperBound: sql.NewBound(20, sql.Above), Typ: types.Int32},
					sql.MySQLRangeColumnExpr{LowerBound: sql.NewBelowNullBound(), UpperBound: sql.NewAboveAllBound(), Typ: types.Float32},
				),
				r(
					sql.MySQLRangeColumnExpr{LowerBound: sql.NewBound(4, sql.Below), UpperBound: sql.NewBound(4, sql.Above), Typ: types.Int32},
					sql.MySQLRangeColumnExpr{LowerBound: sql.NewBelowNullBound(), UpperBound: sql.NewAboveAllBound(), Typ: types.Float32},
				),
				r(
					sql.MySQLRangeColumnExpr{LowerBound: sql.NewBound(39, sql.Below), UpperBound: sql.NewBound(39, sql.Above), Typ: types.Int32},
					sql.MySQLRangeColumnExpr{LowerBound: sql.NewBelowNullBound(), UpperBound: sql.NewAboveAllBound(), Typ: types.Float32},
				),
				r(
					sql.MySQLRangeColumnExpr{LowerBound: sql.NewBelowNullBound(), UpperBound: sql.NewAboveAllBound(), Typ: types.Int32},
					sql.MySQLRangeColumnExpr{LowerBound: sql.NewAboveNullBound(), UpperBound: sql.NewBound(69.67, sql.Below), Typ: types.Float32},
				),
			},
		},
	}

	ctx := sql.NewEmptyContext()
	for _, test := range tests {
		t.Run(fmt.Sprintf("Range: %s", test.ranges.DebugString()), func(t *testing.T) {
			if test.skip {
				t.Skip()
			}
			discreteRanges, err := sql.RemoveOverlappingRanges(ctx, test.ranges...)
			require.NoError(t, err)
			verificationRanges, err := removeOverlappingRangesVerification(ctx, test.ranges...)
			require.NoError(t, err)
			discreteRanges, err = sql.SortRanges(ctx, discreteRanges...)
			require.NoError(t, err)
			verificationRanges, err = sql.SortRanges(ctx, verificationRanges...)
			require.NoError(t, err)
			ok, err := discreteRanges.Equals(ctx, verificationRanges)
			require.NoError(t, err)
			assert.True(t, ok)
			if !ok {
				t.Logf("DiscreteRanges: %s", discreteRanges.DebugString())
				t.Logf("VerificationRanges: %s", verificationRanges.DebugString())
			}

			// TODO: need a way to either verify that the ranges cover the area, or that they're the same
			for i := 0; i < len(discreteRanges)-1; i++ {
				for j := i + 1; j < len(discreteRanges); j++ {
					r1 := discreteRanges[i]
					r2 := discreteRanges[j]
					hasOverlap, err := r1.Overlaps(ctx, r2)
					if hasOverlap {
						t.Logf("Overlap: %s\n%s", r1.String(), r2.String())
					}
					assert.NoError(t, err)
					assert.False(t, hasOverlap)
				}
			}
		})
	}
}

func setup() (x, y, z sql.Expression, values2, values3, valuesNull [][]interface{}) {
	values2 = make([][]interface{}, 0, 100)
	values3 = make([][]interface{}, 0, 1000)
	for i := byte(1); i <= 10; i++ {
		for j := byte(1); j <= 10; j++ {
			for k := byte(1); k <= 10; k++ {
				values3 = append(values3, []interface{}{i, j, k})
			}
			values2 = append(values2, []interface{}{i, j})
			if i%2 == 0 {
				valuesNull = append(valuesNull, []interface{}{nil, j})
			} else {
				valuesNull = append(valuesNull, []interface{}{i, j})
			}
		}
	}
	x = expression.NewGetField(0, rangeType, "x", true)
	y = expression.NewGetField(1, rangeType, "y", true)
	z = expression.NewGetField(2, rangeType, "z", true)
	return
}

func evalRanges(t *testing.T, ctx *sql.Context, ranges []sql.MySQLRange, row []interface{}) bool {
	found := false
	for _, rang := range ranges {
		if evalRange(t, ctx, rang, row) {
			if !found {
				found = true
			} else {
				assert.FailNow(t, "overlap in ranges")
			}
		}
	}
	return found
}

func evalRange(t *testing.T, ctx *sql.Context, rang sql.MySQLRange, row []interface{}) bool {
	rowRange := make(sql.MySQLRange, len(rang))
	for i, val := range row {
		if val == nil {
			rowRange[i] = sql.NullRangeColumnExpr(rangeType)
		} else {
			rowRange[i] = sql.ClosedRangeColumnExpr(val, val, rangeType)
		}
	}
	ok, err := rang.IsSupersetOf(ctx, rowRange)
	require.NoError(t, err)
	return ok
}

func removeOverlappingRangesVerification(ctx *sql.Context, ranges ...sql.MySQLRange) (sql.MySQLRangeCollection, error) {
	if len(ranges) == 0 {
		return nil, nil
	}

	var newRanges sql.MySQLRangeCollection
	for i := 0; i < len(ranges); i++ {
		hadOverlap := false
		for nri := 0; nri < len(newRanges); nri++ {
			if resultingRanges, ok, err := ranges[i].RemoveOverlap(ctx, newRanges[nri]); err != nil {
				return nil, err
			} else if ok {
				hadOverlap = true
				// Remove the overlapping Range from newRanges
				nrLast := len(newRanges) - 1
				newRanges[nri], newRanges[nrLast] = newRanges[nrLast], newRanges[nri]
				newRanges = newRanges[:nrLast]
				// Add the new ranges to the end of the given slice allowing us to compare those against everything else.
				ranges = append(ranges, resultingRanges...)
				break
			}
		}
		if !hadOverlap {
			newRanges = append(newRanges, ranges[i])
		}
	}

	return newRanges, nil
}

func eq(field sql.Expression, val uint8) sql.Expression {
	return expression.NewNullSafeEquals(field, expression.NewLiteral(val, rangeType))
}

func lt(field sql.Expression, val uint8) sql.Expression {
	return expression.NewLessThan(field, expression.NewLiteral(val, rangeType))
}

func lte(field sql.Expression, val uint8) sql.Expression {
	return expression.NewLessThanOrEqual(field, expression.NewLiteral(val, rangeType))
}

func gt(field sql.Expression, val uint8) sql.Expression {
	return expression.NewGreaterThan(field, expression.NewLiteral(val, rangeType))
}

func gte(field sql.Expression, val uint8) sql.Expression {
	return expression.NewGreaterThanOrEqual(field, expression.NewLiteral(val, rangeType))
}

func isNull(field sql.Expression) sql.Expression {
	return expression.NewIsNull(field)
}

func isNotNull(field sql.Expression) sql.Expression {
	return expression.NewNot(expression.NewIsNull(field))
}

func cc(field sql.Expression, lowerbound, upperbound uint8) sql.Expression {
	return and(
		expression.NewGreaterThanOrEqual(field, expression.NewLiteral(lowerbound, rangeType)),
		expression.NewLessThanOrEqual(field, expression.NewLiteral(upperbound, rangeType)),
	)
}

func co(field sql.Expression, lowerbound, upperbound uint8) sql.Expression {
	return and(
		expression.NewGreaterThanOrEqual(field, expression.NewLiteral(lowerbound, rangeType)),
		expression.NewLessThan(field, expression.NewLiteral(upperbound, rangeType)),
	)
}

func oc(field sql.Expression, lowerbound, upperbound uint8) sql.Expression {
	return and(
		expression.NewGreaterThan(field, expression.NewLiteral(lowerbound, rangeType)),
		expression.NewLessThanOrEqual(field, expression.NewLiteral(upperbound, rangeType)),
	)
}

func oo(field sql.Expression, lowerbound, upperbound uint8) sql.Expression {
	return and(
		expression.NewGreaterThan(field, expression.NewLiteral(lowerbound, rangeType)),
		expression.NewLessThan(field, expression.NewLiteral(upperbound, rangeType)),
	)
}

func not(field sql.Expression, val uint8) sql.Expression {
	return expression.NewNot(eq(field, val))
}

func r(colExprs ...sql.MySQLRangeColumnExpr) sql.MySQLRange {
	return colExprs
}

func req(val byte) sql.MySQLRangeColumnExpr {
	return sql.ClosedRangeColumnExpr(val, val, rangeType)
}

func rlt(val byte) sql.MySQLRangeColumnExpr {
	return sql.LessThanRangeColumnExpr(val, rangeType)
}

func rlte(val byte) sql.MySQLRangeColumnExpr {
	return sql.LessOrEqualRangeColumnExpr(val, rangeType)
}

func rgt(val byte) sql.MySQLRangeColumnExpr {
	return sql.GreaterThanRangeColumnExpr(val, rangeType)
}

func rgte(val byte) sql.MySQLRangeColumnExpr {
	return sql.GreaterOrEqualRangeColumnExpr(val, rangeType)
}

func null() sql.MySQLRangeColumnExpr {
	return sql.NullRangeColumnExpr(rangeType)
}

func notNull() sql.MySQLRangeColumnExpr {
	return sql.NotNullRangeColumnExpr(rangeType)
}

func rcc(lowerbound, upperbound byte) sql.MySQLRangeColumnExpr {
	return newRangeColumnExpr(lowerbound, upperbound, sql.Below, sql.Above, rangeType)
}

func rco(lowerbound, upperbound byte) sql.MySQLRangeColumnExpr {
	return newRangeColumnExpr(lowerbound, upperbound, sql.Below, sql.Below, rangeType)
}

func roc(lowerbound, upperbound byte) sql.MySQLRangeColumnExpr {
	return newRangeColumnExpr(lowerbound, upperbound, sql.Above, sql.Above, rangeType)
}

func roo(lowerbound, upperbound byte) sql.MySQLRangeColumnExpr {
	return newRangeColumnExpr(lowerbound, upperbound, sql.Above, sql.Below, rangeType)
}

// CustomRangeColumnExpr returns a MySQLRangeColumnExpr defined by the bounds given.
func newRangeColumnExpr(lower, upper interface{}, lBoundType, uBoundType sql.BoundType, typ sql.Type) sql.MySQLRangeColumnExpr {
	if lower == nil || upper == nil {
		return sql.EmptyRangeColumnExpr(typ)
	}

	lBound := sql.NewBound(lower, lBoundType)
	uBound := sql.NewBound(upper, uBoundType)
	return sql.MySQLRangeColumnExpr{
		LowerBound: lBound,
		UpperBound: uBound,
		Typ:        typ,
	}
}

func or(expressions ...sql.Expression) sql.Expression {
	if len(expressions) == 1 {
		return expressions[0]
	}
	if expressions[0] == nil {
		return or(expressions[1:]...)
	}
	return expression.NewOr(expressions[0], or(expressions[1:]...))
}

func and(expressions ...sql.Expression) sql.Expression {
	if len(expressions) == 1 {
		return expressions[0]
	}
	if expressions[0] == nil {
		return and(expressions[1:]...)
	}
	return expression.NewAnd(expressions[0], and(expressions[1:]...))
}

func buildTestRangeTree(ctx *sql.Context, ranges []sql.MySQLRange) (*sql.MySQLRangeColumnExprTree, error) {
	tree, err := sql.NewMySQLRangeColumnExprTree(ranges[0], []sql.Type{rangeType})
	if err != nil {
		return nil, err
	}
	for _, rng := range ranges[1:] {
		err = tree.Insert(ctx, rng)
		if err != nil {
			return nil, err
		}
	}
	return tree, nil
}

func TestRangeTreeInsert(t *testing.T) {
	tests := []struct {
		name      string
		setupRngs []sql.MySQLRange
		setupExp  string
		rng       sql.MySQLRange
		exp       string
	}{
		{
			name:      "insert smallest",
			setupRngs: []sql.MySQLRange{r(req(7)), r(req(3)), r(req(11))},
			setupExp: "RangeColumnExprTree\n" +
				"│   ┌── [11, 11] max: Above[11] color: 1\n" +
				"└── [7, 7] max: Above[11] color: 0\n" +
				"    └── [3, 3] max: Above[3] color: 1\n" +
				"",
			rng: r(req(0)),
			exp: "RangeColumnExprTree\n" +
				"│   ┌── [11, 11] max: Above[11] color: 0\n" +
				"└── [7, 7] max: Above[11] color: 0\n" +
				"    └── [3, 3] max: Above[3] color: 0\n" +
				"        └── [0, 0] max: Above[0] color: 1\n" +
				"",
		},
		{
			name:      "insert largest",
			setupRngs: []sql.MySQLRange{r(req(7)), r(req(3)), r(req(11))},
			setupExp: "RangeColumnExprTree\n" +
				"│   ┌── [11, 11] max: Above[11] color: 1\n" +
				"└── [7, 7] max: Above[11] color: 0\n" +
				"    └── [3, 3] max: Above[3] color: 1\n" +
				"",
			rng: r(req(14)),
			exp: "RangeColumnExprTree\n" +
				"│       ┌── [14, 14] max: Above[14] color: 1\n" +
				"│   ┌── [11, 11] max: Above[14] color: 0\n" +
				"└── [7, 7] max: Above[14] color: 0\n" +
				"    └── [3, 3] max: Above[3] color: 0\n" +
				"",
		},
		{
			name:      "insert rebalance left child",
			setupRngs: []sql.MySQLRange{r(req(7)), r(req(3)), r(req(11)), r(req(0))},
			setupExp: "RangeColumnExprTree\n" +
				"│   ┌── [11, 11] max: Above[11] color: 0\n" +
				"└── [7, 7] max: Above[11] color: 0\n" +
				"    └── [3, 3] max: Above[3] color: 0\n" +
				"        └── [0, 0] max: Above[0] color: 1\n" +
				"",
			rng: r(req(1)),
			exp: "RangeColumnExprTree\n" +
				"│   ┌── [11, 11] max: Above[11] color: 0\n" +
				"└── [7, 7] max: Above[11] color: 0\n" +
				"    │   ┌── [3, 3] max: Above[3] color: 1\n" +
				"    └── [1, 1] max: Above[3] color: 0\n" +
				"        └── [0, 0] max: Above[0] color: 1\n" +
				"",
		},
		{
			name:      "insert rebalance right child",
			setupRngs: []sql.MySQLRange{r(req(7)), r(req(3)), r(req(11)), r(req(12))},
			setupExp: "RangeColumnExprTree\n" +
				"│       ┌── [12, 12] max: Above[12] color: 1\n" +
				"│   ┌── [11, 11] max: Above[12] color: 0\n" +
				"└── [7, 7] max: Above[12] color: 0\n" +
				"    └── [3, 3] max: Above[3] color: 0\n" +
				"",
			rng: r(req(13)),
			exp: "RangeColumnExprTree\n" +
				"│       ┌── [13, 13] max: Above[13] color: 1\n" +
				"│   ┌── [12, 12] max: Above[13] color: 0\n" +
				"│   │   └── [11, 11] max: Above[11] color: 1\n" +
				"└── [7, 7] max: Above[13] color: 0\n" +
				"    └── [3, 3] max: Above[3] color: 0\n" +
				"",
		},
		{
			name:      "insert rebalance root from left",
			setupRngs: []sql.MySQLRange{r(req(7)), r(req(3)), r(req(11)), r(req(0)), r(req(1)), r(req(2)), r(req(4))},
			setupExp: "RangeColumnExprTree\n" +
				"│   ┌── [11, 11] max: Above[11] color: 0\n" +
				"└── [7, 7] max: Above[11] color: 0\n" +
				"    │       ┌── [4, 4] max: Above[4] color: 1\n" +
				"    │   ┌── [3, 3] max: Above[4] color: 0\n" +
				"    │   │   └── [2, 2] max: Above[2] color: 1\n" +
				"    └── [1, 1] max: Above[4] color: 1\n" +
				"        └── [0, 0] max: Above[0] color: 0\n" +
				"",
			rng: r(req(5)),
			exp: "RangeColumnExprTree\n" +
				"│       ┌── [11, 11] max: Above[11] color: 0\n" +
				"│   ┌── [7, 7] max: Above[11] color: 1\n" +
				"│   │   │   ┌── [5, 5] max: Above[5] color: 1\n" +
				"│   │   └── [4, 4] max: Above[5] color: 0\n" +
				"└── [3, 3] max: Above[11] color: 0\n" +
				"    │   ┌── [2, 2] max: Above[2] color: 0\n" +
				"    └── [1, 1] max: Above[2] color: 1\n" +
				"        └── [0, 0] max: Above[0] color: 0\n" +
				"",
		},
		{
			name:      "insert rebalance root from right",
			setupRngs: []sql.MySQLRange{r(req(7)), r(req(3)), r(req(11)), r(req(8)), r(req(9)), r(req(10)), r(req(12))},
			setupExp: "RangeColumnExprTree\n" +
				"│           ┌── [12, 12] max: Above[12] color: 1\n" +
				"│       ┌── [11, 11] max: Above[12] color: 0\n" +
				"│       │   └── [10, 10] max: Above[10] color: 1\n" +
				"│   ┌── [9, 9] max: Above[12] color: 1\n" +
				"│   │   └── [8, 8] max: Above[8] color: 0\n" +
				"└── [7, 7] max: Above[12] color: 0\n" +
				"    └── [3, 3] max: Above[3] color: 0\n" +
				"",
			rng: r(req(13)),
			exp: "RangeColumnExprTree\n" +
				"│           ┌── [13, 13] max: Above[13] color: 1\n" +
				"│       ┌── [12, 12] max: Above[13] color: 0\n" +
				"│   ┌── [11, 11] max: Above[13] color: 1\n" +
				"│   │   └── [10, 10] max: Above[10] color: 0\n" +
				"└── [9, 9] max: Above[13] color: 0\n" +
				"    │   ┌── [8, 8] max: Above[8] color: 0\n" +
				"    └── [7, 7] max: Above[8] color: 1\n" +
				"        └── [3, 3] max: Above[3] color: 0\n" +
				"",
		},
		{
			name:      "insert smallest",
			setupRngs: []sql.MySQLRange{r(rcc(4, 6)), r(req(3)), r(req(11))},
			setupExp: "RangeColumnExprTree\n" +
				"│   ┌── [11, 11] max: Above[11] color: 1\n" +
				"└── [4, 6] max: Above[11] color: 0\n" +
				"    └── [3, 3] max: Above[3] color: 1\n" +
				"",
			rng: r(req(0)),
			exp: "RangeColumnExprTree\n" +
				"│   ┌── [11, 11] max: Above[11] color: 0\n" +
				"└── [4, 6] max: Above[11] color: 0\n" +
				"    └── [3, 3] max: Above[3] color: 0\n" +
				"        └── [0, 0] max: Above[0] color: 1\n" +
				"",
		},
		{
			name:      "insert compare above and below",
			setupRngs: []sql.MySQLRange{r(roo(4, 6)), r(req(4))},
			setupExp: "RangeColumnExprTree\n" +
				"└── (4, 6) max: Below[6] color: 0\n" +
				"    └── [4, 4] max: Above[4] color: 1\n" +
				"",
			rng: r(req(6)),
			exp: "RangeColumnExprTree\n" +
				"│   ┌── [6, 6] max: Above[6] color: 1\n" +
				"└── (4, 6) max: Above[6] color: 0\n" +
				"    └── [4, 4] max: Above[4] color: 1\n" +
				"",
		},
	}

	ctx := sql.NewEmptyContext()
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tree, err := buildTestRangeTree(ctx, test.setupRngs)
			require.NoError(t, err)
			assert.Equal(t, test.setupExp, tree.String())

			err = tree.Insert(ctx, test.rng)
			require.NoError(t, err)
			assert.Equal(t, test.exp, tree.String())
		})
	}
}

func TestRangeTreeRemove(t *testing.T) {
	tests := []struct {
		name      string
		setupRngs []sql.MySQLRange
		setupExp  string
		rng       sql.MySQLRange
		exp       string
	}{
		{
			name:      "remove smallest",
			setupRngs: []sql.MySQLRange{r(req(7)), r(req(3)), r(req(11)), r(req(1)), r(req(5)), r(req(9)), r(req(13))},
			setupExp: "RangeColumnExprTree\n" +
				"│       ┌── [13, 13] max: Above[13] color: 1\n" +
				"│   ┌── [11, 11] max: Above[13] color: 0\n" +
				"│   │   └── [9, 9] max: Above[9] color: 1\n" +
				"└── [7, 7] max: Above[13] color: 0\n" +
				"    │   ┌── [5, 5] max: Above[5] color: 1\n" +
				"    └── [3, 3] max: Above[5] color: 0\n" +
				"        └── [1, 1] max: Above[1] color: 1\n" +
				"",
			rng: r(req(1)),
			exp: "RangeColumnExprTree\n" +
				"│       ┌── [13, 13] max: Above[13] color: 1\n" +
				"│   ┌── [11, 11] max: Above[13] color: 0\n" +
				"│   │   └── [9, 9] max: Above[9] color: 1\n" +
				"└── [7, 7] max: Above[13] color: 0\n" +
				"    │   ┌── [5, 5] max: Above[5] color: 1\n" +
				"    └── [3, 3] max: Above[5] color: 0\n" +
				"",
		},
		{
			name:      "remove largest",
			setupRngs: []sql.MySQLRange{r(req(7)), r(req(3)), r(req(11)), r(req(1)), r(req(5)), r(req(9)), r(req(13))},
			setupExp: "RangeColumnExprTree\n" +
				"│       ┌── [13, 13] max: Above[13] color: 1\n" +
				"│   ┌── [11, 11] max: Above[13] color: 0\n" +
				"│   │   └── [9, 9] max: Above[9] color: 1\n" +
				"└── [7, 7] max: Above[13] color: 0\n" +
				"    │   ┌── [5, 5] max: Above[5] color: 1\n" +
				"    └── [3, 3] max: Above[5] color: 0\n" +
				"        └── [1, 1] max: Above[1] color: 1\n" +
				"",
			rng: r(req(13)),
			exp: "RangeColumnExprTree\n" +
				"│   ┌── [11, 11] max: Above[11] color: 0\n" +
				"│   │   └── [9, 9] max: Above[9] color: 1\n" +
				"└── [7, 7] max: Above[11] color: 0\n" +
				"    │   ┌── [5, 5] max: Above[5] color: 1\n" +
				"    └── [3, 3] max: Above[5] color: 0\n" +
				"        └── [1, 1] max: Above[1] color: 1\n" +
				"",
		},
		{
			name:      "remove largest without left child",
			setupRngs: []sql.MySQLRange{r(req(7)), r(req(3)), r(req(11)), r(req(1)), r(req(5)), r(req(13))},
			setupExp: "RangeColumnExprTree\n" +
				"│       ┌── [13, 13] max: Above[13] color: 1\n" +
				"│   ┌── [11, 11] max: Above[13] color: 0\n" +
				"└── [7, 7] max: Above[13] color: 0\n" +
				"    │   ┌── [5, 5] max: Above[5] color: 1\n" +
				"    └── [3, 3] max: Above[5] color: 0\n" +
				"        └── [1, 1] max: Above[1] color: 1\n" +
				"",
			rng: r(req(13)),
			exp: "RangeColumnExprTree\n" +
				"│   ┌── [11, 11] max: Above[11] color: 0\n" +
				"└── [7, 7] max: Above[11] color: 0\n" +
				"    │   ┌── [5, 5] max: Above[5] color: 1\n" +
				"    └── [3, 3] max: Above[5] color: 0\n" +
				"        └── [1, 1] max: Above[1] color: 1\n" +
				"",
		},
		{
			name:      "remove left parent",
			setupRngs: []sql.MySQLRange{r(req(7)), r(req(3)), r(req(11)), r(req(1)), r(req(5)), r(req(9)), r(req(13))},
			setupExp: "RangeColumnExprTree\n" +
				"│       ┌── [13, 13] max: Above[13] color: 1\n" +
				"│   ┌── [11, 11] max: Above[13] color: 0\n" +
				"│   │   └── [9, 9] max: Above[9] color: 1\n" +
				"└── [7, 7] max: Above[13] color: 0\n" +
				"    │   ┌── [5, 5] max: Above[5] color: 1\n" +
				"    └── [3, 3] max: Above[5] color: 0\n" +
				"        └── [1, 1] max: Above[1] color: 1\n" +
				"",
			rng: r(req(3)),
			exp: "RangeColumnExprTree\n" +
				"│       ┌── [13, 13] max: Above[13] color: 1\n" +
				"│   ┌── [11, 11] max: Above[13] color: 0\n" +
				"│   │   └── [9, 9] max: Above[9] color: 1\n" +
				"└── [7, 7] max: Above[13] color: 0\n" +
				"    │   ┌── [5, 5] max: Above[5] color: 1\n" +
				"    └── [1, 1] max: Above[5] color: 0\n" +
				"",
		},
		{
			name:      "remove right parent",
			setupRngs: []sql.MySQLRange{r(req(7)), r(req(3)), r(req(11)), r(req(1)), r(req(5)), r(req(9)), r(req(13))},
			setupExp: "RangeColumnExprTree\n" +
				"│       ┌── [13, 13] max: Above[13] color: 1\n" +
				"│   ┌── [11, 11] max: Above[13] color: 0\n" +
				"│   │   └── [9, 9] max: Above[9] color: 1\n" +
				"└── [7, 7] max: Above[13] color: 0\n" +
				"    │   ┌── [5, 5] max: Above[5] color: 1\n" +
				"    └── [3, 3] max: Above[5] color: 0\n" +
				"        └── [1, 1] max: Above[1] color: 1\n" +
				"",
			rng: r(req(11)),
			exp: "RangeColumnExprTree\n" +
				"│       ┌── [13, 13] max: Above[13] color: 1\n" +
				"│   ┌── [9, 9] max: Above[13] color: 0\n" +
				"└── [7, 7] max: Above[13] color: 0\n" +
				"    │   ┌── [5, 5] max: Above[5] color: 1\n" +
				"    └── [3, 3] max: Above[5] color: 0\n" +
				"        └── [1, 1] max: Above[1] color: 1\n" +
				"",
		},
		{
			name:      "remove root",
			setupRngs: []sql.MySQLRange{r(req(7)), r(req(3)), r(req(11)), r(req(1)), r(req(5)), r(req(9)), r(req(13))},
			setupExp: "RangeColumnExprTree\n" +
				"│       ┌── [13, 13] max: Above[13] color: 1\n" +
				"│   ┌── [11, 11] max: Above[13] color: 0\n" +
				"│   │   └── [9, 9] max: Above[9] color: 1\n" +
				"└── [7, 7] max: Above[13] color: 0\n" +
				"    │   ┌── [5, 5] max: Above[5] color: 1\n" +
				"    └── [3, 3] max: Above[5] color: 0\n" +
				"        └── [1, 1] max: Above[1] color: 1\n" +
				"",
			rng: r(req(7)),
			exp: "RangeColumnExprTree\n" +
				"│       ┌── [13, 13] max: Above[13] color: 1\n" +
				"│   ┌── [11, 11] max: Above[13] color: 0\n" +
				"│   │   └── [9, 9] max: Above[9] color: 1\n" +
				"└── [5, 5] max: Above[13] color: 0\n" +
				"    └── [3, 3] max: Above[3] color: 0\n" +
				"        └── [1, 1] max: Above[1] color: 1\n" +
				"",
		},
		{
			name:      "remove rotate left",
			setupRngs: []sql.MySQLRange{r(req(7)), r(req(3)), r(req(11)), r(req(1)), r(req(5))},
			setupExp: "RangeColumnExprTree\n" +
				"│   ┌── [11, 11] max: Above[11] color: 0\n" +
				"└── [7, 7] max: Above[11] color: 0\n" +
				"    │   ┌── [5, 5] max: Above[5] color: 1\n" +
				"    └── [3, 3] max: Above[5] color: 0\n" +
				"        └── [1, 1] max: Above[1] color: 1\n" +
				"",
			rng: r(req(11)),
			exp: "RangeColumnExprTree\n" +
				"│   ┌── [7, 7] max: Above[7] color: 0\n" +
				"│   │   └── [5, 5] max: Above[5] color: 1\n" +
				"└── [3, 3] max: Above[7] color: 0\n" +
				"    └── [1, 1] max: Above[1] color: 0\n" +
				"",
		},
		{
			name:      "remove root",
			setupRngs: []sql.MySQLRange{r(req(7)), r(req(3)), r(req(11)), r(req(9)), r(req(13))},
			setupExp: "RangeColumnExprTree\n" +
				"│       ┌── [13, 13] max: Above[13] color: 1\n" +
				"│   ┌── [11, 11] max: Above[13] color: 0\n" +
				"│   │   └── [9, 9] max: Above[9] color: 1\n" +
				"└── [7, 7] max: Above[13] color: 0\n" +
				"    └── [3, 3] max: Above[3] color: 0\n" +
				"",
			rng: r(req(3)),
			exp: "RangeColumnExprTree\n" +
				"│   ┌── [13, 13] max: Above[13] color: 0\n" +
				"└── [11, 11] max: Above[13] color: 0\n" +
				"    │   ┌── [9, 9] max: Above[9] color: 1\n" +
				"    └── [7, 7] max: Above[9] color: 0\n" +
				"",
		},
		{
			name:      "remove ranges",
			setupRngs: []sql.MySQLRange{r(roo(3, 5)), r(roo(1, 3)), r(roo(5, 7))},
			setupExp: "RangeColumnExprTree\n" +
				"│   ┌── (5, 7) max: Below[7] color: 1\n" +
				"└── (3, 5) max: Below[7] color: 0\n" +
				"    └── (1, 3) max: Below[3] color: 1\n" +
				"",
			rng: r(roo(1, 3)),
			exp: "RangeColumnExprTree\n" +
				"│   ┌── (5, 7) max: Below[7] color: 1\n" +
				"└── (3, 5) max: Below[7] color: 0\n" +
				"",
		},
		{
			name:      "remove ranges",
			setupRngs: []sql.MySQLRange{r(roo(3, 5)), r(roo(1, 3)), r(roo(5, 7))},
			setupExp: "RangeColumnExprTree\n" +
				"│   ┌── (5, 7) max: Below[7] color: 1\n" +
				"└── (3, 5) max: Below[7] color: 0\n" +
				"    └── (1, 3) max: Below[3] color: 1\n" +
				"",
			rng: r(roo(3, 5)),
			exp: "RangeColumnExprTree\n" +
				"│   ┌── (5, 7) max: Below[7] color: 1\n" +
				"└── (1, 3) max: Below[7] color: 0\n" +
				"",
		},
	}

	ctx := sql.NewEmptyContext()
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tree, err := buildTestRangeTree(ctx, test.setupRngs)
			require.NoError(t, err)
			assert.Equal(t, test.setupExp, tree.String())

			err = tree.Remove(ctx, test.rng)
			require.NoError(t, err)
			assert.Equal(t, test.exp, tree.String())
		})
	}
}
