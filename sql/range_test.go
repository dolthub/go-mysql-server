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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

var rangeType = sql.Uint8

func TestRangeOverlapTwoColumns(t *testing.T) {
	ctx := sql.NewEmptyContext()
	x, y, _, values2, _ := setup()

	tests := []struct {
		reference sql.Expression
		ranges    sql.RangeCollection
	}{
		{
			or(
				and(lt(x, 2), gt(y, 5)),
				and(gt(x, 8), gt(y, 5)),
				and(gt(x, 5), gt(y, 8)),
			),
			sql.RangeCollection{
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
			sql.RangeCollection{
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
			sql.RangeCollection{
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
			sql.RangeCollection{
				r(rgt(2), rgt(2)),
				r(req(4), req(4)),
				r(rlt(9), rlt(9)),
				r(req(6), req(6)),
				r(req(8), req(8)),
			},
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("Expr:  %s\nRange: %s", test.reference.String(), test.ranges.DebugString()), func(t *testing.T) {
			discreteRanges, err := sql.RemoveOverlappingRanges(test.ranges...)
			require.NoError(t, err)
			for _, row := range values2 {
				referenceBool, err := test.reference.Eval(ctx, row)
				require.NoError(t, err)
				rangeBool := evalRanges(t, discreteRanges, row)
				assert.Equal(t, referenceBool, rangeBool, fmt.Sprintf("%v: DiscreteRanges: %s", row, discreteRanges.DebugString()))
			}
		})
	}
}

func TestRangeOverlapThreeColumns(t *testing.T) {
	ctx := sql.NewEmptyContext()
	x, y, z, _, values3 := setup()

	tests := []struct {
		reference sql.Expression
		ranges    sql.RangeCollection
	}{
		{
			or(
				and(gt(x, 2), gt(y, 2), gt(z, 2)),
				and(lt(x, 8), lt(y, 8), lt(z, 8)),
			),
			sql.RangeCollection{
				r(rgt(2), rgt(2), rgt(2)),
				r(rlt(8), rlt(8), rlt(8)),
			},
		},
		{
			or(
				and(gte(x, 3), gte(y, 3), gte(z, 3)),
				and(lte(x, 5), lte(y, 5), lte(z, 5)),
			),
			sql.RangeCollection{
				r(rgte(3), rgte(3), rgte(3)),
				r(rlte(5), rlte(5), rlte(5)),
			},
		},
		{
			or(
				and(gte(x, 3), gte(y, 4), gt(z, 5)),
				and(lte(x, 6), lt(y, 7), lte(z, 8)),
			),
			sql.RangeCollection{
				r(rgte(3), rgte(4), rgt(5)),
				r(rlte(6), rlt(7), rlte(8)),
			},
		},
		{
			or(
				and(gte(x, 4), gte(y, 4), gte(z, 3)),
				and(lte(x, 4), lte(y, 4), lte(z, 5)),
			),
			sql.RangeCollection{
				r(rgte(4), rgte(4), rgte(3)),
				r(rlte(4), rlte(4), rlte(5)),
			},
		},
		{
			or(
				and(gte(x, 4), gte(y, 3), gte(z, 4)),
				and(lte(x, 4), lte(y, 5), lte(z, 4)),
			),
			sql.RangeCollection{
				r(rgte(4), rgte(3), rgte(4)),
				r(rlte(4), rlte(5), rlte(4)),
			},
		},
		{
			or(
				and(gte(x, 3), gte(y, 4), gte(z, 4)),
				and(lte(x, 5), lte(y, 4), lte(z, 4)),
			),
			sql.RangeCollection{
				r(rgte(3), rgte(4), rgte(4)),
				r(rlte(5), rlte(4), rlte(4)),
			},
		},
		{
			or(
				and(lt(x, 4), gte(y, 3), lt(z, 4)),
				and(gt(x, 4), lte(y, 5), gt(z, 4)),
			),
			sql.RangeCollection{
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
			sql.RangeCollection{
				r(rgt(3), rgt(2), req(2)),
				r(rlt(4), rgte(7), rlte(2)),
				r(rlte(9), rgt(5), rgt(1)),
			},
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("Expr:  %s\nRange: %s", test.reference.String(), test.ranges.DebugString()), func(t *testing.T) {
			discreteRanges, err := sql.RemoveOverlappingRanges(test.ranges...)
			require.NoError(t, err)
			for _, row := range values3 {
				referenceBool, err := test.reference.Eval(ctx, row)
				require.NoError(t, err)
				rangeBool := evalRanges(t, discreteRanges, row)
				assert.Equal(t, referenceBool, rangeBool, fmt.Sprintf("%v: DiscreteRanges: %s", row, discreteRanges.DebugString()))
			}
		})
	}
}

func setup() (x, y, z sql.Expression, values2, values3 [][]interface{}) {
	values2 = make([][]interface{}, 0, 100)
	values3 = make([][]interface{}, 0, 1000)
	for i := byte(1); i <= 10; i++ {
		for j := byte(1); j <= 10; j++ {
			for k := byte(1); k <= 10; k++ {
				values3 = append(values3, []interface{}{i, j, k})
			}
			values2 = append(values2, []interface{}{i, j})
		}
	}
	x = expression.NewGetField(0, rangeType, "x", true)
	y = expression.NewGetField(1, rangeType, "y", true)
	z = expression.NewGetField(2, rangeType, "z", true)
	return
}

func evalRanges(t *testing.T, ranges []sql.Range, row []interface{}) bool {
	found := false
	for _, rang := range ranges {
		if evalRange(t, rang, row) {
			if !found {
				found = true
			} else {
				assert.FailNow(t, "overlap in ranges")
			}
		}
	}
	return found
}

func evalRange(t *testing.T, rang sql.Range, row []interface{}) bool {
	rowRange := make(sql.Range, len(rang))
	for i, val := range row {
		rowRange[i] = sql.ClosedRangeColumnExpr(val, val, rangeType)
	}
	ok, err := rang.IsSupersetOf(rowRange)
	require.NoError(t, err)
	return ok
}

func eq(field sql.Expression, val uint8) sql.Expression {
	return expression.NewNullSafeEquals(field, expression.NewLiteral(val, rangeType))
}

func lt(field sql.Expression, val uint8) sql.Expression {
	return expression.NewNullSafeLessThan(field, expression.NewLiteral(val, rangeType))
}

func lte(field sql.Expression, val uint8) sql.Expression {
	return expression.NewNullSafeLessThanOrEqual(field, expression.NewLiteral(val, rangeType))
}

func gt(field sql.Expression, val uint8) sql.Expression {
	return expression.NewNullSafeGreaterThan(field, expression.NewLiteral(val, rangeType))
}

func gte(field sql.Expression, val uint8) sql.Expression {
	return expression.NewNullSafeGreaterThanOrEqual(field, expression.NewLiteral(val, rangeType))
}

func not(field sql.Expression, val uint8) sql.Expression {
	return expression.NewNot(eq(field, val))
}

func r(colExprs ...sql.RangeColumnExpr) sql.Range {
	return colExprs
}

func req(val byte) sql.RangeColumnExpr {
	return sql.ClosedRangeColumnExpr(val, val, rangeType)
}

func rlt(val byte) sql.RangeColumnExpr {
	return sql.LessThanRangeColumnExpr(val, rangeType)
}

func rlte(val byte) sql.RangeColumnExpr {
	return sql.LessOrEqualRangeColumnExpr(val, rangeType)
}

func rgt(val byte) sql.RangeColumnExpr {
	return sql.GreaterThanRangeColumnExpr(val, rangeType)
}

func rgte(val byte) sql.RangeColumnExpr {
	return sql.GreaterOrEqualRangeColumnExpr(val, rangeType)
}

func rcc(lowerbound, upperbound byte) sql.RangeColumnExpr {
	return sql.CustomRangeColumnExpr(lowerbound, upperbound, sql.Closed, sql.Closed, rangeType)
}

func rco(lowerbound, upperbound byte) sql.RangeColumnExpr {
	return sql.CustomRangeColumnExpr(lowerbound, upperbound, sql.Closed, sql.Open, rangeType)
}

func roc(lowerbound, upperbound byte) sql.RangeColumnExpr {
	return sql.CustomRangeColumnExpr(lowerbound, upperbound, sql.Open, sql.Closed, rangeType)
}

func roo(lowerbound, upperbound byte) sql.RangeColumnExpr {
	return sql.CustomRangeColumnExpr(lowerbound, upperbound, sql.Open, sql.Open, rangeType)
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
