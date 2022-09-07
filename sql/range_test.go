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
	x, y, _, values2, _, _ := setup()

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
		{
			or(
				and(cc(x, 2, 6), cc(y, 5, 10)),
				and(cc(x, 3, 7), cc(y, 1, 4)),
				and(oo(x, 4, 8), oo(y, 2, 5)),
				and(oc(x, 5, 10), oc(y, 4, 7)),
			),
			sql.RangeCollection{
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
			sql.RangeCollection{
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
			sql.RangeCollection{
				r(rcc(1, 6), rcc(4, 7)),
				r(rcc(4, 5), rcc(3, 8)),
				r(rcc(3, 8), rcc(1, 6)),
			},
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("Expr:  %s\nRange: %s", test.reference.String(), test.ranges.DebugString()), func(t *testing.T) {
			discreteRanges, err := sql.RemoveOverlappingRanges(test.ranges...)
			require.NoError(t, err)
			verificationRanges, err := removeOverlappingRangesVerification(test.ranges...)
			require.NoError(t, err)
			for _, row := range values2 {
				referenceBool, err := test.reference.Eval(ctx, row)
				require.NoError(t, err)
				rangeBool := evalRanges(t, discreteRanges, row)
				assert.Equal(t, referenceBool, rangeBool, fmt.Sprintf("%v: DiscreteRanges: %s", row, discreteRanges.DebugString()))

			}
			discreteRanges, err = sql.SortRanges(discreteRanges...)
			require.NoError(t, err)
			verificationRanges, err = sql.SortRanges(verificationRanges...)
			require.NoError(t, err)
			ok, err := discreteRanges.Equals(verificationRanges)
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
			verificationRanges, err := removeOverlappingRangesVerification(test.ranges...)
			require.NoError(t, err)
			for _, row := range values3 {
				referenceBool, err := test.reference.Eval(ctx, row)
				require.NoError(t, err)
				rangeBool := evalRanges(t, discreteRanges, row)
				assert.Equal(t, referenceBool, rangeBool, fmt.Sprintf("%v: DiscreteRanges: %s", row, discreteRanges.DebugString()))
			}
			discreteRanges, err = sql.SortRanges(discreteRanges...)
			require.NoError(t, err)
			verificationRanges, err = sql.SortRanges(verificationRanges...)
			require.NoError(t, err)
			ok, err := discreteRanges.Equals(verificationRanges)
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
		ranges    sql.RangeCollection
	}{
		{
			reference: or(
				and(isNull(x), gt(y, 5)),
			),
			ranges: sql.RangeCollection{
				r(null(), rgt(5)),
			},
		},
		{
			reference: or(
				and(isNull(x), isNotNull(y)),
			),
			ranges: sql.RangeCollection{
				r(null(), notNull()),
			},
		},
		{
			reference: or(
				and(isNull(x), lt(y, 5)),
			),
			ranges: sql.RangeCollection{
				r(null(), rlt(5)),
			},
		},
		{
			reference: or(
				and(isNull(x), gte(y, 5)),
			),
			ranges: sql.RangeCollection{
				r(null(), rgte(5)),
			},
		},
		{
			reference: or(
				and(isNull(x), lte(y, 5)),
			),
			ranges: sql.RangeCollection{
				r(null(), rlte(5)),
			},
		},
		{
			reference: or(
				and(isNull(x), lte(y, 5)),
			),
			ranges: sql.RangeCollection{
				r(null(), rlte(5)),
			},
		},
		{
			reference: or(
				and(isNull(x), eq(y, 1)),
			),
			ranges: sql.RangeCollection{
				r(null(), req(1)),
			},
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("Expr:  %s\nRange: %s", test.reference.String(), test.ranges.DebugString()), func(t *testing.T) {
			discreteRanges, err := sql.RemoveOverlappingRanges(test.ranges...)
			require.NoError(t, err)
			verificationRanges, err := removeOverlappingRangesVerification(test.ranges...)
			require.NoError(t, err)
			for _, row := range valuesNull {
				referenceBool, err := test.reference.Eval(ctx, row)
				require.NoError(t, err)
				rangeBool := evalRanges(t, discreteRanges, row)
				assert.Equal(t, referenceBool, rangeBool, fmt.Sprintf("%v: DiscreteRanges: %s", row, discreteRanges.DebugString()))
			}
			discreteRanges, err = sql.SortRanges(discreteRanges...)
			require.NoError(t, err)
			verificationRanges, err = sql.SortRanges(verificationRanges...)
			require.NoError(t, err)
			ok, err := discreteRanges.Equals(verificationRanges)
			require.NoError(t, err)
			assert.True(t, ok)
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
		if val == nil {
			rowRange[i] = sql.NullRangeColumnExpr(rangeType)
		} else {
			rowRange[i] = sql.ClosedRangeColumnExpr(val, val, rangeType)
		}
	}
	ok, err := rang.IsSupersetOf(rowRange)
	require.NoError(t, err)
	return ok
}

func removeOverlappingRangesVerification(ranges ...sql.Range) (sql.RangeCollection, error) {
	if len(ranges) == 0 {
		return nil, nil
	}

	var newRanges sql.RangeCollection
	for i := 0; i < len(ranges); i++ {
		hadOverlap := false
		for nri := 0; nri < len(newRanges); nri++ {
			if resultingRanges, ok, err := ranges[i].RemoveOverlap(newRanges[nri]); err != nil {
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

func null() sql.RangeColumnExpr {
	return sql.NullRangeColumnExpr(rangeType)
}

func notNull() sql.RangeColumnExpr {
	return sql.NotNullRangeColumnExpr(rangeType)
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
