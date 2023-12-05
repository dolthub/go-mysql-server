// Copyright 2023 Dolthub, Inc.
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

package analyzer

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestExprFlatten(t *testing.T) {
	tests := []struct {
		name     string
		in       sql.Expression
		exp      string
		leftover string
	}{
		{
			name: "ands",
			in: and(
				and(
					eq(gf(0, "xy", "x"), lit(1)),
					eq(gf(1, "xy", "y"), lit(2)),
				),
				gt(gf(1, "xy", "y"), lit(0)),
			),
			exp: `
(1: and
  (3: xy.x = 1)
  (4: xy.y = 2)
  (5: xy.y > 0))`,
		},
		{
			name: "ands with or",
			in: and(
				and(
					eq(gf(0, "xy", "x"), lit(1)),
					eq(gf(1, "xy", "y"), lit(2)),
				),
				or(
					gt(gf(1, "xy", "y"), lit(0)),
					lt(gf(1, "xy", "y"), lit(7)),
				),
			),
			exp: `
(1: and
  (3: xy.x = 1)
  (4: xy.y = 2)
  (5: or
    (6: xy.y > 0)
    (7: xy.y < 7)))`,
		},
		{
			name: "ors with and",
			in: or(
				or(
					eq(gf(0, "xy", "x"), lit(1)),
					eq(gf(1, "xy", "y"), lit(2)),
				),
				and(
					gt(gf(0, "xy", "y"), lit(0)),
					lt(gf(0, "xy", "y"), lit(7)),
				),
			),
			exp: `
(1: or
  (3: xy.x = 1)
  (4: xy.y = 2)
  (5: and
    (6: xy.y > 0)
    (7: xy.y < 7)))`,
		},
		{
			name: "include top-level leaf",
			in:   gt(gf(0, "xy", "y"), lit(0)),
			exp:  "(1: xy.y > 0)",
		},
		{
			name:     "exclude top-level leaf",
			in:       expression.NewLike(gf(0, "xy", "x"), lit(1), nil),
			exp:      ``,
			leftover: "xy.x LIKE 1",
		},
		{
			name: "exclude leaves",
			in: and(
				and(
					eq(gf(0, "xy", "x"), lit(1)),
					expression.NewLike(gf(0, "xy", "x"), lit(1), nil),
				),
				and(
					and(
						gt(gf(0, "xy", "y"), lit(0)),
						lt(gf(0, "xy", "y"), lit(7)),
					),
					expression.NewLike(gf(1, "xy", "y"), lit(1), nil),
				),
			),
			exp: `
(1: and
  (3: xy.x = 1)
  (7: xy.y > 0)
  (8: xy.y < 7))
`,
			leftover: `
AND
 ├─ xy.x LIKE 1
 └─ xy.y LIKE 1`,
		},
		{
			name: "exclude simple OR",
			in: and(
				or(
					eq(gf(0, "xy", "x"), lit(1)),
					expression.NewLike(gf(0, "xy", "x"), lit(1), nil),
				),
				and(
					gt(gf(0, "xy", "y"), lit(0)),
					lt(gf(0, "xy", "y"), lit(7)),
				),
			),
			exp: `
(1: and
  (5: xy.y > 0)
  (6: xy.y < 7))
`,
			leftover: `
Or
 ├─ Eq
 │   ├─ xy.x:0!null
 │   └─ 1 (bigint)
 └─ xy.x LIKE 1`,
		},
		{
			name: "exclude nested OR",
			in: and(
				or(
					and(
						eq(gf(0, "xy", "x"), lit(1)),
						expression.NewLike(gf(0, "xy", "x"), lit(1), nil),
					),
					eq(gf(0, "xy", "x"), lit(2)),
				),
				and(
					gt(gf(0, "xy", "y"), lit(0)),
					lt(gf(0, "xy", "y"), lit(7)),
				),
			),
			exp: `
(1: and
  (7: xy.y > 0)
  (8: xy.y < 7))
`,
			leftover: `
Or
 ├─ AND
 │   ├─ Eq
 │   │   ├─ xy.x:0!null
 │   │   └─ 1 (bigint)
 │   └─ xy.x LIKE 1
 └─ Eq
     ├─ xy.x:0!null
     └─ 2 (bigint)`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newIndexCoster("xyz")
			root, leftover, _ := c.flatten(tt.in)
			costTree := formatIndexFilter(root)
			require.Equal(t, strings.TrimSpace(tt.exp), strings.TrimSpace(costTree), costTree)
			if leftover != nil {
				leftoverCmp := sql.DebugString(leftover)
				require.Equal(t, strings.TrimSpace(tt.leftover), strings.TrimSpace(leftoverCmp), leftoverCmp)
			} else {
				require.Equal(t, "", tt.leftover)
			}
		})
	}
}

var rangeType = types.Uint8

func TestRangeBuilder(t *testing.T) {
	ctx := sql.NewEmptyContext()
	x := expression.NewGetFieldWithTable(0, 0, rangeType, "mydb", "xyz", "x", true)
	y := expression.NewGetFieldWithTable(0, 1, rangeType, "mydb", "xyz", "y", true)
	z := expression.NewGetFieldWithTable(0, 2, rangeType, "mydb", "xyz", "z", true)

	tests := []struct {
		filter sql.Expression
		exp    sql.RangeCollection
		cnt    int
	}{
		{
			and2(
				gt2(x, 7),
				or2(
					and2(
						gte2(x, 9),
						lte2(x, 8),
					),
					isNull(x),
				),
				or2(
					gte2(y, 1),
					gte2(x, 10),
				),
			),
			sql.RangeCollection{
				r(sql.EmptyRangeColumnExpr(rangeType)),
			},
			2,
		},
		// two column
		{
			or2(
				and(lt2(x, 2), gt2(y, 5)),
				and(gt2(x, 8), gt2(y, 5)),
				and(gt2(x, 5), gt2(y, 8)),
			),
			sql.RangeCollection{
				r(rlt(2), rgt(5)),
				r(rgt(8), rgt(5)),
				r(rgt(5), rgt(8)),
			},
			1,
		},
		{
			or2(
				and(lt2(x, 2), gt2(y, 5)),
				and(gt2(x, 8), gt2(y, 5)),
				and(gt2(x, 5), lt2(y, 8)),
			),
			sql.RangeCollection{
				r(rlt(2), rgt(5)),
				r(rgt(8), rgt(5)),
				r(rgt(5), rlt(8)),
			},
			1,
		},
		{
			or2(
				and(gt2(x, 1), gt2(y, 1)),
				and(gt2(x, 2), gt2(y, 2)),
				and(gt2(x, 3), gt2(y, 3)),
				and(gt2(x, 4), gt2(y, 4)),
				and(gt2(x, 5), gt2(y, 5)),
				and(gt2(x, 6), gt2(y, 6)),
				and(gt2(x, 7), gt2(y, 7)),
				and(gt2(x, 8), gt2(y, 8)),
				and(gt2(x, 9), gt2(y, 9)),
				and(lt2(x, 1), lt2(y, 1)),
				and(lt2(x, 2), lt2(y, 2)),
				and(lt2(x, 3), lt2(y, 3)),
				and(lt2(x, 4), lt2(y, 4)),
				and(lt2(x, 5), lt2(y, 5)),
				and(lt2(x, 6), lt2(y, 6)),
				and(lt2(x, 7), lt2(y, 7)),
				and(lt2(x, 8), lt2(y, 8)),
				and(lt2(x, 9), lt2(y, 9)),
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
			1,
		},
		{
			or2(
				and(gt2(x, 2), gt2(y, 2)),
				and(eq2(x, 4), eq2(y, 4)),
				and(lt2(x, 9), lt2(y, 9)),
				and(eq2(x, 6), eq2(y, 6)),
				and(eq2(x, 8), eq2(y, 8)),
			),
			sql.RangeCollection{
				r(rgt(2), rgt(2)),
				r(req(4), req(4)),
				r(rlt(9), rlt(9)),
				r(req(6), req(6)),
				r(req(8), req(8)),
			},
			1,
		},
		{
			or2(
				and(gt2(x, 2), gt2(y, 2)),
				and(eq2(x, 4), eq2(y, 4)),
				and(lt2(x, 9), lt2(y, 9)),
				and(eq2(x, 6), eq2(y, 6)),
				and(eq2(x, 8), eq2(y, 8)),
			),
			sql.RangeCollection{
				r(rgt(2), rgt(2)),
				r(req(4), req(4)),
				r(rlt(9), rlt(9)),
				r(req(6), req(6)),
				r(req(8), req(8)),
			},
			1,
		},
		{
			or2(
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
			1,
		},
		{
			or2(
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
			1,
		},
		{
			or2(
				and(cc(x, 1, 6), cc(y, 4, 7)),
				and(cc(x, 4, 5), cc(y, 3, 8)),
				and(cc(x, 3, 8), cc(y, 1, 6)),
			),
			sql.RangeCollection{
				r(rcc(1, 6), rcc(4, 7)),
				r(rcc(4, 5), rcc(3, 8)),
				r(rcc(3, 8), rcc(1, 6)),
			},
			1,
		},
		// three columns
		{
			or2(
				and2(gt2(x, 2), gt2(y, 2), gt2(z, 2)),
				and2(lt2(x, 8), lt2(y, 8), lt2(z, 8)),
			),
			sql.RangeCollection{
				r(rgt(2), rgt(2), rgt(2)),
				r(rlt(8), rlt(8), rlt(8)),
			},
			1,
		},
		{
			or2(
				and2(gte2(x, 3), gte2(y, 3), gte2(z, 3)),
				and2(lte2(x, 5), lte2(y, 5), lte2(z, 5)),
			),
			sql.RangeCollection{
				r(rgte(3), rgte(3), rgte(3)),
				r(rlte(5), rlte(5), rlte(5)),
			},
			1,
		},
		{
			or2(
				and2(gte2(x, 3), gte2(y, 4), gt2(z, 5)),
				and2(lte2(x, 6), lt2(y, 7), lte2(z, 8)),
			),
			sql.RangeCollection{
				r(rgte(3), rgte(4), rgt(5)),
				r(rlte(6), rlt(7), rlte(8)),
			},
			1,
		},
		{
			or2(
				and2(gte2(x, 4), gte2(y, 4), gte2(z, 3)),
				and2(lte2(x, 4), lte2(y, 4), lte2(z, 5)),
			),
			sql.RangeCollection{
				r(rgte(4), rgte(4), rgte(3)),
				r(rlte(4), rlte(4), rlte(5)),
			},
			1,
		},
		{
			or2(
				and2(gte2(x, 4), gte2(y, 3), gte2(z, 4)),
				and2(lte2(x, 4), lte2(y, 5), lte2(z, 4)),
			),
			sql.RangeCollection{
				r(rgte(4), rgte(3), rgte(4)),
				r(rlte(4), rlte(5), rlte(4)),
			},
			1,
		},
		{
			or2(
				and2(gte2(x, 3), gte2(y, 4), gte2(z, 4)),
				and2(lte2(x, 5), lte2(y, 4), lte2(z, 4)),
			),
			sql.RangeCollection{
				r(rgte(3), rgte(4), rgte(4)),
				r(rlte(5), rlte(4), rlte(4)),
			},
			1,
		},
		{
			or2(
				and2(lt2(x, 4), gte2(y, 3), lt2(z, 4)),
				and2(gt2(x, 4), lte2(y, 5), gt2(z, 4)),
			),
			sql.RangeCollection{
				r(rlt(4), rgte(3), rlt(4)),
				r(rgt(4), rlte(5), rgt(4)),
			},
			1,
		},
		{
			or2(
				and2(gt2(x, 3), gt2(y, 2), eq2(z, 2)),
				and2(lt2(x, 4), gte2(y, 7), lte2(z, 2)),
				and2(lte2(x, 9), gt2(y, 5), gt2(z, 1)),
			),
			sql.RangeCollection{
				r(rgt(3), rgt(2), req(2)),
				r(rlt(4), rgte(7), rlte(2)),
				r(rlte(9), rgt(5), rgt(1)),
			},
			1,
		},
		// nulls
		{
			or2(
				and2(isNull(x), gt2(y, 5)),
			),
			sql.RangeCollection{
				r(null2(), rgt(5)),
			},
			1,
		},
		{
			or2(
				and2(isNull(x), isNotNull(y)),
			),
			sql.RangeCollection{
				r(null2(), notNull()),
			},
			1,
		},
		{
			or2(
				and2(isNull(x), lt2(y, 5)),
			),
			sql.RangeCollection{
				r(null2(), rlt(5)),
			},
			1,
		},
		{
			or2(
				and(isNull(x), gte2(y, 5)),
			),
			sql.RangeCollection{
				r(null2(), rgte(5)),
			},
			1,
		},
		{
			or2(
				and(isNull(x), lte2(y, 5)),
			),
			sql.RangeCollection{
				r(null2(), rlte(5)),
			},
			1,
		},
		{
			or2(
				and(isNull(x), lte2(y, 5)),
			),
			sql.RangeCollection{
				r(null2(), rlte(5)),
			},
			1,
		},
		{
			or2(
				and2(isNull(x), eq2(y, 1)),
			),
			sql.RangeCollection{
				r(null2(), req(1)),
			},
			1,
		},
	}

	const testDb = "mydb"
	const testTable = "xyz"
	idx_1 := &memory.Index{
		Name:      "x",
		Exprs:     []sql.Expression{x},
		DB:        testDb,
		TableName: testTable,
	}
	idx_2 := &memory.Index{
		Name:      "x_y",
		Exprs:     []sql.Expression{x, y},
		DB:        testDb,
		TableName: testTable,
	}
	idx_3 := &memory.Index{
		Name:      "x_y_z",
		Exprs:     []sql.Expression{x, y, z},
		DB:        testDb,
		TableName: testTable,
	}

	var sch = make(sql.Schema, 3)
	for i, e := range []*expression.GetField{x, y, z} {
		sch[i] = transform.ExpressionToColumn(e, testTable)
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("Expr:  %s\nRange: %s", tt.filter.String(), tt.exp.DebugString()), func(t *testing.T) {

			c := newIndexCoster(testTable)
			root, _, _ := c.flatten(tt.filter)

			var idx sql.Index
			switch len(tt.exp[0]) {
			case 2:
				idx = idx_2
			case 3:
				idx = idx_3
			default:
				idx = idx_1
			}

			stat, err := newUniformDistStatistic("mydb", testTable, sch, idx, 10, 10)
			require.NoError(t, err)

			err = c.cost(root, stat, idx)
			require.NoError(t, err)

			include := c.bestFilters
			// most tests are designed so that all filters are supported
			// |included| = |root.id|
			require.Equal(t, tt.cnt, include.Len())
			if tt.cnt == 1 {
				require.True(t, include.Contains(1))
			}

			b := newIndexScanRangeBuilder(ctx, idx, include, sql.FastIntSet{}, c.idToExpr)
			cmpRanges, err := b.buildRangeCollection(root)
			require.NoError(t, err)
			if tt.cnt == 1 {
				require.Equal(t, 0, len(b.leftover))
			}
			cmpRanges, err = sql.SortRanges(cmpRanges...)
			require.NoError(t, err)

			expRanges, err := sql.RemoveOverlappingRanges(tt.exp...)
			require.NoError(t, err)
			expRanges, err = sql.SortRanges(expRanges...)
			require.NoError(t, err)

			ok, err := expRanges.Equals(cmpRanges)
			require.NoError(t, err)
			assert.True(t, ok)
			if !ok {
				log.Printf("expected: %s\nfound: %s\n", expRanges.DebugString(), cmpRanges.DebugString())
			}
		})
	}
}

func TestRangeBuilderInclude(t *testing.T) {
	x := expression.NewGetFieldWithTable(0, 0, rangeType, "mydb", "xyz", "x", true)
	y := expression.NewGetFieldWithTable(0, 1, rangeType, "mydb", "xyz", "y", true)

	tests := []struct {
		name    string
		in      sql.Expression
		include sql.FastIntSet
		exp     sql.RangeCollection
	}{
		{
			name: "fraction of ands",
			in: and2(
				and2(
					eq2(x, 1),
					eq2(y, 2),
				),
				gt2(y, 0),
			),
			include: sql.NewFastIntSet(3, 5),
			exp: sql.RangeCollection{
				r(req(1), rgt(0)),
			},
		},
		{
			name: "select or",
			in: and2(
				or2(
					and(lt2(x, 1),
						lt2(y, 1),
					),
					and(gt2(x, 5),
						gt2(y, 5),
					),
				),
				gt2(y, 0),
			),
			include: sql.NewFastIntSet(2),
			exp: sql.RangeCollection{
				r(rlt(1), rlt(1)),
				r(rgt(5), rgt(5)),
			},
		},
	}

	const testDb = "mydb"
	const testTable = "xyz"
	dummy1 := &memory.Index{
		Name:      "dummy1",
		Exprs:     []sql.Expression{x, y},
		DB:        testDb,
		TableName: testTable,
	}

	ctx := sql.NewEmptyContext()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			//t.Skip("todo add tests and implement")

			// TODO make index
			c := newIndexCoster("xyz")
			root, _, _ := c.flatten(tt.in)
			b := newIndexScanRangeBuilder(ctx, dummy1, tt.include, sql.FastIntSet{}, c.idToExpr)
			cmpRanges, err := b.buildRangeCollection(root)
			require.NoError(t, err)
			cmpRanges, err = sql.SortRanges(cmpRanges...)
			require.NoError(t, err)

			expRanges, err := sql.RemoveOverlappingRanges(tt.exp...)
			require.NoError(t, err)
			expRanges, err = sql.SortRanges(expRanges...)
			require.NoError(t, err)

			// TODO how to compare ranges, strings?
			ok, err := expRanges.Equals(cmpRanges)
			require.NoError(t, err)
			assert.True(t, ok)
			if !ok {
				log.Printf("expected: %s\nfound: %s\n", expRanges.DebugString(), cmpRanges.DebugString())
			}
		})
	}

}

func eq2(field sql.Expression, val uint8) sql.Expression {
	return expression.NewNullSafeEquals(field, expression.NewLiteral(val, rangeType))
}

func lt2(field sql.Expression, val uint8) sql.Expression {
	return expression.NewLessThan(field, expression.NewLiteral(val, rangeType))
}

func lte2(field sql.Expression, val uint8) sql.Expression {
	return expression.NewLessThanOrEqual(field, expression.NewLiteral(val, rangeType))
}

func gt2(field sql.Expression, val uint8) sql.Expression {
	return expression.NewGreaterThan(field, expression.NewLiteral(val, rangeType))
}

func gte2(field sql.Expression, val uint8) sql.Expression {
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
	return expression.NewNot(eq2(field, val))
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

func null2() sql.RangeColumnExpr {
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

func or2(expressions ...sql.Expression) sql.Expression {
	if len(expressions) == 1 {
		return expressions[0]
	}
	if expressions[0] == nil {
		return or2(expressions[1:]...)
	}
	return expression.NewOr(expressions[0], or2(expressions[1:]...))
}

func and2(expressions ...sql.Expression) sql.Expression {
	if len(expressions) == 1 {
		return expressions[0]
	}
	if expressions[0] == nil {
		return and2(expressions[1:]...)
	}
	return expression.NewAnd(expressions[0], and2(expressions[1:]...))
}

func TestIndexSearchable(t *testing.T) {
	// we want to run costed index scan rule with the indexSearchableTable as input
	input := plan.NewFilter(
		expression.NewEquals(
			expression.NewGetFieldWithTable(0, 0, types.Int64, "mydb", "xy", "x", false),
			expression.NewLiteral(1, types.Int64),
		),
		plan.NewResolvedTable(&indexSearchableTable{underlying: plan.NewDualSqlTable()}, nil, nil),
	)

	exp := `
Filter
 ├─ (xy.x = 1)
 └─ IndexedTableAccess()
     ├─ index: [xy.x]
     └─ filters: [{[1, 1]}]
`
	res, same, err := costedIndexScans(nil, nil, input, nil, nil)
	require.NoError(t, err)
	require.False(t, bool(same))
	require.Equal(t, strings.TrimSpace(exp), strings.TrimSpace(res.String()), "expected:\n%s,\nfound:\n%s\n", exp, res.String())
}

var xIdx = &dummyIdx{id: "primary", database: "mydb", table: "xy", expr: []sql.Expression{expression.NewGetFieldWithTable(0, 0, types.Int64, "mydb", "xy", "x", false)}}

type indexSearchableTable struct {
	underlying sql.Table
}

var _ sql.IndexSearchableTable = (*indexSearchableTable)(nil)
var _ sql.IndexAddressable = (*indexSearchableTable)(nil)
var _ sql.IndexedTable = (*indexSearchableTable)(nil)

func (i *indexSearchableTable) Name() string {
	return i.underlying.Name()
}

func (i *indexSearchableTable) String() string {
	return i.underlying.String()
}

func (i *indexSearchableTable) Schema() sql.Schema {
	return i.underlying.Schema()
}

func (i *indexSearchableTable) Collation() sql.CollationID {
	return i.underlying.Collation()
}

func (i *indexSearchableTable) Partitions(context *sql.Context) (sql.PartitionIter, error) {
	//TODO implement me
	panic("implement me")
}

func (i *indexSearchableTable) PartitionRows(context *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	//TODO implement me
	panic("implement me")
}

func (i *indexSearchableTable) SkipIndexCosting() bool {
	return true
}

func (i *indexSearchableTable) IndexWithPrefix(ctx *sql.Context, expressions []string) (sql.Index, error) {
	//TODO implement me
	panic("implement me")
}

func (i *indexSearchableTable) LookupForExpressions(ctx *sql.Context, exprs []sql.Expression) (sql.IndexLookup, error) {
	if eq, ok := exprs[0].(*expression.Equals); ok {
		if gf, ok := eq.Left().(*expression.GetField); ok && strings.EqualFold(gf.Name(), "x") {
			if lit, ok := eq.Right().(*expression.Literal); ok {
				ranges := sql.RangeCollection{{sql.ClosedRangeColumnExpr(lit.Value(), lit.Value(), lit.Type())}}
				return sql.IndexLookup{Index: xIdx, Ranges: ranges}, nil
			}
		}
	}
	return sql.IndexLookup{}, nil
}

func (i *indexSearchableTable) IndexedAccess(lookup sql.IndexLookup) sql.IndexedTable {
	return i
}

func (i *indexSearchableTable) GetIndexes(ctx *sql.Context) ([]sql.Index, error) {
	return nil, nil
}

func (i *indexSearchableTable) PreciseMatch() bool {
	return false
}
func (i *indexSearchableTable) LookupPartitions(context *sql.Context, lookup sql.IndexLookup) (sql.PartitionIter, error) {
	//TODO implement me
	panic("implement me")
}
