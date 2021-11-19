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

package analyzer

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/expression/function/aggregation"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

func TestEraseProjection(t *testing.T) {
	require := require.New(t)
	f := getRule("erase_projection")

	table := memory.NewTable("mytable", sql.Schema{{
		Name: "i", Source: "mytable", Type: sql.Int64,
	}})

	expected := plan.NewSort(
		[]sql.SortField{{Column: expression.NewGetField(2, sql.Int64, "foo", false)}},
		plan.NewProject(
			[]sql.Expression{
				expression.NewGetFieldWithTable(0, sql.Int64, "mytable", "i", false),
				expression.NewGetField(1, sql.Int64, "bar", false),
				expression.NewAlias("foo", expression.NewLiteral(1, sql.Int64)),
			},
			plan.NewFilter(
				expression.NewEquals(
					expression.NewLiteral(1, sql.Int64),
					expression.NewGetField(1, sql.Int64, "bar", false),
				),
				plan.NewProject(
					[]sql.Expression{
						expression.NewGetFieldWithTable(0, sql.Int64, "mytable", "i", false),
						expression.NewAlias("bar", expression.NewLiteral(2, sql.Int64)),
					},
					plan.NewResolvedTable(table, nil, nil),
				),
			),
		),
	)

	node := plan.NewProject(
		[]sql.Expression{
			expression.NewGetFieldWithTable(0, sql.Int64, "mytable", "i", false),
			expression.NewGetField(1, sql.Int64, "bar", false),
			expression.NewGetField(2, sql.Int64, "foo", false),
		},
		expected,
	)

	result, err := f.Apply(sql.NewEmptyContext(), NewDefault(nil), node, nil)
	require.NoError(err)

	require.Equal(expected, result)

	result, err = f.Apply(sql.NewEmptyContext(), NewDefault(nil), expected, nil)
	require.NoError(err)

	require.Equal(expected, result)
}

func TestOptimizeDistinct(t *testing.T) {
	t1 := memory.NewTable("foo", sql.Schema{
		{Name: "a", Source: "foo"},
		{Name: "b", Source: "foo"},
	})

	testCases := []struct {
		name      string
		child     sql.Node
		optimized bool
	}{
		{
			"without sort",
			plan.NewResolvedTable(t1, nil, nil),
			false,
		},
		{
			"sort but column not projected",
			plan.NewSort(
				[]sql.SortField{
					{Column: gf(0, "foo", "c")},
				},
				plan.NewResolvedTable(t1, nil, nil),
			),
			false,
		},
		{
			"sort and column projected",
			plan.NewSort(
				[]sql.SortField{
					{Column: gf(0, "foo", "a")},
				},
				plan.NewResolvedTable(t1, nil, nil),
			),
			true,
		},
	}

	rule := getRule("optimize_distinct")

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			node, err := rule.Apply(sql.NewEmptyContext(), nil, plan.NewDistinct(tt.child), nil)
			require.NoError(t, err)

			_, ok := node.(*plan.OrderedDistinct)
			require.Equal(t, tt.optimized, ok)
		})
	}
}

func TestMoveJoinConditionsToFilter(t *testing.T) {
	t1 := memory.NewTable("t1", sql.Schema{
		{Name: "a", Source: "t1", Type: sql.Int64},
		{Name: "b", Source: "t1", Type: sql.Int64},
	})

	t2 := memory.NewTable("t2", sql.Schema{
		{Name: "c", Source: "t2", Type: sql.Int64},
		{Name: "d", Source: "t2", Type: sql.Int64},
	})

	t3 := memory.NewTable("t3", sql.Schema{
		{Name: "e", Source: "t3", Type: sql.Int64},
		{Name: "f", Source: "t3", Type: sql.Int64},
	})

	rule := getRule("move_join_conds_to_filter")
	require := require.New(t)

	node := plan.NewInnerJoin(
		plan.NewResolvedTable(t1, nil, nil),
		plan.NewCrossJoin(
			plan.NewResolvedTable(t2, nil, nil),
			plan.NewResolvedTable(t3, nil, nil),
		),
		expression.JoinAnd(
			eq(col(0, "t1", "a"), col(2, "t2", "c")),
			eq(col(0, "t1", "a"), col(4, "t3", "e")),
			eq(col(2, "t2", "c"), col(4, "t3", "e")),
			eq(col(0, "t1", "a"), lit(5)),
		),
	)

	result, err := rule.Apply(sql.NewEmptyContext(), NewDefault(nil), node, nil)
	require.NoError(err)

	var expected sql.Node = plan.NewFilter(
		expression.JoinAnd(
			eq(col(2, "t2", "c"), col(4, "t3", "e")),
			eq(col(0, "t1", "a"), lit(5)),
		),
		plan.NewInnerJoin(
			plan.NewResolvedTable(t1, nil, nil),
			plan.NewCrossJoin(
				plan.NewResolvedTable(t2, nil, nil),
				plan.NewResolvedTable(t3, nil, nil),
			),
			and(
				eq(col(0, "t1", "a"), col(2, "t2", "c")),
				eq(col(0, "t1", "a"), col(4, "t3", "e")),
			),
		),
	)

	assertNodesEqualWithDiff(t, expected, result)

	node = plan.NewInnerJoin(
		plan.NewResolvedTable(t1, nil, nil),
		plan.NewCrossJoin(
			plan.NewResolvedTable(t2, nil, nil),
			plan.NewResolvedTable(t3, nil, nil),
		),
		expression.JoinAnd(
			eq(col(0, "t2", "c"), col(0, "t3", "e")),
			eq(col(0, "t1", "a"), lit(5)),
		),
	)

	result, err = rule.Apply(sql.NewEmptyContext(), NewDefault(nil), node, nil)
	require.NoError(err)

	expected = plan.NewFilter(
		expression.JoinAnd(
			eq(col(0, "t2", "c"), col(0, "t3", "e")),
			eq(col(0, "t1", "a"), lit(5)),
		),
		plan.NewCrossJoin(
			plan.NewResolvedTable(t1, nil, nil),
			plan.NewCrossJoin(
				plan.NewResolvedTable(t2, nil, nil),
				plan.NewResolvedTable(t3, nil, nil),
			),
		),
	)

	assertNodesEqualWithDiff(t, expected, result)

	node = plan.NewInnerJoin(
		plan.NewResolvedTable(t1, nil, nil),
		plan.NewInnerJoin(
			plan.NewResolvedTable(t2, nil, nil),
			plan.NewResolvedTable(t3, nil, nil),
			expression.JoinAnd(
				eq(col(0, "t2", "c"), col(0, "t3", "e")),
				eq(col(0, "t3", "a"), lit(5)),
			),
		),
		expression.JoinAnd(
			eq(col(0, "t1", "c"), col(0, "t2", "e")),
			eq(col(0, "t1", "a"), lit(10)),
		),
	)

	result, err = rule.Apply(sql.NewEmptyContext(), NewDefault(nil), node, nil)
	require.NoError(err)

	expected = plan.NewFilter(
		expression.JoinAnd(
			eq(col(0, "t3", "a"), lit(5)),
			eq(col(0, "t1", "a"), lit(10)),
		),
		plan.NewInnerJoin(
			plan.NewResolvedTable(t1, nil, nil),
			plan.NewInnerJoin(
				plan.NewResolvedTable(t2, nil, nil),
				plan.NewResolvedTable(t3, nil, nil),
				expression.JoinAnd(
					eq(col(0, "t2", "c"), col(0, "t3", "e")),
				),
			),
			expression.JoinAnd(
				eq(col(0, "t1", "c"), col(0, "t2", "e")),
			),
		),
	)

	assertNodesEqualWithDiff(t, expected, result)

	node = plan.NewInnerJoin(
		plan.NewResolvedTable(t1, nil, nil),
		plan.NewInnerJoin(
			plan.NewResolvedTable(t2, nil, nil),
			plan.NewResolvedTable(t3, nil, nil),
			expression.JoinAnd(
				eq(col(0, "t2", "c"), col(0, "t3", "e")),
				eq(col(0, "t3", "a"), lit(5)),
			),
		),
		expression.JoinAnd(
			eq(col(0, "t1", "c"), col(0, "t2", "e")),
		),
	)

	result, err = rule.Apply(sql.NewEmptyContext(), NewDefault(nil), node, nil)
	require.NoError(err)

	expected = plan.NewFilter(
		expression.JoinAnd(
			eq(col(0, "t3", "a"), lit(5)),
		),
		plan.NewInnerJoin(
			plan.NewResolvedTable(t1, nil, nil),
			plan.NewInnerJoin(
				plan.NewResolvedTable(t2, nil, nil),
				plan.NewResolvedTable(t3, nil, nil),
				expression.JoinAnd(
					eq(col(0, "t2", "c"), col(0, "t3", "e")),
				),
			),
			expression.JoinAnd(
				eq(col(0, "t1", "c"), col(0, "t2", "e")),
			),
		),
	)

	assertNodesEqualWithDiff(t, expected, result)
}

func TestEvalFilter(t *testing.T) {
	inner := memory.NewTable("foo", nil)
	rule := getRule("eval_filter")

	testCases := []struct {
		filter   sql.Expression
		expected sql.Node
	}{
		{
			and(
				eq(lit(5), lit(5)),
				eq(col(0, "foo", "bar"), lit(5)),
			),
			plan.NewFilter(
				eq(col(0, "foo", "bar"), lit(5)),
				plan.NewResolvedTable(inner, nil, nil),
			),
		},
		{
			and(
				eq(col(0, "foo", "bar"), lit(5)),
				eq(lit(5), lit(5)),
			),
			plan.NewFilter(
				eq(col(0, "foo", "bar"), lit(5)),
				plan.NewResolvedTable(inner, nil, nil),
			),
		},
		{
			and(
				eq(lit(5), lit(4)),
				eq(col(0, "foo", "bar"), lit(5)),
			),
			plan.EmptyTable,
		},
		{
			and(
				eq(col(0, "foo", "bar"), lit(5)),
				eq(lit(5), lit(4)),
			),
			plan.EmptyTable,
		},
		{
			and(
				eq(lit(4), lit(4)),
				eq(lit(5), lit(5)),
			),
			plan.NewResolvedTable(inner, nil, nil),
		},
		{
			or(
				eq(lit(5), lit(4)),
				eq(col(0, "foo", "bar"), lit(5)),
			),
			plan.NewFilter(
				eq(col(0, "foo", "bar"), lit(5)),
				plan.NewResolvedTable(inner, nil, nil),
			),
		},
		{
			or(
				eq(col(0, "foo", "bar"), lit(5)),
				eq(lit(5), lit(4)),
			),
			plan.NewFilter(
				eq(col(0, "foo", "bar"), lit(5)),
				plan.NewResolvedTable(inner, nil, nil),
			),
		},
		{
			or(
				eq(lit(5), lit(5)),
				eq(col(0, "foo", "bar"), lit(5)),
			),
			plan.NewResolvedTable(inner, nil, nil),
		},
		{
			or(
				eq(col(0, "foo", "bar"), lit(5)),
				eq(lit(5), lit(5)),
			),
			plan.NewResolvedTable(inner, nil, nil),
		},
		{
			or(
				eq(lit(5), lit(4)),
				eq(lit(5), lit(4)),
			),
			plan.EmptyTable,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.filter.String(), func(t *testing.T) {
			require := require.New(t)
			node := plan.NewFilter(tt.filter, plan.NewResolvedTable(inner, nil, nil))
			result, err := rule.Apply(sql.NewEmptyContext(), NewDefault(nil), node, nil)
			require.NoError(err)
			require.Equal(tt.expected, result)
		})
	}
}

func TestRemoveUnnecessaryConverts(t *testing.T) {
	testCases := []struct {
		name      string
		childExpr sql.Expression
		castType  string
		expected  sql.Expression
	}{
		{
			"unnecessary cast",
			expression.NewLiteral([]byte{}, sql.LongBlob),
			"binary",
			expression.NewLiteral([]byte{}, sql.LongBlob),
		},
		{
			"necessary cast",
			expression.NewLiteral("foo", sql.LongText),
			"signed",
			expression.NewConvert(
				expression.NewLiteral("foo", sql.LongText),
				"signed",
			),
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)

			node := plan.NewProject([]sql.Expression{
				expression.NewConvert(tt.childExpr, tt.castType),
			},
				plan.NewResolvedTable(memory.NewTable("foo", nil), nil, nil),
			)

			result, err := removeUnnecessaryConverts(
				sql.NewEmptyContext(),
				NewDefault(nil),
				node,
				nil,
			)
			require.NoError(err)

			resultExpr := result.(*plan.Project).Projections[0]
			require.Equal(tt.expected, resultExpr)
		})
	}
}

func TestConvertCrossJoin(t *testing.T) {
	tableA := memory.NewTable("a", sql.Schema{
		{Name: "x", Type: sql.Int64, Source: "a"},
		{Name: "y", Type: sql.Int64, Source: "a"},
		{Name: "z", Type: sql.Int64, Source: "a"},
	})
	tableB := memory.NewTable("b", sql.Schema{
		{Name: "x", Type: sql.Int64, Source: "b"},
		{Name: "y", Type: sql.Int64, Source: "b"},
		{Name: "z", Type: sql.Int64, Source: "b"},
	})

	fieldAx := expression.NewGetFieldWithTable(0, sql.Int64, "a", "x", false)
	fieldBy := expression.NewGetFieldWithTable(0, sql.Int64, "b", "y", false)
	litOne := expression.NewLiteral(1, sql.Int64)

	matching := []sql.Expression{
		expression.NewEquals(fieldAx, fieldBy),
		expression.NewNullSafeEquals(fieldAx, fieldBy),
		expression.NewGreaterThan(fieldAx, fieldBy),
		expression.NewGreaterThanOrEqual(fieldAx, fieldBy),
		expression.NewNullSafeGreaterThan(fieldAx, fieldBy),
		expression.NewNullSafeGreaterThanOrEqual(fieldAx, fieldBy),
		expression.NewLessThan(fieldAx, fieldBy),
		expression.NewNullSafeLessThan(fieldAx, fieldBy),
		expression.NewLessThanOrEqual(fieldAx, fieldBy),
		expression.NewNullSafeLessThanOrEqual(fieldAx, fieldBy),
		expression.NewAnd(
			expression.NewEquals(fieldAx, fieldBy),
			expression.NewEquals(fieldAx, litOne),
		),
		expression.NewOr(
			expression.NewEquals(fieldAx, fieldBy),
			expression.NewEquals(litOne, litOne),
		),
		expression.NewNot(
			expression.NewEquals(fieldAx, fieldBy),
		),
		expression.NewIsFalse(
			expression.NewEquals(fieldAx, fieldBy),
		),
		expression.NewIsTrue(
			expression.NewEquals(fieldAx, fieldBy),
		),
		expression.NewIsNull(
			expression.NewEquals(fieldAx, fieldBy),
		),
	}

	nonMatching := []sql.Expression{
		expression.NewEquals(
			expression.NewGetFieldWithTable(0, sql.Int64, "b", "x", false),
			expression.NewGetFieldWithTable(0, sql.Int64, "b", "y", false),
		),
		expression.NewEquals(
			expression.NewGetFieldWithTable(0, sql.Int64, "b", "x", false),
			aggregation.NewMax(expression.NewGetFieldWithTable(0, sql.Int64, "b", "y", false)),
		),
	}

	tests := make([]analyzerFnTestCase, 0, len(matching)+len(nonMatching))
	for _, t := range matching {
		new := analyzerFnTestCase{
			name: t.String(),
			node: plan.NewFilter(
				t,
				plan.NewCrossJoin(
					plan.NewResolvedTable(tableA, nil, nil),
					plan.NewTableAlias("b", plan.NewResolvedTable(tableB, nil, nil)),
				),
			),
			expected: plan.NewInnerJoin(
				plan.NewResolvedTable(tableA, nil, nil),
				plan.NewTableAlias("b", plan.NewResolvedTable(tableB, nil, nil)),
				t,
			),
		}
		tests = append(tests, new)
	}
	for _, t := range nonMatching {
		new := analyzerFnTestCase{
			name: t.String(),
			node: plan.NewFilter(
				t,
				plan.NewCrossJoin(
					plan.NewResolvedTable(tableA, nil, nil),
					plan.NewTableAlias("b", plan.NewResolvedTable(tableB, nil, nil)),
				),
			),
			expected: plan.NewFilter(
				t,
				plan.NewCrossJoin(
					plan.NewResolvedTable(tableA, nil, nil),
					plan.NewTableAlias("b", plan.NewResolvedTable(tableB, nil, nil)),
				),
			),
		}
		tests = append(tests, new)
	}

	nested := []analyzerFnTestCase{
		{
			name: "nested cross joins",
			node: plan.NewFilter(
				expression.NewAnd(
					expression.NewEquals(
						fieldAx,
						fieldBy,
					),
					expression.NewAnd(
						expression.NewEquals(
							expression.NewGetFieldWithTable(0, sql.Int64, "b", "x", false),
							expression.NewGetFieldWithTable(1, sql.Int64, "c", "y", false),
						),
						expression.NewAnd(
							expression.NewEquals(
								expression.NewGetFieldWithTable(0, sql.Int64, "a", "x", false),
								expression.NewGetFieldWithTable(0, sql.Int64, "a", "x", false),
							),
							expression.NewEquals(
								expression.NewGetFieldWithTable(0, sql.Int64, "c", "x", false),
								expression.NewGetFieldWithTable(1, sql.Int64, "d", "y", false),
							),
						),
					),
				),
				plan.NewCrossJoin(
					plan.NewResolvedTable(tableA, nil, nil),
					plan.NewCrossJoin(
						plan.NewTableAlias("b", plan.NewResolvedTable(tableB, nil, nil)),
						plan.NewCrossJoin(
							plan.NewTableAlias("c", plan.NewResolvedTable(tableB, nil, nil)),
							plan.NewTableAlias("d", plan.NewResolvedTable(tableB, nil, nil)),
						),
						),
				),
			),
			expected: plan.NewFilter(
				expression.NewEquals(
					expression.NewGetFieldWithTable(0, sql.Int64, "a", "x", false),
					expression.NewGetFieldWithTable(0, sql.Int64, "a", "x", false),
				),
				plan.NewInnerJoin(
					plan.NewResolvedTable(tableA, nil, nil),
					plan.NewInnerJoin(
						plan.NewTableAlias("b", plan.NewResolvedTable(tableB, nil, nil)),
						plan.NewInnerJoin(
							plan.NewTableAlias("c", plan.NewResolvedTable(tableB, nil, nil)),
							plan.NewTableAlias("d", plan.NewResolvedTable(tableB, nil, nil)),
							expression.NewEquals(
								expression.NewGetFieldWithTable(0, sql.Int64, "c", "x", false),
								expression.NewGetFieldWithTable(1, sql.Int64, "d", "y", false),
							),
						),
						expression.NewEquals(
							expression.NewGetFieldWithTable(0, sql.Int64, "b", "x", false),
							expression.NewGetFieldWithTable(1, sql.Int64, "c", "y", false),
						),
						),
					expression.NewEquals(fieldAx, fieldBy),
				),
			),
		},
	}
	tests = append(tests, nested...)

	runTestCases(t, sql.NewEmptyContext(), nested, NewDefault(sql.NewDatabaseProvider()), getRule("replace_cross_joins"))
}
