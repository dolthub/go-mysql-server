// Copyright 2022 DoltHub, Inc.
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

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/expression/function/aggregation"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestConvertCrossJoin(t *testing.T) {
	db := memory.NewDatabase("db")
	pro := memory.NewDBProvider(db)
	ctx := newContext(pro)

	tableA := memory.NewTable(db, "a", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "x", Type: types.Int64, Source: "a"},
		{Name: "y", Type: types.Int64, Source: "a"},
		{Name: "z", Type: types.Int64, Source: "a"},
	}), nil)
	tableB := memory.NewTable(db, "b", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "x", Type: types.Int64, Source: "b"},
		{Name: "y", Type: types.Int64, Source: "b"},
		{Name: "z", Type: types.Int64, Source: "b"},
	}), nil)

	fieldAx := expression.NewGetFieldWithTable(0, types.Int64, "db", "a", "x", false)
	fieldBy := expression.NewGetFieldWithTable(0, types.Int64, "db", "b", "y", false)
	litOne := expression.NewLiteral(1, types.Int64)

	matching := []sql.Expression{
		expression.NewEquals(fieldAx, fieldBy),
		expression.NewNullSafeEquals(fieldAx, fieldBy),
		expression.NewGreaterThan(fieldAx, fieldBy),
		expression.NewGreaterThanOrEqual(fieldAx, fieldBy),
		expression.NewLessThan(fieldAx, fieldBy),
		expression.NewLessThanOrEqual(fieldAx, fieldBy),
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
			expression.NewGetFieldWithTable(0, types.Int64, "db", "b", "x", false),
			expression.NewGetFieldWithTable(0, types.Int64, "db", "b", "y", false),
		),
		expression.NewEquals(
			expression.NewGetFieldWithTable(0, types.Int64, "db", "b", "x", false),
			aggregation.NewMax(expression.NewGetFieldWithTable(0, types.Int64, "db", "b", "y", false)),
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
			name: "split AND into predicate leaves",
			node: plan.NewFilter(
				expression.NewAnd(
					expression.NewEquals(fieldAx, fieldBy),
					expression.NewEquals(fieldAx, litOne),
				),
				plan.NewCrossJoin(
					plan.NewResolvedTable(tableA, nil, nil),
					plan.NewTableAlias("b", plan.NewResolvedTable(tableB, nil, nil)),
				),
			),
			expected: plan.NewFilter(
				expression.NewEquals(fieldAx, litOne),
				plan.NewInnerJoin(
					plan.NewResolvedTable(tableA, nil, nil),
					plan.NewTableAlias("b", plan.NewResolvedTable(tableB, nil, nil)),
					expression.NewEquals(fieldAx, fieldBy),
				),
			),
		},
		{
			name: "carry whole OR expression as join expression",
			node: plan.NewFilter(
				expression.NewAnd(
					expression.NewOr(
						expression.NewEquals(fieldAx, fieldBy),
						expression.NewEquals(fieldAx, litOne),
					),
					expression.NewEquals(fieldAx, litOne),
				),
				plan.NewCrossJoin(
					plan.NewResolvedTable(tableA, nil, nil),
					plan.NewTableAlias("b", plan.NewResolvedTable(tableB, nil, nil)),
				),
			),
			expected: plan.NewFilter(
				expression.NewEquals(fieldAx, litOne),
				plan.NewInnerJoin(
					plan.NewResolvedTable(tableA, nil, nil),
					plan.NewTableAlias("b", plan.NewResolvedTable(tableB, nil, nil)),
					expression.NewOr(
						expression.NewEquals(fieldAx, fieldBy),
						expression.NewEquals(fieldAx, litOne),
					),
				),
			),
		},
		{
			name: "nested cross joins full conversion",
			node: plan.NewFilter(
				expression.NewAnd(
					expression.NewEquals(fieldAx, fieldBy),
					expression.NewAnd(
						expression.NewEquals(
							expression.NewGetFieldWithTable(0, types.Int64, "db", "b", "x", false),
							expression.NewGetFieldWithTable(1, types.Int64, "db", "c", "y", false),
						),
						expression.NewAnd(
							expression.NewEquals(
								expression.NewGetFieldWithTable(0, types.Int64, "db", "a", "x", false),
								expression.NewGetFieldWithTable(0, types.Int64, "db", "a", "x", false),
							),
							expression.NewEquals(
								expression.NewGetFieldWithTable(0, types.Int64, "db", "c", "x", false),
								expression.NewGetFieldWithTable(1, types.Int64, "db", "d", "y", false),
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
					expression.NewGetFieldWithTable(0, types.Int64, "db", "a", "x", false),
					expression.NewGetFieldWithTable(0, types.Int64, "db", "a", "x", false),
				),
				plan.NewInnerJoin(
					plan.NewResolvedTable(tableA, nil, nil),
					plan.NewInnerJoin(
						plan.NewTableAlias("b", plan.NewResolvedTable(tableB, nil, nil)),
						plan.NewInnerJoin(
							plan.NewTableAlias("c", plan.NewResolvedTable(tableB, nil, nil)),
							plan.NewTableAlias("d", plan.NewResolvedTable(tableB, nil, nil)),
							expression.NewEquals(
								expression.NewGetFieldWithTable(0, types.Int64, "db", "c", "x", false),
								expression.NewGetFieldWithTable(1, types.Int64, "db", "d", "y", false),
							),
						),
						expression.NewEquals(
							expression.NewGetFieldWithTable(0, types.Int64, "db", "b", "x", false),
							expression.NewGetFieldWithTable(1, types.Int64, "db", "c", "y", false),
						),
					),
					expression.NewEquals(fieldAx, fieldBy),
				),
			),
		},
		{
			name: "nested cross joins partial conversion",
			node: plan.NewFilter(
				expression.NewAnd(
					expression.NewEquals(fieldAx, fieldBy),
					expression.NewEquals(
						expression.NewGetFieldWithTable(0, types.Int64, "db", "b", "x", false),
						expression.NewGetFieldWithTable(1, types.Int64, "db", "c", "y", false),
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
			expected: plan.NewInnerJoin(
				plan.NewResolvedTable(tableA, nil, nil),
				plan.NewInnerJoin(
					plan.NewTableAlias("b", plan.NewResolvedTable(tableB, nil, nil)),
					plan.NewCrossJoin(
						plan.NewTableAlias("c", plan.NewResolvedTable(tableB, nil, nil)),
						plan.NewTableAlias("d", plan.NewResolvedTable(tableB, nil, nil)),
					),
					expression.NewEquals(
						expression.NewGetFieldWithTable(0, types.Int64, "db", "b", "x", false),
						expression.NewGetFieldWithTable(1, types.Int64, "db", "c", "y", false),
					),
				),
				expression.NewEquals(fieldAx, fieldBy),
			),
		},
	}
	tests = append(tests, nested...)

	runTestCases(t, ctx, tests, NewDefault(sql.NewDatabaseProvider()), getRule(replaceCrossJoinsId))
}
