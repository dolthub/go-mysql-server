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

	"github.com/dolthub/go-mysql-server/sql/expression/function/aggregation"

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

func TestReorderProjection(t *testing.T) {
	f := getRule("reorder_projection")

	table := memory.NewTable("mytable", sql.Schema{
		{Name: "i", Source: "mytable", Type: sql.Int64},
		{Name: "s", Source: "mytable", Type: sql.Int64},
	})

	testCases := []analyzerFnTestCase{
		{
			name: "sort",
			node: plan.NewProject(
				[]sql.Expression{
					gf(0, "mytable", "i"),
					expression.NewAlias("foo", lit(1)),
					expression.NewAlias("bar", lit(2)),
				},
				plan.NewSort(
					[]sql.SortField{
						{Column: uc("foo")},
					},
					plan.NewFilter(
						expression.NewEquals(
							lit(1),
							uc("bar"),
						),
						plan.NewResolvedTable(table, nil, nil),
					),
				),
			),
			expected: plan.NewProject(
				[]sql.Expression{
					gf(0, "mytable", "i"),
					gf(3, "", "foo"),
					gf(2, "", "bar"),
				},
				plan.NewSort(
					[]sql.SortField{{Column: gf(3, "", "foo")}},
					plan.NewProject(
						[]sql.Expression{
							gf(0, "mytable", "i"),
							gf(1, "mytable", "s"),
							gf(2, "", "bar"),
							expression.NewAlias("foo", lit(1)),
						},
						plan.NewFilter(
							expression.NewEquals(
								lit(1),
								gf(2, "", "bar"),
							),
							plan.NewProject(
								[]sql.Expression{
									gf(0, "mytable", "i"),
									gf(1, "mytable", "s"),
									expression.NewAlias("bar", lit(2)),
								},
								plan.NewResolvedTable(table, nil, nil),
							),
						),
					),
				),
			),
		},
		{
			name: "use alias twice",
			node: plan.NewProject(
				[]sql.Expression{
					expression.NewAlias("foo", lit(1)),
				},
				plan.NewFilter(
					expression.NewOr(
						expression.NewEquals(
							lit(1),
							uc("foo"),
						),
						expression.NewEquals(
							lit(1),
							uc("foo"),
						),
					),
					plan.NewResolvedTable(table, nil, nil),
				),
			),
			expected: plan.NewProject(
				[]sql.Expression{
					gf(2, "", "foo"),
				},
				plan.NewFilter(
					expression.NewOr(
						expression.NewEquals(
							lit(1),
							gf(2, "", "foo"),
						),
						expression.NewEquals(
							lit(1),
							gf(2, "", "foo"),
						),
					),
					plan.NewProject(
						[]sql.Expression{
							gf(0, "mytable", "i"),
							gf(1, "mytable", "s"),
							expression.NewAlias("foo", lit(1)),
						},
						plan.NewResolvedTable(table, nil, nil),
					),
				),
			),
		},
	}

	runTestCases(t, nil, testCases, nil, f)
}

func TestReorderProjectionWithSubqueries(t *testing.T) {
	f := getRule("reorder_projection")

	onepk := memory.NewTable("one_pk", sql.Schema{
		{Name: "pk", Source: "one_pk", Type: sql.Int64, PrimaryKey: true},
		{Name: "c1", Source: "one_pk", Type: sql.Int64},
	})
	twopk := memory.NewTable("two_pk", sql.Schema{
		{Name: "pk1", Source: "two_pk", Type: sql.Int64, PrimaryKey: true},
		{Name: "pk2", Source: "two_pk", Type: sql.Int64, PrimaryKey: true},
		{Name: "c1", Source: "two_pk", Type: sql.Int64},
	})

	testCases := []analyzerFnTestCase{
		{
			name: "no reorder needed",
			node: plan.NewSort(
				[]sql.SortField{
					{Column: gf(0, "one_pk", "pk")},
				}, plan.NewProject(
					[]sql.Expression{
						gf(0, "one_pk", "pk"),
						plan.NewSubquery(
							plan.NewGroupBy(
								[]sql.Expression{
									aggregation.NewMax(sql.NewEmptyContext(), uqc("two_pk", "pk1")),
								},
								nil,
								plan.NewFilter(
									expression.NewLessThan(
										gf(2, "one_pk", "pk1"),
										&deferredColumn{uc("pk")},
									),
									plan.NewResolvedTable(twopk, nil, nil),
								),
							),
							"select max(pk1) from two_pk where pk1 < pk"),
					}, plan.NewResolvedTable(onepk, nil, nil)),
			),
			expected: nil,
		},
		{
			name: "subquery with an alias reference",
			node: plan.NewSort(
				[]sql.SortField{
					{Column: gf(0, "one_pk", "pk")},
				}, plan.NewProject(
					[]sql.Expression{
						expression.NewAlias("a", gf(0, "one_pk", "pk")),
						plan.NewSubquery(
							plan.NewGroupBy(
								[]sql.Expression{
									aggregation.NewMax(sql.NewEmptyContext(), uqc("one_pk", "pk")),
								},
								nil,
								plan.NewFilter(
									expression.NewLessThanOrEqual(
										gf(2, "one_pk", "pk"),
										&deferredColumn{uc("a")},
									),
									plan.NewResolvedTable(onepk, nil, nil),
								),
							),
							"SELECT max(pk) FROM one_pk WHERE pk <= a"),
					}, plan.NewResolvedTable(onepk, nil, nil)),
			),
			expected: plan.NewSort(
				[]sql.SortField{
					{Column: gf(0, "one_pk", "pk")},
				}, plan.NewProject(
					[]sql.Expression{
						gf(2, "", "a"),
						plan.NewSubquery(
							plan.NewGroupBy(
								[]sql.Expression{
									aggregation.NewMax(sql.NewEmptyContext(), uqc("one_pk", "pk")),
								},
								nil,
								plan.NewFilter(
									expression.NewLessThanOrEqual(
										gf(2, "one_pk", "pk"),
										&deferredColumn{uc("a")},
									),
									plan.NewResolvedTable(onepk, nil, nil),
								),
							),
							"SELECT max(pk) FROM one_pk WHERE pk <= a"),
					}, plan.NewProject(
						[]sql.Expression{
							gf(0, "one_pk", "pk"),
							gf(1, "one_pk", "c1"),
							expression.NewAlias("a", gf(0, "one_pk", "pk")),
						},
						plan.NewResolvedTable(onepk, nil, nil),
					),
				),
			),
		},
	}

	runTestCases(t, nil, testCases, nil, f)
}
