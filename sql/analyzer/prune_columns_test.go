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

func TestPruneColumns(t *testing.T) {
	rule := getRuleFrom(OnceAfterDefault, "prune_columns")

	t1 := plan.NewResolvedTable(memory.NewTable("t1", sql.Schema{
		{Name: "foo", Type: sql.Int64, Source: "t1"},
		{Name: "bar", Type: sql.Int64, Source: "t1"},
		{Name: "bax", Type: sql.Int64, Source: "t1"},
	}), nil, nil)

	t2 := plan.NewResolvedTable(memory.NewTable("t2", sql.Schema{
		{Name: "foo", Type: sql.Int64, Source: "t2"},
		{Name: "baz", Type: sql.Int64, Source: "t2"},
		{Name: "bux", Type: sql.Int64, Source: "t2"},
	}), nil, nil)

	testCases := []analyzerFnTestCase{
		{
			name: "natural join",
			node: plan.NewProject(
				[]sql.Expression{
					gf(0, "t1", "foo"),
					gf(1, "", "some_alias"),
				},
				plan.NewProject(
					[]sql.Expression{
						gf(0, "t1", "foo"),
						expression.NewAlias("some_alias", gf(1, "t1", "bar")),
					},
					plan.NewFilter(
						eq(gf(0, "t1", "foo"), gf(4, "t2", "baz")),
						plan.NewProject(
							[]sql.Expression{
								gf(0, "t1", "foo"),
								gf(1, "t1", "bar"),
								gf(2, "t1", "bax"),
								gf(4, "t2", "baz"),
								gf(5, "t2", "bux"),
							},
							plan.NewCrossJoin(t1, t2),
						),
					),
				),
			),
			expected: plan.NewProject(
				[]sql.Expression{
					gf(0, "t1", "foo"),
					gf(1, "", "some_alias"),
				},
				plan.NewProject(
					[]sql.Expression{
						gf(0, "t1", "foo"),
						expression.NewAlias("some_alias", gf(1, "t1", "bar")),
					},
					plan.NewFilter(
						eq(gf(0, "t1", "foo"), gf(2, "t2", "baz")),
						plan.NewProject(
							[]sql.Expression{
								gf(0, "t1", "foo"),
								gf(1, "t1", "bar"),
								gf(4, "t2", "baz"),
							},
							plan.NewCrossJoin(t1, t2),
						),
					),
				),
			),
		},
		{
			name: "subquery",
			node: plan.NewProject(
				[]sql.Expression{
					gf(0, "t", "foo"),
					gf(1, "", "some_alias"),
				},
				plan.NewProject(
					[]sql.Expression{
						gf(0, "t", "foo"),
						expression.NewAlias("some_alias", gf(1, "t", "bar")),
					},
					plan.NewFilter(
						eq(gf(0, "t", "foo"), gf(4, "t", "baz")),
						plan.NewSubqueryAlias("t", "",
							plan.NewProject(
								[]sql.Expression{
									gf(0, "t1", "foo"),
									gf(1, "t1", "bar"),
									gf(2, "t1", "bax"),
									gf(4, "t2", "baz"),
									gf(5, "t2", "bux"),
								},
								plan.NewCrossJoin(t1, t2),
							),
						),
					),
				),
			),
			expected: plan.NewProject(
				[]sql.Expression{
					gf(0, "t", "foo"),
					gf(1, "", "some_alias"),
				},
				plan.NewProject(
					[]sql.Expression{
						gf(0, "t", "foo"),
						expression.NewAlias("some_alias", gf(1, "t", "bar")),
					},
					plan.NewFilter(
						eq(gf(0, "t", "foo"), gf(2, "t", "baz")),
						plan.NewSubqueryAlias("t", "",
							plan.NewProject(
								[]sql.Expression{
									gf(0, "t1", "foo"),
									gf(1, "t1", "bar"),
									gf(4, "t2", "baz"),
								},
								plan.NewCrossJoin(t1, t2),
							),
						),
					),
				),
			),
		},
		{
			name: "group by",
			node: plan.NewGroupBy(
				[]sql.Expression{
					gf(0, "t1", "foo"),
					gf(1, "", "some_alias"),
				},
				[]sql.Expression{
					gf(0, "t1", "foo"),
					gf(5, "t2", "bux"),
					gf(1, "", "some_alias"),
				},
				plan.NewProject(
					[]sql.Expression{
						gf(0, "t1", "foo"),
						expression.NewAlias("some_alias", gf(1, "t1", "bar")),
						gf(5, "t2", "bux"),
					},
					plan.NewFilter(
						eq(gf(0, "t1", "foo"), gf(4, "t2", "baz")),
						plan.NewProject(
							[]sql.Expression{
								gf(0, "t1", "foo"),
								gf(1, "t1", "bar"),
								gf(2, "t1", "bax"),
								gf(4, "t2", "baz"),
								gf(5, "t2", "bux"),
							},
							plan.NewCrossJoin(t1, t2),
						),
					),
				),
			),
			expected: plan.NewGroupBy(
				[]sql.Expression{
					gf(0, "t1", "foo"),
					gf(1, "", "some_alias"),
				},
				[]sql.Expression{
					gf(0, "t1", "foo"),
					gf(2, "t2", "bux"),
					gf(1, "", "some_alias"),
				},
				plan.NewProject(
					[]sql.Expression{
						gf(0, "t1", "foo"),
						expression.NewAlias("some_alias", gf(1, "t1", "bar")),
						gf(3, "t2", "bux"),
					},
					plan.NewFilter(
						eq(gf(0, "t1", "foo"), gf(2, "t2", "baz")),
						plan.NewProject(
							[]sql.Expression{
								gf(0, "t1", "foo"),
								gf(1, "t1", "bar"),
								gf(4, "t2", "baz"),
								gf(5, "t2", "bux"),
							},
							plan.NewCrossJoin(t1, t2),
						),
					),
				),
			),
		},
		{
			name: "used inside subquery and not outside",
			node: plan.NewProject(
				[]sql.Expression{
					gf(0, "sq", "foo"),
				},
				plan.NewSubqueryAlias("sq", "",
					plan.NewProject(
						[]sql.Expression{gf(0, "t1", "foo")},
						plan.NewInnerJoin(
							plan.NewProject(
								[]sql.Expression{
									gf(0, "t1", "foo"),
									gf(1, "t1", "bar"),
									gf(2, "t1", "bax"),
								},
								t1,
							),
							plan.NewProject(
								[]sql.Expression{
									gf(0, "t2", "foo"),
									gf(1, "t2", "baz"),
									gf(2, "t2", "bux"),
								},
								t2,
							),
							expression.NewEquals(
								gf(0, "t1", "foo"),
								gf(3, "t2", "foo"),
							),
						),
					),
				),
			),
			expected: plan.NewProject(
				[]sql.Expression{
					gf(0, "sq", "foo"),
				},
				plan.NewSubqueryAlias("sq", "",
					plan.NewProject(
						[]sql.Expression{gf(0, "t1", "foo")},
						plan.NewInnerJoin(
							plan.NewProject(
								[]sql.Expression{
									gf(0, "t1", "foo"),
								},
								t1,
							),
							plan.NewProject(
								[]sql.Expression{
									gf(0, "t2", "foo"),
								},
								t2,
							),
							expression.NewEquals(
								gf(0, "t1", "foo"),
								gf(1, "t2", "foo"),
							),
						),
					),
				),
			),
		},
		{
			name: "Unqualified columns in subquery",
			node: plan.NewProject(
				[]sql.Expression{
					expression.NewUnresolvedQualifiedColumn("t1", "foo"),
					plan.NewSubquery(
						plan.NewFilter(
							expression.NewGreaterThan(
								expression.NewUnresolvedQualifiedColumn("t1", "bar"),
								expression.NewUnresolvedQualifiedColumn("t2", "foo"),
							),
							plan.NewProject(
								[]sql.Expression{
									aggregation.NewMax(sql.NewEmptyContext(), expression.NewUnresolvedQualifiedColumn("t2", "baz")),
								},
								plan.NewResolvedTable(t2, nil, nil),
							),
						),
						""),
				},
				plan.NewResolvedTable(t1, nil, nil),
			),
		},
		{
			name: "Retain projected columns used in subquery",
			node: plan.NewProject(
				[]sql.Expression{
					gf(0, "t1", "foo"),
					plan.NewSubquery(
						plan.NewFilter(
							expression.NewGreaterThan(
								gf(1, "t1", "bar"),
								gf(2, "t2", "foo"),
							),
							plan.NewProject(
								[]sql.Expression{
									aggregation.NewMax(sql.NewEmptyContext(), gf(3, "t2", "baz")),
								},
								plan.NewResolvedTable(t2, nil, nil),
							),
						),
						""),
				},
				plan.NewProject(
					[]sql.Expression{
						gf(0, "t1", "foo"),
						gf(1, "t1", "bar"),
					},
					plan.NewResolvedTable(t1, nil, nil),
				),
			),
		},
		{
			name: "Retain alias used in subquery",
			node: plan.NewProject(
				[]sql.Expression{
					gf(0, "t1", "foo"),
					plan.NewSubquery(
						plan.NewFilter(
							expression.NewGreaterThan(
								gf(1, "t1", "bar"),
								gf(3, "", "x"),
							),
							plan.NewProject(
								[]sql.Expression{
									aggregation.NewMax(sql.NewEmptyContext(), gf(4, "t2", "baz")),
								},
								plan.NewResolvedTable(t2, nil, nil),
							),
						),
						""),
				},
				plan.NewProject(
					[]sql.Expression{
						gf(0, "t1", "foo"),
						gf(1, "t1", "bar"),
						expression.NewAlias("x", gf(0, "t1", "foo")),
					},
					plan.NewResolvedTable(t1, nil, nil),
				),
			),
		},
		{
			name: "Keep projected columns when there is a subquery",
			node: plan.NewProject(
				[]sql.Expression{
					gf(0, "t1", "foo"),
					plan.NewSubquery(
						plan.NewFilter(
							expression.NewGreaterThan(
								gf(1, "t1", "foo"),
								gf(1, "t1", "foo"),
							),
							plan.NewProject(
								[]sql.Expression{
									aggregation.NewMax(sql.NewEmptyContext(), gf(2, "t2", "baz")),
								},
								plan.NewResolvedTable(t2, nil, nil),
							),
						),
						""),
				},
				plan.NewProject(
					[]sql.Expression{
						gf(0, "t1", "foo"),
						gf(1, "t1", "bar"),
					},
					plan.NewResolvedTable(t1, nil, nil),
				),
			),
			expected: plan.NewProject(
				[]sql.Expression{
					gf(0, "t1", "foo"),
					plan.NewSubquery(
						plan.NewFilter(
							expression.NewGreaterThan(
								gf(1, "t1", "foo"),
								gf(1, "t1", "foo"),
							),
							plan.NewProject(
								[]sql.Expression{
									aggregation.NewMax(sql.NewEmptyContext(), gf(2, "t2", "baz")),
								},
								plan.NewResolvedTable(t2, nil, nil),
							),
						),
						""),
				},
				plan.NewProject(
					[]sql.Expression{
						gf(0, "t1", "foo"),
						gf(1, "t1", "bar"),
					},
					plan.NewResolvedTable(t1, nil, nil),
				),
			),
		},
		{
			name: "Fix indexes in subquery expression",
			scope: newScope(plan.NewProject(
				[]sql.Expression{
					gf(0, "t1", "foo"),
					plan.NewSubquery(
						plan.NewFilter(
							expression.NewGreaterThan(
								gf(4, "t1", "foo"),
								gf(5, "t1", "bar"),
							),
							plan.NewProject(
								[]sql.Expression{
									aggregation.NewMax(sql.NewEmptyContext(), gf(6, "t2", "baz")),
								},
								plan.NewResolvedTable(t2, nil, nil),
							),
						),
						""),
				},
				plan.NewProject(
					[]sql.Expression{
						gf(0, "t1", "foo"),
						gf(1, "t1", "bar"),
					},
					plan.NewResolvedTable(t1, nil, nil),
				),
			)),
			node: plan.NewFilter(
				expression.NewGreaterThan(
					gf(4, "t1", "foo"),
					gf(5, "t1", "bar"),
				),
				plan.NewProject(
					[]sql.Expression{
						aggregation.NewMax(sql.NewEmptyContext(), gf(6, "t2", "baz")),
					},
					plan.NewResolvedTable(t2, nil, nil),
				),
			),
			expected: plan.NewFilter(
				expression.NewGreaterThan(
					gf(0, "t1", "foo"),
					gf(1, "t1", "bar"),
				),
				plan.NewProject(
					[]sql.Expression{
						aggregation.NewMax(sql.NewEmptyContext(), gf(3, "t2", "baz")),
					},
					plan.NewResolvedTable(t2, nil, nil),
				),
			),
		},
		{
			name: "Fix indexes in subquery expression with aliases",
			scope: newScope(plan.NewProject(
				[]sql.Expression{
					gf(0, "t1", "foo"),
					plan.NewSubquery(
						plan.NewFilter(
							expression.NewGreaterThan(
								gf(4, "t1", "foo"),
								gf(5, "", "x"),
							),
							plan.NewProject(
								[]sql.Expression{
									aggregation.NewMax(sql.NewEmptyContext(), gf(6, "t2", "baz")),
								},
								plan.NewResolvedTable(t2, nil, nil),
							),
						),
						""),
				},
				plan.NewProject(
					[]sql.Expression{
						gf(0, "t1", "foo"),
						gf(1, "t1", "bar"),
						expression.NewAlias("x", gf(0, "t1", "foo")),
					},
					plan.NewResolvedTable(t1, nil, nil),
				),
			)),
			node: plan.NewFilter(
				expression.NewGreaterThan(
					gf(4, "t1", "foo"),
					gf(5, "", "x"),
				),
				plan.NewProject(
					[]sql.Expression{
						aggregation.NewMax(sql.NewEmptyContext(), gf(6, "t2", "baz")),
					},
					plan.NewResolvedTable(t2, nil, nil),
				),
			),
			expected: plan.NewFilter(
				expression.NewGreaterThan(
					gf(0, "t1", "foo"),
					gf(2, "", "x"),
				),
				plan.NewProject(
					[]sql.Expression{
						aggregation.NewMax(sql.NewEmptyContext(), gf(4, "t2", "baz")),
					},
					plan.NewResolvedTable(t2, nil, nil),
				),
			),
		},
	}

	runTestCases(t, nil, testCases, nil, *rule)
}
