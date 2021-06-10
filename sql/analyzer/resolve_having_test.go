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

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/expression/function/aggregation"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

func TestResolveHaving(t *testing.T) {
	ctx := sql.NewEmptyContext()
	testCases := []analyzerFnTestCase{
		{
			name: "replace existing aggregation in group by",
			node: plan.NewHaving(
				expression.NewGreaterThan(
					aggregation.NewAvg(ctx, expression.NewUnresolvedColumn("foo")),
					expression.NewLiteral(int64(5), sql.Int64),
				),
				plan.NewGroupBy(
					[]sql.Expression{
						expression.NewAlias("x", aggregation.NewAvg(ctx, expression.NewGetFieldWithTable(0, sql.Int64, "t", "foo", false))),
						expression.NewGetField(0, sql.Int64, "foo", false),
					},
					[]sql.Expression{expression.NewGetField(0, sql.Int64, "foo", false)},
					plan.NewResolvedTable(memory.NewTable("t", nil), nil, nil),
				),
			),
			expected: plan.NewHaving(
				expression.NewGreaterThan(
					expression.NewGetField(0, sql.Float64, "x", true),
					expression.NewLiteral(int64(5), sql.Int64),
				),
				plan.NewGroupBy(
					[]sql.Expression{
						expression.NewAlias("x", aggregation.NewAvg(ctx, expression.NewGetFieldWithTable(0, sql.Int64, "t", "foo", false))),
						expression.NewGetField(0, sql.Int64, "foo", false),
					},
					[]sql.Expression{expression.NewGetField(0, sql.Int64, "foo", false)},
					plan.NewResolvedTable(memory.NewTable("t", nil), nil, nil),
				),
			),
		},
		{
			name: "replace existing aggregation in group by, deferred column",
			node: plan.NewHaving(
				expression.NewGreaterThan(
					aggregation.NewAvg(ctx, &deferredColumn{expression.NewUnresolvedColumn("foo")}),
					expression.NewLiteral(int64(5), sql.Int64),
				),
				plan.NewGroupBy(
					[]sql.Expression{
						expression.NewAlias("x", aggregation.NewAvg(ctx, expression.NewGetFieldWithTable(0, sql.Int64, "t", "foo", false))),
						expression.NewGetField(0, sql.Int64, "foo", false),
					},
					[]sql.Expression{expression.NewGetField(0, sql.Int64, "foo", false)},
					plan.NewResolvedTable(memory.NewTable("t", nil), nil, nil),
				),
			),
			expected: plan.NewHaving(
				expression.NewGreaterThan(
					expression.NewGetField(0, sql.Float64, "x", true),
					expression.NewLiteral(int64(5), sql.Int64),
				),
				plan.NewGroupBy(
					[]sql.Expression{
						expression.NewAlias("x", aggregation.NewAvg(ctx, expression.NewGetFieldWithTable(0, sql.Int64, "t", "foo", false))),
						expression.NewGetField(0, sql.Int64, "foo", false),
					},
					[]sql.Expression{expression.NewGetField(0, sql.Int64, "foo", false)},
					plan.NewResolvedTable(memory.NewTable("t", nil), nil, nil),
				),
			),
		},
		{
			name: "push down aggregation to group by",
			node: plan.NewHaving(
				expression.NewGreaterThan(
					aggregation.NewCount(ctx, expression.NewStar()),
					expression.NewLiteral(int64(5), sql.Int64),
				),
				plan.NewGroupBy(
					[]sql.Expression{
						expression.NewAlias("x", aggregation.NewAvg(ctx, expression.NewGetField(0, sql.Int64, "foo", false))),
						expression.NewGetFieldWithTable(0, sql.Int64, "t", "foo", false),
					},
					[]sql.Expression{expression.NewGetField(0, sql.Int64, "foo", false)},
					plan.NewResolvedTable(memory.NewTable("t", nil), nil, nil),
				),
			),
			expected: plan.NewProject(
				[]sql.Expression{
					expression.NewGetField(0, sql.Float64, "x", true),
					expression.NewGetFieldWithTable(1, sql.Int64, "t", "foo", false),
				},
				plan.NewHaving(
					expression.NewGreaterThan(
						expression.NewGetField(2, sql.Int64, "COUNT(*)", false),
						expression.NewLiteral(int64(5), sql.Int64),
					),
					plan.NewGroupBy(
						[]sql.Expression{
							expression.NewAlias("x", aggregation.NewAvg(ctx, expression.NewGetField(0, sql.Int64, "foo", false))),
							expression.NewGetFieldWithTable(0, sql.Int64, "t", "foo", false),
							aggregation.NewCount(ctx, expression.NewStar()),
						},
						[]sql.Expression{expression.NewGetField(0, sql.Int64, "foo", false)},
						plan.NewResolvedTable(memory.NewTable("t", nil), nil, nil),
					),
				),
			),
		},
		// TODO: this should be an error in most cases -- the having clause must only reference columns in the select clause.
		{
			name: "pull up missing column",
			node: plan.NewHaving(
				expression.NewGreaterThan(
					expression.NewUnresolvedColumn("i"),
					expression.NewLiteral(int64(5), sql.Int64),
				),
				plan.NewGroupBy(
					[]sql.Expression{
						expression.NewGetFieldWithTable(1, sql.Int64, "t", "foo", false),
					},
					[]sql.Expression{expression.NewGetFieldWithTable(1, sql.Int64, "t", "foo", false)},
					plan.NewResolvedTable(memory.NewTable("t", sql.Schema{
						{Type: sql.Int64, Name: "i", Source: "t"},
						{Type: sql.Int64, Name: "foo", Source: "t"},
					}), nil, nil),
				),
			),
			expected: plan.NewProject(
				[]sql.Expression{
					expression.NewGetFieldWithTable(0, sql.Int64, "t", "foo", false),
				},
				plan.NewHaving(
					expression.NewGreaterThan(
						expression.NewUnresolvedColumn("i"),
						expression.NewLiteral(int64(5), sql.Int64),
					),
					plan.NewGroupBy(
						[]sql.Expression{
							expression.NewGetFieldWithTable(1, sql.Int64, "t", "foo", false),
							expression.NewGetFieldWithTable(0, sql.Int64, "t", "i", false),
						},
						[]sql.Expression{expression.NewGetFieldWithTable(1, sql.Int64, "t", "foo", false)},
						plan.NewResolvedTable(memory.NewTable("t", sql.Schema{
							{Type: sql.Int64, Name: "i", Source: "t"},
							{Type: sql.Int64, Name: "foo", Source: "t"},
						}), nil, nil),
					),
				),
			),
		},
		{
			name: "pull up missing column with nodes in between",
			node: plan.NewHaving(
				expression.NewGreaterThan(
					expression.NewUnresolvedColumn("i"),
					expression.NewLiteral(int64(5), sql.Int64),
				),
				plan.NewProject(
					[]sql.Expression{
						expression.NewGetFieldWithTable(0, sql.Int64, "t", "foo", false),
					},
					plan.NewGroupBy(
						[]sql.Expression{
							expression.NewGetFieldWithTable(1, sql.Int64, "t", "foo", false),
						},
						[]sql.Expression{expression.NewGetFieldWithTable(1, sql.Int64, "t", "foo", false)},
						plan.NewResolvedTable(memory.NewTable("t", sql.Schema{
							{Type: sql.Int64, Name: "i", Source: "t"},
							{Type: sql.Int64, Name: "foo", Source: "t"},
						}), nil, nil),
					),
				),
			),
			expected: plan.NewProject(
				[]sql.Expression{
					expression.NewGetFieldWithTable(0, sql.Int64, "t", "foo", false),
				},
				plan.NewHaving(
					expression.NewGreaterThan(
						expression.NewUnresolvedColumn("i"),
						expression.NewLiteral(int64(5), sql.Int64),
					),
					plan.NewProject(
						[]sql.Expression{
							expression.NewGetFieldWithTable(0, sql.Int64, "t", "foo", false),
							expression.NewGetFieldWithTable(1, sql.Int64, "t", "i", false),
						},
						plan.NewGroupBy(
							[]sql.Expression{
								expression.NewGetFieldWithTable(1, sql.Int64, "t", "foo", false),
								expression.NewGetFieldWithTable(0, sql.Int64, "t", "i", false),
							},
							[]sql.Expression{expression.NewGetFieldWithTable(1, sql.Int64, "t", "foo", false)},
							plan.NewResolvedTable(memory.NewTable("t", sql.Schema{
								{Type: sql.Int64, Name: "i", Source: "t"},
								{Type: sql.Int64, Name: "foo", Source: "t"},
							}), nil, nil),
						),
					),
				),
			),
		},
		{
			name: "push down aggregations with nodes in between",
			node: plan.NewHaving(
				expression.NewGreaterThan(
					aggregation.NewCount(ctx, expression.NewStar()),
					expression.NewLiteral(int64(5), sql.Int64),
				),
				plan.NewProject(
					[]sql.Expression{
						expression.NewAlias("x", expression.NewGetField(0, sql.Float64, "avg(foo)", false)),
						expression.NewGetFieldWithTable(1, sql.Int64, "t", "foo", false),
					},
					plan.NewGroupBy(
						[]sql.Expression{
							aggregation.NewAvg(ctx, expression.NewGetField(0, sql.Int64, "foo", false)),
							expression.NewGetFieldWithTable(0, sql.Int64, "t", "foo", false),
						},
						[]sql.Expression{expression.NewGetField(0, sql.Int64, "foo", false)},
						plan.NewResolvedTable(memory.NewTable("t", nil), nil, nil),
					),
				),
			),
			expected: plan.NewProject(
				[]sql.Expression{
					expression.NewGetField(0, sql.Float64, "x", false),
					expression.NewGetFieldWithTable(1, sql.Int64, "t", "foo", false),
				},
				plan.NewHaving(
					expression.NewGreaterThan(
						expression.NewGetField(2, sql.Int64, "COUNT(*)", false),
						expression.NewLiteral(int64(5), sql.Int64),
					),
					plan.NewProject(
						[]sql.Expression{
							expression.NewAlias("x", expression.NewGetField(0, sql.Float64, "avg(foo)", false)),
							expression.NewGetFieldWithTable(1, sql.Int64, "t", "foo", false),
							expression.NewGetField(2, sql.Int64, "COUNT(*)", false),
						},
						plan.NewGroupBy(
							[]sql.Expression{
								aggregation.NewAvg(ctx, expression.NewGetField(0, sql.Int64, "foo", false)),
								expression.NewGetFieldWithTable(0, sql.Int64, "t", "foo", false),
								aggregation.NewCount(ctx, expression.NewStar()),
							},
							[]sql.Expression{expression.NewGetField(0, sql.Int64, "foo", false)},
							plan.NewResolvedTable(memory.NewTable("t", nil), nil, nil),
						),
					),
				),
			),
		},
		{
			name: "replace existing aggregation in group by with nodes in between",
			node: plan.NewHaving(
				expression.NewGreaterThan(
					aggregation.NewAvg(ctx, expression.NewUnresolvedColumn("foo")),
					expression.NewLiteral(int64(5), sql.Int64),
				),
				plan.NewProject(
					[]sql.Expression{
						expression.NewGetField(0, sql.Float64, "x", false),
						expression.NewGetField(1, sql.Int64, "foo", false),
					},
					plan.NewGroupBy(
						[]sql.Expression{
							expression.NewAlias("x", aggregation.NewAvg(ctx, expression.NewGetFieldWithTable(0, sql.Int64, "t", "foo", false))),
							expression.NewGetField(0, sql.Int64, "foo", false),
						},
						[]sql.Expression{expression.NewGetField(0, sql.Int64, "foo", false)},
						plan.NewResolvedTable(memory.NewTable("t", nil), nil, nil),
					),
				),
			),
			expected: plan.NewHaving(
				expression.NewGreaterThan(
					expression.NewGetField(0, sql.Float64, "x", false),
					expression.NewLiteral(int64(5), sql.Int64),
				),
				plan.NewProject(
					[]sql.Expression{
						expression.NewGetField(0, sql.Float64, "x", false),
						expression.NewGetField(1, sql.Int64, "foo", false),
					},
					plan.NewGroupBy(
						[]sql.Expression{
							expression.NewAlias("x", aggregation.NewAvg(ctx, expression.NewGetFieldWithTable(0, sql.Int64, "t", "foo", false))),
							expression.NewGetField(0, sql.Int64, "foo", false),
						},
						[]sql.Expression{expression.NewGetField(0, sql.Int64, "foo", false)},
						plan.NewResolvedTable(memory.NewTable("t", nil), nil, nil),
					),
				),
			),
		},
		{
			name: "missing groupby",
			node: plan.NewHaving(
				expression.NewGreaterThan(
					aggregation.NewCount(ctx, expression.NewStar()),
					expression.NewLiteral(int64(5), sql.Int64),
				),
				plan.NewResolvedTable(memory.NewTable("t", nil), nil, nil),
			),
			err: errHavingNeedsGroupBy,
		},
	}

	rule := getRule("resolve_having")
	runTestCases(t, nil, testCases, nil, rule)
}
