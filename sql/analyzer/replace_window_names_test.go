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

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

func TestReplaceWindowNames(t *testing.T) {
	tests := []analyzerFnTestCase{
		{
			name: "embed named window",
			node: plan.NewNamedWindows(
				map[string]*sql.WindowDefinition{
					"w": sql.NewWindowDefinition([]sql.Expression{
						expression.NewUnresolvedColumn("x"),
					}, nil, plan.NewRangeNPrecedingToCurrentRowFrame(
						expression.NewInterval(
							expression.NewLiteral("2:30", sql.LongText),
							"MINUTE_SECOND",
						),
					), "", "w"),
				},
				plan.NewWindow(
					[]sql.Expression{
						expression.NewAlias("row_number() over (w)",
							expression.NewUnresolvedFunction("row_number", true, sql.NewWindowDefinition([]sql.Expression{}, nil, nil, "w", "")),
						),
					},
					plan.NewUnresolvedTable("foo", ""),
				),
			),
			expected: plan.NewWindow(
				[]sql.Expression{
					expression.NewAlias("row_number() over (w)",
						expression.NewUnresolvedFunction(
							"row_number",
							true,
							sql.NewWindowDefinition(
								[]sql.Expression{expression.NewUnresolvedColumn("x")},
								nil,
								plan.NewRangeNPrecedingToCurrentRowFrame(
									expression.NewInterval(
										expression.NewLiteral("2:30", sql.LongText),
										"MINUTE_SECOND",
									),
								), "", "",
							),
						),
					),
				},
				plan.NewUnresolvedTable("foo", ""),
			),
		},
		{
			name: "recursive embed window names",
			node: plan.NewNamedWindows(
				map[string]*sql.WindowDefinition{
					"w1": sql.NewWindowDefinition([]sql.Expression{}, sql.SortFields{
						{
							Column:       expression.NewUnresolvedColumn("x"),
							Order:        sql.Ascending,
							NullOrdering: sql.NullsFirst,
						},
					}, nil, "w2", "w1"),
					"w2": sql.NewWindowDefinition([]sql.Expression{}, nil, nil, "", "w2"),
				},
				plan.NewWindow(
					[]sql.Expression{
						expression.NewUnresolvedColumn("a"),
						expression.NewAlias("row_number() over (w1)",
							expression.NewUnresolvedFunction("row_number", true, sql.NewWindowDefinition([]sql.Expression{}, nil, nil, "w1", "")),
						),
						expression.NewAlias("max(b) over (w2)",
							expression.NewUnresolvedFunction("max", true, sql.NewWindowDefinition([]sql.Expression{}, nil, nil, "w2", ""),
								expression.NewUnresolvedColumn("b"),
							),
						),
					},
					plan.NewUnresolvedTable("foo", ""),
				),
			),
			expected: plan.NewWindow(
				[]sql.Expression{
					expression.NewUnresolvedColumn("a"),
					expression.NewAlias("row_number() over (w1)",
						expression.NewUnresolvedFunction("row_number", true,
							sql.NewWindowDefinition(
								[]sql.Expression{},
								sql.SortFields{
									{
										Column:       expression.NewUnresolvedColumn("x"),
										Order:        sql.Ascending,
										NullOrdering: sql.NullsFirst,
									},
								}, nil, "", ""),
						),
					),
					expression.NewAlias("max(b) over (w2)",
						expression.NewUnresolvedFunction("max", true,
							sql.NewWindowDefinition([]sql.Expression{}, nil, nil, "", ""),
							expression.NewUnresolvedColumn("b"),
						),
					),
				},
				plan.NewUnresolvedTable("foo", ""),
			),
		},
		{
			name: "merge window def recursive",
			node: plan.NewNamedWindows(
				map[string]*sql.WindowDefinition{
					"w1": sql.NewWindowDefinition([]sql.Expression{}, sql.SortFields{
						{
							Column:       expression.NewUnresolvedColumn("x"),
							Order:        sql.Ascending,
							NullOrdering: sql.NullsFirst,
						},
					}, nil, "w2", "w1"),
					"w2": sql.NewWindowDefinition([]sql.Expression{}, nil, nil, "", "w2"),
				}, plan.NewWindow(
					[]sql.Expression{
						expression.NewUnresolvedColumn("a"),
						expression.NewAlias("row_number() over (w1 partition by y)",
							expression.NewUnresolvedFunction("row_number", true, sql.NewWindowDefinition(
								[]sql.Expression{
									expression.NewUnresolvedColumn("y"),
								},
								nil, nil, "w1", "")),
						),
						expression.NewAlias("max(b) over (w2)",
							expression.NewUnresolvedFunction("max", true, sql.NewWindowDefinition([]sql.Expression{}, nil, nil, "w2", ""),
								expression.NewUnresolvedColumn("b"),
							),
						),
					},
					plan.NewUnresolvedTable("foo", ""),
				),
			),
			expected: plan.NewWindow(
				[]sql.Expression{
					expression.NewUnresolvedColumn("a"),
					expression.NewAlias("row_number() over (w1 partition by y)",
						expression.NewUnresolvedFunction("row_number", true,
							sql.NewWindowDefinition(
								[]sql.Expression{
									expression.NewUnresolvedColumn("y"),
								},
								sql.SortFields{
									{
										Column:       expression.NewUnresolvedColumn("x"),
										Order:        sql.Ascending,
										NullOrdering: sql.NullsFirst,
									},
								}, nil, "", ""),
						),
					),
					expression.NewAlias("max(b) over (w2)",
						expression.NewUnresolvedFunction("max", true, sql.NewWindowDefinition([]sql.Expression{}, nil, nil, "", ""),
							expression.NewUnresolvedColumn("b"),
						),
					),
				},
				plan.NewUnresolvedTable("foo", ""),
			),
		},
		{
			name: "window def but no embed",
			node: plan.NewNamedWindows(
				map[string]*sql.WindowDefinition{
					"w": sql.NewWindowDefinition([]sql.Expression{
						expression.NewUnresolvedColumn("x"),
					}, nil, plan.NewRangeNPrecedingToCurrentRowFrame(
						expression.NewInterval(
							expression.NewLiteral("2:30", sql.LongText),
							"MINUTE_SECOND",
						),
					), "", "w"),
				},
				plan.NewWindow(
					[]sql.Expression{
						expression.NewAlias("row_number() over (w)",
							expression.NewUnresolvedFunction("row_number", true, sql.NewWindowDefinition([]sql.Expression{}, nil, nil, "", "")),
						),
					},
					plan.NewUnresolvedTable("foo", ""),
				),
			),
			expected: plan.NewWindow(
				[]sql.Expression{
					expression.NewAlias("row_number() over (w)",
						expression.NewUnresolvedFunction("row_number", true, sql.NewWindowDefinition([]sql.Expression{}, nil, nil, "", "")),
					),
				},
				plan.NewUnresolvedTable("foo", ""),
			),
		},
		{
			name: "window def order by error",
			node: plan.NewNamedWindows(
				map[string]*sql.WindowDefinition{
					"w": sql.NewWindowDefinition(
						[]sql.Expression{},
						sql.SortFields{
							{
								Column:       expression.NewUnresolvedColumn("x"),
								Order:        sql.Ascending,
								NullOrdering: sql.NullsFirst,
							},
						}, nil, "", "w"),
				},
				plan.NewWindow(
					[]sql.Expression{
						expression.NewAlias("row_number() over (w order by y)",
							expression.NewUnresolvedFunction("row_number", true,
								sql.NewWindowDefinition(
									[]sql.Expression{},
									sql.SortFields{
										{
											Column:       expression.NewUnresolvedColumn("y"),
											Order:        sql.Ascending,
											NullOrdering: sql.NullsFirst,
										},
									}, nil, "w", "")),
						),
					},
					plan.NewUnresolvedTable("foo", ""),
				),
			),
			err: sql.ErrInvalidWindowInheritance,
		},
		{
			name: "window def partition by conflict",
			node: plan.NewNamedWindows(
				map[string]*sql.WindowDefinition{
					"w": sql.NewWindowDefinition(
						[]sql.Expression{expression.NewUnresolvedColumn("x")},
						nil, nil, "", "w"),
				},
				plan.NewWindow(
					[]sql.Expression{
						expression.NewAlias("row_number() over (w order by y)",
							expression.NewUnresolvedFunction("row_number", true,
								sql.NewWindowDefinition(
									[]sql.Expression{expression.NewUnresolvedColumn("y")},
									nil, nil, "w", "")),
						),
					},
					plan.NewUnresolvedTable("foo", ""),
				),
			),
			err: sql.ErrInvalidWindowInheritance,
		},
		{
			name: "window def frame conflict",
			node: plan.NewNamedWindows(
				map[string]*sql.WindowDefinition{
					"w": sql.NewWindowDefinition(
						[]sql.Expression{}, nil,
						plan.NewRangeNPrecedingToCurrentRowFrame(
							expression.NewInterval(
								expression.NewLiteral("2:30", sql.LongText),
								"MINUTE_SECOND",
							),
						), "", "w"),
				},
				plan.NewWindow(
					[]sql.Expression{
						expression.NewAlias("row_number() over (w order by y)",
							expression.NewUnresolvedFunction("row_number", true,
								sql.NewWindowDefinition(
									[]sql.Expression{}, nil,
									plan.NewRangeNPrecedingToCurrentRowFrame(
										expression.NewInterval(
											expression.NewLiteral("2:30", sql.LongText),
											"MINUTE_SECOND",
										),
									), "w", "")),
						),
					},
					plan.NewUnresolvedTable("foo", ""),
				),
			),
			err: sql.ErrInvalidWindowInheritance,
		},
		{
			name: "window def is circular conflict",
			node: plan.NewNamedWindows(
				map[string]*sql.WindowDefinition{
					"w1": sql.NewWindowDefinition([]sql.Expression{}, nil, nil, "w2", "w1"),
					"w2": sql.NewWindowDefinition([]sql.Expression{}, nil, nil, "w3", "w2"),
					"w3": sql.NewWindowDefinition([]sql.Expression{}, nil, nil, "w4", "w3"),
					"w4": sql.NewWindowDefinition([]sql.Expression{}, nil, nil, "w1", "w4"),
				},
				plan.NewWindow(
					[]sql.Expression{
						expression.NewAlias("row_number() over (w order by y)",
							expression.NewUnresolvedFunction("row_number", true,
								sql.NewWindowDefinition([]sql.Expression{}, nil, nil, "", ""),
							),
						),
					},
					plan.NewUnresolvedTable("foo", ""),
				),
			),
			err: sql.ErrCircularWindowInheritance,
		},
	}
	runTestCases(t, sql.NewEmptyContext(), tests, NewDefault(sql.NewDatabaseProvider()), getRule("resolve_named_windows"))
}
