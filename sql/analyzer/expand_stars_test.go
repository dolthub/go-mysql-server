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
	"github.com/dolthub/go-mysql-server/sql/expression/function/aggregation/window"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

func TestExpandStars(t *testing.T) {
	ctx := sql.NewEmptyContext()
	f := getRule("expand_stars")

	table := memory.NewTable("mytable", sql.Schema{
		{Name: "a", Type: sql.Int32, Source: "mytable"},
		{Name: "b", Type: sql.Int32, Source: "mytable"},
	})

	table2 := memory.NewTable("mytable2", sql.Schema{
		{Name: "c", Type: sql.Int32, Source: "mytable2"},
		{Name: "d", Type: sql.Int32, Source: "mytable2"},
	})

	testCases := []analyzerFnTestCase{
		{
			name: "unqualified star",
			node: plan.NewProject(
				[]sql.Expression{expression.NewStar()},
				plan.NewResolvedTable(table, nil, nil),
			),
			expected: plan.NewProject(
				[]sql.Expression{
					expression.NewGetFieldWithTable(0, sql.Int32, "mytable", "a", false),
					expression.NewGetFieldWithTable(1, sql.Int32, "mytable", "b", false),
				},
				plan.NewResolvedTable(table, nil, nil),
			),
		},
		{
			name: "qualified star",
			node: plan.NewProject(
				[]sql.Expression{expression.NewQualifiedStar("mytable2")},
				plan.NewCrossJoin(
					plan.NewResolvedTable(table, nil, nil),
					plan.NewResolvedTable(table2, nil, nil),
				),
			),
			expected: plan.NewProject(
				[]sql.Expression{
					expression.NewGetFieldWithTable(2, sql.Int32, "mytable2", "c", false),
					expression.NewGetFieldWithTable(3, sql.Int32, "mytable2", "d", false),
				},
				plan.NewCrossJoin(
					plan.NewResolvedTable(table, nil, nil),
					plan.NewResolvedTable(table2, nil, nil),
				),
			),
		},
		{
			name: "qualified star and unqualified star",
			node: plan.NewProject(
				[]sql.Expression{
					expression.NewStar(),
					expression.NewQualifiedStar("mytable2"),
				},
				plan.NewCrossJoin(
					plan.NewResolvedTable(table, nil, nil),
					plan.NewResolvedTable(table2, nil, nil),
				),
			),
			expected: plan.NewProject(
				[]sql.Expression{
					expression.NewGetFieldWithTable(0, sql.Int32, "mytable", "a", false),
					expression.NewGetFieldWithTable(1, sql.Int32, "mytable", "b", false),
					expression.NewGetFieldWithTable(2, sql.Int32, "mytable2", "c", false),
					expression.NewGetFieldWithTable(3, sql.Int32, "mytable2", "d", false),
					expression.NewGetFieldWithTable(2, sql.Int32, "mytable2", "c", false),
					expression.NewGetFieldWithTable(3, sql.Int32, "mytable2", "d", false),
				},
				plan.NewCrossJoin(
					plan.NewResolvedTable(table, nil, nil),
					plan.NewResolvedTable(table2, nil, nil),
				),
			),
		},
		{
			name: "stars mixed with other expressions",
			node: plan.NewProject(
				[]sql.Expression{
					expression.NewStar(),
					expression.NewUnresolvedColumn("foo"),
					expression.NewQualifiedStar("mytable2"),
				},
				plan.NewCrossJoin(
					plan.NewResolvedTable(table, nil, nil),
					plan.NewResolvedTable(table2, nil, nil),
				),
			),
			expected: plan.NewProject(
				[]sql.Expression{
					expression.NewGetFieldWithTable(0, sql.Int32, "mytable", "a", false),
					expression.NewGetFieldWithTable(1, sql.Int32, "mytable", "b", false),
					expression.NewGetFieldWithTable(2, sql.Int32, "mytable2", "c", false),
					expression.NewGetFieldWithTable(3, sql.Int32, "mytable2", "d", false),
					expression.NewUnresolvedColumn("foo"),
					expression.NewGetFieldWithTable(2, sql.Int32, "mytable2", "c", false),
					expression.NewGetFieldWithTable(3, sql.Int32, "mytable2", "d", false),
				},
				plan.NewCrossJoin(
					plan.NewResolvedTable(table, nil, nil),
					plan.NewResolvedTable(table2, nil, nil),
				),
			),
		},
		{
			name: "star in groupby",
			node: plan.NewGroupBy(
				[]sql.Expression{
					expression.NewStar(),
				},
				nil,
				plan.NewResolvedTable(table, nil, nil),
			),
			expected: plan.NewGroupBy(
				[]sql.Expression{
					expression.NewGetFieldWithTable(0, sql.Int32, "mytable", "a", false),
					expression.NewGetFieldWithTable(1, sql.Int32, "mytable", "b", false),
				},
				nil,
				plan.NewResolvedTable(table, nil, nil),
			),
		},
		{
			name: "star in window",
			node: plan.NewWindow(
				[]sql.Expression{
					expression.NewStar(),
					expression.NewGetFieldWithTable(1, sql.Int32, "mytable", "b", false),
					mustExpr(window.NewRowNumber(ctx).(*window.RowNumber).WithWindow(
						sql.NewWindow(
							[]sql.Expression{
								expression.NewGetFieldWithTable(1, sql.Int32, "mytable", "b", false),
							},
							sql.SortFields{
								{
									Column: expression.NewGetFieldWithTable(0, sql.Int32, "mytable", "a", false),
								},
							},
						),
					)),
				},
				plan.NewResolvedTable(table, nil, nil),
			),
			expected: plan.NewWindow(
				[]sql.Expression{
					expression.NewGetFieldWithTable(0, sql.Int32, "mytable", "a", false),
					expression.NewGetFieldWithTable(1, sql.Int32, "mytable", "b", false),
					expression.NewGetFieldWithTable(1, sql.Int32, "mytable", "b", false),
					mustExpr(window.NewRowNumber(ctx).(*window.RowNumber).WithWindow(
						sql.NewWindow(
							[]sql.Expression{
								expression.NewGetFieldWithTable(1, sql.Int32, "mytable", "b", false),
							},
							sql.SortFields{
								{
									Column: expression.NewGetFieldWithTable(0, sql.Int32, "mytable", "a", false),
								},
							},
						),
					)),
				},
				plan.NewResolvedTable(table, nil, nil),
			),
		},
		{ // note that this behaviour deviates from MySQL
			name: "star after some expressions",
			node: plan.NewProject(
				[]sql.Expression{
					expression.NewUnresolvedColumn("foo"),
					expression.NewStar(),
				},
				plan.NewResolvedTable(table, nil, nil),
			),
			expected: plan.NewProject(
				[]sql.Expression{
					expression.NewUnresolvedColumn("foo"),
					expression.NewGetFieldWithTable(0, sql.Int32, "mytable", "a", false),
					expression.NewGetFieldWithTable(1, sql.Int32, "mytable", "b", false),
				},
				plan.NewResolvedTable(table, nil, nil),
			),
		},
		{ // note that this behaviour deviates from MySQL
			name: "unqualified star used multiple times",
			node: plan.NewProject(
				[]sql.Expression{
					expression.NewStar(),
					expression.NewStar(),
				},
				plan.NewResolvedTable(table, nil, nil),
			),
			expected: plan.NewProject(
				[]sql.Expression{
					expression.NewGetFieldWithTable(0, sql.Int32, "mytable", "a", false),
					expression.NewGetFieldWithTable(1, sql.Int32, "mytable", "b", false),
					expression.NewGetFieldWithTable(0, sql.Int32, "mytable", "a", false),
					expression.NewGetFieldWithTable(1, sql.Int32, "mytable", "b", false),
				},
				plan.NewResolvedTable(table, nil, nil),
			),
		},
	}

	runTestCases(t, nil, testCases, NewDefault(nil), f)
}
