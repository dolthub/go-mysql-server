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

	"github.com/gabereiser/go-mysql-server/memory"
	"github.com/gabereiser/go-mysql-server/sql"
	"github.com/gabereiser/go-mysql-server/sql/expression"
	"github.com/gabereiser/go-mysql-server/sql/expression/function/aggregation/window"
	"github.com/gabereiser/go-mysql-server/sql/plan"
	"github.com/gabereiser/go-mysql-server/sql/types"
)

func TestExpandStars(t *testing.T) {
	f := getRule(expandStarsId)

	table := memory.NewTable("mytable", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "a", Type: types.Int32, Source: "mytable"},
		{Name: "b", Type: types.Int32, Source: "mytable"},
	}), nil)

	table2 := memory.NewTable("mytable2", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "c", Type: types.Int32, Source: "mytable2"},
		{Name: "d", Type: types.Int32, Source: "mytable2"},
	}), nil)

	testCases := []analyzerFnTestCase{
		{
			name: "unqualified star",
			node: plan.NewProject(
				[]sql.Expression{expression.NewStar()},
				plan.NewResolvedTable(table, nil, nil),
			),
			expected: plan.NewProject(
				[]sql.Expression{
					expression.NewGetFieldWithTable(0, types.Int32, "mytable", "a", false),
					expression.NewGetFieldWithTable(1, types.Int32, "mytable", "b", false),
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
					expression.NewGetFieldWithTable(2, types.Int32, "mytable2", "c", false),
					expression.NewGetFieldWithTable(3, types.Int32, "mytable2", "d", false),
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
					expression.NewGetFieldWithTable(0, types.Int32, "mytable", "a", false),
					expression.NewGetFieldWithTable(1, types.Int32, "mytable", "b", false),
					expression.NewGetFieldWithTable(2, types.Int32, "mytable2", "c", false),
					expression.NewGetFieldWithTable(3, types.Int32, "mytable2", "d", false),
					expression.NewGetFieldWithTable(2, types.Int32, "mytable2", "c", false),
					expression.NewGetFieldWithTable(3, types.Int32, "mytable2", "d", false),
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
					expression.NewGetFieldWithTable(0, types.Int32, "mytable", "a", false),
					expression.NewGetFieldWithTable(1, types.Int32, "mytable", "b", false),
					expression.NewGetFieldWithTable(2, types.Int32, "mytable2", "c", false),
					expression.NewGetFieldWithTable(3, types.Int32, "mytable2", "d", false),
					expression.NewUnresolvedColumn("foo"),
					expression.NewGetFieldWithTable(2, types.Int32, "mytable2", "c", false),
					expression.NewGetFieldWithTable(3, types.Int32, "mytable2", "d", false),
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
					expression.NewGetFieldWithTable(0, types.Int32, "mytable", "a", false),
					expression.NewGetFieldWithTable(1, types.Int32, "mytable", "b", false),
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
					expression.NewGetFieldWithTable(1, types.Int32, "mytable", "b", false),
					mustExpr(window.NewRowNumber().(*window.RowNumber).WithWindow(
						sql.NewWindowDefinition([]sql.Expression{
							expression.NewGetFieldWithTable(1, types.Int32, "mytable", "b", false),
						}, sql.SortFields{
							{
								Column: expression.NewGetFieldWithTable(0, types.Int32, "mytable", "a", false),
							},
						}, nil, "", ""),
					)),
				},
				plan.NewResolvedTable(table, nil, nil),
			),
			expected: plan.NewWindow(
				[]sql.Expression{
					expression.NewGetFieldWithTable(0, types.Int32, "mytable", "a", false),
					expression.NewGetFieldWithTable(1, types.Int32, "mytable", "b", false),
					expression.NewGetFieldWithTable(1, types.Int32, "mytable", "b", false),
					mustExpr(window.NewRowNumber().(*window.RowNumber).WithWindow(
						sql.NewWindowDefinition([]sql.Expression{
							expression.NewGetFieldWithTable(1, types.Int32, "mytable", "b", false),
						}, sql.SortFields{
							{
								Column: expression.NewGetFieldWithTable(0, types.Int32, "mytable", "a", false),
							},
						}, nil, "", ""),
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
					expression.NewGetFieldWithTable(0, types.Int32, "mytable", "a", false),
					expression.NewGetFieldWithTable(1, types.Int32, "mytable", "b", false),
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
					expression.NewGetFieldWithTable(0, types.Int32, "mytable", "a", false),
					expression.NewGetFieldWithTable(1, types.Int32, "mytable", "b", false),
					expression.NewGetFieldWithTable(0, types.Int32, "mytable", "a", false),
					expression.NewGetFieldWithTable(1, types.Int32, "mytable", "b", false),
				},
				plan.NewResolvedTable(table, nil, nil),
			),
		},
	}

	runTestCases(t, nil, testCases, NewDefault(nil), f)
}
