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
	"github.com/dolthub/go-mysql-server/sql/expression/function/aggregation/window"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

func TestPushdownSortProject(t *testing.T) {
	rule := getRule("pushdown_sort")
	a := NewDefault(nil)

	table := memory.NewTable("foo", sql.Schema{
		{Name: "a", Type: sql.Int64, Source: "foo"},
		{Name: "b", Type: sql.Int64, Source: "foo"},
	})

	tests := []analyzerFnTestCase{
		{
			name: "no reorder needed",
			node: plan.NewSort(
				[]sql.SortField{
					{Column: expression.NewUnresolvedColumn("x")},
				},
				plan.NewProject(
					[]sql.Expression{
						expression.NewAlias("x", expression.NewGetFieldWithTable(0, sql.Int64, "foo", "a", false)),
					},
					plan.NewResolvedTable(table, nil, nil),
				),
			),
		},
		{
			name: "push sort below project, alias",
			node: plan.NewSort(
				[]sql.SortField{
					{Column: expression.NewUnresolvedColumn("a")},
				},
				plan.NewProject(
					[]sql.Expression{
						expression.NewAlias("x", expression.NewGetFieldWithTable(0, sql.Int64, "foo", "a", false)),
					},
					plan.NewResolvedTable(table, nil, nil),
				),
			),
			expected: plan.NewProject(
				[]sql.Expression{
					expression.NewAlias("x", expression.NewGetFieldWithTable(0, sql.Int64, "foo", "a", false)),
				},
				plan.NewSort(
					[]sql.SortField{
						{Column: expression.NewUnresolvedColumn("a")},
					},
					plan.NewResolvedTable(table, nil, nil),
				),
			),
		},
		{
			name: "push sort below project, missing field",
			node: plan.NewSort(
				[]sql.SortField{
					{Column: expression.NewUnresolvedColumn("a")},
				},
				plan.NewProject(
					[]sql.Expression{
						expression.NewGetFieldWithTable(1, sql.Int64, "foo", "b", false),
					},
					plan.NewResolvedTable(table, nil, nil),
				),
			),
			expected: plan.NewProject(
				[]sql.Expression{
					expression.NewGetFieldWithTable(1, sql.Int64, "foo", "b", false),
				},
				plan.NewSort(
					[]sql.SortField{
						{Column: expression.NewUnresolvedColumn("a")},
					},
					plan.NewResolvedTable(table, nil, nil),
				),
			),
		},
	}

	runTestCases(t, nil, tests, a, rule)
}

func TestPushdownSortGroupby(t *testing.T) {
	rule := getRule("pushdown_sort")
	a := NewDefault(nil)

	table := memory.NewTable("foo", sql.Schema{
		{Name: "a", Type: sql.Int64, Source: "foo"},
		{Name: "b", Type: sql.Int64, Source: "foo"},
	})

	tests := []analyzerFnTestCase{
		{
			name: "no change required",
			node: plan.NewSort(
				[]sql.SortField{
					{Column: expression.NewUnresolvedColumn("x")},
				},
				plan.NewGroupBy(
					[]sql.Expression{
						expression.NewAlias("x", expression.NewGetFieldWithTable(0, sql.Int64, "foo", "a", false)),
					},
					[]sql.Expression{
						expression.NewGetFieldWithTable(0, sql.Int64, "foo", "a", false),
					},
					plan.NewResolvedTable(table, nil, nil),
				),
			),
		},
		{
			name: "push sort below groupby, with alias",
			node: plan.NewSort(
				[]sql.SortField{
					{Column: expression.NewUnresolvedColumn("a")},
				},
				plan.NewGroupBy(
					[]sql.Expression{
						expression.NewAlias("x", expression.NewGetFieldWithTable(0, sql.Int64, "foo", "a", false)),
					},
					[]sql.Expression{
						expression.NewGetFieldWithTable(0, sql.Int64, "foo", "a", false),
					},
					plan.NewResolvedTable(table, nil, nil),
				),
			),
			expected: plan.NewGroupBy(
				[]sql.Expression{
					expression.NewAlias("x", expression.NewGetFieldWithTable(0, sql.Int64, "foo", "a", false)),
				},
				[]sql.Expression{
					expression.NewGetFieldWithTable(0, sql.Int64, "foo", "a", false),
				},
				plan.NewSort(
					[]sql.SortField{
						{Column: expression.NewUnresolvedColumn("a")},
					},
					plan.NewResolvedTable(table, nil, nil),
				),
			),
		},
		{
			name: "push below groupby, multiple sort columns",
			node: plan.NewSort(
				[]sql.SortField{
					{Column: expression.NewUnresolvedColumn("a")},
					{Column: expression.NewUnresolvedColumn("x")},
				},
				plan.NewGroupBy(
					[]sql.Expression{
						expression.NewAlias("x", expression.NewGetFieldWithTable(0, sql.Int64, "foo", "a", false)),
					},
					[]sql.Expression{
						expression.NewGetFieldWithTable(0, sql.Int64, "foo", "a", false),
					},
					plan.NewResolvedTable(table, nil, nil),
				),
			),
			expected: plan.NewProject(
				[]sql.Expression{
					expression.NewGetFieldWithTable(0, sql.Int64, "", "x", false),
				},
				plan.NewSort(
					[]sql.SortField{
						{Column: expression.NewUnresolvedColumn("a")},
						{Column: expression.NewUnresolvedColumn("x")},
					},
					plan.NewGroupBy(
						[]sql.Expression{
							expression.NewAlias("x", expression.NewGetFieldWithTable(0, sql.Int64, "foo", "a", false)),
							expression.NewUnresolvedColumn("a"),
						},
						[]sql.Expression{
							expression.NewGetFieldWithTable(0, sql.Int64, "foo", "a", false),
						},
						plan.NewResolvedTable(table, nil, nil),
					),
				),
			),
		},
	}

	runTestCases(t, nil, tests, a, rule)
}

func TestPushdownSortWindow(t *testing.T) {
	rule := getRule("pushdown_sort")
	a := NewDefault(nil)

	table := memory.NewTable("foo", sql.Schema{
		{Name: "a", Type: sql.Int64, Source: "foo"},
		{Name: "b", Type: sql.Int64, Source: "foo"},
	})

	ctx := sql.NewEmptyContext()
	tests := []analyzerFnTestCase{
		{
			name: "no change required",
			node: plan.NewSort(
				[]sql.SortField{
					{Column: expression.NewUnresolvedColumn("x")},
				},
				plan.NewWindow(
					[]sql.Expression{
						expression.NewAlias("x", expression.NewGetFieldWithTable(0, sql.Int64, "foo", "a", false)),
						mustExpr(window.NewRowNumber(ctx).(*window.RowNumber).WithWindow(
							sql.NewWindow(
								[]sql.Expression{
									expression.NewGetFieldWithTable(0, sql.Int64, "foo", "b", false),
								},
								sql.SortFields{
									{
										Column: expression.NewGetFieldWithTable(0, sql.Int64, "foo", "a", false),
									},
								},
							),
						)),
					},
					plan.NewResolvedTable(table, nil, nil),
				),
			),
		},
		{
			name: "push sort below window, with alias",
			node: plan.NewSort(
				[]sql.SortField{
					{Column: expression.NewUnresolvedColumn("a")},
				},
				plan.NewWindow(
					[]sql.Expression{
						expression.NewAlias("x", expression.NewGetFieldWithTable(0, sql.Int64, "foo", "a", false)),
						mustExpr(window.NewRowNumber(ctx).(*window.RowNumber).WithWindow(
							sql.NewWindow(
								[]sql.Expression{
									expression.NewGetFieldWithTable(0, sql.Int64, "foo", "b", false),
								},
								sql.SortFields{
									{
										Column: expression.NewGetFieldWithTable(0, sql.Int64, "foo", "a", false),
									},
								},
							),
						)),
					},
					plan.NewResolvedTable(table, nil, nil),
				),
			),
			expected: plan.NewWindow(
				[]sql.Expression{
					expression.NewAlias("x", expression.NewGetFieldWithTable(0, sql.Int64, "foo", "a", false)),
					mustExpr(window.NewRowNumber(ctx).(*window.RowNumber).WithWindow(
						sql.NewWindow(
							[]sql.Expression{
								expression.NewGetFieldWithTable(0, sql.Int64, "foo", "b", false),
							},
							sql.SortFields{
								{
									Column: expression.NewGetFieldWithTable(0, sql.Int64, "foo", "a", false),
								},
							},
						),
					)),
				},
				plan.NewSort(
					[]sql.SortField{
						{Column: expression.NewUnresolvedColumn("a")},
					},
					plan.NewResolvedTable(table, nil, nil),
				),
			),
		},
		{
			name: "push sort below window, missing field",
			node: plan.NewSort(
				[]sql.SortField{
					{Column: expression.NewUnresolvedColumn("a")},
				},
				plan.NewWindow(
					[]sql.Expression{
						expression.NewGetFieldWithTable(0, sql.Int64, "foo", "b", false),
						mustExpr(window.NewRowNumber(ctx).(*window.RowNumber).WithWindow(
							sql.NewWindow(
								[]sql.Expression{
									expression.NewGetFieldWithTable(0, sql.Int64, "foo", "b", false),
								},
								sql.SortFields{
									{
										Column: expression.NewGetFieldWithTable(0, sql.Int64, "foo", "a", false),
									},
								},
							),
						)),
					},
					plan.NewResolvedTable(table, nil, nil),
				),
			),
			expected: plan.NewWindow(
				[]sql.Expression{
					expression.NewGetFieldWithTable(0, sql.Int64, "foo", "b", false),
					mustExpr(window.NewRowNumber(ctx).(*window.RowNumber).WithWindow(
						sql.NewWindow(
							[]sql.Expression{
								expression.NewGetFieldWithTable(0, sql.Int64, "foo", "b", false),
							},
							sql.SortFields{
								{
									Column: expression.NewGetFieldWithTable(0, sql.Int64, "foo", "a", false),
								},
							},
						),
					)),
				},
				plan.NewSort(
					[]sql.SortField{
						{Column: expression.NewUnresolvedColumn("a")},
					},
					plan.NewResolvedTable(table, nil, nil),
				),
			),
		},
	}

	runTestCases(t, nil, tests, a, rule)
}

func TestResolveOrderByLiterals(t *testing.T) {
	require := require.New(t)
	f := getRule("resolve_orderby_literals")

	table := memory.NewTable("t", sql.Schema{
		{Name: "a", Type: sql.Int64, Source: "t"},
		{Name: "b", Type: sql.Int64, Source: "t"},
	})

	node := plan.NewSort(
		[]sql.SortField{
			{Column: expression.NewLiteral(int64(2), sql.Int64)},
			{Column: expression.NewLiteral(int64(1), sql.Int64)},
		},
		plan.NewResolvedTable(table, nil, nil),
	)

	result, err := f.Apply(sql.NewEmptyContext(), NewDefault(nil), node, nil)
	require.NoError(err)

	require.Equal(
		plan.NewSort(
			[]sql.SortField{
				{Column: expression.NewUnresolvedQualifiedColumn("t", "b")},
				{Column: expression.NewUnresolvedQualifiedColumn("t", "a")},
			},
			plan.NewResolvedTable(table, nil, nil),
		),
		result,
	)

	node = plan.NewSort(
		[]sql.SortField{
			{Column: expression.NewLiteral(int64(3), sql.Int64)},
			{Column: expression.NewLiteral(int64(1), sql.Int64)},
		},
		plan.NewResolvedTable(table, nil, nil),
	)

	_, err = f.Apply(sql.NewEmptyContext(), NewDefault(nil), node, nil)
	require.Error(err)
	require.True(ErrOrderByColumnIndex.Is(err))
}
