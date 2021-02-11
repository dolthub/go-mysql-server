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
	"github.com/dolthub/go-mysql-server/sql/plan"
)

func TestPushdownSortProject(t *testing.T) {
	rule := getRule("pushdown_sort")
	a := NewDefault(nil)
	ctx := sql.NewEmptyContext()

	table := memory.NewTable("foo", sql.Schema{
		{Name: "a", Type: sql.Int64, Source: "foo"},
		{Name: "b", Type: sql.Int64, Source: "foo"},
	})

	require := require.New(t)
	node := plan.NewSort(
		[]expression.SortField{
			{Column: expression.NewUnresolvedColumn("x")},
		},
		plan.NewProject(
			[]sql.Expression{
				expression.NewAlias("x", expression.NewGetFieldWithTable(0, sql.Int64, "foo", "a", false)),
			},
			plan.NewResolvedTable(table),
		),
	)

	result, err := rule.Apply(ctx, a, node, nil)
	require.NoError(err)

	require.Equal(node, result)

	node = plan.NewSort(
		[]expression.SortField{
			{Column: expression.NewUnresolvedColumn("a")},
		},
		plan.NewProject(
			[]sql.Expression{
				expression.NewAlias("x", expression.NewGetFieldWithTable(0, sql.Int64, "foo", "a", false)),
			},
			plan.NewResolvedTable(table),
		),
	)

	expected := plan.NewProject(
		[]sql.Expression{
			expression.NewAlias("x", expression.NewGetFieldWithTable(0, sql.Int64, "foo", "a", false)),
		},
		plan.NewSort(
			[]expression.SortField{
				{Column: expression.NewUnresolvedColumn("a")},
			},
			plan.NewResolvedTable(table),
		),
	)

	result, err = rule.Apply(ctx, a, node, nil)
	require.NoError(err)

	require.Equal(expected, result)

	node = plan.NewSort(
		[]expression.SortField{
			{Column: expression.NewUnresolvedColumn("a")},
			{Column: expression.NewUnresolvedColumn("x")},
		},
		plan.NewProject(
			[]sql.Expression{
				expression.NewAlias("x", expression.NewGetFieldWithTable(0, sql.Int64, "foo", "a", false)),
			},
			plan.NewResolvedTable(table),
		),
	)

	expected = plan.NewProject(
		[]sql.Expression{
			expression.NewGetFieldWithTable(0, sql.Int64, "", "x", false),
		},
		plan.NewSort(
			[]expression.SortField{
				{Column: expression.NewUnresolvedColumn("a")},
				{Column: expression.NewUnresolvedColumn("x")},
			},
			plan.NewProject(
				[]sql.Expression{
					expression.NewAlias("x", expression.NewGetFieldWithTable(0, sql.Int64, "foo", "a", false)),
					expression.NewUnresolvedColumn("a"),
				},
				plan.NewResolvedTable(table),
			),
		),
	)

	result, err = rule.Apply(ctx, a, node, nil)
	require.NoError(err)

	require.Equal(expected, result)
}

func TestPushdownSortGroupby(t *testing.T) {
	rule := getRule("pushdown_sort")
	a := NewDefault(nil)
	ctx := sql.NewEmptyContext()

	table := memory.NewTable("foo", sql.Schema{
		{Name: "a", Type: sql.Int64, Source: "foo"},
		{Name: "b", Type: sql.Int64, Source: "foo"},
	})

	require := require.New(t)
	node := plan.NewSort(
		[]expression.SortField{
			{Column: expression.NewUnresolvedColumn("x")},
		},
		plan.NewGroupBy(
			[]sql.Expression{
				expression.NewAlias("x", expression.NewGetFieldWithTable(0, sql.Int64, "foo", "a", false)),
			},
			[]sql.Expression{
				expression.NewGetFieldWithTable(0, sql.Int64, "foo", "a", false),
			},
			plan.NewResolvedTable(table),
		),
	)

	result, err := rule.Apply(ctx, a, node, nil)
	require.NoError(err)

	require.Equal(node, result)

	node = plan.NewSort(
		[]expression.SortField{
			{Column: expression.NewUnresolvedColumn("a")},
		},
		plan.NewGroupBy(
			[]sql.Expression{
				expression.NewAlias("x", expression.NewGetFieldWithTable(0, sql.Int64, "foo", "a", false)),
			},
			[]sql.Expression{
				expression.NewGetFieldWithTable(0, sql.Int64, "foo", "a", false),
			},
			plan.NewResolvedTable(table),
		),
	)

	var expected sql.Node = plan.NewGroupBy(
		[]sql.Expression{
			expression.NewAlias("x", expression.NewGetFieldWithTable(0, sql.Int64, "foo", "a", false)),
		},
		[]sql.Expression{
			expression.NewGetFieldWithTable(0, sql.Int64, "foo", "a", false),
		},
		plan.NewSort(
			[]expression.SortField{
				{Column: expression.NewUnresolvedColumn("a")},
			},
			plan.NewResolvedTable(table),
		),
	)

	result, err = rule.Apply(ctx, a, node, nil)
	require.NoError(err)

	require.Equal(expected, result)

	node = plan.NewSort(
		[]expression.SortField{
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
			plan.NewResolvedTable(table),
		),
	)

	expected = plan.NewProject(
		[]sql.Expression{
			expression.NewGetFieldWithTable(0, sql.Int64, "", "x", false),
		},
		plan.NewSort(
			[]expression.SortField{
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
				plan.NewResolvedTable(table),
			),
		),
	)

	result, err = rule.Apply(ctx, a, node, nil)
	require.NoError(err)

	require.Equal(expected, result)
}

func TestResolveOrderByLiterals(t *testing.T) {
	require := require.New(t)
	f := getRule("resolve_orderby_literals")

	table := memory.NewTable("t", sql.Schema{
		{Name: "a", Type: sql.Int64, Source: "t"},
		{Name: "b", Type: sql.Int64, Source: "t"},
	})

	node := plan.NewSort(
		[]expression.SortField{
			{Column: expression.NewLiteral(int64(2), sql.Int64)},
			{Column: expression.NewLiteral(int64(1), sql.Int64)},
		},
		plan.NewResolvedTable(table),
	)

	result, err := f.Apply(sql.NewEmptyContext(), NewDefault(nil), node, nil)
	require.NoError(err)

	require.Equal(
		plan.NewSort(
			[]expression.SortField{
				{Column: expression.NewUnresolvedQualifiedColumn("t", "b")},
				{Column: expression.NewUnresolvedQualifiedColumn("t", "a")},
			},
			plan.NewResolvedTable(table),
		),
		result,
	)

	node = plan.NewSort(
		[]expression.SortField{
			{Column: expression.NewLiteral(int64(3), sql.Int64)},
			{Column: expression.NewLiteral(int64(1), sql.Int64)},
		},
		plan.NewResolvedTable(table),
	)

	_, err = f.Apply(sql.NewEmptyContext(), NewDefault(nil), node, nil)
	require.Error(err)
	require.True(ErrOrderByColumnIndex.Is(err))
}
