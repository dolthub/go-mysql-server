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
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestEraseProjection(t *testing.T) {
	require := require.New(t)
	f := getRule(eraseProjectionId)

	db := memory.NewDatabase("db")

	table := memory.NewTable(db, "mytable", sql.NewPrimaryKeySchema(sql.Schema{{
		Name: "i", Source: "mytable", Type: types.Int64,
	}}), nil)

	expected := plan.NewSort(
		[]sql.SortField{{Column: expression.NewGetField(2, types.Int64, "foo", false)}},
		plan.NewProject(
			[]sql.Expression{
				expression.NewGetFieldWithTable(0, types.Int64, "db", "mytable", "i", false),
				expression.NewGetField(1, types.Int64, "bar", false),
				expression.NewAlias("foo", expression.NewLiteral(1, types.Int64)),
			},
			plan.NewFilter(
				expression.NewEquals(
					expression.NewLiteral(1, types.Int64),
					expression.NewGetField(1, types.Int64, "bar", false),
				),
				plan.NewProject(
					[]sql.Expression{
						expression.NewGetFieldWithTable(0, types.Int64, "db", "mytable", "i", false),
						expression.NewAlias("bar", expression.NewLiteral(2, types.Int64)),
					},
					plan.NewResolvedTable(table, nil, nil),
				),
			),
		),
	)

	node := plan.NewProject(
		[]sql.Expression{
			expression.NewGetFieldWithTable(0, types.Int64, "db", "mytable", "i", false),
			expression.NewGetField(1, types.Int64, "bar", false),
			expression.NewGetField(2, types.Int64, "foo", false),
		},
		expected,
	)

	result, _, err := f.Apply(sql.NewEmptyContext(), NewDefault(nil), node, nil, DefaultRuleSelector)
	require.NoError(err)
	require.Equal(expected, result)

	result, _, err = f.Apply(sql.NewEmptyContext(), NewDefault(nil), expected, nil, DefaultRuleSelector)
	require.NoError(err)
	require.Equal(expected, result)
}

func TestMoveJoinConditionsToFilter(t *testing.T) {
	db := memory.NewDatabase("db")
	pro := memory.NewDBProvider(db)
	ctx := newContext(pro)

	t1 := memory.NewTable(db, "t1", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "a", Source: "t1", Type: types.Int64},
		{Name: "b", Source: "t1", Type: types.Int64},
	}), nil)

	t2 := memory.NewTable(db, "t2", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "c", Source: "t2", Type: types.Int64},
		{Name: "d", Source: "t2", Type: types.Int64},
	}), nil)

	t3 := memory.NewTable(db, "t3", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "e", Source: "t3", Type: types.Int64},
		{Name: "f", Source: "t3", Type: types.Int64},
	}), nil)

	rule := getRule(moveJoinCondsToFilterId)
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

	result, _, err := rule.Apply(ctx, NewDefault(nil), node, nil, DefaultRuleSelector)
	require.NoError(err)

	var expected sql.Node = plan.NewInnerJoin(
		plan.NewFilter(
			eq(col(0, "t1", "a"), lit(5)),
			plan.NewResolvedTable(t1, nil, nil),
		),
		plan.NewFilter(
			eq(col(2, "t2", "c"), col(4, "t3", "e")),
			plan.NewCrossJoin(
				plan.NewResolvedTable(t2, nil, nil),
				plan.NewResolvedTable(t3, nil, nil),
			),
		),
		and(
			eq(col(0, "t1", "a"), col(2, "t2", "c")),
			eq(col(0, "t1", "a"), col(4, "t3", "e")),
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

	result, _, err = rule.Apply(ctx, NewDefault(nil), node, nil, DefaultRuleSelector)
	require.NoError(err)

	expected = plan.NewInnerJoin(
		plan.NewFilter(
			eq(col(0, "t1", "a"), lit(5)),
			plan.NewResolvedTable(t1, nil, nil),
		),
		plan.NewFilter(
			eq(col(0, "t2", "c"), col(0, "t3", "e")),
			plan.NewCrossJoin(
				plan.NewResolvedTable(t2, nil, nil),
				plan.NewResolvedTable(t3, nil, nil),
			),
		),
		expression.NewLiteral(true, types.Boolean),
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

	result, _, err = rule.Apply(ctx, NewDefault(nil), node, nil, DefaultRuleSelector)
	require.NoError(err)

	expected = plan.NewInnerJoin(
		plan.NewFilter(
			eq(col(0, "t1", "a"), lit(10)),
			plan.NewResolvedTable(t1, nil, nil),
		),
		plan.NewInnerJoin(
			plan.NewResolvedTable(t2, nil, nil),
			plan.NewFilter(
				eq(col(0, "t3", "a"), lit(5)),
				plan.NewResolvedTable(t3, nil, nil),
			),
			eq(col(0, "t2", "c"), col(0, "t3", "e")),
		),
		eq(col(0, "t1", "c"), col(0, "t2", "e")),
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

	result, _, err = rule.Apply(ctx, NewDefault(nil), node, nil, DefaultRuleSelector)
	require.NoError(err)

	expected = plan.NewInnerJoin(
		plan.NewResolvedTable(t1, nil, nil),
		plan.NewInnerJoin(
			plan.NewResolvedTable(t2, nil, nil),
			plan.NewFilter(
				eq(col(0, "t3", "a"), lit(5)),
				plan.NewResolvedTable(t3, nil, nil),
			),
			expression.JoinAnd(
				eq(col(0, "t2", "c"), col(0, "t3", "e")),
			),
		),
		expression.JoinAnd(
			eq(col(0, "t1", "c"), col(0, "t2", "e")),
		),
	)

	assertNodesEqualWithDiff(t, expected, result)
}

func TestEvalFilter(t *testing.T) {
	db := memory.NewDatabase("db")
	pro := memory.NewDBProvider(db)
	ctx := newContext(pro)

	inner := memory.NewTable(db, "foo", sql.PrimaryKeySchema{}, nil)
	rule := getRule(evalFilterId)

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
			plan.NewEmptyTableWithSchema(inner.Schema()),
		},
		{
			and(
				eq(col(0, "foo", "bar"), lit(5)),
				eq(lit(5), lit(4)),
			),
			plan.NewEmptyTableWithSchema(inner.Schema()),
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
			plan.NewEmptyTableWithSchema(inner.Schema()),
		},
	}

	for _, tt := range testCases {
		t.Run(tt.filter.String(), func(t *testing.T) {
			require := require.New(t)
			node := plan.NewFilter(tt.filter, plan.NewResolvedTable(inner, nil, nil))
			result, _, err := rule.Apply(ctx, NewDefault(nil), node, nil, DefaultRuleSelector)
			require.NoError(err)
			require.Equal(tt.expected, result)
		})
	}
}
