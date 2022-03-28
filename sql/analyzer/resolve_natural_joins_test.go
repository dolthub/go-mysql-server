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

func TestResolveNaturalJoins(t *testing.T) {
	require := require.New(t)

	left := memory.NewTable("t1", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "a", Type: sql.Int64, Source: "t1"},
		{Name: "b", Type: sql.Int64, Source: "t1"},
		{Name: "c", Type: sql.Int64, Source: "t1"},
	}), nil)

	right := memory.NewTable("t2", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "d", Type: sql.Int64, Source: "t2"},
		{Name: "c", Type: sql.Int64, Source: "t2"},
		{Name: "b", Type: sql.Int64, Source: "t2"},
		{Name: "e", Type: sql.Int64, Source: "t2"},
	}), nil)

	node := plan.NewNaturalJoin(
		plan.NewResolvedTable(left, nil, nil),
		plan.NewResolvedTable(right, nil, nil),
	)
	rule := getRule("resolve_natural_joins")

	result, err := rule.Apply(sql.NewEmptyContext(), NewDefault(nil), node, nil)
	require.NoError(err)

	expected := plan.NewProject(
		[]sql.Expression{
			expression.NewGetFieldWithTable(1, sql.Int64, "t1", "b", false),
			expression.NewGetFieldWithTable(2, sql.Int64, "t1", "c", false),
			expression.NewGetFieldWithTable(0, sql.Int64, "t1", "a", false),
			expression.NewGetFieldWithTable(3, sql.Int64, "t2", "d", false),
			expression.NewGetFieldWithTable(6, sql.Int64, "t2", "e", false),
		},
		plan.NewInnerJoin(
			plan.NewResolvedTable(left, nil, nil),
			plan.NewResolvedTable(right, nil, nil),
			expression.JoinAnd(
				expression.NewEquals(
					expression.NewGetFieldWithTable(1, sql.Int64, "t1", "b", false),
					expression.NewGetFieldWithTable(5, sql.Int64, "t2", "b", false),
				),
				expression.NewEquals(
					expression.NewGetFieldWithTable(2, sql.Int64, "t1", "c", false),
					expression.NewGetFieldWithTable(4, sql.Int64, "t2", "c", false),
				),
			),
		),
	)

	require.Equal(expected, result)
}

func TestResolveNaturalJoinsColumns(t *testing.T) {
	rule := getRule("resolve_natural_joins")
	require := require.New(t)

	left := memory.NewTable("t1", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "a", Type: sql.Int64, Source: "t1"},
		{Name: "b", Type: sql.Int64, Source: "t1"},
		{Name: "c", Type: sql.Int64, Source: "t1"},
	}), nil)

	right := memory.NewTable("t2", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "d", Type: sql.Int64, Source: "t2"},
		{Name: "c", Type: sql.Int64, Source: "t2"},
		{Name: "b", Type: sql.Int64, Source: "t2"},
		{Name: "e", Type: sql.Int64, Source: "t2"},
	}), nil)

	node := plan.NewProject(
		[]sql.Expression{
			expression.NewUnresolvedQualifiedColumn("t2", "b"),
		},
		plan.NewNaturalJoin(
			plan.NewResolvedTable(left, nil, nil),
			plan.NewResolvedTable(right, nil, nil),
		),
	)

	result, err := rule.Apply(sql.NewEmptyContext(), NewDefault(nil), node, nil)
	require.NoError(err)

	expected := plan.NewProject(
		[]sql.Expression{
			expression.NewUnresolvedQualifiedColumn("t1", "b"),
		},
		plan.NewProject(
			[]sql.Expression{
				expression.NewGetFieldWithTable(1, sql.Int64, "t1", "b", false),
				expression.NewGetFieldWithTable(2, sql.Int64, "t1", "c", false),
				expression.NewGetFieldWithTable(0, sql.Int64, "t1", "a", false),
				expression.NewGetFieldWithTable(3, sql.Int64, "t2", "d", false),
				expression.NewGetFieldWithTable(6, sql.Int64, "t2", "e", false),
			},
			plan.NewInnerJoin(
				plan.NewResolvedTable(left, nil, nil),
				plan.NewResolvedTable(right, nil, nil),
				expression.JoinAnd(
					expression.NewEquals(
						expression.NewGetFieldWithTable(1, sql.Int64, "t1", "b", false),
						expression.NewGetFieldWithTable(5, sql.Int64, "t2", "b", false),
					),
					expression.NewEquals(
						expression.NewGetFieldWithTable(2, sql.Int64, "t1", "c", false),
						expression.NewGetFieldWithTable(4, sql.Int64, "t2", "c", false),
					),
				),
			),
		),
	)

	require.Equal(expected, result)
}

func TestResolveNaturalJoinsTableAlias(t *testing.T) {
	rule := getRule("resolve_natural_joins")
	require := require.New(t)

	left := memory.NewTable("t1", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "a", Type: sql.Int64, Source: "t1"},
		{Name: "b", Type: sql.Int64, Source: "t1"},
		{Name: "c", Type: sql.Int64, Source: "t1"},
	}), nil)

	right := memory.NewTable("t2", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "d", Type: sql.Int64, Source: "t2"},
		{Name: "c", Type: sql.Int64, Source: "t2"},
		{Name: "b", Type: sql.Int64, Source: "t2"},
		{Name: "e", Type: sql.Int64, Source: "t2"},
	}), nil)

	node := plan.NewProject(
		[]sql.Expression{
			expression.NewUnresolvedQualifiedColumn("t2-alias", "b"),
			expression.NewUnresolvedQualifiedColumn("t2-alias", "c"),
		},
		plan.NewNaturalJoin(
			plan.NewResolvedTable(left, nil, nil),
			plan.NewTableAlias("t2-alias", plan.NewResolvedTable(right, nil, nil)),
		),
	)

	result, err := rule.Apply(sql.NewEmptyContext(), NewDefault(nil), node, nil)
	require.NoError(err)

	expected := plan.NewProject(
		[]sql.Expression{
			expression.NewUnresolvedQualifiedColumn("t1", "b"),
			expression.NewUnresolvedQualifiedColumn("t1", "c"),
		},
		plan.NewProject(
			[]sql.Expression{
				expression.NewGetFieldWithTable(1, sql.Int64, "t1", "b", false),
				expression.NewGetFieldWithTable(2, sql.Int64, "t1", "c", false),
				expression.NewGetFieldWithTable(0, sql.Int64, "t1", "a", false),
				expression.NewGetFieldWithTable(3, sql.Int64, "t2-alias", "d", false),
				expression.NewGetFieldWithTable(6, sql.Int64, "t2-alias", "e", false),
			},
			plan.NewInnerJoin(
				plan.NewResolvedTable(left, nil, nil),
				plan.NewTableAlias("t2-alias", plan.NewResolvedTable(right, nil, nil)),
				expression.JoinAnd(
					expression.NewEquals(
						expression.NewGetFieldWithTable(1, sql.Int64, "t1", "b", false),
						expression.NewGetFieldWithTable(5, sql.Int64, "t2-alias", "b", false),
					),
					expression.NewEquals(
						expression.NewGetFieldWithTable(2, sql.Int64, "t1", "c", false),
						expression.NewGetFieldWithTable(4, sql.Int64, "t2-alias", "c", false),
					),
				),
			),
		),
	)

	require.Equal(expected, result)
}

func TestResolveNaturalJoinsChained(t *testing.T) {
	rule := getRule("resolve_natural_joins")
	require := require.New(t)

	left := memory.NewTable("t1", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "a", Type: sql.Int64, Source: "t1"},
		{Name: "b", Type: sql.Int64, Source: "t1"},
		{Name: "c", Type: sql.Int64, Source: "t1"},
		{Name: "f", Type: sql.Int64, Source: "t1"},
	}), nil)

	right := memory.NewTable("t2", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "d", Type: sql.Int64, Source: "t2"},
		{Name: "c", Type: sql.Int64, Source: "t2"},
		{Name: "b", Type: sql.Int64, Source: "t2"},
		{Name: "e", Type: sql.Int64, Source: "t2"},
	}), nil)

	upperRight := memory.NewTable("t3", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "a", Type: sql.Int64, Source: "t3"},
		{Name: "b", Type: sql.Int64, Source: "t3"},
		{Name: "f", Type: sql.Int64, Source: "t3"},
		{Name: "g", Type: sql.Int64, Source: "t3"},
	}), nil)

	node := plan.NewProject(
		[]sql.Expression{
			expression.NewUnresolvedQualifiedColumn("t2-alias", "b"),
			expression.NewUnresolvedQualifiedColumn("t2-alias", "c"),
			expression.NewUnresolvedQualifiedColumn("t3-alias", "f"),
		},
		plan.NewNaturalJoin(
			plan.NewNaturalJoin(
				plan.NewResolvedTable(left, nil, nil),
				plan.NewTableAlias("t2-alias", plan.NewResolvedTable(right, nil, nil)),
			),
			plan.NewTableAlias("t3-alias", plan.NewResolvedTable(upperRight, nil, nil)),
		),
	)

	result, err := rule.Apply(sql.NewEmptyContext(), NewDefault(nil), node, nil)
	require.NoError(err)

	expected := plan.NewProject(
		[]sql.Expression{
			expression.NewUnresolvedQualifiedColumn("t1", "b"),
			expression.NewUnresolvedQualifiedColumn("t1", "c"),
			expression.NewUnresolvedQualifiedColumn("t1", "f"),
		},
		plan.NewProject(
			[]sql.Expression{
				expression.NewGetFieldWithTable(0, sql.Int64, "t1", "b", false),
				expression.NewGetFieldWithTable(2, sql.Int64, "t1", "a", false),
				expression.NewGetFieldWithTable(3, sql.Int64, "t1", "f", false),
				expression.NewGetFieldWithTable(1, sql.Int64, "t1", "c", false),
				expression.NewGetFieldWithTable(4, sql.Int64, "t2-alias", "d", false),
				expression.NewGetFieldWithTable(5, sql.Int64, "t2-alias", "e", false),
				expression.NewGetFieldWithTable(9, sql.Int64, "t3-alias", "g", false),
			},
			plan.NewInnerJoin(
				plan.NewProject(
					[]sql.Expression{
						expression.NewGetFieldWithTable(1, sql.Int64, "t1", "b", false),
						expression.NewGetFieldWithTable(2, sql.Int64, "t1", "c", false),
						expression.NewGetFieldWithTable(0, sql.Int64, "t1", "a", false),
						expression.NewGetFieldWithTable(3, sql.Int64, "t1", "f", false),
						expression.NewGetFieldWithTable(4, sql.Int64, "t2-alias", "d", false),
						expression.NewGetFieldWithTable(7, sql.Int64, "t2-alias", "e", false),
					},
					plan.NewInnerJoin(
						plan.NewResolvedTable(left, nil, nil),
						plan.NewTableAlias("t2-alias", plan.NewResolvedTable(right, nil, nil)),
						expression.JoinAnd(
							expression.NewEquals(
								expression.NewGetFieldWithTable(1, sql.Int64, "t1", "b", false),
								expression.NewGetFieldWithTable(6, sql.Int64, "t2-alias", "b", false),
							),
							expression.NewEquals(
								expression.NewGetFieldWithTable(2, sql.Int64, "t1", "c", false),
								expression.NewGetFieldWithTable(5, sql.Int64, "t2-alias", "c", false),
							),
						),
					),
				),
				plan.NewTableAlias("t3-alias", plan.NewResolvedTable(upperRight, nil, nil)),
				expression.JoinAnd(
					expression.NewEquals(
						expression.NewGetFieldWithTable(0, sql.Int64, "t1", "b", false),
						expression.NewGetFieldWithTable(7, sql.Int64, "t3-alias", "b", false),
					),
					expression.NewEquals(
						expression.NewGetFieldWithTable(2, sql.Int64, "t1", "a", false),
						expression.NewGetFieldWithTable(6, sql.Int64, "t3-alias", "a", false),
					),
					expression.NewEquals(
						expression.NewGetFieldWithTable(3, sql.Int64, "t1", "f", false),
						expression.NewGetFieldWithTable(8, sql.Int64, "t3-alias", "f", false),
					),
				),
			),
		),
	)

	require.Equal(expected, result)
}

func TestResolveNaturalJoinsEqual(t *testing.T) {
	require := require.New(t)

	left := memory.NewTable("t1", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "a", Type: sql.Int64, Source: "t1"},
		{Name: "b", Type: sql.Int64, Source: "t1"},
		{Name: "c", Type: sql.Int64, Source: "t1"},
	}), nil)

	right := memory.NewTable("t2", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "a", Type: sql.Int64, Source: "t2"},
		{Name: "b", Type: sql.Int64, Source: "t2"},
		{Name: "c", Type: sql.Int64, Source: "t2"},
	}), nil)

	node := plan.NewNaturalJoin(
		plan.NewResolvedTable(left, nil, nil),
		plan.NewResolvedTable(right, nil, nil),
	)
	rule := getRule("resolve_natural_joins")

	result, err := rule.Apply(sql.NewEmptyContext(), NewDefault(nil), node, nil)
	require.NoError(err)

	expected := plan.NewProject(
		[]sql.Expression{
			expression.NewGetFieldWithTable(0, sql.Int64, "t1", "a", false),
			expression.NewGetFieldWithTable(1, sql.Int64, "t1", "b", false),
			expression.NewGetFieldWithTable(2, sql.Int64, "t1", "c", false),
		},
		plan.NewInnerJoin(
			plan.NewResolvedTable(left, nil, nil),
			plan.NewResolvedTable(right, nil, nil),
			expression.JoinAnd(
				expression.NewEquals(
					expression.NewGetFieldWithTable(0, sql.Int64, "t1", "a", false),
					expression.NewGetFieldWithTable(3, sql.Int64, "t2", "a", false),
				),
				expression.NewEquals(
					expression.NewGetFieldWithTable(1, sql.Int64, "t1", "b", false),
					expression.NewGetFieldWithTable(4, sql.Int64, "t2", "b", false),
				),
				expression.NewEquals(
					expression.NewGetFieldWithTable(2, sql.Int64, "t1", "c", false),
					expression.NewGetFieldWithTable(5, sql.Int64, "t2", "c", false),
				),
			),
		),
	)

	require.Equal(expected, result)
}

func TestResolveNaturalJoinsDisjoint(t *testing.T) {
	require := require.New(t)

	left := memory.NewTable("t1", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "a", Type: sql.Int64, Source: "t1"},
		{Name: "b", Type: sql.Int64, Source: "t1"},
		{Name: "c", Type: sql.Int64, Source: "t1"},
	}), nil)

	right := memory.NewTable("t2", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "d", Type: sql.Int64, Source: "t2"},
		{Name: "e", Type: sql.Int64, Source: "t2"},
	}), nil)

	node := plan.NewNaturalJoin(
		plan.NewResolvedTable(left, nil, nil),
		plan.NewResolvedTable(right, nil, nil),
	)
	rule := getRule("resolve_natural_joins")

	result, err := rule.Apply(sql.NewEmptyContext(), NewDefault(nil), node, nil)
	require.NoError(err)

	expected := plan.NewCrossJoin(
		plan.NewResolvedTable(left, nil, nil),
		plan.NewResolvedTable(right, nil, nil),
	)
	require.Equal(expected, result)
}
