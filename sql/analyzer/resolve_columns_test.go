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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/expression/function/aggregation"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

func TestQualifyColumnsProject(t *testing.T) {
	require := require.New(t)

	table := memory.NewTable("foo", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "a", Type: sql.Text, Source: "foo"},
		{Name: "b", Type: sql.Text, Source: "foo"},
	}), nil)

	node := plan.NewProject(
		[]sql.Expression{
			uc("a"),
			uc("b"),
		},
		plan.NewProject(
			[]sql.Expression{
				uqc("foo", "a"),
			},
			plan.NewResolvedTable(table, nil, nil),
		),
	)

	result, _, err := qualifyColumns(sql.NewEmptyContext(), NewDefault(nil), node, nil, DefaultRuleSelector)
	require.NoError(err)

	expected := plan.NewProject(
		[]sql.Expression{
			uqc("foo", "a"),
			// b is not qualified because it's not projected
			uc("b"),
		},
		plan.NewProject(
			[]sql.Expression{
				uqc("foo", "a"),
			},
			plan.NewResolvedTable(table, nil, nil),
		),
	)

	require.Equal(expected, result)
}

func TestMisusedAlias(t *testing.T) {
	require := require.New(t)
	f := getRule(resolveColumnsId)

	table := memory.NewTable("mytable", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "i", Type: sql.Int32},
	}), nil)

	node := plan.NewProject(
		[]sql.Expression{
			expression.NewAlias("alias_i", uc("i")),
			// like most missing column error cases, this error takes 2 passes to manifest and gets deferred on the first pass
			&deferredColumn{uc("alias_i")},
		},
		plan.NewResolvedTable(table, nil, nil),
	)

	_, _, err := f.Apply(sql.NewEmptyContext(), nil, node, nil, DefaultRuleSelector)
	require.EqualError(err, sql.ErrMisusedAlias.New("alias_i").Error())
}

func TestQualifyVariables(t *testing.T) {
	assert := assert.New(t)
	f := getRule(qualifyColumnsId)

	sessionTable := memory.NewTable("@@session", sql.NewPrimaryKeySchema(sql.Schema{{Name: "autocommit", Type: sql.Int64, Source: "@@session"}}), nil)
	globalTable := memory.NewTable("@@global", sql.NewPrimaryKeySchema(sql.Schema{{Name: "max_allowed_packet", Type: sql.Int64, Source: "@@global"}}), nil)

	node := plan.NewProject(
		[]sql.Expression{
			uc("@@max_allowed_packet"),
		},
		plan.NewResolvedTable(globalTable, nil, nil),
	)
	col, ok := node.Projections[0].(*expression.UnresolvedColumn)
	assert.True(ok)
	assert.Truef(strings.HasPrefix(col.Name(), "@@") || strings.HasPrefix(col.Table(), "@@"),
		"@@max_allowed_packet is not global or session column")

	expected := plan.NewProject(
		[]sql.Expression{
			uqc("", "@@max_allowed_packet"),
		},
		plan.NewResolvedTable(globalTable, nil, nil),
	)

	result, _, err := f.Apply(sql.NewEmptyContext(), nil, node, nil, DefaultRuleSelector)

	assert.NoError(err)
	assert.Equal(expected, result)

	node = plan.NewProject(
		[]sql.Expression{
			uc("@@autocommit"),
		},
		plan.NewResolvedTable(sessionTable, nil, nil),
	)
	col, ok = node.Projections[0].(*expression.UnresolvedColumn)
	assert.True(ok)
	assert.Truef(strings.HasPrefix(col.Name(), "@@") || strings.HasPrefix(col.Table(), "@@"),
		"@@autocommit is not global or session column")

	expected = plan.NewProject(
		[]sql.Expression{
			uqc("", "@@autocommit"),
		},
		plan.NewResolvedTable(sessionTable, nil, nil),
	)

	result, _, err = f.Apply(sql.NewEmptyContext(), nil, node, nil, DefaultRuleSelector)
	assert.NoError(err)
	assert.Equal(expected, result)
}

func TestQualifyColumns(t *testing.T) {
	f := getRule(qualifyColumnsId)
	table := memory.NewTable("mytable", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "i", Type: sql.Int32, Source: "mytable"},
		{Name: "x", Type: sql.Int32, Source: "mytable"},
	}), nil)
	table2 := memory.NewTable("mytable2", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "i", Type: sql.Int32, Source: "mytable2"},
		{Name: "y", Type: sql.Int32, Source: "mytable2"},
	}), nil)

	testCases := []analyzerFnTestCase{
		{
			name: "simple",
			node: plan.NewProject(
				[]sql.Expression{
					uc("i"),
				},
				plan.NewResolvedTable(table, nil, nil),
			),
			expected: plan.NewProject(
				[]sql.Expression{
					uqc("mytable", "i"),
				},
				plan.NewResolvedTable(table, nil, nil),
			),
		},
		{
			name: "already qualified",
			node: plan.NewProject(
				[]sql.Expression{
					uqc("mytable", "i"),
				},
				plan.NewResolvedTable(table, nil, nil),
			),
		},
		{
			name: "already qualified with alias",
			node: plan.NewProject(
				[]sql.Expression{
					uqc("a", "i"),
				},
				plan.NewTableAlias("a", plan.NewResolvedTable(table, nil, nil)),
			),
		},
		{
			name: "unknown column",
			node: plan.NewProject(
				[]sql.Expression{
					uc("z"),
				},
				plan.NewTableAlias("a", plan.NewResolvedTable(table, nil, nil)),
			),
		},
		{
			name: "qualified with unknown table name",
			node: plan.NewProject(
				[]sql.Expression{
					uqc("foo", "i"),
				},
				plan.NewTableAlias("a", plan.NewResolvedTable(table, nil, nil)),
			),
			err: sql.ErrTableNotFound,
		},
		{
			name: "ambiguous column name",
			node: plan.NewProject(
				[]sql.Expression{
					uc("i"),
				},
				plan.NewCrossJoin(
					plan.NewResolvedTable(table, nil, nil),
					plan.NewResolvedTable(table2, nil, nil),
				),
			),
			err: sql.ErrAmbiguousColumnName,
		},
		{
			name: "subquery, all columns already qualified",
			node: plan.NewProject(
				[]sql.Expression{
					uqc("a", "i"),
				},
				plan.NewCrossJoin(
					plan.NewTableAlias("a", plan.NewResolvedTable(table, nil, nil)),
					plan.NewSubqueryAlias(
						"b", "",
						plan.NewProject(
							[]sql.Expression{
								expression.NewGetFieldWithTable(0, sql.Int64, "mytable", "i", false),
							},
							plan.NewResolvedTable(table, nil, nil),
						),
					),
				),
			),
			expected: plan.NewProject(
				[]sql.Expression{
					uqc("a", "i"),
				},
				plan.NewCrossJoin(
					plan.NewTableAlias("a", plan.NewResolvedTable(table, nil, nil)),
					plan.NewSubqueryAlias(
						"b", "",
						plan.NewProject(
							[]sql.Expression{
								expression.NewGetFieldWithTable(0, sql.Int64, "mytable", "i", false),
							},
							plan.NewResolvedTable(table, nil, nil),
						),
					),
				),
			),
		},
		{
			name: "subquery expression, columns not qualified",
			node: plan.NewProject(
				[]sql.Expression{
					uc("i"),
					plan.NewSubquery(
						plan.NewFilter(
							gt(
								uc("x"),
								uc("i"),
							),
							plan.NewProject(
								[]sql.Expression{
									aggregation.NewMax(uc("y")),
								},
								plan.NewResolvedTable(table2, nil, nil),
							),
						),
						""),
				},
				plan.NewResolvedTable(table, nil, nil),
			),
			expected: plan.NewProject(
				[]sql.Expression{
					uqc("mytable", "i"),
					plan.NewSubquery(
						plan.NewFilter(
							gt(
								uc("x"),
								uc("i"),
							),
							plan.NewProject(
								[]sql.Expression{
									aggregation.NewMax(uc("y")),
								},
								plan.NewResolvedTable(table2, nil, nil),
							),
						),
						""),
				},
				plan.NewResolvedTable(table, nil, nil),
			),
		},
		{
			name: "qualify in subquery expression",
			scope: newScope(plan.NewProject(
				[]sql.Expression{
					uc("i"),
					plan.NewSubquery(
						plan.NewProject(
							[]sql.Expression{
								aggregation.NewMax(uc("y")),
							},
							plan.NewFilter(
								gt(
									uc("x"),
									uc("i"),
								),
								plan.NewResolvedTable(table2, nil, nil),
							),
						),
						"select y from mytable2 where x > i"),
				},
				plan.NewResolvedTable(table, nil, nil),
			)),
			node: plan.NewProject(
				[]sql.Expression{
					aggregation.NewMax(uc("y")),
				},
				plan.NewFilter(
					gt(
						uc("x"),
						uc("i"),
					),
					plan.NewResolvedTable(table2, nil, nil),
				),
			),
			expected: plan.NewProject(
				[]sql.Expression{
					aggregation.NewMax(uqc("mytable2", "y")),
				},
				plan.NewFilter(
					gt(
						uqc("mytable", "x"),
						uqc("mytable2", "i"),
					),
					plan.NewResolvedTable(table2, nil, nil),
				),
			),
		},
		{
			name: "qualify in subquery expression, already qualified",
			scope: newScope(plan.NewProject(
				[]sql.Expression{
					uc("i"),
					plan.NewSubquery(
						plan.NewProject(
							[]sql.Expression{
								aggregation.NewMax(uqc("mytable2", "y")),
							},
							plan.NewFilter(
								gt(
									uqc("mytable", "x"),
									uqc("mytable2", "i"),
								),
								plan.NewResolvedTable(table2, nil, nil),
							),
						),
						"select y from mytable2 where x > i"),
				},
				plan.NewResolvedTable(table, nil, nil),
			)),
			node: plan.NewProject(
				[]sql.Expression{
					aggregation.NewMax(uqc("mytable2", "y")),
				},
				plan.NewFilter(
					gt(
						uqc("mytable", "x"),
						uqc("mytable2", "i"),
					),
					plan.NewResolvedTable(table2, nil, nil),
				),
			),
		},
	}

	runTestCases(t, nil, testCases, nil, f)
}

func TestQualifyColumnsQualifiedStar(t *testing.T) {
	require := require.New(t)
	f := getRule(qualifyColumnsId)

	table := memory.NewTable("mytable", sql.NewPrimaryKeySchema(sql.Schema{{Name: "i", Type: sql.Int32}}), nil)

	node := plan.NewProject(
		[]sql.Expression{
			expression.NewUnresolvedFunction(
				"count",
				true,
				nil,
				expression.NewQualifiedStar("mytable"),
			),
		},
		plan.NewResolvedTable(table, nil, nil),
	)

	expected := plan.NewProject(
		[]sql.Expression{
			expression.NewUnresolvedFunction(
				"count",
				true,
				nil,
				expression.NewStar(),
			),
		},
		plan.NewResolvedTable(table, nil, nil),
	)

	result, _, err := f.Apply(sql.NewEmptyContext(), nil, node, nil, DefaultRuleSelector)
	require.NoError(err)
	require.Equal(expected, result)
}

func TestResolveColumns(t *testing.T) {
	f := getRule(resolveColumnsId)
	t1 := memory.NewTable("t1", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "i", Type: sql.Int64, Source: "t1"},
		{Name: "x", Type: sql.Int64, Source: "t1"},
	}), nil)
	t2 := memory.NewTable("t2", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "i", Type: sql.Int64, Source: "t2"},
		{Name: "y", Type: sql.Int64, Source: "t2"},
	}), nil)

	testCases := []analyzerFnTestCase{
		{
			name: "Project with filter, one table",
			node: plan.NewProject(
				[]sql.Expression{
					uqc("t2", "y"),
				},
				plan.NewFilter(
					gt(
						uqc("t2", "y"),
						uqc("t2", "i"),
					),
					plan.NewResolvedTable(t2, nil, nil),
				),
			),
			expected: plan.NewProject(
				[]sql.Expression{
					gf(1, "t2", "y"),
				},
				plan.NewFilter(
					gt(
						gf(1, "t2", "y"),
						gf(0, "t2", "i"),
					),
					plan.NewResolvedTable(t2, nil, nil),
				),
			),
		},
		{
			name: "Project with filter, two tables",
			node: plan.NewProject(
				[]sql.Expression{
					uqc("t2", "y"),
					uqc("t1", "i"),
				},
				plan.NewFilter(
					gt(
						uqc("t1", "x"),
						uqc("t2", "i"),
					),
					plan.NewCrossJoin(
						plan.NewResolvedTable(t1, nil, nil),
						plan.NewResolvedTable(t2, nil, nil),
					),
				),
			),
			expected: plan.NewProject(
				[]sql.Expression{
					gf(3, "t2", "y"),
					gf(0, "t1", "i"),
				},
				plan.NewFilter(
					gt(
						gf(1, "t1", "x"),
						gf(2, "t2", "i"),
					),
					plan.NewCrossJoin(
						plan.NewResolvedTable(t1, nil, nil),
						plan.NewResolvedTable(t2, nil, nil),
					),
				),
			),
		},
		{
			name: "Unfound columns deferred",
			node: plan.NewProject(
				[]sql.Expression{
					uqc("t2", "x"),
				},
				plan.NewFilter(
					gt(
						uqc("t2", "y"),
						uqc("t2", "i"),
					),
					plan.NewResolvedTable(t2, nil, nil),
				),
			),
			expected: plan.NewProject(
				[]sql.Expression{
					&deferredColumn{uqc("t2", "x")},
				},
				plan.NewFilter(
					gt(
						gf(1, "t2", "y"),
						gf(0, "t2", "i"),
					),
					plan.NewResolvedTable(t2, nil, nil),
				),
			),
		},
		{
			name: "Deferred columns resolved",
			node: plan.NewProject(
				[]sql.Expression{
					&deferredColumn{uqc("t2", "y")},
				},
				plan.NewFilter(
					gt(
						gf(1, "t2", "y"),
						gf(0, "t2", "i"),
					),
					plan.NewResolvedTable(t2, nil, nil),
				),
			),
			expected: plan.NewProject(
				[]sql.Expression{
					gf(1, "t2", "y"),
				},
				plan.NewFilter(
					gt(
						gf(1, "t2", "y"),
						gf(0, "t2", "i"),
					),
					plan.NewResolvedTable(t2, nil, nil),
				),
			),
		},
		{
			name: "Deferred columns still not found throw error",
			node: plan.NewProject(
				[]sql.Expression{
					&deferredColumn{uqc("t2", "x")},
				},
				plan.NewFilter(
					gt(
						gf(1, "t2", "y"),
						gf(0, "t2", "i"),
					),
					plan.NewResolvedTable(t2, nil, nil),
				),
			),
			err: sql.ErrTableColumnNotFound,
		},
		{
			name: "resolve deferred columns in subquery expressions",
			scope: newScope(plan.NewProject(
				[]sql.Expression{
					uc("i"),
					plan.NewSubquery(
						plan.NewProject(
							[]sql.Expression{
								aggregation.NewMax(gf(3, "t2", "y")),
							},
							plan.NewFilter(
								gt(
									&deferredColumn{uqc("t1", "x")},
									gf(2, "t2", "i"),
								),
								plan.NewResolvedTable(t2, nil, nil),
							),
						),
						"select y from t2 where x > i"),
				},
				plan.NewResolvedTable(t1, nil, nil),
			)),
			node: plan.NewProject(
				[]sql.Expression{
					aggregation.NewMax(gf(3, "t2", "y")),
				},
				plan.NewFilter(
					gt(
						&deferredColumn{uqc("t1", "x")},
						gf(2, "t2", "i"),
					),
					plan.NewResolvedTable(t2, nil, nil),
				),
			),
			expected: plan.NewProject(
				[]sql.Expression{
					aggregation.NewMax(gf(3, "t2", "y")),
				},
				plan.NewFilter(
					gt(
						gf(1, "t1", "x"),
						gf(2, "t2", "i"),
					),
					plan.NewResolvedTable(t2, nil, nil),
				),
			),
		},
	}

	runTestCases(t, nil, testCases, nil, f)
}

func TestPushdownGroupByAliases(t *testing.T) {
	require := require.New(t)

	a := NewDefault(nil)
	node := plan.NewGroupBy(
		[]sql.Expression{
			expression.NewAlias("c", expression.NewUnresolvedFunction("foo", false, nil,
				uc("c"),
			)),
			expression.NewAlias("b", uc("d")),
			expression.NewUnresolvedFunction("bar", true, nil,
				uc("b"),
			),
		},
		[]sql.Expression{
			uc("a"),
			uc("b"),
		},
		plan.NewResolvedTable(memory.NewTable("table", sql.PrimaryKeySchema{}, nil), nil, nil),
	)

	expected := plan.NewGroupBy(
		[]sql.Expression{
			expression.NewAlias("c", expression.NewUnresolvedFunction("foo", false, nil,
				uc("c"),
			)),
			uc("b"),
			expression.NewUnresolvedFunction("bar", true, nil,
				uc("b_01"),
			),
		},
		[]sql.Expression{
			uc("a"),
			uc("b"),
		},
		plan.NewProject(
			[]sql.Expression{
				expression.NewAlias("b", uc("d")),
				uc("a"),
				expression.NewAlias("b_01", uc("b")),
				uc("c"),
			},
			plan.NewResolvedTable(memory.NewTable("table", sql.PrimaryKeySchema{}, nil), nil, nil),
		),
	)

	result, _, err := pushdownGroupByAliases(sql.NewEmptyContext(), a, node, nil, DefaultRuleSelector)
	require.NoError(err)

	require.Equal(expected, result)
}
