package analyzer

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/liquidata-inc/go-mysql-server/memory"
	"github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/liquidata-inc/go-mysql-server/sql/expression"
	"github.com/liquidata-inc/go-mysql-server/sql/expression/function/aggregation"
	"github.com/liquidata-inc/go-mysql-server/sql/plan"
)

func TestQualifyColumnsProject(t *testing.T) {
	require := require.New(t)

	table := memory.NewTable("foo", sql.Schema{
		{Name: "a", Type: sql.Text, Source: "foo"},
		{Name: "b", Type: sql.Text, Source: "foo"},
	})

	node := plan.NewProject(
		[]sql.Expression{
			uc("a"),
			uc("b"),
		},
		plan.NewProject(
			[]sql.Expression{
				uqc("foo", "a"),
			},
			plan.NewResolvedTable(table),
		),
	)

	result, err := qualifyColumns(sql.NewEmptyContext(), NewDefault(nil), node, nil)
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
			plan.NewResolvedTable(table),
		),
	)

	require.Equal(expected, result)
}

func TestMisusedAlias(t *testing.T) {
	require := require.New(t)
	f := getRule("check_aliases")

	table := memory.NewTable("mytable", sql.Schema{
		{Name: "i", Type: sql.Int32},
	})

	node := plan.NewProject(
		[]sql.Expression{
			expression.NewAlias("alias_i", uc("i")),
			uc("alias_i"),
		},
		plan.NewResolvedTable(table),
	)

	_, err := f.Apply(sql.NewEmptyContext(), nil, node, nil)
	require.EqualError(err, sql.ErrMisusedAlias.New("alias_i").Error())
}

func TestQualifyVariables(t *testing.T) {
	assert := assert.New(t)
	f := getRule("qualify_columns")

	sessionTable := memory.NewTable("@@session", sql.Schema{{Name: "autocommit", Type: sql.Int64, Source: "@@session"}})
	globalTable := memory.NewTable("@@global", sql.Schema{{Name: "max_allowed_packet", Type: sql.Int64, Source: "@@global"}})

	node := plan.NewProject(
		[]sql.Expression{
			uc("@@max_allowed_packet"),
		},
		plan.NewResolvedTable(globalTable),
	)
	col, ok := node.Projections[0].(*expression.UnresolvedColumn)
	assert.True(ok)
	assert.Truef(isSystemVariable(col), "@@max_allowed_packet is not global or session column")

	expected := plan.NewProject(
		[]sql.Expression{
			uqc("", "@@max_allowed_packet"),
		},
		plan.NewResolvedTable(globalTable),
	)

	result, err := f.Apply(sql.NewEmptyContext(), nil, node, nil)
	assert.NoError(err)
	assert.Equal(expected, result)

	node = plan.NewProject(
		[]sql.Expression{
			uc("@@autocommit"),
		},
		plan.NewResolvedTable(sessionTable),
	)
	col, ok = node.Projections[0].(*expression.UnresolvedColumn)
	assert.True(ok)
	assert.Truef(isSystemVariable(col), "@@autocommit is not global or session column")

	expected = plan.NewProject(
		[]sql.Expression{
			uqc("", "@@autocommit"),
		},
		plan.NewResolvedTable(sessionTable),
	)

	result, err = f.Apply(sql.NewEmptyContext(), nil, node, nil)
	assert.NoError(err)
	assert.Equal(expected, result)
}

func TestQualifyColumns(t *testing.T) {
	f := getRule("qualify_columns")
	table := memory.NewTable("mytable", sql.Schema{
		{Name: "i", Type: sql.Int32, Source: "mytable"},
		{Name: "x", Type: sql.Int32, Source: "mytable"},
	})
	table2 := memory.NewTable("mytable2", sql.Schema{
		{Name: "i", Type: sql.Int32, Source: "mytable2"},
		{Name: "y", Type: sql.Int32, Source: "mytable2"},
	})

	testCases := []analyzerFnTestCase{
		{
			name: "simple",
			node: plan.NewProject(
				[]sql.Expression{
					uc("i"),
				},
				plan.NewResolvedTable(table),
			),
			expected: plan.NewProject(
				[]sql.Expression{
					uqc("mytable", "i"),
				},
				plan.NewResolvedTable(table),
			),
		},
		{
			name: "already qualified",
			node: plan.NewProject(
				[]sql.Expression{
					uqc("mytable", "i"),
				},
				plan.NewResolvedTable(table),
			),
		},
		{
			name: "already qualified with alias",
			node: plan.NewProject(
				[]sql.Expression{
					uqc("a", "i"),
				},
				plan.NewTableAlias("a", plan.NewResolvedTable(table)),
			),
		},
		{
			name: "unknown column",
			node: plan.NewProject(
				[]sql.Expression{
					uc("z"),
				},
				plan.NewTableAlias("a", plan.NewResolvedTable(table)),
			),
		},
		{
			name: "qualified with unknown table name",
			node: plan.NewProject(
				[]sql.Expression{
					uqc("foo", "i"),
				},
				plan.NewTableAlias("a", plan.NewResolvedTable(table)),
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
					plan.NewResolvedTable(table),
					plan.NewResolvedTable(table2),
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
					plan.NewTableAlias("a", plan.NewResolvedTable(table)),
					plan.NewSubqueryAlias(
						"b", "",
						plan.NewProject(
							[]sql.Expression{
								expression.NewGetFieldWithTable(0, sql.Int64, "mytable", "i", false),
							},
							plan.NewResolvedTable(table),
						),
					),
				),
			),
			expected: plan.NewProject(
				[]sql.Expression{
					uqc("a", "i"),
				},
				plan.NewCrossJoin(
					plan.NewTableAlias("a", plan.NewResolvedTable(table)),
					plan.NewSubqueryAlias(
						"b", "",
						plan.NewProject(
							[]sql.Expression{
								expression.NewGetFieldWithTable(0, sql.Int64, "mytable", "i", false),
							},
							plan.NewResolvedTable(table),
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
								plan.NewResolvedTable(table2),
							),
						),
						""),
				},
				plan.NewResolvedTable(table),
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
								plan.NewResolvedTable(table2),
							),
						),
						""),
				},
				plan.NewResolvedTable(table),
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
								plan.NewResolvedTable(table2),
							),
						),
						"select y from mytable2 where x > i"),
				},
				plan.NewResolvedTable(table),
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
					plan.NewResolvedTable(table2),
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
					plan.NewResolvedTable(table2),
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
								plan.NewResolvedTable(table2),
							),
						),
						"select y from mytable2 where x > i"),
				},
				plan.NewResolvedTable(table),
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
					plan.NewResolvedTable(table2),
				),
			),
		},
	}

	runTestCases(t, nil, testCases, nil, f)
}

func TestQualifyColumnsQualifiedStar(t *testing.T) {
	require := require.New(t)
	f := getRule("qualify_columns")

	table := memory.NewTable("mytable", sql.Schema{{Name: "i", Type: sql.Int32}})

	node := plan.NewProject(
		[]sql.Expression{
			expression.NewUnresolvedFunction(
				"count",
				true,
				expression.NewQualifiedStar("mytable"),
			),
		},
		plan.NewResolvedTable(table),
	)

	expected := plan.NewProject(
		[]sql.Expression{
			expression.NewUnresolvedFunction(
				"count",
				true,
				expression.NewStar(),
			),
		},
		plan.NewResolvedTable(table),
	)

	result, err := f.Apply(sql.NewEmptyContext(), nil, node, nil)
	require.NoError(err)
	require.Equal(expected, result)
}

func TestResolveColumns(t *testing.T) {
	f := getRule("resolve_columns")
	t1 := memory.NewTable("t1", sql.Schema{
		{Name: "i", Type: sql.Int64, Source: "t1"},
		{Name: "x", Type: sql.Int64, Source: "t1"},
	})
	t2 := memory.NewTable("t2", sql.Schema{
		{Name: "i", Type: sql.Int64, Source: "t2"},
		{Name: "y", Type: sql.Int64, Source: "t2"},
	})

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
					plan.NewResolvedTable(t2),
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
					plan.NewResolvedTable(t2),
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
						plan.NewResolvedTable(t1),
						plan.NewResolvedTable(t2),
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
						plan.NewResolvedTable(t1),
						plan.NewResolvedTable(t2),
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
					plan.NewResolvedTable(t2),
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
					plan.NewResolvedTable(t2),
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
					plan.NewResolvedTable(t2),
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
					plan.NewResolvedTable(t2),
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
					plan.NewResolvedTable(t2),
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
								plan.NewResolvedTable(t2),
							),
						),
						"select y from t2 where x > i"),
				},
				plan.NewResolvedTable(t1),
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
					plan.NewResolvedTable(t2),
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
					plan.NewResolvedTable(t2),
				),
			),
		},
	}

	runTestCases(t, nil, testCases, nil, f)
}

func TestResolveColumnsSession(t *testing.T) {
	require := require.New(t)

	ctx := sql.NewContext(context.Background(), sql.WithSession(sql.NewBaseSession()))
	err := ctx.Set(ctx, "foo_bar", sql.Int64, int64(42))
	require.NoError(err)
	err = ctx.Set(ctx, "autocommit", sql.Boolean, true)
	require.NoError(err)

	node := plan.NewProject(
		[]sql.Expression{
			uc("@@foo_bar"),
			uc("@@bar_baz"),
			uc("@@autocommit"),
			uc("@myvar"),
		},
		plan.NewResolvedTable(dualTable),
	)

	result, err := resolveColumns(ctx, NewDefault(nil), node, nil)
	require.NoError(err)

	expected := plan.NewProject(
		[]sql.Expression{
			expression.NewSystemVar("foo_bar", sql.Int64),
			expression.NewSystemVar("bar_baz", sql.Null),
			expression.NewSystemVar("autocommit", sql.Boolean),
			expression.NewUserVar("myvar"),
		},
		plan.NewResolvedTable(dualTable),
	)

	require.Equal(expected, result)
}

func TestPushdownGroupByAliases(t *testing.T) {
	require := require.New(t)

	a := NewDefault(nil)
	node := plan.NewGroupBy(
		[]sql.Expression{
			expression.NewAlias("c", expression.NewUnresolvedFunction("foo", true,
				uc("c"),
			)),
			expression.NewAlias("b", uc("d")),
			expression.NewUnresolvedFunction("bar", false,
				uc("b"),
			),
		},
		[]sql.Expression{
			uc("a"),
			uc("b"),
		},
		plan.NewResolvedTable(memory.NewTable("table", nil)),
	)

	expected := plan.NewGroupBy(
		[]sql.Expression{
			expression.NewAlias("c", expression.NewUnresolvedFunction("foo", true,
				uc("c"),
			)),
			uc("b"),
			expression.NewUnresolvedFunction("bar", false,
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
			plan.NewResolvedTable(memory.NewTable("table", nil)),
		),
	)

	result, err := pushdownGroupByAliases(sql.NewEmptyContext(), a, node, nil)
	require.NoError(err)

	require.Equal(expected, result)
}
