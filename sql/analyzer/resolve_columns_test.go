package analyzer

import (
	"context"
	"github.com/liquidata-inc/go-mysql-server/sql/expression/function/aggregation"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/liquidata-inc/go-mysql-server/memory"
	"github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/liquidata-inc/go-mysql-server/sql/expression"
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
			expression.NewUnresolvedColumn("a"),
			expression.NewUnresolvedColumn("b"),
		},
		plan.NewProject(
			[]sql.Expression{
				expression.NewUnresolvedQualifiedColumn("foo", "a"),
			},
			plan.NewResolvedTable(table),
		),
	)

	result, err := qualifyColumns(sql.NewEmptyContext(), NewDefault(nil), node, nil)
	require.NoError(err)

	expected := plan.NewProject(
		[]sql.Expression{
			expression.NewUnresolvedQualifiedColumn("foo", "a"),
			// b is not qualified because it's not projected
			expression.NewUnresolvedColumn("b"),
		},
		plan.NewProject(
			[]sql.Expression{
				expression.NewUnresolvedQualifiedColumn("foo", "a"),
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
			expression.NewAlias("alias_i", expression.NewUnresolvedColumn("i")),
			expression.NewUnresolvedColumn("alias_i"),
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
			expression.NewUnresolvedColumn("@@max_allowed_packet"),
		},
		plan.NewResolvedTable(globalTable),
	)
	col, ok := node.Projections[0].(*expression.UnresolvedColumn)
	assert.True(ok)
	assert.Truef(isGlobalOrSessionColumn(col), "@@max_allowed_packet is not global or session column")

	expected := plan.NewProject(
		[]sql.Expression{
			expression.NewUnresolvedQualifiedColumn("", "@@max_allowed_packet"),
		},
		plan.NewResolvedTable(globalTable),
	)

	result, err := f.Apply(sql.NewEmptyContext(), nil, node, nil)
	assert.NoError(err)
	assert.Equal(expected, result)

	node = plan.NewProject(
		[]sql.Expression{
			expression.NewUnresolvedColumn("@@autocommit"),
		},
		plan.NewResolvedTable(sessionTable),
	)
	col, ok = node.Projections[0].(*expression.UnresolvedColumn)
	assert.True(ok)
	assert.Truef(isGlobalOrSessionColumn(col), "@@autocommit is not global or session column")

	expected = plan.NewProject(
		[]sql.Expression{
			expression.NewUnresolvedQualifiedColumn("", "@@autocommit"),
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
					expression.NewUnresolvedColumn("i"),
				},
				plan.NewResolvedTable(table),
			),
			expected: plan.NewProject(
				[]sql.Expression{
					expression.NewUnresolvedQualifiedColumn("mytable", "i"),
				},
				plan.NewResolvedTable(table),
			),
		},
		{
			name: "already qualified",
			node: plan.NewProject(
				[]sql.Expression{
					expression.NewUnresolvedQualifiedColumn("mytable", "i"),
				},
				plan.NewResolvedTable(table),
			),
		},
		{
			name: "already qualified with alias",
			node: plan.NewProject(
				[]sql.Expression{
					expression.NewUnresolvedQualifiedColumn("a", "i"),
				},
				plan.NewTableAlias("a", plan.NewResolvedTable(table)),
			),
		},
		{
			name: "unknown column",
			node: plan.NewProject(
				[]sql.Expression{
					expression.NewUnresolvedColumn("z"),
				},
				plan.NewTableAlias("a", plan.NewResolvedTable(table)),
			),
		},
		{
			name: "qualified with unknown table name",
			node: plan.NewProject(
				[]sql.Expression{
					expression.NewUnresolvedQualifiedColumn("foo", "i"),
				},
				plan.NewTableAlias("a", plan.NewResolvedTable(table)),
			),
			err: sql.ErrTableNotFound,
		},
		{
			name: "ambiguous column name",
			node: plan.NewProject(
				[]sql.Expression{
					expression.NewUnresolvedColumn("i"),
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
					expression.NewUnresolvedQualifiedColumn("a", "i"),
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
					expression.NewUnresolvedQualifiedColumn("a", "i"),
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
					expression.NewUnresolvedColumn("i"),
					plan.NewSubquery(
						plan.NewFilter(
							expression.NewGreaterThan(
								expression.NewUnresolvedColumn("x"),
								expression.NewUnresolvedColumn("i"),
							),
							plan.NewProject(
								[]sql.Expression{
									aggregation.NewMax(expression.NewUnresolvedColumn("y")),
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
					expression.NewUnresolvedQualifiedColumn("mytable", "i"),
					plan.NewSubquery(
						plan.NewFilter(
							expression.NewGreaterThan(
								expression.NewUnresolvedColumn("x"),
								expression.NewUnresolvedColumn("i"),
							),
							plan.NewProject(
								[]sql.Expression{
									aggregation.NewMax(expression.NewUnresolvedColumn("y")),
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
					expression.NewUnresolvedColumn("i"),
					plan.NewSubquery(
						plan.NewProject(
							[]sql.Expression{
								aggregation.NewMax(expression.NewUnresolvedColumn("y")),
							},
							plan.NewFilter(
								expression.NewGreaterThan(
									expression.NewUnresolvedColumn("x"),
									expression.NewUnresolvedColumn("i"),
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
					aggregation.NewMax(expression.NewUnresolvedColumn("y")),
				},
				plan.NewFilter(
					expression.NewGreaterThan(
						expression.NewUnresolvedColumn("x"),
						expression.NewUnresolvedColumn("i"),
					),
					plan.NewResolvedTable(table2),
				),
			),
			expected: plan.NewProject(
				[]sql.Expression{
					aggregation.NewMax(expression.NewUnresolvedQualifiedColumn("mytable2", "y")),
				},
				plan.NewFilter(
					expression.NewGreaterThan(
						expression.NewUnresolvedQualifiedColumn("mytable","x"),
						expression.NewUnresolvedQualifiedColumn("mytable2","i"),
					),
					plan.NewResolvedTable(table2),
				),
			),
		},
	}

	runTestCases(t, testCases, f)
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

func TestResolveColumnsSession(t *testing.T) {
	require := require.New(t)

	ctx := sql.NewContext(context.Background(), sql.WithSession(sql.NewBaseSession()))
	err := ctx.Set(ctx, "foo_bar", sql.Int64, int64(42))
	require.NoError(err)
	err = ctx.Set(ctx, "autocommit", sql.Boolean, true)
	require.NoError(err)

	node := plan.NewProject(
		[]sql.Expression{
			expression.NewUnresolvedColumn("@@foo_bar"),
			expression.NewUnresolvedColumn("@@bar_baz"),
			expression.NewUnresolvedColumn("@@autocommit"),
		},
		plan.NewResolvedTable(dualTable),
	)

	result, err := resolveColumns(ctx, NewDefault(nil), node, nil)
	require.NoError(err)

	expected := plan.NewProject(
		[]sql.Expression{
			expression.NewGetSessionField("foo_bar", sql.Int64, int64(42)),
			expression.NewGetSessionField("bar_baz", sql.Null, nil),
			expression.NewGetSessionField("autocommit", sql.Boolean, true),
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
				expression.NewUnresolvedColumn("c"),
			)),
			expression.NewAlias("b", expression.NewUnresolvedColumn("d")),
			expression.NewUnresolvedFunction("bar", false,
				expression.NewUnresolvedColumn("b"),
			),
		},
		[]sql.Expression{
			expression.NewUnresolvedColumn("a"),
			expression.NewUnresolvedColumn("b"),
		},
		plan.NewResolvedTable(memory.NewTable("table", nil)),
	)

	expected := plan.NewGroupBy(
		[]sql.Expression{
			expression.NewAlias("c", expression.NewUnresolvedFunction("foo", true,
				expression.NewUnresolvedColumn("c"),
			)),
			expression.NewUnresolvedColumn("b"),
			expression.NewUnresolvedFunction("bar", false,
				expression.NewUnresolvedColumn("b_01"),
			),
		},
		[]sql.Expression{
			expression.NewUnresolvedColumn("a"),
			expression.NewUnresolvedColumn("b"),
		},
		plan.NewProject(
			[]sql.Expression{
				expression.NewAlias("b", expression.NewUnresolvedColumn("d")),
				expression.NewUnresolvedColumn("a"),
				expression.NewAlias("b_01", expression.NewUnresolvedColumn("b")),
				expression.NewUnresolvedColumn("c"),
			},
			plan.NewResolvedTable(memory.NewTable("table", nil)),
		),
	)

	result, err := pushdownGroupByAliases(sql.NewEmptyContext(), a, node, nil)
	require.NoError(err)

	require.Equal(expected, result)
}
