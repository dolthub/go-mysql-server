package analyzer

import (
	"context"
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

	result, err := qualifyColumns(sql.NewEmptyContext(), NewDefault(nil), node)
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
			expression.NewAlias(
				expression.NewUnresolvedColumn("i"),
				"alias_i",
			),
			expression.NewUnresolvedColumn("alias_i"),
		},
		plan.NewResolvedTable(table),
	)

	_, err := f.Apply(sql.NewEmptyContext(), nil, node)
	require.EqualError(err, ErrMisusedAlias.New("alias_i").Error())
}

func TestQualifyColumns(t *testing.T) {
	assert := assert.New(t)
	f := getRule("qualify_columns")

	table := memory.NewTable("mytable", sql.Schema{{Name: "i", Type: sql.Int32, Source: "mytable"}})
	table2 := memory.NewTable("mytable2", sql.Schema{{Name: "i", Type: sql.Int32, Source: "mytable2"}})
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

	result, err := f.Apply(sql.NewEmptyContext(), nil, node)
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

	result, err = f.Apply(sql.NewEmptyContext(), nil, node)
	assert.NoError(err)
	assert.Equal(expected, result)

	node = plan.NewProject(
		[]sql.Expression{
			expression.NewUnresolvedColumn("i"),
		},
		plan.NewResolvedTable(table),
	)

	expected = plan.NewProject(
		[]sql.Expression{
			expression.NewUnresolvedQualifiedColumn("mytable", "i"),
		},
		plan.NewResolvedTable(table),
	)

	result, err = f.Apply(sql.NewEmptyContext(), nil, node)
	assert.NoError(err)
	assert.Equal(expected, result)

	node = plan.NewProject(
		[]sql.Expression{
			expression.NewUnresolvedQualifiedColumn("mytable", "i"),
		},
		plan.NewResolvedTable(table),
	)

	result, err = f.Apply(sql.NewEmptyContext(), nil, node)
	assert.NoError(err)
	assert.Equal(expected, result)

	node = plan.NewProject(
		[]sql.Expression{
			expression.NewUnresolvedQualifiedColumn("a", "i"),
		},
		plan.NewTableAlias("a", plan.NewResolvedTable(table)),
	)

	expected = plan.NewProject(
		[]sql.Expression{
			expression.NewUnresolvedQualifiedColumn("a", "i"),
		},
		plan.NewTableAlias("a", plan.NewResolvedTable(table)),
	)

	result, err = f.Apply(sql.NewEmptyContext(), nil, node)
	assert.NoError(err)
	assert.Equal(expected, result)

	node = plan.NewProject(
		[]sql.Expression{
			expression.NewUnresolvedColumn("z"),
		},
		plan.NewTableAlias("a", plan.NewResolvedTable(table)),
	)

	result, err = f.Apply(sql.NewEmptyContext(), nil, node)
	assert.NoError(err)
	assert.Equal(node, result)

	node = plan.NewProject(
		[]sql.Expression{
			expression.NewUnresolvedQualifiedColumn("foo", "i"),
		},
		plan.NewTableAlias("a", plan.NewResolvedTable(table)),
	)

	result, err = f.Apply(sql.NewEmptyContext(), nil, node)
	assert.Error(err)
	assert.True(sql.ErrTableNotFound.Is(err))

	node = plan.NewProject(
		[]sql.Expression{
			expression.NewUnresolvedColumn("i"),
		},
		plan.NewCrossJoin(
			plan.NewResolvedTable(table),
			plan.NewResolvedTable(table2),
		),
	)

	_, err = f.Apply(sql.NewEmptyContext(), nil, node)
	assert.Error(err)
	assert.True(ErrAmbiguousColumnName.Is(err))

	subquery := plan.NewSubqueryAlias(
		"b", "",
		plan.NewProject(
			[]sql.Expression{
				expression.NewGetFieldWithTable(0, sql.Int64, "mytable", "i", false),
			},
			plan.NewResolvedTable(table),
		),
	)
	// preload schema
	_ = subquery.Schema()

	node = plan.NewProject(
		[]sql.Expression{
			expression.NewUnresolvedQualifiedColumn("a", "i"),
		},
		plan.NewCrossJoin(
			plan.NewTableAlias("a", plan.NewResolvedTable(table)),
			subquery,
		),
	)

	expected = plan.NewProject(
		[]sql.Expression{
			expression.NewUnresolvedQualifiedColumn("a", "i"),
		},
		plan.NewCrossJoin(
			plan.NewTableAlias("a", plan.NewResolvedTable(table)),
			subquery,
		),
	)

	result, err = f.Apply(sql.NewEmptyContext(), nil, node)
	assert.NoError(err)
	assert.Equal(expected, result)
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

	result, err := f.Apply(sql.NewEmptyContext(), nil, node)
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

	result, err := resolveColumns(ctx, NewDefault(nil), node)
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

func TestResolveGroupingColumns(t *testing.T) {
	require := require.New(t)

	a := NewDefault(nil)
	node := plan.NewGroupBy(
		[]sql.Expression{
			expression.NewAlias(
				expression.NewUnresolvedFunction("foo", true,
					expression.NewUnresolvedColumn("c"),
				),
				"c",
			),
			expression.NewAlias(
				expression.NewUnresolvedColumn("d"),
				"b",
			),
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
			expression.NewAlias(
				expression.NewUnresolvedFunction("foo", true,
					expression.NewUnresolvedColumn("c"),
				),
				"c",
			),
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
				expression.NewAlias(
					expression.NewUnresolvedColumn("d"),
					"b",
				),
				expression.NewUnresolvedColumn("a"),
				expression.NewAlias(
					expression.NewUnresolvedColumn("b"),
					"b_01",
				),
				expression.NewUnresolvedColumn("c"),
			},
			plan.NewResolvedTable(memory.NewTable("table", nil)),
		),
	)

	result, err := resolveGroupingColumns(sql.NewEmptyContext(), a, node)
	require.NoError(err)

	require.Equal(expected, result)
}
