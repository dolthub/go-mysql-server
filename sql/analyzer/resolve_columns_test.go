package analyzer

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/mem"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
	"gopkg.in/src-d/go-mysql-server.v0/sql/plan"
)

func TestMisusedAlias(t *testing.T) {
	require := require.New(t)
	f := getRule("resolve_columns")

	table := mem.NewTable("mytable", sql.Schema{
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
		plan.NewResolvedTable("mytable", table),
	)

	// the first iteration wrap the unresolved column "alias_i" as a maybeAlias
	n, err := f.Apply(sql.NewEmptyContext(), nil, node)
	require.NoError(err)

	// if maybeAlias is not resolved it fails
	_, err = f.Apply(sql.NewEmptyContext(), nil, n)
	require.EqualError(err, ErrMisusedAlias.New("alias_i").Error())
}

func TestQualifyColumns(t *testing.T) {
	require := require.New(t)
	f := getRule("qualify_columns")

	table := mem.NewTable("mytable", sql.Schema{{Name: "i", Type: sql.Int32}})
	table2 := mem.NewTable("mytable2", sql.Schema{{Name: "i", Type: sql.Int32}})

	node := plan.NewProject(
		[]sql.Expression{
			expression.NewUnresolvedColumn("i"),
		},
		plan.NewResolvedTable("mytable", table),
	)

	expected := plan.NewProject(
		[]sql.Expression{
			expression.NewUnresolvedQualifiedColumn("mytable", "i"),
		},
		plan.NewResolvedTable("mytable", table),
	)

	result, err := f.Apply(sql.NewEmptyContext(), nil, node)
	require.NoError(err)
	require.Equal(expected, result)

	node = plan.NewProject(
		[]sql.Expression{
			expression.NewUnresolvedQualifiedColumn("mytable", "i"),
		},
		plan.NewResolvedTable("mytable", table),
	)

	result, err = f.Apply(sql.NewEmptyContext(), nil, node)
	require.NoError(err)
	require.Equal(expected, result)

	node = plan.NewProject(
		[]sql.Expression{
			expression.NewUnresolvedQualifiedColumn("a", "i"),
		},
		plan.NewTableAlias("a", plan.NewResolvedTable("mytable", table)),
	)

	expected = plan.NewProject(
		[]sql.Expression{
			expression.NewUnresolvedQualifiedColumn("mytable", "i"),
		},
		plan.NewTableAlias("a", plan.NewResolvedTable("mytable", table)),
	)

	result, err = f.Apply(sql.NewEmptyContext(), nil, node)
	require.NoError(err)
	require.Equal(expected, result)

	node = plan.NewProject(
		[]sql.Expression{
			expression.NewUnresolvedColumn("z"),
		},
		plan.NewTableAlias("a", plan.NewResolvedTable("mytable", table)),
	)

	result, err = f.Apply(sql.NewEmptyContext(), nil, node)
	require.NoError(err)
	require.Equal(node, result)

	node = plan.NewProject(
		[]sql.Expression{
			expression.NewUnresolvedQualifiedColumn("foo", "i"),
		},
		plan.NewTableAlias("a", plan.NewResolvedTable("mytable", table)),
	)

	result, err = f.Apply(sql.NewEmptyContext(), nil, node)
	require.Error(err)
	require.True(sql.ErrTableNotFound.Is(err))

	node = plan.NewProject(
		[]sql.Expression{
			expression.NewUnresolvedColumn("i"),
		},
		plan.NewCrossJoin(
			plan.NewResolvedTable("mytable", table),
			plan.NewResolvedTable("mytable2", table2),
		),
	)

	_, err = f.Apply(sql.NewEmptyContext(), nil, node)
	require.Error(err)
	require.True(ErrAmbiguousColumnName.Is(err))

	subquery := plan.NewSubqueryAlias(
		"b",
		plan.NewProject(
			[]sql.Expression{
				expression.NewGetFieldWithTable(0, sql.Int64, "mytable", "i", false),
			},
			plan.NewResolvedTable("mytable", table),
		),
	)
	// preload schema
	_ = subquery.Schema()

	node = plan.NewProject(
		[]sql.Expression{
			expression.NewUnresolvedQualifiedColumn("a", "i"),
		},
		plan.NewCrossJoin(
			plan.NewTableAlias("a", plan.NewResolvedTable("mytable", table)),
			subquery,
		),
	)

	expected = plan.NewProject(
		[]sql.Expression{
			expression.NewUnresolvedQualifiedColumn("mytable", "i"),
		},
		plan.NewCrossJoin(
			plan.NewTableAlias("a", plan.NewResolvedTable("mytable", table)),
			subquery,
		),
	)

	result, err = f.Apply(sql.NewEmptyContext(), nil, node)
	require.NoError(err)
	require.Equal(expected, result)
}

func TestQualifyColumnsQualifiedStar(t *testing.T) {
	require := require.New(t)
	f := getRule("qualify_columns")

	table := mem.NewTable("mytable", sql.Schema{{Name: "i", Type: sql.Int32}})

	node := plan.NewProject(
		[]sql.Expression{
			expression.NewUnresolvedFunction(
				"count",
				true,
				expression.NewQualifiedStar("mytable"),
			),
		},
		plan.NewResolvedTable("mytable", table),
	)

	expected := plan.NewProject(
		[]sql.Expression{
			expression.NewUnresolvedFunction(
				"count",
				true,
				expression.NewStar(),
			),
		},
		plan.NewResolvedTable("mytable", table),
	)

	result, err := f.Apply(sql.NewEmptyContext(), nil, node)
	require.NoError(err)
	require.Equal(expected, result)
}
