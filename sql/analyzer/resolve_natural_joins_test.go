package analyzer

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/mem"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
	"gopkg.in/src-d/go-mysql-server.v0/sql/plan"
)

func TestResolveNaturalJoins(t *testing.T) {
	require := require.New(t)

	left := mem.NewTable("t1", sql.Schema{
		{Name: "a", Type: sql.Int64, Source: "t1"},
		{Name: "b", Type: sql.Int64, Source: "t1"},
		{Name: "c", Type: sql.Int64, Source: "t1"},
	})

	right := mem.NewTable("t2", sql.Schema{
		{Name: "d", Type: sql.Int64, Source: "t2"},
		{Name: "c", Type: sql.Int64, Source: "t2"},
		{Name: "b", Type: sql.Int64, Source: "t2"},
		{Name: "e", Type: sql.Int64, Source: "t2"},
	})

	node := plan.NewNaturalJoin(
		plan.NewResolvedTable(left),
		plan.NewResolvedTable(right),
	)
	rule := getRule("resolve_natural_joins")

	result, err := rule.Apply(sql.NewEmptyContext(), NewDefault(nil), node)
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
			plan.NewResolvedTable(left),
			plan.NewResolvedTable(right),
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

	left := mem.NewTable("t1", sql.Schema{
		{Name: "a", Type: sql.Int64, Source: "t1"},
		{Name: "b", Type: sql.Int64, Source: "t1"},
		{Name: "c", Type: sql.Int64, Source: "t1"},
	})

	right := mem.NewTable("t2", sql.Schema{
		{Name: "d", Type: sql.Int64, Source: "t2"},
		{Name: "c", Type: sql.Int64, Source: "t2"},
		{Name: "b", Type: sql.Int64, Source: "t2"},
		{Name: "e", Type: sql.Int64, Source: "t2"},
	})

	node := plan.NewProject(
		[]sql.Expression{
			expression.NewUnresolvedQualifiedColumn("t2", "b"),
		},
		plan.NewNaturalJoin(
			plan.NewResolvedTable(left),
			plan.NewResolvedTable(right),
		),
	)

	result, err := rule.Apply(sql.NewEmptyContext(), NewDefault(nil), node)
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
				plan.NewResolvedTable(left),
				plan.NewResolvedTable(right),
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

	left := mem.NewTable("t1", sql.Schema{
		{Name: "a", Type: sql.Int64, Source: "t1"},
		{Name: "b", Type: sql.Int64, Source: "t1"},
		{Name: "c", Type: sql.Int64, Source: "t1"},
	})

	right := mem.NewTable("t2", sql.Schema{
		{Name: "d", Type: sql.Int64, Source: "t2"},
		{Name: "c", Type: sql.Int64, Source: "t2"},
		{Name: "b", Type: sql.Int64, Source: "t2"},
		{Name: "e", Type: sql.Int64, Source: "t2"},
	})

	node := plan.NewProject(
		[]sql.Expression{
			expression.NewUnresolvedQualifiedColumn("t2", "b"),
			expression.NewUnresolvedQualifiedColumn("t2-alias", "c"),
		},
		plan.NewNaturalJoin(
			plan.NewResolvedTable(left),
			plan.NewTableAlias("t2-alias", plan.NewResolvedTable(right)),
		),
	)

	result, err := rule.Apply(sql.NewEmptyContext(), NewDefault(nil), node)
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
				expression.NewGetFieldWithTable(3, sql.Int64, "t2", "d", false),
				expression.NewGetFieldWithTable(6, sql.Int64, "t2", "e", false),
			},
			plan.NewInnerJoin(
				plan.NewResolvedTable(left),
				plan.NewTableAlias("t2-alias", plan.NewResolvedTable(right)),
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

func TestResolveNaturalJoinsChained(t *testing.T) {
	rule := getRule("resolve_natural_joins")
	require := require.New(t)

	left := mem.NewTable("t1", sql.Schema{
		{Name: "a", Type: sql.Int64, Source: "t1"},
		{Name: "b", Type: sql.Int64, Source: "t1"},
		{Name: "c", Type: sql.Int64, Source: "t1"},
		{Name: "f", Type: sql.Int64, Source: "t1"},
	})

	right := mem.NewTable("t2", sql.Schema{
		{Name: "d", Type: sql.Int64, Source: "t2"},
		{Name: "c", Type: sql.Int64, Source: "t2"},
		{Name: "b", Type: sql.Int64, Source: "t2"},
		{Name: "e", Type: sql.Int64, Source: "t2"},
	})

	upperRight := mem.NewTable("t3", sql.Schema{
		{Name: "a", Type: sql.Int64, Source: "t3"},
		{Name: "b", Type: sql.Int64, Source: "t3"},
		{Name: "f", Type: sql.Int64, Source: "t3"},
		{Name: "g", Type: sql.Int64, Source: "t3"},
	})

	node := plan.NewProject(
		[]sql.Expression{
			expression.NewUnresolvedQualifiedColumn("t2", "b"),
			expression.NewUnresolvedQualifiedColumn("t2-alias", "c"),
			expression.NewUnresolvedQualifiedColumn("t3-alias", "f"),
		},
		plan.NewNaturalJoin(
			plan.NewNaturalJoin(
				plan.NewResolvedTable(left),
				plan.NewTableAlias("t2-alias", plan.NewResolvedTable(right)),
			),
			plan.NewTableAlias("t3-alias", plan.NewResolvedTable(upperRight)),
		),
	)

	result, err := rule.Apply(sql.NewEmptyContext(), NewDefault(nil), node)
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
				expression.NewGetFieldWithTable(4, sql.Int64, "t2", "d", false),
				expression.NewGetFieldWithTable(5, sql.Int64, "t2", "e", false),
				expression.NewGetFieldWithTable(9, sql.Int64, "t3", "g", false),
			},
			plan.NewInnerJoin(
				plan.NewProject(
					[]sql.Expression{
						expression.NewGetFieldWithTable(1, sql.Int64, "t1", "b", false),
						expression.NewGetFieldWithTable(2, sql.Int64, "t1", "c", false),
						expression.NewGetFieldWithTable(0, sql.Int64, "t1", "a", false),
						expression.NewGetFieldWithTable(3, sql.Int64, "t1", "f", false),
						expression.NewGetFieldWithTable(4, sql.Int64, "t2", "d", false),
						expression.NewGetFieldWithTable(7, sql.Int64, "t2", "e", false),
					},
					plan.NewInnerJoin(
						plan.NewResolvedTable(left),
						plan.NewTableAlias("t2-alias", plan.NewResolvedTable(right)),
						expression.JoinAnd(
							expression.NewEquals(
								expression.NewGetFieldWithTable(1, sql.Int64, "t1", "b", false),
								expression.NewGetFieldWithTable(6, sql.Int64, "t2", "b", false),
							),
							expression.NewEquals(
								expression.NewGetFieldWithTable(2, sql.Int64, "t1", "c", false),
								expression.NewGetFieldWithTable(5, sql.Int64, "t2", "c", false),
							),
						),
					),
				),
				plan.NewTableAlias("t3-alias", plan.NewResolvedTable(upperRight)),
				expression.JoinAnd(
					expression.NewEquals(
						expression.NewGetFieldWithTable(0, sql.Int64, "t1", "b", false),
						expression.NewGetFieldWithTable(7, sql.Int64, "t3", "b", false),
					),
					expression.NewEquals(
						expression.NewGetFieldWithTable(2, sql.Int64, "t1", "a", false),
						expression.NewGetFieldWithTable(6, sql.Int64, "t3", "a", false),
					),
					expression.NewEquals(
						expression.NewGetFieldWithTable(3, sql.Int64, "t1", "f", false),
						expression.NewGetFieldWithTable(8, sql.Int64, "t3", "f", false),
					),
				),
			),
		),
	)

	require.Equal(expected, result)
}

func TestResolveNaturalJoinsEqual(t *testing.T) {
	require := require.New(t)

	left := mem.NewTable("t1", sql.Schema{
		{Name: "a", Type: sql.Int64, Source: "t1"},
		{Name: "b", Type: sql.Int64, Source: "t1"},
		{Name: "c", Type: sql.Int64, Source: "t1"},
	})

	right := mem.NewTable("t2", sql.Schema{
		{Name: "a", Type: sql.Int64, Source: "t2"},
		{Name: "b", Type: sql.Int64, Source: "t2"},
		{Name: "c", Type: sql.Int64, Source: "t2"},
	})

	node := plan.NewNaturalJoin(
		plan.NewResolvedTable(left),
		plan.NewResolvedTable(right),
	)
	rule := getRule("resolve_natural_joins")

	result, err := rule.Apply(sql.NewEmptyContext(), NewDefault(nil), node)
	require.NoError(err)

	expected := plan.NewProject(
		[]sql.Expression{
			expression.NewGetFieldWithTable(0, sql.Int64, "t1", "a", false),
			expression.NewGetFieldWithTable(1, sql.Int64, "t1", "b", false),
			expression.NewGetFieldWithTable(2, sql.Int64, "t1", "c", false),
		},
		plan.NewInnerJoin(
			plan.NewResolvedTable(left),
			plan.NewResolvedTable(right),
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

	left := mem.NewTable("t1", sql.Schema{
		{Name: "a", Type: sql.Int64, Source: "t1"},
		{Name: "b", Type: sql.Int64, Source: "t1"},
		{Name: "c", Type: sql.Int64, Source: "t1"},
	})

	right := mem.NewTable("t2", sql.Schema{
		{Name: "d", Type: sql.Int64, Source: "t2"},
		{Name: "e", Type: sql.Int64, Source: "t2"},
	})

	node := plan.NewNaturalJoin(
		plan.NewResolvedTable(left),
		plan.NewResolvedTable(right),
	)
	rule := getRule("resolve_natural_joins")

	result, err := rule.Apply(sql.NewEmptyContext(), NewDefault(nil), node)
	require.NoError(err)

	expected := plan.NewCrossJoin(
		plan.NewResolvedTable(left),
		plan.NewResolvedTable(right),
	)
	require.Equal(expected, result)
}
