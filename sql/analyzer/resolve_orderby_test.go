package analyzer

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/mem"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
	"gopkg.in/src-d/go-mysql-server.v0/sql/plan"
)

func TestResolveOrderBy(t *testing.T) {
	rule := getRule("resolve_orderby")
	a := NewDefault(nil)
	ctx := sql.NewEmptyContext()

	table := mem.NewTable("foo", sql.Schema{
		{Name: "a", Type: sql.Int64, Source: "foo"},
		{Name: "b", Type: sql.Int64, Source: "foo"},
	})

	t.Run("with project", func(t *testing.T) {
		require := require.New(t)
		node := plan.NewSort(
			[]plan.SortField{
				{Column: expression.NewUnresolvedColumn("x")},
			},
			plan.NewProject(
				[]sql.Expression{
					expression.NewAlias(
						expression.NewGetFieldWithTable(0, sql.Int64, "foo", "a", false),
						"x",
					),
				},
				plan.NewResolvedTable(table),
			),
		)

		result, err := rule.Apply(ctx, a, node)
		require.NoError(err)

		require.Equal(node, result)

		node = plan.NewSort(
			[]plan.SortField{
				{Column: expression.NewUnresolvedColumn("a")},
			},
			plan.NewProject(
				[]sql.Expression{
					expression.NewAlias(
						expression.NewGetFieldWithTable(0, sql.Int64, "foo", "a", false),
						"x",
					),
				},
				plan.NewResolvedTable(table),
			),
		)

		expected := plan.NewProject(
			[]sql.Expression{
				expression.NewAlias(
					expression.NewGetFieldWithTable(0, sql.Int64, "foo", "a", false),
					"x",
				),
			},
			plan.NewSort(
				[]plan.SortField{
					{Column: expression.NewUnresolvedColumn("a")},
				},
				plan.NewResolvedTable(table),
			),
		)

		result, err = rule.Apply(ctx, a, node)
		require.NoError(err)

		require.Equal(expected, result)

		node = plan.NewSort(
			[]plan.SortField{
				{Column: expression.NewUnresolvedColumn("a")},
				{Column: expression.NewUnresolvedColumn("x")},
			},
			plan.NewProject(
				[]sql.Expression{
					expression.NewAlias(
						expression.NewGetFieldWithTable(0, sql.Int64, "foo", "a", false),
						"x",
					),
				},
				plan.NewResolvedTable(table),
			),
		)

		expected = plan.NewProject(
			[]sql.Expression{
				expression.NewGetFieldWithTable(0, sql.Int64, "", "x", false),
			},
			plan.NewSort(
				[]plan.SortField{
					{Column: expression.NewUnresolvedColumn("a")},
					{Column: expression.NewUnresolvedColumn("x")},
				},
				plan.NewProject(
					[]sql.Expression{
						expression.NewAlias(
							expression.NewGetFieldWithTable(0, sql.Int64, "foo", "a", false),
							"x",
						),
						expression.NewUnresolvedColumn("a"),
					},
					plan.NewResolvedTable(table),
				),
			),
		)

		result, err = rule.Apply(ctx, a, node)
		require.NoError(err)

		require.Equal(expected, result)
	})

	t.Run("with group by", func(t *testing.T) {
		require := require.New(t)
		node := plan.NewSort(
			[]plan.SortField{
				{Column: expression.NewUnresolvedColumn("x")},
			},
			plan.NewGroupBy(
				[]sql.Expression{
					expression.NewAlias(
						expression.NewGetFieldWithTable(0, sql.Int64, "foo", "a", false),
						"x",
					),
				},
				[]sql.Expression{
					expression.NewGetFieldWithTable(0, sql.Int64, "foo", "a", false),
				},
				plan.NewResolvedTable(table),
			),
		)

		result, err := rule.Apply(ctx, a, node)
		require.NoError(err)

		require.Equal(node, result)

		node = plan.NewSort(
			[]plan.SortField{
				{Column: expression.NewUnresolvedColumn("a")},
			},
			plan.NewGroupBy(
				[]sql.Expression{
					expression.NewAlias(
						expression.NewGetFieldWithTable(0, sql.Int64, "foo", "a", false),
						"x",
					),
				},
				[]sql.Expression{
					expression.NewGetFieldWithTable(0, sql.Int64, "foo", "a", false),
				},
				plan.NewResolvedTable(table),
			),
		)

		var expected sql.Node = plan.NewGroupBy(
			[]sql.Expression{
				expression.NewAlias(
					expression.NewGetFieldWithTable(0, sql.Int64, "foo", "a", false),
					"x",
				),
			},
			[]sql.Expression{
				expression.NewGetFieldWithTable(0, sql.Int64, "foo", "a", false),
			},
			plan.NewSort(
				[]plan.SortField{
					{Column: expression.NewUnresolvedColumn("a")},
				},
				plan.NewResolvedTable(table),
			),
		)

		result, err = rule.Apply(ctx, a, node)
		require.NoError(err)

		require.Equal(expected, result)

		node = plan.NewSort(
			[]plan.SortField{
				{Column: expression.NewUnresolvedColumn("a")},
				{Column: expression.NewUnresolvedColumn("x")},
			},
			plan.NewGroupBy(
				[]sql.Expression{
					expression.NewAlias(
						expression.NewGetFieldWithTable(0, sql.Int64, "foo", "a", false),
						"x",
					),
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
				[]plan.SortField{
					{Column: expression.NewUnresolvedColumn("a")},
					{Column: expression.NewUnresolvedColumn("x")},
				},
				plan.NewGroupBy(
					[]sql.Expression{
						expression.NewAlias(
							expression.NewGetFieldWithTable(0, sql.Int64, "foo", "a", false),
							"x",
						),
						expression.NewUnresolvedColumn("a"),
					},
					[]sql.Expression{
						expression.NewGetFieldWithTable(0, sql.Int64, "foo", "a", false),
					},
					plan.NewResolvedTable(table),
				),
			),
		)

		result, err = rule.Apply(ctx, a, node)
		require.NoError(err)

		require.Equal(expected, result)
	})
}

func TestResolveOrderByLiterals(t *testing.T) {
	require := require.New(t)
	f := getRule("resolve_orderby_literals")

	table := mem.NewTable("t", sql.Schema{
		{Name: "a", Type: sql.Int64, Source: "t"},
		{Name: "b", Type: sql.Int64, Source: "t"},
	})

	node := plan.NewSort(
		[]plan.SortField{
			{Column: expression.NewLiteral(int64(2), sql.Int64)},
			{Column: expression.NewLiteral(int64(1), sql.Int64)},
		},
		plan.NewResolvedTable(table),
	)

	result, err := f.Apply(sql.NewEmptyContext(), NewDefault(nil), node)
	require.NoError(err)

	require.Equal(
		plan.NewSort(
			[]plan.SortField{
				{Column: expression.NewUnresolvedColumn("b")},
				{Column: expression.NewUnresolvedColumn("a")},
			},
			plan.NewResolvedTable(table),
		),
		result,
	)

	node = plan.NewSort(
		[]plan.SortField{
			{Column: expression.NewLiteral(int64(3), sql.Int64)},
			{Column: expression.NewLiteral(int64(1), sql.Int64)},
		},
		plan.NewResolvedTable(table),
	)

	_, err = f.Apply(sql.NewEmptyContext(), NewDefault(nil), node)
	require.Error(err)
	require.True(ErrOrderByColumnIndex.Is(err))
}
