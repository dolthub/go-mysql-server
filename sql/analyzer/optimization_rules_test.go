package analyzer

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/mem"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
	"gopkg.in/src-d/go-mysql-server.v0/sql/plan"
)

func TestReorderProjection(t *testing.T) {
	require := require.New(t)
	f := getRule("reorder_projection")

	table := mem.NewTable("mytable", sql.Schema{{
		Name: "i", Source: "mytable", Type: sql.Int64,
	}})

	node := plan.NewProject(
		[]sql.Expression{
			expression.NewGetFieldWithTable(0, sql.Int64, "mytable", "i", false),
			expression.NewAlias(expression.NewLiteral(1, sql.Int64), "foo"),
			expression.NewAlias(expression.NewLiteral(2, sql.Int64), "bar"),
		},
		plan.NewSort(
			[]plan.SortField{
				{Column: expression.NewUnresolvedColumn("foo")},
			},
			plan.NewFilter(
				expression.NewEquals(
					expression.NewLiteral(1, sql.Int64),
					expression.NewUnresolvedColumn("bar"),
				),
				table,
			),
		),
	)

	expected := plan.NewProject(
		[]sql.Expression{
			expression.NewGetFieldWithTable(0, sql.Int64, "mytable", "i", false),
			expression.NewGetField(2, sql.Int64, "foo", false),
			expression.NewGetField(1, sql.Int64, "bar", false),
		},
		plan.NewSort(
			[]plan.SortField{{Column: expression.NewGetField(2, sql.Int64, "foo", false)}},
			plan.NewProject(
				[]sql.Expression{
					expression.NewGetFieldWithTable(0, sql.Int64, "mytable", "i", false),
					expression.NewGetField(1, sql.Int64, "bar", false),
					expression.NewAlias(expression.NewLiteral(1, sql.Int64), "foo"),
				},
				plan.NewFilter(
					expression.NewEquals(
						expression.NewLiteral(1, sql.Int64),
						expression.NewGetField(1, sql.Int64, "bar", false),
					),
					plan.NewProject(
						[]sql.Expression{
							expression.NewGetFieldWithTable(0, sql.Int64, "mytable", "i", false),
							expression.NewAlias(expression.NewLiteral(2, sql.Int64), "bar"),
						},
						table,
					),
				),
			),
		),
	)

	result, err := f.Apply(sql.NewEmptyContext(), NewDefault(nil), node)
	require.NoError(err)

	require.Equal(expected, result)
}

func TestEraseProjection(t *testing.T) {
	require := require.New(t)
	f := getRule("erase_projection")

	table := mem.NewTable("mytable", sql.Schema{{
		Name: "i", Source: "mytable", Type: sql.Int64,
	}})

	expected := plan.NewSort(
		[]plan.SortField{{Column: expression.NewGetField(2, sql.Int64, "foo", false)}},
		plan.NewProject(
			[]sql.Expression{
				expression.NewGetFieldWithTable(0, sql.Int64, "mytable", "i", false),
				expression.NewGetField(1, sql.Int64, "bar", false),
				expression.NewAlias(expression.NewLiteral(1, sql.Int64), "foo"),
			},
			plan.NewFilter(
				expression.NewEquals(
					expression.NewLiteral(1, sql.Int64),
					expression.NewGetField(1, sql.Int64, "bar", false),
				),
				plan.NewProject(
					[]sql.Expression{
						expression.NewGetFieldWithTable(0, sql.Int64, "mytable", "i", false),
						expression.NewAlias(expression.NewLiteral(2, sql.Int64), "bar"),
					},
					table,
				),
			),
		),
	)

	node := plan.NewProject(
		[]sql.Expression{
			expression.NewGetFieldWithTable(0, sql.Int64, "mytable", "i", false),
			expression.NewGetField(1, sql.Int64, "bar", false),
			expression.NewGetField(2, sql.Int64, "foo", false),
		},
		expected,
	)

	result, err := f.Apply(sql.NewEmptyContext(), NewDefault(nil), node)
	require.NoError(err)

	require.Equal(expected, result)

	result, err = f.Apply(sql.NewEmptyContext(), NewDefault(nil), expected)
	require.NoError(err)

	require.Equal(expected, result)
}

func TestOptimizeDistinct(t *testing.T) {
	require := require.New(t)
	notSorted := plan.NewDistinct(mem.NewTable("foo", nil))
	sorted := plan.NewDistinct(plan.NewSort(nil, mem.NewTable("foo", nil)))

	rule := getRule("optimize_distinct")

	analyzedNotSorted, err := rule.Apply(sql.NewEmptyContext(), nil, notSorted)
	require.NoError(err)

	analyzedSorted, err := rule.Apply(sql.NewEmptyContext(), nil, sorted)
	require.NoError(err)

	require.Equal(notSorted, analyzedNotSorted)
	require.Equal(plan.NewOrderedDistinct(sorted.Child), analyzedSorted)
}

func TestMoveJoinConditionsToFilter(t *testing.T) {
	t1 := mem.NewTable("t1", sql.Schema{
		{Name: "a", Source: "t1"},
		{Name: "b", Source: "t1"},
	})

	t2 := mem.NewTable("t2", sql.Schema{
		{Name: "c", Source: "t2"},
		{Name: "d", Source: "t2"},
	})

	t3 := mem.NewTable("t3", sql.Schema{
		{Name: "e", Source: "t3"},
		{Name: "f", Source: "t3"},
	})

	rule := getRule("move_join_conds_to_filter")
	require := require.New(t)

	node := plan.NewInnerJoin(
		t1,
		plan.NewCrossJoin(t2, t3),
		expression.JoinAnd(
			eq(col(0, "t1", "a"), col(2, "t2", "c")),
			eq(col(0, "t1", "a"), col(4, "t3", "e")),
			eq(col(2, "t2", "c"), col(4, "t3", "e")),
			eq(col(0, "t1", "a"), lit(5)),
		),
	)

	result, err := rule.Apply(sql.NewEmptyContext(), NewDefault(nil), node)
	require.NoError(err)

	var expected sql.Node = plan.NewInnerJoin(
		plan.NewFilter(
			eq(col(0, "t1", "a"), lit(5)),
			t1,
		),
		plan.NewFilter(
			eq(col(0, "t2", "c"), col(2, "t3", "e")),
			plan.NewCrossJoin(t2, t3),
		),
		and(
			eq(col(0, "t1", "a"), col(2, "t2", "c")),
			eq(col(0, "t1", "a"), col(4, "t3", "e")),
		),
	)

	require.Equal(expected, result)

	node = plan.NewInnerJoin(
		t1,
		plan.NewCrossJoin(t2, t3),
		expression.JoinAnd(
			eq(col(0, "t2", "c"), col(0, "t3", "e")),
			eq(col(0, "t1", "a"), lit(5)),
		),
	)

	result, err = rule.Apply(sql.NewEmptyContext(), NewDefault(nil), node)
	require.NoError(err)

	expected = plan.NewCrossJoin(
		plan.NewFilter(
			eq(col(0, "t1", "a"), lit(5)),
			t1,
		),
		plan.NewFilter(
			eq(col(0, "t2", "c"), col(2, "t3", "e")),
			plan.NewCrossJoin(t2, t3),
		),
	)

	require.Equal(result, expected)
}

func TestEvalFilter(t *testing.T) {
	inner := mem.NewTable("foo", nil)
	rule := getRule("eval_filter")

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
				inner,
			),
		},
		{
			and(
				eq(col(0, "foo", "bar"), lit(5)),
				eq(lit(5), lit(5)),
			),
			plan.NewFilter(
				eq(col(0, "foo", "bar"), lit(5)),
				inner,
			),
		},
		{
			and(
				eq(lit(5), lit(4)),
				eq(col(0, "foo", "bar"), lit(5)),
			),
			plan.EmptyTable,
		},
		{
			and(
				eq(col(0, "foo", "bar"), lit(5)),
				eq(lit(5), lit(4)),
			),
			plan.EmptyTable,
		},
		{
			and(
				eq(lit(4), lit(4)),
				eq(lit(5), lit(5)),
			),
			inner,
		},
		{
			or(
				eq(lit(5), lit(4)),
				eq(col(0, "foo", "bar"), lit(5)),
			),
			plan.NewFilter(
				eq(col(0, "foo", "bar"), lit(5)),
				inner,
			),
		},
		{
			or(
				eq(col(0, "foo", "bar"), lit(5)),
				eq(lit(5), lit(4)),
			),
			plan.NewFilter(
				eq(col(0, "foo", "bar"), lit(5)),
				inner,
			),
		},
		{
			or(
				eq(lit(5), lit(5)),
				eq(col(0, "foo", "bar"), lit(5)),
			),
			inner,
		},
		{
			or(
				eq(col(0, "foo", "bar"), lit(5)),
				eq(lit(5), lit(5)),
			),
			inner,
		},
		{
			or(
				eq(lit(5), lit(4)),
				eq(lit(5), lit(4)),
			),
			plan.EmptyTable,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.filter.String(), func(t *testing.T) {
			require := require.New(t)
			node := plan.NewFilter(tt.filter, inner)
			result, err := rule.Apply(sql.NewEmptyContext(), NewDefault(nil), node)
			require.NoError(err)
			require.Equal(tt.expected, result)
		})
	}
}
