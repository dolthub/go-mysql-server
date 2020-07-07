package analyzer

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/liquidata-inc/go-mysql-server/memory"
	"github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/liquidata-inc/go-mysql-server/sql/expression"
	"github.com/liquidata-inc/go-mysql-server/sql/plan"
)

func TestReorderProjection(t *testing.T) {
	f := getRule("reorder_projection")

	table := memory.NewTable("mytable", sql.Schema{{
		Name: "i", Source: "mytable", Type: sql.Int64,
	}})

	testCases := []struct {
		name     string
		project  sql.Node
		expected sql.Node
	}{
		{
			"sort",
			plan.NewProject(
				[]sql.Expression{
					expression.NewGetFieldWithTable(0, sql.Int64, "mytable", "i", false),
					expression.NewAlias("foo", expression.NewLiteral(1, sql.Int64)),
					expression.NewAlias("bar", expression.NewLiteral(2, sql.Int64)),
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
						plan.NewResolvedTable(table),
					),
				),
			),
			plan.NewProject(
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
							expression.NewAlias("foo", expression.NewLiteral(1, sql.Int64)),
						},
						plan.NewFilter(
							expression.NewEquals(
								expression.NewLiteral(1, sql.Int64),
								expression.NewGetField(1, sql.Int64, "bar", false),
							),
							plan.NewProject(
								[]sql.Expression{
									expression.NewGetFieldWithTable(0, sql.Int64, "mytable", "i", false),
									expression.NewAlias("bar", expression.NewLiteral(2, sql.Int64)),
								},
								plan.NewResolvedTable(table),
							),
						),
					),
				),
			),
		},
		{
			"use alias twice",
			plan.NewProject(
				[]sql.Expression{
					expression.NewAlias("foo", expression.NewLiteral(1, sql.Int64)),
				},
				plan.NewFilter(
					expression.NewOr(
						expression.NewEquals(
							expression.NewLiteral(1, sql.Int64),
							expression.NewUnresolvedColumn("foo"),
						),
						expression.NewEquals(
							expression.NewLiteral(1, sql.Int64),
							expression.NewUnresolvedColumn("foo"),
						),
					),
					plan.NewResolvedTable(table),
				),
			),
			plan.NewProject(
				[]sql.Expression{
					expression.NewGetField(1, sql.Int64, "foo", false),
				},
				plan.NewFilter(
					expression.NewOr(
						expression.NewEquals(
							expression.NewLiteral(1, sql.Int64),
							expression.NewGetField(1, sql.Int64, "foo", false),
						),
						expression.NewEquals(
							expression.NewLiteral(1, sql.Int64),
							expression.NewGetField(1, sql.Int64, "foo", false),
						),
					),
					plan.NewProject(
						[]sql.Expression{
							expression.NewGetFieldWithTable(0, sql.Int64, "mytable", "i", false),
							expression.NewAlias("foo", expression.NewLiteral(1, sql.Int64)),
						},
						plan.NewResolvedTable(table),
					),
				),
			),
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)

			result, err := f.Apply(sql.NewEmptyContext(), NewDefault(nil), tt.project, nil)
			require.NoError(err)

			require.Equal(tt.expected, result)
		})
	}
}

func TestEraseProjection(t *testing.T) {
	require := require.New(t)
	f := getRule("erase_projection")

	table := memory.NewTable("mytable", sql.Schema{{
		Name: "i", Source: "mytable", Type: sql.Int64,
	}})

	expected := plan.NewSort(
		[]plan.SortField{{Column: expression.NewGetField(2, sql.Int64, "foo", false)}},
		plan.NewProject(
			[]sql.Expression{
				expression.NewGetFieldWithTable(0, sql.Int64, "mytable", "i", false),
				expression.NewGetField(1, sql.Int64, "bar", false),
				expression.NewAlias("foo", expression.NewLiteral(1, sql.Int64)),
			},
			plan.NewFilter(
				expression.NewEquals(
					expression.NewLiteral(1, sql.Int64),
					expression.NewGetField(1, sql.Int64, "bar", false),
				),
				plan.NewProject(
					[]sql.Expression{
						expression.NewGetFieldWithTable(0, sql.Int64, "mytable", "i", false),
						expression.NewAlias("bar", expression.NewLiteral(2, sql.Int64)),
					},
					plan.NewResolvedTable(table),
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

	result, err := f.Apply(sql.NewEmptyContext(), NewDefault(nil), node, nil)
	require.NoError(err)

	require.Equal(expected, result)

	result, err = f.Apply(sql.NewEmptyContext(), NewDefault(nil), expected, nil)
	require.NoError(err)

	require.Equal(expected, result)
}

func TestOptimizeDistinct(t *testing.T) {
	t1 := memory.NewTable("foo", sql.Schema{
		{Name: "a", Source: "foo"},
		{Name: "b", Source: "foo"},
	})

	testCases := []struct {
		name      string
		child     sql.Node
		optimized bool
	}{
		{
			"without sort",
			plan.NewResolvedTable(t1),
			false,
		},
		{
			"sort but column not projected",
			plan.NewSort(
				[]plan.SortField{
					{Column: gf(0, "foo", "c")},
				},
				plan.NewResolvedTable(t1),
			),
			false,
		},
		{
			"sort and column projected",
			plan.NewSort(
				[]plan.SortField{
					{Column: gf(0, "foo", "a")},
				},
				plan.NewResolvedTable(t1),
			),
			true,
		},
	}

	rule := getRule("optimize_distinct")

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			node, err := rule.Apply(sql.NewEmptyContext(), nil, plan.NewDistinct(tt.child), nil)
			require.NoError(t, err)

			_, ok := node.(*plan.OrderedDistinct)
			require.Equal(t, tt.optimized, ok)
		})
	}
}

func TestMoveJoinConditionsToFilter(t *testing.T) {
	t1 := memory.NewTable("t1", sql.Schema{
		{Name: "a", Source: "t1", Type: sql.Int64},
		{Name: "b", Source: "t1", Type: sql.Int64},
	})

	t2 := memory.NewTable("t2", sql.Schema{
		{Name: "c", Source: "t2", Type: sql.Int64},
		{Name: "d", Source: "t2", Type: sql.Int64},
	})

	t3 := memory.NewTable("t3", sql.Schema{
		{Name: "e", Source: "t3", Type: sql.Int64},
		{Name: "f", Source: "t3", Type: sql.Int64},
	})

	rule := getRule("move_join_conds_to_filter")
	require := require.New(t)

	node := plan.NewInnerJoin(
		plan.NewResolvedTable(t1),
		plan.NewCrossJoin(
			plan.NewResolvedTable(t2),
			plan.NewResolvedTable(t3),
		),
		expression.JoinAnd(
			eq(col(0, "t1", "a"), col(2, "t2", "c")),
			eq(col(0, "t1", "a"), col(4, "t3", "e")),
			eq(col(2, "t2", "c"), col(4, "t3", "e")),
			eq(col(0, "t1", "a"), lit(5)),
		),
	)

	result, err := rule.Apply(sql.NewEmptyContext(), NewDefault(nil), node, nil)
	require.NoError(err)

	var expected sql.Node = plan.NewInnerJoin(
		plan.NewFilter(
			eq(col(0, "t1", "a"), lit(5)),
			plan.NewResolvedTable(t1),
		),
		plan.NewFilter(
			eq(col(0, "t2", "c"), col(2, "t3", "e")),
			plan.NewCrossJoin(
				plan.NewResolvedTable(t2),
				plan.NewResolvedTable(t3),
			),
		),
		and(
			eq(col(0, "t1", "a"), col(2, "t2", "c")),
			eq(col(0, "t1", "a"), col(4, "t3", "e")),
		),
	)

	require.Equal(expected, result)

	node = plan.NewInnerJoin(
		plan.NewResolvedTable(t1),
		plan.NewCrossJoin(
			plan.NewResolvedTable(t2),
			plan.NewResolvedTable(t3),
		),
		expression.JoinAnd(
			eq(col(0, "t2", "c"), col(0, "t3", "e")),
			eq(col(0, "t1", "a"), lit(5)),
		),
	)

	result, err = rule.Apply(sql.NewEmptyContext(), NewDefault(nil), node, nil)
	require.NoError(err)

	expected = plan.NewCrossJoin(
		plan.NewFilter(
			eq(col(0, "t1", "a"), lit(5)),
			plan.NewResolvedTable(t1),
		),
		plan.NewFilter(
			eq(col(0, "t2", "c"), col(2, "t3", "e")),
			plan.NewCrossJoin(
				plan.NewResolvedTable(t2),
				plan.NewResolvedTable(t3),
			),
		),
	)

	require.Equal(result, expected)
}

func TestEvalFilter(t *testing.T) {
	inner := memory.NewTable("foo", nil)
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
				plan.NewResolvedTable(inner),
			),
		},
		{
			and(
				eq(col(0, "foo", "bar"), lit(5)),
				eq(lit(5), lit(5)),
			),
			plan.NewFilter(
				eq(col(0, "foo", "bar"), lit(5)),
				plan.NewResolvedTable(inner),
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
			plan.NewResolvedTable(inner),
		},
		{
			or(
				eq(lit(5), lit(4)),
				eq(col(0, "foo", "bar"), lit(5)),
			),
			plan.NewFilter(
				eq(col(0, "foo", "bar"), lit(5)),
				plan.NewResolvedTable(inner),
			),
		},
		{
			or(
				eq(col(0, "foo", "bar"), lit(5)),
				eq(lit(5), lit(4)),
			),
			plan.NewFilter(
				eq(col(0, "foo", "bar"), lit(5)),
				plan.NewResolvedTable(inner),
			),
		},
		{
			or(
				eq(lit(5), lit(5)),
				eq(col(0, "foo", "bar"), lit(5)),
			),
			plan.NewResolvedTable(inner),
		},
		{
			or(
				eq(col(0, "foo", "bar"), lit(5)),
				eq(lit(5), lit(5)),
			),
			plan.NewResolvedTable(inner),
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
			node := plan.NewFilter(tt.filter, plan.NewResolvedTable(inner))
			result, err := rule.Apply(sql.NewEmptyContext(), NewDefault(nil), node, nil)
			require.NoError(err)
			require.Equal(tt.expected, result)
		})
	}
}

func TestRemoveUnnecessaryConverts(t *testing.T) {
	testCases := []struct {
		name      string
		childExpr sql.Expression
		castType  string
		expected  sql.Expression
	}{
		{
			"unnecessary cast",
			expression.NewLiteral([]byte{}, sql.LongBlob),
			"binary",
			expression.NewLiteral([]byte{}, sql.LongBlob),
		},
		{
			"necessary cast",
			expression.NewLiteral("foo", sql.LongText),
			"signed",
			expression.NewConvert(
				expression.NewLiteral("foo", sql.LongText),
				"signed",
			),
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)

			node := plan.NewProject([]sql.Expression{
				expression.NewConvert(tt.childExpr, tt.castType),
			},
				plan.NewResolvedTable(memory.NewTable("foo", nil)),
			)

			result, err := removeUnnecessaryConverts(
				sql.NewEmptyContext(),
				NewDefault(nil),
				node,
				nil,
			)
			require.NoError(err)

			resultExpr := result.(*plan.Project).Projections[0]
			require.Equal(tt.expected, resultExpr)
		})
	}
}
