package analyzer

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/mem"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression/function/aggregation"
	"gopkg.in/src-d/go-mysql-server.v0/sql/plan"
)

func TestReorderAggregations(t *testing.T) {
	require := require.New(t)

	table := mem.NewTable("foo", sql.Schema{
		{Name: "a", Type: sql.Int64, Source: "foo"},
		{Name: "b", Type: sql.Int64, Source: "foo"},
		{Name: "c", Type: sql.Int64, Source: "foo"},
	})
	rule := getRule("reorder_aggregations")

	node := plan.NewGroupBy(
		[]sql.Expression{
			expression.NewArithmetic(
				aggregation.NewSum(
					expression.NewGetFieldWithTable(0, sql.Int64, "foo", "a", false),
				),
				expression.NewLiteral(int64(1), sql.Int64),
				"+",
			),
		},
		[]sql.Expression{
			expression.NewGetFieldWithTable(0, sql.Int64, "foo", "a", false),
		},
		plan.NewResolvedTable(table),
	)

	expected := plan.NewProject(
		[]sql.Expression{
			expression.NewArithmetic(
				expression.NewGetField(0, sql.Float64, "SUM(foo.a)", false),
				expression.NewLiteral(int64(1), sql.Int64),
				"+",
			),
		},
		plan.NewGroupBy(
			[]sql.Expression{
				aggregation.NewSum(
					expression.NewGetFieldWithTable(0, sql.Int64, "foo", "a", false),
				),
			},
			[]sql.Expression{
				expression.NewGetFieldWithTable(0, sql.Int64, "foo", "a", false),
			},
			plan.NewResolvedTable(table),
		),
	)

	result, err := rule.Apply(sql.NewEmptyContext(), NewDefault(nil), node)
	require.NoError(err)

	require.Equal(expected, result)
}

func TestReorderAggregationsMultiple(t *testing.T) {
	require := require.New(t)

	table := mem.NewTable("foo", sql.Schema{
		{Name: "a", Type: sql.Int64, Source: "foo"},
		{Name: "b", Type: sql.Int64, Source: "foo"},
		{Name: "c", Type: sql.Int64, Source: "foo"},
	})
	rule := getRule("reorder_aggregations")

	node := plan.NewGroupBy(
		[]sql.Expression{
			expression.NewArithmetic(
				aggregation.NewSum(
					expression.NewGetFieldWithTable(0, sql.Int64, "foo", "a", false),
				),
				aggregation.NewCount(
					expression.NewGetFieldWithTable(0, sql.Int64, "foo", "a", false),
				),
				"/",
			),
			expression.NewGetFieldWithTable(1, sql.Int64, "foo", "b", false),
		},
		[]sql.Expression{
			expression.NewGetFieldWithTable(0, sql.Int64, "foo", "a", false),
			expression.NewGetFieldWithTable(1, sql.Int64, "foo", "b", false),
		},
		plan.NewResolvedTable(table),
	)

	expected := plan.NewProject(
		[]sql.Expression{
			expression.NewArithmetic(
				expression.NewGetField(0, sql.Float64, "SUM(foo.a)", false),
				expression.NewGetField(1, sql.Int32, "COUNT(foo.a)", false),
				"/",
			),
			expression.NewGetFieldWithTable(2, sql.Int64, "foo", "b", false),
		},
		plan.NewGroupBy(
			[]sql.Expression{
				aggregation.NewSum(
					expression.NewGetFieldWithTable(0, sql.Int64, "foo", "a", false),
				),
				aggregation.NewCount(
					expression.NewGetFieldWithTable(0, sql.Int64, "foo", "a", false),
				),
				expression.NewGetFieldWithTable(1, sql.Int64, "foo", "b", false),
			},
			[]sql.Expression{
				expression.NewGetFieldWithTable(0, sql.Int64, "foo", "a", false),
				expression.NewGetFieldWithTable(1, sql.Int64, "foo", "b", false),
			},
			plan.NewResolvedTable(table),
		),
	)

	result, err := rule.Apply(sql.NewEmptyContext(), NewDefault(nil), node)
	require.NoError(err)

	require.Equal(expected, result)
}
