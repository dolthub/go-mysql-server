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

func TestResolveHaving(t *testing.T) {
	require := require.New(t)

	var node sql.Node = plan.NewHaving(
		expression.NewGreaterThan(
			aggregation.NewCount(expression.NewStar()),
			expression.NewLiteral(int64(5), sql.Int64),
		),
		plan.NewGroupBy(
			[]sql.Expression{
				expression.NewAlias(aggregation.NewCount(expression.NewGetField(0, sql.Int64, "foo", false)), "x"),
				expression.NewGetField(0, sql.Int64, "foo", false),
			},
			[]sql.Expression{expression.NewGetField(0, sql.Int64, "foo", false)},
			plan.NewResolvedTable(mem.NewTable("t", nil)),
		),
	)

	var expected sql.Node = plan.NewHaving(
		expression.NewGreaterThan(
			expression.NewGetField(0, sql.Int64, "x", false),
			expression.NewLiteral(int64(5), sql.Int64),
		),
		plan.NewGroupBy(
			[]sql.Expression{
				expression.NewAlias(aggregation.NewCount(expression.NewGetField(0, sql.Int64, "foo", false)), "x"),
				expression.NewGetField(0, sql.Int64, "foo", false),
			},
			[]sql.Expression{expression.NewGetField(0, sql.Int64, "foo", false)},
			plan.NewResolvedTable(mem.NewTable("t", nil)),
		),
	)

	result, err := resolveHaving(sql.NewEmptyContext(), nil, node)
	require.NoError(err)
	require.Equal(expected, result)

	node = plan.NewHaving(
		expression.NewGreaterThan(
			aggregation.NewCount(expression.NewStar()),
			expression.NewLiteral(int64(5), sql.Int64),
		),
		plan.NewGroupBy(
			[]sql.Expression{
				expression.NewAlias(aggregation.NewAvg(expression.NewGetField(0, sql.Int64, "foo", false)), "x"),
				expression.NewGetFieldWithTable(0, sql.Int64, "t", "foo", false),
			},
			[]sql.Expression{expression.NewGetField(0, sql.Int64, "foo", false)},
			plan.NewResolvedTable(mem.NewTable("t", nil)),
		),
	)

	expected = plan.NewProject(
		[]sql.Expression{
			expression.NewGetField(0, sql.Float64, "x", true),
			expression.NewGetFieldWithTable(1, sql.Int64, "t", "foo", false),
		},
		plan.NewHaving(
			expression.NewGreaterThan(
				expression.NewGetField(2, sql.Int64, "COUNT(*)", false),
				expression.NewLiteral(int64(5), sql.Int64),
			),
			plan.NewGroupBy(
				[]sql.Expression{
					expression.NewAlias(aggregation.NewAvg(expression.NewGetField(0, sql.Int64, "foo", false)), "x"),
					expression.NewGetFieldWithTable(0, sql.Int64, "t", "foo", false),
					aggregation.NewCount(expression.NewStar()),
				},
				[]sql.Expression{expression.NewGetField(0, sql.Int64, "foo", false)},
				plan.NewResolvedTable(mem.NewTable("t", nil)),
			),
		),
	)

	result, err = resolveHaving(sql.NewEmptyContext(), nil, node)
	require.NoError(err)
	require.Equal(expected, result)

	node = plan.NewHaving(
		expression.NewGreaterThan(
			aggregation.NewCount(expression.NewStar()),
			expression.NewLiteral(int64(5), sql.Int64),
		),
		plan.NewResolvedTable(mem.NewTable("t", nil)),
	)

	_, err = resolveHaving(sql.NewEmptyContext(), nil, node)
	require.Error(err)
	require.True(errHavingNeedsGroupBy.Is(err))
}
