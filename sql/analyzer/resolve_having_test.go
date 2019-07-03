package analyzer

import (
	"testing"

	"github.com/src-d/go-mysql-server/mem"
	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/expression"
	"github.com/src-d/go-mysql-server/sql/expression/function/aggregation"
	"github.com/src-d/go-mysql-server/sql/plan"
	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-errors.v1"
)

func TestResolveHaving(t *testing.T) {
	testCases := []struct {
		name     string
		input    sql.Node
		expected sql.Node
		err      *errors.Kind
	}{
		{
			"replace existing aggregation in group by",
			plan.NewHaving(
				expression.NewGreaterThan(
					aggregation.NewAvg(expression.NewUnresolvedColumn("foo")),
					expression.NewLiteral(int64(5), sql.Int64),
				),
				plan.NewGroupBy(
					[]sql.Expression{
						expression.NewAlias(aggregation.NewAvg(expression.NewGetFieldWithTable(0, sql.Int64, "t", "foo", false)), "x"),
						expression.NewGetField(0, sql.Int64, "foo", false),
					},
					[]sql.Expression{expression.NewGetField(0, sql.Int64, "foo", false)},
					plan.NewResolvedTable(mem.NewTable("t", nil)),
				),
			),
			plan.NewHaving(
				expression.NewGreaterThan(
					expression.NewGetField(0, sql.Float64, "x", true),
					expression.NewLiteral(int64(5), sql.Int64),
				),
				plan.NewGroupBy(
					[]sql.Expression{
						expression.NewAlias(aggregation.NewAvg(expression.NewGetFieldWithTable(0, sql.Int64, "t", "foo", false)), "x"),
						expression.NewGetField(0, sql.Int64, "foo", false),
					},
					[]sql.Expression{expression.NewGetField(0, sql.Int64, "foo", false)},
					plan.NewResolvedTable(mem.NewTable("t", nil)),
				),
			),
			nil,
		},
		{
			"push down aggregation to group by",
			plan.NewHaving(
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
			),
			plan.NewProject(
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
			),
			nil,
		},
		{
			"push up missing column",
			plan.NewHaving(
				expression.NewGreaterThan(
					expression.NewUnresolvedColumn("i"),
					expression.NewLiteral(int64(5), sql.Int64),
				),
				plan.NewGroupBy(
					[]sql.Expression{
						expression.NewGetFieldWithTable(1, sql.Int64, "t", "foo", false),
					},
					[]sql.Expression{expression.NewGetFieldWithTable(1, sql.Int64, "t", "foo", false)},
					plan.NewResolvedTable(mem.NewTable("t", sql.Schema{
						{Type: sql.Int64, Name: "i", Source: "t"},
						{Type: sql.Int64, Name: "i", Source: "foo"},
					})),
				),
			),
			plan.NewProject(
				[]sql.Expression{
					expression.NewGetFieldWithTable(0, sql.Int64, "t", "foo", false),
				},
				plan.NewHaving(
					expression.NewGreaterThan(
						expression.NewUnresolvedColumn("i"),
						expression.NewLiteral(int64(5), sql.Int64),
					),
					plan.NewGroupBy(
						[]sql.Expression{
							expression.NewGetFieldWithTable(1, sql.Int64, "t", "foo", false),
							expression.NewGetFieldWithTable(0, sql.Int64, "t", "i", false),
						},
						[]sql.Expression{expression.NewGetFieldWithTable(1, sql.Int64, "t", "foo", false)},
						plan.NewResolvedTable(mem.NewTable("t", sql.Schema{
							{Type: sql.Int64, Name: "i", Source: "t"},
							{Type: sql.Int64, Name: "i", Source: "foo"},
						})),
					),
				),
			),
			nil,
		},
		{
			"push up missing column with nodes in between",
			plan.NewHaving(
				expression.NewGreaterThan(
					expression.NewUnresolvedColumn("i"),
					expression.NewLiteral(int64(5), sql.Int64),
				),
				plan.NewProject(
					[]sql.Expression{
						expression.NewGetFieldWithTable(0, sql.Int64, "t", "foo", false),
					},
					plan.NewGroupBy(
						[]sql.Expression{
							expression.NewGetFieldWithTable(1, sql.Int64, "t", "foo", false),
						},
						[]sql.Expression{expression.NewGetFieldWithTable(1, sql.Int64, "t", "foo", false)},
						plan.NewResolvedTable(mem.NewTable("t", sql.Schema{
							{Type: sql.Int64, Name: "i", Source: "t"},
							{Type: sql.Int64, Name: "i", Source: "foo"},
						})),
					),
				),
			),
			plan.NewProject(
				[]sql.Expression{
					expression.NewGetFieldWithTable(0, sql.Int64, "t", "foo", false),
				},
				plan.NewHaving(
					expression.NewGreaterThan(
						expression.NewUnresolvedColumn("i"),
						expression.NewLiteral(int64(5), sql.Int64),
					),
					plan.NewProject(
						[]sql.Expression{
							expression.NewGetFieldWithTable(0, sql.Int64, "t", "foo", false),
							expression.NewGetFieldWithTable(1, sql.Int64, "t", "i", false),
						},
						plan.NewGroupBy(
							[]sql.Expression{
								expression.NewGetFieldWithTable(1, sql.Int64, "t", "foo", false),
								expression.NewGetFieldWithTable(0, sql.Int64, "t", "i", false),
							},
							[]sql.Expression{expression.NewGetFieldWithTable(1, sql.Int64, "t", "foo", false)},
							plan.NewResolvedTable(mem.NewTable("t", sql.Schema{
								{Type: sql.Int64, Name: "i", Source: "t"},
								{Type: sql.Int64, Name: "i", Source: "foo"},
							})),
						),
					),
				),
			),
			nil,
		},
		{
			"push down aggregations with nodes in between",
			plan.NewHaving(
				expression.NewGreaterThan(
					aggregation.NewCount(expression.NewStar()),
					expression.NewLiteral(int64(5), sql.Int64),
				),
				plan.NewProject(
					[]sql.Expression{
						expression.NewAlias(expression.NewGetField(0, sql.Float64, "avg(foo)", false), "x"),
						expression.NewGetFieldWithTable(1, sql.Int64, "t", "foo", false),
					},
					plan.NewGroupBy(
						[]sql.Expression{
							aggregation.NewAvg(expression.NewGetField(0, sql.Int64, "foo", false)),
							expression.NewGetFieldWithTable(0, sql.Int64, "t", "foo", false),
						},
						[]sql.Expression{expression.NewGetField(0, sql.Int64, "foo", false)},
						plan.NewResolvedTable(mem.NewTable("t", nil)),
					),
				),
			),
			plan.NewProject(
				[]sql.Expression{
					expression.NewGetField(0, sql.Float64, "x", false),
					expression.NewGetFieldWithTable(1, sql.Int64, "t", "foo", false),
				},
				plan.NewHaving(
					expression.NewGreaterThan(
						expression.NewGetField(2, sql.Int64, "COUNT(*)", false),
						expression.NewLiteral(int64(5), sql.Int64),
					),
					plan.NewProject(
						[]sql.Expression{
							expression.NewAlias(expression.NewGetField(0, sql.Float64, "avg(foo)", false), "x"),
							expression.NewGetFieldWithTable(1, sql.Int64, "t", "foo", false),
							expression.NewGetField(2, sql.Int64, "COUNT(*)", false),
						},
						plan.NewGroupBy(
							[]sql.Expression{
								aggregation.NewAvg(expression.NewGetField(0, sql.Int64, "foo", false)),
								expression.NewGetFieldWithTable(0, sql.Int64, "t", "foo", false),
								aggregation.NewCount(expression.NewStar()),
							},
							[]sql.Expression{expression.NewGetField(0, sql.Int64, "foo", false)},
							plan.NewResolvedTable(mem.NewTable("t", nil)),
						),
					),
				),
			),
			nil,
		},
		{
			"replace existing aggregation in group by with nodes in between",
			plan.NewHaving(
				expression.NewGreaterThan(
					aggregation.NewAvg(expression.NewUnresolvedColumn("foo")),
					expression.NewLiteral(int64(5), sql.Int64),
				),
				plan.NewProject(
					[]sql.Expression{
						expression.NewGetField(0, sql.Float64, "x", false),
						expression.NewGetField(1, sql.Int64, "foo", false),
					},
					plan.NewGroupBy(
						[]sql.Expression{
							expression.NewAlias(aggregation.NewAvg(expression.NewGetFieldWithTable(0, sql.Int64, "t", "foo", false)), "x"),
							expression.NewGetField(0, sql.Int64, "foo", false),
						},
						[]sql.Expression{expression.NewGetField(0, sql.Int64, "foo", false)},
						plan.NewResolvedTable(mem.NewTable("t", nil)),
					),
				),
			),
			plan.NewHaving(
				expression.NewGreaterThan(
					expression.NewGetField(0, sql.Float64, "x", false),
					expression.NewLiteral(int64(5), sql.Int64),
				),
				plan.NewProject(
					[]sql.Expression{
						expression.NewGetField(0, sql.Float64, "x", false),
						expression.NewGetField(1, sql.Int64, "foo", false),
					},
					plan.NewGroupBy(
						[]sql.Expression{
							expression.NewAlias(aggregation.NewAvg(expression.NewGetFieldWithTable(0, sql.Int64, "t", "foo", false)), "x"),
							expression.NewGetField(0, sql.Int64, "foo", false),
						},
						[]sql.Expression{expression.NewGetField(0, sql.Int64, "foo", false)},
						plan.NewResolvedTable(mem.NewTable("t", nil)),
					),
				),
			),
			nil,
		},
		{
			"missing groupby",
			plan.NewHaving(
				expression.NewGreaterThan(
					aggregation.NewCount(expression.NewStar()),
					expression.NewLiteral(int64(5), sql.Int64),
				),
				plan.NewResolvedTable(mem.NewTable("t", nil)),
			),
			nil,
			errHavingNeedsGroupBy,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			result, err := resolveHaving(sql.NewEmptyContext(), nil, tt.input)
			if tt.err != nil {
				require.Error(err)
				require.True(tt.err.Is(err))
			} else {
				require.NoError(err)
				require.Equal(tt.expected, result)
			}
		})
	}
}
