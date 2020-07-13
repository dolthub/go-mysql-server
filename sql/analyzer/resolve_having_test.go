package analyzer

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-errors.v1"

	"github.com/liquidata-inc/go-mysql-server/memory"
	"github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/liquidata-inc/go-mysql-server/sql/expression"
	"github.com/liquidata-inc/go-mysql-server/sql/expression/function/aggregation"
	"github.com/liquidata-inc/go-mysql-server/sql/plan"
)

func TestResolveHaving(t *testing.T) {
	testCases := []struct {
		name     string
		input    sql.Node
		expected sql.Node
		err      *errors.Kind
	}{
		{
			name: "replace existing aggregation in group by",
			input: plan.NewHaving(
				expression.NewGreaterThan(
					aggregation.NewAvg(expression.NewUnresolvedColumn("foo")),
					expression.NewLiteral(int64(5), sql.Int64),
				),
				plan.NewGroupBy(
					[]sql.Expression{
						expression.NewAlias("x", aggregation.NewAvg(expression.NewGetFieldWithTable(0, sql.Int64, "t", "foo", false))),
						expression.NewGetField(0, sql.Int64, "foo", false),
					},
					[]sql.Expression{expression.NewGetField(0, sql.Int64, "foo", false)},
					plan.NewResolvedTable(memory.NewTable("t", nil)),
				),
			),
			expected: plan.NewHaving(
				expression.NewGreaterThan(
					expression.NewGetField(0, sql.Float64, "x", true),
					expression.NewLiteral(int64(5), sql.Int64),
				),
				plan.NewGroupBy(
					[]sql.Expression{
						expression.NewAlias("x", aggregation.NewAvg(expression.NewGetFieldWithTable(0, sql.Int64, "t", "foo", false))),
						expression.NewGetField(0, sql.Int64, "foo", false),
					},
					[]sql.Expression{expression.NewGetField(0, sql.Int64, "foo", false)},
					plan.NewResolvedTable(memory.NewTable("t", nil)),
				),
			),
		},
		{
			name: "push down aggregation to group by",
			input: plan.NewHaving(
				expression.NewGreaterThan(
					aggregation.NewCount(expression.NewStar()),
					expression.NewLiteral(int64(5), sql.Int64),
				),
				plan.NewGroupBy(
					[]sql.Expression{
						expression.NewAlias("x", aggregation.NewAvg(expression.NewGetField(0, sql.Int64, "foo", false))),
						expression.NewGetFieldWithTable(0, sql.Int64, "t", "foo", false),
					},
					[]sql.Expression{expression.NewGetField(0, sql.Int64, "foo", false)},
					plan.NewResolvedTable(memory.NewTable("t", nil)),
				),
			),
			expected: plan.NewProject(
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
							expression.NewAlias("x", aggregation.NewAvg(expression.NewGetField(0, sql.Int64, "foo", false))),
							expression.NewGetFieldWithTable(0, sql.Int64, "t", "foo", false),
							aggregation.NewCount(expression.NewStar()),
						},
						[]sql.Expression{expression.NewGetField(0, sql.Int64, "foo", false)},
						plan.NewResolvedTable(memory.NewTable("t", nil)),
					),
				),
			),
		},
		// TODO: this should be an error in most cases -- the having clause must only reference columns in the select clause.
		{
			name: "pull up missing column",
			input: plan.NewHaving(
				expression.NewGreaterThan(
					expression.NewUnresolvedColumn("i"),
					expression.NewLiteral(int64(5), sql.Int64),
				),
				plan.NewGroupBy(
					[]sql.Expression{
						expression.NewGetFieldWithTable(1, sql.Int64, "t", "foo", false),
					},
					[]sql.Expression{expression.NewGetFieldWithTable(1, sql.Int64, "t", "foo", false)},
					plan.NewResolvedTable(memory.NewTable("t", sql.Schema{
						{Type: sql.Int64, Name: "i", Source: "t"},
						{Type: sql.Int64, Name: "foo", Source: "t"},
					})),
				),
			),
			expected: plan.NewProject(
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
						plan.NewResolvedTable(memory.NewTable("t", sql.Schema{
							{Type: sql.Int64, Name: "i", Source: "t"},
							{Type: sql.Int64, Name: "foo", Source: "t"},
						})),
					),
				),
			),
		},
		{
			name: "pull up missing column with nodes in between",
			input: plan.NewHaving(
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
						plan.NewResolvedTable(memory.NewTable("t", sql.Schema{
							{Type: sql.Int64, Name: "i", Source: "t"},
							{Type: sql.Int64, Name: "foo", Source: "t"},
						})),
					),
				),
			),
			expected: plan.NewProject(
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
							plan.NewResolvedTable(memory.NewTable("t", sql.Schema{
								{Type: sql.Int64, Name: "i", Source: "t"},
								{Type: sql.Int64, Name: "foo", Source: "t"},
							})),
						),
					),
				),
			),
		},
		{
			name: "push down aggregations with nodes in between",
			input: plan.NewHaving(
				expression.NewGreaterThan(
					aggregation.NewCount(expression.NewStar()),
					expression.NewLiteral(int64(5), sql.Int64),
				),
				plan.NewProject(
					[]sql.Expression{
						expression.NewAlias("x", expression.NewGetField(0, sql.Float64, "avg(foo)", false)),
						expression.NewGetFieldWithTable(1, sql.Int64, "t", "foo", false),
					},
					plan.NewGroupBy(
						[]sql.Expression{
							aggregation.NewAvg(expression.NewGetField(0, sql.Int64, "foo", false)),
							expression.NewGetFieldWithTable(0, sql.Int64, "t", "foo", false),
						},
						[]sql.Expression{expression.NewGetField(0, sql.Int64, "foo", false)},
						plan.NewResolvedTable(memory.NewTable("t", nil)),
					),
				),
			),
			expected: plan.NewProject(
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
							expression.NewAlias("x", expression.NewGetField(0, sql.Float64, "avg(foo)", false)),
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
							plan.NewResolvedTable(memory.NewTable("t", nil)),
						),
					),
				),
			),
		},
		{
			name: "replace existing aggregation in group by with nodes in between",
			input: plan.NewHaving(
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
							expression.NewAlias("x", aggregation.NewAvg(expression.NewGetFieldWithTable(0, sql.Int64, "t", "foo", false))),
							expression.NewGetField(0, sql.Int64, "foo", false),
						},
						[]sql.Expression{expression.NewGetField(0, sql.Int64, "foo", false)},
						plan.NewResolvedTable(memory.NewTable("t", nil)),
					),
				),
			),
			expected: plan.NewHaving(
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
							expression.NewAlias("x", aggregation.NewAvg(expression.NewGetFieldWithTable(0, sql.Int64, "t", "foo", false))),
							expression.NewGetField(0, sql.Int64, "foo", false),
						},
						[]sql.Expression{expression.NewGetField(0, sql.Int64, "foo", false)},
						plan.NewResolvedTable(memory.NewTable("t", nil)),
					),
				),
			),
		},
		{
			name: "missing groupby",
			input: plan.NewHaving(
				expression.NewGreaterThan(
					aggregation.NewCount(expression.NewStar()),
					expression.NewLiteral(int64(5), sql.Int64),
				),
				plan.NewResolvedTable(memory.NewTable("t", nil)),
			),
			err: errHavingNeedsGroupBy,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			result, err := resolveHaving(sql.NewEmptyContext(), nil, tt.input, nil)
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
