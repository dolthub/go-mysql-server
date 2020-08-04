package analyzer

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/liquidata-inc/go-mysql-server/memory"
	"github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/liquidata-inc/go-mysql-server/sql/expression"
	"github.com/liquidata-inc/go-mysql-server/sql/plan"
)

func TestPruneColumns(t *testing.T) {
	rule := getRuleFrom(OnceAfterDefault, "prune_columns")
	a := NewDefault(nil)

	t1 := plan.NewResolvedTable(memory.NewTable("t1", sql.Schema{
		{Name: "foo", Type: sql.Int64, Source: "t1"},
		{Name: "bar", Type: sql.Int64, Source: "t1"},
		{Name: "bax", Type: sql.Int64, Source: "t1"},
	}))

	t2 := plan.NewResolvedTable(memory.NewTable("t2", sql.Schema{
		{Name: "foo", Type: sql.Int64, Source: "t2"},
		{Name: "baz", Type: sql.Int64, Source: "t2"},
		{Name: "bux", Type: sql.Int64, Source: "t2"},
	}))

	testCases := []struct {
		name     string
		input    sql.Node
		expected sql.Node
	}{
		{
			"natural join",
			plan.NewProject(
				[]sql.Expression{
					gf(0, "t1", "foo"),
					gf(1, "", "some_alias"),
				},
				plan.NewProject(
					[]sql.Expression{
						gf(0, "t1", "foo"),
						expression.NewAlias("some_alias", gf(1, "t1", "bar")),
					},
					plan.NewFilter(
						eq(gf(0, "t1", "foo"), gf(4, "t2", "baz")),
						plan.NewProject(
							[]sql.Expression{
								gf(0, "t1", "foo"),
								gf(1, "t1", "bar"),
								gf(2, "t1", "bax"),
								gf(4, "t2", "baz"),
								gf(5, "t2", "bux"),
							},
							plan.NewCrossJoin(t1, t2),
						),
					),
				),
			),

			plan.NewProject(
				[]sql.Expression{
					gf(0, "t1", "foo"),
					gf(1, "", "some_alias"),
				},
				plan.NewProject(
					[]sql.Expression{
						gf(0, "t1", "foo"),
						expression.NewAlias("some_alias", gf(1, "t1", "bar")),
					},
					plan.NewFilter(
						eq(gf(0, "t1", "foo"), gf(2, "t2", "baz")),
						plan.NewProject(
							[]sql.Expression{
								gf(0, "t1", "foo"),
								gf(1, "t1", "bar"),
								gf(4, "t2", "baz"),
							},
							plan.NewCrossJoin(t1, t2),
						),
					),
				),
			),
		},

		{
			"subquery",
			plan.NewProject(
				[]sql.Expression{
					gf(0, "t", "foo"),
					gf(1, "", "some_alias"),
				},
				plan.NewProject(
					[]sql.Expression{
						gf(0, "t", "foo"),
						expression.NewAlias("some_alias", gf(1, "t", "bar")),
					},
					plan.NewFilter(
						eq(gf(0, "t", "foo"), gf(4, "t", "baz")),
						plan.NewSubqueryAlias("t", "",
							plan.NewProject(
								[]sql.Expression{
									gf(0, "t1", "foo"),
									gf(1, "t1", "bar"),
									gf(2, "t1", "bax"),
									gf(4, "t2", "baz"),
									gf(5, "t2", "bux"),
								},
								plan.NewCrossJoin(t1, t2),
							),
						),
					),
				),
			),

			plan.NewProject(
				[]sql.Expression{
					gf(0, "t", "foo"),
					gf(1, "", "some_alias"),
				},
				plan.NewProject(
					[]sql.Expression{
						gf(0, "t", "foo"),
						expression.NewAlias("some_alias", gf(1, "t", "bar")),
					},
					plan.NewFilter(
						eq(gf(0, "t", "foo"), gf(2, "t", "baz")),
						plan.NewSubqueryAlias("t", "",
							plan.NewProject(
								[]sql.Expression{
									gf(0, "t1", "foo"),
									gf(1, "t1", "bar"),
									gf(4, "t2", "baz"),
								},
								plan.NewCrossJoin(t1, t2),
							),
						),
					),
				),
			),
		},

		{
			"group by",
			plan.NewGroupBy(
				[]sql.Expression{
					gf(0, "t1", "foo"),
					gf(1, "", "some_alias"),
				},
				[]sql.Expression{
					gf(0, "t1", "foo"),
					gf(5, "t2", "bux"),
					gf(1, "", "some_alias"),
				},
				plan.NewProject(
					[]sql.Expression{
						gf(0, "t1", "foo"),
						expression.NewAlias("some_alias", gf(1, "t1", "bar")),
						gf(5, "t2", "bux"),
					},
					plan.NewFilter(
						eq(gf(0, "t1", "foo"), gf(4, "t2", "baz")),
						plan.NewProject(
							[]sql.Expression{
								gf(0, "t1", "foo"),
								gf(1, "t1", "bar"),
								gf(2, "t1", "bax"),
								gf(4, "t2", "baz"),
								gf(5, "t2", "bux"),
							},
							plan.NewCrossJoin(t1, t2),
						),
					),
				),
			),

			plan.NewGroupBy(
				[]sql.Expression{
					gf(0, "t1", "foo"),
					gf(1, "", "some_alias"),
				},
				[]sql.Expression{
					gf(0, "t1", "foo"),
					gf(2, "t2", "bux"),
					gf(1, "", "some_alias"),
				},
				plan.NewProject(
					[]sql.Expression{
						gf(0, "t1", "foo"),
						expression.NewAlias("some_alias", gf(1, "t1", "bar")),
						gf(3, "t2", "bux"),
					},
					plan.NewFilter(
						eq(gf(0, "t1", "foo"), gf(2, "t2", "baz")),
						plan.NewProject(
							[]sql.Expression{
								gf(0, "t1", "foo"),
								gf(1, "t1", "bar"),
								gf(4, "t2", "baz"),
								gf(5, "t2", "bux"),
							},
							plan.NewCrossJoin(t1, t2),
						),
					),
				),
			),
		},

		{
			"used inside subquery and not outside",
			plan.NewProject(
				[]sql.Expression{
					gf(0, "sq", "foo"),
				},
				plan.NewSubqueryAlias("sq", "",
					plan.NewProject(
						[]sql.Expression{gf(0, "t1", "foo")},
						plan.NewInnerJoin(
							plan.NewProject(
								[]sql.Expression{
									gf(0, "t1", "foo"),
									gf(1, "t1", "bar"),
									gf(2, "t1", "bax"),
								},
								t1,
							),
							plan.NewProject(
								[]sql.Expression{
									gf(0, "t2", "foo"),
									gf(1, "t2", "baz"),
									gf(2, "t2", "bux"),
								},
								t2,
							),
							expression.NewEquals(
								gf(0, "t1", "foo"),
								gf(3, "t2", "foo"),
							),
						),
					),
				),
			),
			plan.NewProject(
				[]sql.Expression{
					gf(0, "sq", "foo"),
				},
				plan.NewSubqueryAlias("sq", "",
					plan.NewProject(
						[]sql.Expression{gf(0, "t1", "foo")},
						plan.NewInnerJoin(
							plan.NewProject(
								[]sql.Expression{
									gf(0, "t1", "foo"),
								},
								t1,
							),
							plan.NewProject(
								[]sql.Expression{
									gf(0, "t2", "foo"),
								},
								t2,
							),
							expression.NewEquals(
								gf(0, "t1", "foo"),
								gf(1, "t2", "foo"),
							),
						),
					),
				),
			),
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			fmt.Println(sql.DebugString(tt.input))
			fmt.Println(sql.DebugString(tt.expected))
			result, err := rule.Apply(sql.NewEmptyContext(), a, tt.input, nil)
			require.NoError(err)
			require.Equal(tt.expected.Schema(), result.Schema())
			assertNodesEqualWithDiff(t, tt.expected, result)
//			require.Equal(tt.expected, result)
		})
	}
}

func gf(idx int, table, name string) *expression.GetField {
	return expression.NewGetFieldWithTable(idx, sql.Int64, table, name, false)
}
