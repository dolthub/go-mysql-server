package plan

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

func TestApplyBindings(t *testing.T) {
	type tc struct {
		Name     string
		Node     sql.Node
		Bindings map[string]sql.Expression
		Expected sql.Node
	}
	cases := []tc{
		tc {
			"SingleV1",
			NewProject(
		                []sql.Expression{
					expression.NewStar(),
		                },
				NewFilter(
					expression.NewEquals(
						expression.NewUnresolvedColumn("foo"),
						expression.NewBindVar("v1"),
					),
                        		NewUnresolvedTable("t1", ""),
				),
			),
			map[string]sql.Expression{
				"v1": expression.NewLiteral("Four score and seven years ago...", sql.LongText),
			},
			NewProject(
		                []sql.Expression{
					expression.NewStar(),
		                },
				NewFilter(
					expression.NewEquals(
						expression.NewUnresolvedColumn("foo"),
						expression.NewLiteral("Four score and seven years ago...", sql.LongText),
					),
                        		NewUnresolvedTable("t1", ""),
				),
			),
		},
		tc {
			"VarNotBound",
			NewProject(
		                []sql.Expression{
					expression.NewStar(),
		                },
				NewFilter(
					expression.NewEquals(
						expression.NewUnresolvedColumn("foo"),
						expression.NewBindVar("v1"),
					),
                        		NewUnresolvedTable("t1", ""),
				),
			),
			map[string]sql.Expression{
			},
			NewProject(
		                []sql.Expression{
					expression.NewStar(),
		                },
				NewFilter(
					expression.NewEquals(
						expression.NewUnresolvedColumn("foo"),
						expression.NewBindVar("v1"),
					),
                        		NewUnresolvedTable("t1", ""),
				),
			),
		},
		tc {
			"SameVarMultipleTimes",
			NewProject(
		                []sql.Expression{
					expression.NewStar(),
		                },
				NewFilter(
					expression.NewOr(
						expression.NewAnd(
							expression.NewEquals(
								expression.NewUnresolvedColumn("foo"),
								expression.NewBindVar("strvar"),
							),
							expression.NewEquals(
								expression.NewUnresolvedColumn("bar"),
								expression.NewBindVar("strvar"),
							),
						),
						expression.NewLessThan(
							expression.NewUnresolvedColumn("icol"),
							expression.NewBindVar("intvar"),
						),
					),
                        		NewUnresolvedTable("t1", ""),
				),
			),
			map[string]sql.Expression{
				"strvar": expression.NewLiteral("Four score and seven years ago...", sql.LongText),
				"intvar": expression.NewLiteral(int8(10), sql.Int8),
			},
			NewProject(
		                []sql.Expression{
					expression.NewStar(),
		                },
				NewFilter(
					expression.NewOr(
						expression.NewAnd(
							expression.NewEquals(
								expression.NewUnresolvedColumn("foo"),
								expression.NewLiteral("Four score and seven years ago...", sql.LongText),
							),
							expression.NewEquals(
								expression.NewUnresolvedColumn("bar"),
								expression.NewLiteral("Four score and seven years ago...", sql.LongText),
							),
						),
						expression.NewLessThan(
							expression.NewUnresolvedColumn("icol"),
							expression.NewLiteral(int8(10), sql.Int8),
						),
					),
                        		NewUnresolvedTable("t1", ""),
				),
			),
		},
	}

	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			res, err := ApplyBindings(c.Node, c.Bindings)
			if assert.NoError(t, err) {
				assert.Equal(t, res, c.Expected)
			}
		})
	}
}
