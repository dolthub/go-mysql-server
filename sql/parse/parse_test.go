package parse

import (
	"testing"

	"github.com/gitql/gitql/sql"
	"github.com/gitql/gitql/sql/expression"
	"github.com/gitql/gitql/sql/plan"

	"github.com/stretchr/testify/assert"
)

var fixtures = map[string]sql.Node{
	`SELECT foo, bar FROM foo;`: plan.NewProject(
		[]sql.Expression{
			expression.NewUnresolvedColumn("foo"),
			expression.NewUnresolvedColumn("bar"),
		},
		plan.NewUnresolvedTable("foo"),
	),
	`SELECT foo AS bar FROM foo;`: plan.NewProject(
		[]sql.Expression{
			expression.NewAlias(
				expression.NewUnresolvedColumn("foo"),
				"bar",
			),
		},
		plan.NewUnresolvedTable("foo"),
	),
	`SELECT foo, bar FROM foo WHERE foo = bar;`: plan.NewProject(
		[]sql.Expression{
			expression.NewUnresolvedColumn("foo"),
			expression.NewUnresolvedColumn("bar"),
		},
		plan.NewFilter(
			expression.NewEquals(
				expression.NewUnresolvedColumn("foo"),
				expression.NewUnresolvedColumn("bar"),
			),
			plan.NewUnresolvedTable("foo"),
		),
	),
	`SELECT foo, bar FROM foo WHERE foo = 'bar';`: plan.NewProject(
		[]sql.Expression{
			expression.NewUnresolvedColumn("foo"),
			expression.NewUnresolvedColumn("bar"),
		},
		plan.NewFilter(
			expression.NewEquals(
				expression.NewUnresolvedColumn("foo"),
				expression.NewLiteral("bar", sql.String),
			),
			plan.NewUnresolvedTable("foo"),
		),
	),
	`SELECT foo, bar FROM foo LIMIT 10;`: plan.NewProject(
		[]sql.Expression{
			expression.NewUnresolvedColumn("foo"),
			expression.NewUnresolvedColumn("bar"),
		},
		plan.NewLimit(int64(10),
			plan.NewUnresolvedTable("foo"),
		),
	),
	`SELECT foo, bar FROM foo ORDER BY baz DESC;`: plan.NewProject(
		[]sql.Expression{
			expression.NewUnresolvedColumn("foo"),
			expression.NewUnresolvedColumn("bar"),
		},
		plan.NewSort(
			[]plan.SortField{{expression.NewUnresolvedColumn("baz"), plan.Descending}},
			plan.NewUnresolvedTable("foo"),
		),
	),
	`SELECT foo, bar FROM foo WHERE foo = bar LIMIT 10;`: plan.NewProject(
		[]sql.Expression{
			expression.NewUnresolvedColumn("foo"),
			expression.NewUnresolvedColumn("bar"),
		},
		plan.NewLimit(int64(10),
			plan.NewFilter(
				expression.NewEquals(
					expression.NewUnresolvedColumn("foo"),
					expression.NewUnresolvedColumn("bar"),
				),
				plan.NewUnresolvedTable("foo"),
			),
		),
	),
	`SELECT foo, bar FROM foo ORDER BY baz DESC LIMIT 1;`: plan.NewProject(
		[]sql.Expression{
			expression.NewUnresolvedColumn("foo"),
			expression.NewUnresolvedColumn("bar"),
		},
		plan.NewLimit(int64(1),
			plan.NewSort(
				[]plan.SortField{{expression.NewUnresolvedColumn("baz"), plan.Descending}},
				plan.NewUnresolvedTable("foo"),
			),
		),
	),
	`SELECT foo, bar FROM foo WHERE qux = 1 ORDER BY baz DESC LIMIT 1;`: plan.NewProject(
		[]sql.Expression{
			expression.NewUnresolvedColumn("foo"),
			expression.NewUnresolvedColumn("bar"),
		},
		plan.NewLimit(int64(1),
			plan.NewSort(
				[]plan.SortField{{expression.NewUnresolvedColumn("baz"), plan.Descending}},
				plan.NewFilter(
					expression.NewEquals(
						expression.NewUnresolvedColumn("qux"),
						expression.NewLiteral(int64(1), sql.BigInteger),
					),
					plan.NewUnresolvedTable("foo"),
				),
			),
		),
	),
}

func TestParse(t *testing.T) {
	assert := assert.New(t)
	for query, expectedPlan := range fixtures {
		p, err := Parse(query)
		assert.Nil(err, "error for query '%s'", query)
		assert.Exactly(expectedPlan, p,
			"plans do not match for query '%s'", query)
	}
}
