package parse

import (
	"testing"

	"gopkg.in/sqle/sqle.v0/sql"
	"gopkg.in/sqle/sqle.v0/sql/expression"
	"gopkg.in/sqle/sqle.v0/sql/plan"

	"github.com/stretchr/testify/assert"
)

var fixtures = map[string]sql.Node{
	`DESCRIBE TABLE foo;`: plan.NewDescribe(
		plan.NewUnresolvedTable("foo"),
	),
	`SELECT foo, bar FROM foo;`: plan.NewProject(
		[]sql.Expression{
			expression.NewUnresolvedColumn("foo"),
			expression.NewUnresolvedColumn("bar"),
		},
		plan.NewUnresolvedTable("foo"),
	),
	`SELECT foo IS NULL, bar IS NOT NULL FROM foo;`: plan.NewProject(
		[]sql.Expression{
			expression.NewIsNull(expression.NewUnresolvedColumn("foo")),
			expression.NewNot(expression.NewIsNull(expression.NewUnresolvedColumn("bar"))),
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
	`SELECT * FROM foo WHERE foo != 'bar';`: plan.NewProject(
		[]sql.Expression{
			expression.NewStar(),
		},
		plan.NewFilter(
			expression.NewNot(expression.NewEquals(
				expression.NewUnresolvedColumn("foo"),
				expression.NewLiteral("bar", sql.String),
			)),
			plan.NewUnresolvedTable("foo"),
		),
	),
	`SELECT foo, bar FROM foo LIMIT 10;`: plan.NewLimit(int64(10),
		plan.NewProject(
			[]sql.Expression{
				expression.NewUnresolvedColumn("foo"),
				expression.NewUnresolvedColumn("bar"),
			},
			plan.NewUnresolvedTable("foo"),
		),
	),
	`SELECT foo, bar FROM foo ORDER BY baz DESC;`: plan.NewProject(
		[]sql.Expression{
			expression.NewUnresolvedColumn("foo"),
			expression.NewUnresolvedColumn("bar"),
		},
		plan.NewSort(
			[]plan.SortField{{Column: expression.NewUnresolvedColumn("baz"), Order: plan.Descending, NullOrdering: plan.NullsFirst}},
			plan.NewUnresolvedTable("foo"),
		),
	),
	`SELECT foo, bar FROM foo WHERE foo = bar LIMIT 10;`: plan.NewLimit(int64(10),
		plan.NewProject(
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
	),
	`SELECT foo, bar FROM foo ORDER BY baz DESC LIMIT 1;`: plan.NewLimit(int64(1),
		plan.NewProject(
			[]sql.Expression{
				expression.NewUnresolvedColumn("foo"),
				expression.NewUnresolvedColumn("bar"),
			},
			plan.NewSort(
				[]plan.SortField{{Column: expression.NewUnresolvedColumn("baz"), Order: plan.Descending, NullOrdering: plan.NullsFirst}},
				plan.NewUnresolvedTable("foo"),
			),
		),
	),
	`SELECT foo, bar FROM foo WHERE qux = 1 ORDER BY baz DESC LIMIT 1;`: plan.NewLimit(int64(1),
		plan.NewProject(
			[]sql.Expression{
				expression.NewUnresolvedColumn("foo"),
				expression.NewUnresolvedColumn("bar"),
			},
			plan.NewSort(
				[]plan.SortField{{Column: expression.NewUnresolvedColumn("baz"), Order: plan.Descending, NullOrdering: plan.NullsFirst}},
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
	`SELECT foo, bar FROM t1, t2;`: plan.NewProject(
		[]sql.Expression{
			expression.NewUnresolvedColumn("foo"),
			expression.NewUnresolvedColumn("bar"),
		},
		plan.NewCrossJoin(
			plan.NewUnresolvedTable("t1"),
			plan.NewUnresolvedTable("t2"),
		),
	),
	`SELECT foo, bar FROM t1 GROUP BY foo, bar;`: plan.NewGroupBy(
		[]sql.Expression{
			expression.NewUnresolvedColumn("foo"),
			expression.NewUnresolvedColumn("bar"),
		},
		[]sql.Expression{
			expression.NewUnresolvedColumn("foo"),
			expression.NewUnresolvedColumn("bar"),
		},
		plan.NewUnresolvedTable("t1"),
	),
	`SELECT COUNT(*) FROM t1;`: plan.NewGroupBy(
		[]sql.Expression{
			expression.NewUnresolvedFunction("count", true,
				expression.NewStar()),
		},
		[]sql.Expression{},
		plan.NewUnresolvedTable("t1"),
	),
	`SELECT a FROM t1 where a regexp '.*test.*';`: plan.NewProject(
		[]sql.Expression{
			expression.NewUnresolvedColumn("a"),
		},
		plan.NewFilter(
			expression.NewRegexp(
				expression.NewUnresolvedColumn("a"),
				expression.NewLiteral(".*test.*", sql.String),
			),
			plan.NewUnresolvedTable("t1"),
		),
	),
	`INSERT INTO t1 (col1, col2) VALUES ('a', 1)`: plan.NewInsertInto(
		plan.NewUnresolvedTable("t1"),
		plan.NewValues([][]sql.Expression{{
			expression.NewLiteral("a", sql.String),
			expression.NewLiteral(int64(1), sql.BigInteger),
		}}),
		[]string{"col1", "col2"},
	),
}

func TestParse(t *testing.T) {
	for query, expectedPlan := range fixtures {
		t.Run(query, func(t *testing.T) {
			assert := assert.New(t)
			p, err := Parse(query)
			assert.Nil(err, "error for query '%s'", query)
			assert.Exactly(expectedPlan, p,
				"plans do not match for query '%s'", query)
		})

	}
}
