package parse

import (
	"testing"

	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
	"gopkg.in/src-d/go-mysql-server.v0/sql/plan"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

var fixtures = map[string]sql.Node{
	`CREATE TABLE t1(a INTEGER, b TEXT, c DATE, d TIMESTAMP, e VARCHAR(20), f BLOB NOT NULL)`: plan.NewCreateTable(
		&sql.UnresolvedDatabase{},
		"t1",
		sql.Schema{{
			Name:     "a",
			Type:     sql.Int32,
			Nullable: true,
		}, {
			Name:     "b",
			Type:     sql.Text,
			Nullable: true,
		}, {
			Name:     "c",
			Type:     sql.Date,
			Nullable: true,
		}, {
			Name:     "d",
			Type:     sql.Timestamp,
			Nullable: true,
		}, {
			Name:     "e",
			Type:     sql.Text,
			Nullable: true,
		}, {
			Name:     "f",
			Type:     sql.Blob,
			Nullable: false,
		}},
	),
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
				expression.NewLiteral("bar", sql.Text),
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
				expression.NewLiteral("bar", sql.Text),
			)),
			plan.NewUnresolvedTable("foo"),
		),
	),
	`SELECT foo, bar FROM foo LIMIT 10;`: plan.NewLimit(10,
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
	`SELECT foo, bar FROM foo WHERE foo = bar LIMIT 10;`: plan.NewLimit(10,
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
	`SELECT foo, bar FROM foo ORDER BY baz DESC LIMIT 1;`: plan.NewLimit(1,
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
	`SELECT foo, bar FROM foo WHERE qux = 1 ORDER BY baz DESC LIMIT 1;`: plan.NewLimit(1,
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
						expression.NewLiteral(int64(1), sql.Int64),
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
				expression.NewLiteral(".*test.*", sql.Text),
			),
			plan.NewUnresolvedTable("t1"),
		),
	),
	`SELECT a FROM t1 where a not regexp '.*test.*';`: plan.NewProject(
		[]sql.Expression{
			expression.NewUnresolvedColumn("a"),
		},
		plan.NewFilter(
			expression.NewNot(
				expression.NewRegexp(
					expression.NewUnresolvedColumn("a"),
					expression.NewLiteral(".*test.*", sql.Text),
				),
			),
			plan.NewUnresolvedTable("t1"),
		),
	),
	`INSERT INTO t1 (col1, col2) VALUES ('a', 1)`: plan.NewInsertInto(
		plan.NewUnresolvedTable("t1"),
		plan.NewValues([][]sql.Expression{{
			expression.NewLiteral("a", sql.Text),
			expression.NewLiteral(int64(1), sql.Int64),
		}}),
		[]string{"col1", "col2"},
	),
	`SHOW TABLES`: plan.NewShowTables(&sql.UnresolvedDatabase{}),
	`SELECT DISTINCT foo, bar FROM foo;`: plan.NewDistinct(
		plan.NewProject(
			[]sql.Expression{
				expression.NewUnresolvedColumn("foo"),
				expression.NewUnresolvedColumn("bar"),
			},
			plan.NewUnresolvedTable("foo"),
		),
	),
	`SELECT * FROM foo`: plan.NewProject(
		[]sql.Expression{
			expression.NewStar(),
		},
		plan.NewUnresolvedTable("foo"),
	),
	`SELECT foo, bar FROM foo LIMIT 2 OFFSET 5;`: plan.NewOffset(5,
		plan.NewLimit(2, plan.NewProject(
			[]sql.Expression{
				expression.NewUnresolvedColumn("foo"),
				expression.NewUnresolvedColumn("bar"),
			},
			plan.NewUnresolvedTable("foo"),
		)),
	),
	`SELECT * FROM foo WHERE (a = 1)`: plan.NewProject(
		[]sql.Expression{
			expression.NewStar(),
		},
		plan.NewFilter(
			expression.NewEquals(
				expression.NewUnresolvedColumn("a"),
				expression.NewLiteral(int64(1), sql.Int64),
			),
			plan.NewUnresolvedTable("foo"),
		),
	),
	`SELECT * FROM foo, bar, baz, qux`: plan.NewProject(
		[]sql.Expression{expression.NewStar()},
		plan.NewCrossJoin(
			plan.NewCrossJoin(
				plan.NewCrossJoin(
					plan.NewUnresolvedTable("foo"),
					plan.NewUnresolvedTable("bar"),
				),
				plan.NewUnresolvedTable("baz"),
			),
			plan.NewUnresolvedTable("qux"),
		),
	),
	`SELECT * FROM foo WHERE a = b AND c = d`: plan.NewProject(
		[]sql.Expression{expression.NewStar()},
		plan.NewFilter(
			expression.NewAnd(
				expression.NewEquals(
					expression.NewUnresolvedColumn("a"),
					expression.NewUnresolvedColumn("b"),
				),
				expression.NewEquals(
					expression.NewUnresolvedColumn("c"),
					expression.NewUnresolvedColumn("d"),
				),
			),
			plan.NewUnresolvedTable("foo"),
		),
	),
	`SELECT * FROM foo WHERE a = b OR c = d`: plan.NewProject(
		[]sql.Expression{expression.NewStar()},
		plan.NewFilter(
			expression.NewOr(
				expression.NewEquals(
					expression.NewUnresolvedColumn("a"),
					expression.NewUnresolvedColumn("b"),
				),
				expression.NewEquals(
					expression.NewUnresolvedColumn("c"),
					expression.NewUnresolvedColumn("d"),
				),
			),
			plan.NewUnresolvedTable("foo"),
		),
	),
	`SELECT * FROM foo as bar`: plan.NewProject(
		[]sql.Expression{expression.NewStar()},
		plan.NewTableAlias(
			"bar",
			plan.NewUnresolvedTable("foo"),
		),
	),
	`SELECT * FROM (SELECT * FROM foo) AS bar`: plan.NewProject(
		[]sql.Expression{expression.NewStar()},
		plan.NewSubqueryAlias(
			"bar",
			plan.NewProject(
				[]sql.Expression{expression.NewStar()},
				plan.NewUnresolvedTable("foo"),
			),
		),
	),
	`SELECT * FROM foo WHERE 1 NOT BETWEEN 2 AND 5`: plan.NewProject(
		[]sql.Expression{expression.NewStar()},
		plan.NewFilter(
			expression.NewNot(
				expression.NewBetween(
					expression.NewLiteral(int64(1), sql.Int64),
					expression.NewLiteral(int64(2), sql.Int64),
					expression.NewLiteral(int64(5), sql.Int64),
				),
			),
			plan.NewUnresolvedTable("foo"),
		),
	),
	`SELECT * FROM foo WHERE 1 BETWEEN 2 AND 5`: plan.NewProject(
		[]sql.Expression{expression.NewStar()},
		plan.NewFilter(
			expression.NewBetween(
				expression.NewLiteral(int64(1), sql.Int64),
				expression.NewLiteral(int64(2), sql.Int64),
				expression.NewLiteral(int64(5), sql.Int64),
			),
			plan.NewUnresolvedTable("foo"),
		),
	),
	`SELECT 0x01AF`: plan.NewProject(
		[]sql.Expression{
			expression.NewLiteral(int64(431), sql.Int64),
		},
		plan.NewUnresolvedTable("dual"),
	),
	`SELECT X'41'`: plan.NewProject(
		[]sql.Expression{
			expression.NewLiteral([]byte{'A'}, sql.Blob),
		},
		plan.NewUnresolvedTable("dual"),
	),
	`SELECT * FROM b WHERE SOMEFUNC((1, 2), (3, 4))`: plan.NewProject(
		[]sql.Expression{expression.NewStar()},
		plan.NewFilter(
			expression.NewUnresolvedFunction(
				"somefunc",
				false,
				expression.NewTuple(
					expression.NewLiteral(int64(1), sql.Int64),
					expression.NewLiteral(int64(2), sql.Int64),
				),
				expression.NewTuple(
					expression.NewLiteral(int64(3), sql.Int64),
					expression.NewLiteral(int64(4), sql.Int64),
				),
			),
			plan.NewUnresolvedTable("b"),
		),
	),
	`SELECT * FROM foo WHERE :foo_id = 2`: plan.NewProject(
		[]sql.Expression{expression.NewStar()},
		plan.NewFilter(
			expression.NewEquals(
				expression.NewLiteral(":foo_id", sql.Text),
				expression.NewLiteral(int64(2), sql.Int64),
			),
			plan.NewUnresolvedTable("foo"),
		),
	),
	`SELECT * FROM foo INNER JOIN bar ON a = b`: plan.NewProject(
		[]sql.Expression{expression.NewStar()},
		plan.NewInnerJoin(
			plan.NewUnresolvedTable("foo"),
			plan.NewUnresolvedTable("bar"),
			expression.NewEquals(
				expression.NewUnresolvedColumn("a"),
				expression.NewUnresolvedColumn("b"),
			),
		),
	),
	`SELECT foo.a FROM foo`: plan.NewProject(
		[]sql.Expression{
			expression.NewUnresolvedQualifiedColumn("foo", "a"),
		},
		plan.NewUnresolvedTable("foo"),
	),
	`SELECT CAST(-3 AS UNSIGNED) FROM foo`: plan.NewProject(
		[]sql.Expression{
			expression.NewConvert(expression.NewLiteral(int64(-3), sql.Int64), expression.ConvertToUnsigned),
		},
		plan.NewUnresolvedTable("foo"),
	),
	`SELECT 2 = 2 FROM foo`: plan.NewProject(
		[]sql.Expression{
			expression.NewEquals(expression.NewLiteral(int64(2), sql.Int64), expression.NewLiteral(int64(2), sql.Int64)),
		},
		plan.NewUnresolvedTable("foo"),
	),
	`SELECT *, bar FROM foo`: plan.NewProject(
		[]sql.Expression{
			expression.NewStar(),
			expression.NewUnresolvedColumn("bar"),
		},
		plan.NewUnresolvedTable("foo"),
	),
	`SELECT *, foo.* FROM foo`: plan.NewProject(
		[]sql.Expression{
			expression.NewStar(),
			expression.NewQualifiedStar("foo"),
		},
		plan.NewUnresolvedTable("foo"),
	),
	`SELECT bar, foo.* FROM foo`: plan.NewProject(
		[]sql.Expression{
			expression.NewUnresolvedColumn("bar"),
			expression.NewQualifiedStar("foo"),
		},
		plan.NewUnresolvedTable("foo"),
	),
	`SELECT bar, *, foo.* FROM foo`: plan.NewProject(
		[]sql.Expression{
			expression.NewUnresolvedColumn("bar"),
			expression.NewStar(),
			expression.NewQualifiedStar("foo"),
		},
		plan.NewUnresolvedTable("foo"),
	),
	`SELECT *, * FROM foo`: plan.NewProject(
		[]sql.Expression{
			expression.NewStar(),
			expression.NewStar(),
		},
		plan.NewUnresolvedTable("foo"),
	),
	`SELECT * FROM foo WHERE 1 IN ('1', 2)`: plan.NewProject(
		[]sql.Expression{expression.NewStar()},
		plan.NewFilter(
			expression.NewIn(
				expression.NewLiteral(int64(1), sql.Int64),
				expression.NewTuple(
					expression.NewLiteral("1", sql.Text),
					expression.NewLiteral(int64(2), sql.Int64),
				),
			),
			plan.NewUnresolvedTable("foo"),
		),
	),
	`SELECT * FROM foo WHERE 1 NOT IN ('1', 2)`: plan.NewProject(
		[]sql.Expression{expression.NewStar()},
		plan.NewFilter(
			expression.NewNotIn(
				expression.NewLiteral(int64(1), sql.Int64),
				expression.NewTuple(
					expression.NewLiteral("1", sql.Text),
					expression.NewLiteral(int64(2), sql.Int64),
				),
			),
			plan.NewUnresolvedTable("foo"),
		),
	),
	`SELECT a, b FROM t ORDER BY 2, 1`: plan.NewProject(
		[]sql.Expression{
			expression.NewUnresolvedColumn("a"),
			expression.NewUnresolvedColumn("b"),
		},
		plan.NewSort(
			[]plan.SortField{
				{
					Column:       expression.NewLiteral(int64(2), sql.Int64),
					Order:        plan.Ascending,
					NullOrdering: plan.NullsFirst,
				},
				{
					Column:       expression.NewLiteral(int64(1), sql.Int64),
					Order:        plan.Ascending,
					NullOrdering: plan.NullsFirst,
				},
			},
			plan.NewUnresolvedTable("t"),
		),
	),
	`SELECT 1 + 1;`: plan.NewProject(
		[]sql.Expression{
			expression.NewPlus(expression.NewLiteral(int64(1), sql.Int64), expression.NewLiteral(int64(1), sql.Int64)),
		},
		plan.NewUnresolvedTable("dual"),
	),
	`SELECT 1 * (2 + 1);`: plan.NewProject(
		[]sql.Expression{
			expression.NewMult(expression.NewLiteral(int64(1), sql.Int64),
				expression.NewPlus(expression.NewLiteral(int64(2), sql.Int64), expression.NewLiteral(int64(1), sql.Int64))),
		},
		plan.NewUnresolvedTable("dual"),
	),
	`SELECT (0 - 1) * (1 | 1);`: plan.NewProject(
		[]sql.Expression{
			expression.NewMult(
				expression.NewMinus(expression.NewLiteral(int64(0), sql.Int64), expression.NewLiteral(int64(1), sql.Int64)),
				expression.NewBitOr(expression.NewLiteral(int64(1), sql.Int64), expression.NewLiteral(int64(1), sql.Int64)),
			),
		},
		plan.NewUnresolvedTable("dual"),
	),
	`SELECT (1 << 3) % (2 div 1);`: plan.NewProject(
		[]sql.Expression{
			expression.NewMod(
				expression.NewShiftLeft(expression.NewLiteral(int64(1), sql.Int64), expression.NewLiteral(int64(3), sql.Int64)),
				expression.NewIntDiv(expression.NewLiteral(int64(2), sql.Int64), expression.NewLiteral(int64(1), sql.Int64))),
		},
		plan.NewUnresolvedTable("dual"),
	),
	`SELECT 1.0 * a + 2.0 * b FROM t;`: plan.NewProject(
		[]sql.Expression{
			expression.NewPlus(
				expression.NewMult(expression.NewLiteral(float64(1.0), sql.Float64), expression.NewUnresolvedColumn("a")),
				expression.NewMult(expression.NewLiteral(float64(2.0), sql.Float64), expression.NewUnresolvedColumn("b")),
			),
		},
		plan.NewUnresolvedTable("t"),
	),
	`SELECT '1.0' + 2;`: plan.NewProject(
		[]sql.Expression{
			expression.NewPlus(
				expression.NewLiteral("1.0", sql.Text), expression.NewLiteral(int64(2), sql.Int64),
			),
		},
		plan.NewUnresolvedTable("dual"),
	),
	`SELECT '1' + '2';`: plan.NewProject(
		[]sql.Expression{
			expression.NewPlus(
				expression.NewLiteral("1", sql.Text), expression.NewLiteral("2", sql.Text),
			),
		},
		plan.NewUnresolvedTable("dual"),
	),
	`CREATE INDEX idx ON foo(fn(bar, baz))`: plan.NewCreateIndex(
		"idx",
		plan.NewUnresolvedTable("foo"),
		[]sql.Expression{expression.NewUnresolvedFunction(
			"fn", false,
			expression.NewUnresolvedColumn("bar"),
			expression.NewUnresolvedColumn("baz"),
		)},
		"",
		make(map[string]string),
	),
	`SELECT * FROM foo NATURAL JOIN bar`: plan.NewProject(
		[]sql.Expression{expression.NewStar()},
		plan.NewNaturalJoin(
			plan.NewUnresolvedTable("foo"),
			plan.NewUnresolvedTable("bar"),
		),
	),
	`SELECT * FROM foo NATURAL JOIN bar NATURAL JOIN baz`: plan.NewProject(
		[]sql.Expression{expression.NewStar()},
		plan.NewNaturalJoin(
			plan.NewNaturalJoin(
				plan.NewUnresolvedTable("foo"),
				plan.NewUnresolvedTable("bar"),
			),
			plan.NewUnresolvedTable("baz"),
		),
	),
	`DROP INDEX foo ON bar`: plan.NewDropIndex(
		"foo",
		plan.NewUnresolvedTable("bar"),
	),
}

func TestParse(t *testing.T) {
	for query, expectedPlan := range fixtures {
		t.Run(query, func(t *testing.T) {
			require := require.New(t)
			ctx := sql.NewEmptyContext()
			p, err := Parse(ctx, query)
			require.Nil(err, "error for query '%s'", query)
			require.Exactly(expectedPlan, p,
				"plans do not match for query '%s'", query)
		})

	}
}

var fixturesErrors = map[string]error{
	`SHOW METHEMONEY`: ErrUnsupportedFeature.New(`SHOW METHEMONEY`),
}

func TestParseErrors(t *testing.T) {
	for query, expectedError := range fixturesErrors {
		t.Run(query, func(t *testing.T) {
			require := require.New(t)
			ctx := sql.NewEmptyContext()
			_, err := Parse(ctx, query)
			require.Error(err)
			require.Equal(expectedError.Error(), err.Error())
		})
	}
}
