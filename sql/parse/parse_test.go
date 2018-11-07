package parse

import (
	"testing"

	errors "gopkg.in/src-d/go-errors.v1"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
	"gopkg.in/src-d/go-mysql-server.v0/sql/plan"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

var fixtures = map[string]sql.Node{
	`CREATE TABLE t1(a INTEGER, b TEXT, c DATE, d TIMESTAMP, e VARCHAR(20), f BLOB NOT NULL)`: plan.NewCreateTable(
		sql.UnresolvedDatabase(""),
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
		plan.NewUnresolvedTable("foo", ""),
	),
	`DESC TABLE foo;`: plan.NewDescribe(
		plan.NewUnresolvedTable("foo", ""),
	),
	`SELECT foo, bar FROM foo;`: plan.NewProject(
		[]sql.Expression{
			expression.NewUnresolvedColumn("foo"),
			expression.NewUnresolvedColumn("bar"),
		},
		plan.NewUnresolvedTable("foo", ""),
	),
	`SELECT foo IS NULL, bar IS NOT NULL FROM foo;`: plan.NewProject(
		[]sql.Expression{
			expression.NewIsNull(expression.NewUnresolvedColumn("foo")),
			expression.NewNot(expression.NewIsNull(expression.NewUnresolvedColumn("bar"))),
		},
		plan.NewUnresolvedTable("foo", ""),
	),
	`SELECT foo AS bar FROM foo;`: plan.NewProject(
		[]sql.Expression{
			expression.NewAlias(
				expression.NewUnresolvedColumn("foo"),
				"bar",
			),
		},
		plan.NewUnresolvedTable("foo", ""),
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
			plan.NewUnresolvedTable("foo", ""),
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
			plan.NewUnresolvedTable("foo", ""),
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
			plan.NewUnresolvedTable("foo", ""),
		),
	),
	`SELECT foo, bar FROM foo LIMIT 10;`: plan.NewLimit(10,
		plan.NewProject(
			[]sql.Expression{
				expression.NewUnresolvedColumn("foo"),
				expression.NewUnresolvedColumn("bar"),
			},
			plan.NewUnresolvedTable("foo", ""),
		),
	),
	`SELECT foo, bar FROM foo ORDER BY baz DESC;`: plan.NewSort(
		[]plan.SortField{{Column: expression.NewUnresolvedColumn("baz"), Order: plan.Descending, NullOrdering: plan.NullsFirst}},
		plan.NewProject(
			[]sql.Expression{
				expression.NewUnresolvedColumn("foo"),
				expression.NewUnresolvedColumn("bar"),
			},
			plan.NewUnresolvedTable("foo", ""),
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
				plan.NewUnresolvedTable("foo", ""),
			),
		),
	),
	`SELECT foo, bar FROM foo ORDER BY baz DESC LIMIT 1;`: plan.NewLimit(1,
		plan.NewSort(
			[]plan.SortField{{Column: expression.NewUnresolvedColumn("baz"), Order: plan.Descending, NullOrdering: plan.NullsFirst}},
			plan.NewProject(
				[]sql.Expression{
					expression.NewUnresolvedColumn("foo"),
					expression.NewUnresolvedColumn("bar"),
				},
				plan.NewUnresolvedTable("foo", ""),
			),
		),
	),
	`SELECT foo, bar FROM foo WHERE qux = 1 ORDER BY baz DESC LIMIT 1;`: plan.NewLimit(1,
		plan.NewSort(
			[]plan.SortField{{Column: expression.NewUnresolvedColumn("baz"), Order: plan.Descending, NullOrdering: plan.NullsFirst}},
			plan.NewProject(
				[]sql.Expression{
					expression.NewUnresolvedColumn("foo"),
					expression.NewUnresolvedColumn("bar"),
				},
				plan.NewFilter(
					expression.NewEquals(
						expression.NewUnresolvedColumn("qux"),
						expression.NewLiteral(int64(1), sql.Int64),
					),
					plan.NewUnresolvedTable("foo", ""),
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
			plan.NewUnresolvedTable("t1", ""),
			plan.NewUnresolvedTable("t2", ""),
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
		plan.NewUnresolvedTable("t1", ""),
	),
	`SELECT foo, bar FROM t1 GROUP BY 1, 2;`: plan.NewGroupBy(
		[]sql.Expression{
			expression.NewUnresolvedColumn("foo"),
			expression.NewUnresolvedColumn("bar"),
		},
		[]sql.Expression{
			expression.NewUnresolvedColumn("foo"),
			expression.NewUnresolvedColumn("bar"),
		},
		plan.NewUnresolvedTable("t1", ""),
	),
	`SELECT COUNT(*) FROM t1;`: plan.NewGroupBy(
		[]sql.Expression{
			expression.NewUnresolvedFunction("count", true,
				expression.NewStar()),
		},
		[]sql.Expression{},
		plan.NewUnresolvedTable("t1", ""),
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
			plan.NewUnresolvedTable("t1", ""),
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
			plan.NewUnresolvedTable("t1", ""),
		),
	),
	`INSERT INTO t1 (col1, col2) VALUES ('a', 1)`: plan.NewInsertInto(
		plan.NewUnresolvedTable("t1", ""),
		plan.NewValues([][]sql.Expression{{
			expression.NewLiteral("a", sql.Text),
			expression.NewLiteral(int64(1), sql.Int64),
		}}),
		[]string{"col1", "col2"},
	),
	`SHOW TABLES`: plan.NewShowTables(sql.UnresolvedDatabase("")),
	`SELECT DISTINCT foo, bar FROM foo;`: plan.NewDistinct(
		plan.NewProject(
			[]sql.Expression{
				expression.NewUnresolvedColumn("foo"),
				expression.NewUnresolvedColumn("bar"),
			},
			plan.NewUnresolvedTable("foo", ""),
		),
	),
	`SELECT * FROM foo`: plan.NewProject(
		[]sql.Expression{
			expression.NewStar(),
		},
		plan.NewUnresolvedTable("foo", ""),
	),
	`SELECT foo, bar FROM foo LIMIT 2 OFFSET 5;`: plan.NewOffset(5,
		plan.NewLimit(2, plan.NewProject(
			[]sql.Expression{
				expression.NewUnresolvedColumn("foo"),
				expression.NewUnresolvedColumn("bar"),
			},
			plan.NewUnresolvedTable("foo", ""),
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
			plan.NewUnresolvedTable("foo", ""),
		),
	),
	`SELECT * FROM foo, bar, baz, qux`: plan.NewProject(
		[]sql.Expression{expression.NewStar()},
		plan.NewCrossJoin(
			plan.NewCrossJoin(
				plan.NewCrossJoin(
					plan.NewUnresolvedTable("foo", ""),
					plan.NewUnresolvedTable("bar", ""),
				),
				plan.NewUnresolvedTable("baz", ""),
			),
			plan.NewUnresolvedTable("qux", ""),
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
			plan.NewUnresolvedTable("foo", ""),
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
			plan.NewUnresolvedTable("foo", ""),
		),
	),
	`SELECT * FROM foo as bar`: plan.NewProject(
		[]sql.Expression{expression.NewStar()},
		plan.NewTableAlias(
			"bar",
			plan.NewUnresolvedTable("foo", ""),
		),
	),
	`SELECT * FROM (SELECT * FROM foo) AS bar`: plan.NewProject(
		[]sql.Expression{expression.NewStar()},
		plan.NewSubqueryAlias(
			"bar",
			plan.NewProject(
				[]sql.Expression{expression.NewStar()},
				plan.NewUnresolvedTable("foo", ""),
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
			plan.NewUnresolvedTable("foo", ""),
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
			plan.NewUnresolvedTable("foo", ""),
		),
	),
	`SELECT 0x01AF`: plan.NewProject(
		[]sql.Expression{
			expression.NewLiteral(int64(431), sql.Int64),
		},
		plan.NewUnresolvedTable("dual", ""),
	),
	`SELECT X'41'`: plan.NewProject(
		[]sql.Expression{
			expression.NewLiteral([]byte{'A'}, sql.Blob),
		},
		plan.NewUnresolvedTable("dual", ""),
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
			plan.NewUnresolvedTable("b", ""),
		),
	),
	`SELECT * FROM foo WHERE :foo_id = 2`: plan.NewProject(
		[]sql.Expression{expression.NewStar()},
		plan.NewFilter(
			expression.NewEquals(
				expression.NewLiteral(":foo_id", sql.Text),
				expression.NewLiteral(int64(2), sql.Int64),
			),
			plan.NewUnresolvedTable("foo", ""),
		),
	),
	`SELECT * FROM foo INNER JOIN bar ON a = b`: plan.NewProject(
		[]sql.Expression{expression.NewStar()},
		plan.NewInnerJoin(
			plan.NewUnresolvedTable("foo", ""),
			plan.NewUnresolvedTable("bar", ""),
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
		plan.NewUnresolvedTable("foo", ""),
	),
	`SELECT CAST(-3 AS UNSIGNED) FROM foo`: plan.NewProject(
		[]sql.Expression{
			expression.NewConvert(expression.NewLiteral(int64(-3), sql.Int64), expression.ConvertToUnsigned),
		},
		plan.NewUnresolvedTable("foo", ""),
	),
	`SELECT 2 = 2 FROM foo`: plan.NewProject(
		[]sql.Expression{
			expression.NewEquals(expression.NewLiteral(int64(2), sql.Int64), expression.NewLiteral(int64(2), sql.Int64)),
		},
		plan.NewUnresolvedTable("foo", ""),
	),
	`SELECT *, bar FROM foo`: plan.NewProject(
		[]sql.Expression{
			expression.NewStar(),
			expression.NewUnresolvedColumn("bar"),
		},
		plan.NewUnresolvedTable("foo", ""),
	),
	`SELECT *, foo.* FROM foo`: plan.NewProject(
		[]sql.Expression{
			expression.NewStar(),
			expression.NewQualifiedStar("foo"),
		},
		plan.NewUnresolvedTable("foo", ""),
	),
	`SELECT bar, foo.* FROM foo`: plan.NewProject(
		[]sql.Expression{
			expression.NewUnresolvedColumn("bar"),
			expression.NewQualifiedStar("foo"),
		},
		plan.NewUnresolvedTable("foo", ""),
	),
	`SELECT bar, *, foo.* FROM foo`: plan.NewProject(
		[]sql.Expression{
			expression.NewUnresolvedColumn("bar"),
			expression.NewStar(),
			expression.NewQualifiedStar("foo"),
		},
		plan.NewUnresolvedTable("foo", ""),
	),
	`SELECT *, * FROM foo`: plan.NewProject(
		[]sql.Expression{
			expression.NewStar(),
			expression.NewStar(),
		},
		plan.NewUnresolvedTable("foo", ""),
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
			plan.NewUnresolvedTable("foo", ""),
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
			plan.NewUnresolvedTable("foo", ""),
		),
	),
	`SELECT a, b FROM t ORDER BY 2, 1`: plan.NewSort(
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
		plan.NewProject(
			[]sql.Expression{
				expression.NewUnresolvedColumn("a"),
				expression.NewUnresolvedColumn("b"),
			},
			plan.NewUnresolvedTable("t", ""),
		),
	),
	`SELECT 1 + 1;`: plan.NewProject(
		[]sql.Expression{
			expression.NewPlus(expression.NewLiteral(int64(1), sql.Int64), expression.NewLiteral(int64(1), sql.Int64)),
		},
		plan.NewUnresolvedTable("dual", ""),
	),
	`SELECT 1 * (2 + 1);`: plan.NewProject(
		[]sql.Expression{
			expression.NewMult(expression.NewLiteral(int64(1), sql.Int64),
				expression.NewPlus(expression.NewLiteral(int64(2), sql.Int64), expression.NewLiteral(int64(1), sql.Int64))),
		},
		plan.NewUnresolvedTable("dual", ""),
	),
	`SELECT (0 - 1) * (1 | 1);`: plan.NewProject(
		[]sql.Expression{
			expression.NewMult(
				expression.NewMinus(expression.NewLiteral(int64(0), sql.Int64), expression.NewLiteral(int64(1), sql.Int64)),
				expression.NewBitOr(expression.NewLiteral(int64(1), sql.Int64), expression.NewLiteral(int64(1), sql.Int64)),
			),
		},
		plan.NewUnresolvedTable("dual", ""),
	),
	`SELECT (1 << 3) % (2 div 1);`: plan.NewProject(
		[]sql.Expression{
			expression.NewMod(
				expression.NewShiftLeft(expression.NewLiteral(int64(1), sql.Int64), expression.NewLiteral(int64(3), sql.Int64)),
				expression.NewIntDiv(expression.NewLiteral(int64(2), sql.Int64), expression.NewLiteral(int64(1), sql.Int64))),
		},
		plan.NewUnresolvedTable("dual", ""),
	),
	`SELECT 1.0 * a + 2.0 * b FROM t;`: plan.NewProject(
		[]sql.Expression{
			expression.NewPlus(
				expression.NewMult(expression.NewLiteral(float64(1.0), sql.Float64), expression.NewUnresolvedColumn("a")),
				expression.NewMult(expression.NewLiteral(float64(2.0), sql.Float64), expression.NewUnresolvedColumn("b")),
			),
		},
		plan.NewUnresolvedTable("t", ""),
	),
	`SELECT '1.0' + 2;`: plan.NewProject(
		[]sql.Expression{
			expression.NewPlus(
				expression.NewLiteral("1.0", sql.Text), expression.NewLiteral(int64(2), sql.Int64),
			),
		},
		plan.NewUnresolvedTable("dual", ""),
	),
	`SELECT '1' + '2';`: plan.NewProject(
		[]sql.Expression{
			expression.NewPlus(
				expression.NewLiteral("1", sql.Text), expression.NewLiteral("2", sql.Text),
			),
		},
		plan.NewUnresolvedTable("dual", ""),
	),
	`CREATE INDEX idx ON foo USING bar (fn(bar, baz))`: plan.NewCreateIndex(
		"idx",
		plan.NewUnresolvedTable("foo", ""),
		[]sql.Expression{expression.NewUnresolvedFunction(
			"fn", false,
			expression.NewUnresolvedColumn("bar"),
			expression.NewUnresolvedColumn("baz"),
		)},
		"bar",
		make(map[string]string),
	),
	`      CREATE INDEX idx ON foo USING bar (fn(bar, baz))`: plan.NewCreateIndex(
		"idx",
		plan.NewUnresolvedTable("foo", ""),
		[]sql.Expression{expression.NewUnresolvedFunction(
			"fn", false,
			expression.NewUnresolvedColumn("bar"),
			expression.NewUnresolvedColumn("baz"),
		)},
		"bar",
		make(map[string]string),
	),
	`SELECT * FROM foo NATURAL JOIN bar`: plan.NewProject(
		[]sql.Expression{expression.NewStar()},
		plan.NewNaturalJoin(
			plan.NewUnresolvedTable("foo", ""),
			plan.NewUnresolvedTable("bar", ""),
		),
	),
	`SELECT * FROM foo NATURAL JOIN bar NATURAL JOIN baz`: plan.NewProject(
		[]sql.Expression{expression.NewStar()},
		plan.NewNaturalJoin(
			plan.NewNaturalJoin(
				plan.NewUnresolvedTable("foo", ""),
				plan.NewUnresolvedTable("bar", ""),
			),
			plan.NewUnresolvedTable("baz", ""),
		),
	),
	`DROP INDEX foo ON bar`: plan.NewDropIndex(
		"foo",
		plan.NewUnresolvedTable("bar", ""),
	),
	`DESCRIBE FORMAT=TREE SELECT * FROM foo`: plan.NewDescribeQuery(
		"tree",
		plan.NewProject(
			[]sql.Expression{expression.NewStar()},
			plan.NewUnresolvedTable("foo", ""),
		),
	),
	`SELECT MAX(i)/2 FROM foo`: plan.NewGroupBy(
		[]sql.Expression{
			expression.NewArithmetic(
				expression.NewUnresolvedFunction(
					"max", true, expression.NewUnresolvedColumn("i"),
				),
				expression.NewLiteral(int64(2), sql.Int64),
				"/",
			),
		},
		[]sql.Expression{},
		plan.NewUnresolvedTable("foo", ""),
	),
	`SHOW INDEXES FROM foo`: plan.NewShowIndexes(sql.UnresolvedDatabase(""), "foo", nil),
	`SHOW INDEX FROM foo`:   plan.NewShowIndexes(sql.UnresolvedDatabase(""), "foo", nil),
	`SHOW KEYS FROM foo`:    plan.NewShowIndexes(sql.UnresolvedDatabase(""), "foo", nil),
	`SHOW INDEXES IN foo`:   plan.NewShowIndexes(sql.UnresolvedDatabase(""), "foo", nil),
	`SHOW INDEX IN foo`:     plan.NewShowIndexes(sql.UnresolvedDatabase(""), "foo", nil),
	`SHOW KEYS IN foo`:      plan.NewShowIndexes(sql.UnresolvedDatabase(""), "foo", nil),
	`create index foo on bar using qux (baz)`: plan.NewCreateIndex(
		"foo",
		plan.NewUnresolvedTable("bar", ""),
		[]sql.Expression{expression.NewUnresolvedColumn("baz")},
		"qux",
		make(map[string]string),
	),
	`SHOW FULL PROCESSLIST`: plan.NewShowProcessList(),
	`SHOW PROCESSLIST`:      plan.NewShowProcessList(),
	`SELECT @@allowed_max_packet`: plan.NewProject([]sql.Expression{
		expression.NewUnresolvedColumn("@@allowed_max_packet"),
	}, plan.NewUnresolvedTable("dual", "")),
	`SET autocommit=1, foo="bar"`: plan.NewSet(
		plan.SetVariable{
			Name:  "autocommit",
			Value: expression.NewLiteral(int64(1), sql.Int64),
		},
		plan.SetVariable{
			Name:  "foo",
			Value: expression.NewLiteral("bar", sql.Text),
		},
	),
	`SET @@session.autocommit=1, foo="bar"`: plan.NewSet(
		plan.SetVariable{
			Name:  "@@session.autocommit",
			Value: expression.NewLiteral(int64(1), sql.Int64),
		},
		plan.SetVariable{
			Name:  "foo",
			Value: expression.NewLiteral("bar", sql.Text),
		},
	),
	`SET autocommit=ON, on="1"`: plan.NewSet(
		plan.SetVariable{
			Name:  "autocommit",
			Value: expression.NewLiteral(int64(1), sql.Int64),
		},
		plan.SetVariable{
			Name:  "on",
			Value: expression.NewLiteral("1", sql.Text),
		},
	),
	`SET @@session.autocommit=OFF, off="0"`: plan.NewSet(
		plan.SetVariable{
			Name:  "@@session.autocommit",
			Value: expression.NewLiteral(int64(0), sql.Int64),
		},
		plan.SetVariable{
			Name:  "off",
			Value: expression.NewLiteral("0", sql.Text),
		},
	),
	`SET @@session.autocommit=ON`: plan.NewSet(
		plan.SetVariable{
			Name:  "@@session.autocommit",
			Value: expression.NewLiteral(int64(1), sql.Int64),
		},
	),
	`SET autocommit=off`: plan.NewSet(
		plan.SetVariable{
			Name:  "autocommit",
			Value: expression.NewLiteral(int64(0), sql.Int64),
		},
	),
	`SET autocommit=true`: plan.NewSet(
		plan.SetVariable{
			Name:  "autocommit",
			Value: expression.NewLiteral(true, sql.Boolean),
		},
	),
	`SET autocommit="true"`: plan.NewSet(
		plan.SetVariable{
			Name:  "autocommit",
			Value: expression.NewLiteral(true, sql.Boolean),
		},
	),
	`SET autocommit=false`: plan.NewSet(
		plan.SetVariable{
			Name:  "autocommit",
			Value: expression.NewLiteral(false, sql.Boolean),
		},
	),
	`SET autocommit="false"`: plan.NewSet(
		plan.SetVariable{
			Name:  "autocommit",
			Value: expression.NewLiteral(false, sql.Boolean),
		},
	),
	`SET SESSION NET_READ_TIMEOUT= 700, SESSION NET_WRITE_TIMEOUT= 700`: plan.NewSet(
		plan.SetVariable{
			Name:  "@@session.net_read_timeout",
			Value: expression.NewLiteral(int64(700), sql.Int64),
		},
		plan.SetVariable{
			Name:  "@@session.net_write_timeout",
			Value: expression.NewLiteral(int64(700), sql.Int64),
		},
	),
	`SET gtid_mode=DEFAULT`: plan.NewSet(
		plan.SetVariable{
			Name:  "gtid_mode",
			Value: expression.NewDefaultColumn(""),
		},
	),
	`SET @@sql_select_limit=default`: plan.NewSet(
		plan.SetVariable{
			Name:  "@@sql_select_limit",
			Value: expression.NewDefaultColumn(""),
		},
	),
	`/*!40101 SET NAMES utf8 */`: plan.Nothing,
	`SELECT /*!40101 SET NAMES utf8 */ * FROM foo`: plan.NewProject(
		[]sql.Expression{
			expression.NewStar(),
		},
		plan.NewUnresolvedTable("foo", ""),
	),
	`SHOW DATABASES`: plan.NewShowDatabases(),
	`SELECT * FROM foo WHERE i LIKE 'foo'`: plan.NewProject(
		[]sql.Expression{expression.NewStar()},
		plan.NewFilter(
			expression.NewLike(
				expression.NewUnresolvedColumn("i"),
				expression.NewLiteral("foo", sql.Text),
			),
			plan.NewUnresolvedTable("foo", ""),
		),
	),
	`SELECT * FROM foo WHERE i NOT LIKE 'foo'`: plan.NewProject(
		[]sql.Expression{expression.NewStar()},
		plan.NewFilter(
			expression.NewNot(expression.NewLike(
				expression.NewUnresolvedColumn("i"),
				expression.NewLiteral("foo", sql.Text),
			)),
			plan.NewUnresolvedTable("foo", ""),
		),
	),
	`SHOW FIELDS FROM foo`:       plan.NewShowColumns(false, plan.NewUnresolvedTable("foo", "")),
	`SHOW FULL COLUMNS FROM foo`: plan.NewShowColumns(true, plan.NewUnresolvedTable("foo", "")),
	`SHOW FIELDS FROM foo WHERE Field = 'bar'`: plan.NewFilter(
		expression.NewEquals(
			expression.NewUnresolvedColumn("Field"),
			expression.NewLiteral("bar", sql.Text),
		),
		plan.NewShowColumns(false, plan.NewUnresolvedTable("foo", "")),
	),
	`SHOW FIELDS FROM foo LIKE 'bar'`: plan.NewFilter(
		expression.NewLike(
			expression.NewUnresolvedColumn("Field"),
			expression.NewLiteral("bar", sql.Text),
		),
		plan.NewShowColumns(false, plan.NewUnresolvedTable("foo", "")),
	),
	`SHOW TABLE STATUS LIKE 'foo'`: plan.NewFilter(
		expression.NewLike(
			expression.NewUnresolvedColumn("Name"),
			expression.NewLiteral("foo", sql.Text),
		),
		plan.NewShowTableStatus(),
	),
	`SHOW TABLE STATUS FROM foo`: plan.NewShowTableStatus("foo"),
	`SHOW TABLE STATUS IN foo`:   plan.NewShowTableStatus("foo"),
	`SHOW TABLE STATUS`:          plan.NewShowTableStatus(),
	`SHOW TABLE STATUS WHERE Name = 'foo'`: plan.NewFilter(
		expression.NewEquals(
			expression.NewUnresolvedColumn("Name"),
			expression.NewLiteral("foo", sql.Text),
		),
		plan.NewShowTableStatus(),
	),
	`USE foo`: plan.NewUse(sql.UnresolvedDatabase("foo")),
	`DESCRIBE TABLE foo.bar`: plan.NewDescribe(
		plan.NewUnresolvedTable("bar", "foo"),
	),
	`DESC TABLE foo.bar`: plan.NewDescribe(
		plan.NewUnresolvedTable("bar", "foo"),
	),
	`SELECT * FROM foo.bar`: plan.NewProject(
		[]sql.Expression{
			expression.NewStar(),
		},
		plan.NewUnresolvedTable("bar", "foo"),
	),
	`SHOW VARIABLES`:                           plan.NewShowVariables(sql.NewEmptyContext().GetAll(), ""),
	`SHOW GLOBAL VARIABLES`:                    plan.NewShowVariables(sql.NewEmptyContext().GetAll(), ""),
	`SHOW SESSION VARIABLES`:                   plan.NewShowVariables(sql.NewEmptyContext().GetAll(), ""),
	`SHOW VARIABLES LIKE 'gtid_mode'`:          plan.NewShowVariables(sql.NewEmptyContext().GetAll(), "gtid_mode"),
	`SHOW SESSION VARIABLES LIKE 'autocommit'`: plan.NewShowVariables(sql.NewEmptyContext().GetAll(), "autocommit"),
	`UNLOCK TABLES`:                            plan.NewUnlockTables(),
	`LOCK TABLES foo READ`: plan.NewLockTables([]*plan.TableLock{
		{Table: plan.NewUnresolvedTable("foo", "")},
	}),
	`LOCK TABLES foo123 READ`: plan.NewLockTables([]*plan.TableLock{
		{Table: plan.NewUnresolvedTable("foo123", "")},
	}),
	`LOCK TABLES foo f READ`: plan.NewLockTables([]*plan.TableLock{
		{Table: plan.NewUnresolvedTable("foo", "")},
	}),
	`LOCK TABLES foo AS f READ`: plan.NewLockTables([]*plan.TableLock{
		{Table: plan.NewUnresolvedTable("foo", "")},
	}),
	`LOCK TABLES foo READ LOCAL`: plan.NewLockTables([]*plan.TableLock{
		{Table: plan.NewUnresolvedTable("foo", "")},
	}),
	`LOCK TABLES foo WRITE`: plan.NewLockTables([]*plan.TableLock{
		{Table: plan.NewUnresolvedTable("foo", ""), Write: true},
	}),
	`LOCK TABLES foo LOW_PRIORITY WRITE`: plan.NewLockTables([]*plan.TableLock{
		{Table: plan.NewUnresolvedTable("foo", ""), Write: true},
	}),
	`LOCK TABLES foo WRITE, bar READ`: plan.NewLockTables([]*plan.TableLock{
		{Table: plan.NewUnresolvedTable("foo", ""), Write: true},
		{Table: plan.NewUnresolvedTable("bar", "")},
	}),
	"LOCK TABLES `foo` WRITE, `bar` READ": plan.NewLockTables([]*plan.TableLock{
		{Table: plan.NewUnresolvedTable("foo", ""), Write: true},
		{Table: plan.NewUnresolvedTable("bar", "")},
	}),
	`LOCK TABLES foo READ, bar WRITE, baz READ`: plan.NewLockTables([]*plan.TableLock{
		{Table: plan.NewUnresolvedTable("foo", "")},
		{Table: plan.NewUnresolvedTable("bar", ""), Write: true},
		{Table: plan.NewUnresolvedTable("baz", "")},
	}),
	`SHOW CREATE DATABASE foo`:               plan.NewShowCreateDatabase(sql.UnresolvedDatabase("foo"), false),
	`SHOW CREATE SCHEMA foo`:                 plan.NewShowCreateDatabase(sql.UnresolvedDatabase("foo"), false),
	`SHOW CREATE DATABASE IF NOT EXISTS foo`: plan.NewShowCreateDatabase(sql.UnresolvedDatabase("foo"), true),
	`SHOW CREATE SCHEMA IF NOT EXISTS foo`:   plan.NewShowCreateDatabase(sql.UnresolvedDatabase("foo"), true),
	`SELECT -i FROM mytable`: plan.NewProject(
		[]sql.Expression{
			expression.NewUnaryMinus(
				expression.NewUnresolvedColumn("i"),
			),
		},
		plan.NewUnresolvedTable("mytable", ""),
	),
	`SHOW WARNINGS`:                            plan.NewOffset(0, plan.ShowWarnings(sql.NewEmptyContext().Warnings())),
	`SHOW WARNINGS LIMIT 10`:                   plan.NewLimit(10, plan.NewOffset(0, plan.ShowWarnings(sql.NewEmptyContext().Warnings()))),
	`SHOW WARNINGS LIMIT 5,10`:                 plan.NewLimit(10, plan.NewOffset(5, plan.ShowWarnings(sql.NewEmptyContext().Warnings()))),
	"SHOW CREATE DATABASE `foo`":               plan.NewShowCreateDatabase(sql.UnresolvedDatabase("foo"), false),
	"SHOW CREATE SCHEMA `foo`":                 plan.NewShowCreateDatabase(sql.UnresolvedDatabase("foo"), false),
	"SHOW CREATE DATABASE IF NOT EXISTS `foo`": plan.NewShowCreateDatabase(sql.UnresolvedDatabase("foo"), true),
	"SHOW CREATE SCHEMA IF NOT EXISTS `foo`":   plan.NewShowCreateDatabase(sql.UnresolvedDatabase("foo"), true),
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

var fixturesErrors = map[string]*errors.Kind{
	`SHOW METHEMONEY`:                   ErrUnsupportedFeature,
	`LOCK TABLES foo AS READ`:           errUnexpectedSyntax,
	`LOCK TABLES foo LOW_PRIORITY READ`: errUnexpectedSyntax,
}

func TestParseErrors(t *testing.T) {
	for query, expectedError := range fixturesErrors {
		t.Run(query, func(t *testing.T) {
			require := require.New(t)
			ctx := sql.NewEmptyContext()
			_, err := Parse(ctx, query)
			require.Error(err)
			require.True(expectedError.Is(err))
		})
	}
}

func TestRemoveComments(t *testing.T) {
	testCases := []struct {
		input  string
		output string
	}{
		{
			`/* FOO BAR BAZ */`,
			``,
		},
		{
			`SELECT 1 -- something`,
			`SELECT 1 `,
		},
		{
			`SELECT 1 --something`,
			`SELECT 1 --something`,
		},
		{
			`SELECT ' -- something'`,
			`SELECT ' -- something'`,
		},
		{
			`SELECT /* FOO */ 1;`,
			`SELECT  1;`,
		},
		{
			`SELECT '/* FOO */ 1';`,
			`SELECT '/* FOO */ 1';`,
		},
		{
			`SELECT "\"/* FOO */ 1\"";`,
			`SELECT "\"/* FOO */ 1\"";`,
		},
		{
			`SELECT '\'/* FOO */ 1\'';`,
			`SELECT '\'/* FOO */ 1\'';`,
		},
	}
	for _, tt := range testCases {
		t.Run(tt.input, func(t *testing.T) {
			require.Equal(
				t,
				tt.output,
				removeComments(tt.input),
			)
		})
	}
}

func TestFixSetQuery(t *testing.T) {
	testCases := []struct {
		in, out string
	}{
		{"set session foo = 1, session bar = 2", "set @@session.foo = 1, @@session.bar = 2"},
		{"set global foo = 1, session bar = 2", "set @@global.foo = 1, @@session.bar = 2"},
		{"set SESSION foo = 1, GLOBAL bar = 2", "set @@session.foo = 1, @@global.bar = 2"},
	}

	for _, tt := range testCases {
		t.Run(tt.in, func(t *testing.T) {
			require.Equal(t, tt.out, fixSetQuery(tt.in))
		})
	}
}
