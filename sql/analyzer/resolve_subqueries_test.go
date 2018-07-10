package analyzer

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/mem"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
	"gopkg.in/src-d/go-mysql-server.v0/sql/plan"
)

func TestResolveSubqueries(t *testing.T) {
	require := require.New(t)

	table1 := mem.NewTable("foo", sql.Schema{{Name: "a", Type: sql.Int64, Source: "foo"}})
	table2 := mem.NewTable("bar", sql.Schema{
		{Name: "b", Type: sql.Int64, Source: "bar"},
		{Name: "k", Type: sql.Int64, Source: "bar"},
	})
	table3 := mem.NewTable("baz", sql.Schema{{Name: "c", Type: sql.Int64, Source: "baz"}})
	db := mem.NewDatabase("mydb")
	db.AddTable("foo", table1)
	db.AddTable("bar", table2)
	db.AddTable("baz", table3)

	catalog := &sql.Catalog{Databases: []sql.Database{db}}
	a := NewDefault(catalog)
	a.CurrentDatabase = "mydb"

	// SELECT * FROM
	// 	(SELECT a FROM foo) t1,
	// 	(SELECT b FROM (SELECT b FROM bar) t2alias) t2,
	//  baz
	node := plan.NewProject(
		[]sql.Expression{expression.NewStar()},
		plan.NewCrossJoin(
			plan.NewCrossJoin(
				plan.NewSubqueryAlias(
					"t1",
					plan.NewProject(
						[]sql.Expression{expression.NewUnresolvedColumn("a")},
						plan.NewUnresolvedTable("foo"),
					),
				),
				plan.NewSubqueryAlias(
					"t2",
					plan.NewProject(
						[]sql.Expression{expression.NewUnresolvedColumn("b")},
						plan.NewSubqueryAlias(
							"t2alias",
							plan.NewProject(
								[]sql.Expression{expression.NewUnresolvedColumn("b")},
								plan.NewUnresolvedTable("bar"),
							),
						),
					),
				),
			),
			plan.NewUnresolvedTable("baz"),
		),
	)

	subquery := plan.NewSubqueryAlias(
		"t2alias",
		plan.NewProject(
			[]sql.Expression{
				expression.NewGetFieldWithTable(0, sql.Int64, "bar", "b", false),
			},
			plan.NewPushdownProjectionAndFiltersTable([]sql.Expression{
				expression.NewGetFieldWithTable(0, sql.Int64, "bar", "b", false),
			}, nil, table2),
		),
	)
	_ = subquery.Schema()

	expected := plan.NewProject(
		[]sql.Expression{expression.NewStar()},
		plan.NewCrossJoin(
			plan.NewCrossJoin(
				plan.NewSubqueryAlias(
					"t1",
					plan.NewPushdownProjectionAndFiltersTable([]sql.Expression{
						expression.NewGetFieldWithTable(0, sql.Int64, "foo", "a", false),
					}, nil, table1),
				),
				plan.NewSubqueryAlias(
					"t2",
					subquery,
				),
			),
			plan.NewUnresolvedTable("baz"),
		),
	)

	result, err := resolveSubqueries(sql.NewEmptyContext(), a, node)
	require.NoError(err)

	require.Equal(expected, result)
}
