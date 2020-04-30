package analyzer

import (
	"context"
	"testing"

	"github.com/liquidata-inc/go-mysql-server/memory"
	"github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/liquidata-inc/go-mysql-server/sql/expression"
	"github.com/liquidata-inc/go-mysql-server/sql/plan"
	"github.com/stretchr/testify/require"
)

func TestResolveSubqueries(t *testing.T) {
	require := require.New(t)

	table1 := memory.NewTable("foo", sql.Schema{{Name: "a", Type: sql.Int64, Source: "foo"}})
	table2 := memory.NewTable("bar", sql.Schema{
		{Name: "b", Type: sql.Int64, Source: "bar"},
		{Name: "k", Type: sql.Int64, Source: "bar"},
	})
	table3 := memory.NewTable("baz", sql.Schema{{Name: "c", Type: sql.Int64, Source: "baz"}})
	db := memory.NewDatabase("mydb")
	db.AddTable("foo", table1)
	db.AddTable("bar", table2)
	db.AddTable("baz", table3)

	catalog := sql.NewCatalog()
	catalog.AddDatabase(db)
	a := withoutProcessTracking(NewDefault(catalog))

	// SELECT * FROM
	// 	(SELECT a FROM foo) t1,
	// 	(SELECT b FROM (SELECT b FROM bar) t2alias) t2,
	//  baz
	node := plan.NewProject(
		[]sql.Expression{expression.NewStar()},
		plan.NewCrossJoin(
			plan.NewCrossJoin(
				plan.NewSubqueryAlias(
					"t1", "",
					plan.NewProject(
						[]sql.Expression{expression.NewUnresolvedColumn("a")},
						plan.NewUnresolvedTable("foo", ""),
					),
				),
				plan.NewSubqueryAlias(
					"t2", "",
					plan.NewProject(
						[]sql.Expression{expression.NewUnresolvedColumn("b")},
						plan.NewSubqueryAlias(
							"t2alias", "",
							plan.NewProject(
								[]sql.Expression{expression.NewUnresolvedColumn("b")},
								plan.NewUnresolvedTable("bar", ""),
							),
						),
					),
				),
			),
			plan.NewUnresolvedTable("baz", ""),
		),
	)

	subquery := plan.NewSubqueryAlias(
		"t2alias", "",
		plan.NewResolvedTable(table2.WithProjection([]string{"b"})),
	)
	_ = subquery.Schema()

	expected := plan.NewProject(
		[]sql.Expression{expression.NewStar()},
		plan.NewCrossJoin(
			plan.NewCrossJoin(
				plan.NewSubqueryAlias(
					"t1", "",
					plan.NewResolvedTable(table1.WithProjection([]string{"a"})),
				),
				plan.NewSubqueryAlias(
					"t2", "",
					subquery,
				),
			),
			plan.NewUnresolvedTable("baz", ""),
		),
	)

	ctx := sql.NewContext(context.Background(), sql.WithIndexRegistry(sql.NewIndexRegistry()), sql.WithViewRegistry(sql.NewViewRegistry())).WithCurrentDB("mydb")
	result, err := resolveSubqueries(ctx, a, node)
	require.NoError(err)
	require.Equal(expected, result)
}
