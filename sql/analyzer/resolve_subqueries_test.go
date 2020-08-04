package analyzer

import (
	"context"
	"testing"

	"github.com/liquidata-inc/go-mysql-server/memory"
	"github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/liquidata-inc/go-mysql-server/sql/expression"
	"github.com/liquidata-inc/go-mysql-server/sql/plan"
)

func TestResolveSubqueries(t *testing.T) {
	foo := memory.NewTable("foo", sql.Schema{
		{Name: "a", Type: sql.Int64, Source: "foo"},
	})
	bar := memory.NewTable("bar", sql.Schema{
		{Name: "b", Type: sql.Int64, Source: "bar"},
		{Name: "k", Type: sql.Int64, Source: "bar"},
	})
	baz := memory.NewTable("baz", sql.Schema{
		{Name: "c", Type: sql.Int64, Source: "baz"},
	})
	db := memory.NewDatabase("mydb")
	db.AddTable("foo", foo)
	db.AddTable("bar", bar)
	db.AddTable("baz", baz)

	// Unlike most analyzer functions, resolving subqueries needs a fully functioning analyzer
	catalog := sql.NewCatalog()
	catalog.AddDatabase(db)
	a := withoutProcessTracking(NewDefault(catalog))

	testCases := []analyzerFnTestCase{
		{
			name: `SELECT * FROM
			(SELECT a FROM foo) t1,
			(SELECT b FROM (SELECT b FROM bar) t2alias) t2, baz`,
			node: plan.NewProject(
				[]sql.Expression{expression.NewStar()},
				plan.NewCrossJoin(
					plan.NewCrossJoin(
						plan.NewSubqueryAlias(
							"t1", "",
							plan.NewProject(
								[]sql.Expression{uc("a")},
								plan.NewUnresolvedTable("foo", ""),
							),
						),
						plan.NewSubqueryAlias(
							"t2", "",
							plan.NewProject(
								[]sql.Expression{uc("b")},
								plan.NewSubqueryAlias(
									"t2alias", "",
									plan.NewProject(
										[]sql.Expression{uc("b")},
										plan.NewUnresolvedTable("bar", ""),
									),
								),
							),
						),
					),
					plan.NewUnresolvedTable("baz", ""),
				),
			),
			expected: plan.NewProject(
				[]sql.Expression{expression.NewStar()},
				plan.NewCrossJoin(
					plan.NewCrossJoin(
						plan.NewSubqueryAlias(
							"t1", "",
							plan.NewResolvedTable(foo.WithProjection([]string{"a"})),
						),
						plan.NewSubqueryAlias(
							"t2", "",
							plan.NewSubqueryAlias(
								"t2alias", "",
								plan.NewResolvedTable(bar.WithProjection([]string{"b"})),
							),
						),
					),
					plan.NewUnresolvedTable("baz", ""),
				),
			),
		},
	}

	ctx := sql.NewContext(context.Background(),
		sql.WithIndexRegistry(sql.NewIndexRegistry()),
		sql.WithViewRegistry(sql.NewViewRegistry())).WithCurrentDB("mydb")
	runTestCases(t, ctx, testCases, a, getRule("resolve_subqueries"))
}
