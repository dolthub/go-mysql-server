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

func TestResolveSubqueryExpressions(t *testing.T) {
	table := memory.NewTable("mytable", sql.Schema{
		{Name: "i", Type: sql.Int64, Source: "mytable"},
		{Name: "x", Type: sql.Int64, Source: "mytable"},
	})
	table2 := memory.NewTable("mytable2", sql.Schema{
		{Name: "i", Type: sql.Int64, Source: "mytable2"},
		{Name: "y", Type: sql.Int64, Source: "mytable2"},
	})

	db := memory.NewDatabase("mydb")
	db.AddTable("mytable", table)
	db.AddTable("mytable2", table2)

	// Unlike most analyzer functions, resolving subqueries needs a fully functioning analyzer
	catalog := sql.NewCatalog()
	catalog.AddDatabase(db)
	a := withoutProcessTracking(NewDefault(catalog))

	testCases := []analyzerFnTestCase{
		{
			name: "columns not qualified",
			node: plan.NewProject(
				[]sql.Expression{
					uc("i"),
					plan.NewSubquery(
						plan.NewProject(
							[]sql.Expression{
								uc("y"),
							},
							plan.NewFilter(
								gt(
									uc("x"),
									uc("i"),
								),
								plan.NewUnresolvedTable("mytable2", ""),
							),
						),
						""),
				},
				plan.NewResolvedTable(table),
			),
			expected: plan.NewProject(
				[]sql.Expression{
					uc("i"),
					plan.NewSubquery(
						plan.NewProject(
							[]sql.Expression{
								gf(3, "mytable2", "y"),
							},
							plan.NewFilter(
								gt(
									gf(1, "mytable", "x"),
									gf(2, "mytable2", "i"),
								),
								plan.NewResolvedTable(table2),
							),
						),
						""),
				},
				plan.NewResolvedTable(table),
			),
		},
		{
			name: "columns qualified",
			node: plan.NewProject(
				[]sql.Expression{
					uc("i"),
					plan.NewSubquery(
						plan.NewProject(
							[]sql.Expression{
								uqc("mytable2", "y"),
							},
							plan.NewFilter(
								gt(
									uqc("mytable", "x"),
									uqc("mytable", "i"),
								),
								plan.NewUnresolvedTable("mytable2", ""),
							),
						),
						""),
				},
				plan.NewResolvedTable(table),
			),
			expected: plan.NewProject(
				[]sql.Expression{
					uc("i"),
					plan.NewSubquery(
						plan.NewProject(
							[]sql.Expression{
								gf(3, "mytable2", "y"),
							},
							plan.NewFilter(
								gt(
									gf(1, "mytable", "x"),
									gf(0, "mytable", "i"),
								),
								plan.NewResolvedTable(table2),
							),
						),
						""),
				},
				plan.NewResolvedTable(table),
			),
		},
		{
			name: "table not found in expression",
			node: plan.NewProject(
				[]sql.Expression{
					uc("i"),
					plan.NewSubquery(
						plan.NewProject(
							[]sql.Expression{
								uqc("notable", "y"),
							},
							plan.NewFilter(
								gt(
									uqc("mytable", "x"),
									uqc("mytable", "i"),
								),
								plan.NewUnresolvedTable("mytable2", ""),
							),
						),
						""),
				},
				plan.NewResolvedTable(table),
			),
			err: sql.ErrTableNotFound,
		},
		{
			name: "table not found in FROM",
			node: plan.NewProject(
				[]sql.Expression{
					uc("i"),
					plan.NewSubquery(
						plan.NewProject(
							[]sql.Expression{
								uc("y"),
							},
							plan.NewFilter(
								gt(
									uqc("mytable", "x"),
									uqc("mytable", "i"),
								),
								plan.NewUnresolvedTable("notable", ""),
							),
						),
						""),
				},
				plan.NewResolvedTable(table),
			),
			err: sql.ErrTableNotFound,
		},
		{
			name: "table column not found in outer scope",
			node: plan.NewProject(
				[]sql.Expression{
					uc("i"),
					plan.NewSubquery(
						plan.NewProject(
							[]sql.Expression{
								uqc("mytable", "z"),
							},
							plan.NewFilter(
								gt(
									uqc("mytable", "x"),
									uqc("mytable", "i"),
								),
								plan.NewUnresolvedTable("mytable2", ""),
							),
						),
						""),
				},
				plan.NewResolvedTable(table),
			),
			expected: plan.NewProject(
				[]sql.Expression{
					uc("i"),
					plan.NewSubquery(
						plan.NewProject(
							[]sql.Expression{
								&deferredColumn{uqc("mytable", "z")},
							},
							plan.NewFilter(
								gt(
									gf(1, "mytable", "x"),
									gf(0, "mytable", "i"),
								),
								plan.NewResolvedTable(table2),
							),
						),
						""),
				},
				plan.NewResolvedTable(table),
			),
		},
		{
			name: "deferred column gets resolved",
			node: plan.NewProject(
				[]sql.Expression{
					uc("i"),
					plan.NewSubquery(
						plan.NewProject(
							[]sql.Expression{
								&deferredColumn{uqc("mytable", "x")},
							},
							plan.NewFilter(
								gt(
									gf(1, "mytable", "x"),
									gf(2, "mytable2", "i"),
								),
								plan.NewResolvedTable(table2),
							),
						),
						""),
				},
				plan.NewResolvedTable(table),
			),
			expected: plan.NewProject(
				[]sql.Expression{
					uc("i"),
					plan.NewSubquery(
						plan.NewProject(
							[]sql.Expression{
								gf(1, "mytable", "x"),
							},
							plan.NewFilter(
								gt(
									gf(1, "mytable", "x"),
									gf(2, "mytable2", "i"),
								),
								plan.NewResolvedTable(table2),
							),
						),
						""),
				},
				plan.NewResolvedTable(table),
			),
		},
		{
			name: "doubly nested subquery",
			node: plan.NewProject(
				[]sql.Expression{
					uc("i"),
					plan.NewSubquery(
						plan.NewProject(
							[]sql.Expression{
								uc("y"),
							},
							plan.NewFilter(
								gt(
									uc("x"),
									plan.NewSubquery(
										plan.NewProject(
											[]sql.Expression{
												uc("y"),
											},
											plan.NewFilter(
												gt(
													uc("x"),
													uc("i"),
												),
												plan.NewUnresolvedTable("mytable2", ""),
											),
										),
										""),
								),
								plan.NewUnresolvedTable("mytable2", ""),
							),
						),
						""),
				},
				plan.NewResolvedTable(table),
			),
			expected: plan.NewProject(
				[]sql.Expression{
					uc("i"),
					plan.NewSubquery(
						plan.NewProject(
							[]sql.Expression{
								gf(3, "mytable2", "y"),
							},
							plan.NewFilter(
								gt(
									gf(1, "mytable", "x"),
									plan.NewSubquery(
										plan.NewProject(
											[]sql.Expression{
												gf(5, "mytable2", "y"),
											},
											plan.NewFilter(
												gt(
													gf(1, "mytable", "x"),
													gf(4, "mytable2", "i"),
												),
												plan.NewResolvedTable(table2),
											),
										),
										""),
								),
								plan.NewResolvedTable(table2),
							),
						),
						""),
				},
				plan.NewResolvedTable(table),
			),
		},
	}

	ctx := sql.NewContext(context.Background(),
		sql.WithIndexRegistry(sql.NewIndexRegistry()),
		sql.WithViewRegistry(sql.NewViewRegistry())).WithCurrentDB("mydb")
	runTestCases(t, ctx, testCases, a, getRule("resolve_subquery_exprs"))
}
