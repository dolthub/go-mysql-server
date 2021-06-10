// Copyright 2020-2021 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package analyzer

import (
	"context"
	"testing"

	"github.com/dolthub/go-mysql-server/sql/expression/function"

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
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
							plan.NewDecoratedNode("Projected table access on [a]",
								plan.NewResolvedTable(foo.WithProjection([]string{"a"}), db, nil)),
						),
						plan.NewSubqueryAlias(
							"t2", "",
							plan.NewSubqueryAlias(
								"t2alias", "",
								plan.NewProject(
									[]sql.Expression{gf(0, "bar", "b")},
									plan.NewDecoratedNode("Projected table access on [b]",
										plan.NewResolvedTable(bar.WithProjection([]string{"b"}), db, nil)),
								),
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
	resolveSubqueries := getRule("resolve_subqueries")
	finalizeSubqueries := getRule("finalize_subqueries")
	runTestCases(t, ctx, testCases, a, Rule{
		Name: "subqueries",
		Apply: func(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
			n, err := resolveSubqueries.Apply(ctx, a, n, scope)
			if err != nil {
				return nil, err
			}
			return finalizeSubqueries.Apply(ctx, a, n, scope)
		},
	})
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
				plan.NewResolvedTable(table, db, nil),
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
								plan.NewDecoratedNode("Projected table access on [y i]",
									plan.NewResolvedTable(table2.WithProjection([]string{"y", "i"}), db, nil),
								),
							),
						),
						""),
				},
				plan.NewResolvedTable(table, db, nil),
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
				plan.NewResolvedTable(table, db, nil),
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
								plan.NewDecoratedNode("Projected table access on [y]",
									plan.NewResolvedTable(table2.WithProjection([]string{"y"}), db, nil),
								),
							),
						),
						""),
				},
				plan.NewResolvedTable(table, db, nil),
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
				plan.NewResolvedTable(table, db, nil),
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
				plan.NewResolvedTable(table, db, nil),
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
				plan.NewResolvedTable(table, db, nil),
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
								plan.NewResolvedTable(table2, db, nil),
							),
						),
						""),
				},
				plan.NewResolvedTable(table, db, nil),
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
								plan.NewResolvedTable(table2, db, nil),
							),
						),
						""),
				},
				plan.NewResolvedTable(table, db, nil),
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
								plan.NewDecoratedNode("Projected table access on [i]",
									plan.NewResolvedTable(table2.WithProjection([]string{"i"}), db, nil),
								),
							),
						),
						""),
				},
				plan.NewResolvedTable(table, db, nil),
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
				plan.NewResolvedTable(table, db, nil),
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
												plan.NewDecoratedNode("Projected table access on [y i]",
													plan.NewResolvedTable(table2.WithProjection([]string{"y", "i"}), db, nil),
												),
											),
										),
										""),
								),
								plan.NewResolvedTable(table2, db, nil),
							),
						),
						""),
				},
				plan.NewResolvedTable(table, db, nil),
			),
		},
	}

	ctx := sql.NewContext(context.Background(),
		sql.WithIndexRegistry(sql.NewIndexRegistry()),
		sql.WithViewRegistry(sql.NewViewRegistry())).WithCurrentDB("mydb")
	runTestCases(t, ctx, testCases, a, getRule("resolve_subquery_exprs"))
}

func TestCacheSubqueryResults(t *testing.T) {
	table := memory.NewTable("mytable", sql.Schema{
		{Name: "i", Type: sql.Int64, Source: "mytable"},
		{Name: "x", Type: sql.Int64, Source: "mytable"},
	})
	table2 := memory.NewTable("mytable2", sql.Schema{
		{Name: "i", Type: sql.Int64, Source: "mytable2"},
		{Name: "y", Type: sql.Int64, Source: "mytable2"},
	})

	testCases := []analyzerFnTestCase{
		{
			name: "not resolved",
			node: plan.NewProject(
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
								plan.NewResolvedTable(table2, nil, nil),
							),
						),
						""),
				},
				plan.NewResolvedTable(table, nil, nil),
			),
		},
		{
			name: "cacheable",
			node: plan.NewProject(
				[]sql.Expression{
					gf(0, "mytable", "i"),
					plan.NewSubquery(
						plan.NewProject(
							[]sql.Expression{
								gf(3, "mytables", "x"),
							},
							plan.NewFilter(
								gt(
									gf(2, "mytable2", "i"),
									gf(3, "mytable2", "x"),
								),
								plan.NewResolvedTable(table2, nil, nil),
							),
						),
						""),
				},
				plan.NewResolvedTable(table, nil, nil),
			),
			expected: plan.NewProject(
				[]sql.Expression{
					gf(0, "mytable", "i"),
					plan.NewSubquery(
						plan.NewProject(
							[]sql.Expression{
								gf(3, "mytables", "x"),
							},
							plan.NewFilter(
								gt(
									gf(2, "mytable2", "i"),
									gf(3, "mytable2", "x"),
								),
								plan.NewResolvedTable(table2, nil, nil),
							),
						),
						"").WithCachedResults(),
				},
				plan.NewResolvedTable(table, nil, nil),
			),
		},
		{
			name: "not cacheable, outer scope referenced",
			node: plan.NewProject(
				[]sql.Expression{
					gf(0, "mytable", "i"),
					plan.NewSubquery(
						plan.NewProject(
							[]sql.Expression{
								gf(3, "mytables", "x"),
							},
							plan.NewFilter(
								gt(
									gf(0, "mytable", "i"),
									gf(3, "mytable2", "x"),
								),
								plan.NewResolvedTable(table2, nil, nil),
							),
						),
						""),
				},
				plan.NewResolvedTable(table, nil, nil),
			),
		},
		{
			name: "not cacheable, non-deterministic expression",
			node: plan.NewProject(
				[]sql.Expression{
					gf(0, "mytable", "i"),
					plan.NewSubquery(
						plan.NewProject(
							[]sql.Expression{
								gf(3, "mytables", "x"),
							},
							plan.NewFilter(
								gt(
									mustExpr(function.NewRand(sql.NewEmptyContext())),
									gf(3, "mytable2", "x"),
								),
								plan.NewResolvedTable(table2, nil, nil),
							),
						),
						""),
				},
				plan.NewResolvedTable(table, nil, nil),
			),
		},
	}

	runTestCases(t, sql.NewEmptyContext(), testCases, nil, getRule("cache_subquery_results"))
}

func mustExpr(e sql.Expression, err error) sql.Expression {
	if err != nil {
		panic(err)
	}
	return e
}
