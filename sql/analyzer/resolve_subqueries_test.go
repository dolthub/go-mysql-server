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

	"github.com/dolthub/go-mysql-server/sql/rowexec"

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/expression/function"
	"github.com/dolthub/go-mysql-server/sql/expression/function/aggregation"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestResolveSubqueries(t *testing.T) {
	foo := memory.NewTable("foo", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "a", Type: types.Int64, Source: "foo"},
	}), nil)
	bar := memory.NewTable("bar", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "b", Type: types.Int64, Source: "bar"},
		{Name: "k", Type: types.Int64, Source: "bar"},
	}), nil)
	baz := memory.NewTable("baz", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "c", Type: types.Int64, Source: "baz"},
	}), nil)
	db := memory.NewDatabase("mydb")
	db.AddTable("foo", foo)
	db.AddTable("bar", bar)
	db.AddTable("baz", baz)

	// Unlike most analyzer functions, resolving subqueries needs a fully functioning analyzer
	a := withoutProcessTracking(NewDefault(sql.NewDatabaseProvider(db)))

	testCases := []analyzerFnTestCase{
		{
			// Test with a query containing a subquery alias that has outer scope visibility
			name: `SELECT (select MAX(a) from (select a from bar) sqa1) FROM foo`,
			node: plan.NewProject(
				[]sql.Expression{
					plan.NewSubquery(
						plan.NewGroupBy(
							[]sql.Expression{expression.NewUnresolvedFunction("max", true, nil, uc("a"))},
							[]sql.Expression{},
							plan.NewSubqueryAlias(
								"sqa1", "select a from bar",
								plan.NewProject(
									[]sql.Expression{uc("a")},
									plan.NewUnresolvedTable("bar", "")),
							)), "select MAX(a) from (select a from bar) sqa1").WithExecBuilder(rowexec.DefaultBuilder),
				},
				plan.NewResolvedTable(foo.WithProjections([]string{"a"}), db, nil)),
			expected: plan.NewProject(
				[]sql.Expression{
					plan.NewSubquery(
						plan.NewGroupBy(
							[]sql.Expression{aggregation.NewMax(expression.NewGetFieldWithTable(1, types.Int64, "sqa1", "a", false))},
							[]sql.Expression{},
							newSubqueryAlias("sqa1", "select a from bar", true, false,
								plan.NewProject(
									[]sql.Expression{expression.NewGetFieldWithTable(0, types.Int64, "foo", "a", false)},
									plan.NewResolvedTable(bar.WithProjections(make([]string, 0)), db, nil)),
							),
						), "select MAX(a) from (select a from bar) sqa1",
					).WithExecBuilder(rowexec.DefaultBuilder),
				},
				plan.NewResolvedTable(foo.WithProjections([]string{"a"}), db, nil),
			),
		},
		{
			// Test a query with multiple subquery aliases, but no outer scope visibility
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
						newSubqueryAlias("t1", "", false, true, plan.NewResolvedTable(foo.WithProjections([]string{"a"}), db, nil)),

						newSubqueryAlias("t2", "", false, true,
							newSubqueryAlias("t2alias", "", false, true,
								plan.NewResolvedTable(bar.WithProjections([]string{"b"}), db, nil),
							),
						),
					),
					plan.NewUnresolvedTable("baz", ""),
				),
			),
		},
	}

	ctx := sql.NewContext(context.Background())
	ctx.SetCurrentDatabase("mydb")
	resolveSubqueries := getRule(resolveSubqueriesId)
	cacheSubqueryResults := getRule(cacheSubqueryResultsId)
	finalizeSubqueries := getRule(finalizeSubqueriesId)
	runTestCases(t, ctx, testCases, a, Rule{
		Id: -1,
		Apply: func(ctx *sql.Context, a *Analyzer, n sql.Node, scope *plan.Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
			n, _, err := resolveSubqueries.Apply(ctx, a, n, scope, DefaultRuleSelector)
			if err != nil {
				return nil, transform.SameTree, err
			}
			n, _, err = cacheSubqueryResults.Apply(ctx, a, n, scope, DefaultRuleSelector)
			if err != nil {
				return nil, transform.SameTree, err
			}
			return finalizeSubqueries.Apply(ctx, a, n, scope, NewFinalizeSubquerySel(sel))
		},
	})
}

func newSubqueryAlias(name, textDefinition string, hasOuterScopeVisibility, canCacheResults bool, child sql.Node) *plan.SubqueryAlias {
	sqa := plan.NewSubqueryAlias(name, textDefinition, child)
	sqa.OuterScopeVisibility = hasOuterScopeVisibility
	sqa.CanCacheResults = canCacheResults
	return sqa
}

func TestResolveSubqueryExpressions(t *testing.T) {
	table := memory.NewTable("mytable", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "i", Type: types.Int64, Source: "mytable"},
		{Name: "x", Type: types.Int64, Source: "mytable"},
	}), nil)
	table2 := memory.NewTable("mytable2", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "i", Type: types.Int64, Source: "mytable2"},
		{Name: "y", Type: types.Int64, Source: "mytable2"},
	}), nil)

	db := memory.NewDatabase("mydb")
	db.AddTable("mytable", table)
	db.AddTable("mytable2", table2)

	// Unlike most analyzer functions, resolving subqueries needs a fully functioning analyzer
	a := withoutProcessTracking(NewDefault(sql.NewDatabaseProvider(db)))

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
						"").WithExecBuilder(rowexec.DefaultBuilder),
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
								plan.NewResolvedTable(table2.WithProjections([]string{"i", "y"}), db, nil),
							),
						),
						"").WithExecBuilder(rowexec.DefaultBuilder),
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
						"").WithExecBuilder(rowexec.DefaultBuilder),
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
						"").WithExecBuilder(rowexec.DefaultBuilder),
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
						"").WithExecBuilder(rowexec.DefaultBuilder),
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
								plan.NewResolvedTable(table2.WithProjections(make([]string, 0)), db, nil),
							),
						),
						"").WithExecBuilder(rowexec.DefaultBuilder),
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
								plan.NewResolvedTable(table2.WithProjections([]string{"i"}), db, nil),
							),
						),
						"").WithExecBuilder(rowexec.DefaultBuilder),
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
										"").WithExecBuilder(rowexec.DefaultBuilder),
								),
								plan.NewUnresolvedTable("mytable2", ""),
							),
						),
						"").WithExecBuilder(rowexec.DefaultBuilder),
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
												plan.NewResolvedTable(table2.WithProjections([]string{"i", "y"}), db, nil),
											),
										),
										"").WithExecBuilder(rowexec.DefaultBuilder),
								),
								plan.NewResolvedTable(table2, db, nil),
							),
						),
						"").WithExecBuilder(rowexec.DefaultBuilder),
				},
				plan.NewResolvedTable(table, db, nil),
			),
		},
	}

	ctx := sql.NewContext(context.Background())
	ctx.SetCurrentDatabase("mydb")
	runTestCases(t, ctx, testCases, a, getRule(resolveSubqueriesId))
}

func TestFinalizeSubqueryExpressions(t *testing.T) {
	table := memory.NewTable("mytable", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "i", Type: types.Int64, Source: "mytable"},
		{Name: "x", Type: types.Int64, Source: "mytable"},
	}), nil)
	table2 := memory.NewTable("mytable2", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "i", Type: types.Int64, Source: "mytable2"},
		{Name: "y", Type: types.Int64, Source: "mytable2"},
	}), nil)

	db := memory.NewDatabase("mydb")
	db.AddTable("mytable", table)
	db.AddTable("mytable2", table2)

	// Unlike most analyzer functions, resolving subqueries needs a fully functioning analyzer
	a := withoutProcessTracking(NewDefault(sql.NewDatabaseProvider(db)))

	testCases := []analyzerFnTestCase{
		{
			name: "table column not found in outer scope",
			node: plan.NewProject(
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
						"").WithExecBuilder(rowexec.DefaultBuilder),
				},
				plan.NewResolvedTable(table, db, nil),
			),
			// FinalizeSubqueries will throw any errors instead of deferring resolution
			err: sql.ErrTableColumnNotFound,
		},
	}

	ctx := sql.NewContext(context.Background())
	ctx.SetCurrentDatabase("mydb")
	runTestCases(t, ctx, testCases, a, getRule(finalizeSubqueriesId))
}

func TestCacheSubqueryResults(t *testing.T) {
	table := memory.NewTable("mytable", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "i", Type: types.Int64, Source: "mytable"},
		{Name: "x", Type: types.Int64, Source: "mytable"},
	}), nil)
	table2 := memory.NewTable("mytable2", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "i", Type: types.Int64, Source: "mytable2"},
		{Name: "y", Type: types.Int64, Source: "mytable2"},
	}), nil)

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
						"").WithExecBuilder(rowexec.DefaultBuilder),
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
						"").WithExecBuilder(rowexec.DefaultBuilder),
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
						"").WithCachedResults().WithExecBuilder(rowexec.DefaultBuilder),
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
						"").WithExecBuilder(rowexec.DefaultBuilder),
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
									mustExpr(function.NewRand()),
									gf(3, "mytable2", "x"),
								),
								plan.NewResolvedTable(table2, nil, nil),
							),
						),
						"").WithExecBuilder(rowexec.DefaultBuilder),
				},
				plan.NewResolvedTable(table, nil, nil),
			),
		},
	}

	runTestCases(t, sql.NewEmptyContext(), testCases, nil, getRule(cacheSubqueryResultsId))
}

func mustExpr(e sql.Expression, err error) sql.Expression {
	if err != nil {
		panic(err)
	}
	return e
}
