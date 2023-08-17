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
	"testing"

	"github.com/dolthub/go-mysql-server/sql/rowexec"

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression/function"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"
)

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
