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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestPushdownProjectionToTables(t *testing.T) {
	table := memory.NewTable("mytable", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "i", Type: types.Int32, Source: "mytable"},
		{Name: "f", Type: types.Float64, Source: "mytable"},
		{Name: "t", Type: types.Text, Source: "mytable"},
	}), nil)

	table2 := memory.NewTable("mytable2", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "i2", Type: types.Int32, Source: "mytable2"},
		{Name: "f2", Type: types.Float64, Source: "mytable2"},
		{Name: "t2", Type: types.Text, Source: "mytable2"},
	}), nil)

	db := memory.NewDatabase("mydb")
	db.AddTable("mytable", table)
	db.AddTable("mytable2", table2)

	a := NewDefault(sql.NewDatabaseProvider())

	// TODO: test interaction with filtered tables
	tests := []analyzerFnTestCase{
		{
			name: "pushdown projections to tables",
			node: plan.NewProject(
				[]sql.Expression{
					expression.NewGetFieldWithTable(2, types.Text, "mytable2", "t2", false),
				},
				plan.NewFilter(
					expression.NewOr(
						expression.NewEquals(
							expression.NewGetFieldWithTable(1, types.Float64, "mytable", "f", false),
							expression.NewLiteral(3.14, types.Float64),
						),
						expression.NewIsNull(
							expression.NewGetFieldWithTable(0, types.Int32, "mytable2", "i2", false),
						),
					),
					plan.NewCrossJoin(
						plan.NewResolvedTable(table, nil, nil),
						plan.NewResolvedTable(table2, nil, nil),
					),
				),
			),
			expected: plan.NewProject(
				[]sql.Expression{
					expression.NewGetFieldWithTable(2, types.Text, "mytable2", "t2", false),
				},
				plan.NewFilter(
					expression.NewOr(
						expression.NewEquals(
							expression.NewGetFieldWithTable(1, types.Float64, "mytable", "f", false),
							expression.NewLiteral(3.14, types.Float64),
						),
						expression.NewIsNull(
							expression.NewGetFieldWithTable(0, types.Int32, "mytable2", "i2", false),
						),
					),
					plan.NewCrossJoin(
						plan.NewResolvedTable(table.WithProjections([]string{"f"}), nil, nil),
						plan.NewResolvedTable(table2.WithProjections([]string{"i2", "t2"}), nil, nil),
					),
				),
			),
		},
	}

	runTestCases(t, sql.NewEmptyContext(), tests, a, getRule(pruneTablesId))
}

func TestPushdownFilterToTables(t *testing.T) {
	table := memory.NewFilteredTable("mytable", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "i", Type: types.Int32, Source: "mytable"},
		{Name: "f", Type: types.Float64, Source: "mytable"},
		{Name: "t", Type: types.Text, Source: "mytable"},
	}), nil)

	table2 := memory.NewFilteredTable("mytable2", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "i2", Type: types.Int32, Source: "mytable2"},
		{Name: "f2", Type: types.Float64, Source: "mytable2"},
		{Name: "t2", Type: types.Text, Source: "mytable2"},
	}), nil)

	db := memory.NewDatabase("mydb")
	db.AddTable("mytable", table)
	db.AddTable("mytable2", table2)

	a := NewDefault(sql.NewDatabaseProvider(db))

	tests := []analyzerFnTestCase{
		{
			name: "pushdown filter to tables",
			node: plan.NewProject(
				[]sql.Expression{
					expression.NewGetFieldWithTable(2, types.Text, "mytable2", "t2", false),
				},
				plan.NewFilter(
					expression.NewAnd(
						expression.NewEquals(
							expression.NewGetFieldWithTable(1, types.Float64, "mytable", "f", false),
							expression.NewLiteral(3.14, types.Float64),
						),
						expression.NewIsNull(
							expression.NewGetFieldWithTable(0, types.Int32, "mytable2", "i2", false),
						),
					),
					plan.NewCrossJoin(
						plan.NewResolvedTable(table, nil, nil),
						plan.NewResolvedTable(table2, nil, nil),
					),
				),
			),
			expected: plan.NewProject(
				[]sql.Expression{
					expression.NewGetFieldWithTable(5, types.Text, "mytable2", "t2", false),
				},
				plan.NewCrossJoin(
					plan.NewResolvedTable(
						newFilteredTableWithFilters(table, []sql.Expression{
							expression.NewEquals(
								expression.NewGetFieldWithTable(1, types.Float64, "mytable", "f", false),
								expression.NewLiteral(3.14, types.Float64),
							),
						}), nil, nil),
					plan.NewResolvedTable(
						newFilteredTableWithFilters(table2, []sql.Expression{
							expression.NewIsNull(
								expression.NewGetFieldWithTable(0, types.Int32, "mytable2", "i2", false),
							),
						}), nil, nil),
				),
			),
		},
		{
			name: "pushdown filter to aliased tables",
			node: plan.NewProject(
				[]sql.Expression{
					expression.NewGetFieldWithTable(0, types.Int32, "t1", "i", true),
				},
				plan.NewFilter(
					and(
						expression.NewEquals(
							expression.NewGetFieldWithTable(1, types.Float64, "t1", "f", false),
							expression.NewLiteral(3.14, types.Float64),
						),
						expression.NewIsNull(
							expression.NewGetFieldWithTable(0, types.Int32, "t2", "i2", false),
						),
					),
					plan.NewCrossJoin(
						plan.NewTableAlias("t1",
							plan.NewResolvedTable(table, nil, nil)),
						plan.NewTableAlias("t2",
							plan.NewResolvedTable(table2, nil, nil)),
					),
				),
			),
			expected: plan.NewProject(
				[]sql.Expression{
					expression.NewGetFieldWithTable(0, types.Int32, "t1", "i", true),
				},
				plan.NewCrossJoin(
					plan.NewTableAlias("t1",
						plan.NewResolvedTable(
							newFilteredTableWithFilters(table, []sql.Expression{
								expression.NewEquals(
									expression.NewGetFieldWithTable(1, types.Float64, "t1", "f", false),
									expression.NewLiteral(3.14, types.Float64),
								),
							}), nil, nil)),
					plan.NewTableAlias("t2",
						plan.NewResolvedTable(
							newFilteredTableWithFilters(table2, []sql.Expression{
								expression.NewIsNull(
									expression.NewGetFieldWithTable(0, types.Int32, "t2", "i2", false),
								),
							}), nil, nil)),
				),
			),
		},
		{
			name: "push filters down onto projected table",
			node: plan.NewProject(
				[]sql.Expression{
					expression.NewGetFieldWithTable(1, types.Text, "mytable2", "t2", false),
				},
				plan.NewFilter(
					expression.NewAnd(
						expression.NewEquals(
							expression.NewGetFieldWithTable(0, types.Float64, "mytable", "f", false),
							expression.NewLiteral(3.14, types.Float64),
						),
						expression.NewIsNull(
							expression.NewGetFieldWithTable(2, types.Int32, "mytable2", "i2", false),
						),
					),
					plan.NewCrossJoin(
						plan.NewResolvedTable(
							newFilteredTableWithProjections(table, []string{"f"}),
							nil, nil),
						plan.NewResolvedTable(
							newFilteredTableWithProjections(table2, []string{"t2", "i2"}),
							nil, nil),
					),
				),
			),
			expected: plan.NewProject(
				[]sql.Expression{
					expression.NewGetFieldWithTable(5, types.Text, "mytable2", "t2", false),
				},
				plan.NewCrossJoin(
					plan.NewResolvedTable(
						newFilteredTableWithProjectionsAndFilters(
							table, []string{"f"}, []sql.Expression{
								eq(expression.NewGetFieldWithTable(1, types.Float64, "mytable", "f", false), expression.NewLiteral(3.14, types.Float64)),
							}), nil, nil),
					plan.NewResolvedTable(
						newFilteredTableWithProjectionsAndFilters(
							table2, []string{"t2", "i2"}, []sql.Expression{
								expression.NewIsNull(expression.NewGetFieldWithTable(0, types.Int32, "mytable2", "i2", false)),
							}), nil, nil),
				),
			),
		},
	}

	runTestCases(t, sql.NewEmptyContext(), tests, a, getRule(pushdownFiltersId))
}

func TestPushdownFiltersAboveTables(t *testing.T) {
	table := memory.NewTable("mytable", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "i", Type: types.Int32, Source: "mytable"},
		{Name: "f", Type: types.Float64, Source: "mytable"},
		{Name: "t", Type: types.Text, Source: "mytable"},
	}), nil)

	table2 := memory.NewTable("mytable2", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "i2", Type: types.Int32, Source: "mytable2"},
		{Name: "f2", Type: types.Float64, Source: "mytable2"},
		{Name: "t2", Type: types.Text, Source: "mytable2"},
	}), nil)

	db := memory.NewDatabase("mydb")
	db.AddTable("mytable", table)
	db.AddTable("mytable2", table2)

	a := NewDefault(sql.NewDatabaseProvider(db))

	tests := []analyzerFnTestCase{
		{
			name: "pushdown filters to under join node",
			node: plan.NewProject(
				[]sql.Expression{
					expression.NewGetFieldWithTable(0, types.Int32, "mytable", "i", true),
				},
				plan.NewFilter(
					and(
						expression.NewEquals(
							expression.NewGetFieldWithTable(1, types.Float64, "mytable", "f", true),
							expression.NewLiteral(3.14, types.Float64),
						),
						and(
							expression.NewEquals(
								expression.NewGetFieldWithTable(3, types.Int32, "mytable2", "i2", true),
								expression.NewLiteral(21, types.Int32),
							),
							expression.NewEquals(
								expression.NewGetFieldWithTable(5, types.Int32, "mytable2", "t2", true),
								expression.NewLiteral("hello", types.Text),
							),
						),
					),
					plan.NewCrossJoin(
						plan.NewResolvedTable(table, nil, nil),
						plan.NewResolvedTable(table2, nil, nil),
					),
				),
			),
			expected: plan.NewProject(
				[]sql.Expression{
					expression.NewGetFieldWithTable(0, types.Int32, "mytable", "i", true),
				},
				plan.NewCrossJoin(
					plan.NewFilter(
						expression.NewEquals(
							expression.NewGetFieldWithTable(1, types.Float64, "mytable", "f", true),
							expression.NewLiteral(3.14, types.Float64),
						),
						plan.NewResolvedTable(table, nil, nil),
					),
					plan.NewFilter(
						and(
							expression.NewEquals(
								expression.NewGetFieldWithTable(0, types.Int32, "mytable2", "i2", true),
								expression.NewLiteral(21, types.Int32),
							),
							expression.NewEquals(
								expression.NewGetFieldWithTable(2, types.Int32, "mytable2", "t2", true),
								expression.NewLiteral("hello", types.Text),
							),
						),
						plan.NewResolvedTable(table2, nil, nil),
					),
				),
			),
		},
		{
			name: "pushdown filters to under join node, aliased tables",
			node: plan.NewProject(
				[]sql.Expression{
					expression.NewGetFieldWithTable(0, types.Int32, "t1", "i", true),
				},
				plan.NewFilter(
					and(
						expression.NewEquals(
							expression.NewGetFieldWithTable(1, types.Float64, "t1", "f", true),
							expression.NewLiteral(3.14, types.Float64),
						),
						and(
							expression.NewEquals(
								expression.NewGetFieldWithTable(3, types.Int32, "t2", "i2", true),
								expression.NewLiteral(21, types.Int32),
							),
							expression.NewEquals(
								expression.NewGetFieldWithTable(5, types.Int32, "t2", "t2", true),
								expression.NewLiteral("hello", types.Text),
							),
						),
					),
					plan.NewCrossJoin(
						plan.NewTableAlias("t1",
							plan.NewResolvedTable(table, nil, nil),
						),
						plan.NewTableAlias("t2",
							plan.NewResolvedTable(table2, nil, nil),
						),
					),
				),
			),
			expected: plan.NewProject(
				[]sql.Expression{
					expression.NewGetFieldWithTable(0, types.Int32, "t1", "i", true),
				},
				plan.NewCrossJoin(
					plan.NewFilter(
						expression.NewEquals(
							expression.NewGetFieldWithTable(1, types.Float64, "t1", "f", true),
							expression.NewLiteral(3.14, types.Float64),
						),
						plan.NewTableAlias("t1",
							plan.NewResolvedTable(table, nil, nil),
						),
					),
					plan.NewFilter(
						and(
							expression.NewEquals(
								expression.NewGetFieldWithTable(0, types.Int32, "t2", "i2", true),
								expression.NewLiteral(21, types.Int32),
							),
							expression.NewEquals(
								expression.NewGetFieldWithTable(2, types.Int32, "t2", "t2", true),
								expression.NewLiteral("hello", types.Text),
							),
						),
						plan.NewTableAlias("t2",
							plan.NewResolvedTable(table2, nil, nil),
						),
					),
				),
			),
		},
		{
			name: "pushdown filter to left join",
			node: plan.NewProject(
				[]sql.Expression{
					expression.NewGetFieldWithTable(2, types.Text, "mytable2", "t2", false),
				},
				plan.NewFilter(
					expression.NewAnd(
						expression.NewEquals(
							expression.NewGetFieldWithTable(1, types.Float64, "mytable", "f", false),
							expression.NewLiteral(3.14, types.Float64),
						),
						expression.NewIsNull(
							expression.NewGetFieldWithTable(0, types.Int32, "mytable2", "i2", false),
						),
					),
					plan.NewLeftOuterJoin(
						plan.NewResolvedTable(table, nil, nil),
						plan.NewResolvedTable(table2, nil, nil),
						eq(gf(0, "mytable", "i"), gf(3, "mytable2", "i2")),
					),
				),
			),
			expected: plan.NewProject(
				[]sql.Expression{
					expression.NewGetFieldWithTable(5, types.Text, "mytable2", "t2", false),
				},
				plan.NewFilter(
					expression.NewIsNull(
						expression.NewGetFieldWithTable(3, types.Int32, "mytable2", "i2", false),
					),
					plan.NewLeftOuterJoin(
						plan.NewFilter(
							expression.NewEquals(
								expression.NewGetFieldWithTable(1, types.Float64, "mytable", "f", false),
								expression.NewLiteral(3.14, types.Float64),
							),
							plan.NewResolvedTable(table, nil, nil),
						),
						plan.NewResolvedTable(table2, nil, nil),
						eq(gf(0, "mytable", "i"), gf(3, "mytable2", "i2")),
					),
				),
			),
		},
		{
			name: "filter contains join condition",
			node: plan.NewProject(
				[]sql.Expression{
					expression.NewGetFieldWithTable(0, types.Int32, "mytable", "i", true),
				},
				plan.NewFilter(
					and(
						expression.NewEquals(
							expression.NewGetFieldWithTable(1, types.Float64, "mytable", "f", true),
							expression.NewLiteral(3.14, types.Float64),
						),
						and(
							expression.NewEquals(
								expression.NewGetFieldWithTable(0, types.Int32, "mytable", "i", true),
								expression.NewGetFieldWithTable(3, types.Int32, "mytable2", "i2", true),
							),
							expression.NewEquals(
								expression.NewGetFieldWithTable(3, types.Int32, "mytable2", "i2", true),
								expression.NewLiteral(20, types.Int32),
							),
						),
					),
					plan.NewCrossJoin(
						plan.NewResolvedTable(table, nil, nil),
						plan.NewResolvedTable(table2, nil, nil),
					),
				),
			),
			expected: plan.NewProject(
				[]sql.Expression{
					expression.NewGetFieldWithTable(0, types.Int32, "mytable", "i", true),
				},
				plan.NewFilter(
					expression.NewEquals(
						expression.NewGetFieldWithTable(0, types.Int32, "mytable", "i", true),
						expression.NewGetFieldWithTable(3, types.Int32, "mytable2", "i2", true),
					),
					plan.NewCrossJoin(
						plan.NewFilter(
							expression.NewEquals(
								expression.NewGetFieldWithTable(1, types.Float64, "mytable", "f", true),
								expression.NewLiteral(3.14, types.Float64),
							),
							plan.NewResolvedTable(table, nil, nil),
						),
						plan.NewFilter(
							expression.NewEquals(
								expression.NewGetFieldWithTable(0, types.Int32, "mytable2", "i2", true),
								expression.NewLiteral(20, types.Int32),
							),
							plan.NewResolvedTable(table2, nil, nil),
						),
					),
				),
			),
		},
		{
			name: "filter contains a subquery",
			node: plan.NewProject(
				[]sql.Expression{
					expression.NewGetFieldWithTable(0, types.Int32, "mytable", "i", true),
				},
				plan.NewFilter(
					plan.NewInSubquery(
						expression.NewGetFieldWithTable(1, types.Float64, "mytable", "f", true),
						plan.NewSubquery(
							plan.NewProject(
								[]sql.Expression{
									expression.NewLiteral(1, types.Int32),
								},
								plan.NewEmptyTableWithSchema(table.Schema()),
							),
							"SELECT 1 FROM DUAL",
						),
					),
					plan.NewCrossJoin(
						plan.NewResolvedTable(table, db, nil),
						plan.NewResolvedTable(table2, db, nil),
					),
				),
			),
		},
	}

	runTestCases(t, sql.NewEmptyContext(), tests, a, getRule(pushdownFiltersId))
}

// TODO: this needs tests for pushing a merged index lookup down to a table
func TestPushdownIndex(t *testing.T) {
	require := require.New(t)
	ctx := sql.NewEmptyContext()

	myTableF := &sql.Column{Name: "f", Type: types.Float64, Source: "mytable"}
	myTableI := &sql.Column{Name: "i", Type: types.Int32, Source: "mytable", PrimaryKey: true}
	table := memory.NewTable("mytable", sql.NewPrimaryKeySchema(sql.Schema{
		myTableI,
		myTableF,
		{Name: "t", Type: types.Text, Source: "mytable"},
	}), nil)

	table.EnablePrimaryKeyIndexes()
	err := table.CreateIndex(ctx, sql.IndexDef{
		Name:       "f",
		Columns:    []sql.IndexColumn{{Name: "f", Length: 0}},
		Constraint: sql.IndexConstraint_None,
		Storage:    sql.IndexUsing_BTree,
	})
	require.NoError(err)

	table1Idxes, err := table.GetIndexes(ctx)
	require.NoError(err)
	idxtable1I := table1Idxes[0]
	fmt.Sprintf("%v", idxtable1I)
	idxTable1F := table1Idxes[1]

	mytable2I := &sql.Column{Name: "i2", Type: types.Int32, Source: "mytable2", PrimaryKey: true}
	table2 := memory.NewTable("mytable2", sql.NewPrimaryKeySchema(sql.Schema{
		mytable2I,
		{Name: "f2", Type: types.Float64, Source: "mytable2"},
		{Name: "t2", Type: types.Text, Source: "mytable2"},
	}), nil)

	table2.EnablePrimaryKeyIndexes()
	table2Idxes, err := table2.GetIndexes(ctx)
	require.NoError(err)
	idxTable2I2 := table2Idxes[0]

	db := memory.NewDatabase("")
	db.AddTable("mytable", table)
	db.AddTable("mytable2", table2)

	a := NewDefault(sql.NewDatabaseProvider(db))

	tests := []analyzerFnTestCase{
		{
			name: "single index",
			node: plan.NewProject(
				[]sql.Expression{
					expression.NewGetFieldWithTable(0, types.Int32, "mytable", "i", true),
				},
				plan.NewFilter(
					expression.NewEquals(
						expression.NewGetFieldWithTable(1, types.Float64, "mytable", "f", true),
						expression.NewLiteral(3.14, types.Float64),
					),
					plan.NewResolvedTable(table, nil, nil),
				),
			),
			expected: plan.NewProject(
				[]sql.Expression{
					expression.NewGetFieldWithTable(0, types.Int32, "mytable", "i", true),
				},
				mustStaticIta(
					plan.NewResolvedTable(table, nil, nil),
					mustIndexLookup(sql.NewIndexBuilder(idxTable1F).Equals(ctx, "mytable.f", 3.14).Build(ctx)),
				),
			),
		},
		{
			name: "single index with extra predicate",
			node: plan.NewProject(
				[]sql.Expression{
					expression.NewGetFieldWithTable(0, types.Int32, "mytable", "i", true),
				},
				plan.NewFilter(
					and(
						expression.NewEquals(
							expression.NewGetFieldWithTable(1, types.Float64, "mytable", "f", true),
							expression.NewLiteral(3.14, types.Float64),
						),
						expression.NewEquals(
							expression.NewGetFieldWithTable(2, types.Text, "mytable", "t", true),
							expression.NewLiteral("hello", types.Text),
						),
					),
					plan.NewResolvedTable(table, nil, nil),
				),
			),
			expected: plan.NewProject(
				[]sql.Expression{
					expression.NewGetFieldWithTable(0, types.Int32, "mytable", "i", true),
				},
				plan.NewFilter(
					expression.NewEquals(
						expression.NewGetFieldWithTable(2, types.Text, "mytable", "t", true),
						expression.NewLiteral("hello", types.Text),
					),
					mustStaticIta(
						plan.NewResolvedTable(table, nil, nil),
						mustIndexLookup(sql.NewIndexBuilder(idxTable1F).Equals(ctx, "mytable.f", 3.14).Build(ctx)),
					),
				),
			),
		},
		{
			name: "single index with extra predicates",
			node: plan.NewProject(
				[]sql.Expression{
					expression.NewGetFieldWithTable(0, types.Int32, "mytable", "i", true),
				},
				plan.NewFilter(
					and(
						expression.NewEquals(
							expression.NewGetFieldWithTable(1, types.Float64, "mytable", "f", true),
							expression.NewLiteral(3.14, types.Float64),
						),
						and(
							expression.NewEquals(
								expression.NewGetFieldWithTable(2, types.Text, "mytable", "t", true),
								expression.NewLiteral("hello", types.Text),
							),
							expression.NewEquals(
								expression.NewGetFieldWithTable(2, types.Text, "mytable", "t", true),
								expression.NewLiteral("goodbye", types.Text),
							),
						),
					),
					plan.NewResolvedTable(table, nil, nil),
				),
			),
			expected: plan.NewProject(
				[]sql.Expression{
					expression.NewGetFieldWithTable(0, types.Int32, "mytable", "i", true),
				},
				plan.NewFilter(
					and(
						expression.NewEquals(
							expression.NewGetFieldWithTable(2, types.Text, "mytable", "t", true),
							expression.NewLiteral("hello", types.Text),
						),
						expression.NewEquals(
							expression.NewGetFieldWithTable(2, types.Text, "mytable", "t", true),
							expression.NewLiteral("goodbye", types.Text),
						),
					),
					mustStaticIta(
						plan.NewResolvedTable(table, nil, nil),
						mustIndexLookup(sql.NewIndexBuilder(idxTable1F).Equals(ctx, "mytable.f", 3.14).Build(ctx)),
					),
				),
			),
		},
		{
			name: "single index to each of two tables",
			node: plan.NewProject(
				[]sql.Expression{
					expression.NewGetFieldWithTable(0, types.Int32, "mytable", "i", true),
				},
				plan.NewFilter(
					and(
						expression.NewEquals(
							expression.NewGetFieldWithTable(1, types.Float64, "mytable", "f", true),
							expression.NewLiteral(3.14, types.Float64),
						),
						expression.NewEquals(
							expression.NewGetFieldWithTable(3, types.Int32, "mytable2", "i2", true),
							expression.NewLiteral(21, types.Int32),
						),
					),
					plan.NewCrossJoin(
						plan.NewResolvedTable(table, nil, nil),
						plan.NewResolvedTable(table2, nil, nil),
					),
				),
			),
			expected: plan.NewProject(
				[]sql.Expression{
					expression.NewGetFieldWithTable(0, types.Int32, "mytable", "i", true),
				},
				plan.NewCrossJoin(
					mustStaticIta(
						plan.NewResolvedTable(table, nil, nil),
						mustIndexLookup(sql.NewIndexBuilder(idxTable1F).Equals(ctx, "mytable.f", 3.14).Build(ctx)),
					),
					mustStaticIta(
						plan.NewResolvedTable(table2, nil, nil),
						mustIndexLookup(sql.NewIndexBuilder(idxTable2I2).Equals(ctx, "mytable2.i2", 21).Build(ctx)),
					),
				),
			),
		},
		{
			// This scenario can't happen in the current analyzer rule ordering. But the rule should behave correctly anyway.
			name: "single index to each of two tables, filters already pushed down",
			node: plan.NewProject(
				[]sql.Expression{
					expression.NewGetFieldWithTable(0, types.Int32, "mytable", "i", true),
				},
				plan.NewCrossJoin(
					plan.NewFilter(
						expression.NewEquals(
							expression.NewGetFieldWithTable(1, types.Float64, "mytable", "f", true),
							expression.NewLiteral(3.14, types.Float64),
						),
						plan.NewResolvedTable(table, nil, nil),
					),
					plan.NewFilter(
						expression.NewEquals(
							expression.NewGetFieldWithTable(0, types.Int32, "mytable2", "i2", true),
							expression.NewLiteral(21, types.Int32),
						),
						plan.NewResolvedTable(table2, nil, nil),
					),
				),
			),
			expected: plan.NewProject(
				[]sql.Expression{
					expression.NewGetFieldWithTable(0, types.Int32, "mytable", "i", true),
				},
				plan.NewCrossJoin(
					mustStaticIta(
						plan.NewResolvedTable(table, nil, nil),
						mustIndexLookup(sql.NewIndexBuilder(idxTable1F).Equals(ctx, "mytable.f", 3.14).Build(ctx)),
					),
					mustStaticIta(
						plan.NewResolvedTable(table2, nil, nil),
						mustIndexLookup(sql.NewIndexBuilder(idxTable2I2).Equals(ctx, "mytable2.i2", 21).Build(ctx)),
					),
				),
			),
		},
		{
			name: "Index already pushed down, no change to plan",
			node: plan.NewProject(
				[]sql.Expression{
					expression.NewGetFieldWithTable(0, types.Int32, "mytable", "i", true),
				},
				plan.NewCrossJoin(
					mustStaticIta(
						plan.NewResolvedTable(table, nil, nil),
						mustIndexLookup(sql.NewIndexBuilder(idxTable1F).Equals(ctx, "mytable.f", 3.14).Build(ctx)),
					),
					mustStaticIta(
						plan.NewResolvedTable(table2, nil, nil),
						mustIndexLookup(sql.NewIndexBuilder(idxTable2I2).Equals(ctx, "mytable2.i2", 21).Build(ctx)),
					),
				),
			),
		},
		{
			name: "single index to each of two tables, extra predicates",
			node: plan.NewProject(
				[]sql.Expression{
					expression.NewGetFieldWithTable(0, types.Int32, "mytable", "i", true),
				},
				plan.NewFilter(
					and(
						expression.NewEquals(
							expression.NewGetFieldWithTable(1, types.Float64, "mytable", "f", true),
							expression.NewLiteral(3.14, types.Float64),
						),
						and(
							expression.NewEquals(
								expression.NewGetFieldWithTable(3, types.Int32, "mytable2", "i2", true),
								expression.NewLiteral(21, types.Int32),
							),
							expression.NewEquals(
								expression.NewGetFieldWithTable(5, types.Int32, "mytable2", "t2", true),
								expression.NewLiteral("hello", types.Text),
							),
						),
					),
					plan.NewCrossJoin(
						plan.NewResolvedTable(table, nil, nil),
						plan.NewResolvedTable(table2, nil, nil),
					),
				),
			),
			expected: plan.NewProject(
				[]sql.Expression{
					expression.NewGetFieldWithTable(0, types.Int32, "mytable", "i", true),
				},
				plan.NewCrossJoin(
					mustStaticIta(
						plan.NewResolvedTable(table, nil, nil),
						mustIndexLookup(sql.NewIndexBuilder(idxTable1F).Equals(ctx, "mytable.f", 3.14).Build(ctx)),
					),
					plan.NewFilter(
						expression.NewEquals(
							expression.NewGetFieldWithTable(2, types.Int32, "mytable2", "t2", true),
							expression.NewLiteral("hello", types.Text),
						),
						mustStaticIta(
							plan.NewResolvedTable(table2, nil, nil),
							mustIndexLookup(sql.NewIndexBuilder(idxTable2I2).Equals(ctx, "mytable2.i2", 21).Build(ctx)),
						),
					),
				),
			),
		},
		{
			name: "single index on aliased table",
			node: plan.NewProject(
				[]sql.Expression{
					expression.NewGetFieldWithTable(0, types.Int32, "t1", "i", true),
				},
				plan.NewFilter(
					expression.NewEquals(
						expression.NewGetFieldWithTable(1, types.Float64, "t1", "f", true),
						expression.NewLiteral(3.14, types.Float64),
					),
					plan.NewTableAlias("t1",
						plan.NewResolvedTable(table, nil, nil),
					),
				),
			),
			expected: plan.NewProject(
				[]sql.Expression{
					expression.NewGetFieldWithTable(0, types.Int32, "t1", "i", true),
				},
				plan.NewFilter(
					expression.NewEquals(
						expression.NewGetFieldWithTable(1, types.Float64, "t1", "f", true),
						expression.NewLiteral(3.14, types.Float64),
					),
					plan.NewTableAlias("t1",
						mustStaticIta(
							plan.NewResolvedTable(table, nil, nil),
							mustIndexLookup(sql.NewIndexBuilder(idxTable1F).Equals(ctx, "mytable.f", 3.14).Build(ctx)),
						),
					),
				),
			),
		},
		{
			name: "single index on aliased table, extra predicate",
			node: plan.NewProject(
				[]sql.Expression{
					expression.NewGetFieldWithTable(0, types.Int32, "t1", "i", true),
				},
				plan.NewFilter(
					and(
						expression.NewEquals(
							expression.NewGetFieldWithTable(1, types.Float64, "t1", "f", true),
							expression.NewLiteral(3.14, types.Float64),
						),
						expression.NewEquals(
							expression.NewGetFieldWithTable(2, types.Text, "t1", "t", true),
							expression.NewLiteral("hello", types.Text),
						),
					),
					plan.NewTableAlias("t1",
						plan.NewResolvedTable(table, nil, nil),
					),
				),
			),
			expected: plan.NewProject(
				[]sql.Expression{
					expression.NewGetFieldWithTable(0, types.Int32, "t1", "i", true),
				},
				plan.NewFilter(
					and(
						expression.NewEquals(
							expression.NewGetFieldWithTable(1, types.Float64, "t1", "f", true),
							expression.NewLiteral(3.14, types.Float64),
						),
						expression.NewEquals(
							expression.NewGetFieldWithTable(2, types.Text, "t1", "t", true),
							expression.NewLiteral("hello", types.Text),
						),
					),
					plan.NewTableAlias("t1",
						mustStaticIta(
							plan.NewResolvedTable(table, nil, nil),
							mustIndexLookup(sql.NewIndexBuilder(idxTable1F).Equals(ctx, "mytable.f", 3.14).Build(ctx)),
						),
					),
				),
			),
		},
		{
			name: "single index to each of two aliased tables",
			node: plan.NewProject(
				[]sql.Expression{
					expression.NewGetFieldWithTable(0, types.Int32, "t1", "i", true),
				},
				plan.NewFilter(
					and(
						expression.NewEquals(
							expression.NewGetFieldWithTable(1, types.Float64, "t1", "f", true),
							expression.NewLiteral(3.14, types.Float64),
						),
						expression.NewEquals(
							expression.NewGetFieldWithTable(3, types.Int32, "t2", "i2", true),
							expression.NewLiteral(21, types.Int32),
						),
					),
					plan.NewCrossJoin(
						plan.NewTableAlias("t1",
							plan.NewResolvedTable(table, nil, nil),
						),
						plan.NewTableAlias("t2",
							plan.NewResolvedTable(table2, nil, nil),
						),
					),
				),
			),
			expected: plan.NewProject(
				[]sql.Expression{
					expression.NewGetFieldWithTable(0, types.Int32, "t1", "i", true),
				},
				plan.NewCrossJoin(
					plan.NewFilter(
						expression.NewEquals(
							expression.NewGetFieldWithTable(1, types.Float64, "t1", "f", true),
							expression.NewLiteral(3.14, types.Float64),
						),
						plan.NewTableAlias("t1",
							mustStaticIta(
								plan.NewResolvedTable(table, nil, nil),
								mustIndexLookup(sql.NewIndexBuilder(idxTable1F).Equals(ctx, "mytable.f", 3.14).Build(ctx)),
							),
						),
					),
					plan.NewFilter(
						expression.NewEquals(
							expression.NewGetFieldWithTable(0, types.Int32, "t2", "i2", true),
							expression.NewLiteral(21, types.Int32),
						),
						plan.NewTableAlias("t2",
							mustStaticIta(
								plan.NewResolvedTable(table2, nil, nil),
								mustIndexLookup(sql.NewIndexBuilder(idxTable2I2).Equals(ctx, "mytable2.i2", 21).Build(ctx)),
							),
						),
					),
				),
			),
		},
		{
			name: "single index to each of two aliased tables, extra predicates",
			node: plan.NewProject(
				[]sql.Expression{
					expression.NewGetFieldWithTable(0, types.Int32, "t1", "i", true),
				},
				plan.NewFilter(
					and(
						and(
							expression.NewEquals(
								expression.NewGetFieldWithTable(1, types.Float64, "t1", "f", true),
								expression.NewLiteral(3.14, types.Float64),
							),
							expression.NewEquals(
								expression.NewGetFieldWithTable(3, types.Int32, "t2", "i2", true),
								expression.NewLiteral(21, types.Int32),
							),
						),
						and(
							expression.NewEquals(
								expression.NewGetFieldWithTable(2, types.Text, "t1", "t", true),
								expression.NewLiteral("hello", types.Text),
							),
							expression.NewEquals(
								expression.NewGetFieldWithTable(5, types.Text, "t2", "t2", true),
								expression.NewLiteral("goodbye", types.Text),
							),
						),
					),
					plan.NewCrossJoin(
						plan.NewTableAlias("t1",
							plan.NewResolvedTable(table, nil, nil),
						),
						plan.NewTableAlias("t2",
							plan.NewResolvedTable(table2, nil, nil),
						),
					),
				),
			),
			expected: plan.NewProject(
				[]sql.Expression{
					expression.NewGetFieldWithTable(0, types.Int32, "t1", "i", true),
				},
				plan.NewCrossJoin(
					plan.NewFilter(
						and(
							expression.NewEquals(
								expression.NewGetFieldWithTable(1, types.Float64, "t1", "f", true),
								expression.NewLiteral(3.14, types.Float64),
							),
							expression.NewEquals(
								expression.NewGetFieldWithTable(2, types.Text, "t1", "t", true),
								expression.NewLiteral("hello", types.Text),
							),
						),
						plan.NewTableAlias("t1",
							mustStaticIta(
								plan.NewResolvedTable(table, nil, nil),
								mustIndexLookup(sql.NewIndexBuilder(idxTable1F).Equals(ctx, "mytable.f", 3.14).Build(ctx)),
							),
						),
					),
					plan.NewFilter(
						and(
							expression.NewEquals(
								expression.NewGetFieldWithTable(0, types.Int32, "t2", "i2", true),
								expression.NewLiteral(21, types.Int32),
							),
							expression.NewEquals(
								expression.NewGetFieldWithTable(2, types.Text, "t2", "t2", true),
								expression.NewLiteral("goodbye", types.Text),
							),
						),
						plan.NewTableAlias("t2",
							mustStaticIta(
								plan.NewResolvedTable(table2, nil, nil),
								mustIndexLookup(sql.NewIndexBuilder(idxTable2I2).Equals(ctx, "mytable2.i2", 21).Build(ctx)),
							),
						),
					),
				),
			),
		},
		{
			name: "two aliased tables, indexed join, no index on secondary table",
			node: plan.NewProject(
				[]sql.Expression{
					expression.NewGetFieldWithTable(0, types.Int32, "t1", "i", true),
				},
				plan.NewFilter(
					and(
						expression.NewEquals(
							expression.NewGetFieldWithTable(3, types.Int32, "t2", "i2", true),
							expression.NewLiteral(21, types.Int32),
						),
						and(
							expression.NewEquals(
								expression.NewGetFieldWithTable(0, types.Int32, "t1", "i", true),
								expression.NewLiteral(100, types.Int32),
							),
							expression.NewEquals(
								expression.NewGetFieldWithTable(5, types.Text, "t2", "t2", true),
								expression.NewLiteral("goodbye", types.Text),
							),
						),
					),
					plan.NewLookupJoin(plan.NewTableAlias("t1",
						plan.NewResolvedTable(table, nil, nil),
					), plan.NewTableAlias("t2",
						plan.NewResolvedTable(table2, nil, nil),
					), eq(gf(0, "mytable", "i"), gf(3, "mytable2", "i2"))),
				),
			),
			expected: plan.NewProject(
				[]sql.Expression{
					expression.NewGetFieldWithTable(0, types.Int32, "t1", "i", true),
				},
				plan.NewLookupJoin(plan.NewFilter(
					expression.NewEquals(
						expression.NewGetFieldWithTable(0, types.Int32, "t1", "i", true),
						expression.NewLiteral(100, types.Int32),
					),
					plan.NewTableAlias("t1",
						mustStaticIta(
							plan.NewResolvedTable(table, nil, nil),
							mustIndexLookup(sql.NewIndexBuilder(idxtable1I).Equals(ctx, "mytable.i", 100).Build(ctx)),
						),
					),
				), plan.NewFilter(
					and(
						expression.NewEquals(
							expression.NewGetFieldWithTable(0, types.Int32, "t2", "i2", true),
							expression.NewLiteral(21, types.Int32),
						),
						expression.NewEquals(
							expression.NewGetFieldWithTable(2, types.Text, "t2", "t2", true),
							expression.NewLiteral("goodbye", types.Text),
						),
					),
					plan.NewTableAlias("t2",
						plan.NewResolvedTable(table2, nil, nil),
					),
				), eq(gf(0, "mytable", "i"), gf(3, "mytable2", "i2"))),
			),
		},
		{
			name: "two aliased tables, indexed join, index on secondary table",
			node: plan.NewProject(
				[]sql.Expression{
					expression.NewGetFieldWithTable(0, types.Int32, "t1", "i", true),
				},
				plan.NewFilter(
					and(
						expression.NewEquals(
							expression.NewGetFieldWithTable(3, types.Int32, "t2", "i2", true),
							expression.NewLiteral(21, types.Int32),
						),
						and(
							expression.NewEquals(
								expression.NewGetFieldWithTable(0, types.Int32, "t1", "i", true),
								expression.NewLiteral(100, types.Int32),
							),
							expression.NewEquals(
								expression.NewGetFieldWithTable(5, types.Text, "t2", "t2", true),
								expression.NewLiteral("goodbye", types.Text),
							),
						),
					),
					plan.NewLookupJoin(plan.NewTableAlias("t1",
						plan.NewResolvedTable(table, nil, nil),
					), plan.NewTableAlias("t2",
						mustNewIta(
							plan.NewResolvedTable(table2, nil, nil),
							plan.NewLookupBuilder(idxTable2I2, []sql.Expression{gf(0, "t1", "i")}, []bool{false}),
						),
					), eq(gf(0, "mytable", "i"), gf(3, "mytable2", "i2"))),
				),
			),
			expected: plan.NewProject(
				[]sql.Expression{
					expression.NewGetFieldWithTable(0, types.Int32, "t1", "i", true),
				},
				plan.NewLookupJoin(plan.NewFilter(
					expression.NewEquals(
						expression.NewGetFieldWithTable(0, types.Int32, "t1", "i", true),
						expression.NewLiteral(100, types.Int32),
					),
					plan.NewTableAlias("t1",
						mustStaticIta(
							plan.NewResolvedTable(table, nil, nil),
							mustIndexLookup(sql.NewIndexBuilder(idxtable1I).Equals(ctx, "mytable.i", 100).Build(ctx)),
						),
					),
				), plan.NewFilter(
					and(
						expression.NewEquals(
							expression.NewGetFieldWithTable(0, types.Int32, "t2", "i2", true),
							expression.NewLiteral(21, types.Int32),
						),
						expression.NewEquals(
							expression.NewGetFieldWithTable(2, types.Text, "t2", "t2", true),
							expression.NewLiteral("goodbye", types.Text),
						),
					),
					plan.NewTableAlias("t2",
						mustNewIta(
							plan.NewResolvedTable(table2, nil, nil),
							plan.NewLookupBuilder(idxTable2I2, []sql.Expression{gf(0, "t1", "i")}, []bool{false}),
						),
					),
				), eq(gf(0, "mytable", "i"), gf(3, "mytable2", "i2"))),
			),
		},
		{
			name: "two aliased tables, left indexed join",
			node: plan.NewProject(
				[]sql.Expression{
					expression.NewGetFieldWithTable(0, types.Int32, "t1", "i", true),
				},
				plan.NewFilter(
					and(
						expression.NewEquals(
							expression.NewGetFieldWithTable(3, types.Int32, "t2", "i2", true),
							expression.NewLiteral(21, types.Int32),
						),
						and(
							expression.NewEquals(
								expression.NewGetFieldWithTable(0, types.Int32, "t1", "i", true),
								expression.NewLiteral(100, types.Int32),
							),
							expression.NewEquals(
								expression.NewGetFieldWithTable(5, types.Text, "t2", "t2", true),
								expression.NewLiteral("goodbye", types.Text),
							),
						),
					),
					plan.NewLeftOuterLookupJoin(plan.NewTableAlias("t1",
						plan.NewResolvedTable(table, nil, nil),
					), plan.NewTableAlias("t2",
						plan.NewResolvedTable(table2, nil, nil),
					), eq(gf(0, "mytable", "i"), gf(3, "mytable2", "i2"))),
				),
			),
			expected: plan.NewProject(
				[]sql.Expression{
					expression.NewGetFieldWithTable(0, types.Int32, "t1", "i", true),
				},
				plan.NewFilter(
					and(
						expression.NewEquals(
							expression.NewGetFieldWithTable(3, types.Int32, "t2", "i2", true),
							expression.NewLiteral(21, types.Int32),
						),
						expression.NewEquals(
							expression.NewGetFieldWithTable(5, types.Text, "t2", "t2", true),
							expression.NewLiteral("goodbye", types.Text),
						),
					),
					plan.NewLeftOuterLookupJoin(plan.NewFilter(
						expression.NewEquals(
							expression.NewGetFieldWithTable(0, types.Int32, "t1", "i", true),
							expression.NewLiteral(100, types.Int32),
						),
						plan.NewTableAlias("t1",
							mustStaticIta(
								plan.NewResolvedTable(table, nil, nil),
								mustIndexLookup(sql.NewIndexBuilder(idxtable1I).Equals(ctx, "mytable.i", 100).Build(ctx)),
							),
						),
					), plan.NewTableAlias("t2",
						plan.NewResolvedTable(table2, nil, nil),
					), eq(gf(0, "mytable", "i"), gf(3, "mytable2", "i2"))),
				),
			),
		},
	}

	runTestCases(t, sql.NewEmptyContext(), tests, a, getRule(pushdownFiltersId))
}

func mustIndexLookup(lookup sql.IndexLookup, err error) sql.IndexLookup {
	if err != nil {
		panic(err)
	}
	return lookup
}

// newFilteredTableWithFilters creates a new memory.FilteredTable object and sets the specified filters.
func newFilteredTableWithFilters(table *memory.FilteredTable, filters []sql.Expression) *memory.FilteredTable {
	newFilteredTable := memory.NewFilteredTable(table.Name(), table.PrimaryKeySchema(), nil)
	newFilteredTable.WithFilters(sql.NewEmptyContext(), filters)

	return newFilteredTable
}

// newFilteredTableWithProjections creates a new memory.FilteredTable object and sets the specified column projections.
func newFilteredTableWithProjections(table *memory.FilteredTable, colNames []string) *memory.FilteredTable {
	newFilteredTable := memory.NewFilteredTable(table.Name(), table.PrimaryKeySchema(), nil)
	newFilteredTable.WithProjections(colNames)

	return newFilteredTable
}

// newFilteredTableWithProjectionsAndFilters creates a new memory.FilteredTable object and sets the specified filters
// and column projections.
func newFilteredTableWithProjectionsAndFilters(table *memory.FilteredTable, colNames []string, filters []sql.Expression) *memory.FilteredTable {
	newFilteredTable := memory.NewFilteredTable(table.Name(), table.PrimaryKeySchema(), nil)
	newFilteredTable.WithFilters(sql.NewEmptyContext(), filters)
	newFilteredTable.WithProjections(colNames)

	return newFilteredTable
}

func mustNewIta(rt *plan.ResolvedTable, builder *plan.LookupBuilder) *plan.IndexedTableAccess {
	ret, err := plan.NewIndexedAccessForResolvedTable(rt, builder)
	if err != nil {
		panic(err)
	}
	return ret
}

func mustStaticIta(rt *plan.ResolvedTable, lookup sql.IndexLookup) *plan.IndexedTableAccess {
	ret, err := plan.NewStaticIndexedAccessForResolvedTable(rt, lookup)
	if err != nil {
		panic(err)
	}
	return ret
}
