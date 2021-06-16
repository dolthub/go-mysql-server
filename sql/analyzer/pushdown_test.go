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
)

func TestPushdownProjectionToTables(t *testing.T) {
	table := memory.NewTable("mytable", sql.Schema{
		{Name: "i", Type: sql.Int32, Source: "mytable"},
		{Name: "f", Type: sql.Float64, Source: "mytable"},
		{Name: "t", Type: sql.Text, Source: "mytable"},
	})

	table2 := memory.NewTable("mytable2", sql.Schema{
		{Name: "i2", Type: sql.Int32, Source: "mytable2"},
		{Name: "f2", Type: sql.Float64, Source: "mytable2"},
		{Name: "t2", Type: sql.Text, Source: "mytable2"},
	})

	db := memory.NewDatabase("mydb")
	db.AddTable("mytable", table)
	db.AddTable("mytable2", table2)

	catalog := sql.NewCatalog()
	catalog.AddDatabase(db)
	a := NewDefault(catalog)

	// TODO: test interaction with filtered tables
	tests := []analyzerFnTestCase{
		{
			name: "pushdown projections to tables",
			node: plan.NewProject(
				[]sql.Expression{
					expression.NewGetFieldWithTable(2, sql.Text, "mytable2", "t2", false),
				},
				plan.NewFilter(
					expression.NewOr(
						expression.NewEquals(
							expression.NewGetFieldWithTable(1, sql.Float64, "mytable", "f", false),
							expression.NewLiteral(3.14, sql.Float64),
						),
						expression.NewIsNull(
							expression.NewGetFieldWithTable(0, sql.Int32, "mytable2", "i2", false),
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
					expression.NewGetFieldWithTable(5, sql.Text, "mytable2", "t2", false),
				},
				plan.NewFilter(
					expression.NewOr(
						expression.NewEquals(
							expression.NewGetFieldWithTable(1, sql.Float64, "mytable", "f", false),
							expression.NewLiteral(3.14, sql.Float64),
						),
						expression.NewIsNull(
							expression.NewGetFieldWithTable(3, sql.Int32, "mytable2", "i2", false),
						),
					),
					plan.NewCrossJoin(
						plan.NewDecoratedNode("Projected table access on [f]", plan.NewResolvedTable(table.WithProjection([]string{"f"}), nil, nil)),
						plan.NewDecoratedNode("Projected table access on [t2 i2]", plan.NewResolvedTable(table2.WithProjection([]string{"t2", "i2"}), nil, nil)),
					),
				),
			),
		},
	}

	runTestCases(t, sql.NewEmptyContext(), tests, a, getRule("pushdown_projections"))
}

func TestPushdownFilterToTables(t *testing.T) {
	table := memory.NewFilteredTable("mytable", sql.Schema{
		{Name: "i", Type: sql.Int32, Source: "mytable"},
		{Name: "f", Type: sql.Float64, Source: "mytable"},
		{Name: "t", Type: sql.Text, Source: "mytable"},
	})

	table2 := memory.NewFilteredTable("mytable2", sql.Schema{
		{Name: "i2", Type: sql.Int32, Source: "mytable2"},
		{Name: "f2", Type: sql.Float64, Source: "mytable2"},
		{Name: "t2", Type: sql.Text, Source: "mytable2"},
	})

	db := memory.NewDatabase("mydb")
	db.AddTable("mytable", table)
	db.AddTable("mytable2", table2)

	catalog := sql.NewCatalog()
	catalog.AddDatabase(db)
	a := NewDefault(catalog)

	tests := []analyzerFnTestCase{
		{
			name: "pushdown filter to tables",
			node: plan.NewProject(
				[]sql.Expression{
					expression.NewGetFieldWithTable(2, sql.Text, "mytable2", "t2", false),
				},
				plan.NewFilter(
					expression.NewAnd(
						expression.NewEquals(
							expression.NewGetFieldWithTable(1, sql.Float64, "mytable", "f", false),
							expression.NewLiteral(3.14, sql.Float64),
						),
						expression.NewIsNull(
							expression.NewGetFieldWithTable(0, sql.Int32, "mytable2", "i2", false),
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
					expression.NewGetFieldWithTable(5, sql.Text, "mytable2", "t2", false),
				},
				plan.NewCrossJoin(
					plan.NewDecoratedNode("Filtered table access on [(mytable.f = 3.14)]", plan.NewResolvedTable(table.WithFilters(sql.NewEmptyContext(), []sql.Expression{
						expression.NewEquals(
							expression.NewGetFieldWithTable(1, sql.Float64, "mytable", "f", false),
							expression.NewLiteral(3.14, sql.Float64),
						),
					}), nil, nil)),
					plan.NewDecoratedNode("Filtered table access on [mytable2.i2 IS NULL]", plan.NewResolvedTable(table2.WithFilters(sql.NewEmptyContext(), []sql.Expression{
						expression.NewIsNull(
							expression.NewGetFieldWithTable(0, sql.Int32, "mytable2", "i2", false),
						),
					}), nil, nil)),
				),
			),
		},
		{
			name: "push filters down onto projected table",
			node: plan.NewProject(
				[]sql.Expression{
					expression.NewGetFieldWithTable(1, sql.Text, "mytable2", "t2", false),
				},
				plan.NewFilter(
					expression.NewAnd(
						expression.NewEquals(
							expression.NewGetFieldWithTable(0, sql.Float64, "mytable", "f", false),
							expression.NewLiteral(3.14, sql.Float64),
						),
						expression.NewIsNull(
							expression.NewGetFieldWithTable(2, sql.Int32, "mytable2", "i2", false),
						),
					),
					plan.NewCrossJoin(
						plan.NewDecoratedNode("Projected table access on [f]",
							plan.NewResolvedTable(table.WithProjection([]string{"f"}), nil, nil),
						),
						plan.NewDecoratedNode("Projected table access on [t2 i2]",
							plan.NewResolvedTable(table2.WithProjection([]string{"t2", "i2"}), nil, nil),
						),
					),
				),
			),
			expected: plan.NewProject(
				[]sql.Expression{
					expression.NewGetFieldWithTable(5, sql.Text, "mytable2", "t2", false),
				},
				plan.NewCrossJoin(
					plan.NewDecoratedNode("Projected table access on [f]",
						plan.NewDecoratedNode("Filtered table access on [(mytable.f = 3.14)]",
							plan.NewResolvedTable(table.WithProjection([]string{"f"}).(*memory.FilteredTable).WithFilters(sql.NewEmptyContext(), []sql.Expression{
								eq(expression.NewGetFieldWithTable(1, sql.Float64, "mytable", "f", false), expression.NewLiteral(3.14, sql.Float64)),
							}), nil, nil),
						),
					),
					plan.NewDecoratedNode("Projected table access on [t2 i2]",
						plan.NewDecoratedNode("Filtered table access on [mytable2.i2 IS NULL]",
							plan.NewResolvedTable(table2.WithProjection([]string{"t2", "i2"}).(*memory.FilteredTable).WithFilters(sql.NewEmptyContext(), []sql.Expression{
								expression.NewIsNull(expression.NewGetFieldWithTable(0, sql.Int32, "mytable2", "i2", false)),
							}), nil, nil),
						),
					),
				),
			),
		},
	}

	runTestCases(t, sql.NewEmptyContext(), tests, a, getRule("pushdown_filters"))
}

func TestPushdownFiltersAboveTables(t *testing.T) {
	table := memory.NewTable("mytable", sql.Schema{
		{Name: "i", Type: sql.Int32, Source: "mytable"},
		{Name: "f", Type: sql.Float64, Source: "mytable"},
		{Name: "t", Type: sql.Text, Source: "mytable"},
	})

	table2 := memory.NewTable("mytable2", sql.Schema{
		{Name: "i2", Type: sql.Int32, Source: "mytable2"},
		{Name: "f2", Type: sql.Float64, Source: "mytable2"},
		{Name: "t2", Type: sql.Text, Source: "mytable2"},
	})

	db := memory.NewDatabase("mydb")
	db.AddTable("mytable", table)
	db.AddTable("mytable2", table2)

	catalog := sql.NewCatalog()
	catalog.AddDatabase(db)
	a := NewDefault(catalog)

	tests := []analyzerFnTestCase{
		{
			name: "pushdown filters to under join node",
			node: plan.NewProject(
				[]sql.Expression{
					expression.NewGetFieldWithTable(0, sql.Int32, "mytable", "i", true),
				},
				plan.NewFilter(
					and(
						expression.NewEquals(
							expression.NewGetFieldWithTable(1, sql.Float64, "mytable", "f", true),
							expression.NewLiteral(3.14, sql.Float64),
						),
						and(
							expression.NewEquals(
								expression.NewGetFieldWithTable(3, sql.Int32, "mytable2", "i2", true),
								expression.NewLiteral(21, sql.Int32),
							),
							expression.NewEquals(
								expression.NewGetFieldWithTable(5, sql.Int32, "mytable2", "t2", true),
								expression.NewLiteral("hello", sql.Text),
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
					expression.NewGetFieldWithTable(0, sql.Int32, "mytable", "i", true),
				},
				plan.NewCrossJoin(
					plan.NewFilter(
						expression.NewEquals(
							expression.NewGetFieldWithTable(1, sql.Float64, "mytable", "f", true),
							expression.NewLiteral(3.14, sql.Float64),
						),
						plan.NewResolvedTable(table, nil, nil),
					),
					plan.NewFilter(
						and(
							expression.NewEquals(
								expression.NewGetFieldWithTable(0, sql.Int32, "mytable2", "i2", true),
								expression.NewLiteral(21, sql.Int32),
							),
							expression.NewEquals(
								expression.NewGetFieldWithTable(2, sql.Int32, "mytable2", "t2", true),
								expression.NewLiteral("hello", sql.Text),
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
					expression.NewGetFieldWithTable(0, sql.Int32, "t1", "i", true),
				},
				plan.NewFilter(
					and(
						expression.NewEquals(
							expression.NewGetFieldWithTable(1, sql.Float64, "t1", "f", true),
							expression.NewLiteral(3.14, sql.Float64),
						),
						and(
							expression.NewEquals(
								expression.NewGetFieldWithTable(3, sql.Int32, "t2", "i2", true),
								expression.NewLiteral(21, sql.Int32),
							),
							expression.NewEquals(
								expression.NewGetFieldWithTable(5, sql.Int32, "t2", "t2", true),
								expression.NewLiteral("hello", sql.Text),
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
					expression.NewGetFieldWithTable(0, sql.Int32, "t1", "i", true),
				},
				plan.NewCrossJoin(
					plan.NewFilter(
						expression.NewEquals(
							expression.NewGetFieldWithTable(1, sql.Float64, "t1", "f", true),
							expression.NewLiteral(3.14, sql.Float64),
						),
						plan.NewTableAlias("t1",
							plan.NewResolvedTable(table, nil, nil),
						),
					),
					plan.NewFilter(
						and(
							expression.NewEquals(
								expression.NewGetFieldWithTable(0, sql.Int32, "t2", "i2", true),
								expression.NewLiteral(21, sql.Int32),
							),
							expression.NewEquals(
								expression.NewGetFieldWithTable(2, sql.Int32, "t2", "t2", true),
								expression.NewLiteral("hello", sql.Text),
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
					expression.NewGetFieldWithTable(2, sql.Text, "mytable2", "t2", false),
				},
				plan.NewFilter(
					expression.NewAnd(
						expression.NewEquals(
							expression.NewGetFieldWithTable(1, sql.Float64, "mytable", "f", false),
							expression.NewLiteral(3.14, sql.Float64),
						),
						expression.NewIsNull(
							expression.NewGetFieldWithTable(0, sql.Int32, "mytable2", "i2", false),
						),
					),
					plan.NewLeftJoin(
						plan.NewResolvedTable(table, nil, nil),
						plan.NewResolvedTable(table2, nil, nil),
						eq(gf(0, "mytable", "i"), gf(3, "mytable2", "i2")),
					),
				),
			),
			expected: plan.NewProject(
				[]sql.Expression{
					expression.NewGetFieldWithTable(5, sql.Text, "mytable2", "t2", false),
				},
				plan.NewFilter(
					expression.NewIsNull(
						expression.NewGetFieldWithTable(3, sql.Int32, "mytable2", "i2", false),
					),
					plan.NewLeftJoin(
						plan.NewFilter(
							expression.NewEquals(
								expression.NewGetFieldWithTable(1, sql.Float64, "mytable", "f", false),
								expression.NewLiteral(3.14, sql.Float64),
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
			name: "pushdown filter to right join",
			node: plan.NewProject(
				[]sql.Expression{
					expression.NewGetFieldWithTable(2, sql.Text, "mytable2", "t2", false),
				},
				plan.NewFilter(
					expression.NewAnd(
						expression.NewEquals(
							expression.NewGetFieldWithTable(1, sql.Float64, "mytable", "f", false),
							expression.NewLiteral(3.14, sql.Float64),
						),
						expression.NewIsNull(
							expression.NewGetFieldWithTable(0, sql.Int32, "mytable2", "i2", false),
						),
					),
					plan.NewRightJoin(
						plan.NewResolvedTable(table, nil, nil),
						plan.NewResolvedTable(table2, nil, nil),
						eq(gf(0, "mytable", "i"), gf(3, "mytable2", "i2")),
					),
				),
			),
			expected: plan.NewProject(
				[]sql.Expression{
					expression.NewGetFieldWithTable(5, sql.Text, "mytable2", "t2", false),
				},
				plan.NewFilter(
					expression.NewEquals(
						expression.NewGetFieldWithTable(1, sql.Float64, "mytable", "f", false),
						expression.NewLiteral(3.14, sql.Float64),
					),
					plan.NewRightJoin(
						plan.NewResolvedTable(table, nil, nil),
						plan.NewFilter(
							expression.NewIsNull(
								expression.NewGetFieldWithTable(0, sql.Int32, "mytable2", "i2", false),
							),
							plan.NewResolvedTable(table2, nil, nil),
						),
						eq(gf(0, "mytable", "i"), gf(3, "mytable2", "i2")),
					),
				),
			),
		},
		{
			name: "filter contains join condition",
			node: plan.NewProject(
				[]sql.Expression{
					expression.NewGetFieldWithTable(0, sql.Int32, "mytable", "i", true),
				},
				plan.NewFilter(
					and(
						expression.NewEquals(
							expression.NewGetFieldWithTable(1, sql.Float64, "mytable", "f", true),
							expression.NewLiteral(3.14, sql.Float64),
						),
						and(
							expression.NewEquals(
								expression.NewGetFieldWithTable(0, sql.Int32, "mytable", "i", true),
								expression.NewGetFieldWithTable(3, sql.Int32, "mytable2", "i2", true),
							),
							expression.NewEquals(
								expression.NewGetFieldWithTable(3, sql.Int32, "mytable2", "i2", true),
								expression.NewLiteral(20, sql.Int32),
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
					expression.NewGetFieldWithTable(0, sql.Int32, "mytable", "i", true),
				},
				plan.NewFilter(
					expression.NewEquals(
						expression.NewGetFieldWithTable(0, sql.Int32, "mytable", "i", true),
						expression.NewGetFieldWithTable(3, sql.Int32, "mytable2", "i2", true),
					),
					plan.NewCrossJoin(
						plan.NewFilter(
							expression.NewEquals(
								expression.NewGetFieldWithTable(1, sql.Float64, "mytable", "f", true),
								expression.NewLiteral(3.14, sql.Float64),
							),
							plan.NewResolvedTable(table, nil, nil),
						),
						plan.NewFilter(
							expression.NewEquals(
								expression.NewGetFieldWithTable(0, sql.Int32, "mytable2", "i2", true),
								expression.NewLiteral(20, sql.Int32),
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
					expression.NewGetFieldWithTable(0, sql.Int32, "mytable", "i", true),
				},
				plan.NewFilter(
					plan.NewInSubquery(
						expression.NewGetFieldWithTable(1, sql.Float64, "mytable", "f", true),
						plan.NewSubquery(
							plan.NewProject(
								[]sql.Expression{
									expression.NewLiteral(1, sql.Int32),
								},
								plan.EmptyTable,
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

	runTestCases(t, sql.NewEmptyContext(), tests, a, getRule("pushdown_filters"))
}

// TODO: this needs tests for pushing a merged index lookup down to a table
func TestPushdownIndex(t *testing.T) {
	require := require.New(t)

	myTableF := &sql.Column{Name: "f", Type: sql.Float64, Source: "mytable"}
	myTableI := &sql.Column{Name: "i", Type: sql.Int32, Source: "mytable", PrimaryKey: true}
	table := memory.NewTable("mytable", sql.Schema{
		myTableI,
		myTableF,
		{Name: "t", Type: sql.Text, Source: "mytable"},
	})

	table.EnablePrimaryKeyIndexes()
	err := table.CreateIndex(sql.NewEmptyContext(), "f", sql.IndexUsing_BTree, sql.IndexConstraint_None, []sql.IndexColumn{
		{
			Name:   "f",
			Length: 0,
		},
	}, "")
	require.NoError(err)

	table1Idxes, err := table.GetIndexes(sql.NewEmptyContext())
	require.NoError(err)
	idxtable1I := table1Idxes[0]
	fmt.Sprintf("%v", idxtable1I)
	idxTable1F := table1Idxes[1]

	mytable2I := &sql.Column{Name: "i2", Type: sql.Int32, Source: "mytable2", PrimaryKey: true}
	table2 := memory.NewTable("mytable2", sql.Schema{
		mytable2I,
		{Name: "f2", Type: sql.Float64, Source: "mytable2"},
		{Name: "t2", Type: sql.Text, Source: "mytable2"},
	})

	table2.EnablePrimaryKeyIndexes()
	table2Idxes, err := table2.GetIndexes(sql.NewEmptyContext())
	require.NoError(err)
	idxTable2I2 := table2Idxes[0]

	db := memory.NewDatabase("")
	db.AddTable("mytable", table)
	db.AddTable("mytable2", table2)

	catalog := sql.NewCatalog()
	catalog.AddDatabase(db)
	a := NewDefault(catalog)

	tests := []analyzerFnTestCase{
		{
			name: "single index",
			node: plan.NewProject(
				[]sql.Expression{
					expression.NewGetFieldWithTable(0, sql.Int32, "mytable", "i", true),
				},
				plan.NewFilter(
					expression.NewEquals(
						expression.NewGetFieldWithTable(1, sql.Float64, "mytable", "f", true),
						expression.NewLiteral(3.14, sql.Float64),
					),
					plan.NewResolvedTable(table, nil, nil),
				),
			),
			expected: plan.NewProject(
				[]sql.Expression{
					expression.NewGetFieldWithTable(0, sql.Int32, "mytable", "i", true),
				},
				plan.NewFilter(
					expression.NewEquals(
						expression.NewGetFieldWithTable(1, sql.Float64, "mytable", "f", true),
						expression.NewLiteral(3.14, sql.Float64),
					),
					plan.NewStaticIndexedTableAccess(
						plan.NewResolvedTable(table, nil, nil),
						mustIndexLookup(idxTable1F.Get(3.14)),
						idxTable1F,
						[]sql.Expression{gfCol(1, myTableF)},
					),
				),
			),
		},
		{
			name: "single index with extra predicate",
			node: plan.NewProject(
				[]sql.Expression{
					expression.NewGetFieldWithTable(0, sql.Int32, "mytable", "i", true),
				},
				plan.NewFilter(
					and(
						expression.NewEquals(
							expression.NewGetFieldWithTable(1, sql.Float64, "mytable", "f", true),
							expression.NewLiteral(3.14, sql.Float64),
						),
						expression.NewEquals(
							expression.NewGetFieldWithTable(2, sql.Text, "mytable", "t", true),
							expression.NewLiteral("hello", sql.Text),
						),
					),
					plan.NewResolvedTable(table, nil, nil),
				),
			),
			expected: plan.NewProject(
				[]sql.Expression{
					expression.NewGetFieldWithTable(0, sql.Int32, "mytable", "i", true),
				},
				plan.NewFilter(
					and(
						expression.NewEquals(
							expression.NewGetFieldWithTable(1, sql.Float64, "mytable", "f", true),
							expression.NewLiteral(3.14, sql.Float64),
						),
						expression.NewEquals(
							expression.NewGetFieldWithTable(2, sql.Text, "mytable", "t", true),
							expression.NewLiteral("hello", sql.Text),
						),
					),
					plan.NewStaticIndexedTableAccess(
						plan.NewResolvedTable(table, nil, nil),
						mustIndexLookup(idxTable1F.Get(3.14)),
						idxTable1F,
						[]sql.Expression{gfCol(1, myTableF)},
					),
				),
			),
		},
		{
			name: "single index with extra predicates",
			node: plan.NewProject(
				[]sql.Expression{
					expression.NewGetFieldWithTable(0, sql.Int32, "mytable", "i", true),
				},
				plan.NewFilter(
					and(
						expression.NewEquals(
							expression.NewGetFieldWithTable(1, sql.Float64, "mytable", "f", true),
							expression.NewLiteral(3.14, sql.Float64),
						),
						and(
							expression.NewEquals(
								expression.NewGetFieldWithTable(2, sql.Text, "mytable", "t", true),
								expression.NewLiteral("hello", sql.Text),
							),
							expression.NewEquals(
								expression.NewGetFieldWithTable(2, sql.Text, "mytable", "t", true),
								expression.NewLiteral("goodbye", sql.Text),
							),
						),
					),
					plan.NewResolvedTable(table, nil, nil),
				),
			),
			expected: plan.NewProject(
				[]sql.Expression{
					expression.NewGetFieldWithTable(0, sql.Int32, "mytable", "i", true),
				},
				plan.NewFilter(
					and(
						expression.NewEquals(
							expression.NewGetFieldWithTable(1, sql.Float64, "mytable", "f", true),
							expression.NewLiteral(3.14, sql.Float64),
						),
						and(
							expression.NewEquals(
								expression.NewGetFieldWithTable(2, sql.Text, "mytable", "t", true),
								expression.NewLiteral("hello", sql.Text),
							),
							expression.NewEquals(
								expression.NewGetFieldWithTable(2, sql.Text, "mytable", "t", true),
								expression.NewLiteral("goodbye", sql.Text),
							),
						),
					),
					plan.NewStaticIndexedTableAccess(
						plan.NewResolvedTable(table, nil, nil),
						mustIndexLookup(idxTable1F.Get(3.14)),
						idxTable1F,
						[]sql.Expression{gfCol(1, myTableF)},
					),
				),
			),
		},
		{
			name: "single index to each of two tables",
			node: plan.NewProject(
				[]sql.Expression{
					expression.NewGetFieldWithTable(0, sql.Int32, "mytable", "i", true),
				},
				plan.NewFilter(
					and(
						expression.NewEquals(
							expression.NewGetFieldWithTable(1, sql.Float64, "mytable", "f", true),
							expression.NewLiteral(3.14, sql.Float64),
						),
						expression.NewEquals(
							expression.NewGetFieldWithTable(3, sql.Int32, "mytable2", "i2", true),
							expression.NewLiteral(21, sql.Int32),
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
					expression.NewGetFieldWithTable(0, sql.Int32, "mytable", "i", true),
				},
				plan.NewCrossJoin(
					plan.NewFilter(
						expression.NewEquals(
							expression.NewGetFieldWithTable(1, sql.Float64, "mytable", "f", true),
							expression.NewLiteral(3.14, sql.Float64),
						),
						plan.NewStaticIndexedTableAccess(
							plan.NewResolvedTable(table, nil, nil),
							mustIndexLookup(idxTable1F.Get(3.14)),
							idxTable1F,
							[]sql.Expression{gfCol(1, myTableF)},
						),
					),
					plan.NewFilter(
						expression.NewEquals(
							expression.NewGetFieldWithTable(0, sql.Int32, "mytable2", "i2", true),
							expression.NewLiteral(21, sql.Int32),
						),
						plan.NewStaticIndexedTableAccess(
							plan.NewResolvedTable(table2, nil, nil),
							mustIndexLookup(idxTable2I2.Get(21)),
							idxTable2I2,
							[]sql.Expression{gfCol(0, mytable2I)},
						),
					),
				),
			),
		},
		{
			// This scenario can't happen in the current analyzer rule ordering. But the rule should behave correctly anyway.
			name: "single index to each of two tables, filters already pushed down",
			node: plan.NewProject(
				[]sql.Expression{
					expression.NewGetFieldWithTable(0, sql.Int32, "mytable", "i", true),
				},
				plan.NewCrossJoin(
					plan.NewFilter(
						expression.NewEquals(
							expression.NewGetFieldWithTable(1, sql.Float64, "mytable", "f", true),
							expression.NewLiteral(3.14, sql.Float64),
						),
						plan.NewResolvedTable(table, nil, nil),
					),
					plan.NewFilter(
						expression.NewEquals(
							expression.NewGetFieldWithTable(0, sql.Int32, "mytable2", "i2", true),
							expression.NewLiteral(21, sql.Int32),
						),
						plan.NewResolvedTable(table2, nil, nil),
					),
				),
			),
			expected: plan.NewProject(
				[]sql.Expression{
					expression.NewGetFieldWithTable(0, sql.Int32, "mytable", "i", true),
				},
				plan.NewCrossJoin(
					plan.NewFilter(
						expression.NewEquals(
							expression.NewGetFieldWithTable(1, sql.Float64, "mytable", "f", true),
							expression.NewLiteral(3.14, sql.Float64),
						),
						plan.NewStaticIndexedTableAccess(
							plan.NewResolvedTable(table, nil, nil),
							mustIndexLookup(idxTable1F.Get(3.14)),
							idxTable1F,
							[]sql.Expression{gfCol(1, myTableF)},
						),
					),
					plan.NewFilter(
						expression.NewEquals(
							expression.NewGetFieldWithTable(0, sql.Int32, "mytable2", "i2", true),
							expression.NewLiteral(21, sql.Int32),
						),
						plan.NewStaticIndexedTableAccess(
							plan.NewResolvedTable(table2, nil, nil),
							mustIndexLookup(idxTable2I2.Get(21)),
							idxTable2I2,
							[]sql.Expression{gfCol(0, mytable2I)},
						),
					),
				),
			),
		},
		{
			name: "Index already pushed down, no change to plan",
			node: plan.NewProject(
				[]sql.Expression{
					expression.NewGetFieldWithTable(0, sql.Int32, "mytable", "i", true),
				},
				plan.NewCrossJoin(
					plan.NewFilter(
						expression.NewEquals(
							expression.NewGetFieldWithTable(1, sql.Float64, "mytable", "f", true),
							expression.NewLiteral(3.14, sql.Float64),
						),
						plan.NewStaticIndexedTableAccess(
							plan.NewResolvedTable(table, nil, nil),
							mustIndexLookup(idxTable1F.Get(3.14)),
							idxTable1F,
							[]sql.Expression{eq(gfCol(1, myTableF), litT(3.14, sql.Float64))},
						),
					),
					plan.NewFilter(
						expression.NewEquals(
							expression.NewGetFieldWithTable(0, sql.Int32, "mytable2", "i2", true),
							expression.NewLiteral(21, sql.Int32),
						),
						plan.NewStaticIndexedTableAccess(
							plan.NewResolvedTable(table2, nil, nil),
							mustIndexLookup(idxTable2I2.Get(21)),
							idxTable2I2,
							[]sql.Expression{eq(gfCol(0, mytable2I), litT(21, sql.Int32))},
						),
					),
				),
			),
		},
		{
			name: "single index to each of two tables, extra predicates",
			node: plan.NewProject(
				[]sql.Expression{
					expression.NewGetFieldWithTable(0, sql.Int32, "mytable", "i", true),
				},
				plan.NewFilter(
					and(
						expression.NewEquals(
							expression.NewGetFieldWithTable(1, sql.Float64, "mytable", "f", true),
							expression.NewLiteral(3.14, sql.Float64),
						),
						and(
							expression.NewEquals(
								expression.NewGetFieldWithTable(3, sql.Int32, "mytable2", "i2", true),
								expression.NewLiteral(21, sql.Int32),
							),
							expression.NewEquals(
								expression.NewGetFieldWithTable(5, sql.Int32, "mytable2", "t2", true),
								expression.NewLiteral("hello", sql.Text),
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
					expression.NewGetFieldWithTable(0, sql.Int32, "mytable", "i", true),
				},
				plan.NewCrossJoin(
					plan.NewFilter(
						expression.NewEquals(
							expression.NewGetFieldWithTable(1, sql.Float64, "mytable", "f", true),
							expression.NewLiteral(3.14, sql.Float64),
						),
						plan.NewStaticIndexedTableAccess(
							plan.NewResolvedTable(table, nil, nil),
							mustIndexLookup(idxTable1F.Get(3.14)),
							idxTable1F,
							[]sql.Expression{gfCol(1, myTableF)},
						),
					),
					plan.NewFilter(
						and(
							expression.NewEquals(
								expression.NewGetFieldWithTable(0, sql.Int32, "mytable2", "i2", true),
								expression.NewLiteral(21, sql.Int32),
							),
							expression.NewEquals(
								expression.NewGetFieldWithTable(2, sql.Int32, "mytable2", "t2", true),
								expression.NewLiteral("hello", sql.Text),
							),
						),
						plan.NewStaticIndexedTableAccess(
							plan.NewResolvedTable(table2, nil, nil),
							mustIndexLookup(idxTable2I2.Get(21)),
							idxTable2I2,
							[]sql.Expression{gfCol(0, mytable2I)},
						),
					),
				),
			),
		},
		{
			name: "single index on aliased table",
			node: plan.NewProject(
				[]sql.Expression{
					expression.NewGetFieldWithTable(0, sql.Int32, "t1", "i", true),
				},
				plan.NewFilter(
					expression.NewEquals(
						expression.NewGetFieldWithTable(1, sql.Float64, "t1", "f", true),
						expression.NewLiteral(3.14, sql.Float64),
					),
					plan.NewTableAlias("t1",
						plan.NewResolvedTable(table, nil, nil),
					),
				),
			),
			expected: plan.NewProject(
				[]sql.Expression{
					expression.NewGetFieldWithTable(0, sql.Int32, "t1", "i", true),
				},
				plan.NewFilter(
					expression.NewEquals(
						expression.NewGetFieldWithTable(1, sql.Float64, "t1", "f", true),
						expression.NewLiteral(3.14, sql.Float64),
					),
					plan.NewTableAlias("t1",
						plan.NewStaticIndexedTableAccess(
							plan.NewResolvedTable(table, nil, nil),
							mustIndexLookup(idxTable1F.Get(3.14)),
							idxTable1F,
							[]sql.Expression{gfColAlias(1, myTableF, "t1")},
						),
					),
				),
			),
		},
		{
			name: "single index on aliased table, extra predicate",
			node: plan.NewProject(
				[]sql.Expression{
					expression.NewGetFieldWithTable(0, sql.Int32, "t1", "i", true),
				},
				plan.NewFilter(
					and(
						expression.NewEquals(
							expression.NewGetFieldWithTable(1, sql.Float64, "t1", "f", true),
							expression.NewLiteral(3.14, sql.Float64),
						),
						expression.NewEquals(
							expression.NewGetFieldWithTable(2, sql.Text, "t1", "t", true),
							expression.NewLiteral("hello", sql.Text),
						),
					),
					plan.NewTableAlias("t1",
						plan.NewResolvedTable(table, nil, nil),
					),
				),
			),
			expected: plan.NewProject(
				[]sql.Expression{
					expression.NewGetFieldWithTable(0, sql.Int32, "t1", "i", true),
				},
				plan.NewFilter(
					and(
						expression.NewEquals(
							expression.NewGetFieldWithTable(1, sql.Float64, "t1", "f", true),
							expression.NewLiteral(3.14, sql.Float64),
						),
						expression.NewEquals(
							expression.NewGetFieldWithTable(2, sql.Text, "t1", "t", true),
							expression.NewLiteral("hello", sql.Text),
						),
					),
					plan.NewTableAlias("t1",
						plan.NewStaticIndexedTableAccess(
							plan.NewResolvedTable(table, nil, nil),
							mustIndexLookup(idxTable1F.Get(3.14)),
							idxTable1F,
							[]sql.Expression{gfColAlias(1, myTableF, "t1")},
						),
					),
				),
			),
		},
		{
			name: "single index to each of two aliased tables",
			node: plan.NewProject(
				[]sql.Expression{
					expression.NewGetFieldWithTable(0, sql.Int32, "t1", "i", true),
				},
				plan.NewFilter(
					and(
						expression.NewEquals(
							expression.NewGetFieldWithTable(1, sql.Float64, "t1", "f", true),
							expression.NewLiteral(3.14, sql.Float64),
						),
						expression.NewEquals(
							expression.NewGetFieldWithTable(3, sql.Int32, "t2", "i2", true),
							expression.NewLiteral(21, sql.Int32),
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
					expression.NewGetFieldWithTable(0, sql.Int32, "t1", "i", true),
				},
				plan.NewCrossJoin(
					plan.NewFilter(
						expression.NewEquals(
							expression.NewGetFieldWithTable(1, sql.Float64, "t1", "f", true),
							expression.NewLiteral(3.14, sql.Float64),
						),
						plan.NewTableAlias("t1",
							plan.NewStaticIndexedTableAccess(
								plan.NewResolvedTable(table, nil, nil),
								mustIndexLookup(idxTable1F.Get(3.14)),
								idxTable1F,
								[]sql.Expression{gfColAlias(1, myTableF, "t1")},
							),
						),
					),
					plan.NewFilter(
						expression.NewEquals(
							expression.NewGetFieldWithTable(0, sql.Int32, "t2", "i2", true),
							expression.NewLiteral(21, sql.Int32),
						),
						plan.NewTableAlias("t2",
							plan.NewStaticIndexedTableAccess(
								plan.NewResolvedTable(table2, nil, nil),
								mustIndexLookup(idxTable2I2.Get(21)),
								idxTable2I2,
								[]sql.Expression{gfColAlias(0, mytable2I, "t2")},
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
					expression.NewGetFieldWithTable(0, sql.Int32, "t1", "i", true),
				},
				plan.NewFilter(
					and(
						and(
							expression.NewEquals(
								expression.NewGetFieldWithTable(1, sql.Float64, "t1", "f", true),
								expression.NewLiteral(3.14, sql.Float64),
							),
							expression.NewEquals(
								expression.NewGetFieldWithTable(3, sql.Int32, "t2", "i2", true),
								expression.NewLiteral(21, sql.Int32),
							),
						),
						and(
							expression.NewEquals(
								expression.NewGetFieldWithTable(2, sql.Text, "t1", "t", true),
								expression.NewLiteral("hello", sql.Text),
							),
							expression.NewEquals(
								expression.NewGetFieldWithTable(5, sql.Text, "t2", "t2", true),
								expression.NewLiteral("goodbye", sql.Text),
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
					expression.NewGetFieldWithTable(0, sql.Int32, "t1", "i", true),
				},
				plan.NewCrossJoin(
					plan.NewFilter(
						and(
							expression.NewEquals(
								expression.NewGetFieldWithTable(1, sql.Float64, "t1", "f", true),
								expression.NewLiteral(3.14, sql.Float64),
							),
							expression.NewEquals(
								expression.NewGetFieldWithTable(2, sql.Text, "t1", "t", true),
								expression.NewLiteral("hello", sql.Text),
							),
						),
						plan.NewTableAlias("t1",
							plan.NewStaticIndexedTableAccess(
								plan.NewResolvedTable(table, nil, nil),
								mustIndexLookup(idxTable1F.Get(3.14)),
								idxTable1F,
								[]sql.Expression{gfColAlias(1, myTableF, "t1")},
							),
						),
					),
					plan.NewFilter(
						and(
							expression.NewEquals(
								expression.NewGetFieldWithTable(0, sql.Int32, "t2", "i2", true),
								expression.NewLiteral(21, sql.Int32),
							),
							expression.NewEquals(
								expression.NewGetFieldWithTable(2, sql.Text, "t2", "t2", true),
								expression.NewLiteral("goodbye", sql.Text),
							),
						),
						plan.NewTableAlias("t2",
							plan.NewStaticIndexedTableAccess(
								plan.NewResolvedTable(table2, nil, nil),
								mustIndexLookup(idxTable2I2.Get(21)),
								idxTable2I2,
								[]sql.Expression{gfColAlias(0, mytable2I, "t2")},
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
					expression.NewGetFieldWithTable(0, sql.Int32, "t1", "i", true),
				},
				plan.NewFilter(
					and(
						expression.NewEquals(
							expression.NewGetFieldWithTable(3, sql.Int32, "t2", "i2", true),
							expression.NewLiteral(21, sql.Int32),
						),
						and(
							expression.NewEquals(
								expression.NewGetFieldWithTable(0, sql.Int32, "t1", "i", true),
								expression.NewLiteral(100, sql.Int32),
							),
							expression.NewEquals(
								expression.NewGetFieldWithTable(5, sql.Text, "t2", "t2", true),
								expression.NewLiteral("goodbye", sql.Text),
							),
						),
					),
					plan.NewIndexedJoin(
						plan.NewTableAlias("t1",
							plan.NewResolvedTable(table, nil, nil),
						),
						plan.NewTableAlias("t2",
							plan.NewResolvedTable(table2, nil, nil),
						),
						plan.JoinTypeInner,
						eq(gf(0, "mytable", "i"), gf(3, "mytable2", "i2")),
						0,
					),
				),
			),
			expected: plan.NewProject(
				[]sql.Expression{
					expression.NewGetFieldWithTable(0, sql.Int32, "t1", "i", true),
				},
				plan.NewIndexedJoin(
					plan.NewFilter(
						expression.NewEquals(
							expression.NewGetFieldWithTable(0, sql.Int32, "t1", "i", true),
							expression.NewLiteral(100, sql.Int32),
						),
						plan.NewTableAlias("t1",
							plan.NewStaticIndexedTableAccess(
								plan.NewResolvedTable(table, nil, nil),
								mustIndexLookup(idxtable1I.Get(100)),
								idxtable1I,
								[]sql.Expression{gfColAlias(0, myTableI, "t1")},
							),
						),
					),
					plan.NewFilter(
						and(
							expression.NewEquals(
								expression.NewGetFieldWithTable(0, sql.Int32, "t2", "i2", true),
								expression.NewLiteral(21, sql.Int32),
							),
							expression.NewEquals(
								expression.NewGetFieldWithTable(2, sql.Text, "t2", "t2", true),
								expression.NewLiteral("goodbye", sql.Text),
							),
						),
						plan.NewTableAlias("t2",
							plan.NewResolvedTable(table2, nil, nil),
						),
					),
					plan.JoinTypeInner,
					eq(gf(0, "mytable", "i"), gf(3, "mytable2", "i2")),
					0,
				),
			),
		},
		{
			name: "two aliased tables, indexed join, index on secondary table",
			node: plan.NewProject(
				[]sql.Expression{
					expression.NewGetFieldWithTable(0, sql.Int32, "t1", "i", true),
				},
				plan.NewFilter(
					and(
						expression.NewEquals(
							expression.NewGetFieldWithTable(3, sql.Int32, "t2", "i2", true),
							expression.NewLiteral(21, sql.Int32),
						),
						and(
							expression.NewEquals(
								expression.NewGetFieldWithTable(0, sql.Int32, "t1", "i", true),
								expression.NewLiteral(100, sql.Int32),
							),
							expression.NewEquals(
								expression.NewGetFieldWithTable(5, sql.Text, "t2", "t2", true),
								expression.NewLiteral("goodbye", sql.Text),
							),
						),
					),
					plan.NewIndexedJoin(
						plan.NewTableAlias("t1",
							plan.NewResolvedTable(table, nil, nil),
						),
						plan.NewTableAlias("t2",
							plan.NewIndexedTableAccess(
								plan.NewResolvedTable(table2, nil, nil),
								idxTable2I2,
								[]sql.Expression{gf(0, "t1", "i")},
							),
						),
						plan.JoinTypeInner,
						eq(gf(0, "mytable", "i"), gf(3, "mytable2", "i2")),
						0,
					),
				),
			),
			expected: plan.NewProject(
				[]sql.Expression{
					expression.NewGetFieldWithTable(0, sql.Int32, "t1", "i", true),
				},
				plan.NewIndexedJoin(
					plan.NewFilter(
						expression.NewEquals(
							expression.NewGetFieldWithTable(0, sql.Int32, "t1", "i", true),
							expression.NewLiteral(100, sql.Int32),
						),
						plan.NewTableAlias("t1",
							plan.NewStaticIndexedTableAccess(
								plan.NewResolvedTable(table, nil, nil),
								mustIndexLookup(idxtable1I.Get(100)),
								idxtable1I,
								[]sql.Expression{gfColAlias(0, myTableI, "t1")},
							),
						),
					),
					plan.NewFilter(
						and(
							expression.NewEquals(
								expression.NewGetFieldWithTable(0, sql.Int32, "t2", "i2", true),
								expression.NewLiteral(21, sql.Int32),
							),
							expression.NewEquals(
								expression.NewGetFieldWithTable(2, sql.Text, "t2", "t2", true),
								expression.NewLiteral("goodbye", sql.Text),
							),
						),
						plan.NewTableAlias("t2",
							plan.NewIndexedTableAccess(
								plan.NewResolvedTable(table2, nil, nil),
								idxTable2I2,
								[]sql.Expression{gf(0, "t1", "i")},
							),
						),
					),
					plan.JoinTypeInner,
					eq(gf(0, "mytable", "i"), gf(3, "mytable2", "i2")),
					0,
				),
			),
		},
		{
			name: "two aliased tables, left indexed join",
			node: plan.NewProject(
				[]sql.Expression{
					expression.NewGetFieldWithTable(0, sql.Int32, "t1", "i", true),
				},
				plan.NewFilter(
					and(
						expression.NewEquals(
							expression.NewGetFieldWithTable(3, sql.Int32, "t2", "i2", true),
							expression.NewLiteral(21, sql.Int32),
						),
						and(
							expression.NewEquals(
								expression.NewGetFieldWithTable(0, sql.Int32, "t1", "i", true),
								expression.NewLiteral(100, sql.Int32),
							),
							expression.NewEquals(
								expression.NewGetFieldWithTable(5, sql.Text, "t2", "t2", true),
								expression.NewLiteral("goodbye", sql.Text),
							),
						),
					),
					plan.NewIndexedJoin(
						plan.NewTableAlias("t1",
							plan.NewResolvedTable(table, nil, nil),
						),
						plan.NewTableAlias("t2",
							plan.NewResolvedTable(table2, nil, nil),
						),
						plan.JoinTypeLeft,
						eq(gf(0, "mytable", "i"), gf(3, "mytable2", "i2")),
						0,
					),
				),
			),
			expected: plan.NewProject(
				[]sql.Expression{
					expression.NewGetFieldWithTable(0, sql.Int32, "t1", "i", true),
				},
				plan.NewFilter(
					and(
						expression.NewEquals(
							expression.NewGetFieldWithTable(3, sql.Int32, "t2", "i2", true),
							expression.NewLiteral(21, sql.Int32),
						),
						expression.NewEquals(
							expression.NewGetFieldWithTable(5, sql.Text, "t2", "t2", true),
							expression.NewLiteral("goodbye", sql.Text),
						),
					),
					plan.NewIndexedJoin(
						plan.NewFilter(
							expression.NewEquals(
								expression.NewGetFieldWithTable(0, sql.Int32, "t1", "i", true),
								expression.NewLiteral(100, sql.Int32),
							),
							plan.NewTableAlias("t1",
								plan.NewStaticIndexedTableAccess(
									plan.NewResolvedTable(table, nil, nil),
									mustIndexLookup(idxtable1I.Get(100)),
									idxtable1I,
									[]sql.Expression{gfColAlias(0, myTableI, "t1")},
								),
							),
						),
						plan.NewTableAlias("t2",
							plan.NewResolvedTable(table2, nil, nil),
						),
						plan.JoinTypeLeft,
						eq(gf(0, "mytable", "i"), gf(3, "mytable2", "i2")),
						0,
					),
				),
			),
		},
		{
			name: "two aliased tables, right indexed join",
			node: plan.NewProject(
				[]sql.Expression{
					expression.NewGetFieldWithTable(0, sql.Int32, "t1", "i", true),
				},
				plan.NewFilter(
					and(
						expression.NewEquals(
							expression.NewGetFieldWithTable(3, sql.Int32, "t2", "i2", true),
							expression.NewLiteral(21, sql.Int32),
						),
						and(
							expression.NewEquals(
								expression.NewGetFieldWithTable(0, sql.Int32, "t1", "i", true),
								expression.NewLiteral(100, sql.Int32),
							),
							expression.NewEquals(
								expression.NewGetFieldWithTable(5, sql.Text, "t2", "t2", true),
								expression.NewLiteral("goodbye", sql.Text),
							),
						),
					),
					plan.NewIndexedJoin(
						plan.NewTableAlias("t2",
							plan.NewResolvedTable(table2, nil, nil),
						),
						plan.NewTableAlias("t1",
							plan.NewResolvedTable(table, nil, nil),
						),
						plan.JoinTypeRight,
						eq(gf(0, "mytable", "i"), gf(3, "mytable2", "i2")),
						0,
					),
				),
			),
			expected: plan.NewProject(
				[]sql.Expression{
					expression.NewGetFieldWithTable(3, sql.Int32, "t1", "i", true),
				},
				plan.NewFilter(
					expression.NewEquals(
						expression.NewGetFieldWithTable(3, sql.Int32, "t1", "i", true),
						expression.NewLiteral(100, sql.Int32),
					),
					plan.NewIndexedJoin(
						plan.NewFilter(
							and(
								expression.NewEquals(
									expression.NewGetFieldWithTable(0, sql.Int32, "t2", "i2", true),
									expression.NewLiteral(21, sql.Int32),
								),
								expression.NewEquals(
									expression.NewGetFieldWithTable(2, sql.Text, "t2", "t2", true),
									expression.NewLiteral("goodbye", sql.Text),
								),
							),
							plan.NewTableAlias("t2",
								plan.NewStaticIndexedTableAccess(
									plan.NewResolvedTable(table2, nil, nil),
									mustIndexLookup(idxTable2I2.Get(21)),
									idxTable2I2,
									[]sql.Expression{gfColAlias(0, mytable2I, "t2")},
								),
							),
						),
						plan.NewTableAlias("t1",
							plan.NewResolvedTable(table, nil, nil),
						),
						plan.JoinTypeRight,
						eq(gf(0, "mytable", "i"), gf(3, "mytable2", "i2")),
						0,
					),
				),
			),
		},
	}

	runTestCases(t, sql.NewEmptyContext(), tests, a, getRule("pushdown_filters"))
}

func mustIndexLookup(lookup sql.IndexLookup, err error) sql.IndexLookup {
	if err != nil {
		panic(err)
	}
	return lookup
}
