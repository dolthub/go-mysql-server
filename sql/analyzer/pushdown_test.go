package analyzer

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

func TestPushdownProjection(t *testing.T) {
	table := memory.NewPushdownTable("mytable", sql.Schema{
		{Name: "i", Type: sql.Int32, Source: "mytable"},
		{Name: "f", Type: sql.Float64, Source: "mytable"},
		{Name: "t", Type: sql.Text, Source: "mytable"},
	})

	table2 := memory.NewPushdownTable("mytable2", sql.Schema{
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

	tests := []analyzerFnTestCase {
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
						plan.NewResolvedTable(table),
						plan.NewResolvedTable(table2),
					),
				),
			),
			expected: plan.NewProject(
				[]sql.Expression{
					expression.NewGetFieldWithTable(1, sql.Text, "mytable2", "t2", false),
				},
				plan.NewFilter(
					expression.NewOr(
						expression.NewEquals(
							expression.NewGetFieldWithTable(0, sql.Float64, "mytable", "f", false),
							expression.NewLiteral(3.14, sql.Float64),
						),
						expression.NewIsNull(
							expression.NewGetFieldWithTable(2, sql.Int32, "mytable2", "i2", false),
						),
					),
					plan.NewCrossJoin(
						plan.NewDecoratedNode("Projected table access on [f]", plan.NewResolvedTable(
							table.WithProjection([]string{"f"}),
						)),
						plan.NewDecoratedNode("Projected table access on [t2 i2]", plan.NewResolvedTable(
							table2.WithProjection([]string{"t2", "i2"}),
						)),
					),
				),
			),
		},
		{
			name: "pushing projections down onto a filtered table",
			node: plan.NewProject(
				[]sql.Expression{
					expression.NewGetFieldWithTable(5, sql.Text, "mytable2", "t2", false),
				},
				plan.NewCrossJoin(
					plan.NewDecoratedNode("Filtered table access on [mytable.f = 3.14]", plan.NewResolvedTable(
						table.WithFilters([]sql.Expression{
							expression.NewEquals(
								expression.NewGetFieldWithTable(1, sql.Float64, "mytable", "f", false),
								expression.NewLiteral(3.14, sql.Float64),
							),
						}),
					)),
					plan.NewDecoratedNode("Filtered table access on [mytable2.i2 IS NULL]", plan.NewResolvedTable(
						table2.WithFilters([]sql.Expression{
							expression.NewIsNull(
								expression.NewGetFieldWithTable(0, sql.Int32, "mytable2", "i2", false),
							),
						}),
					)),
				),
			),
			expected: plan.NewProject(
				[]sql.Expression{
					expression.NewGetFieldWithTable(3, sql.Text, "mytable2", "t2", false),
				},
				plan.NewCrossJoin(
					plan.NewDecoratedNode("Filtered table access on [mytable.f = 3.14]", plan.NewResolvedTable(
						table.WithFilters([]sql.Expression{
							expression.NewEquals(
								expression.NewGetFieldWithTable(1, sql.Float64, "mytable", "f", false),
								expression.NewLiteral(3.14, sql.Float64),
							),
						}),
					)),
					plan.NewDecoratedNode("Filtered table access on [mytable2.i2 IS NULL]",
						plan.NewDecoratedNode("Projected table access on [t2]",
							plan.NewResolvedTable(
								table2.WithFilters([]sql.Expression{
									expression.NewIsNull(
										expression.NewGetFieldWithTable(0, sql.Int32, "mytable2", "i2", false),
									),
								}).(*memory.PushdownTable).WithProjection([]string{"t2"}),
							),
						),
					),
				),
			),
		},
	}

	runTestCases(t, sql.NewEmptyContext(), tests, a, getRule("pushdown_projections"))
}

func TestPushdownFilter(t *testing.T) {
	table := memory.NewPushdownTable("mytable", sql.Schema{
		{Name: "i", Type: sql.Int32, Source: "mytable"},
		{Name: "f", Type: sql.Float64, Source: "mytable"},
		{Name: "t", Type: sql.Text, Source: "mytable"},
	})

	table2 := memory.NewPushdownTable("mytable2", sql.Schema{
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
						plan.NewResolvedTable(table),
						plan.NewResolvedTable(table2),
					),
				),
			),
			expected: plan.NewProject(
				[]sql.Expression{
					expression.NewGetFieldWithTable(5, sql.Text, "mytable2", "t2", false),
				},
				plan.NewCrossJoin(
					plan.NewDecoratedNode("Filtered table access on [mytable.f = 3.14]", plan.NewResolvedTable(
						table.WithFilters([]sql.Expression{
							expression.NewEquals(
								expression.NewGetFieldWithTable(1, sql.Float64, "mytable", "f", false),
								expression.NewLiteral(3.14, sql.Float64),
							),
						}),
					)),
					plan.NewDecoratedNode("Filtered table access on [mytable2.i2 IS NULL]", plan.NewResolvedTable(
						table2.WithFilters([]sql.Expression{
							expression.NewIsNull(
								expression.NewGetFieldWithTable(0, sql.Int32, "mytable2", "i2", false),
							),
						}),
					)),
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
							plan.NewResolvedTable(
								table.WithProjection([]string{"f"}),
							),
						),
						plan.NewDecoratedNode("Projected table access on [t2 i2]",
							plan.NewResolvedTable(
								table2.WithProjection([]string{"t2", "i2"}),
							),
						),
					),
				),
			),
			expected: plan.NewProject(
				[]sql.Expression{
					expression.NewGetFieldWithTable(1, sql.Text, "mytable2", "t2", false),
				},
				plan.NewCrossJoin(
					plan.NewDecoratedNode("Projected table access on [f]",
						plan.NewDecoratedNode("Filtered table access on [mytable.f = 3.14]",
							plan.NewResolvedTable(
								table.WithProjection([]string{"f"}).
								(*memory.PushdownTable).WithFilters([]sql.Expression{
									eq(expression.NewGetFieldWithTable(0, sql.Float64, "mytable", "f", false), expression.NewLiteral(3.14, sql.Float64)),
								}),
							),
						),
					),
					plan.NewDecoratedNode("Projected table access on [t2 i2]",
						plan.NewDecoratedNode("Filtered table access on [mytable2.i2 IS NULL]",
							plan.NewResolvedTable(
								table2.WithProjection([]string{"t2", "i2"}).
								(*memory.PushdownTable).WithFilters([]sql.Expression{
									expression.NewIsNull(expression.NewGetFieldWithTable(1, sql.Int32, "mytable2", "i2", false)),
								}),
							),
						),
					),
				),
			),
		},
	}

	runTestCases(t, sql.NewEmptyContext(), tests, a, getRule("pushdown_filters"))
}

func TestPushdownIndex(t *testing.T) {
	require := require.New(t)

	table := memory.NewTable("mytable", sql.Schema{
		{Name: "i", Type: sql.Int32, Source: "mytable", PrimaryKey: true},
		{Name: "f", Type: sql.Float64, Source: "mytable"},
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
	idx1, idx2 := table1Idxes[0], table1Idxes[1]

	table2 := memory.NewTable("mytable2", sql.Schema{
		{Name: "i2", Type: sql.Int32, Source: "mytable2", PrimaryKey: true},
		{Name: "f2", Type: sql.Float64, Source: "mytable2"},
		{Name: "t2", Type: sql.Text, Source: "mytable2"},
	})

	table2.EnablePrimaryKeyIndexes()
	table2Idxes, err := table2.GetIndexes(sql.NewEmptyContext())
	require.NoError(err)
	idx3 := table2Idxes[0]

	db := memory.NewDatabase("")
	db.AddTable("mytable", table)
	db.AddTable("mytable2", table2)

	catalog := sql.NewCatalog()
	catalog.AddDatabase(db)
	a := NewDefault(catalog)

	tests := []analyzerFnTestCase{
		{
			node: plan.NewProject(
				[]sql.Expression{
					expression.NewUnresolvedQualifiedColumn("mytable", "i"),
				},
				plan.NewFilter(
					expression.NewAnd(
						expression.NewAnd(
							expression.NewEquals(
								expression.NewUnresolvedQualifiedColumn("mytable", "f"),
								expression.NewLiteral(3.14, sql.Float64),
							),
							expression.NewGreaterThan(
								expression.NewUnresolvedQualifiedColumn("mytable", "i"),
								expression.NewLiteral(1, sql.Int32),
							),
						),
						expression.NewNot(
							expression.NewEquals(
								expression.NewUnresolvedQualifiedColumn("mytable2", "i2"),
								expression.NewLiteral(2, sql.Int32),
							),
						),
					),
					plan.NewCrossJoin(
						plan.NewResolvedTable(table),
						plan.NewResolvedTable(table2),
					),
				),
			),
			expected: plan.NewProject(
				[]sql.Expression{
					expression.NewGetFieldWithTable(0, sql.Int32, "mytable", "i", false),
				},
				plan.NewCrossJoin(
					plan.NewResolvedTable(table.WithIndexLookup(
						// TODO: These two indexes should not be mergeable, and fetching the values of
						//  them will not yield correct results with the current implementation of these indexes.
						&memory.MergedIndexLookup{
							Intersections: []sql.IndexLookup{
								&memory.MergeableIndexLookup{
									Key:   []interface{}{float64(3.14)},
									Index: idx2.(memory.ExpressionsIndex),
								},
								&memory.DescendIndexLookup{
									Gt:    []interface{}{1},
									Index: idx1.(memory.ExpressionsIndex),
								},
							},
							Index: idx2.(memory.ExpressionsIndex),
						},
					),
					),
					plan.NewResolvedTable(
						table2.WithIndexLookup(&memory.NegateIndexLookup{
							Lookup: &memory.MergeableIndexLookup{
								Key:   []interface{}{2},
								Index: idx3.(memory.ExpressionsIndex),
							},
							Index: idx3.(memory.ExpressionsIndex),
						}),
					),
				),
			),
		},
	}


	runTestCases(t, sql.NewEmptyContext(), tests, a, getRule("pushdown_filters"))
}
