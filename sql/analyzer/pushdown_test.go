package analyzer

import (
	"testing"

	"github.com/src-d/go-mysql-server/memory"
	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/expression"
	"github.com/src-d/go-mysql-server/sql/plan"
	"github.com/stretchr/testify/require"
)

func TestPushdownProjectionAndFilters(t *testing.T) {
	require := require.New(t)
	f := getRule("pushdown")

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

	node := plan.NewProject(
		[]sql.Expression{
			expression.NewGetFieldWithTable(0, sql.Int32, "mytable", "i", false),
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
	)

	expected := plan.NewProject(
		[]sql.Expression{
			expression.NewGetFieldWithTable(0, sql.Int32, "mytable", "i", false),
		},
		plan.NewCrossJoin(
			plan.NewResolvedTable(
				table.WithFilters([]sql.Expression{
					expression.NewEquals(
						expression.NewGetFieldWithTable(1, sql.Float64, "mytable", "f", false),
						expression.NewLiteral(3.14, sql.Float64),
					),
				}).(*memory.Table).WithProjection([]string{"i", "f"}),
			),
			plan.NewResolvedTable(
				table2.WithFilters([]sql.Expression{
					expression.NewIsNull(
						expression.NewGetFieldWithTable(0, sql.Int32, "mytable2", "i2", false),
					),
				}).(*memory.Table).WithProjection([]string{"i2"}),
			),
		),
	)

	result, err := f.Apply(sql.NewEmptyContext(), a, node)
	require.NoError(err)
	require.Equal(expected, result)
}

func TestPushdownIndexable(t *testing.T) {
	require := require.New(t)

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

	db := memory.NewDatabase("")
	db.AddTable("mytable", table)
	db.AddTable("mytable2", table2)

	catalog := sql.NewCatalog()
	catalog.AddDatabase(db)

	idx1 := &memory.MergeableDummyIndex{
		TableName: "mytable",
		Exprs: []sql.Expression{
			expression.NewGetFieldWithTable(0, sql.Int32, "mytable", "i", false),
		},
	}
	done, ready, err := catalog.AddIndex(idx1)
	require.NoError(err)
	close(done)
	<-ready

	idx2 := &memory.MergeableDummyIndex{
		TableName: "mytable",
		Exprs: []sql.Expression{
			expression.NewGetFieldWithTable(1, sql.Float64, "mytable", "f", false),
		},
	}
	done, ready, err = catalog.AddIndex(idx2)
	require.NoError(err)
	close(done)
	<-ready

	idx3 := &memory.MergeableDummyIndex{
		TableName: "mytable2",
		Exprs: []sql.Expression{
			expression.NewGetFieldWithTable(0, sql.Int32, "mytable2", "i2", false),
		},
	}
	done, ready, err = catalog.AddIndex(idx3)

	require.NoError(err)
	close(done)
	<-ready

	a := withoutProcessTracking(NewDefault(catalog))

	node := plan.NewProject(
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
	)

	expected := &releaser{
		plan.NewProject(
			[]sql.Expression{
				expression.NewGetFieldWithTable(0, sql.Int32, "mytable", "i", false),
			},
			plan.NewCrossJoin(
				plan.NewResolvedTable(
					table.WithFilters([]sql.Expression{
						expression.NewEquals(
							expression.NewGetFieldWithTable(1, sql.Float64, "mytable", "f", false),
							expression.NewLiteral(3.14, sql.Float64),
						),
						expression.NewGreaterThan(
							expression.NewGetFieldWithTable(0, sql.Int32, "mytable", "i", false),
							expression.NewLiteral(1, sql.Int32),
						),
					}).(*memory.Table).
						WithProjection([]string{"i", "f"}).(*memory.Table).
							WithIndexLookup(
								// TODO: this is arguably a bug. These two indexes should not be mergeable, and fetching the values of
								//  them will not yield correct results with the current implementation of these indexes.
								&memory.MergedIndexLookup{
									Intersections: []sql.IndexLookup{
										&memory.MergeableIndexLookup{
											Key:   []interface{}{float64(3.14)},
											Index: idx2,
										},
										&memory.DescendIndexLookup{
											Gt:    []interface{}{1},
											Index: idx1,
										},
									},
									Index: idx2,
								},
							),
				),
				plan.NewResolvedTable(
					table2.WithFilters([]sql.Expression{
						expression.NewNot(
							expression.NewEquals(
								expression.NewGetFieldWithTable(0, sql.Int32, "mytable2", "i2", false),
								expression.NewLiteral(2, sql.Int32),
							),
						),
					}).(*memory.Table).
						WithProjection([]string{"i2"}).(*memory.Table).
							WithIndexLookup(&memory.NegateIndexLookup{
								Lookup: &memory.MergeableIndexLookup{
									Key:   []interface{}{2},
									Index: idx3,
								},
								Index: idx3,
							}),
				),
			),
		),
		nil,
	}

	result, err := a.Analyze(sql.NewEmptyContext(), node)
	require.NoError(err)

	// we need to remove the release function to compare, otherwise it will fail
	result, err = plan.TransformUp(result, func(node sql.Node) (sql.Node, error) {
		switch node := node.(type) {
		case *releaser:
			return &releaser{Child: node.Child}, nil
		default:
			return node, nil
		}
	})
	require.NoError(err)

	require.Equal(expected, result)
}
