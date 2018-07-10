package analyzer

import (
	"fmt"
	"testing"

	"gopkg.in/src-d/go-mysql-server.v0/mem"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
	"gopkg.in/src-d/go-mysql-server.v0/sql/plan"

	"github.com/stretchr/testify/require"
)

func TestAnalyzer_Analyze(t *testing.T) {
	require := require.New(t)

	table := mem.NewTable("mytable", sql.Schema{
		{Name: "i", Type: sql.Int32, Source: "mytable"},
		{Name: "t", Type: sql.Text, Source: "mytable"},
	})
	table2 := mem.NewTable("mytable2", sql.Schema{{Name: "i2", Type: sql.Int32, Source: "mytable2"}})
	db := mem.NewDatabase("mydb")
	db.AddTable("mytable", table)
	db.AddTable("mytable2", table2)

	catalog := sql.NewCatalog()
	catalog.AddDatabase(db)
	a := NewDefault(catalog)
	a.CurrentDatabase = "mydb"

	emptyCols := []sql.Expression{}

	var notAnalyzed sql.Node = plan.NewUnresolvedTable("mytable")
	analyzed, err := a.Analyze(sql.NewEmptyContext(), notAnalyzed)
	require.NoError(err)
	require.Equal(
		plan.NewPushdownProjectionAndFiltersTable(emptyCols, nil, table),
		analyzed,
	)

	notAnalyzed = plan.NewUnresolvedTable("nonexistant")
	analyzed, err = a.Analyze(sql.NewEmptyContext(), notAnalyzed)
	require.Error(err)
	require.Nil(analyzed)

	analyzed, err = a.Analyze(sql.NewEmptyContext(), table)
	require.NoError(err)
	require.Equal(
		plan.NewPushdownProjectionAndFiltersTable(emptyCols, nil, table),
		analyzed,
	)

	notAnalyzed = plan.NewProject(
		[]sql.Expression{expression.NewUnresolvedColumn("o")},
		plan.NewUnresolvedTable("mytable"),
	)
	_, err = a.Analyze(sql.NewEmptyContext(), notAnalyzed)
	require.Error(err)

	notAnalyzed = plan.NewProject(
		[]sql.Expression{expression.NewUnresolvedColumn("i")},
		plan.NewUnresolvedTable("mytable"),
	)
	analyzed, err = a.Analyze(sql.NewEmptyContext(), notAnalyzed)
	var expected sql.Node = plan.NewProject(
		[]sql.Expression{expression.NewGetFieldWithTable(0, sql.Int32, "mytable", "i", false)},
		plan.NewPushdownProjectionAndFiltersTable(
			[]sql.Expression{expression.NewGetFieldWithTable(0, sql.Int32, "mytable", "i", false)},
			nil,
			table,
		),
	)
	require.NoError(err)
	require.Equal(expected, analyzed)

	notAnalyzed = plan.NewDescribe(
		plan.NewUnresolvedTable("mytable"),
	)
	analyzed, err = a.Analyze(sql.NewEmptyContext(), notAnalyzed)
	expected = plan.NewDescribe(
		plan.NewPushdownProjectionAndFiltersTable(emptyCols, nil, table),
	)
	require.NoError(err)
	require.Equal(expected, analyzed)

	notAnalyzed = plan.NewProject(
		[]sql.Expression{expression.NewStar()},
		plan.NewUnresolvedTable("mytable"),
	)
	analyzed, err = a.Analyze(sql.NewEmptyContext(), notAnalyzed)
	require.NoError(err)
	require.Equal(
		plan.NewPushdownProjectionAndFiltersTable(
			[]sql.Expression{
				expression.NewGetFieldWithTable(0, sql.Int32, "mytable", "i", false),
				expression.NewGetFieldWithTable(1, sql.Text, "mytable", "t", false),
			},
			nil,
			table,
		),
		analyzed,
	)

	notAnalyzed = plan.NewProject(
		[]sql.Expression{expression.NewStar()},
		plan.NewProject(
			[]sql.Expression{expression.NewStar()},
			plan.NewUnresolvedTable("mytable"),
		),
	)
	analyzed, err = a.Analyze(sql.NewEmptyContext(), notAnalyzed)
	require.NoError(err)
	require.Equal(
		plan.NewPushdownProjectionAndFiltersTable(
			[]sql.Expression{
				expression.NewGetFieldWithTable(0, sql.Int32, "mytable", "i", false),
				expression.NewGetFieldWithTable(1, sql.Text, "mytable", "t", false),
			},
			nil,
			table,
		),
		analyzed,
	)

	notAnalyzed = plan.NewProject(
		[]sql.Expression{
			expression.NewAlias(
				expression.NewUnresolvedColumn("i"),
				"foo",
			),
		},
		plan.NewUnresolvedTable("mytable"),
	)
	analyzed, err = a.Analyze(sql.NewEmptyContext(), notAnalyzed)
	expected = plan.NewProject(
		[]sql.Expression{
			expression.NewAlias(
				expression.NewGetFieldWithTable(0, sql.Int32, "mytable", "i", false),
				"foo",
			),
		},
		plan.NewPushdownProjectionAndFiltersTable(
			[]sql.Expression{
				expression.NewGetFieldWithTable(0, sql.Int32, "mytable", "i", false),
			},
			nil,
			table,
		),
	)
	require.NoError(err)
	require.Equal(expected, analyzed)

	notAnalyzed = plan.NewProject(
		[]sql.Expression{expression.NewUnresolvedColumn("i")},
		plan.NewFilter(
			expression.NewEquals(
				expression.NewUnresolvedColumn("i"),
				expression.NewLiteral(int32(1), sql.Int32),
			),
			plan.NewUnresolvedTable("mytable"),
		),
	)
	analyzed, err = a.Analyze(sql.NewEmptyContext(), notAnalyzed)
	expected = plan.NewProject(
		[]sql.Expression{
			expression.NewGetFieldWithTable(0, sql.Int32, "mytable", "i", false),
		},
		plan.NewFilter(
			expression.NewEquals(
				expression.NewGetFieldWithTable(0, sql.Int32, "mytable", "i", false),
				expression.NewLiteral(int32(1), sql.Int32),
			),
			plan.NewPushdownProjectionAndFiltersTable(
				[]sql.Expression{
					expression.NewGetFieldWithTable(0, sql.Int32, "mytable", "i", false),
				},
				nil,
				table,
			),
		),
	)
	require.NoError(err)
	require.Equal(expected, analyzed)

	notAnalyzed = plan.NewProject(
		[]sql.Expression{
			expression.NewUnresolvedColumn("i"),
			expression.NewUnresolvedColumn("i2"),
		},
		plan.NewCrossJoin(
			plan.NewUnresolvedTable("mytable"),
			plan.NewUnresolvedTable("mytable2"),
		),
	)
	analyzed, err = a.Analyze(sql.NewEmptyContext(), notAnalyzed)
	expected = plan.NewProject(
		[]sql.Expression{
			expression.NewGetFieldWithTable(0, sql.Int32, "mytable", "i", false),
			expression.NewGetFieldWithTable(2, sql.Int32, "mytable2", "i2", false),
		},
		plan.NewCrossJoin(
			plan.NewPushdownProjectionAndFiltersTable(
				[]sql.Expression{
					expression.NewGetFieldWithTable(0, sql.Int32, "mytable", "i", false),
				},
				nil,
				table,
			),
			plan.NewPushdownProjectionAndFiltersTable(
				[]sql.Expression{
					expression.NewGetFieldWithTable(0, sql.Int32, "mytable2", "i2", false),
				},
				nil,
				table2,
			),
		),
	)
	require.NoError(err)
	require.Equal(expected, analyzed)

	notAnalyzed = plan.NewLimit(int64(1),
		plan.NewProject(
			[]sql.Expression{
				expression.NewUnresolvedColumn("i"),
			},
			plan.NewUnresolvedTable("mytable"),
		),
	)
	analyzed, err = a.Analyze(sql.NewEmptyContext(), notAnalyzed)
	expected = plan.NewLimit(int64(1),
		plan.NewProject(
			[]sql.Expression{
				expression.NewGetFieldWithTable(0, sql.Int32, "mytable", "i", false),
			},
			plan.NewPushdownProjectionAndFiltersTable(
				[]sql.Expression{
					expression.NewGetFieldWithTable(0, sql.Int32, "mytable", "i", false),
				},
				nil,
				table,
			),
		),
	)
	require.NoError(err)
	require.Equal(expected, analyzed)
}

func TestMaxIterations(t *testing.T) {
	require := require.New(t)
	tName := "my-table"
	table := mem.NewTable(tName, sql.Schema{
		{Name: "i", Type: sql.Int32, Source: tName},
		{Name: "t", Type: sql.Text, Source: tName},
	})
	db := mem.NewDatabase("mydb")
	db.AddTable(tName, table)

	catalog := sql.NewCatalog()
	catalog.AddDatabase(db)

	count := 0
	a := NewBuilder(catalog).AddPostAnalyzeRule("loop",
		func(c *sql.Context, a *Analyzer, n sql.Node) (sql.Node, error) {

			switch n.(type) {
			case *plan.PushdownProjectionAndFiltersTable:
				tName := fmt.Sprintf("mytable-%v", count)
				table := mem.NewTable(tName, sql.Schema{
					{Name: "i", Type: sql.Int32, Source: tName},
					{Name: "t", Type: sql.Text, Source: tName},
				})

				n = plan.NewPushdownProjectionAndFiltersTable([]sql.Expression{}, nil, table)
				count++
			}

			return n, nil
		}).Build()

	a.CurrentDatabase = "mydb"

	notAnalyzed := plan.NewUnresolvedTable(tName)
	analyzed, err := a.Analyze(sql.NewEmptyContext(), notAnalyzed)
	require.NoError(err)
	require.Equal(
		plan.NewPushdownProjectionAndFiltersTable([]sql.Expression{}, nil,
			mem.NewTable("mytable-1000", sql.Schema{
				{Name: "i", Type: sql.Int32, Source: "mytable-1000"},
				{Name: "t", Type: sql.Text, Source: "mytable-1000"},
			})),
		analyzed,
	)
	require.Equal(1001, count)
}

func TestAddRule(t *testing.T) {
	require := require.New(t)

	defRulesCount := countRules(NewDefault(nil).Batches)

	a := NewBuilder(nil).AddPostAnalyzeRule("foo", pushdown).Build()

	require.Equal(countRules(a.Batches), defRulesCount+1)
}

func TestAddPreValidationRule(t *testing.T) {
	require := require.New(t)

	defRulesCount := countRules(NewDefault(nil).Batches)

	a := NewBuilder(nil).AddPreValidationRule("foo", pushdown).Build()

	require.Equal(countRules(a.Batches), defRulesCount+1)
}

func TestAddPostValidationRule(t *testing.T) {
	require := require.New(t)

	defRulesCount := countRules(NewDefault(nil).Batches)

	a := NewBuilder(nil).AddPostValidationRule("foo", pushdown).Build()

	require.Equal(countRules(a.Batches), defRulesCount+1)
}

func countRules(batches []*Batch) int {
	var count int
	for _, b := range batches {
		count = count + len(b.Rules)
	}
	return count

}

func TestMixInnerAndNaturalJoins(t *testing.T) {
	require := require.New(t)

	table := &pushdownProjectionAndFiltersTable{mem.NewTable("mytable", sql.Schema{
		{Name: "i", Type: sql.Int32, Source: "mytable"},
		{Name: "f", Type: sql.Float64, Source: "mytable"},
		{Name: "t", Type: sql.Text, Source: "mytable"},
	})}

	table2 := &pushdownProjectionAndFiltersTable{mem.NewTable("mytable2", sql.Schema{
		{Name: "i2", Type: sql.Int32, Source: "mytable2"},
		{Name: "f2", Type: sql.Float64, Source: "mytable2"},
		{Name: "t2", Type: sql.Text, Source: "mytable2"},
	})}

	table3 := &pushdownProjectionAndFiltersTable{mem.NewTable("mytable3", sql.Schema{
		{Name: "i", Type: sql.Int32, Source: "mytable3"},
		{Name: "f2", Type: sql.Float64, Source: "mytable3"},
		{Name: "t3", Type: sql.Text, Source: "mytable3"},
	})}

	db := mem.NewDatabase("mydb")
	db.AddTable("mytable", table)
	db.AddTable("mytable2", table2)
	db.AddTable("mytable3", table3)

	catalog := &sql.Catalog{Databases: []sql.Database{db}}
	a := NewDefault(catalog)
	a.CurrentDatabase = "mydb"

	node := plan.NewProject(
		[]sql.Expression{
			expression.NewStar(),
		},
		plan.NewNaturalJoin(
			plan.NewInnerJoin(
				plan.NewUnresolvedTable("mytable"),
				plan.NewUnresolvedTable("mytable2"),
				expression.NewEquals(
					expression.NewUnresolvedQualifiedColumn("mytable", "i"),
					expression.NewUnresolvedQualifiedColumn("mytable2", "i2"),
				),
			),
			plan.NewUnresolvedTable("mytable3"),
		),
	)

	expected := plan.NewProject(
		[]sql.Expression{
			expression.NewGetFieldWithTable(0, sql.Int32, "mytable", "i", false),
			expression.NewGetFieldWithTable(4, sql.Float64, "mytable2", "f2", false),
			expression.NewGetFieldWithTable(1, sql.Float64, "mytable", "f", false),
			expression.NewGetFieldWithTable(2, sql.Text, "mytable", "t", false),
			expression.NewGetFieldWithTable(3, sql.Int32, "mytable2", "i2", false),
			expression.NewGetFieldWithTable(5, sql.Text, "mytable2", "t2", false),
			expression.NewGetFieldWithTable(8, sql.Text, "mytable3", "t3", false),
		},
		plan.NewInnerJoin(
			plan.NewInnerJoin(
				plan.NewPushdownProjectionAndFiltersTable(
					[]sql.Expression{
						expression.NewGetFieldWithTable(0, sql.Int32, "mytable", "i", false),
						expression.NewGetFieldWithTable(1, sql.Float64, "mytable", "f", false),
						expression.NewGetFieldWithTable(2, sql.Text, "mytable", "t", false),
					},
					nil,
					table,
				),
				plan.NewPushdownProjectionAndFiltersTable(
					[]sql.Expression{
						expression.NewGetFieldWithTable(1, sql.Float64, "mytable2", "f2", false),
						expression.NewGetFieldWithTable(0, sql.Int32, "mytable2", "i2", false),
						expression.NewGetFieldWithTable(2, sql.Text, "mytable2", "t2", false),
					},
					nil,
					table2,
				),
				expression.NewEquals(
					expression.NewGetFieldWithTable(0, sql.Int32, "mytable", "i", false),
					expression.NewGetFieldWithTable(3, sql.Int32, "mytable2", "i2", false),
				),
			),
			plan.NewPushdownProjectionAndFiltersTable(
				[]sql.Expression{
					expression.NewGetFieldWithTable(2, sql.Text, "mytable3", "t3", false),
					expression.NewGetFieldWithTable(0, sql.Int32, "mytable3", "i", false),
					expression.NewGetFieldWithTable(1, sql.Float64, "mytable3", "f2", false),
				},
				nil,
				table3,
			),
			expression.NewAnd(
				expression.NewEquals(
					expression.NewGetFieldWithTable(0, sql.Int32, "mytable", "i", false),
					expression.NewGetFieldWithTable(6, sql.Int32, "mytable3", "i", false),
				),
				expression.NewEquals(
					expression.NewGetFieldWithTable(4, sql.Float64, "mytable2", "f2", false),
					expression.NewGetFieldWithTable(7, sql.Float64, "mytable3", "f2", false),
				),
			),
		),
	)

	result, err := a.Analyze(sql.NewEmptyContext(), node)
	require.NoError(err)
	require.Equal(expected, result)
}
