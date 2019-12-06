package analyzer

import (
	"fmt"
	"testing"

	"github.com/src-d/go-mysql-server/memory"
	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/expression"
	"github.com/src-d/go-mysql-server/sql/plan"
	"github.com/stretchr/testify/require"
)

func TestResolveTables(t *testing.T) {
	require := require.New(t)

	f := getRule("resolve_tables")

	table := memory.NewTable("mytable", sql.Schema{{Name: "i", Type: sql.Int32}})
	db := memory.NewDatabase("mydb")
	db.AddTable("mytable", table)

	catalog := sql.NewCatalog()
	catalog.AddDatabase(db)

	a := NewBuilder(catalog).AddPostAnalyzeRule(f.Name, f.Apply).Build()

	var notAnalyzed sql.Node = plan.NewUnresolvedTable("mytable", "")
	analyzed, err := f.Apply(sql.NewEmptyContext(), a, notAnalyzed)
	require.NoError(err)
	require.Equal(plan.NewResolvedTable(table), analyzed)

	notAnalyzed = plan.NewUnresolvedTable("MyTable", "")
	analyzed, err = f.Apply(sql.NewEmptyContext(), a, notAnalyzed)
	require.NoError(err)
	require.Equal(plan.NewResolvedTable(table), analyzed)

	notAnalyzed = plan.NewUnresolvedTable("nonexistant", "")
	analyzed, err = f.Apply(sql.NewEmptyContext(), a, notAnalyzed)
	require.Error(err)
	require.Nil(analyzed)

	analyzed, err = f.Apply(sql.NewEmptyContext(), a, plan.NewResolvedTable(table))
	require.NoError(err)
	require.Equal(plan.NewResolvedTable(table), analyzed)

	notAnalyzed = plan.NewUnresolvedTable("dual", "")
	analyzed, err = f.Apply(sql.NewEmptyContext(), a, notAnalyzed)
	require.NoError(err)
	require.Equal(plan.NewResolvedTable(dualTable), analyzed)
}

func TestResolveTablesNested(t *testing.T) {
	require := require.New(t)

	f := getRule("resolve_tables")

	table := memory.NewTable("mytable", sql.Schema{{Name: "i", Type: sql.Int32}})
	table2 := memory.NewTable("my_other_table", sql.Schema{{Name: "i", Type: sql.Int32}})
	db := memory.NewDatabase("mydb")
	db.AddTable("mytable", table)
	catalog := sql.NewCatalog()
	catalog.AddDatabase(db)

	db2 := memory.NewDatabase("my_other_db")
	db2.AddTable("my_other_table", table2)
	catalog.AddDatabase(db2)

	a := NewBuilder(catalog).AddPostAnalyzeRule(f.Name, f.Apply).Build()

	notAnalyzed := plan.NewProject(
		[]sql.Expression{expression.NewGetField(0, sql.Int32, "i", true)},
		plan.NewUnresolvedTable("mytable", ""),
	)
	analyzed, err := f.Apply(sql.NewEmptyContext(), a, notAnalyzed)
	require.NoError(err)
	expected := plan.NewProject(
		[]sql.Expression{expression.NewGetField(0, sql.Int32, "i", true)},
		plan.NewResolvedTable(table),
	)
	require.Equal(expected, analyzed)

	notAnalyzed = plan.NewProject(
		[]sql.Expression{expression.NewGetField(0, sql.Int32, "i", true)},
		plan.NewUnresolvedTable("my_other_table", "my_other_db"),
	)
	analyzed, err = f.Apply(sql.NewEmptyContext(), a, notAnalyzed)
	require.NoError(err)
	expected = plan.NewProject(
		[]sql.Expression{expression.NewGetField(0, sql.Int32, "i", true)},
		plan.NewResolvedTable(table2),
	)
	require.Equal(expected, analyzed)
}

func TestResolveViews(t *testing.T) {
	require := require.New(t)

	f := getRule("resolve_tables")

	table := memory.NewTable("mytable", sql.Schema{{Name: "i", Type: sql.Int32, Source: "mytable"}})
	db := memory.NewDatabase("mydb")
	db.AddTable("mytable", table)

	subqueryDefinition := plan.NewSubqueryAlias(
		"myview",
		plan.NewProject(
			[]sql.Expression{expression.NewUnresolvedColumn("i")},
			plan.NewUnresolvedTable("mytable", ""),
		),
	)
	subqueryAnalyzed := plan.NewSubqueryAlias(
		"myview",
		plan.NewResolvedTable(table.WithProjection([]string{"i"})),
	)
	view := sql.NewView("myview", subqueryDefinition)

	catalog := sql.NewCatalog()
	catalog.AddDatabase(db)
	err := catalog.ViewRegistry.Register(db.Name(), view)
	require.NoError(err)

	a := NewBuilder(catalog).AddPostAnalyzeRule(f.Name, f.Apply).Build()

	var notAnalyzed sql.Node = plan.NewUnresolvedTable("myview", "")
	analyzed, err := f.Apply(sql.NewEmptyContext(), a, notAnalyzed)
	require.NoError(err)

	// A bit of a kludge. We assert on the serialized form of the analyzed
	// subquery, which shows the projected table scan in this case, because
	// the fully analyzed subquery is going to have a slightly different
	// ResolvedTable node than the one we've constructed in
	// subqueryAnalyzed.
	expected := fmt.Sprintf("%v", subqueryAnalyzed)
	require.Equal(expected, fmt.Sprintf("%v", analyzed.Children()[0]))

	notAnalyzed = plan.NewUnresolvedTable("MyVieW", "")
	analyzed, err = f.Apply(sql.NewEmptyContext(), a, notAnalyzed)
	require.NoError(err)
	require.Equal(expected, fmt.Sprintf("%v", analyzed.Children()[0]))
}
