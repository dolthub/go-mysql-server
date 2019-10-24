package analyzer

import (
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

// Tests the resolution of views (ensuring it is case-insensitive), that should
// result in the replacement of the UnresolvedTable with the SubqueryAlias that
// represents the view
func TestResolveViews(t *testing.T) {
	require := require.New(t)

	f := getRule("resolve_tables")

	table := memory.NewTable("mytable", sql.Schema{{Name: "i", Type: sql.Int32}})
	db := memory.NewDatabase("mydb")
	db.AddTable("mytable", table)

	// Resolved plan that corresponds to query "SELECT i FROM mytable"
	subquery := plan.NewProject(
		[]sql.Expression{
			expression.NewGetFieldWithTable(
				1, sql.Int32, table.Name(), "i", true),
		},
		plan.NewResolvedTable(table),
	)
	subqueryAlias := plan.NewSubqueryAlias("myview", subquery)
	view := sql.NewView("myview", subqueryAlias)

	// Register the view in the catalog
	catalog := sql.NewCatalog()
	catalog.AddDatabase(db)
	catalog.ViewRegistry.Register(db.Name(), view)

	a := NewBuilder(catalog).AddPostAnalyzeRule(f.Name, f.Apply).Build()

	// Check whether the view is resolved and replaced with the subquery
	var notAnalyzed sql.Node = plan.NewUnresolvedTable("myview", "")
	analyzed, err := f.Apply(sql.NewEmptyContext(), a, notAnalyzed)
	require.NoError(err)
	require.Equal(subqueryAlias, analyzed)

	// Ensures that the resolution is case-insensitive
	notAnalyzed = plan.NewUnresolvedTable("MyVieW", "")
	analyzed, err = f.Apply(sql.NewEmptyContext(), a, notAnalyzed)
	require.NoError(err)
	require.Equal(subqueryAlias, analyzed)

	// Ensures that the resolution is idempotent
	analyzed, err = f.Apply(sql.NewEmptyContext(), a, subqueryAlias)
	require.NoError(err)
	require.Equal(subqueryAlias, analyzed)
}
