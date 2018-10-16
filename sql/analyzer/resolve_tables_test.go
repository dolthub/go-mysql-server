package analyzer

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/mem"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
	"gopkg.in/src-d/go-mysql-server.v0/sql/plan"
)

func TestResolveTables(t *testing.T) {
	require := require.New(t)

	f := getRule("resolve_tables")

	table := mem.NewTable("mytable", sql.Schema{{Name: "i", Type: sql.Int32}})
	db := mem.NewDatabase("mydb")
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

	table := mem.NewTable("mytable", sql.Schema{{Name: "i", Type: sql.Int32}})
	table2 := mem.NewTable("my_other_table", sql.Schema{{Name: "i", Type: sql.Int32}})
	db := mem.NewDatabase("mydb")
	db.AddTable("mytable", table)
	catalog := sql.NewCatalog()
	catalog.AddDatabase(db)

	db2 := mem.NewDatabase("my_other_db")
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
