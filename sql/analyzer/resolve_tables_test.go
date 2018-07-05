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

	catalog := &sql.Catalog{Databases: []sql.Database{db}}

	a := NewBuilder(catalog).AddPostAnalyzeRule(f.Name, f.Apply).Build()

	a.CurrentDatabase = "mydb"
	var notAnalyzed sql.Node = plan.NewUnresolvedTable("mytable")
	analyzed, err := f.Apply(sql.NewEmptyContext(), a, notAnalyzed)
	require.NoError(err)
	require.Equal(table, analyzed)

	notAnalyzed = plan.NewUnresolvedTable("nonexistant")
	analyzed, err = f.Apply(sql.NewEmptyContext(), a, notAnalyzed)
	require.Error(err)
	require.Nil(analyzed)

	analyzed, err = f.Apply(sql.NewEmptyContext(), a, table)
	require.NoError(err)
	require.Equal(table, analyzed)

	notAnalyzed = plan.NewUnresolvedTable("dual")
	analyzed, err = f.Apply(sql.NewEmptyContext(), a, notAnalyzed)
	require.NoError(err)
	require.Equal(dualTable, analyzed)
}

func TestResolveTablesNested(t *testing.T) {
	require := require.New(t)

	f := getRule("resolve_tables")

	table := mem.NewTable("mytable", sql.Schema{{Name: "i", Type: sql.Int32}})
	db := mem.NewDatabase("mydb")
	db.AddTable("mytable", table)

	catalog := &sql.Catalog{Databases: []sql.Database{db}}

	a := NewBuilder(catalog).AddPostAnalyzeRule(f.Name, f.Apply).Build()

	a.CurrentDatabase = "mydb"

	notAnalyzed := plan.NewProject(
		[]sql.Expression{expression.NewGetField(0, sql.Int32, "i", true)},
		plan.NewUnresolvedTable("mytable"),
	)
	analyzed, err := f.Apply(sql.NewEmptyContext(), a, notAnalyzed)
	require.NoError(err)
	expected := plan.NewProject(
		[]sql.Expression{expression.NewGetField(0, sql.Int32, "i", true)},
		table,
	)
	require.Equal(expected, analyzed)
}
