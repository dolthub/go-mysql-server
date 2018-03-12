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

	a := New(catalog)
	a.Rules = []Rule{f}

	a.CurrentDatabase = "mydb"
	var notAnalyzed sql.Node = plan.NewUnresolvedTable("mytable")
	analyzed, err := f.Apply(a, notAnalyzed)
	require.NoError(err)
	require.Equal(table, analyzed)

	notAnalyzed = plan.NewUnresolvedTable("nonexistant")
	analyzed, err = f.Apply(a, notAnalyzed)
	require.Error(err)
	require.Nil(analyzed)

	analyzed, err = f.Apply(a, table)
	require.NoError(err)
	require.Equal(table, analyzed)
}

func TestResolveTablesNested(t *testing.T) {
	require := require.New(t)

	f := getRule("resolve_tables")

	table := mem.NewTable("mytable", sql.Schema{{Name: "i", Type: sql.Int32}})
	db := mem.NewDatabase("mydb")
	db.AddTable("mytable", table)

	catalog := &sql.Catalog{Databases: []sql.Database{db}}

	a := New(catalog)
	a.Rules = []Rule{f}
	a.CurrentDatabase = "mydb"

	notAnalyzed := plan.NewProject(
		[]sql.Expression{expression.NewGetField(0, sql.Int32, "i", true)},
		plan.NewUnresolvedTable("mytable"),
	)
	analyzed, err := f.Apply(a, notAnalyzed)
	require.NoError(err)
	expected := plan.NewProject(
		[]sql.Expression{expression.NewGetField(0, sql.Int32, "i", true)},
		table,
	)
	require.Equal(expected, analyzed)
}

func TestResolveStar(t *testing.T) {
	require := require.New(t)
	f := getRule("resolve_star")

	table := mem.NewTable("mytable", sql.Schema{{Name: "i", Type: sql.Int32}})
	db := mem.NewDatabase("mydb")
	db.AddTable("mytable", table)

	catalog := &sql.Catalog{Databases: []sql.Database{db}}

	a := New(catalog)
	a.Rules = []Rule{f}
	a.CurrentDatabase = "mydb"

	notAnalyzed := plan.NewProject(
		[]sql.Expression{expression.NewStar()},
		table,
	)
	analyzed, err := f.Apply(a, notAnalyzed)
	require.NoError(err)
	expected := plan.NewProject(
		[]sql.Expression{expression.NewGetField(0, sql.Int32, "i", false)},
		table,
	)
	require.Equal(expected, analyzed)
}

func TestQualifyColumns(t *testing.T) {
	require := require.New(t)
	f := getRule("qualify_columns")

	table := mem.NewTable("mytable", sql.Schema{{Name: "i", Type: sql.Int32}})
	table2 := mem.NewTable("mytable2", sql.Schema{{Name: "i", Type: sql.Int32}})

	node := plan.NewProject(
		[]sql.Expression{
			expression.NewUnresolvedColumn("i"),
		},
		table,
	)

	expected := plan.NewProject(
		[]sql.Expression{
			expression.NewUnresolvedQualifiedColumn("mytable", "i"),
		},
		table,
	)

	result, err := f.Apply(nil, node)
	require.NoError(err)
	require.Equal(expected, result)

	node = plan.NewProject(
		[]sql.Expression{
			expression.NewUnresolvedQualifiedColumn("mytable", "i"),
		},
		table,
	)

	result, err = f.Apply(nil, node)
	require.NoError(err)
	require.Equal(expected, result)

	node = plan.NewProject(
		[]sql.Expression{
			expression.NewUnresolvedQualifiedColumn("a", "i"),
		},
		plan.NewTableAlias("a", table),
	)

	expected = plan.NewProject(
		[]sql.Expression{
			expression.NewUnresolvedQualifiedColumn("mytable", "i"),
		},
		plan.NewTableAlias("a", table),
	)

	result, err = f.Apply(nil, node)
	require.NoError(err)
	require.Equal(expected, result)

	node = plan.NewProject(
		[]sql.Expression{
			expression.NewUnresolvedQualifiedColumn("foo", "i"),
		},
		plan.NewTableAlias("a", table),
	)

	result, err = f.Apply(nil, node)
	require.Error(err)
	require.True(ErrTableNotFound.Is(err))

	node = plan.NewProject(
		[]sql.Expression{
			expression.NewUnresolvedColumn("b"),
		},
		plan.NewTableAlias("a", table),
	)

	_, err = f.Apply(nil, node)
	require.Error(err)
	require.True(ErrColumnTableNotFound.Is(err))

	node = plan.NewProject(
		[]sql.Expression{
			expression.NewUnresolvedColumn("i"),
		},
		plan.NewCrossJoin(table, table2),
	)

	_, err = f.Apply(nil, node)
	require.Error(err)
	require.True(ErrAmbiguousColumnName.Is(err))
}

func TestOptimizeDistinct(t *testing.T) {
	require := require.New(t)
	notSorted := plan.NewDistinct(mem.NewTable("foo", nil))
	sorted := plan.NewDistinct(plan.NewSort(nil, mem.NewTable("foo", nil)))

	rule := getRule("optimize_distinct")

	analyzedNotSorted, err := rule.Apply(nil, notSorted)
	require.NoError(err)

	analyzedSorted, err := rule.Apply(nil, sorted)
	require.NoError(err)

	require.Equal(notSorted, analyzedNotSorted)
	require.Equal(plan.NewOrderedDistinct(sorted.Child), analyzedSorted)
}

func getRule(name string) Rule {
	for _, rule := range DefaultRules {
		if rule.Name == name {
			return rule
		}
	}
	panic("missing rule")
}
