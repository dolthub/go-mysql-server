package analyzer_test

import (
	"testing"

	"github.com/src-d/go-mysql-server/mem"
	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/analyzer"
	"github.com/src-d/go-mysql-server/sql/expression"
	"github.com/src-d/go-mysql-server/sql/plan"

	"github.com/stretchr/testify/assert"
)

func Test_resolveTables(t *testing.T) {
	assert := assert.New(t)

	f := getRule("resolve_tables")

	table := mem.NewTable("mytable", sql.Schema{{Name: "i", Type: sql.Int32}})
	db := mem.NewDatabase("mydb")
	db.AddTable("mytable", table)

	catalog := &sql.Catalog{Databases: []sql.Database{db}}

	a := analyzer.New(catalog)
	a.Rules = []analyzer.Rule{f}

	a.CurrentDatabase = "mydb"
	var notAnalyzed sql.Node = plan.NewUnresolvedTable("mytable")
	analyzed := f.Apply(a, notAnalyzed)
	assert.Equal(table, analyzed)

	notAnalyzed = plan.NewUnresolvedTable("nonexistant")
	analyzed = f.Apply(a, notAnalyzed)
	assert.Equal(notAnalyzed, analyzed)

	analyzed = f.Apply(a, table)
	assert.Equal(table, analyzed)

}

func Test_resolveTables_Nested(t *testing.T) {
	assert := assert.New(t)

	f := getRule("resolve_tables")

	table := mem.NewTable("mytable", sql.Schema{{Name: "i", Type: sql.Int32}})
	db := mem.NewDatabase("mydb")
	db.AddTable("mytable", table)

	catalog := &sql.Catalog{Databases: []sql.Database{db}}

	a := analyzer.New(catalog)
	a.Rules = []analyzer.Rule{f}
	a.CurrentDatabase = "mydb"

	notAnalyzed := plan.NewProject(
		[]sql.Expression{expression.NewGetField(0, sql.Int32, "i", true)},
		plan.NewUnresolvedTable("mytable"),
	)
	analyzed := f.Apply(a, notAnalyzed)
	expected := plan.NewProject(
		[]sql.Expression{expression.NewGetField(0, sql.Int32, "i", true)},
		table,
	)
	assert.Equal(expected, analyzed)
}

func getRule(name string) analyzer.Rule {
	for _, rule := range analyzer.DefaultRules {
		if rule.Name == name {
			return rule
		}
	}
	panic("missing rule")
}
