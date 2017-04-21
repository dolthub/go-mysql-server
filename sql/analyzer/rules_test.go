package analyzer_test

import (
	"testing"

	"gopkg.in/sqle/sqle.v0/mem"
	"gopkg.in/sqle/sqle.v0/sql"
	"gopkg.in/sqle/sqle.v0/sql/analyzer"
	"gopkg.in/sqle/sqle.v0/sql/expression"
	"gopkg.in/sqle/sqle.v0/sql/plan"

	"github.com/stretchr/testify/assert"
)

func Test_resolveTables(t *testing.T) {
	assert := assert.New(t)

	f := getRule("resolve_tables")

	table := mem.NewTable("mytable", sql.Schema{{Name: "i", Type: sql.Integer}})
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

	table := mem.NewTable("mytable", sql.Schema{{Name: "i", Type: sql.Integer}})
	db := mem.NewDatabase("mydb")
	db.AddTable("mytable", table)

	catalog := &sql.Catalog{Databases: []sql.Database{db}}

	a := analyzer.New(catalog)
	a.Rules = []analyzer.Rule{f}
	a.CurrentDatabase = "mydb"

	notAnalyzed := plan.NewProject(
		[]sql.Expression{expression.NewGetField(0, sql.Integer, "i", true)},
		plan.NewUnresolvedTable("mytable"),
	)
	analyzed := f.Apply(a, notAnalyzed)
	expected := plan.NewProject(
		[]sql.Expression{expression.NewGetField(0, sql.Integer, "i", true)},
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
