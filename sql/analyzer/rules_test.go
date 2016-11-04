package analyzer_test

import (
	"testing"

	"github.com/gitql/gitql/mem"
	"github.com/gitql/gitql/sql"
	"github.com/gitql/gitql/sql/analyzer"
	"github.com/gitql/gitql/sql/expression"
	"github.com/gitql/gitql/sql/plan"

	"github.com/stretchr/testify/assert"
)

func Test_resolveTables(t *testing.T) {
	assert := assert.New(t)

	f := getRule("resolve_tables")

	table := mem.NewTable("mytable", sql.Schema{{"i", sql.Integer}})
	db := mem.NewDatabase("mydb")
	db.AddTable("mytable", table)

	catalog := &sql.Catalog{Databases: []sql.Database{db}}

	a := analyzer.New(catalog)
	a.Rules = []analyzer.Rule{f}

	a.CurrentDatabase = "mydb"
	var notAnalyzed sql.Node = plan.NewUnresolvedRelation("mytable")
	analyzed := f.Apply(a, notAnalyzed)
	assert.Equal(table, analyzed)

	notAnalyzed = plan.NewUnresolvedRelation("nonexistant")
	analyzed = f.Apply(a, notAnalyzed)
	assert.Equal(notAnalyzed, analyzed)

	analyzed = f.Apply(a, table)
	assert.Equal(table, analyzed)

}

func Test_resolveTables_Nested(t *testing.T) {
	assert := assert.New(t)

	f := getRule("resolve_tables")

	table := mem.NewTable("mytable", sql.Schema{{"i", sql.Integer}})
	db := mem.NewDatabase("mydb")
	db.AddTable("mytable", table)

	catalog := &sql.Catalog{Databases: []sql.Database{db}}

	a := analyzer.New(catalog)
	a.Rules = []analyzer.Rule{f}
	a.CurrentDatabase = "mydb"

	notAnalyzed := plan.NewProject(
		[]sql.Expression{expression.NewGetField(0, sql.Integer, "i")},
		plan.NewUnresolvedRelation("mytable"),
	)
	analyzed := f.Apply(a, notAnalyzed)
	expected := plan.NewProject(
		[]sql.Expression{expression.NewGetField(0, sql.Integer, "i")},
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
