package analyzer_test

import (
	"fmt"
	"testing"

	"github.com/mvader/gitql/mem"
	"github.com/mvader/gitql/sql"
	"github.com/mvader/gitql/sql/analyzer"
	"github.com/mvader/gitql/sql/expression"
	"github.com/mvader/gitql/sql/plan"
	"github.com/stretchr/testify/require"
)

func TestAnalyzer_Analyze(t *testing.T) {
	assert := require.New(t)

	table := mem.NewTable("mytable", sql.Schema{{"i", sql.Integer}})
	db := mem.NewDatabase("mydb")
	db.AddTable("mytable", table)

	catalog := &sql.Catalog{Databases:[]sql.Database{db}}
	a := analyzer.New(catalog)
	a.CurrentDatabase = "mydb"

	var notAnalyzed sql.Node = plan.NewUnresolvedRelation("mytable")
	analyzed, err := a.Analyze(notAnalyzed)
	assert.Nil(err)
	assert.Equal(table, analyzed)

	notAnalyzed = plan.NewUnresolvedRelation("nonexistant")
	analyzed, err = a.Analyze(notAnalyzed)
	assert.NotNil(err)
	assert.Equal(notAnalyzed, analyzed)

	analyzed, err = a.Analyze(table)
	assert.Nil(err)
	assert.Equal(table, analyzed)

	notAnalyzed = plan.NewProject(
		[]sql.Expression{expression.NewUnresolvedColumn("i")},
		plan.NewUnresolvedRelation("mytable"),
	)
	analyzed, err = a.Analyze(notAnalyzed)
	expected := plan.NewProject(
		[]sql.Expression{expression.NewGetField(0, sql.Integer, "i")},
		table,
	)
	assert.Nil(err)
	assert.Equal(expected, analyzed)
}

func TestAnalyzer_Analyze_MaxIterations(t *testing.T) {
	assert := require.New(t)

	catalog := &sql.Catalog{}
	a := analyzer.New(catalog)
	a.CurrentDatabase = "mydb"

	i := 0
	a.Rules = []analyzer.Rule{{
		"infinite",
		func(a *analyzer.Analyzer, n sql.Node) sql.Node {
			i += 1
			return plan.NewUnresolvedRelation(fmt.Sprintf("rel%d", i))
		},
	}}

	notAnalyzed := plan.NewUnresolvedRelation("mytable")
	analyzed, err := a.Analyze(notAnalyzed)
	assert.NotNil(err)
	assert.Equal(plan.NewUnresolvedRelation("rel1001"), analyzed)
}
