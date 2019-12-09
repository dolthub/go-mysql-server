package analyzer

import (
	"testing"

	"github.com/src-d/go-mysql-server/memory"
	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/expression"
	"github.com/src-d/go-mysql-server/sql/plan"
	"github.com/stretchr/testify/require"
)

func TestResolveViews(t *testing.T) {
	require := require.New(t)

	f := getRule("resolve_views")

	viewDefinition := plan.NewSubqueryAlias(
		"myview",
		plan.NewProject(
			[]sql.Expression{expression.NewUnresolvedColumn("i")},
			plan.NewUnresolvedTable("mytable", ""),
		),
	)
	view := sql.NewView("myview", viewDefinition)

	db := memory.NewDatabase("mydb")
	catalog := sql.NewCatalog()
	catalog.AddDatabase(db)
	err := catalog.ViewRegistry.Register(db.Name(), view)
	require.NoError(err)

	a := NewBuilder(catalog).AddPostAnalyzeRule(f.Name, f.Apply).Build()

	var notAnalyzed sql.Node = plan.NewUnresolvedTable("myview", "")
	analyzed, err := f.Apply(sql.NewEmptyContext(), a, notAnalyzed)
	require.NoError(err)
	require.Equal(viewDefinition, analyzed)
}
