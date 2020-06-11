package analyzer

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/liquidata-inc/go-mysql-server/memory"
	"github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/liquidata-inc/go-mysql-server/sql/expression"
	"github.com/liquidata-inc/go-mysql-server/sql/plan"
)

func TestResolveViews(t *testing.T) {
	require := require.New(t)

	f := getRule("resolve_views")

	viewDefinition := plan.NewSubqueryAlias(
		"myview", "select i from mytable",
		plan.NewProject(
			[]sql.Expression{expression.NewUnresolvedColumn("i")},
			plan.NewUnresolvedTable("mytable", ""),
		),
	)
	view := sql.NewView("myview", viewDefinition, "select i from mytable")

	db := memory.NewDatabase("mydb")
	catalog := sql.NewCatalog()
	catalog.AddDatabase(db)
	viewReg := sql.NewViewRegistry()
	err := viewReg.Register(db.Name(), view)
	require.NoError(err)

	a := NewBuilder(catalog).AddPostAnalyzeRule(f.Name, f.Apply).Build()

	ctx := sql.NewContext(context.Background(), sql.WithIndexRegistry(sql.NewIndexRegistry()), sql.WithViewRegistry(viewReg)).WithCurrentDB("mydb")
	// AS OF expressions on a view should be pushed down to unresolved tables
	var notAnalyzed sql.Node = plan.NewUnresolvedTable("myview", "")
	analyzed, err := f.Apply(ctx, a, notAnalyzed, nil)
	require.NoError(err)
	require.Equal(viewDefinition, analyzed)

	viewDefinitionWithAsOf := plan.NewSubqueryAlias(
		"myview", "select i from mytable",
		plan.NewProject(
			[]sql.Expression{expression.NewUnresolvedColumn("i")},
			plan.NewUnresolvedTableAsOf("mytable", "", expression.NewLiteral("2019-01-01", sql.LongText)),
		),
	)
	var notAnalyzedAsOf sql.Node = plan.NewUnresolvedTableAsOf("myview", "", expression.NewLiteral("2019-01-01", sql.LongText))

	analyzed, err = f.Apply(ctx, a, notAnalyzedAsOf, nil)
	require.NoError(err)
	require.Equal(viewDefinitionWithAsOf, analyzed)

	// Views that are defined with AS OF clauses cannot have an AS OF pushed down to them
	viewWithAsOf := sql.NewView("viewWithAsOf", viewDefinitionWithAsOf, "select i from mytable as of '2019-01-01'")
	err = viewReg.Register(db.Name(), viewWithAsOf)
	require.NoError(err)

	notAnalyzedAsOf = plan.NewUnresolvedTableAsOf("viewWithAsOf", "", expression.NewLiteral("2019-01-01", sql.LongText))
	analyzed, err = f.Apply(ctx, a, notAnalyzedAsOf, nil)
	require.Error(err)
	require.True(sql.ErrIncompatibleAsOf.Is(err), "wrong error type")
}
