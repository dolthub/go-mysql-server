// Copyright 2020-2021 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package analyzer

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gabereiser/go-mysql-server/memory"
	"github.com/gabereiser/go-mysql-server/sql"
	"github.com/gabereiser/go-mysql-server/sql/expression"
	"github.com/gabereiser/go-mysql-server/sql/plan"
	"github.com/gabereiser/go-mysql-server/sql/types"
)

var viewDefinition = plan.NewSubqueryAlias(
	"myview1", "select i from mytable",
	plan.NewProject(
		[]sql.Expression{expression.NewUnresolvedColumn("i")},
		plan.NewUnresolvedTable("mytable", ""),
	),
)

var viewDefinitionWithUnion = plan.NewSubqueryAlias(
	"myview2", "select i from mytable1 union select i from mytable2",
	plan.NewProject(
		[]sql.Expression{expression.NewUnresolvedColumn("i")},
		plan.NewUnion(plan.NewUnresolvedTable("mytable1", ""), plan.NewUnresolvedTable("mytable2", ""), false, nil, nil),
	),
)

var viewDefinitionWithAsOf = plan.NewSubqueryAlias(
	"viewWithAsOf", "select i from mytable as of '2019-01-01'",
	plan.NewProject(
		[]sql.Expression{expression.NewUnresolvedColumn("i")},
		plan.NewUnresolvedTableAsOf("mytable", "", expression.NewLiteral("2019-01-01", types.LongText)),
	),
)

func TestResolveViews(t *testing.T) {
	// Initialize views and DB
	view := sql.NewView(viewDefinition.Name(), viewDefinition, viewDefinition.TextDefinition, getCreateViewStr(viewDefinition.Name(), viewDefinition.TextDefinition))
	viewWithUnion := sql.NewView(viewDefinitionWithUnion.Name(), viewDefinitionWithUnion, viewDefinitionWithUnion.TextDefinition, getCreateViewStr(viewDefinitionWithUnion.Name(), viewDefinitionWithUnion.TextDefinition))
	viewWithAsOf := sql.NewView(viewDefinitionWithAsOf.Name(), viewDefinitionWithAsOf, viewDefinitionWithAsOf.TextDefinition, getCreateViewStr(viewDefinitionWithAsOf.Name(), viewDefinitionWithAsOf.TextDefinition))

	db := memory.NewDatabase("mydb")
	viewReg := sql.NewViewRegistry()
	require.NoError(t, viewReg.Register(db.Name(), view))
	require.NoError(t, viewReg.Register(db.Name(), viewWithUnion))
	require.NoError(t, viewReg.Register(db.Name(), viewWithAsOf))

	f := getRule(resolveViewsId)
	a := NewBuilder(sql.NewDatabaseProvider(db)).AddPostAnalyzeRule(f.Id, f.Apply).Build()

	sess := sql.NewBaseSession()
	sess.SetViewRegistry(viewReg)
	ctx := sql.NewContext(context.Background(), sql.WithSession(sess))
	ctx.SetCurrentDatabase("mydb")

	// AS OF expressions on a view should be pushed down to unresolved tables
	var notAnalyzed sql.Node = plan.NewUnresolvedTable("myview1", "")
	analyzed, _, err := f.Apply(ctx, a, notAnalyzed, nil, DefaultRuleSelector)
	require.NoError(t, err)
	require.Equal(t, viewDefinition, analyzed)
	expectedViewDefinition := plan.NewSubqueryAlias(
		"myview1", "select i from mytable",
		plan.NewProject(
			[]sql.Expression{expression.NewUnresolvedColumn("i")},
			plan.NewUnresolvedTableAsOf("mytable", "", expression.NewLiteral("2019-01-01", types.LongText)),
		),
	)
	var notAnalyzedAsOf sql.Node = plan.NewUnresolvedTableAsOf("myview1", "", expression.NewLiteral("2019-01-01", types.LongText))
	analyzed, _, err = f.Apply(ctx, a, notAnalyzedAsOf, nil, DefaultRuleSelector)
	require.NoError(t, err)
	require.Equal(t, expectedViewDefinition, analyzed)

	// Views using a union statement should have AsOf pushed to their unresolved tables, even though union is opaque
	expectedViewDefinition = plan.NewSubqueryAlias(
		"myview2", "select i from mytable1 union select i from mytable2",
		plan.NewProject(
			[]sql.Expression{expression.NewUnresolvedColumn("i")},
			plan.NewUnion(plan.NewUnresolvedTableAsOf("mytable1", "", expression.NewLiteral("2019-01-01", types.LongText)), plan.NewUnresolvedTableAsOf("mytable2", "", expression.NewLiteral("2019-01-01", types.LongText)), false, nil, nil),
		),
	)
	notAnalyzedAsOf = plan.NewUnresolvedTableAsOf("myview2", "", expression.NewLiteral("2019-01-01", types.LongText))
	analyzed, _, err = f.Apply(ctx, a, notAnalyzedAsOf, nil, DefaultRuleSelector)
	require.NoError(t, err)
	require.Equal(t, expectedViewDefinition, analyzed)

	// Views that are defined with AS OF clauses cannot have an AS OF pushed down to them
	notAnalyzedAsOf = plan.NewUnresolvedTableAsOf("viewWithAsOf", "", expression.NewLiteral("2019-01-01", types.LongText))
	analyzed, _, err = f.Apply(ctx, a, notAnalyzedAsOf, nil, DefaultRuleSelector)
	require.Error(t, err)
	require.True(t, sql.ErrIncompatibleAsOf.Is(err), "wrong error type")
}

func getCreateViewStr(name, def string) string {
	return fmt.Sprintf("CREATE VIEW %s AS %s", name, def)
}
