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

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/expression/function/aggregation"
	"github.com/dolthub/go-mysql-server/sql/information_schema"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestMaxIterations(t *testing.T) {
	require := require.New(t)
	tName := "my-table"
	db := memory.NewDatabase("mydb")
	table := memory.NewTable(tName, sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "i", Type: types.Int32, Source: tName},
		{Name: "t", Type: types.Text, Source: tName},
	}), db.GetForeignKeyCollection())
	db.AddTable(tName, table)

	provider := sql.NewDatabaseProvider(db)

	count := 0
	a := withoutProcessTracking(NewBuilder(provider).AddPostAnalyzeRule(-1,
		func(c *sql.Context, a *Analyzer, n sql.Node, scope *plan.Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
			switch n.(type) {
			case *plan.ResolvedTable:
				count++
				name := fmt.Sprintf("mytable-%v", count)
				table := memory.NewTable(name, sql.NewPrimaryKeySchema(sql.Schema{
					{Name: "i", Type: types.Int32, Source: name},
					{Name: "t", Type: types.Text, Source: name},
				}), db.GetForeignKeyCollection())
				n = plan.NewResolvedTable(table, nil, nil)
			}

			return n, transform.NewTree, nil
		}).Build())

	ctx := sql.NewContext(context.Background())
	ctx.SetCurrentDatabase("mydb")
	notAnalyzed := plan.NewUnresolvedTable(tName, "")
	analyzed, err := a.Analyze(ctx, notAnalyzed, nil)
	require.Error(err)
	require.True(ErrMaxAnalysisIters.Is(err))
	require.Equal(
		plan.NewResolvedTable(memory.NewTable("mytable-8", sql.NewPrimaryKeySchema(sql.Schema{
			{Name: "i", Type: types.Int32, Source: "mytable-8"},
			{Name: "t", Type: types.Text, Source: "mytable-8"},
		}), db.GetForeignKeyCollection()), nil, nil),
		analyzed,
	)
	require.Equal(maxAnalysisIterations, count)
}

func TestAddRule(t *testing.T) {
	require := require.New(t)

	defRulesCount := countRules(NewDefault(nil).Batches)

	a := NewBuilder(nil).AddPostAnalyzeRule(-1, pushdownFilters).Build()

	require.Equal(countRules(a.Batches), defRulesCount+1)
}

func TestAddPreValidationRule(t *testing.T) {
	require := require.New(t)

	defRulesCount := countRules(NewDefault(nil).Batches)

	a := NewBuilder(nil).AddPreValidationRule(-1, pushdownFilters).Build()

	require.Equal(countRules(a.Batches), defRulesCount+1)
}

func TestAddPostValidationRule(t *testing.T) {
	require := require.New(t)

	defRulesCount := countRules(NewDefault(nil).Batches)

	a := NewBuilder(nil).AddPostValidationRule(-1, pushdownFilters).Build()

	require.Equal(countRules(a.Batches), defRulesCount+1)
}

func TestRemoveOnceBeforeRule(t *testing.T) {
	require := require.New(t)

	a := NewBuilder(nil).RemoveOnceBeforeRule(resolveViewsId).Build()

	defRulesCount := countRules(NewDefault(nil).Batches)

	require.Equal(countRules(a.Batches), defRulesCount-1)
}

func TestRemoveDefaultRule(t *testing.T) {
	require := require.New(t)

	a := NewBuilder(nil).RemoveDefaultRule(resolveNaturalJoinsId).Build()

	defRulesCount := countRules(NewDefault(nil).Batches)

	require.Equal(countRules(a.Batches), defRulesCount-1)
}

func TestRemoveOnceAfterRule(t *testing.T) {
	require := require.New(t)

	a := NewBuilder(nil).RemoveOnceAfterRule(loadTriggersId).Build()

	defRulesCount := countRules(NewDefault(nil).Batches)

	require.Equal(countRules(a.Batches), defRulesCount-1)
}

func TestRemoveValidationRule(t *testing.T) {
	require := require.New(t)

	a := NewBuilder(nil).RemoveValidationRule(validateResolvedId).Build()

	defRulesCount := countRules(NewDefault(nil).Batches)

	require.Equal(countRules(a.Batches), defRulesCount-1)
}

func TestRemoveAfterAllRule(t *testing.T) {
	require := require.New(t)

	a := NewBuilder(nil).RemoveAfterAllRule(TrackProcessId).Build()

	defRulesCount := countRules(NewDefault(nil).Batches)

	require.Equal(countRules(a.Batches), defRulesCount-1)
}

func countRules(batches []*Batch) int {
	var count int
	for _, b := range batches {
		count = count + len(b.Rules)
	}
	return count

}

func TestReorderProjectionUnresolvedChild(t *testing.T) {
	require := require.New(t)
	node := plan.NewProject(
		[]sql.Expression{
			expression.NewUnresolvedQualifiedColumn("rc", "commit_hash"),
			expression.NewUnresolvedColumn("commit_author_when"),
		},
		plan.NewFilter(
			expression.JoinAnd(
				expression.NewEquals(
					expression.NewUnresolvedQualifiedColumn("rc", "repository_id"),
					expression.NewLiteral("foo", types.LongText),
				),
				expression.NewEquals(
					expression.NewUnresolvedQualifiedColumn("rc", "ref_name"),
					expression.NewLiteral("HEAD", types.LongText),
				),
				expression.NewEquals(
					expression.NewUnresolvedQualifiedColumn("rc", "history_index"),
					expression.NewLiteral(int64(0), types.Int64),
				),
			),
			plan.NewNaturalJoin(
				plan.NewInnerJoin(
					plan.NewUnresolvedTable("refs", ""),
					plan.NewTableAlias("rc",
						plan.NewUnresolvedTable("ref_commits", ""),
					),
					expression.NewAnd(
						expression.NewEquals(
							expression.NewUnresolvedQualifiedColumn("refs", "ref_name"),
							expression.NewUnresolvedQualifiedColumn("rc", "ref_name"),
						),
						expression.NewEquals(
							expression.NewUnresolvedQualifiedColumn("refs", "repository_id"),
							expression.NewUnresolvedQualifiedColumn("rc", "repository_id"),
						),
					),
				),
				plan.NewTableAlias("c",
					plan.NewUnresolvedTable("commits", ""),
				),
			),
		),
	)

	commits := memory.NewTable("commits", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "repository_id", Source: "commits", Type: types.Text},
		{Name: "commit_hash", Source: "commits", Type: types.Text},
		{Name: "commit_author_when", Source: "commits", Type: types.Text},
	}), nil)

	refs := memory.NewTable("refs", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "repository_id", Source: "refs", Type: types.Text},
		{Name: "ref_name", Source: "refs", Type: types.Text},
	}), nil)

	refCommits := memory.NewTable("ref_commits", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "repository_id", Source: "ref_commits", Type: types.Text},
		{Name: "ref_name", Source: "ref_commits", Type: types.Text},
		{Name: "commit_hash", Source: "ref_commits", Type: types.Text},
		{Name: "history_index", Source: "ref_commits", Type: types.Int64},
	}), nil)

	db := memory.NewDatabase("")
	db.AddTable("refs", refs)
	db.AddTable("ref_commits", refCommits)
	db.AddTable("commits", commits)

	provider := sql.NewDatabaseProvider(db, information_schema.NewInformationSchemaDatabase())
	a := withoutProcessTracking(NewDefault(provider))

	ctx := sql.NewContext(context.Background())
	result, err := a.Analyze(ctx, node, nil)
	require.NoError(err)
	require.True(result.Resolved())
}

func TestDeepCopyNode(t *testing.T) {
	tests := []struct {
		node sql.Node
		exp  sql.Node
	}{
		{
			node: plan.NewProject(
				[]sql.Expression{
					expression.NewLiteral(1, types.Int64),
				},
				plan.NewNaturalJoin(
					plan.NewInnerJoin(
						plan.NewUnresolvedTable("mytable", ""),
						plan.NewUnresolvedTable("mytable2", ""),
						expression.NewEquals(
							expression.NewUnresolvedQualifiedColumn("mytable", "i"),
							expression.NewUnresolvedQualifiedColumn("mytable2", "i2"),
						),
					),
					plan.NewFilter(
						expression.NewEquals(
							expression.NewBindVar("v1"),
							expression.NewBindVar("v2"),
						),
						plan.NewUnresolvedTable("mytable3", ""),
					),
				),
			),
		},
		{
			node: plan.NewProject(
				[]sql.Expression{
					expression.NewLiteral(1, types.Int64),
				},
				plan.NewUnion(plan.NewProject(
					[]sql.Expression{
						expression.NewLiteral(1, types.Int64),
					},
					plan.NewUnresolvedTable("mytable", ""),
				), plan.NewProject(
					[]sql.Expression{
						expression.NewBindVar("v1"),
						expression.NewBindVar("v2"),
					},
					plan.NewUnresolvedTable("mytable", ""),
				), false, nil, nil, nil),
			),
		},
		{
			node: plan.NewFilter(
				expression.NewEquals(
					expression.NewLiteral(1, types.Int64),
					expression.NewLiteral(1, types.Int64),
				),
				plan.NewWindow(
					[]sql.Expression{
						aggregation.NewSum(
							expression.NewGetFieldWithTable(0, types.Int64, "a", "x", false),
						),
						expression.NewGetFieldWithTable(1, types.Int64, "a", "x", false),
						expression.NewBindVar("v1"),
					},
					plan.NewProject(
						[]sql.Expression{
							expression.NewBindVar("v2"),
						},
						plan.NewUnresolvedTable("x", ""),
					),
				),
			),
		},
		{
			node: plan.NewFilter(
				expression.NewEquals(
					expression.NewLiteral(1, types.Int64),
					expression.NewLiteral(1, types.Int64),
				),
				plan.NewSubqueryAlias("cte1", "select x from a",
					plan.NewProject(
						[]sql.Expression{
							expression.NewBindVar("v1"),
							expression.NewUnresolvedColumn("v2"),
						},
						plan.NewUnresolvedTable("a", ""),
					),
				),
			),
		},
	}

	for i, tt := range tests {
		t.Run(fmt.Sprintf("DeepCopyTest_%d", i), func(t *testing.T) {
			cop, err := DeepCopyNode(tt.node)
			require.NoError(t, err)
			cop, _, err = plan.ApplyBindings(cop, map[string]sql.Expression{
				"v1": expression.NewLiteral(1, types.Int64),
				"v2": expression.NewLiteral("x", types.Text),
			})
			require.NoError(t, err)
			require.NotEqual(t, cop, tt.node)
		})
	}
}
