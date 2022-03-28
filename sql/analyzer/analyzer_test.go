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
	"github.com/dolthub/go-mysql-server/sql/plan"
)

func TestMaxIterations(t *testing.T) {
	require := require.New(t)
	tName := "my-table"
	db := memory.NewDatabase("mydb")
	table := memory.NewTable(tName, sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "i", Type: sql.Int32, Source: tName},
		{Name: "t", Type: sql.Text, Source: tName},
	}), db.GetForeignKeyCollection())
	db.AddTable(tName, table)

	provider := sql.NewDatabaseProvider(db)

	count := 0
	a := withoutProcessTracking(NewBuilder(provider).AddPostAnalyzeRule("loop",
		func(c *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {

			switch n.(type) {
			case *plan.ResolvedTable:
				count++
				name := fmt.Sprintf("mytable-%v", count)
				table := memory.NewTable(name, sql.NewPrimaryKeySchema(sql.Schema{
					{Name: "i", Type: sql.Int32, Source: name},
					{Name: "t", Type: sql.Text, Source: name},
				}), db.GetForeignKeyCollection())
				n = plan.NewResolvedTable(table, nil, nil)
			}

			return n, nil
		}).Build())

	ctx := sql.NewContext(context.Background()).WithCurrentDB("mydb")
	notAnalyzed := plan.NewUnresolvedTable(tName, "")
	analyzed, err := a.Analyze(ctx, notAnalyzed, nil)
	require.Error(err)
	require.True(ErrMaxAnalysisIters.Is(err))
	require.Equal(
		plan.NewResolvedTable(memory.NewTable("mytable-8", sql.NewPrimaryKeySchema(sql.Schema{
			{Name: "i", Type: sql.Int32, Source: "mytable-8"},
			{Name: "t", Type: sql.Text, Source: "mytable-8"},
		}), db.GetForeignKeyCollection()), nil, nil),
		analyzed,
	)
	require.Equal(maxAnalysisIterations, count)
}

func TestAddRule(t *testing.T) {
	require := require.New(t)

	defRulesCount := countRules(NewDefault(nil).Batches)

	a := NewBuilder(nil).AddPostAnalyzeRule("foo", pushdownFilters).Build()

	require.Equal(countRules(a.Batches), defRulesCount+1)
}

func TestAddPreValidationRule(t *testing.T) {
	require := require.New(t)

	defRulesCount := countRules(NewDefault(nil).Batches)

	a := NewBuilder(nil).AddPreValidationRule("foo", pushdownFilters).Build()

	require.Equal(countRules(a.Batches), defRulesCount+1)
}

func TestAddPostValidationRule(t *testing.T) {
	require := require.New(t)

	defRulesCount := countRules(NewDefault(nil).Batches)

	a := NewBuilder(nil).AddPostValidationRule("foo", pushdownFilters).Build()

	require.Equal(countRules(a.Batches), defRulesCount+1)
}

func TestRemoveOnceBeforeRule(t *testing.T) {
	require := require.New(t)

	a := NewBuilder(nil).RemoveOnceBeforeRule("resolve_views").Build()

	defRulesCount := countRules(NewDefault(nil).Batches)

	require.Equal(countRules(a.Batches), defRulesCount-1)
}

func TestRemoveDefaultRule(t *testing.T) {
	require := require.New(t)

	a := NewBuilder(nil).RemoveDefaultRule("resolve_natural_joins").Build()

	defRulesCount := countRules(NewDefault(nil).Batches)

	require.Equal(countRules(a.Batches), defRulesCount-1)
}

func TestRemoveOnceAfterRule(t *testing.T) {
	require := require.New(t)

	a := NewBuilder(nil).RemoveOnceAfterRule("load_triggers").Build()

	defRulesCount := countRules(NewDefault(nil).Batches)

	require.Equal(countRules(a.Batches), defRulesCount-1)
}

func TestRemoveValidationRule(t *testing.T) {
	require := require.New(t)

	a := NewBuilder(nil).RemoveValidationRule(validateResolvedRule).Build()

	defRulesCount := countRules(NewDefault(nil).Batches)

	require.Equal(countRules(a.Batches), defRulesCount-1)
}

func TestRemoveAfterAllRule(t *testing.T) {
	require := require.New(t)

	a := NewBuilder(nil).RemoveAfterAllRule("track_process").Build()

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

func TestMixInnerAndNaturalJoins(t *testing.T) {
	var require = require.New(t)

	table := memory.NewFilteredTable("mytable", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "i", Type: sql.Int32, Source: "mytable"},
		{Name: "f", Type: sql.Float64, Source: "mytable"},
		{Name: "t", Type: sql.Text, Source: "mytable"},
	}), nil)

	table2 := memory.NewFilteredTable("mytable2", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "i2", Type: sql.Int32, Source: "mytable2"},
		{Name: "f2", Type: sql.Float64, Source: "mytable2"},
		{Name: "t2", Type: sql.Text, Source: "mytable2"},
	}), nil)

	table3 := memory.NewFilteredTable("mytable3", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "i", Type: sql.Int32, Source: "mytable3"},
		{Name: "f2", Type: sql.Float64, Source: "mytable3"},
		{Name: "t3", Type: sql.Text, Source: "mytable3"},
	}), nil)

	db := memory.NewDatabase("mydb")
	db.AddTable("mytable", table)
	db.AddTable("mytable2", table2)
	db.AddTable("mytable3", table3)

	provider := sql.NewDatabaseProvider(db)
	a := withoutProcessTracking(NewDefault(provider))

	node := plan.NewProject(
		[]sql.Expression{
			expression.NewStar(),
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
			plan.NewUnresolvedTable("mytable3", ""),
		),
	)

	expected := plan.NewProject(
		[]sql.Expression{
			expression.NewGetFieldWithTable(0, sql.Int32, "mytable", "i", false),
			expression.NewGetFieldWithTable(4, sql.Float64, "mytable2", "f2", false),
			expression.NewGetFieldWithTable(1, sql.Float64, "mytable", "f", false),
			expression.NewGetFieldWithTable(2, sql.Text, "mytable", "t", false),
			expression.NewGetFieldWithTable(3, sql.Int32, "mytable2", "i2", false),
			expression.NewGetFieldWithTable(5, sql.Text, "mytable2", "t2", false),
			expression.NewGetFieldWithTable(8, sql.Text, "mytable3", "t3", false),
		},
		plan.NewInnerJoin(
			plan.NewInnerJoin(
				plan.NewDecoratedNode("Projected table access on [i f t]", plan.NewResolvedTable(table.WithProjection([]string{"i", "f", "t"}), db, nil)),
				plan.NewDecoratedNode("Projected table access on [f2 i2 t2]", plan.NewResolvedTable(table2.WithProjection([]string{"f2", "i2", "t2"}), db, nil)),
				expression.NewEquals(
					expression.NewGetFieldWithTable(0, sql.Int32, "mytable", "i", false),
					expression.NewGetFieldWithTable(3, sql.Int32, "mytable2", "i2", false),
				),
			),
			plan.NewDecoratedNode("Projected table access on [t3 i f2]", plan.NewResolvedTable(table3.WithProjection([]string{"t3", "i", "f2"}), db, nil)),
			expression.NewAnd(
				expression.NewEquals(
					expression.NewGetFieldWithTable(0, sql.Int32, "mytable", "i", false),
					expression.NewGetFieldWithTable(6, sql.Int32, "mytable3", "i", false),
				),
				expression.NewEquals(
					expression.NewGetFieldWithTable(4, sql.Float64, "mytable2", "f2", false),
					expression.NewGetFieldWithTable(7, sql.Float64, "mytable3", "f2", false),
				),
			),
		),
	)

	ctx := sql.NewContext(context.Background()).WithCurrentDB("mydb")
	result, err := a.Analyze(ctx, node, nil)
	require.NoError(err)

	assertNodesEqualWithDiff(t, expected, result)
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
					expression.NewLiteral("foo", sql.LongText),
				),
				expression.NewEquals(
					expression.NewUnresolvedQualifiedColumn("rc", "ref_name"),
					expression.NewLiteral("HEAD", sql.LongText),
				),
				expression.NewEquals(
					expression.NewUnresolvedQualifiedColumn("rc", "history_index"),
					expression.NewLiteral(int64(0), sql.Int64),
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
		{Name: "repository_id", Source: "commits", Type: sql.Text},
		{Name: "commit_hash", Source: "commits", Type: sql.Text},
		{Name: "commit_author_when", Source: "commits", Type: sql.Text},
	}), nil)

	refs := memory.NewTable("refs", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "repository_id", Source: "refs", Type: sql.Text},
		{Name: "ref_name", Source: "refs", Type: sql.Text},
	}), nil)

	refCommits := memory.NewTable("ref_commits", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "repository_id", Source: "ref_commits", Type: sql.Text},
		{Name: "ref_name", Source: "ref_commits", Type: sql.Text},
		{Name: "commit_hash", Source: "ref_commits", Type: sql.Text},
		{Name: "history_index", Source: "ref_commits", Type: sql.Int64},
	}), nil)

	db := memory.NewDatabase("")
	db.AddTable("refs", refs)
	db.AddTable("ref_commits", refCommits)
	db.AddTable("commits", commits)

	provider := sql.NewDatabaseProvider(db)
	a := withoutProcessTracking(NewDefault(provider))

	ctx := sql.NewContext(context.Background())
	result, err := a.Analyze(ctx, node, nil)
	require.NoError(err)
	require.True(result.Resolved())
}
