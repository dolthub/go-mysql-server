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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// TestPreparedStatementQueryTracking asserts that there is no process tracking done when preparing a statement, and
// that when a prepared statement is analyzed, it correctly gets a QueryProcess node added so that its process is tracked.
func TestPreparedStatementQueryTracking(t *testing.T) {
	ctx := sql.NewContext(context.Background())

	node := plan.NewProject(
		[]sql.Expression{expression.NewStar()}, plan.NewUnresolvedTable("commits", ""))

	commits := memory.NewTable("commits", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "repository_id", Source: "commits", Type: types.Text},
		{Name: "commit_hash", Source: "commits", Type: types.Text},
		{Name: "commit_author_when", Source: "commits", Type: types.Text},
	}), nil)

	db := memory.NewDatabase("mydb")
	db.AddTable("commits", commits)
	ctx.SetCurrentDatabase("mydb")

	provider := sql.NewDatabaseProvider(db)
	a := NewDefault(provider)

	prepared, err := a.PrepareQuery(ctx, node, nil)
	require.NoError(t, err)

	// Preparing a query should NOT apply process tracking
	_, isQueryProcess := prepared.(*plan.QueryProcess)
	require.False(t, isQueryProcess)

	// Analyzing a prepared query SHOULD apply process tracking
	analyzed, identity, err := a.AnalyzePrepared(ctx, prepared, nil)
	require.NoError(t, err)
	require.Equal(t, transform.NewTree, identity)
	_, isQueryProcess = analyzed.(*plan.QueryProcess)
	require.True(t, isQueryProcess)
}

func TestTrackProcessSubquery(t *testing.T) {
	require := require.New(t)
	rule := getRuleFrom(OnceAfterAll_Experimental, TrackProcessId)
	a := NewDefault(sql.NewDatabaseProvider())

	node := plan.NewProject(
		nil,
		plan.NewSubqueryAlias("f", "",
			plan.NewQueryProcess(
				plan.NewResolvedTable(memory.NewTable("foo", sql.PrimaryKeySchema{}, nil), nil, nil),
				nil,
			),
		),
	)

	result, _, err := rule.Apply(sql.NewEmptyContext(), a, node, nil, DefaultRuleSelector)
	require.NoError(err)

	expectedChild := plan.NewProject(
		nil,
		plan.NewSubqueryAlias("f", "",
			plan.NewResolvedTable(memory.NewTable("foo", sql.PrimaryKeySchema{}, nil), nil, nil),
		),
	)

	proc, ok := result.(*plan.QueryProcess)
	require.True(ok)
	require.Equal(expectedChild, proc.Child())
}

func withoutProcessTracking(a *Analyzer) *Analyzer {
	afterAll := a.Batches[len(a.Batches)-1]
	afterAll.Rules = afterAll.Rules[2:]
	return a
}

// wrapper around sql.Table to make it not indexable
type table struct {
	sql.Table
}

var _ sql.PartitionCounter = (*table)(nil)

func (t *table) PartitionCount(ctx *sql.Context) (int64, error) {
	return t.Table.(sql.PartitionCounter).PartitionCount(ctx)
}
