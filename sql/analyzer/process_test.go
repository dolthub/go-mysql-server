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
	"time"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

func TestTrackProcess(t *testing.T) {
	require := require.New(t)
	rule := getRuleFrom(OnceAfterAll, "track_process")
	catalog := sql.NewCatalog()
	a := NewDefault(catalog)

	node := plan.NewInnerJoin(
		plan.NewResolvedTable(&table{memory.NewPartitionedTable("foo", nil, 2)}, nil, nil),
		plan.NewResolvedTable(memory.NewPartitionedTable("bar", nil, 4), nil, nil),
		expression.NewLiteral(int64(1), sql.Int64),
	)

	ctx := sql.NewContext(context.Background(), sql.WithPid(1))
	ctx, err := catalog.AddProcess(ctx, "SELECT foo")
	require.NoError(err)

	result, err := rule.Apply(ctx, a, node, nil)
	require.NoError(err)

	processes := catalog.Processes()
	require.Len(processes, 1)
	require.Equal("SELECT foo", processes[0].Query)
	require.Equal(
		map[string]sql.TableProgress{
			"foo": sql.TableProgress{
				Progress:           sql.Progress{Name: "foo", Done: 0, Total: 2},
				PartitionsProgress: map[string]sql.PartitionProgress{}},
			"bar": sql.TableProgress{
				Progress:           sql.Progress{Name: "bar", Done: 0, Total: 4},
				PartitionsProgress: map[string]sql.PartitionProgress{}},
		},
		processes[0].Progress)

	proc, ok := result.(*plan.QueryProcess)
	require.True(ok)

	join, ok := proc.Child.(*plan.InnerJoin)
	require.True(ok)

	lhs, ok := join.Left().(*plan.ResolvedTable)
	require.True(ok)
	_, ok = lhs.Table.(*plan.ProcessTable)
	require.True(ok)

	rhs, ok := join.Right().(*plan.ResolvedTable)
	require.True(ok)
	_, ok = rhs.Table.(*plan.ProcessIndexableTable)
	require.True(ok)

	iter, err := proc.RowIter(ctx, nil)
	require.NoError(err)
	_, err = sql.RowIterToRows(ctx, iter)
	require.NoError(err)

	require.Len(catalog.Processes(), 0)

	select {
	case <-ctx.Done():
	case <-time.After(5 * time.Millisecond):
		t.Errorf("expecting context to be cancelled")
	}
}

func TestTrackProcessSubquery(t *testing.T) {
	require := require.New(t)
	rule := getRuleFrom(OnceAfterAll, "track_process")
	catalog := sql.NewCatalog()
	a := NewDefault(catalog)

	node := plan.NewProject(
		nil,
		plan.NewSubqueryAlias("f", "",
			plan.NewQueryProcess(
				plan.NewResolvedTable(memory.NewTable("foo", nil), nil, nil),
				nil,
			),
		),
	)

	result, err := rule.Apply(sql.NewEmptyContext(), a, node, nil)
	require.NoError(err)

	expectedChild := plan.NewProject(
		nil,
		plan.NewSubqueryAlias("f", "",
			plan.NewResolvedTable(memory.NewTable("foo", nil), nil, nil),
		),
	)

	proc, ok := result.(*plan.QueryProcess)
	require.True(ok)
	require.Equal(expectedChild, proc.Child)
}

func withoutProcessTracking(a *Analyzer) *Analyzer {
	afterAll := a.Batches[len(a.Batches)-1]
	afterAll.Rules = afterAll.Rules[1:]
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
