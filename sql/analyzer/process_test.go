package analyzer

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/mem"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
	"gopkg.in/src-d/go-mysql-server.v0/sql/plan"
)

func TestTrackProcess(t *testing.T) {
	require := require.New(t)
	rule := getRuleFrom(OnceAfterAll, "track_process")
	catalog := sql.NewCatalog()
	a := NewDefault(catalog)

	node := plan.NewInnerJoin(
		plan.NewResolvedTable(&table{mem.NewPartitionedTable("foo", nil, 2)}),
		plan.NewResolvedTable(mem.NewPartitionedTable("bar", nil, 4)),
		expression.NewLiteral(int64(1), sql.Int64),
	)

	ctx := sql.NewContext(context.Background(), sql.WithPid(1))
	ctx, err := catalog.AddProcess(ctx, sql.QueryProcess, "SELECT foo")
	require.NoError(err)

	result, err := rule.Apply(ctx, a, node)
	require.NoError(err)

	processes := catalog.Processes()
	require.Len(processes, 1)
	require.Equal("SELECT foo", processes[0].Query)
	require.Equal(sql.QueryProcess, processes[0].Type)
	require.Equal(map[string]sql.Progress{
		"foo": sql.Progress{Total: 2},
		"bar": sql.Progress{Total: 4},
	}, processes[0].Progress)

	proc, ok := result.(*plan.QueryProcess)
	require.True(ok)

	join, ok := proc.Child.(*plan.InnerJoin)
	require.True(ok)

	lhs, ok := join.Left.(*plan.ResolvedTable)
	require.True(ok)
	_, ok = lhs.Table.(*plan.ProcessTable)
	require.True(ok)

	rhs, ok := join.Right.(*plan.ResolvedTable)
	require.True(ok)
	_, ok = rhs.Table.(*plan.ProcessIndexableTable)
	require.True(ok)

	iter, err := proc.RowIter(ctx)
	require.NoError(err)
	_, err = sql.RowIterToRows(iter)
	require.NoError(err)

	require.Len(catalog.Processes(), 0)

	select {
	case <-ctx.Done():
	case <-time.After(5 * time.Millisecond):
		t.Errorf("expecting context to be cancelled")
	}
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
