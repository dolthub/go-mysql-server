package plan

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
)

func TestShowProcessList(t *testing.T) {
	require := require.New(t)

	addr := "127.0.0.1:34567"

	n := NewShowProcessList()
	p := sql.NewProcessList()
	sess := sql.NewSession("0.0.0.0:3306", addr, "foo", 1)
	ctx := sql.NewContext(context.Background(), sql.WithPid(1), sql.WithSession(sess))

	ctx, err := p.AddProcess(ctx, sql.QueryProcess, "SELECT foo")
	require.NoError(err)

	p.AddTableProgress(ctx.Pid(), "a", 5)
	p.AddTableProgress(ctx.Pid(), "b", 6)

	ctx = sql.NewContext(context.Background(), sql.WithPid(2), sql.WithSession(sess))
	ctx, err = p.AddProcess(ctx, sql.CreateIndexProcess, "SELECT bar")
	require.NoError(err)

	p.AddTableProgress(ctx.Pid(), "foo", 2)

	p.UpdateTableProgress(1, "a", 3)
	p.UpdateTableProgress(1, "a", 1)
	p.UpdatePartitionProgress(1, "a", "a-1", 7)
	p.UpdatePartitionProgress(1, "a", "a-2", 9)
	p.UpdateTableProgress(1, "b", 2)
	p.UpdateTableProgress(2, "foo", 1)

	n.ProcessList = p
	n.Database = "foo"

	iter, err := n.RowIter(ctx, nil)
	require.NoError(err)
	rows, err := sql.RowIterToRows(iter)
	require.NoError(err)

	expected := []sql.Row{
		{int64(1), "foo", addr, "foo", "query", int64(0),
			`
a (4/5 partitions)
 ├─ a-1 (7/? rows)
 └─ a-2 (9/? rows)

b (2/6 partitions)
`, "SELECT foo"},
		{int64(1), "foo", addr, "foo", "create_index", int64(0), "\nfoo (1/2 partitions)\n", "SELECT bar"},
	}

	require.ElementsMatch(expected, rows)
}
