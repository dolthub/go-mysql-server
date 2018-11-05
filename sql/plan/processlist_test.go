package plan

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
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

	p.AddProgressItem(ctx.Pid(), "a", 5)
	p.AddProgressItem(ctx.Pid(), "b", 6)

	ctx = sql.NewContext(context.Background(), sql.WithPid(2), sql.WithSession(sess))
	ctx, err = p.AddProcess(ctx, sql.CreateIndexProcess, "SELECT bar")
	require.NoError(err)

	p.AddProgressItem(ctx.Pid(), "foo", 2)

	p.UpdateProgress(1, "a", 3)
	p.UpdateProgress(1, "a", 1)
	p.UpdateProgress(1, "b", 2)
	p.UpdateProgress(2, "foo", 1)

	n.ProcessList = p
	n.Database = "foo"

	iter, err := n.RowIter(ctx)
	require.NoError(err)
	rows, err := sql.RowIterToRows(iter)
	require.NoError(err)

	expected := []sql.Row{
		{int64(1), "foo", addr, "foo", "query", int64(0), "a(4/5), b(2/6)", "SELECT foo"},
		{int64(2), "foo", addr, "foo", "create_index", int64(0), "foo(1/2)", "SELECT bar"},
	}

	require.ElementsMatch(expected, rows)
}
