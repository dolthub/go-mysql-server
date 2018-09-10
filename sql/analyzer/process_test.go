package analyzer

import (
	"testing"

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
		plan.NewResolvedTable(mem.NewPartitionedTable("foo", nil, 2)),
		&tableNodeAdapter{"bar", mem.NewPartitionedTable("bar", nil, 4)},
		expression.NewLiteral(int64(1), sql.Int64),
	)

	ctx := sql.NewEmptyContext().WithQuery("SELECT foo")

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

	proc, ok := result.(*queryProcess)
	require.True(ok)

	join, ok := proc.Node.(*plan.InnerJoin)
	require.True(ok)

	lhs, ok := join.Left.(*plan.ResolvedTable)
	require.True(ok)
	_, ok = lhs.Table.(*processTable)
	require.True(ok)

	rhs, ok := join.Right.(*plan.ResolvedTable)
	require.True(ok)
	_, ok = rhs.Table.(*processTable)
	require.True(ok)

	iter, err := proc.RowIter(ctx)
	require.NoError(err)
	_, err = sql.RowIterToRows(iter)
	require.NoError(err)

	require.Len(catalog.Processes(), 0)
}

type tableNodeAdapter struct {
	name string
	*mem.Table
}

func (t *tableNodeAdapter) Name() string      { return t.name }
func (tableNodeAdapter) Children() []sql.Node { return nil }
func (tableNodeAdapter) Resolved() bool       { return true }
func (tableNodeAdapter) RowIter(*sql.Context) (sql.RowIter, error) {
	panic("RowIter of tableNodeAdapter is a placeholder")
}
func (t *tableNodeAdapter) TransformUp(f sql.TransformNodeFunc) (sql.Node, error) {
	return f(t)
}
func (t *tableNodeAdapter) TransformExpressionsUp(f sql.TransformExprFunc) (sql.Node, error) {
	return t, nil
}

func removeProcessNodes(t *testing.T, n sql.Node) sql.Node {
	n, err := n.TransformUp(func(n sql.Node) (sql.Node, error) {
		switch n := n.(type) {
		case *plan.ResolvedTable:
			if pt, ok := n.Table.(*processTable); ok {
				return plan.NewResolvedTable(pt.Table), nil
			}
		case *plan.SubqueryAlias:
			nc := *n
			nc.Child = removeProcessNodes(t, n.Child)
			return &nc, nil
		}
		return n, nil
	})
	require.NoError(t, err)

	if p, ok := n.(*queryProcess); ok {
		return p.Node
	}

	return n
}
