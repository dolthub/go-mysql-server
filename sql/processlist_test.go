package sql

import (
	"context"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProcessList(t *testing.T) {
	require := require.New(t)

	p := NewProcessList()
	sess := NewSession("0.0.0.0:1234", "foo")
	ctx := NewContext(context.Background(), WithSession(sess))
	pid := p.AddProcess(ctx, QueryProcess, "SELECT foo")

	require.Equal(uint64(1), pid)
	require.Len(p.procs, 1)

	p.AddProgressItem(pid, "a", 5)
	p.AddProgressItem(pid, "b", 6)

	expectedProcess := &Process{
		Pid:  1,
		Type: QueryProcess,
		Progress: map[string]Progress{
			"a": Progress{0, 5},
			"b": Progress{0, 6},
		},
		User:      "foo",
		Query:     "SELECT foo",
		StartedAt: p.procs[pid].StartedAt,
	}
	require.Equal(expectedProcess, p.procs[pid])

	pid = p.AddProcess(ctx, CreateIndexProcess, "SELECT bar")
	p.AddProgressItem(pid, "foo", 2)

	require.Equal(uint64(2), pid)
	require.Len(p.procs, 2)

	p.UpdateProgress(1, "a", 3)
	p.UpdateProgress(1, "a", 1)
	p.UpdateProgress(1, "b", 2)
	p.UpdateProgress(2, "foo", 1)

	require.Equal(int64(4), p.procs[1].Progress["a"].Done)
	require.Equal(int64(2), p.procs[1].Progress["b"].Done)
	require.Equal(int64(1), p.procs[2].Progress["foo"].Done)

	var expected []Process
	for _, p := range p.procs {
		expected = append(expected, *p)
	}

	result := p.Processes()

	sortByPid(expected)
	sortByPid(result)
	require.Equal(expected, result)

	p.Done(2)

	require.Len(p.procs, 1)
	_, ok := p.procs[1]
	require.True(ok)
}

func sortByPid(slice []Process) {
	sort.Slice(slice, func(i, j int) bool {
		return slice[i].Pid < slice[j].Pid
	})
}
