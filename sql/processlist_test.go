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
	sess := NewSession("0.0.0.0:3306", "127.0.0.1:34567", "foo", 1)
	ctx := NewContext(context.Background(), WithPid(1), WithSession(sess))
	ctx, err := p.AddProcess(ctx, QueryProcess, "SELECT foo")
	require.NoError(err)

	require.Equal(uint64(1), ctx.Pid())
	require.Len(p.procs, 1)

	p.AddProgressItem(ctx.Pid(), "a", 5)
	p.AddProgressItem(ctx.Pid(), "b", 6)

	expectedProcess := &Process{
		Pid:        1,
		Connection: 1,
		Type:       QueryProcess,
		Progress: map[string]Progress{
			"a": Progress{0, 5},
			"b": Progress{0, 6},
		},
		User:      "foo",
		Query:     "SELECT foo",
		StartedAt: p.procs[ctx.Pid()].StartedAt,
	}
	require.NotNil(p.procs[ctx.Pid()].Kill)
	p.procs[ctx.Pid()].Kill = nil
	require.Equal(expectedProcess, p.procs[ctx.Pid()])

	ctx = NewContext(context.Background(), WithPid(2), WithSession(sess))
	ctx, err = p.AddProcess(ctx, CreateIndexProcess, "SELECT bar")
	require.NoError(err)

	p.AddProgressItem(ctx.Pid(), "foo", 2)

	require.Equal(uint64(2), ctx.Pid())
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
		np := *p
		np.Kill = nil
		expected = append(expected, np)
	}

	result := p.Processes()
	for i := range result {
		result[i].Kill = nil
	}

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

func TestKillConnection(t *testing.T) {
	pl := NewProcessList()

	s1 := NewSession("", "", "", 1)
	s2 := NewSession("", "", "", 2)

	var killed = make(map[uint64]bool)
	for i := uint64(1); i <= 3; i++ {
		// Odds get s1, evens get s2
		s := s1
		if i%2 == 0 {
			s = s2
		}

		_, err := pl.AddProcess(
			NewContext(context.Background(), WithPid(i), WithSession(s)),
			QueryProcess,
			"foo",
		)
		require.NoError(t, err)

		i := i
		pl.procs[i].Kill = func() {
			killed[i] = true
		}
	}

	pl.Kill(1)
	require.Len(t, pl.procs, 1)

	// Odds should have been killed
	require.True(t, killed[1])
	require.False(t, killed[2])
	require.True(t, killed[3])
}
