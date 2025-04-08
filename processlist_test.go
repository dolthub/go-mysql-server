// Copyright 2021 Dolthub, Inc.
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

package sqle

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/variables"
)

func TestProcessList(t *testing.T) {
	require := require.New(t)
	variables.InitStatusVariables()

	clientHostOne := "127.0.0.1:34567"
	clientHostTwo := "127.0.0.1:34568"
	p := NewProcessList()
	p.AddConnection(1, clientHostOne)
	sess := sql.NewBaseSessionWithClientServer("0.0.0.0:3306", sql.Client{Address: clientHostOne, User: "foo"}, 1)
	sess.SetCurrentDatabase("test_db")
	p.ConnectionReady(sess)
	ctx := sql.NewContext(context.Background(), sql.WithPid(1), sql.WithSession(sess))
	ctx, err := p.BeginQuery(ctx, "SELECT foo")
	require.NoError(err)

	require.Equal(uint64(1), ctx.Pid())
	require.Len(p.procs, 1)

	p.AddTableProgress(ctx.Pid(), "a", 5)
	p.AddTableProgress(ctx.Pid(), "b", 6)

	expectedProcess := &sql.Process{
		QueryPid:   1,
		Connection: 1,
		Host:       clientHostOne,
		Progress: map[string]sql.TableProgress{
			"a": {sql.Progress{Name: "a", Done: 0, Total: 5}, map[string]sql.PartitionProgress{}},
			"b": {sql.Progress{Name: "b", Done: 0, Total: 6}, map[string]sql.PartitionProgress{}},
		},
		User:      "foo",
		Query:     "SELECT foo",
		Command:   sql.ProcessCommandQuery,
		StartedAt: p.procs[1].StartedAt,
		Database:  "test_db",
	}
	require.NotNil(p.procs[1].Kill)
	p.procs[1].Kill = nil
	require.Equal(expectedProcess, p.procs[1])

	p.AddPartitionProgress(ctx.Pid(), "b", "b-1", -1)
	p.AddPartitionProgress(ctx.Pid(), "b", "b-2", -1)
	p.AddPartitionProgress(ctx.Pid(), "b", "b-3", -1)

	p.UpdatePartitionProgress(ctx.Pid(), "b", "b-2", 1)

	p.RemovePartitionProgress(ctx.Pid(), "b", "b-3")

	expectedProgress := map[string]sql.TableProgress{
		"a": {sql.Progress{Name: "a", Total: 5}, map[string]sql.PartitionProgress{}},
		"b": {sql.Progress{Name: "b", Total: 6}, map[string]sql.PartitionProgress{
			"b-1": {sql.Progress{Name: "b-1", Done: 0, Total: -1}},
			"b-2": {sql.Progress{Name: "b-2", Done: 1, Total: -1}},
		}},
	}
	require.Equal(expectedProgress, p.procs[1].Progress)

	p.AddConnection(2, clientHostTwo)
	sess = sql.NewBaseSessionWithClientServer("0.0.0.0:3306", sql.Client{Address: clientHostTwo, User: "foo"}, 2)
	p.ConnectionReady(sess)
	ctx = sql.NewContext(context.Background(), sql.WithPid(2), sql.WithSession(sess))
	ctx, err = p.BeginQuery(ctx, "SELECT bar")
	require.NoError(err)

	p.AddTableProgress(ctx.Pid(), "foo", 2)

	require.Equal(uint64(2), ctx.Pid())
	require.Len(p.procs, 2)

	p.UpdateTableProgress(1, "a", 3)
	p.UpdateTableProgress(1, "a", 1)
	p.UpdateTableProgress(1, "b", 2)
	p.UpdateTableProgress(2, "foo", 1)

	require.Equal(int64(4), p.procs[1].Progress["a"].Done)
	require.Equal(int64(2), p.procs[1].Progress["b"].Done)
	require.Equal(int64(1), p.procs[2].Progress["foo"].Done)

	var expected []sql.Process
	for _, p := range p.procs {
		np := *p
		np.Kill = nil
		expected = append(expected, np)
	}

	result := p.Processes()
	for i := range result {
		result[i].Kill = nil
	}

	sortById(expected)
	sortById(result)
	require.Equal(expected, result)

	p.EndQuery(ctx)

	require.Len(p.procs, 2)
	proc, ok := p.procs[2]
	require.True(ok)
	require.Equal(sql.ProcessCommandSleep, proc.Command)
}

func sortById(slice []sql.Process) {
	sort.Slice(slice, func(i, j int) bool {
		return slice[i].Connection < slice[j].Connection
	})
}

func TestKillConnection(t *testing.T) {
	pl := NewProcessList()

	pl.AddConnection(1, "")
	pl.AddConnection(2, "")
	s1 := sql.NewBaseSessionWithClientServer("", sql.Client{}, 1)
	s2 := sql.NewBaseSessionWithClientServer("", sql.Client{}, 2)
	pl.ConnectionReady(s1)
	pl.ConnectionReady(s2)

	_, err := pl.BeginQuery(
		sql.NewContext(context.Background(), sql.WithPid(3), sql.WithSession(s1)),
		"foo",
	)
	require.NoError(t, err)

	_, err = pl.BeginQuery(
		sql.NewContext(context.Background(), sql.WithPid(4), sql.WithSession(s2)),
		"foo",
	)
	require.NoError(t, err)

	var killed = make(map[uint64]bool)

	pl.procs[1].Kill = func() {
		killed[1] = true
	}
	pl.procs[2].Kill = func() {
		killed[2] = true
	}

	pl.Kill(1)
	require.Len(t, pl.procs, 2)

	require.True(t, killed[1])
	require.False(t, killed[2])
}

func TestBeginEndOperation(t *testing.T) {
	knownSession := sql.NewBaseSessionWithClientServer("", sql.Client{}, 1)
	unknownSession := sql.NewBaseSessionWithClientServer("", sql.Client{}, 2)

	pl := NewProcessList()
	pl.AddConnection(1, "")

	// Begining an operation with an unknown connection returns an error.
	ctx := sql.NewContext(context.Background(), sql.WithSession(unknownSession))
	_, err := pl.BeginOperation(ctx)
	require.Error(t, err)

	// Can begin and end operation before connection is ready.
	ctx = sql.NewContext(context.Background(), sql.WithSession(knownSession))
	subCtx, err := pl.BeginOperation(ctx)
	require.NoError(t, err)
	pl.EndOperation(subCtx)

	// Can begin and end operation across the connection ready boundary.
	subCtx, err = pl.BeginOperation(ctx)
	require.NoError(t, err)
	pl.ConnectionReady(knownSession)
	pl.EndOperation(subCtx)

	// Ending the operation cancels the subcontext.
	subCtx, err = pl.BeginOperation(ctx)
	require.NoError(t, err)
	done := make(chan struct{})
	context.AfterFunc(subCtx, func() {
		close(done)
	})
	pl.EndOperation(subCtx)
	<-done

	// Kill on the connection cancels the subcontext.
	subCtx, err = pl.BeginOperation(ctx)
	require.NoError(t, err)
	done = make(chan struct{})
	context.AfterFunc(subCtx, func() {
		close(done)
	})
	pl.Kill(1)
	<-done
	pl.EndOperation(subCtx)

	// Beginning an operation while one is outstanding errors.
	subCtx, err = pl.BeginOperation(ctx)
	require.NoError(t, err)
	_, err = pl.BeginOperation(ctx)
	require.Error(t, err)
	pl.EndOperation(subCtx)
}

// TestSlowQueryTracking tests that processes that take longer than @@long_query_time increment the
// Slow_queries status variable.
func TestSlowQueryTracking(t *testing.T) {
	_, value, ok := sql.StatusVariables.GetGlobal("Slow_queries")
	require.True(t, ok)
	require.Equal(t, uint64(0), value)

	p := NewProcessList()
	p.AddConnection(1, "127.0.0.1:34567")
	sess := sql.NewBaseSessionWithClientServer("0.0.0.0:3306",
		sql.Client{Address: "127.0.0.1:34567", User: "foo"}, 1)
	sess.SetCurrentDatabase("test_db")
	p.ConnectionReady(sess)
	ctx := sql.NewContext(context.Background(), sql.WithPid(1), sql.WithSession(sess))
	ctx, err := p.BeginQuery(ctx, "SELECT foo")
	require.NoError(t, err)

	// Change @@long_query_time so we don't have to wait for 10 seconds
	require.NoError(t, sql.SystemVariables.SetGlobal(ctx, "long_query_time", 1))
	time.Sleep(1_500 * time.Millisecond)
	p.EndQuery(ctx)

	// Status variables are updated asynchronously, so try a few times to find the updated value
	found := false
	for range 10 {
		_, value, ok = sql.StatusVariables.GetGlobal("Slow_queries")
		require.True(t, ok)
		if value == uint64(1) {
			found = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	require.True(t, found, "Never found Slow_queries value updated")
}
