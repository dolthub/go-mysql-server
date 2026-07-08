// Copyright 2026 Dolthub, Inc.
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

package server

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dolthub/vitess/go/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeWatcher is an injectable connWatcher.startWatch that records installed
// watches and lets a test drive their lifecycle without a real socket. It mirrors
// the production watch: the goroutine ends (closing done) when ctx is cancelled
// (teardown) or when the test simulates a client disconnect (firing cancelQuery).
type fakeWatcher struct {
	mu      sync.Mutex
	watches []*fakeWatch
}

type fakeWatch struct {
	done       chan struct{}
	disconnect chan struct{}
}

func (f *fakeWatcher) start(cs *connState, ctx context.Context, done chan struct{}, cancelQuery context.CancelCauseFunc) {
	fw := &fakeWatch{done: done, disconnect: make(chan struct{})}
	f.mu.Lock()
	f.watches = append(f.watches, fw)
	f.mu.Unlock()
	go func() {
		defer close(done)
		select {
		case <-ctx.Done():
		case <-fw.disconnect:
			if cancelQuery != nil {
				cancelQuery(ErrConnectionWasClosed.New())
			}
		}
	}()
}

func (f *fakeWatcher) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.watches)
}

func (f *fakeWatcher) last() *fakeWatch {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.watches[len(f.watches)-1]
}

func (fw *fakeWatch) triggerDisconnect() { close(fw.disconnect) }

// manualWatcher builds a connWatcher with a controllable clock and the fake
// startWatch, and does NOT start the sweeper goroutine (logic tests call
// scanOnce directly for determinism).
func manualWatcher(t *testing.T, delay time.Duration) (*connWatcher, *fakeWatcher, *atomic.Int64) {
	t.Helper()
	var now atomic.Int64
	fw := &fakeWatcher{}
	w := &connWatcher{
		delay:      delay,
		tick:       time.Millisecond,
		conns:      make(map[uint32]*connState),
		wakeCh:     make(chan struct{}, 1),
		closeCh:    make(chan struct{}),
		doneCh:     make(chan struct{}),
		nowNanos:   func() int64 { return now.Load() },
		startWatch: fw.start,
	}
	return w, fw, &now
}

func registerQuery(t *testing.T, w *connWatcher, id uint32) (*connState, context.Context, context.CancelCauseFunc) {
	t.Helper()
	cs := w.Register(&mysql.Conn{ConnectionID: id}, nil)
	require.NotNil(t, cs)
	qctx, cancel := context.WithCancelCause(context.Background())
	cs.QueryStarted(cancel)
	return cs, qctx, cancel
}

// A fast query that finishes before the delay must never be watched.
func TestConnWatcherFastPath(t *testing.T) {
	w, fw, now := manualWatcher(t, 10*time.Millisecond)
	cs, _, _ := registerQuery(t, w, 1) // startNanos = 0

	cs.QueryEnded() // ends immediately
	assert.Equal(t, uint64(0), cs.slot.Load())

	now.Store(int64(50 * time.Millisecond)) // sweeper runs much later
	assert.False(t, w.scanOnce(w.nowNanos()))
	assert.Equal(t, 0, fw.count(), "no watch should ever be installed for a fast query")
}

// A query that outlives the delay is promoted to a watch, and QueryEnded then
// tears it down (the CAS-failure slow path).
func TestConnWatcherPromoteThenEnd(t *testing.T) {
	w, fw, now := manualWatcher(t, 10*time.Millisecond)
	cs, _, _ := registerQuery(t, w, 1)

	now.Store(int64(20 * time.Millisecond)) // past the delay
	assert.False(t, w.scanOnce(w.nowNanos()), "a watched query is no longer pending")
	assert.Equal(t, 1, fw.count())
	assert.Equal(t, uint64(1<<1|1), cs.slot.Load(), "slot should be watching gen 1")

	cs.QueryEnded() // CAS(2,0) fails -> teardown stops+joins the watch
	assert.Equal(t, uint64(0), cs.slot.Load())
}

// While a query is running but younger than the delay, the sweeper reports it as
// pending so it keeps ticking; once promoted, pending goes false (quiescence).
func TestConnWatcherPendingThenQuiesces(t *testing.T) {
	w, _, now := manualWatcher(t, 10*time.Millisecond)
	registerQuery(t, w, 1)

	now.Store(int64(3 * time.Millisecond))
	assert.True(t, w.scanOnce(w.nowNanos()), "young running query is pending")

	now.Store(int64(20 * time.Millisecond))
	assert.False(t, w.scanOnce(w.nowNanos()), "promoted query is no longer pending")
}

// If the client disconnects while watched, the watch cancels the in-flight query
// with ErrConnectionWasClosed.
func TestConnWatcherDisconnectCancelsQuery(t *testing.T) {
	w, fw, now := manualWatcher(t, 10*time.Millisecond)
	cs, qctx, _ := registerQuery(t, w, 1)

	now.Store(int64(20 * time.Millisecond))
	require.False(t, w.scanOnce(w.nowNanos()))
	require.Equal(t, 1, fw.count())

	fw.last().triggerDisconnect()

	select {
	case <-qctx.Done():
	case <-time.After(time.Second):
		t.Fatal("query context was not cancelled on disconnect")
	}
	assert.True(t, ErrConnectionWasClosed.Is(context.Cause(qctx)))

	cs.QueryEnded() // teardown after the watch already exited; must not hang
	assert.Equal(t, uint64(0), cs.slot.Load())
}

// QueryEnded winning the race (ending before the sweeper's scan) leaves no watch.
func TestConnWatcherEndBeforeSweep(t *testing.T) {
	w, fw, now := manualWatcher(t, 10*time.Millisecond)
	cs, _, _ := registerQuery(t, w, 1)

	now.Store(int64(20 * time.Millisecond))
	cs.QueryEnded() // wins: slot -> 0 before the scan
	assert.False(t, w.scanOnce(w.nowNanos()))
	assert.Equal(t, 0, fw.count())
}

// Unregister tears down a live watch and prevents a stale sweeper pointer from
// installing a new one (the dead flag).
func TestConnWatcherUnregisterTeardown(t *testing.T) {
	w, fw, now := manualWatcher(t, 10*time.Millisecond)
	cs, _, _ := registerQuery(t, w, 1)

	now.Store(int64(20 * time.Millisecond))
	require.False(t, w.scanOnce(w.nowNanos()))
	require.Equal(t, 1, fw.count())

	w.Unregister(&mysql.Conn{ConnectionID: 1})
	assert.True(t, cs.dead)
	assert.Equal(t, uint64(0), cs.slot.Load())

	// A stale scan over the removed state must not install another watch.
	cs.slot.Store(1 << 1) // pretend it still looks "running"
	cs.maybePromote(w.nowNanos(), int64(w.delay))
	assert.Equal(t, 1, fw.count(), "dead state must not be re-watched")
}

// A single scan over multiple connections must promote each one independently.
func TestConnWatcherScanMultiple(t *testing.T) {
	w, fw, now := manualWatcher(t, 10*time.Millisecond)
	const n = 5
	conns := make([]*connState, n)
	for i := 0; i < n; i++ {
		conns[i], _, _ = registerQuery(t, w, uint32(i+1))
	}

	now.Store(int64(20 * time.Millisecond)) // all past the delay
	assert.False(t, w.scanOnce(w.nowNanos()))
	assert.Equal(t, n, fw.count(), "every long-running connection should be watched")
	for i, cs := range conns {
		assert.Equalf(t, uint64(1<<1|1), cs.slot.Load(), "conn %d should be watching", i+1)
	}
}

// End-to-end with the real sweeper goroutine (real time, fake watch): the sweeper
// parks when idle, wakes on a new query, promotes after the delay, then parks
// again because the watch is event-driven.
func TestConnWatcherSweeperQuiescence(t *testing.T) {
	fw := &fakeWatcher{}
	w := &connWatcher{
		delay:      20 * time.Millisecond,
		tick:       2 * time.Millisecond,
		conns:      make(map[uint32]*connState),
		wakeCh:     make(chan struct{}, 1),
		closeCh:    make(chan struct{}),
		doneCh:     make(chan struct{}),
		nowNanos:   func() int64 { return time.Now().UnixNano() },
		startWatch: fw.start,
	}
	w.start()
	defer w.Close()

	// Idle: the sweeper should park.
	assert.Eventually(t, func() bool { return !w.awake.Load() }, time.Second, time.Millisecond,
		"sweeper should quiesce with no work")

	cs := w.Register(&mysql.Conn{ConnectionID: 1}, nil)
	qctx, cancel := context.WithCancelCause(context.Background())
	cs.QueryStarted(cancel)

	// A new query wakes the parked sweeper...
	assert.Eventually(t, func() bool { return w.awake.Load() }, time.Second, time.Millisecond,
		"a new query should wake the sweeper")
	// ...which promotes it once it outlives the delay...
	assert.Eventually(t, func() bool { return fw.count() == 1 }, time.Second, time.Millisecond,
		"the long-running query should be watched")
	// ...and then parks again, since the watch is event-driven, not polled.
	assert.Eventually(t, func() bool { return !w.awake.Load() }, time.Second, time.Millisecond,
		"sweeper should re-quiesce after promotion")

	w.Unregister(&mysql.Conn{ConnectionID: 1})
	_ = qctx
}
