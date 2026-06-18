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
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dolthub/vitess/go/mysql"
	"github.com/sirupsen/logrus"
)

const (
	// connWatchStartDelay is how long a query must run before it is watched for a
	// client disconnect. Queries that finish sooner never touch the socket.
	connWatchStartDelay = 10 * time.Millisecond
	// connWatchTick is how often the sweeper scans while it has pending work.
	// Larger means lower steady-state overhead and a coarser detection-start
	// granularity (effective delay is in [connWatchStartDelay,
	// connWatchStartDelay+connWatchTick]); detection latency on long queries is
	// immaterial. The sweeper does not tick at all on an idle server.
	connWatchTick = 50 * time.Millisecond
)

// connWatcher detects clients that disconnect (or send unexpected bytes) while a
// query is executing, and cancels the in-flight query so it does not run to
// completion for a client that will never read the result.
//
// Mechanism. A query that runs longer than |delay| is watched by an outstanding
// vitess Conn.WaitForClientActivity Peek on its socket (the protocol is
// half-duplex with no pipelining, so the socket is quiescent during a query;
// any readable event means the client is gone or misbehaving). Rather than arm a
// per-query timer to start that watch — which, at high QPS, reprograms the
// runtime netpoller on every query and burns CPU in runtime.futex — a single
// background sweeper goroutine periodically scans the live connections and starts
// a watch only on the ones whose query has outlived |delay|. The overwhelming
// majority of queries finish well under |delay| and never touch the socket, a
// goroutine, or the sweeper at all; their only per-query cost is two atomic
// stores (see connState.QueryStarted / QueryEnded).
//
// Quiescence. The sweeper does not poll on an idle server. It only ticks while
// there is at least one query that is running but not yet being watched
// ("pending"). Once every in-flight query has either finished or been promoted
// to an event-driven Peek watch, the sweeper parks on a channel with no timer
// armed, so a server with many active-but-idle connections produces zero
// periodic wakeups. A new query wakes the sweeper via |wakeCh| only when it is
// parked (see connState.QueryStarted's awake check).
type connWatcher struct {
	// delay is how long a query must run before it is watched. tick is how often
	// the sweeper scans while it has pending work.
	delay time.Duration
	tick  time.Duration

	// disabled makes the whole apparatus a no-op (Config.DisableConnectionWatcher).
	disabled bool

	// nowNanos returns the current time in nanoseconds; injectable for tests.
	nowNanos func() int64

	// startWatch spawns the actual watch for a promoted query. It must arrange
	// for close(done) when the watch ends, and call cancelQuery if the client
	// went away. Injectable so unit tests can drive it without real sockets.
	startWatch func(cs *connState, ctx context.Context, done chan struct{}, cancelQuery context.CancelCauseFunc)

	// conns is the sweeper's private registry of live connections. It is separate
	// from the SessionManager's map so that per-tick scans never contend on the
	// session lock; it holds the same *connState pointers. Maintained only at
	// connect/disconnect.
	mu    sync.RWMutex
	conns map[uint32]*connState

	// scanBuf is a reusable snapshot buffer, touched only by the sweeper
	// goroutine, so the scan can drop mu before installing watchers.
	scanBuf []*connState

	// awake is true while the sweeper is actively ticking, false while it is
	// parked. Written only by the sweeper; read by QueryStarted on the hot path,
	// so on a busy server it stays a shared-clean cache line (just a load, no
	// cross-core bouncing, no global counter).
	awake atomic.Bool

	wakeCh  chan struct{} // buffered(1): pokes a parked sweeper
	closeCh chan struct{} // closed by Close to stop the sweeper
	doneCh  chan struct{} // closed when the sweeper goroutine has exited
}

// connState is the per-connection disconnect-watch state. The connection's
// single handler goroutine drives QueryStarted/QueryEnded; the sweeper goroutine
// drives maybePromote. They coordinate through slot (a lock-free CAS handshake)
// and mu (which publishes the watch handle).
//
// slot encodes the query generation and phase:
//
//	slot == 0          idle: no query running
//	slot == g<<1       running: query generation g (g >= 1), no watcher installed
//	slot == g<<1 | 1   watching: a watcher is installed for generation g
//
// At query end the handler CAS's g<<1 -> 0. Success means it won the race and no
// watcher exists (the lock-free fast path). Failure means the sweeper installed
// a watcher (slot is g<<1|1); the handler then tears it down under mu. The CAS
// failure *is* the signal that there is a watcher to join.
type connState struct {
	w *connWatcher

	slot atomic.Uint64

	// curGen is the generation of the current query. Read and written only by the
	// connection's handler goroutine (QueryStarted/QueryEnded run serially on it),
	// so it needs no synchronization.
	curGen uint64

	// startNanos and cancelQuery are published by QueryStarted before it stores
	// slot, and read by the sweeper after it observes the running slot; the slot
	// atomic provides the happens-before, so plain atomics are race-free here.
	startNanos  atomic.Int64
	cancelQuery atomic.Pointer[cancelCause]

	conn   *mysql.Conn
	logger *logrus.Entry

	// done/stop are published by the sweeper under mu when it installs a watcher,
	// and read by QueryEnded/teardown. dead is set by Unregister so a sweeper that
	// still holds a stale pointer does not install a watcher on a removed conn.
	mu   sync.Mutex
	done chan struct{}
	stop context.CancelFunc
	dead bool
}

// cancelCause boxes a context.CancelCauseFunc so it can live in an atomic.Pointer.
type cancelCause struct{ fn context.CancelCauseFunc }

// newConnWatcher builds a connWatcher and, unless disabled, starts its sweeper.
func newConnWatcher(delay, tick time.Duration, disabled bool) *connWatcher {
	w := &connWatcher{
		delay:    delay,
		tick:     tick,
		disabled: disabled,
		conns:    make(map[uint32]*connState),
		wakeCh:   make(chan struct{}, 1),
		closeCh:  make(chan struct{}),
		doneCh:   make(chan struct{}),
	}
	w.nowNanos = func() int64 { return time.Now().UnixNano() }
	w.startWatch = defaultStartWatch
	if disabled {
		close(w.doneCh)
		return w
	}
	w.start()
	return w
}

// start launches the sweeper. Separated from newConnWatcher so tests can build a
// connWatcher with injected fields and start it (or not) explicitly.
func (w *connWatcher) start() {
	w.awake.Store(true)
	go w.run()
}

// Close stops the sweeper and waits for it to exit. Idempotent inputs (nil /
// disabled) are no-ops.
func (w *connWatcher) Close() {
	if w == nil || w.disabled {
		return
	}
	close(w.closeCh)
	<-w.doneCh
}

// Register starts tracking a connection and returns its watch handle, which the
// caller stashes for QueryStarted/QueryEnded. Returns nil when disabled.
func (w *connWatcher) Register(c *mysql.Conn, logger *logrus.Entry) *connState {
	if w == nil || w.disabled {
		return nil
	}
	cs := &connState{w: w, conn: c, logger: logger}
	w.mu.Lock()
	w.conns[c.ConnectionID] = cs
	w.mu.Unlock()
	return cs
}

// Unregister stops tracking a connection and tears down any watcher still
// installed on it. Safe to call once per connection at close.
func (w *connWatcher) Unregister(c *mysql.Conn) {
	if w == nil || w.disabled {
		return
	}
	w.mu.Lock()
	cs := w.conns[c.ConnectionID]
	delete(w.conns, c.ConnectionID)
	w.mu.Unlock()
	if cs != nil {
		cs.markDeadAndTeardown()
	}
}

// wake pokes the sweeper if it is parked. Non-blocking: a full buffer already
// means a wake is pending.
func (w *connWatcher) wake() {
	select {
	case w.wakeCh <- struct{}{}:
	default:
	}
}

// run is the sweeper loop. It ticks (one reused timer for the whole process)
// only while there is pending work, and parks on wakeCh with no timer otherwise.
func (w *connWatcher) run() {
	defer close(w.doneCh)
	timer := time.NewTimer(w.tick)
	if !timer.Stop() {
		<-timer.C
	}
	for {
		if w.scanOnce(w.nowNanos()) {
			// Pending work remains: wait one tick, or rescan early if a new query
			// arrives, or exit.
			timer.Reset(w.tick)
			select {
			case <-timer.C:
			case <-w.wakeCh:
				if !timer.Stop() {
					<-timer.C
				}
			case <-w.closeCh:
				if !timer.Stop() {
					<-timer.C
				}
				return
			}
			continue
		}

		// Nothing pending: become a candidate for dormancy. Declare not-awake,
		// then rescan once to catch a query that registered during the window
		// (whose QueryStarted, seeing awake==false, will have sent wakeCh).
		w.awake.Store(false)
		if w.scanOnce(w.nowNanos()) {
			w.awake.Store(true)
			continue
		}
		select {
		case <-w.wakeCh:
			w.awake.Store(true)
		case <-w.closeCh:
			return
		}
	}
}

// scanOnce scans every live connection, promoting any whose query has outlived
// delay to a watched state. It returns whether any connection is still pending
// (running but not yet watched), which is the sweeper's signal to keep ticking.
func (w *connWatcher) scanOnce(nowNanos int64) (pending bool) {
	delayNanos := int64(w.delay)

	// Snapshot the pointers under RLock, then release it before promoting, since
	// promotion takes cs.mu and may spawn a goroutine.
	w.mu.RLock()
	w.scanBuf = w.scanBuf[:0]
	for _, cs := range w.conns {
		w.scanBuf = append(w.scanBuf, cs)
	}
	w.mu.RUnlock()

	for _, cs := range w.scanBuf {
		if cs.maybePromote(nowNanos, delayNanos) {
			pending = true
		}
		w.scanBuf[len(w.scanBuf)-1] = nil // help GC; not strictly required
	}
	w.scanBuf = w.scanBuf[:0]
	return pending
}

// QueryStarted notifies that a query has begun on this connection. Lock-free hot
// path: publish the query's cancel and start time, mark the slot running, and
// wake the sweeper only if it is parked.
func (cs *connState) QueryStarted(cancelQuery context.CancelCauseFunc) {
	if cs == nil { // watcher disabled
		return
	}
	cs.curGen++
	g := cs.curGen
	cs.startNanos.Store(cs.w.nowNanos())
	cs.cancelQuery.Store(&cancelCause{fn: cancelQuery})
	cs.slot.Store(g << 1) // publish "running gen g" (release)
	if !cs.w.awake.Load() {
		cs.w.wake()
	}
}

// QueryEnded notifies that the current query has finished. Lock-free fast path:
// CAS running -> idle. If that fails, the sweeper installed a watcher and we take
// the slow path to stop and join it.
func (cs *connState) QueryEnded() {
	if cs == nil {
		return
	}
	g := cs.curGen
	if cs.slot.CompareAndSwap(g<<1, 0) {
		return // won the race: no watcher was installed
	}
	cs.teardown()
}

// maybePromote is the sweeper's per-connection step. It returns whether the
// connection is pending (running but not yet watched) after this step.
func (cs *connState) maybePromote(nowNanos, delayNanos int64) (pending bool) {
	s := cs.slot.Load()
	if s == 0 || s&1 == 1 {
		return false // idle, or already watching: not pending
	}
	if nowNanos-cs.startNanos.Load() < delayNanos {
		return true // running but too young to watch yet: keep ticking
	}

	cs.mu.Lock()
	if cs.dead {
		cs.mu.Unlock()
		return false
	}
	if !cs.slot.CompareAndSwap(s, s|1) {
		// The query ended (slot -> 0) between our load and now. Not pending.
		cs.mu.Unlock()
		return false
	}
	// We own the install. Publish the watch handle under mu so a concurrent
	// QueryEnded that observes the failed CAS reads a fully-populated handle.
	watchCtx, stop := context.WithCancel(context.Background())
	done := make(chan struct{})
	cs.done = done
	cs.stop = stop
	cc := cs.cancelQuery.Load()
	cs.mu.Unlock()

	var cancelQuery context.CancelCauseFunc
	if cc != nil {
		cancelQuery = cc.fn
	}
	cs.w.startWatch(cs, watchCtx, done, cancelQuery)
	return false // now watching: no longer pending
}

// teardown stops and joins a watcher the sweeper installed, then returns the slot
// to idle. Safe if no watcher is present (done/stop nil).
func (cs *connState) teardown() {
	cs.mu.Lock()
	done := cs.done
	stop := cs.stop
	cs.done = nil
	cs.stop = nil
	cs.mu.Unlock()

	if stop != nil {
		stop()
	}
	if done != nil {
		<-done
	}
	cs.slot.Store(0)
}

// markDeadAndTeardown is teardown for Unregister: it first marks the state dead
// (under mu, ordered against maybePromote's install) so a sweeper holding a stale
// pointer cannot install a new watcher after we have torn down.
func (cs *connState) markDeadAndTeardown() {
	cs.mu.Lock()
	cs.dead = true
	done := cs.done
	stop := cs.stop
	cs.done = nil
	cs.stop = nil
	cs.mu.Unlock()

	if stop != nil {
		stop()
	}
	if done != nil {
		<-done
	}
	cs.slot.Store(0)
}

// defaultStartWatch is the production watch: a goroutine blocked in vitess's
// event-driven WaitForClientActivity Peek. It maps any non-nil return (client
// disconnected or wrote unexpectedly) to cancelling the in-flight query.
func defaultStartWatch(cs *connState, ctx context.Context, done chan struct{}, cancelQuery context.CancelCauseFunc) {
	go func() {
		defer close(done)
		defer func() {
			if r := recover(); r != nil && cs.logger != nil {
				cs.logger.Errorf("panic recovered in connection watcher: %v\n%s", r, debug.Stack())
			}
		}()
		if err := cs.conn.WaitForClientActivity(ctx); err != nil {
			if cs.logger != nil {
				cs.logger.WithError(err).Warn("client connection went away while a query was executing")
			}
			if cancelQuery != nil {
				cancelQuery(ErrConnectionWasClosed.New())
			}
		}
	}()
}
