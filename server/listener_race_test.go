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
	"net"
	"runtime"
	"sync"
	"testing"
	"time"
)

// TestListenerCloseNoRace exercises the race between the accept loops
// sending into l.conns and Listener.Close closing that channel. Before the
// fix, Close closed l.conns while the loops could still be selected to send
// into it (Go's select chooses randomly among ready cases), which panicked
// or tripped the race detector under -race. Run with:
//
//	go test -race -run TestListenerCloseNoRace -count=100 ./server/
func TestListenerCloseNoRace(t *testing.T) {
	const iterations = 50
	for i := 0; i < iterations; i++ {
		l, err := NewListener("tcp", "127.0.0.1:0", "")
		if err != nil {
			t.Fatalf("iter %d: NewListener: %v", i, err)
		}

		// Deliberately do NOT drain Accept() until after Close. A blocked
		// consumer means the accept loop's `l.conns <- connRes{...}` send
		// stalls inside the select, maximising the window in which Close
		// (on the buggy impl) closes l.conns underneath a live sender.
		releaseConsumer := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-releaseConsumer
			for {
				c, aerr := l.Accept()
				if aerr != nil {
					return
				}
				_ = c.Close()
			}
		}()

		// Establish several real connections so the accept loop accepts them
		// and reaches the stalled `conns <-` send.
		addr := l.Addr().String()
		var dialWG sync.WaitGroup
		for j := 0; j < 8; j++ {
			dialWG.Add(1)
			go func() {
				defer dialWG.Done()
				if c, derr := net.Dial("tcp", addr); derr == nil {
					_ = c.Close()
				}
			}()
		}
		// Let the kernel complete the dials and the loop accept them so the
		// send is truly in flight (blocked on the undrained channel).
		time.Sleep(5 * time.Millisecond)

		// Close concurrently with the stalled sends. The fixed Close waits
		// for the loop to exit before closing conns, so no send hits a closed
		// channel. Release the consumer afterward so the drainer exits.
		if cerr := l.Close(); cerr != nil {
			t.Fatalf("iter %d: Close: %v", i, cerr)
		}
		close(releaseConsumer)
		dialWG.Wait()
		wg.Wait()
	}
	runtime.GC() // encourage any leaked goroutines to surface
}

// TestListenerCloseUnixToo extends the above to the unix-socket accept loop,
// which shares the same conns channel and thus the same race surface.
func TestListenerCloseUnixToo(t *testing.T) {
	dir := t.TempDir()
	sock := dir + "/mysql.sock"
	l, err := NewListener("tcp", "127.0.0.1:0", sock)
	if err != nil {
		t.Fatalf("NewListener: %v", err)
	}

	releaseConsumer := make(chan struct{})
	done := make(chan struct{})
	go func() {
		<-releaseConsumer
		for {
			c, aerr := l.Accept()
			if aerr != nil {
				close(done)
				return
			}
			_ = c.Close()
		}
	}()

	// Dial the unix socket repeatedly while closing so its accept loop also
	// stalls on a send into conns.
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if c, derr := net.Dial("unix", sock); derr == nil {
				_ = c.Close()
			}
		}()
	}
	time.Sleep(5 * time.Millisecond)

	if cerr := l.Close(); cerr != nil {
		t.Fatalf("Close: %v", cerr)
	}
	close(releaseConsumer)
	wg.Wait()
	<-done
}

// TestListenerAcceptReturnsErrClosedAfterClose confirms the observable
// contract survives the reordered Close: Accept must return net.ErrClosed
// (not hang) once Close has completed.
func TestListenerAcceptReturnsErrClosedAfterClose(t *testing.T) {
	l, err := NewListener("tcp", "127.0.0.1:0", "")
	if err != nil {
		t.Fatalf("NewListener: %v", err)
	}
	if cerr := l.Close(); cerr != nil {
		t.Fatalf("Close: %v", cerr)
	}
	// A fresh Accept after close must unblock with ErrClosed.
	c, aerr := l.Accept()
	if c != nil {
		_ = c.Close()
		t.Fatal("expected nil conn after Close")
	}
	if aerr != net.ErrClosed {
		t.Fatalf("expected net.ErrClosed, got %v", aerr)
	}
}
