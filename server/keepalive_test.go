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

//go:build !windows

package server

import (
	"net"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// TestAcceptedConnHasKeepAlive verifies keepAliveListener arms TCP keepalive on
// every accepted connection -- the wiring that lets the server detect half-open
// (dead-peer) clients that never send a FIN. It asserts the socket option
// directly rather than trying to reap a dead peer, which cannot be simulated on
// loopback (the client kernel answers keepalive probes).
func TestAcceptedConnHasKeepAlive(t *testing.T) {
	base, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer base.Close()

	// Values distinguishable from any OS default so the assertions are meaningful.
	l := keepAliveListener{Listener: base, cfg: net.KeepAliveConfig{
		Enable:   true,
		Idle:     37 * time.Second,
		Interval: 11 * time.Second,
		Count:    5,
	}}

	accepted := make(chan net.Conn, 1)
	go func() {
		if c, err := l.Accept(); err == nil {
			accepted <- c
		}
	}()

	client, err := net.Dial("tcp", base.Addr().String())
	require.NoError(t, err)
	defer client.Close()

	var srv net.Conn
	select {
	case srv = <-accepted:
	case <-time.After(5 * time.Second):
		t.Fatal("listener never accepted the connection")
	}
	defer srv.Close()

	tc, ok := srv.(*net.TCPConn)
	require.True(t, ok, "accepted conn should be *net.TCPConn")

	raw, err := tc.SyscallConn()
	require.NoError(t, err)

	var (
		keepAlive int
		idle      int
		idleErr   error
		getErr    error
	)
	require.NoError(t, raw.Control(func(fd uintptr) {
		keepAlive, getErr = unix.GetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_KEEPALIVE)
		if runtime.GOOS == "linux" {
			// TCP_KEEPIDLE (seconds) is Linux-specific; other unixes map Idle
			// differently, so only assert the value there.
			idle, idleErr = unix.GetsockoptInt(int(fd), unix.IPPROTO_TCP, unix.TCP_KEEPIDLE)
		}
	}))
	require.NoError(t, getErr)
	require.Equal(t, 1, keepAlive, "SO_KEEPALIVE should be enabled on the accepted conn")
	if runtime.GOOS == "linux" {
		require.NoError(t, idleErr)
		require.Equal(t, 37, idle, "TCP_KEEPIDLE should match the configured Idle")
	}
}
