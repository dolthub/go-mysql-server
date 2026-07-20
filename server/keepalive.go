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
	"time"
)

// DefaultTCPKeepAlive is applied to accepted TCP connections so the server
// detects peers that died without a clean close -- a client host crash, a kill
// -9 of the client host, or a network partition -- and reaps their in-flight
// queries and open transactions within a bounded window, instead of leaving them
// until net_read_timeout (default 8h) fires.
//
// It does not affect connections whose peer is alive: an idle-but-live client's
// kernel answers keepalive probes at the TCP layer regardless of whether the
// application is reading, so such connections are never closed by this mechanism.
// That is the property that makes keepalive a false-positive-free liveness
// signal, unlike an application-level read deadline.
//
// Worst-case detection latency is Idle + Interval*Count = 120s.
var DefaultTCPKeepAlive = net.KeepAliveConfig{
	Enable:   true,
	Idle:     60 * time.Second,
	Interval: 15 * time.Second,
	Count:    4,
}

// keepAliveListener enables TCP keepalive on every accepted connection. Non-TCP
// connections (unix socket, in-memory) pass through untouched.
type keepAliveListener struct {
	net.Listener
	cfg net.KeepAliveConfig
}

func (l keepAliveListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	if tc, ok := c.(*net.TCPConn); ok {
		// Best-effort: SetKeepAliveConfig applies what the platform supports and
		// returns an error only for unsupported fields, which we tolerate.
		_ = tc.SetKeepAliveConfig(l.cfg)
	}
	return c, nil
}
