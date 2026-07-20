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

// DefaultTCPKeepAlive detects peers that died without a clean close (host crash,
// partition) so their queries/transactions are reaped in a bounded window rather
// than lingering until net_read_timeout (8h). Live peers answer probes at the TCP
// layer, so idle connections are never affected. Detection latency is
// Idle+Interval*Count = 120s.
var DefaultTCPKeepAlive = net.KeepAliveConfig{
	Enable:   true,
	Idle:     60 * time.Second,
	Interval: 15 * time.Second,
	Count:    4,
}

// keepAliveListener enables TCP keepalive on accepted connections. Non-TCP conns
// pass through untouched.
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
		_ = tc.SetKeepAliveConfig(l.cfg) // best-effort: unsupported fields are ignored
	}
	return c, nil
}
