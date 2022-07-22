// Copyright 2020-2022 Dolthub, Inc.
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

	"golang.org/x/sync/errgroup"
)

type connRes struct {
	conn net.Conn
	err  error
}

type Listener struct {
	netListener  net.Listener
	fileListener net.Listener
	eg           *errgroup.Group
	conns        chan connRes
	shutdown     chan struct{}
}

// NewListener creates a new Listener.
func NewListener(protocol, address string, socket string) (*Listener, error) {
	netl, err := net.Listen(protocol, address)
	if err != nil {
		return nil, err
	}

	var unixl net.Listener
	if socket != "" {
		unixListener, err := net.ListenUnix("unix", &net.UnixAddr{Name: socket, Net: "unix"})
		if err != nil {
			return nil, err
		}
		unixl = unixListener
	}

	l := &Listener{
		netListener:  netl,
		fileListener: unixl,
		conns:        make(chan connRes),
		eg:           new(errgroup.Group),
		shutdown:     make(chan struct{}),
	}
	l.eg.Go(func() error {
		for {
			conn, err := l.netListener.Accept()
			if err != nil {
				if err == net.ErrClosed || err.Error() == "use of closed network connection" {
					return nil
				}
			}

			if conn == nil {
				return nil
			}

			select {
			case <-l.shutdown:
				conn.Close()
				return nil
			case l.conns <- connRes{conn, err}:
			}
		}
	})

	if l.fileListener != nil {
		l.eg.Go(func() error {
			for {
				conn, err := l.fileListener.Accept()
				if err != nil {
					if err == net.ErrClosed || err.Error() == "use of closed network connection" {
						return nil
					}
				}

				if conn == nil {
					return nil
				}

				select {
				case <-l.shutdown:
					conn.Close()
					return nil
				case l.conns <- connRes{conn, err}:
				}
			}
		})
	}

	return l, nil
}

func (l *Listener) Accept() (net.Conn, error) {
	cr, ok := <-l.conns
	if !ok {
		return nil, net.ErrClosed
	}
	return cr.conn, cr.err
}

func (l *Listener) Close() error {
	close(l.shutdown)
	l.netListener.Close()
	if l.fileListener != nil {
		l.fileListener.Close()
	}
	err := l.eg.Wait()
	close(l.conns)
	return err
}

func (l *Listener) Addr() net.Addr {
	return l.netListener.Addr()
}
