// Copyright 2020-2021 Dolthub, Inc.
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
	"os"

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
func NewListener(protocol, address string) (*Listener, error) {
	netl, err := net.Listen(protocol, address)
	if err != nil {
		return nil, err
	}
	socketf, err := os.OpenFile("/var/tmp/mysql.sock", os.O_CREATE, os.ModeSocket | 0755)
	if err != nil {
		return nil, err
	}
	fl, err := net.FileListener(socketf) // nonsense here
	if err != nil {
		return nil, err
	}
	l := &Listener{
		netListener:  netl,
		fileListener: fl,
		conns:        make(chan connRes),
		eg:           new(errgroup.Group),
		shutdown:     make(chan struct{}),
	}
	l.eg.Go(func() error {
		for {
			conn, err := l.netListener.Accept()
			if err == net.ErrClosed {
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
	l.eg.Go(func() error {
		for {
			conn, err := l.fileListener.Accept()
			if err == net.ErrClosed {
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
	l.fileListener.Close()
	err := l.eg.Wait()
	close(l.conns)
	return err
}

func (l *Listener) Addr() net.Addr {
	return l.netListener.Addr()
}
