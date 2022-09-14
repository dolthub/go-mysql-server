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
	"errors"
	"fmt"
	"net"
	"runtime"
	"sync"

	"golang.org/x/sync/errgroup"
)

// connRes represents a connection made to a listener and an error result
type connRes struct {
	conn net.Conn
	err  error
}

// Listener implements a single listener with two net.Listener,
// one for TCP socket and another for unix socket connections.
type Listener struct {
	// netListener is a tcp socket listener
	netListener net.Listener
	// unixListener is a unix socket listener
	unixListener net.Listener
	eg           *errgroup.Group
	wg           sync.WaitGroup
	// channel to receive connections on either listener
	conns chan connRes
	// channel to close both listener
	shutdown chan struct{}
}

// NewListener creates a new Listener.
// 'protocol' takes "tcp" and 'address' takes "host:port" information for TCP socket connection.
// For unix socket connection, 'unixSocketPath' takes a path for the unix socket file.
// If 'unixSocketPath' is empty, no need to create the second listener.
func NewListener(protocol, address string, unixSocketPath string) (*Listener, error) {
	netl, err := net.Listen(protocol, address)
	if err != nil {
		return nil, err
	}

	var unixl net.Listener
	if unixSocketPath != "" {
		if runtime.GOOS == "windows" {
			return nil, fmt.Errorf("unable to create unix socket listener on Windows")
		}
		unixListener, err := net.ListenUnix("unix", &net.UnixAddr{Name: unixSocketPath, Net: "unix"})
		if err != nil {
			return nil, err
		}
		unixl = unixListener
	}

	l := &Listener{
		netListener:  netl,
		unixListener: unixl,
		conns:        make(chan connRes),
		eg:           new(errgroup.Group),
		shutdown:     make(chan struct{}),
	}
	l.wg.Add(1)
	l.eg.Go(func() error {
		for {
			conn, err := l.netListener.Accept()
			// connection can be closed already from the other goroutine
			if errors.Is(err, net.ErrClosed) {
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

	if l.unixListener != nil {
		l.wg.Add(1)
		l.eg.Go(func() error {
			for {
				conn, err := l.unixListener.Accept()
				// connection can be closed already from the other goroutine
				if errors.Is(err, net.ErrClosed) {
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

	go func() {
		defer l.wg.Done()
	}()

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
	l.wg.Wait()
	if l.shutdown != nil {
		close(l.shutdown)
		l.shutdown = nil
	}

	err := l.netListener.Close()
	if err != nil && !errors.Is(err, net.ErrClosed) {
		return err
	}
	if l.unixListener != nil {
		err = l.unixListener.Close()
		if err != nil && !errors.Is(err, net.ErrClosed) {
			return err
		}
	}
	err = l.eg.Wait()
	if l.conns != nil {
		close(l.conns)
		l.conns = nil
	}
	return err
}

func (l *Listener) Addr() net.Addr {
	return l.netListener.Addr()
}
