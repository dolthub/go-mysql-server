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
	"syscall"

	"golang.org/x/sync/errgroup"
)

var UnixSocketInUseError = errors.New("bind address at given unix socket path is already in use")

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
	// channel to receive connections on either listener
	conns chan connRes
	// channel to close both listener
	shutdown chan struct{}
	once     *sync.Once
}

// NewListener creates a new Listener.
// 'protocol' takes "tcp" and 'address' takes "host:port" information for TCP socket connection.
// For unix socket connection, 'unixSocketPath' takes a path for the unix socket file.
// If 'unixSocketPath' is empty, no need to create the second listener.
func NewListener(protocol, address string, unixSocketPath string) (*Listener, error) {
	netl, err := newNetListener(protocol, address)
	if err != nil {
		return nil, err
	}

	var unixl net.Listener
	var unixSocketInUse error
	if unixSocketPath != "" {
		if runtime.GOOS == "windows" {
			return nil, fmt.Errorf("unable to create unix socket listener on Windows")
		}
		unixListener, err := net.ListenUnix("unix", &net.UnixAddr{Name: unixSocketPath, Net: "unix"})
		if err == nil {
			unixl = unixListener
		} else if errors.Is(err, syscall.EADDRINUSE) {
			// we continue if error is unix socket bind address is already in use
			// we return UnixSocketInUseError error to track the error back to where server gets started and add warning
			unixSocketInUse = UnixSocketInUseError
		} else {
			return nil, err
		}
	}

	l := &Listener{
		netListener:  netl,
		unixListener: unixl,
		conns:        make(chan connRes),
		eg:           new(errgroup.Group),
		shutdown:     make(chan struct{}),
		once:         &sync.Once{},
	}
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

	return l, unixSocketInUse
}

func (l *Listener) Accept() (net.Conn, error) {
	cr, ok := <-l.conns
	if !ok {
		return nil, net.ErrClosed
	}
	return cr.conn, cr.err
}

func (l *Listener) Close() error {
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
	l.once.Do(func() {
		close(l.shutdown)
		close(l.conns)
	})
	return l.eg.Wait()
}

func (l *Listener) Addr() net.Addr {
	return l.netListener.Addr()
}
