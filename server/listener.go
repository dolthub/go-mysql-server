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
	"os/exec"
	"runtime"
	"strings"
	"sync"
	"syscall"

	"golang.org/x/sync/errgroup"

	"github.com/sirupsen/logrus"
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
		// TODO: We're still seeing errors about address already in use, e.g:
		//       https://github.com/dolthub/dolt/actions/runs/5395898150/jobs/9798898619?pr=6245#step:18:2240
		// More examples:
		//      https://github.com/dolthub/dolt/actions/runs/5395439523/jobs/9797921148#step:18:2249
		//      https://github.com/dolthub/dolt/actions/runs/5404900318/jobs/9819723916?pr=6245#step:18:2216
		if err != nil {
			if strings.Contains(strings.ToLower(err.Error()), "address already in use") {
				split := strings.Split(address, ":")
				if len(split) == 2 {
					port := split[1]
					// if we're on unix, we should attempt to run lsof to see what is using the port and how
					// and output that information in the error to the user:
					//    lsof -i:<port>
					if runtime.GOOS != "windows" {
						cmd := exec.Command("lsof", fmt.Sprintf("-i:%s", port))
						output, err := cmd.CombinedOutput()
						if err != nil {
							logrus.StandardLogger().Warnf("Unable to run lsof to detect what is using port %s: %s", port, err.Error())
						} else {
							logrus.StandardLogger().Warnf("lsof output: %s", string(output))
						}
					}
				} else {
					logrus.StandardLogger().Warnf("Unable to parse address into `host:port`: %s", address)
				}
			}
		}

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
