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
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/dolthub/vitess/go/mysql"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/trace"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/sql"
)

// ProtocolListener handles connections based on the configuration it was given. These listeners also implement
// their own protocol, which by default will be the MySQL wire protocol, but another protocol may be provided.
type ProtocolListener interface {
	Addr() net.Addr
	Accept()
	Close()
}

// ProtocolListenerFunc returns a ProtocolListener based on the configuration it was given.
type ProtocolListenerFunc func(cfg Config, listenerCfg mysql.ListenerConfig, sel ServerEventListener) (ProtocolListener, error)

func MySQLProtocolListenerFactory(cfg Config, listenerCfg mysql.ListenerConfig, sel ServerEventListener) (ProtocolListener, error) {
	vtListener, err := mysql.NewListenerWithConfig(listenerCfg)
	if err != nil {
		return nil, err
	}
	if cfg.Version != "" {
		vtListener.ServerVersion = cfg.Version
	}
	vtListener.TLSConfig = cfg.TLSConfig
	vtListener.RequireSecureTransport = cfg.RequireSecureTransport
	return vtListener, nil
}

type ServerEventListener interface {
	ClientConnected()
	ClientDisconnected()
	QueryStarted()
	QueryCompleted(success bool, duration time.Duration)
}

// NewServer creates a server with the given protocol, address, authentication
// details given a SQLe engine and a session builder.
func NewServer(cfg Config, e *sqle.Engine, ctxFactory sql.ContextFactory, sb SessionBuilder, listener ServerEventListener) (*Server, error) {
	return NewServerWithHandler(cfg, e, ctxFactory, sb, listener, noopHandlerWrapper)
}

// HandlerWrapper provides a way for clients to wrap the mysql.Handler used by the server with a custom implementation
// that wraps it.
type HandlerWrapper func(h mysql.Handler) (mysql.Handler, error)

func noopHandlerWrapper(h mysql.Handler) (mysql.Handler, error) {
	return h, nil
}

// NewServerWithHandler creates a Server with a handler wrapped by the provided wrapper function.
func NewServerWithHandler(
	cfg Config,
	e *sqle.Engine,
	ctxFactory sql.ContextFactory,
	sb SessionBuilder,
	listener ServerEventListener,
	wrapper HandlerWrapper,
) (*Server, error) {
	var tracer trace.Tracer
	if cfg.Tracer != nil {
		tracer = cfg.Tracer
	} else {
		tracer = sql.NoopTracer
	}

	sm := NewSessionManager(ctxFactory, sb, tracer, e.Analyzer.Catalog.Database, e.MemoryManager, e.ProcessList, cfg.Address)
	h := &Handler{
		e:                 e,
		sm:                sm,
		readTimeout:       cfg.ConnReadTimeout,
		disableMultiStmts: cfg.DisableClientMultiStatements,
		maxLoggedQueryLen: cfg.MaxLoggedQueryLen,
		encodeLoggedQuery: cfg.EncodeLoggedQuery,
		sel:               listener,
	}

	handler, err := wrapper(h)
	if err != nil {
		return nil, err
	}

	return newServerFromHandler(cfg, e, sm, handler, listener)
}

func portInUse(hostPort string) bool {
	timeout := time.Second
	conn, _ := net.DialTimeout("tcp", hostPort, timeout)
	if conn != nil {
		defer conn.Close()
		return true
	}
	return false
}

func newServerFromHandler(cfg Config, e *sqle.Engine, sm *SessionManager, handler mysql.Handler, sel ServerEventListener) (*Server, error) {
	if cfg.ConnReadTimeout < 0 {
		cfg.ConnReadTimeout = 0
	}
	if cfg.ConnWriteTimeout < 0 {
		cfg.ConnWriteTimeout = 0
	}
	if cfg.MaxConnections < 0 {
		cfg.MaxConnections = 0
	}

	for _, opt := range cfg.Options {
		e, sm, handler = opt(e, sm, handler)
	}

	l := cfg.Listener
	var unixSocketInUse error
	if l == nil {
		if portInUse(cfg.Address) {
			unixSocketInUse = fmt.Errorf("Port %s already in use.", cfg.Address)
		}

		var err error
		l, err = NewListener(cfg.Protocol, cfg.Address, cfg.Socket)
		if err != nil {
			if errors.Is(err, UnixSocketInUseError) {
				unixSocketInUse = err
			} else {
				return nil, err
			}
		}
	}

	listenerCfg := mysql.ListenerConfig{
		Listener:                 l,
		AuthServer:               e.Analyzer.Catalog.MySQLDb,
		Handler:                  handler,
		ConnReadTimeout:          cfg.ConnReadTimeout,
		ConnWriteTimeout:         cfg.ConnWriteTimeout,
		MaxConns:                 cfg.MaxConnections,
		MaxWaitConns:             cfg.MaxWaitConnections,
		MaxWaitConnsTimeout:      cfg.MaxWaitConnectionsTimeout,
		ConnReadBufferSize:       mysql.DefaultConnBufferSize,
		AllowClearTextWithoutTLS: cfg.AllowClearTextWithoutTLS,
	}
	plf := cfg.ProtocolListenerFactory
	if plf == nil {
		plf = MySQLProtocolListenerFactory
	}
	protocolListener, err := plf(cfg, listenerCfg, sel)
	if err != nil {
		return nil, err
	}

	return &Server{
		Listener:   protocolListener,
		handler:    handler,
		sessionMgr: sm,
		Engine:     e,
	}, unixSocketInUse
}

// Start starts accepting connections on the server.
func (s *Server) Start() error {
	logrus.Infof("Server ready. Accepting connections.")
	s.WarnIfLoadFileInsecure()
	s.Listener.Accept()
	return nil
}

func (s *Server) WarnIfLoadFileInsecure() {
	_, v, ok := sql.SystemVariables.GetGlobal("secure_file_priv")
	if ok {
		if v == "" {
			logrus.Warn("secure_file_priv is set to \"\", which is insecure.")
			logrus.Warn("Any user with GRANT FILE privileges will be able to read any file which the sql-server process can read.")
			logrus.Warn("Please consider restarting the server with secure_file_priv set to a safe (or non-existent) directory.")
		}
	}
}

// Close closes the server connection.
func (s *Server) Close() error {
	logrus.Infof("Server closing listener. No longer accepting connections.")
	s.Listener.Close()
	return nil
}

// SessionManager returns the session manager for this server.
func (s *Server) SessionManager() *SessionManager {
	return s.sessionMgr
}
