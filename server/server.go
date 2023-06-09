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
	"time"

	"github.com/dolthub/vitess/go/mysql"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/trace"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/server/golden"
	"github.com/dolthub/go-mysql-server/sql"
)

type ServerEventListener interface {
	ClientConnected()
	ClientDisconnected()
	QueryStarted()
	QueryCompleted(success bool, duration time.Duration)
}

// NewDefaultServer creates a Server with the default session builder.
func NewDefaultServer(cfg Config, e *sqle.Engine) (*Server, error) {
	return NewServer(cfg, e, DefaultSessionBuilder, nil)
}

// NewServer creates a server with the given protocol, address, authentication
// details given a SQLe engine and a session builder.
func NewServer(cfg Config, e *sqle.Engine, sb SessionBuilder, listener ServerEventListener) (*Server, error) {
	var tracer trace.Tracer
	if cfg.Tracer != nil {
		tracer = cfg.Tracer
	} else {
		tracer = sql.NoopTracer
	}

	sm := NewSessionManager(sb, tracer, e.Analyzer.Catalog.Database, e.MemoryManager, e.ProcessList, cfg.Address)
	handler := &Handler{
		e:                 e,
		sm:                sm,
		readTimeout:       cfg.ConnReadTimeout,
		disableMultiStmts: cfg.DisableClientMultiStatements,
		maxLoggedQueryLen: cfg.MaxLoggedQueryLen,
		encodeLoggedQuery: cfg.EncodeLoggedQuery,
		sel:               listener,
	}
	//handler = NewHandler_(e, sm, cfg.ConnReadTimeout, cfg.DisableClientMultiStatements, cfg.MaxLoggedQueryLen, cfg.EncodeLoggedQuery, listener)
	return newServerFromHandler(cfg, e, sm, handler)
}

// NewValidatingServer creates a Server that validates its query results using a MySQL connection
// as a source of golden-value query result sets.
func NewValidatingServer(
	cfg Config,
	e *sqle.Engine,
	sb SessionBuilder,
	listener ServerEventListener,
	mySqlConn string,
) (*Server, error) {
	var tracer trace.Tracer
	if cfg.Tracer != nil {
		tracer = cfg.Tracer
	} else {
		tracer = sql.NoopTracer
	}

	sm := NewSessionManager(sb, tracer, e.Analyzer.Catalog.Database, e.MemoryManager, e.ProcessList, cfg.Address)
	h := &Handler{
		e:                 e,
		sm:                sm,
		readTimeout:       cfg.ConnReadTimeout,
		disableMultiStmts: cfg.DisableClientMultiStatements,
		maxLoggedQueryLen: cfg.MaxLoggedQueryLen,
		encodeLoggedQuery: cfg.EncodeLoggedQuery,
		sel:               listener,
	}

	handler, err := golden.NewValidatingHandler(h, mySqlConn, logrus.StandardLogger())
	if err != nil {
		return nil, err
	}
	return newServerFromHandler(cfg, e, sm, handler)
}

func newServerFromHandler(cfg Config, e *sqle.Engine, sm *SessionManager, handler mysql.Handler) (*Server, error) {
	if cfg.ConnReadTimeout < 0 {
		cfg.ConnReadTimeout = 0
	}
	if cfg.ConnWriteTimeout < 0 {
		cfg.ConnWriteTimeout = 0
	}
	if cfg.MaxConnections < 0 {
		cfg.MaxConnections = 0
	}

	var unixSocketInUse error
	l, err := NewListener(cfg.Protocol, cfg.Address, cfg.Socket)
	if err != nil {
		if errors.Is(err, UnixSocketInUseError) {
			unixSocketInUse = err
		} else {
			return nil, err
		}
	}

	listenerCfg := mysql.ListenerConfig{
		Listener:                 l,
		AuthServer:               e.Analyzer.Catalog.MySQLDb,
		Handler:                  handler,
		ConnReadTimeout:          cfg.ConnReadTimeout,
		ConnWriteTimeout:         cfg.ConnWriteTimeout,
		MaxConns:                 cfg.MaxConnections,
		ConnReadBufferSize:       mysql.DefaultConnBufferSize,
		AllowClearTextWithoutTLS: cfg.AllowClearTextWithoutTLS,
	}
	vtListnr, err := mysql.NewListenerWithConfig(listenerCfg)
	if err != nil {
		return nil, err
	}

	if cfg.Version != "" {
		vtListnr.ServerVersion = cfg.Version
	}
	vtListnr.TLSConfig = cfg.TLSConfig
	vtListnr.RequireSecureTransport = cfg.RequireSecureTransport

	return &Server{
		Listener:   vtListnr,
		handler:    handler,
		sessionMgr: sm,
		Engine:     e,
	}, unixSocketInUse
}

// Start starts accepting connections on the server.
func (s *Server) Start() error {
	logrus.Infof("Server ready. Accepting connections.")
	s.Listener.Accept()
	return nil
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
