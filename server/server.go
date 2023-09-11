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
	"github.com/dolthub/go-mysql-server/server/golden"
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
type ProtocolListenerFunc func(cfg mysql.ListenerConfig) (ProtocolListener, error)

// DefaultProtocolListenerFunc is the protocol listener, which defaults to Vitess' protocol listener. Changing
// this function will change the protocol listener used when creating all servers. If multiple servers are needed
// with different protocols, then create each server after changing this function. Servers retain the protocol that
// they were created with.
var DefaultProtocolListenerFunc ProtocolListenerFunc = func(cfg mysql.ListenerConfig) (ProtocolListener, error) {
	return mysql.NewListenerWithConfig(cfg)
}

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

func portInUse(hostPort string) bool {
	timeout := time.Second
	conn, _ := net.DialTimeout("tcp", hostPort, timeout)
	if conn != nil {
		defer conn.Close()
		return true
	}
	return false
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

	if portInUse(cfg.Address) {
		unixSocketInUse = fmt.Errorf("Port %s already in use.", cfg.Address)
	}

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
	protocolListener, err := DefaultProtocolListenerFunc(listenerCfg)
	if err != nil {
		return nil, err
	}

	if vtListener, ok := protocolListener.(*mysql.Listener); ok {
		if cfg.Version != "" {
			vtListener.ServerVersion = cfg.Version
		}
		vtListener.TLSConfig = cfg.TLSConfig
		vtListener.RequireSecureTransport = cfg.RequireSecureTransport
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
			logrus.Warn("Please consider restarting the server with secure_file_priv set to a safe (or non-existant) directory.")
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
