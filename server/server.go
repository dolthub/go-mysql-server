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
	"time"

	"github.com/dolthub/vitess/go/mysql"
	"github.com/opentracing/opentracing-go"

	sqle "github.com/dolthub/go-mysql-server"
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
	var tracer opentracing.Tracer
	if cfg.Tracer != nil {
		tracer = cfg.Tracer
	} else {
		tracer = opentracing.NoopTracer{}
	}

	if cfg.ConnReadTimeout < 0 {
		cfg.ConnReadTimeout = 0
	}

	if cfg.ConnWriteTimeout < 0 {
		cfg.ConnWriteTimeout = 0
	}

	if cfg.MaxConnections < 0 {
		cfg.MaxConnections = 0
	}

	handler := NewHandler(e,
		NewSessionManager(
			sb,
			tracer,
			e.Analyzer.Catalog.HasDB,
			e.MemoryManager,
			e.ProcessList,
			cfg.Address),
		cfg.ConnReadTimeout,
		cfg.DisableClientMultiStatements,
		listener,
	)
	a := cfg.Auth.Mysql()
	l, err := NewListener(cfg.Protocol, cfg.Address, handler)
	if err != nil {
		return nil, err
	}

	listenerCfg := mysql.ListenerConfig{
		Listener:           l,
		AuthServer:         a,
		Handler:            handler,
		ConnReadTimeout:    cfg.ConnReadTimeout,
		ConnWriteTimeout:   cfg.ConnWriteTimeout,
		MaxConns:           cfg.MaxConnections,
		ConnReadBufferSize: mysql.DefaultConnBufferSize,
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

	return &Server{Listener: vtListnr, h: handler}, nil
}

// Start starts accepting connections on the server.
func (s *Server) Start() error {
	s.Listener.Accept()
	return nil
}

// Close closes the server connection.
func (s *Server) Close() error {
	s.Listener.Close()
	return nil
}
