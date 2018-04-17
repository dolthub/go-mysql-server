package server

import (
	"io"

	opentracing "github.com/opentracing/opentracing-go"
	"gopkg.in/src-d/go-mysql-server.v0"

	"gopkg.in/src-d/go-vitess.v0/mysql"
)

// Server is a MySQL server for SQLe engines.
type Server struct {
	Listener *mysql.Listener
	closer   io.Closer
}

// NewDefaultServer creates a Server with the default session builder.
func NewDefaultServer(cfg Config, e *sqle.Engine) (*Server, error) {
	return NewServer(cfg, e, DefaultSessionBuilder)
}

// NewServer creates a server with the given protocol, address, authentication
// details given a SQLe engine and a session builder.
func NewServer(cfg Config, e *sqle.Engine, sb SessionBuilder) (*Server, error) {
	tracer, close, err := cfg.Tracer()
	if err != nil {
		return nil, err
	}
	opentracing.SetGlobalTracer(tracer)

	handler := NewHandler(e, NewSessionManager(sb, tracer))
	l, err := mysql.NewListener(cfg.Protocol, cfg.Address, cfg.Auth, handler)
	if err != nil {
		return nil, err
	}

	return &Server{Listener: l, closer: close}, nil
}

// Start starts accepting connections on the server.
func (s *Server) Start() error {
	s.Listener.Accept()
	return nil
}

// Close closes the server connection.
func (s *Server) Close() error {
	s.Listener.Close()
	return s.closer.Close()
}
