package server // import "gopkg.in/src-d/go-mysql-server.v0/server"

import (
	opentracing "github.com/opentracing/opentracing-go"
	"gopkg.in/src-d/go-mysql-server.v0"

	"gopkg.in/src-d/go-vitess.v0/mysql"
)

// Server is a MySQL server for SQLe engines.
type Server struct {
	Listener *mysql.Listener
}

// Config for the mysql server.
type Config struct {
	// Protocol for the connection.
	Protocol string
	// Address of the server.
	Address string
	// Auth of the server.
	Auth mysql.AuthServer
	// Tracer to use in the server. By default, a noop tracer will be used if
	// no tracer is provided.
	Tracer opentracing.Tracer
}

// NewDefaultServer creates a Server with the default session builder.
func NewDefaultServer(cfg Config, e *sqle.Engine) (*Server, error) {
	return NewServer(cfg, e, DefaultSessionBuilder)
}

// NewServer creates a server with the given protocol, address, authentication
// details given a SQLe engine and a session builder.
func NewServer(cfg Config, e *sqle.Engine, sb SessionBuilder) (*Server, error) {
	var tracer opentracing.Tracer
	if cfg.Tracer != nil {
		tracer = cfg.Tracer
	} else {
		tracer = opentracing.NoopTracer{}
	}

	handler := NewHandler(e, NewSessionManager(sb, tracer))
	l, err := mysql.NewListener(cfg.Protocol, cfg.Address, cfg.Auth, handler)
	if err != nil {
		return nil, err
	}

	return &Server{Listener: l}, nil
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
