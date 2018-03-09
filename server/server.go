package server

import (
	"gopkg.in/src-d/go-mysql-server.v0"

	"gopkg.in/src-d/go-vitess.v0/mysql"
)

// Server is a MySQL server for SQLe engines.
type Server struct {
	Listener *mysql.Listener
}

// NewDefaultServer creates a Server with the default session builder.
func NewDefaultServer(
	protocol, address string,
	auth mysql.AuthServer,
	e *sqle.Engine,
) (*Server, error) {
	return NewServer(protocol, address, auth, e, DefaultSessionBuilder)
}

// NewServer creates a server with the given protocol, address, authentication
// details given a SQLe engine and a session builder.
func NewServer(protocol, address string, auth mysql.AuthServer, e *sqle.Engine, sb SessionBuilder) (*Server, error) {
	handler := NewHandler(e, NewSessionManager(sb))
	l, err := mysql.NewListener(protocol, address, auth, handler)
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
