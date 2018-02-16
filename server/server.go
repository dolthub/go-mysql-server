package server

import (
	"gopkg.in/src-d/go-mysql-server.v0"

	"gopkg.in/src-d/go-vitess.v0/mysql"
)

// Server is a MySQL server for SQLe engines.
type Server struct {
	Listener *mysql.Listener
}

// NewServer creates a server with the given protocol, address, authentication
// details given a SQLe engine.
func NewServer(protocol, address string, auth mysql.AuthServer, e *sqle.Engine) (*Server, error) {
	l, err := mysql.NewListener(protocol, address, auth, NewHandler(e))
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
