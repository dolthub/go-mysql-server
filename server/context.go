package server

import (
	"context"
	"sync"

	opentracing "github.com/opentracing/opentracing-go"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-vitess.v1/mysql"
)

// SessionBuilder creates sessions given a MySQL connection and a server address.
type SessionBuilder func(conn *mysql.Conn, addr string) sql.Session

// DoneFunc is a function that must be executed when the session is used and
// it can be disposed.
type DoneFunc func()

// DefaultSessionBuilder is a SessionBuilder that returns a base session.
func DefaultSessionBuilder(c *mysql.Conn, addr string) sql.Session {
	return sql.NewSession(addr, c.User, c.ConnectionID)
}

// SessionManager is in charge of creating new sessions for the given
// connections and keep track of which sessions are in each connection, so
// they can be cancelled is the connection is closed.
type SessionManager struct {
	addr     string
	tracer   opentracing.Tracer
	mu       *sync.Mutex
	builder  SessionBuilder
	sessions map[uint32]sql.Session
	pid      uint64
}

// NewSessionManager creates a SessionManager with the given SessionBuilder.
func NewSessionManager(
	builder SessionBuilder,
	tracer opentracing.Tracer,
	addr string,
) *SessionManager {
	return &SessionManager{
		addr:     addr,
		tracer:   tracer,
		mu:       new(sync.Mutex),
		builder:  builder,
		sessions: make(map[uint32]sql.Session),
	}
}

func (s *SessionManager) nextPid() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pid++
	return s.pid
}

// NewSession creates a Session for the given connection.
func (s *SessionManager) NewSession(conn *mysql.Conn) {
	s.mu.Lock()
	s.sessions[conn.ConnectionID] = s.builder(conn, s.addr)
	s.mu.Unlock()
}

// NewContext creates a new context for the session at the given conn.
func (s *SessionManager) NewContext(conn *mysql.Conn) *sql.Context {
	s.mu.Lock()
	sess := s.sessions[conn.ConnectionID]
	s.mu.Unlock()
	context := sql.NewContext(
		context.Background(),
		sql.WithSession(sess),
		sql.WithTracer(s.tracer),
		sql.WithPid(s.nextPid()),
	)

	return context
}

// CloseConn closes the connection in the session manager and all its
// associated contexts, which are cancelled.
func (s *SessionManager) CloseConn(conn *mysql.Conn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.sessions, conn.ConnectionID)
}
