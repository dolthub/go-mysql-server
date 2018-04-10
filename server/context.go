package server

import (
	"context"
	"sync"

	opentracing "github.com/opentracing/opentracing-go"
	uuid "github.com/satori/go.uuid"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-vitess.v0/mysql"
)

// SessionBuilder creates sessions given a context and a MySQL connection.
type SessionBuilder func(*mysql.Conn) sql.Session

// DoneFunc is a function that must be executed when the session is used and
// it can be disposed.
type DoneFunc func()

// DefaultSessionBuilder is a SessionBuilder that returns a base session.
func DefaultSessionBuilder(_ *mysql.Conn) sql.Session {
	return sql.NewBaseSession()
}

// SessionManager is in charge of creating new sessions for the given
// connections and keep track of which sessions are in each connection, so
// they can be cancelled is the connection is closed.
type SessionManager struct {
	tracer          opentracing.Tracer
	mu              *sync.Mutex
	builder         SessionBuilder
	sessions        map[uint32]sql.Session
	sessionContexts map[uint32][]uuid.UUID
	contexts        map[uuid.UUID]context.CancelFunc
}

// NewSessionManager creates a SessionManager with the given ContextBuilder.
func NewSessionManager(builder SessionBuilder, tracer opentracing.Tracer) *SessionManager {
	return &SessionManager{
		tracer:          tracer,
		mu:              new(sync.Mutex),
		builder:         builder,
		sessions:        make(map[uint32]sql.Session),
		sessionContexts: make(map[uint32][]uuid.UUID),
		contexts:        make(map[uuid.UUID]context.CancelFunc),
	}
}

// NewSession creates a Session for the given connection.
func (s *SessionManager) NewSession(conn *mysql.Conn) {
	s.mu.Lock()
	s.sessions[conn.ConnectionID] = s.builder(conn)
	s.mu.Unlock()
}

// NewContext creates a new context for the session at the given conn.
func (s *SessionManager) NewContext(conn *mysql.Conn) (*sql.Context, DoneFunc, error) {
	ctx, cancel := context.WithCancel(context.Background())
	s.mu.Lock()
	sess := s.sessions[conn.ConnectionID]
	s.mu.Unlock()
	context := sql.NewContext(ctx, sql.WithSession(sess), sql.WithTracer(s.tracer))
	id, err := uuid.NewV4()
	if err != nil {
		cancel()
		return nil, nil, err
	}

	s.mu.Lock()
	s.sessionContexts[conn.ConnectionID] = append(s.sessionContexts[conn.ConnectionID], id)
	s.contexts[id] = cancel
	s.mu.Unlock()

	return context, func() {
		s.mu.Lock()
		defer s.mu.Unlock()

		delete(s.contexts, id)
		ids := s.sessionContexts[conn.ConnectionID]
		for i, sessID := range ids {
			if sessID == id {
				s.sessionContexts[conn.ConnectionID] = append(ids[:i], ids[i+1:]...)
				break
			}
		}
	}, nil
}

// CloseConn closes the connection in the session manager and all its
// associated contexts, which are cancelled.
func (s *SessionManager) CloseConn(conn *mysql.Conn) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, id := range s.sessionContexts[conn.ConnectionID] {
		s.contexts[id]()
		delete(s.contexts, id)
	}
	delete(s.sessionContexts, conn.ConnectionID)
}
