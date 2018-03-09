package server

import (
	"context"
	"sync"

	uuid "github.com/satori/go.uuid"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-vitess.v0/mysql"
)

// SessionBuilder creates sessions given a context and a MySQL connection.
// The context already has a cancel in it.
type SessionBuilder func(context.Context, *mysql.Conn) sql.Session

// DoneFunc is a function that must be executed when the session is used and
// it can be disposed.
type DoneFunc func()

// DefaultSessionBuilder is a SessionBuilder that returns a base session with
// just the passed context in it.
func DefaultSessionBuilder(ctx context.Context, _ *mysql.Conn) sql.Session {
	return sql.NewBaseSession(ctx)
}

// SessionManager is in charge of creating new sessions for the given
// connections and keep track of which sessions are in each connection, so
// they can be cancelled is the connection is closed.
type SessionManager struct {
	mu           *sync.Mutex
	builder      SessionBuilder
	connSessions map[uint32][]uuid.UUID
	sessions     map[uuid.UUID]context.CancelFunc
}

// NewSessionManager creates a SessionManager with the given SessionBuilder.
func NewSessionManager(builder SessionBuilder) *SessionManager {
	return &SessionManager{
		mu:           new(sync.Mutex),
		builder:      builder,
		connSessions: make(map[uint32][]uuid.UUID),
		sessions:     make(map[uuid.UUID]context.CancelFunc),
	}
}

// NewSession creates a Session for the given connection. It also returns a
// DoneFunc that must be executed as soon as the user is done with the session
// so it can be disposed.
func (s *SessionManager) NewSession(conn *mysql.Conn) (sql.Session, DoneFunc, error) {
	ctx, cancel := context.WithCancel(context.Background())
	sess := s.builder(ctx, conn)
	id, err := uuid.NewV4()
	if err != nil {
		cancel()
		return nil, nil, err
	}

	s.mu.Lock()
	s.connSessions[conn.ConnectionID] = append(s.connSessions[conn.ConnectionID], id)
	s.sessions[id] = cancel
	s.mu.Unlock()

	return sess, func() {
		s.mu.Lock()
		defer s.mu.Unlock()

		delete(s.sessions, id)
		ids := s.connSessions[conn.ConnectionID]
		for i, sessID := range ids {
			if sessID == id {
				s.connSessions[conn.ConnectionID] = append(ids[:i], ids[i+1:]...)
				break
			}
		}
	}, nil
}

// CloseConn closes the connection in the session manager and all its
// associated sessions, as well as cancelling all contexts passed along with
// the sessions.
func (s *SessionManager) CloseConn(conn *mysql.Conn) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, id := range s.connSessions[conn.ConnectionID] {
		s.sessions[id]()
		delete(s.sessions, id)
	}
	delete(s.connSessions, conn.ConnectionID)
}
