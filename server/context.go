package server

import (
	"context"
	"sync"

	"github.com/opentracing/opentracing-go"
	"github.com/src-d/go-mysql-server/sql"
	"vitess.io/vitess/go/mysql"
)

// SessionBuilder creates sessions given a MySQL connection and a server address.
type SessionBuilder func(ctx context.Context, conn *mysql.Conn, addr string) (sql.Session, *sql.IndexRegistry, *sql.ViewRegistry, error)

// DoneFunc is a function that must be executed when the session is used and
// it can be disposed.
type DoneFunc func()

// DefaultSessionBuilder is a SessionBuilder that returns a base session.
func DefaultSessionBuilder(ctx context.Context, c *mysql.Conn, addr string) (sql.Session, *sql.IndexRegistry, *sql.ViewRegistry, error) {
	client := c.RemoteAddr().String()
	return sql.NewSession(addr, client, c.User, c.ConnectionID), sql.NewIndexRegistry(), sql.NewViewRegistry(), nil
}

// SessionManager is in charge of creating new sessions for the given
// connections and keep track of which sessions are in each connection, so
// they can be cancelled if the connection is closed.
type SessionManager struct {
	addr     string
	tracer   opentracing.Tracer
	memory   *sql.MemoryManager
	mu       *sync.Mutex
	builder  SessionBuilder
	sessions map[uint32]sql.Session
	idxRegs  map[uint32]*sql.IndexRegistry
	viewRegs map[uint32]*sql.ViewRegistry
	pid      uint64
}

// NewSessionManager creates a SessionManager with the given SessionBuilder.
func NewSessionManager(
	builder SessionBuilder,
	tracer opentracing.Tracer,
	memory *sql.MemoryManager,
	addr string,
) *SessionManager {
	return &SessionManager{
		addr:     addr,
		tracer:   tracer,
		memory:   memory,
		mu:       new(sync.Mutex),
		builder:  builder,
		sessions: make(map[uint32]sql.Session),
		idxRegs: make(map[uint32]*sql.IndexRegistry),
		viewRegs: make(map[uint32]*sql.ViewRegistry),
	}
}

func (s *SessionManager) nextPid() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pid++
	return s.pid
}

// NewSession creates a Session for the given connection and saves it to
// session pool.
func (s *SessionManager) NewSession(ctx context.Context, conn *mysql.Conn) error {
	var err error

	s.mu.Lock()
	s.sessions[conn.ConnectionID], s.idxRegs[conn.ConnectionID], s.viewRegs[conn.ConnectionID], err = s.builder(ctx, conn, s.addr)
	s.mu.Unlock()

	return err
}

func (s *SessionManager) SetDB(conn *mysql.Conn, db string) error {
	sess, _, _, err := s.getOrCreateSession(context.Background(), conn)

	if err != nil {
		return err
	}

	sess.SetCurrentDatabase(db)
	return nil
}

func (s *SessionManager) session(conn *mysql.Conn) sql.Session {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sessions[conn.ConnectionID]
}

// NewContext creates a new context for the session at the given conn.
func (s *SessionManager) NewContext(conn *mysql.Conn) (*sql.Context, error) {
	return s.NewContextWithQuery(conn, "")
}

func (s *SessionManager) getOrCreateSession(ctx context.Context, conn *mysql.Conn) (sql.Session, *sql.IndexRegistry, *sql.ViewRegistry, error){
	s.mu.Lock()
	sess, ok := s.sessions[conn.ConnectionID]
	ir := s.idxRegs[conn.ConnectionID]
	vr := s.viewRegs[conn.ConnectionID]
	if !ok {
		var err error
		sess, ir, vr, err = s.builder(ctx, conn, s.addr)

		if err != nil {
			return nil, nil, nil, err
		}

		s.sessions[conn.ConnectionID] = sess
		s.idxRegs[conn.ConnectionID] = ir
		s.viewRegs[conn.ConnectionID] = vr
	}
	s.mu.Unlock()

	return sess, ir, vr, nil
}

// NewContextWithQuery creates a new context for the session at the given conn.
func (s *SessionManager) NewContextWithQuery(conn *mysql.Conn, query string, ) (*sql.Context, error) {
	ctx := context.Background()
	sess, ir, vr, err := s.getOrCreateSession(ctx, conn)

	if err != nil {
		return nil, err
	}

	context := sql.NewContext(
		ctx,
		sql.WithSession(sess),
		sql.WithTracer(s.tracer),
		sql.WithPid(s.nextPid()),
		sql.WithQuery(query),
		sql.WithMemoryManager(s.memory),
		sql.WithRootSpan(s.tracer.StartSpan("query")),
		sql.WithIndexRegistry(ir),
		sql.WithViewRegistry(vr),
	)

	return context, nil
}

// CloseConn closes the connection in the session manager and all its
// associated contexts, which are cancelled.
func (s *SessionManager) CloseConn(conn *mysql.Conn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.sessions, conn.ConnectionID)
	delete(s.idxRegs, conn.ConnectionID)
	delete(s.viewRegs, conn.ConnectionID)
}
