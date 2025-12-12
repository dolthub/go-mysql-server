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
	"context"
	"sync"
	"time"

	"github.com/dolthub/vitess/go/mysql"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/trace"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/mysql_db"
)

// SessionBuilder creates sessions given a MySQL connection and a server address.
type SessionBuilder func(ctx context.Context, conn *mysql.Conn, addr string) (sql.Session, error)

// DoneFunc is a function that must be executed when the session is used and
// it can be disposed.
type DoneFunc func()

// SessionManager is in charge of creating new sessions for the given
// connections and keep track of which sessions are in each connection, so
// they can be cancelled if the connection is closed.
type SessionManager struct {
	tracer      trace.Tracer
	processlist sql.ProcessList
	getDbFunc   func(ctx *sql.Context, db string) (sql.Database, error)
	memory      *sql.MemoryManager
	mu          *sync.Mutex
	builder     SessionBuilder
	sessions    map[uint32]sql.Session
	connections map[uint32]*mysql.Conn
	ctxFactory  sql.ContextFactory
	addr        string
	// Implements WaitForClosedConnections(), which is only used
	// at server shutdown to allow the integrator to ensure that
	// no connections are being handled by handlers.
	wg      sync.WaitGroup
	lastPid uint64
}

// NewSessionManager creates a SessionManager with the given SessionBuilder.
func NewSessionManager(
	ctxFactory sql.ContextFactory,
	builder SessionBuilder,
	tracer trace.Tracer,
	getDbFunc func(ctx *sql.Context, db string) (sql.Database, error),
	memory *sql.MemoryManager,
	processlist sql.ProcessList,
	addr string,
) *SessionManager {
	return &SessionManager{
		addr:        addr,
		tracer:      tracer,
		getDbFunc:   getDbFunc,
		memory:      memory,
		processlist: processlist,
		mu:          new(sync.Mutex),
		builder:     builder,
		sessions:    make(map[uint32]sql.Session),
		connections: make(map[uint32]*mysql.Conn),
		ctxFactory:  ctxFactory,
	}
}

func (s *SessionManager) nextPid() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastPid++
	return s.lastPid
}

// Block the calling thread until all known connections are closed. It
// is an error to call this concurrently while the server might still
// be accepting new connections.
func (s *SessionManager) WaitForClosedConnections() {
	s.wg.Wait()
}

// AddConn adds a connection to be tracked by the SessionManager. Should be called as
// soon as possible after the server has accepted the connection. Results in
// the connection being tracked by ProcessList and being available through
// KillConnection. The connection will be tracked until RemoveConn is called,
// so clients should ensure a call to AddConn is always paired up with a call
// to RemoveConn.
func (s *SessionManager) AddConn(conn *mysql.Conn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.connections[conn.ConnectionID] = conn
	s.processlist.AddConnection(conn.ConnectionID, conn.RemoteAddr().String())
	s.wg.Add(1)
}

// Called once a connection is authenticated and in a ready
// state. Responsible for creating the session associated with the
// connection and registering the session, with appropriate
// authentication information, with the process list.
func (s *SessionManager) ConnReady(ctx context.Context, conn *mysql.Conn) error {
	sess, err := s.getOrCreateSession(ctx, conn)
	if err != nil {
		return err
	}
	s.processlist.ConnectionReady(sess)
	return nil
}

// NewSession creates a Session for the given connection and saves it to the session pool.
func (s *SessionManager) NewSession(ctx context.Context, conn *mysql.Conn) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	session, err := s.builder(ctx, conn, s.addr)
	if err != nil {
		return err
	}

	session.SetConnectionId(conn.ConnectionID)

	if cur, ok := s.sessions[conn.ConnectionID]; ok {
		sql.SessionEnd(cur)
	}

	s.sessions[conn.ConnectionID] = session

	logger := session.GetLogger()
	if logger == nil {
		log := logrus.StandardLogger()
		logger = logrus.NewEntry(log)
	}

	session.SetLogger(
		logger.WithField(sql.ConnectionIdLogField, conn.ConnectionID).
			WithField(sql.ConnectTimeLogKey, time.Now()),
	)

	return err
}

// SetDB sets the current database of the given connection session.
// If the session does not exist, it creates a new session with given connection.
func (s *SessionManager) SetDB(ctx context.Context, conn *mysql.Conn, dbName string) error {
	sess, err := s.getOrCreateSession(ctx, conn)
	if err != nil {
		return err
	}

	err = sql.SessionCommandBegin(sess)
	if err != nil {
		return err
	}
	defer sql.SessionCommandEnd(sess)

	sqlCtx := s.ctxFactory(ctx, sql.WithSession(sess))
	sqlCtx, err = s.processlist.BeginOperation(sqlCtx)
	if err != nil {
		return err
	}
	defer s.processlist.EndOperation(sqlCtx)
	var db sql.Database
	if dbName != "" {
		db, err = s.getDbFunc(sqlCtx, dbName)
		if err != nil {
			return err
		}
	}

	sess.SetCurrentDatabase(dbName)
	if dbName != "" {
		if pdb, ok := db.(mysql_db.PrivilegedDatabase); ok {
			db = pdb.Unwrap()
		}
		err = sess.UseDatabase(sqlCtx, db)
		if err != nil {
			return err
		}
	}

	// We do this here and in ConnReady.
	//
	// Previously, Vitess did not have a ConnectionAuthenticated
	// callback on the Handler and the only time we updated the
	// authenticated user information in the processlist was on
	// ComInitDB. This resulted in "unathenticated user" being
	// shown in the process list if a connection chose to run
	// queries without issuing ComInitDB.
	//
	// Calling this here makes certain the current database
	// updates StartedAt and allows the newly selected database to
	// be correctly reflected in the process list.
	s.processlist.ConnectionReady(sess)
	return nil
}

// GetCurrentDB returns the current database name of the given connection session.
func (s *SessionManager) GetCurrentDB(conn *mysql.Conn) string {
	sess := s.session(conn)
	return sess.GetCurrentDatabase()
}

// Iter iterates over the active sessions and executes the specified callback function on each one.
func (s *SessionManager) Iter(f func(session sql.Session) (stop bool, err error)) error {
	// Lock the mutex guarding the sessions map while we make a copy of it to prevent errors from
	// mutating a map while iterating over it. Making a copy of the map also allows us to guard
	// against long running callback functions being passed in that could cause long mutex blocking.
	s.mu.Lock()
	sessions := make([]sql.Session, 0, len(s.sessions))
	for _, value := range s.sessions {
		sessions = append(sessions, value)
	}
	s.mu.Unlock()

	for _, sess := range sessions {
		stop, err := f(sess)
		if stop == true || err != nil {
			return err
		}
	}
	return nil
}

func (s *SessionManager) session(conn *mysql.Conn) sql.Session {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sessions[conn.ConnectionID]
}

func (s *SessionManager) getOrCreateSession(ctx context.Context, conn *mysql.Conn) (sql.Session, error) {
	s.mu.Lock()
	sess, ok := s.sessions[conn.ConnectionID]
	// Release this lock immediately. If we call NewSession below, we
	// cannot hold the lock. We will relock if we need to.
	s.mu.Unlock()

	if !ok {
		err := s.NewSession(ctx, conn)
		if err != nil {
			return nil, err
		}

		s.mu.Lock()
		sess = s.sessions[conn.ConnectionID]
		s.mu.Unlock()
	}

	return sess, nil
}

// InitSessionDefaultVariable sets a default value to a parameter of a session at start.
func (s *SessionManager) InitSessionDefaultVariable(ctx context.Context, conn *mysql.Conn, name, value string) error {
	sess, err := s.getOrCreateSession(ctx, conn)
	if err != nil {
		return err
	}
	return sess.InitSessionVariableDefault(s.ctxFactory(ctx, sql.WithSession(sess)), name, value)
}

// NewContextWithQuery creates a new context for the session at the given conn.
func (s *SessionManager) NewContextWithQuery(ctx context.Context, conn *mysql.Conn, query string) (*sql.Context, error) {
	sess, err := s.getOrCreateSession(ctx, conn)

	if err != nil {
		return nil, err
	}

	ctx, span := s.tracer.Start(ctx, "query")

	createdCtx := s.ctxFactory(
		ctx,
		sql.WithSession(sess),
		sql.WithTracer(s.tracer),
		sql.WithPid(s.nextPid()),
		sql.WithQuery(query),
		sql.WithMemoryManager(s.memory),
		sql.WithProcessList(s.processlist),
		sql.WithRootSpan(span),
		sql.WithServices(sql.Services{
			KillConnection: s.KillConnection,
			LoadInfile:     conn.LoadInfile,
		}),
	)

	return createdCtx, nil
}

// Exposed through (*sql.Context).Services.KillConnection. Calls Close on the
// tracked connection with |connID|. The full teardown of the connection is
// asychronous, similar to how |Process.Kill| for tearing down an inflight
// query is asynchronous. The connection and any running query will remain in
// the ProcessList and in the SessionManager until it has been torn down by the
// server handler.
func (s *SessionManager) KillConnection(connID uint32) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if conn, ok := s.connections[connID]; ok {
		conn.Close()
	}
	return nil
}

// Remove the session assosiated with |conn| from the session manager.
func (s *SessionManager) RemoveConn(conn *mysql.Conn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.wg.Done()
	if cur, ok := s.sessions[conn.ConnectionID]; ok {
		sql.SessionEnd(cur)
	}
	delete(s.sessions, conn.ConnectionID)
	delete(s.connections, conn.ConnectionID)
	s.processlist.RemoveConnection(conn.ConnectionID)
}
