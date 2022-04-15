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
	"github.com/opentracing/opentracing-go"
	"github.com/sirupsen/logrus"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/grant_tables"
)

// SessionBuilder creates sessions given a MySQL connection and a server address.
type SessionBuilder func(ctx context.Context, conn *mysql.Conn, addr string) (sql.Session, error)

// DoneFunc is a function that must be executed when the session is used and
// it can be disposed.
type DoneFunc func()

// DefaultSessionBuilder is a SessionBuilder that returns a base session.
func DefaultSessionBuilder(ctx context.Context, c *mysql.Conn, addr string) (sql.Session, error) {
	host := ""
	user := ""
	mysqlConnectionUser, ok := c.UserData.(grant_tables.MysqlConnectionUser)
	if ok {
		host = mysqlConnectionUser.Host
		user = mysqlConnectionUser.User
	}
	client := sql.Client{Address: host, User: user, Capabilities: c.Capabilities}
	return sql.NewBaseSessionWithClientServer(addr, client, c.ConnectionID), nil
}

type managedSession struct {
	session sql.Session
	conn    *mysql.Conn
}

// SessionManager is in charge of creating new sessions for the given
// connections and keep track of which sessions are in each connection, so
// they can be cancelled if the connection is closed.
type SessionManager struct {
	addr        string
	tracer      opentracing.Tracer
	hasDBFunc   func(ctx *sql.Context, name string) bool
	memory      *sql.MemoryManager
	processlist sql.ProcessList
	mu          *sync.Mutex
	builder     SessionBuilder
	sessions    map[uint32]*managedSession
	pid         uint64
}

// NewSessionManager creates a SessionManager with the given SessionBuilder.
func NewSessionManager(
	builder SessionBuilder,
	tracer opentracing.Tracer,
	hasDBFunc func(ctx *sql.Context, name string) bool,
	memory *sql.MemoryManager,
	processlist sql.ProcessList,
	addr string,
) *SessionManager {
	return &SessionManager{
		addr:        addr,
		tracer:      tracer,
		hasDBFunc:   hasDBFunc,
		memory:      memory,
		processlist: processlist,
		mu:          new(sync.Mutex),
		builder:     builder,
		sessions:    make(map[uint32]*managedSession),
	}
}

func (s *SessionManager) nextPid() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pid++
	return s.pid
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

	s.sessions[conn.ConnectionID] = &managedSession{session, conn}

	logger := s.sessions[conn.ConnectionID].session.GetLogger()
	if logger == nil {
		log := logrus.StandardLogger()
		logger = logrus.NewEntry(log)
	}

	s.sessions[conn.ConnectionID].session.SetLogger(
		logger.WithField(sqle.ConnectionIdLogField, conn.ConnectionID).
			WithField(sqle.ConnectTimeLogKey, time.Now()),
	)

	return err
}

func (s *SessionManager) SetDB(conn *mysql.Conn, db string) error {
	sess, err := s.getOrCreateSession(context.Background(), conn)
	if err != nil {
		return err
	}

	ctx := sql.NewContext(context.Background(), sql.WithSession(sess))
	if db != "" && !s.hasDBFunc(ctx, db) {
		return sql.ErrDatabaseNotFound.New(db)
	}

	sess.SetCurrentDatabase(db)
	return nil
}

func (s *SessionManager) session(conn *mysql.Conn) sql.Session {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sessions[conn.ConnectionID].session
}

// NewContext creates a new context for the session at the given conn.
func (s *SessionManager) NewContext(conn *mysql.Conn) (*sql.Context, error) {
	return s.NewContextWithQuery(conn, "")
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

	return sess.session, nil
}

// NewContextWithQuery creates a new context for the session at the given conn.
func (s *SessionManager) NewContextWithQuery(conn *mysql.Conn, query string) (*sql.Context, error) {
	ctx := context.Background()
	sess, err := s.getOrCreateSession(ctx, conn)

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
		sql.WithProcessList(s.processlist),
		sql.WithRootSpan(s.tracer.StartSpan("query")),
		sql.WithServices(sql.Services{
			KillConnection: s.killConnection,
			LoadInfile:     conn.LoadInfile,
		}),
	)

	return context, nil
}

// Exposed through sql.Services.KillConnection. At the time that this is
// called, any outstanding process has been killed through ProcessList.Kill()
// as well.
func (s *SessionManager) killConnection(connID uint32) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if entry, ok := s.sessions[connID]; ok {
		delete(s.sessions, connID)
		entry.conn.Close()
	}
	return nil
}

// Remove the session assosiated with |conn| from the session manager.
func (s *SessionManager) CloseConn(conn *mysql.Conn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.sessions, conn.ConnectionID)
}
