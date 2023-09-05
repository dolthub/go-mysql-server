// Copyright 2023 Dolthub, Inc.
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

package memory

import (
	"context"
	"strings"
	

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/mysql_db"
	"github.com/dolthub/vitess/go/mysql"
)

type Session struct {
	*sql.BaseSession
	editAccumulators map[string]tableEditAccumulator	
}

var _ sql.Session = (*Session)(nil)
var _ sql.TransactionSession = (*Session)(nil)

// NewSession returns the new session for this object
func NewSession(baseSession *sql.BaseSession) *Session {
	return &Session{
		BaseSession: baseSession,
		editAccumulators: make(map[string]tableEditAccumulator),
	}
}

func SessionFromContext(ctx *sql.Context) *Session {
	return ctx.Session.(*Session)
}

func SessionBuilder(ctx context.Context, c *mysql.Conn, addr string) (sql.Session, error) {
	host := ""
	user := ""
	mysqlConnectionUser, ok := c.UserData.(mysql_db.MysqlConnectionUser)
	if ok {
		host = mysqlConnectionUser.Host
		user = mysqlConnectionUser.User
	}
	client := sql.Client{Address: host, User: user, Capabilities: c.Capabilities}
	baseSession := sql.NewBaseSessionWithClientServer(addr, client, c.ConnectionID)
	return NewSession(baseSession), nil
}

// editAccumulator returns the edit accumulator for the table provided for this session, creating one if it
// doesn't exist
func (s *Session) editAccumulator(t *Table) tableEditAccumulator {
	ea, ok := s.editAccumulators[strings.ToLower(t.name)]
	if !ok {
		ea = NewTableEditAccumulator(t.copy())
		s.editAccumulators[strings.ToLower(t.name)] = ea
	}
	
	return ea
}

// activeEditAccumulator returns the edit accumulator for the table provided for this session and whether it exists
func (s *Session) activeEditAccumulator(t *Table) (tableEditAccumulator, bool) {
	ea, ok := s.editAccumulators[strings.ToLower(t.name)]
	return ea, ok
}

func (s *Session) clearEditAccumulator(t *Table) {
	delete(s.editAccumulators, strings.ToLower(t.name))
}

func (s *Session) StartTransaction(ctx *sql.Context, tCharacteristic sql.TransactionCharacteristic) (sql.Transaction, error) {
	// TODO implement me
	panic("implement me")
}

func (s *Session) CommitTransaction(ctx *sql.Context, tx sql.Transaction) error {
	// TODO implement me
	panic("implement me")
}

func (s *Session) Rollback(ctx *sql.Context, transaction sql.Transaction) error {
	// TODO implement me
	panic("implement me")
}

func (s *Session) CreateSavepoint(ctx *sql.Context, transaction sql.Transaction, name string) error {
	// TODO implement me
	panic("implement me")
}

func (s *Session) RollbackToSavepoint(ctx *sql.Context, transaction sql.Transaction, name string) error {
	// TODO implement me
	panic("implement me")
}

func (s *Session) ReleaseSavepoint(ctx *sql.Context, transaction sql.Transaction, name string) error {
	// TODO implement me
	panic("implement me")
}
