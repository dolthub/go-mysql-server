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
	"fmt"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
)

type Session struct {
	*sql.BaseSession
	dbProvider *DbProvider
	editAccumulators map[tableKey]tableEditAccumulator
	tables           map[tableKey]*TableData
}

var _ sql.Session = (*Session)(nil)
var _ sql.TransactionSession = (*Session)(nil)
var _ sql.Transaction = (*Transaction)(nil)

// NewSession returns the new session for this object
func NewSession(baseSession *sql.BaseSession, provider *DbProvider) *Session {
	return &Session{
		BaseSession:      baseSession,
		dbProvider: 			 provider,
		editAccumulators: make(map[tableKey]tableEditAccumulator),
		tables:           make(map[tableKey]*TableData),
	}
}

func SessionFromContext(ctx *sql.Context) *Session {
	return ctx.Session.(*Session)
}

type Transaction struct {
	readOnly bool
}

var _ sql.Transaction = (*Transaction)(nil)

func (s *Transaction) String() string {
	return "in-memory transaction"
}

func (s *Transaction) IsReadOnly() bool {
	return s.readOnly
}


// editAccumulator returns the edit accumulator for the table provided for this session, creating one if it
// doesn't exist
func (s *Session) editAccumulator(t *Table) tableEditAccumulator {
	tableKey := key(t.data)
	ea, ok := s.editAccumulators[tableKey]
	if !ok {
		data := t.copy()
		ea = NewTableEditAccumulator(data)
		s.editAccumulators[tableKey] = ea
	}
	
	return ea
}

type tableKey struct {
	db string
	table string
}

func key(t *TableData) tableKey {
	return tableKey{strings.ToLower(t.dbName), strings.ToLower(t.tableName)}
}

// activeEditAccumulator returns the edit accumulator for the table provided for this session and whether it exists
func (s *Session) activeEditAccumulator(t *Table) (tableEditAccumulator, bool) {
	ea, ok := s.editAccumulators[key(t.data)]
	return ea, ok
}

func (s *Session) clearEditAccumulator(t *Table) {
	delete(s.editAccumulators, key(t.data))
}

// tableData returns the table data for this session for the table provided 
func (s *Session) tableData(t *Table) *TableData {
	td, ok := s.tables[key(t.data)]
	if !ok {
		s.tables[key(t.data)] = t.data
		return t.data
	}

	return td
}

// putTable stores the table data for this session for the table provided 
func (s *Session) putTable(d *TableData) {
	s.tables[key(d)] = d
	delete(s.editAccumulators, key(d))
}

// StartTransaction clears session state and returns a new transaction object.
// Because we don't support concurrency, we store table data changes in the session, rather than the transaction itself.
func (s *Session) StartTransaction(ctx *sql.Context, tCharacteristic sql.TransactionCharacteristic) (sql.Transaction, error) {
	s.editAccumulators = make(map[tableKey]tableEditAccumulator)
	s.tables = make(map[tableKey]*TableData)
	return &Transaction{tCharacteristic == sql.ReadOnly}, nil
}

func (s *Session) CommitTransaction(ctx *sql.Context, tx sql.Transaction) error {
	for key := range s.tables {
		if key.db == "" && key.table == "" {
			// dual table
			continue
		}
		db, err := s.dbProvider.Database(ctx, key.db)
		if err != nil {
			return err
		}
		
		var baseDb *BaseDatabase
		switch db := db.(type) {
		case *BaseDatabase:
			baseDb = db
		case *Database:
			baseDb = db.BaseDatabase
		case *HistoryDatabase:
			baseDb = db.BaseDatabase
		default:
			return fmt.Errorf("unknown database type %T", db)
		}
		baseDb.putTable(s.tables[key].Table(baseDb))
	}
	
	return nil
}

func (s *Session) Rollback(ctx *sql.Context, transaction sql.Transaction) error {
	s.editAccumulators = make(map[tableKey]tableEditAccumulator)
	s.tables = make(map[tableKey]*TableData)
	return nil
}

func (s *Session) CreateSavepoint(ctx *sql.Context, transaction sql.Transaction, name string) error {
	return fmt.Errorf("savepoints are not supported in memory sessions")
}

func (s *Session) RollbackToSavepoint(ctx *sql.Context, transaction sql.Transaction, name string) error {
	return fmt.Errorf("savepoints are not supported in memory sessions")
}

func (s *Session) ReleaseSavepoint(ctx *sql.Context, transaction sql.Transaction, name string) error {
	return fmt.Errorf("savepoints are not supported in memory sessions")
}
