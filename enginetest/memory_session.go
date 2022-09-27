// Copyright 2022 Dolthub, Inc.
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

package enginetest

import (
	"github.com/sirupsen/logrus"

	"github.com/dolthub/go-mysql-server/sql"
)

type InMemoryBaseSession struct {
	Session sql.Session
	idx     int
}

func NewInMemoryBaseSession(sess sql.Session) *InMemoryBaseSession {
	return &InMemoryBaseSession{Session: sess, idx: 0}
}

// ValidateSession implements the Session interface.
func (s *InMemoryBaseSession) ValidateSession(ctx *sql.Context, dbName string) error {
	s.idx++
	return s.Session.ValidateSession(ctx, dbName)
}

func (s *InMemoryBaseSession) GetIdx() int {
	return s.idx
}

// Address implements the Session interface.
func (s *InMemoryBaseSession) Address() string {
	return s.Session.Address()
}

// Client implements the Session interface.
func (s *InMemoryBaseSession) Client() sql.Client {
	return s.Session.Client()
}

// SetClient implements the Session interface.
func (s *InMemoryBaseSession) SetClient(client sql.Client) {
	s.Session.SetClient(client)
}

// SetSessionVariable implements the Session interface.
func (s *InMemoryBaseSession) SetSessionVariable(ctx *sql.Context, sysVarName string, value interface{}) error {
	return s.Session.SetSessionVariable(ctx, sysVarName, value)
}

// InitSessionVariable implements the Session interface.
func (s *InMemoryBaseSession) InitSessionVariable(ctx *sql.Context, sysVarName string, value interface{}) error {
	return s.Session.InitSessionVariable(ctx, sysVarName, value)
}

// SetUserVariable implements the Session interface.
func (s *InMemoryBaseSession) SetUserVariable(ctx *sql.Context, varName string, value interface{}) error {
	return s.Session.SetUserVariable(ctx, varName, value)
}

// GetSessionVariable implements the Session interface.
func (s *InMemoryBaseSession) GetSessionVariable(ctx *sql.Context, sysVarName string) (interface{}, error) {
	return s.Session.GetSessionVariable(ctx, sysVarName)
}

// GetUserVariable implements the Session interface.
func (s *InMemoryBaseSession) GetUserVariable(ctx *sql.Context, varName string) (sql.Type, interface{}, error) {
	return s.Session.GetUserVariable(ctx, varName)
}

// GetAllSessionVariables implements the Session interface.
func (s *InMemoryBaseSession) GetAllSessionVariables() map[string]interface{} {
	return s.Session.GetAllSessionVariables()
}

// GetCurrentDatabase implements the Session interface.
func (s *InMemoryBaseSession) GetCurrentDatabase() string {
	return s.Session.GetCurrentDatabase()
}

// SetCurrentDatabase implements the Session interface.
func (s *InMemoryBaseSession) SetCurrentDatabase(dbName string) {
	s.Session.SetCurrentDatabase(dbName)
}

// CommitTransaction implements the Session interface.
func (s *InMemoryBaseSession) CommitTransaction(ctx *sql.Context, dbName string, transaction sql.Transaction) error {
	return s.Session.CommitTransaction(ctx, dbName, transaction)
}

// ID implements the Session interface.
func (s *InMemoryBaseSession) ID() uint32 {
	return s.Session.ID()
}

// Warn implements the Session interface.
func (s *InMemoryBaseSession) Warn(warn *sql.Warning) {
	s.Session.Warn(warn)
}

// Warnings implements the Session interface.
func (s *InMemoryBaseSession) Warnings() []*sql.Warning {
	return s.Session.Warnings()
}

// ClearWarnings implements the Session interface.
func (s *InMemoryBaseSession) ClearWarnings() {
	s.Session.ClearWarnings()
}

// WarningCount implements the Session interface.
func (s *InMemoryBaseSession) WarningCount() uint16 {
	return s.Session.WarningCount()
}

// AddLock implements the Session interface.
func (s *InMemoryBaseSession) AddLock(lockName string) error {
	return s.Session.AddLock(lockName)
}

// DelLock implements the Session interface.
func (s *InMemoryBaseSession) DelLock(lockName string) error {
	return s.Session.DelLock(lockName)
}

// IterLocks implements the Session interface.
func (s *InMemoryBaseSession) IterLocks(cb func(name string) error) error {
	return s.Session.IterLocks(cb)
}

// SetLastQueryInfo implements the Session interface.
func (s *InMemoryBaseSession) SetLastQueryInfo(key string, value int64) {
	s.Session.SetLastQueryInfo(key, value)
}

// GetLastQueryInfo implements the Session interface.
func (s *InMemoryBaseSession) GetLastQueryInfo(key string) int64 {
	return s.Session.GetLastQueryInfo(key)
}

// GetTransaction implements the Session interface.
func (s *InMemoryBaseSession) GetTransaction() sql.Transaction {
	return s.Session.GetTransaction()
}

// SetTransaction implements the Session interface.
func (s *InMemoryBaseSession) SetTransaction(tx sql.Transaction) {
	s.Session.SetTransaction(tx)
}

// SetIgnoreAutoCommit implements the Session interface.
func (s *InMemoryBaseSession) SetIgnoreAutoCommit(ignore bool) {
	s.Session.SetIgnoreAutoCommit(ignore)
}

// GetIgnoreAutoCommit implements the Session interface.
func (s *InMemoryBaseSession) GetIgnoreAutoCommit() bool {
	return s.Session.GetIgnoreAutoCommit()
}

// GetLogger implements the Session interface.
func (s *InMemoryBaseSession) GetLogger() *logrus.Entry {
	return s.Session.GetLogger()
}

// SetLogger implements the Session interface.
func (s *InMemoryBaseSession) SetLogger(entry *logrus.Entry) {
	s.Session.SetLogger(entry)
}

// GetIndexRegistry implements the Session interface.
func (s *InMemoryBaseSession) GetIndexRegistry() *sql.IndexRegistry {
	return s.Session.GetIndexRegistry()
}

// GetViewRegistry implements the Session interface.
func (s *InMemoryBaseSession) GetViewRegistry() *sql.ViewRegistry {
	return s.Session.GetViewRegistry()
}

// SetIndexRegistry implements the Session interface.
func (s *InMemoryBaseSession) SetIndexRegistry(registry *sql.IndexRegistry) {
	s.Session.SetIndexRegistry(registry)
}

// SetViewRegistry implements the Session interface.
func (s *InMemoryBaseSession) SetViewRegistry(registry *sql.ViewRegistry) {
	s.Session.SetViewRegistry(registry)
}

// SetConnectionId implements the Session interface.
func (s *InMemoryBaseSession) SetConnectionId(connId uint32) {
	s.Session.SetConnectionId(connId)
}

// GetCharacterSet implements the Session interface.
func (s *InMemoryBaseSession) GetCharacterSet() sql.CharacterSetID {
	return s.Session.GetCharacterSet()
}

// GetCharacterSetResults implements the Session interface.
func (s *InMemoryBaseSession) GetCharacterSetResults() sql.CharacterSetID {
	return s.Session.GetCharacterSetResults()
}

// GetCollation implements the Session interface.
func (s *InMemoryBaseSession) GetCollation() sql.CollationID {
	return s.Session.GetCollation()
}
