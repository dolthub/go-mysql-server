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

package sql

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/dolthub/vitess/go/mysql"
	"github.com/sirupsen/logrus"
)

// BaseSession is the basic session implementation. Integrators should typically embed this type into their custom
// session implementations to get base functionality.
type BaseSession struct {
	id     uint32
	addr   string
	client Client

	// TODO(andy): in principle, we shouldn't
	//   have concurrent access to the session.
	//   Needs investigation.
	mu sync.RWMutex

	// |mu| protects the following state
	logger           *logrus.Entry
	currentDB        string
	transactionDb    string
	systemVars       map[string]SystemVarValue
	statusVars       map[string]StatusVarValue
	userVars         SessionUserVariables
	idxReg           *IndexRegistry
	viewReg          *ViewRegistry
	warnings         []*Warning
	warncnt          uint16
	locks            map[string]bool
	queriedDb        string
	lastQueryInfo    map[string]any
	tx               Transaction
	ignoreAutocommit bool

	// When the MySQL database updates any tables related to privileges, it increments its counter. We then update our
	// privilege set if our counter doesn't equal the database's counter.
	privSetCounter uint64
	privilegeSet   PrivilegeSet
}

func (s *BaseSession) GetLogger() *logrus.Entry {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.logger == nil {
		s.logger = s.newLogger()
	}
	return s.logger
}

func (s *BaseSession) newLogger() *logrus.Entry {
	log := logrus.StandardLogger()
	return logrus.NewEntry(log)
}

func (s *BaseSession) SetLogger(logger *logrus.Entry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.logger = logger
}

func (s *BaseSession) SetIgnoreAutoCommit(ignore bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ignoreAutocommit = ignore
}

func (s *BaseSession) GetIgnoreAutoCommit() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.ignoreAutocommit
}

var _ Session = (*BaseSession)(nil)

func (s *BaseSession) SetTransactionDatabase(dbName string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.transactionDb = dbName
}

func (s *BaseSession) GetTransactionDatabase() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.transactionDb
}

// Address returns the server address.
func (s *BaseSession) Address() string { return s.addr }

// Client returns session's client information.
func (s *BaseSession) Client() Client { return s.client }

// SetClient implements the Session interface.
func (s *BaseSession) SetClient(c Client) {
	s.client = c
	return
}

// GetAllSessionVariables implements the Session interface.
func (s *BaseSession) GetAllSessionVariables() map[string]interface{} {
	m := make(map[string]interface{})
	s.mu.RLock()
	defer s.mu.RUnlock()

	for k, v := range s.systemVars {
		if sysType, ok := v.Var.GetType().(SetType); ok {
			if sv, ok := v.Val.(uint64); ok {
				if svStr, err := sysType.BitsToString(sv); err == nil {
					m[k] = svStr
				}
				continue
			}
		}
		m[k] = v.Val
	}
	return m
}

// SetSessionVariable implements the Session interface.
func (s *BaseSession) SetSessionVariable(ctx *Context, sysVarName string, value interface{}) error {
	sysVarName = strings.ToLower(sysVarName)
	sysVar, ok := s.systemVars[sysVarName]

	// Since we initialized the system variables in this session at session start time, any variables that were added since that time
	// will need to be added dynamically here.
	// TODO: fix this with proper session lifecycle management
	if !ok {
		if SystemVariables != nil {
			sv, _, ok := SystemVariables.GetGlobal(sysVarName)
			if !ok {
				return ErrUnknownSystemVariable.New(sysVarName)
			}
			return s.setSessVar(ctx, sv, value, false)
		} else {
			return ErrUnknownSystemVariable.New(sysVarName)
		}
	}

	if sysVar.Var.IsReadOnly() {
		return ErrSystemVariableReadOnly.New(sysVarName)
	}
	return s.setSessVar(ctx, sysVar.Var, value, false)
}

// InitSessionVariable implements the Session interface and is used to initialize variables (Including read-only variables)
func (s *BaseSession) InitSessionVariable(ctx *Context, sysVarName string, value interface{}) error {
	sysVar, _, ok := SystemVariables.GetGlobal(sysVarName)
	if !ok {
		return ErrUnknownSystemVariable.New(sysVarName)
	}

	sysVarName = strings.ToLower(sysVarName)
	val, ok := s.systemVars[sysVarName]
	if ok && val.Val != sysVar.GetDefault() {
		return ErrSystemVariableReinitialized.New(sysVarName)
	}

	return s.setSessVar(ctx, sysVar, value, true)
}

func (s *BaseSession) setSessVar(ctx *Context, sysVar SystemVariable, value interface{}, init bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var svv SystemVarValue
	var err error
	if init {
		svv, err = sysVar.InitValue(value, false)
		if err != nil {
			return err
		}
	} else {
		svv, err = sysVar.SetValue(value, false)
		if err != nil {
			return err
		}
	}
	sysVarName := strings.ToLower(sysVar.GetName())
	s.systemVars[sysVarName] = svv
	return nil
}

// SetUserVariable implements the Session interface.
func (s *BaseSession) SetUserVariable(ctx *Context, varName string, value interface{}, typ Type) error {
	return s.userVars.SetUserVariable(ctx, varName, value, typ)
}

// GetSessionVariable implements the Session interface.
func (s *BaseSession) GetSessionVariable(ctx *Context, sysVarName string) (interface{}, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	sysVarName = strings.ToLower(sysVarName)
	sysVar, ok := s.systemVars[sysVarName]
	if !ok {
		return nil, ErrUnknownSystemVariable.New(sysVarName)
	}
	// TODO: this is duplicated from within variables.globalSystemVariables, suggesting the need for an interface
	if sysType, ok := sysVar.Var.GetType().(SetType); ok {
		if sv, ok := sysVar.Val.(uint64); ok {
			return sysType.BitsToString(sv)
		}
	}
	return sysVar.Val, nil
}

// GetUserVariable implements the Session interface.
func (s *BaseSession) GetUserVariable(ctx *Context, varName string) (Type, interface{}, error) {
	return s.userVars.GetUserVariable(ctx, varName)
}

// GetStatusVariable implements the Session interface.
func (s *BaseSession) GetStatusVariable(_ *Context, statVarName string) (interface{}, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	statVar, ok := s.statusVars[statVarName]
	if !ok {
		return nil, ErrUnknownSystemVariable.New(statVarName)
	}
	return statVar.Val, nil
}

// SetStatusVariable implements the Session interface.
func (s *BaseSession) SetStatusVariable(_ *Context, statVarName string, val interface{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	statVar, ok := s.statusVars[statVarName]
	if !ok {
		return ErrUnknownSystemVariable.New(statVarName)
	}
	statVar.Val = val
	s.statusVars[statVarName] = statVar
	return nil
}

// GetAllStatusVariables implements the Session interface.
func (s *BaseSession) GetAllStatusVariables(_ *Context) map[string]StatusVarValue {
	s.mu.RLock()
	defer s.mu.RUnlock()

	m := make(map[string]StatusVarValue)
	for k, v := range s.statusVars {
		m[k] = v
	}
	return m
}

// IncrementStatusVariable implements the Session interface.
func (s *BaseSession) IncrementStatusVariable(_ *Context, statVarName string, val int) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	statVar, ok := s.statusVars[statVarName]
	if !ok {
		return ErrUnknownSystemVariable.New(statVarName)
	}
	statVal, ok := statVar.Val.(uint64)
	if !ok {
		return fmt.Errorf("status variable %s is not a uint64", statVarName)
	}
	if val < 0 {
		statVar.Val = statVal - uint64(-val)
	} else {
		statVar.Val = statVal + uint64(val)
	}
	s.statusVars[statVarName] = statVar
	return nil
}

// GetCharacterSet returns the character set for this session (defined by the system variable `character_set_connection`).
func (s *BaseSession) GetCharacterSet() CharacterSetID {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sysVar, _ := s.systemVars[characterSetConnectionSysVarName]
	if sysVar.Val == nil {
		return CharacterSet_Unspecified
	}
	charSet, err := ParseCharacterSet(sysVar.Val.(string))
	if err != nil {
		panic(err) // shouldn't happen
	}
	return charSet
}

// GetCharacterSetResults returns the result character set for this session (defined by the system variable `character_set_results`).
func (s *BaseSession) GetCharacterSetResults() CharacterSetID {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sysVar, _ := s.systemVars[characterSetResultsSysVarName]
	if sysVar.Val == nil {
		return CharacterSet_Unspecified
	}
	charSet, err := ParseCharacterSet(sysVar.Val.(string))
	if err != nil {
		panic(err) // shouldn't happen
	}
	return charSet
}

// GetCollation returns the collation for this session (defined by the system variable `collation_connection`).
func (s *BaseSession) GetCollation() CollationID {
	s.mu.Lock()
	defer s.mu.Unlock()
	sysVar, ok := s.systemVars[collationConnectionSysVarName]

	// In tests, the collation may not be set because the sys vars haven't been initialized
	if !ok {
		return Collation_Default
	}
	if sysVar.Val == nil {
		return Collation_Unspecified
	}
	valStr := sysVar.Val.(string)
	collation, err := ParseCollation("", valStr, false)
	if err != nil {
		panic(err) // shouldn't happen
	}
	return collation
}

// ValidateSession provides integrators a chance to do any custom validation of this session before any query is executed in it.
func (s *BaseSession) ValidateSession(ctx *Context) error {
	return nil
}

// GetCurrentDatabase gets the current database for this session
func (s *BaseSession) GetCurrentDatabase() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.currentDB
}

// SetCurrentDatabase sets the current database for this session
func (s *BaseSession) SetCurrentDatabase(dbName string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.currentDB = dbName
	logger := s.logger
	if logger == nil {
		logger = s.newLogger()
	}
	s.logger = logger.WithField(ConnectionDbLogField, dbName)
}

func (s *BaseSession) UseDatabase(ctx *Context, db Database) error {
	// Nothing to do for default implementation
	// Integrators should override this method on custom session implementations as necessary
	return nil
}

// ID implements the Session interface.
func (s *BaseSession) ID() uint32 { return s.id }

// SetConnectionId sets the [id] for this session
func (s *BaseSession) SetConnectionId(id uint32) {
	s.id = id
	return
}

// Warn stores the warning in the session.
func (s *BaseSession) Warn(warn *Warning) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.warnings = append(s.warnings, warn)
}

// Warnings returns a copy of session warnings (from the most recent - the last one)
// The function implements sql.Session interface
func (s *BaseSession) Warnings() []*Warning {
	s.mu.RLock()
	defer s.mu.RUnlock()

	n := len(s.warnings)
	warns := make([]*Warning, n)
	for i := 0; i < n; i++ {
		warns[i] = s.warnings[n-i-1]
	}

	return warns
}

// ClearWarnings cleans up session warnings
func (s *BaseSession) ClearWarnings() {
	s.mu.Lock()
	defer s.mu.Unlock()

	cnt := uint16(len(s.warnings))
	if s.warncnt == cnt {
		if s.warnings != nil {
			s.warnings = s.warnings[:0]
		}
		s.warncnt = 0
	} else {
		s.warncnt = cnt
	}
}

// WarningCount returns a number of session warnings
func (s *BaseSession) WarningCount() uint16 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return uint16(len(s.warnings))
}

// AddLock adds a lock to the set of locks owned by this user which will need to be released if this session terminates
func (s *BaseSession) AddLock(lockName string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.locks[lockName] = true
	return nil
}

// DelLock removes a lock from the set of locks owned by this user
func (s *BaseSession) DelLock(lockName string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.locks, lockName)
	return nil
}

// IterLocks iterates through all locks owned by this user
func (s *BaseSession) IterLocks(cb func(name string) error) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for name := range s.locks {
		err := cb(name)

		if err != nil {
			return err
		}
	}

	return nil
}

// GetQueriedDatabase implements the Session interface.
func (s *BaseSession) GetQueriedDatabase() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.queriedDb
}

// SetQueriedDatabase implements the Session interface.
func (s *BaseSession) SetQueriedDatabase(dbName string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.queriedDb = dbName
}

func (s *BaseSession) GetIndexRegistry() *IndexRegistry {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.idxReg
}

func (s *BaseSession) GetViewRegistry() *ViewRegistry {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.viewReg
}

func (s *BaseSession) SetIndexRegistry(reg *IndexRegistry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.idxReg = reg
}

func (s *BaseSession) SetViewRegistry(reg *ViewRegistry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.viewReg = reg
}

func (s *BaseSession) SetLastQueryInfoInt(key string, value int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastQueryInfo[key] = value
}

func (s *BaseSession) GetLastQueryInfoInt(key string) int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	value, ok := s.lastQueryInfo[key].(int64)
	if !ok {
		panic(fmt.Sprintf("last query info value stored for %s is not an int64 value, but a %T", key, s.lastQueryInfo[key]))
	}
	return value
}

func (s *BaseSession) SetLastQueryInfoString(key string, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastQueryInfo[key] = value
}

func (s *BaseSession) GetLastQueryInfoString(key string) string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	value, ok := s.lastQueryInfo[key].(string)
	if !ok {
		panic(fmt.Sprintf("last query info value stored for %s is not a string value, but a %T", key, s.lastQueryInfo[key]))
	}
	return value
}

func (s *BaseSession) GetTransaction() Transaction {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.tx
}

func (s *BaseSession) SetTransaction(tx Transaction) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tx = tx
}

func (s *BaseSession) GetPrivilegeSet() (PrivilegeSet, uint64) {
	return s.privilegeSet, s.privSetCounter
}

func (s *BaseSession) SetPrivilegeSet(newPs PrivilegeSet, counter uint64) {
	s.privSetCounter = counter
	s.privilegeSet = newPs
}

// BaseSessionFromConnection is a SessionBuilder that returns a base session for the given connection and remote address
func BaseSessionFromConnection(ctx context.Context, c *mysql.Conn, addr string) (*BaseSession, error) {
	host := ""
	user := ""
	mysqlConnectionUser, ok := c.UserData.(MysqlConnectionUser)
	if ok {
		host = mysqlConnectionUser.Host
		user = mysqlConnectionUser.User
	}
	client := Client{Address: host, User: user, Capabilities: c.Capabilities}
	return NewBaseSessionWithClientServer(addr, client, c.ConnectionID), nil
}

// NewBaseSessionWithClientServer creates a new session with data.
func NewBaseSessionWithClientServer(server string, client Client, id uint32) *BaseSession {
	// TODO: if system variable "activate_all_roles_on_login" if set, activate all roles
	var systemVars map[string]SystemVarValue
	if SystemVariables != nil {
		systemVars = SystemVariables.NewSessionMap()
	} else {
		systemVars = make(map[string]SystemVarValue)
	}
	var statusVars map[string]StatusVarValue
	if StatusVariables != nil {
		statusVars = StatusVariables.NewSessionMap()
	} else {
		statusVars = make(map[string]StatusVarValue)
	}
	return &BaseSession{
		addr:           server,
		client:         client,
		id:             id,
		systemVars:     systemVars,
		statusVars:     statusVars,
		userVars:       NewUserVars(),
		idxReg:         NewIndexRegistry(),
		viewReg:        NewViewRegistry(),
		mu:             sync.RWMutex{},
		locks:          make(map[string]bool),
		lastQueryInfo:  defaultLastQueryInfo(),
		privSetCounter: 0,
	}
}

// NewBaseSession creates a new empty session.
func NewBaseSession() *BaseSession {
	// TODO: if system variable "activate_all_roles_on_login" if set, activate all roles
	var systemVars map[string]SystemVarValue
	if SystemVariables != nil {
		systemVars = SystemVariables.NewSessionMap()
	} else {
		systemVars = make(map[string]SystemVarValue)
	}
	var statusVars map[string]StatusVarValue
	if StatusVariables != nil {
		statusVars = StatusVariables.NewSessionMap()
	} else {
		statusVars = make(map[string]StatusVarValue)
	}
	return &BaseSession{
		id:             atomic.AddUint32(&autoSessionIDs, 1),
		systemVars:     systemVars,
		statusVars:     statusVars,
		userVars:       NewUserVars(),
		idxReg:         NewIndexRegistry(),
		viewReg:        NewViewRegistry(),
		mu:             sync.RWMutex{},
		locks:          make(map[string]bool),
		lastQueryInfo:  defaultLastQueryInfo(),
		privSetCounter: 0,
	}
}
