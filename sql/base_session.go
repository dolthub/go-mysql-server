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
	"strings"
	"sync"
	"sync/atomic"

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
	userVars         SessionUserVariables
	idxReg           *IndexRegistry
	viewReg          *ViewRegistry
	warnings         []*Warning
	warncnt          uint16
	locks            map[string]bool
	queriedDb        string
	lastQueryInfo    map[string]int64
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
		log := logrus.StandardLogger()
		s.logger = logrus.NewEntry(log)
	}
	return s.logger
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
			return s.setSessVar(ctx, sv, value)
		} else {
			return ErrUnknownSystemVariable.New(sysVarName)
		}
	}

	if !sysVar.Var.Dynamic {
		return ErrSystemVariableReadOnly.New(sysVarName)
	}
	return s.setSessVar(ctx, sysVar.Var, value)
}

// InitSessionVariable implements the Session interface and is used to initialize variables (Including read-only variables)
func (s *BaseSession) InitSessionVariable(ctx *Context, sysVarName string, value interface{}) error {
	sysVar, _, ok := SystemVariables.GetGlobal(sysVarName)
	if !ok {
		return ErrUnknownSystemVariable.New(sysVarName)
	}

	val, ok := s.systemVars[sysVar.Name]
	if ok && val.Val != sysVar.Default {
		return ErrSystemVariableReinitialized.New(sysVarName)
	}

	return s.setSessVar(ctx, sysVar, value)
}

func (s *BaseSession) setSessVar(ctx *Context, sysVar SystemVariable, value interface{}) error {
	if sysVar.Scope == SystemVariableScope_Global {
		return ErrSystemVariableGlobalOnly.New(sysVar.Name)
	}
	convertedVal, err := sysVar.Type.Convert(value)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.systemVars[sysVar.Name] = SystemVarValue{
		Var: sysVar,
		Val: convertedVal,
	}
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
	if sysType, ok := sysVar.Var.Type.(SetType); ok {
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
	collation, err := ParseCollation(nil, &valStr, false)
	if err != nil {
		panic(err) // shouldn't happen
	}
	return collation
}

// ValidateSession provides integrators a chance to do any custom validation of this session before any query is executed in it.
func (s *BaseSession) ValidateSession(ctx *Context, dbName string) error {
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

func (s *BaseSession) SetLastQueryInfo(key string, value int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastQueryInfo[key] = value
}

func (s *BaseSession) GetLastQueryInfo(key string) int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lastQueryInfo[key]
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

// NewBaseSessionWithClientServer creates a new session with data.
func NewBaseSessionWithClientServer(server string, client Client, id uint32) *BaseSession {
	// TODO: if system variable "activate_all_roles_on_login" if set, activate all roles
	var sessionVars map[string]SystemVarValue
	if SystemVariables != nil {
		sessionVars = SystemVariables.NewSessionMap()
	} else {
		sessionVars = make(map[string]SystemVarValue)
	}
	return &BaseSession{
		addr:           server,
		client:         client,
		id:             id,
		systemVars:     sessionVars,
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
	var sessionVars map[string]SystemVarValue
	if SystemVariables != nil {
		sessionVars = SystemVariables.NewSessionMap()
	} else {
		sessionVars = make(map[string]SystemVarValue)
	}
	return &BaseSession{
		id:             atomic.AddUint32(&autoSessionIDs, 1),
		systemVars:     sessionVars,
		userVars:       NewUserVars(),
		idxReg:         NewIndexRegistry(),
		viewReg:        NewViewRegistry(),
		mu:             sync.RWMutex{},
		locks:          make(map[string]bool),
		lastQueryInfo:  defaultLastQueryInfo(),
		privSetCounter: 0,
	}
}
