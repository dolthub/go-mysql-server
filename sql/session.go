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

package sql

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"
)

type key uint

const (
	// QueryKey to access query in the context.
	QueryKey key = iota
)

const (
	CurrentDBSessionVar              = "current_database"
	AutoCommitSessionVar             = "autocommit"
	characterSetConnectionSysVarName = "character_set_connection"
	characterSetResultsSysVarName    = "character_set_results"
	collationConnectionSysVarName    = "collation_connection"
)

var NoopTracer = trace.NewNoopTracerProvider().Tracer("github.com/dolthub/go-mysql-server/sql")
var _, noopSpan = NoopTracer.Start(context.Background(), "noop")

// Client holds session user information.
type Client struct {
	// User of the session.
	User string
	// Address of the client.
	Address string
	// Capabilities of the client
	Capabilities uint32
}

// Session holds the session data.
type Session interface {
	// Address of the server.
	Address() string
	// Client returns the user of the session.
	Client() Client
	// SetClient returns a new session with the given client.
	SetClient(Client)
	// SetSessionVariable sets the given system variable to the value given for this session.
	SetSessionVariable(ctx *Context, sysVarName string, value interface{}) error
	// InitSessionVariable sets the given system variable to the value given for this session and will allow for
	// initialization of readonly variables.
	InitSessionVariable(ctx *Context, sysVarName string, value interface{}) error
	// SetUserVariable sets the given user variable to the value given for this session, or creates it for this session.
	SetUserVariable(ctx *Context, varName string, value interface{}) error
	// GetSessionVariable returns this session's value of the system variable with the given name.
	GetSessionVariable(ctx *Context, sysVarName string) (interface{}, error)
	// GetUserVariable returns this session's value of the user variable with the given name, along with its most
	// appropriate type.
	GetUserVariable(ctx *Context, varName string) (Type, interface{}, error)
	// GetAllSessionVariables returns a copy of all session variable values.
	GetAllSessionVariables() map[string]interface{}
	// GetCurrentDatabase gets the current database for this session
	GetCurrentDatabase() string
	// SetCurrentDatabase sets the current database for this session
	SetCurrentDatabase(dbName string)
	// ID returns the unique ID of the connection.
	ID() uint32
	// Warn stores the warning in the session.
	Warn(warn *Warning)
	// Warnings returns a copy of session warnings (from the most recent).
	Warnings() []*Warning
	// ClearWarnings cleans up session warnings.
	ClearWarnings()
	// WarningCount returns a number of session warnings
	WarningCount() uint16
	// AddLock adds a lock to the set of locks owned by this user which will need to be released if this session terminates
	AddLock(lockName string) error
	// DelLock removes a lock from the set of locks owned by this user
	DelLock(lockName string) error
	// IterLocks iterates through all locks owned by this user
	IterLocks(cb func(name string) error) error
	// SetLastQueryInfo sets session-level query info for the key given, applying to the query just executed.
	SetLastQueryInfo(key string, value int64)
	// GetLastQueryInfo returns the session-level query info for the key given, for the query most recently executed.
	GetLastQueryInfo(key string) int64
	// GetTransaction returns the active transaction, if any
	GetTransaction() Transaction
	// SetTransaction sets the session's transaction
	SetTransaction(tx Transaction)
	// SetIgnoreAutoCommit instructs the session to ignore the value of the @@autocommit variable, or consider it again
	SetIgnoreAutoCommit(ignore bool)
	// GetIgnoreAutoCommit returns whether this session should ignore the @@autocommit variable
	GetIgnoreAutoCommit() bool
	// GetLogger returns the logger for this session, useful if clients want to log messages with the same format / output
	// as the running server. Clients should instantiate their own global logger with formatting options, and session
	// implementations should return the logger to be used for the running server.
	GetLogger() *logrus.Entry
	// SetLogger sets the logger to use for this session, which will always be an extension of the one returned by
	// GetLogger, extended with session information
	SetLogger(*logrus.Entry)
	// GetIndexRegistry returns the index registry for this session
	GetIndexRegistry() *IndexRegistry
	// GetViewRegistry returns the view registry for this session
	GetViewRegistry() *ViewRegistry
	// SetIndexRegistry sets the index registry for this session. Integrators should set an index registry in the event
	// they are using an index driver.
	SetIndexRegistry(*IndexRegistry)
	// SetViewRegistry sets the view registry for this session. Integrators should set a view registry if their database
	// doesn't implement ViewDatabase and they want views created to persist across sessions.
	SetViewRegistry(*ViewRegistry)
	// SetConnectionId sets this sessions unique ID
	SetConnectionId(connId uint32)
	// GetCharacterSet returns the character set for this session (defined by the system variable `character_set_connection`).
	GetCharacterSet() CharacterSetID
	// GetCharacterSetResults returns the result character set for this session (defined by the system variable `character_set_results`).
	GetCharacterSetResults() CharacterSetID
	// GetCollation returns the collation for this session (defined by the system variable `collation_connection`).
	GetCollation() CollationID
	// GetPrivilegeSet returns the cached privilege set associated with this session, along with its counter. The
	// PrivilegeSet is only valid when the counter is greater than zero.
	GetPrivilegeSet() (PrivilegeSet, uint64)
	// SetPrivilegeSet updates this session's cache with the given counter and privilege set. Setting the counter to a
	// value of zero will force the cache to reload. This is an internal function and is not intended to be used by
	// integrators.
	SetPrivilegeSet(newPs PrivilegeSet, counter uint64)
	// ValidateSession provides integrators a chance to do any custom validation of this session before any query is executed in it. For example, Dolt uses this hook to validate that the session's working set is valid.
	ValidateSession(ctx *Context, dbName string) error
	// SetTransactionDatabase is called when a transaction begins, and is set to the name of the database in scope for
	// that transaction. GetTransactionDatabase can be called by integrators to retrieve this database later, when it's
	// time to commit via TransactionSession.CommitTransaction. This supports implementations that can only support a
	// single database being modified per transaction.
	SetTransactionDatabase(dbName string)
	// GetTransactionDatabase returns the name of the database considered in scope when the current transaction began.
	GetTransactionDatabase() string
}

// PersistableSession supports serializing/deserializing global system variables/
type PersistableSession interface {
	Session
	// PersistGlobal writes to the persisted global system variables file
	PersistGlobal(sysVarName string, value interface{}) error
	// RemovePersistedGlobal deletes a variable from the persisted globals file
	RemovePersistedGlobal(sysVarName string) error
	// RemoveAllPersistedGlobals clears the contents of the persisted globals file
	RemoveAllPersistedGlobals() error
	// GetPersistedValue returns persisted value for a global system variable
	GetPersistedValue(k string) (interface{}, error)
}

// TransactionSession can BEGIN, ROLLBACK and COMMIT transactions, as well as create SAVEPOINTS and restore to them.
// Transactions can span multiple databases, and integrators must do their own error handling to prevent this if they
// cannot support multiple databases in a single transaction. Such integrators can use Session.GetTransactionDatabase
// to determine the database that was considered in scope when a transaction began.
type TransactionSession interface {
	Session
	// StartTransaction starts a new transaction and returns it
	StartTransaction(ctx *Context, tCharacteristic TransactionCharacteristic) (Transaction, error)
	// CommitTransaction commits the transaction given
	CommitTransaction(ctx *Context, tx Transaction) error
	// Rollback restores the database to the state recorded in the transaction given
	Rollback(ctx *Context, transaction Transaction) error
	// CreateSavepoint records a savepoint for the transaction given with the name given. If the name is already in use
	// for this transaction, the new savepoint replaces the old one.
	CreateSavepoint(ctx *Context, transaction Transaction, name string) error
	// RollbackToSavepoint restores the database to the state named by the savepoint
	RollbackToSavepoint(ctx *Context, transaction Transaction, name string) error
	// ReleaseSavepoint removes the savepoint named from the transaction given
	ReleaseSavepoint(ctx *Context, transaction Transaction, name string) error
}

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
	systemVars       map[string]interface{}
	userVars         map[string]interface{}
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
		m[k] = v
	}
	return m
}

// SetSessionVariable implements the Session interface.
func (s *BaseSession) SetSessionVariable(ctx *Context, sysVarName string, value interface{}) error {
	sysVar, _, ok := SystemVariables.GetGlobal(sysVarName)
	if !ok {
		return ErrUnknownSystemVariable.New(sysVarName)
	}
	if !sysVar.Dynamic {
		return ErrSystemVariableReadOnly.New(sysVarName)
	}
	return s.setSessVar(ctx, sysVar, sysVarName, value)
}

// InitSessionVariable implements the Session interface and is used to initialize variables (Including read-only variables)
func (s *BaseSession) InitSessionVariable(ctx *Context, sysVarName string, value interface{}) error {
	sysVar, _, ok := SystemVariables.GetGlobal(sysVarName)
	if !ok {
		return ErrUnknownSystemVariable.New(sysVarName)
	}

	val, ok := s.systemVars[sysVar.Name]
	if ok && val != sysVar.Default {
		return ErrSystemVariableReinitialized.New(sysVarName)
	}

	return s.setSessVar(ctx, sysVar, sysVarName, value)
}

func (s *BaseSession) setSessVar(ctx *Context, sysVar SystemVariable, sysVarName string, value interface{}) error {
	if sysVar.Scope == SystemVariableScope_Global {
		return ErrSystemVariableGlobalOnly.New(sysVarName)
	}
	convertedVal, err := sysVar.Type.Convert(value)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.systemVars[sysVar.Name] = convertedVal
	return nil
}

// SetUserVariable implements the Session interface.
func (s *BaseSession) SetUserVariable(ctx *Context, varName string, value interface{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.userVars[strings.ToLower(varName)] = value
	return nil
}

// GetSessionVariable implements the Session interface.
func (s *BaseSession) GetSessionVariable(ctx *Context, sysVarName string) (interface{}, error) {
	sysVar, _, ok := SystemVariables.GetGlobal(sysVarName)
	if !ok {
		return nil, ErrUnknownSystemVariable.New(sysVarName)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	val, ok := s.systemVars[strings.ToLower(sysVarName)]
	if !ok {
		s.systemVars[strings.ToLower(sysVarName)] = sysVar.Default
		val = sysVar.Default
	}
	if sysType, ok := sysVar.Type.(SetType); ok {
		if sv, ok := val.(uint64); ok {
			var err error
			val, err = sysType.BitsToString(sv)
			if err != nil {
				return nil, err
			}
		}
	}
	return val, nil
}

// GetUserVariable implements the Session interface.
func (s *BaseSession) GetUserVariable(ctx *Context, varName string) (Type, interface{}, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	val, ok := s.userVars[strings.ToLower(varName)]
	if !ok {
		return Null, nil, nil
	}
	return ApproximateTypeFromValue(val), val, nil
}

// GetCharacterSet returns the character set for this session (defined by the system variable `character_set_connection`).
func (s *BaseSession) GetCharacterSet() CharacterSetID {
	s.mu.RLock()
	defer s.mu.RUnlock()
	val, _ := s.systemVars[characterSetConnectionSysVarName]
	if val == nil {
		return CharacterSet_Unspecified
	}
	charSet, err := ParseCharacterSet(val.(string))
	if err != nil {
		panic(err) // shouldn't happen
	}
	return charSet
}

// GetCharacterSetResults returns the result character set for this session (defined by the system variable `character_set_results`).
func (s *BaseSession) GetCharacterSetResults() CharacterSetID {
	s.mu.RLock()
	defer s.mu.RUnlock()
	val, _ := s.systemVars[characterSetResultsSysVarName]
	if val == nil {
		return CharacterSet_Unspecified
	}
	charSet, err := ParseCharacterSet(val.(string))
	if err != nil {
		panic(err) // shouldn't happen
	}
	return charSet
}

// GetCollation returns the collation for this session (defined by the system variable `collation_connection`).
func (s *BaseSession) GetCollation() CollationID {
	s.mu.Lock()
	defer s.mu.Unlock()
	val, _ := s.systemVars[collationConnectionSysVarName]
	if val == nil {
		return Collation_Unspecified
	}
	valStr := val.(string)
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

type (
	// TypedValue is a value along with its type.
	TypedValue struct {
		Typ   Type
		Value interface{}
	}

	// Warning stands for mySQL warning record.
	Warning struct {
		Level   string
		Message string
		Code    int
	}
)

const (
	RowCount     = "row_count"
	FoundRows    = "found_rows"
	LastInsertId = "last_insert_id"
)

func defaultLastQueryInfo() map[string]int64 {
	return map[string]int64{
		RowCount:     0,
		FoundRows:    1, // this is kind of a hack -- it handles the case of `select found_rows()` before any select statement is issued
		LastInsertId: 0,
	}
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

// cc: https://dev.mysql.com/doc/refman/8.0/en/temporary-files.html
func GetTmpdirSessionVar() string {
	ret := os.Getenv("TMPDIR")
	if ret != "" {
		return ret
	}

	ret = os.Getenv("TEMP")
	if ret != "" {
		return ret
	}

	ret = os.Getenv("TMP")
	if ret != "" {
		return ret
	}

	return ""
}

// HasDefaultValue checks if session variable value is the default one.
func HasDefaultValue(ctx *Context, s Session, key string) (bool, interface{}) {
	val, err := s.GetSessionVariable(ctx, key)
	if err == nil {
		sysVar, _, ok := SystemVariables.GetGlobal(key)
		if ok {
			return sysVar.Default == val, val
		}
	}
	return true, nil
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
	//TODO: if system variable "activate_all_roles_on_login" if set, activate all roles
	return &BaseSession{
		addr:           server,
		client:         client,
		id:             id,
		systemVars:     SystemVariables.NewSessionMap(),
		userVars:       make(map[string]interface{}),
		idxReg:         NewIndexRegistry(),
		viewReg:        NewViewRegistry(),
		mu:             sync.RWMutex{},
		locks:          make(map[string]bool),
		lastQueryInfo:  defaultLastQueryInfo(),
		privSetCounter: 0,
	}
}

// Session ID 0 used as invalid SessionID
var autoSessionIDs uint32 = 1

// NewBaseSession creates a new empty session.
func NewBaseSession() *BaseSession {
	//TODO: if system variable "activate_all_roles_on_login" if set, activate all roles
	return &BaseSession{
		id:             atomic.AddUint32(&autoSessionIDs, 1),
		systemVars:     SystemVariables.NewSessionMap(),
		userVars:       make(map[string]interface{}),
		idxReg:         NewIndexRegistry(),
		viewReg:        NewViewRegistry(),
		mu:             sync.RWMutex{},
		locks:          make(map[string]bool),
		lastQueryInfo:  defaultLastQueryInfo(),
		privSetCounter: 0,
	}
}

// Context of the query execution.
type Context struct {
	context.Context
	Session
	Memory      *MemoryManager
	ProcessList ProcessList
	services    Services
	pid         uint64
	query       string
	queryTime   time.Time
	tracer      trace.Tracer
	rootSpan    trace.Span
}

// ContextOption is a function to configure the context.
type ContextOption func(*Context)

// WithSession adds the given session to the context.
func WithSession(s Session) ContextOption {
	return func(ctx *Context) {
		ctx.Session = s
	}
}

// WithTracer adds the given tracer to the context.
func WithTracer(t trace.Tracer) ContextOption {
	return func(ctx *Context) {
		ctx.tracer = t
	}
}

// WithPid adds the given pid to the context.
func WithPid(pid uint64) ContextOption {
	return func(ctx *Context) {
		ctx.pid = pid
	}
}

// WithQuery adds the given query to the context.
func WithQuery(q string) ContextOption {
	return func(ctx *Context) {
		ctx.query = q
	}
}

// WithMemoryManager adds the given memory manager to the context.
func WithMemoryManager(m *MemoryManager) ContextOption {
	return func(ctx *Context) {
		ctx.Memory = m
	}
}

// WithRootSpan sets the root span of the context.
func WithRootSpan(s trace.Span) ContextOption {
	return func(ctx *Context) {
		ctx.rootSpan = s
	}
}

func WithProcessList(p ProcessList) ContextOption {
	return func(ctx *Context) {
		ctx.ProcessList = p
	}
}

// WithServices sets the services for the Context
func WithServices(services Services) ContextOption {
	return func(ctx *Context) {
		ctx.services = services
	}
}

var ctxNowFunc = time.Now
var ctxNowFuncMutex = &sync.Mutex{}

func RunWithNowFunc(nowFunc func() time.Time, fn func() error) error {
	ctxNowFuncMutex.Lock()
	defer ctxNowFuncMutex.Unlock()

	initialNow := ctxNowFunc
	ctxNowFunc = nowFunc
	defer func() {
		ctxNowFunc = initialNow
	}()

	return fn()
}

// NewContext creates a new query context. Options can be passed to configure
// the context. If some aspect of the context is not configure, the default
// value will be used.
// By default, the context will have an empty base session, a noop tracer and
// a memory manager using the process reporter.
func NewContext(
	ctx context.Context,
	opts ...ContextOption,
) *Context {
	c := &Context{
		Context:   ctx,
		Session:   nil,
		queryTime: ctxNowFunc(),
		tracer:    NoopTracer,
	}
	for _, opt := range opts {
		opt(c)
	}

	if c.Memory == nil {
		c.Memory = NewMemoryManager(ProcessMemory)
	}
	if c.ProcessList == nil {
		c.ProcessList = EmptyProcessList{}
	}
	if c.Session == nil {
		c.Session = NewBaseSession()
	}

	return c
}

// Applys the options given to the context. Mostly for tests, not safe for use after construction of the context.
func (c *Context) ApplyOpts(opts ...ContextOption) {
	for _, opt := range opts {
		opt(c)
	}
}

// NewEmptyContext returns a default context with default values.
func NewEmptyContext() *Context { return NewContext(context.TODO()) }

// Pid returns the process id associated with this context.
func (c *Context) Pid() uint64 { return c.pid }

// Query returns the query string associated with this context.
func (c *Context) Query() string { return c.query }

func (c Context) WithQuery(q string) *Context {
	c.query = q
	return &c
}

// QueryTime returns the time.Time when the context associated with this query was created
func (c *Context) QueryTime() time.Time {
	return c.queryTime
}

// Span creates a new tracing span with the given context.
// It will return the span and a new context that should be passed to all
// children of this span.
func (c *Context) Span(
	opName string,
	opts ...trace.SpanStartOption,
) (trace.Span, *Context) {
	if c.tracer == NoopTracer {
		return noopSpan, c
	}

	ctx, span := c.tracer.Start(c.Context, opName, opts...)
	return span, c.WithContext(ctx)
}

// NewSubContext creates a new sub-context with the current context as parent. Returns the resulting context.CancelFunc
// as well as the new *sql.Context, which be used to cancel the new context before the parent is finished.
func (c *Context) NewSubContext() (*Context, context.CancelFunc) {
	ctx, cancelFunc := context.WithCancel(c.Context)

	return c.WithContext(ctx), cancelFunc
}

func (c *Context) WithCurrentDB(db string) *Context {
	c.SetCurrentDatabase(db)
	return c
}

// WithContext returns a new context with the given underlying context.
func (c *Context) WithContext(ctx context.Context) *Context {
	nc := *c
	nc.Context = ctx
	return &nc
}

// RootSpan returns the root span, if any.
func (c *Context) RootSpan() trace.Span {
	return c.rootSpan
}

// Error adds an error as warning to the session.
func (c *Context) Error(code int, msg string, args ...interface{}) {
	c.Session.Warn(&Warning{
		Level:   "Error",
		Code:    code,
		Message: fmt.Sprintf(msg, args...),
	})
}

// Warn adds a warning to the session.
func (c *Context) Warn(code int, msg string, args ...interface{}) {
	c.Session.Warn(&Warning{
		Level:   "Warning",
		Code:    code,
		Message: fmt.Sprintf(msg, args...),
	})
}

// Terminate the connection associated with |connID|.
func (c *Context) KillConnection(connID uint32) error {
	if c.services.KillConnection != nil {
		return c.services.KillConnection(connID)
	}
	return nil
}

// Load the remote file |filename| from the client. Returns a |ReadCloser| for
// the file's contents. Returns an error if this functionality is not
// supported.
func (c *Context) LoadInfile(filename string) (io.ReadCloser, error) {
	if c.services.LoadInfile != nil {
		return c.services.LoadInfile(filename)
	}
	return nil, ErrUnsupportedFeature.New("LOAD DATA LOCAL INFILE ...")
}

func (c *Context) NewErrgroup() (*errgroup.Group, *Context) {
	eg, egCtx := errgroup.WithContext(c.Context)
	return eg, c.WithContext(egCtx)
}

// NewCtxWithClient returns a new Context with the given [client]
func (c *Context) NewCtxWithClient(client Client) *Context {
	nc := *c
	nc.Session.SetClient(client)
	nc.Session.SetPrivilegeSet(nil, 0)
	return &nc
}

// Services are handles to optional or plugin functionality that can be
// used by the SQL implementation in certain situations. An integrator can set
// methods on Services for a given *Context and different parts of go-mysql-server
// will inspect it in order to fulfill their implementations. Currently, the
// KillConnection service is available. Set these with |WithServices|; the
// implementation will access them through the corresponding methods on
// *Context, such as |KillConnection|.
type Services struct {
	KillConnection func(connID uint32) error
	LoadInfile     func(filename string) (io.ReadCloser, error)
}

// NewSpanIter creates a RowIter executed in the given span.
// Currently inactive, returns the iter returned unaltered.
func NewSpanIter(span trace.Span, iter RowIter) RowIter {
	// In the default, non traced case, we should not bother with
	// collecting the timings below.
	if !span.IsRecording() {
		return iter
	} else {
		var iter2 RowIter2
		iter2, _ = iter.(RowIter2)
		return &spanIter{
			span:  span,
			iter:  iter,
			iter2: iter2,
		}
	}
}

type spanIter struct {
	span  trace.Span
	iter  RowIter
	iter2 RowIter2
	count int
	max   time.Duration
	min   time.Duration
	total time.Duration
	done  bool
}

var _ RowIter = (*spanIter)(nil)
var _ RowIter2 = (*spanIter)(nil)

func (i *spanIter) updateTimings(start time.Time) {
	elapsed := time.Since(start)
	if i.max < elapsed {
		i.max = elapsed
	}

	if i.min > elapsed || i.min == 0 {
		i.min = elapsed
	}

	i.total += elapsed
}

func (i *spanIter) Next(ctx *Context) (Row, error) {
	start := time.Now()

	row, err := i.iter.Next(ctx)
	if err == io.EOF {
		i.finish()
		return nil, err
	}

	if err != nil {
		i.finishWithError(err)
		return nil, err
	}

	i.count++
	i.updateTimings(start)
	return row, nil
}

func (i *spanIter) Next2(ctx *Context, frame *RowFrame) error {
	start := time.Now()

	err := i.iter2.Next2(ctx, frame)
	if err == io.EOF {
		i.finish()
		return err
	}

	if err != nil {
		i.finishWithError(err)
		return err
	}

	i.count++
	i.updateTimings(start)
	return nil
}

func (i *spanIter) finish() {
	var avg time.Duration
	if i.count > 0 {
		avg = i.total / time.Duration(i.count)
	}

	i.span.AddEvent("finish", trace.WithAttributes(
		attribute.Int("rows", i.count),
		attribute.Stringer("total_time", i.total),
		attribute.Stringer("max_time", i.max),
		attribute.Stringer("min_time", i.min),
		attribute.Stringer("avg_time", avg),
	))
	i.span.End()
	i.done = true
}

func (i *spanIter) finishWithError(err error) {
	var avg time.Duration
	if i.count > 0 {
		avg = i.total / time.Duration(i.count)
	}

	i.span.RecordError(err)
	i.span.AddEvent("finish", trace.WithAttributes(
		attribute.Int("rows", i.count),
		attribute.Stringer("total_time", i.total),
		attribute.Stringer("max_time", i.max),
		attribute.Stringer("min_time", i.min),
		attribute.Stringer("avg_time", avg),
	))
	i.span.End()
	i.done = true
}

func (i *spanIter) Close(ctx *Context) error {
	if !i.done {
		i.finish()
	}
	return i.iter.Close(ctx)
}
