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

	opentracing "github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/log"
)

type key uint

const (
	// QueryKey to access query in the context.
	QueryKey key = iota
)

const (
	CurrentDBSessionVar  = "current_database"
	AutoCommitSessionVar = "autocommit"
)

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
	// SetSessionVariable sets the given system variable to the value given for this session.
	SetSessionVariable(ctx *Context, sysVarName string, value interface{}) error
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
	// CommitTransaction commits the current transaction for this session for the current database
	CommitTransaction(ctx *Context, dbName string, transaction Transaction) error
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
}

// BaseSession is the basic session type.
type BaseSession struct {
	id               uint32
	addr             string
	currentDB        string
	client           Client
	mu               *sync.RWMutex
	systemVars       map[string]interface{}
	userVars         map[string]interface{}
	warnings         []*Warning
	warncnt          uint16
	locks            map[string]bool
	queriedDb        string
	lastQueryInfo    map[string]int64
	tx               Transaction
	ignoreAutocommit bool
}

func (s *BaseSession) SetIgnoreAutoCommit(ignore bool) {
	s.ignoreAutocommit = ignore
}

func (s *BaseSession) GetIgnoreAutoCommit() bool {
	return s.ignoreAutocommit
}

var _ Session = (*BaseSession)(nil)

// CommitTransaction commits the current transaction for the current database.
func (s *BaseSession) CommitTransaction(*Context, string, Transaction) error {
	// no-op on BaseSession
	return nil
}

// Address returns the server address.
func (s *BaseSession) Address() string { return s.addr }

// Client returns session's client information.
func (s *BaseSession) Client() Client { return s.client }

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
	if sysVar.Scope == SystemVariableScope_Global {
		return ErrSystemVariableGlobalOnly.New(sysVarName)
	}
	if !sysVar.Dynamic {
		return ErrSystemVariableReadOnly.New(sysVarName)
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

// GetCurrentDatabase gets the current database for this session
func (s *BaseSession) GetCurrentDatabase() string {
	return s.currentDB
}

// SetCurrentDatabase sets the current database for this session
func (s *BaseSession) SetCurrentDatabase(dbName string) {
	s.currentDB = dbName
}

// ID implements the Session interface.
func (s *BaseSession) ID() uint32 { return s.id }

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
	return s.queriedDb
}

// SetQueriedDatabase implements the Session interface.
func (s *BaseSession) SetQueriedDatabase(dbName string) {
	s.queriedDb = dbName
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
	return s.tx
}

func (s *BaseSession) SetTransaction(tx Transaction) {
	s.tx = tx
}

// NewSession creates a new session with data.
func NewSession(server string, client Client, id uint32) Session {
	return &BaseSession{
		addr:          server,
		client:        client,
		id:            id,
		systemVars:    SystemVariables.NewSessionMap(),
		userVars:      make(map[string]interface{}),
		mu:            &sync.RWMutex{},
		locks:         make(map[string]bool),
		lastQueryInfo: defaultLastQueryInfo(),
	}
}

// Session ID 0 used as invalid SessionID
var autoSessionIDs uint32 = 1

// NewBaseSession creates a new empty session.
func NewBaseSession() Session {
	return &BaseSession{
		id:            atomic.AddUint32(&autoSessionIDs, 1),
		systemVars:    SystemVariables.NewSessionMap(),
		userVars:      make(map[string]interface{}),
		mu:            &sync.RWMutex{},
		locks:         make(map[string]bool),
		lastQueryInfo: defaultLastQueryInfo(),
	}
}

// Context of the query execution.
type Context struct {
	context.Context
	Session
	*IndexRegistry
	*ViewRegistry
	Memory    *MemoryManager
	pid       uint64
	query     string
	queryTime time.Time
	tracer    opentracing.Tracer
	rootSpan  opentracing.Span
}

// ContextOption is a function to configure the context.
type ContextOption func(*Context)

// WithSession adds the given session to the context.
func WithSession(s Session) ContextOption {
	return func(ctx *Context) {
		ctx.Session = s
	}
}

func WithIndexRegistry(ir *IndexRegistry) ContextOption {
	return func(ctx *Context) {
		ctx.IndexRegistry = ir
	}
}

func WithViewRegistry(vr *ViewRegistry) ContextOption {
	return func(ctx *Context) {
		ctx.ViewRegistry = vr
	}
}

// WithTracer adds the given tracer to the context.
func WithTracer(t opentracing.Tracer) ContextOption {
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
func WithRootSpan(s opentracing.Span) ContextOption {
	return func(ctx *Context) {
		ctx.rootSpan = s
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
	c := &Context{ctx, NewBaseSession(), nil, nil, nil, 0, "", ctxNowFunc(), opentracing.NoopTracer{}, nil}
	for _, opt := range opts {
		opt(c)
	}

	if c.IndexRegistry == nil {
		c.IndexRegistry = NewIndexRegistry()
	}

	if c.ViewRegistry == nil {
		c.ViewRegistry = NewViewRegistry()
	}

	if c.Memory == nil {
		c.Memory = NewMemoryManager(ProcessMemory)
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

// QueryTime returns the time.Time when the context associated with this query was created
func (c *Context) QueryTime() time.Time {
	return c.queryTime
}

// Span creates a new tracing span with the given context.
// It will return the span and a new context that should be passed to all
// children of this span.
func (c *Context) Span(
	opName string,
	opts ...opentracing.StartSpanOption,
) (opentracing.Span, *Context) {
	parentSpan := opentracing.SpanFromContext(c.Context)
	if parentSpan != nil {
		opts = append(opts, opentracing.ChildOf(parentSpan.Context()))
	}
	span := c.tracer.StartSpan(opName, opts...)
	ctx := opentracing.ContextWithSpan(c.Context, span)

	return span, &Context{
		Context:       ctx,
		Session:       c.Session,
		IndexRegistry: c.IndexRegistry,
		ViewRegistry:  c.ViewRegistry,
		Memory:        c.Memory,
		pid:           c.Pid(),
		query:         c.Query(),
		queryTime:     c.queryTime,
		tracer:        c.tracer,
		rootSpan:      c.rootSpan,
	}
}

// NewSubContext creates a new sub-context with the current context as parent. Returns the resulting context.CancelFunc
// as well as the new *sql.Context, which be used to cancel the new context before the parent is finished.
func (c *Context) NewSubContext() (*Context, context.CancelFunc) {
	ctx, cancelFunc := context.WithCancel(c.Context)
	return &Context{
		Context:       ctx,
		Session:       c.Session,
		IndexRegistry: c.IndexRegistry,
		ViewRegistry:  c.ViewRegistry,
		Memory:        c.Memory,
		pid:           c.Pid(),
		query:         c.Query(),
		queryTime:     c.queryTime,
		tracer:        c.tracer,
		rootSpan:      c.rootSpan,
	}, cancelFunc
}

func (c *Context) WithCurrentDB(db string) *Context {
	c.SetCurrentDatabase(db)
	return c
}

// WithContext returns a new context with the given underlying context.
func (c *Context) WithContext(ctx context.Context) *Context {
	return &Context{
		Context:       ctx,
		Session:       c.Session,
		IndexRegistry: c.IndexRegistry,
		ViewRegistry:  c.ViewRegistry,
		Memory:        c.Memory,
		pid:           c.Pid(),
		query:         c.Query(),
		queryTime:     c.queryTime,
		tracer:        c.tracer,
		rootSpan:      c.rootSpan,
	}
}

// RootSpan returns the root span, if any.
func (c *Context) RootSpan() opentracing.Span {
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

// NewSpanIter creates a RowIter executed in the given span.
// Currently inactive, returns the iter returned unaltered.
func NewSpanIter(span opentracing.Span, iter RowIter) RowIter {
	// In the default, non traced case, we should not bother with
	// collecting the timings below.
	if (span.Tracer() == opentracing.NoopTracer{}) {
		return iter
	} else {
		return &spanIter{
			span: span,
			iter: iter,
		}
	}
}

type spanIter struct {
	span  opentracing.Span
	iter  RowIter
	count int
	max   time.Duration
	min   time.Duration
	total time.Duration
	done  bool
}

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

func (i *spanIter) Next() (Row, error) {
	start := time.Now()

	row, err := i.iter.Next()
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

func (i *spanIter) finish() {
	var avg time.Duration
	if i.count > 0 {
		avg = i.total / time.Duration(i.count)
	}

	i.span.FinishWithOptions(opentracing.FinishOptions{
		LogRecords: []opentracing.LogRecord{
			{
				Timestamp: time.Now(),
				Fields: []log.Field{
					log.Int("rows", i.count),
					log.String("total_time", i.total.String()),
					log.String("max_time", i.max.String()),
					log.String("min_time", i.min.String()),
					log.String("avg_time", avg.String()),
				},
			},
		},
	})
	i.done = true
}

func (i *spanIter) finishWithError(err error) {
	i.span.FinishWithOptions(opentracing.FinishOptions{
		LogRecords: []opentracing.LogRecord{
			{
				Timestamp: time.Now(),
				Fields:    []log.Field{log.String("error", err.Error())},
			},
		},
	})
	i.done = true
}

func (i *spanIter) Close(ctx *Context) error {
	if !i.done {
		i.finish()
	}
	return i.iter.Close(ctx)
}
