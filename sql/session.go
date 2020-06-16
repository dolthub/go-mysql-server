package sql

import (
	"context"
	"fmt"
	"io"
	"math"
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
	CurrentDBSessionVar = "current_database"
	AutoCommitSessionVar = "autocommit"
)

// Client holds session user information.
type Client struct {
	// User of the session.
	User string
	// Address of the client.
	Address string
}

// Session holds the session data.
type Session interface {
	// Address of the server.
	Address() string
	// User of the session.
	Client() Client
	// Set session configuration.
	Set(ctx context.Context, key string, typ Type, value interface{}) error
	// Get session configuration.
	Get(key string) (Type, interface{})
	// GetCurrentDatabase gets the current database for this session
	GetCurrentDatabase() string
	// SetDefaultDatabase sets the current database for this session
	SetCurrentDatabase(dbName string)
	// CommitTransaction commits the current transaction for this session for the current database
	CommitTransaction(*Context) error
	// GetAll returns a copy of session configuration
	GetAll() map[string]TypedValue
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
}

// BaseSession is the basic session type.
type BaseSession struct {
	id        uint32
	addr      string
	currentDB string
	client    Client
	mu        sync.RWMutex
	config    map[string]TypedValue
	warnings  []*Warning
	warncnt   uint16
}

// CommitTransaction commits the current transaction for the current database.
func (s *BaseSession) CommitTransaction(*Context) error {
	// no-op on BaseSession
	return nil
}

// Address returns the server address.
func (s *BaseSession) Address() string { return s.addr }

// Client returns session's client information.
func (s *BaseSession) Client() Client { return s.client }

// Set implements the Session interface.
func (s *BaseSession) Set(ctx context.Context, key string, typ Type, value interface{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.config[key] = TypedValue{typ, value}
	return nil
}

// Get implements the Session interface.
func (s *BaseSession) Get(key string) (Type, interface{}) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.config[key]
	if !ok {
		return Null, nil
	}

	return v.Typ, v.Value
}

// GetAll returns a copy of session configuration
func (s *BaseSession) GetAll() map[string]TypedValue {
	m := make(map[string]TypedValue)
	s.mu.RLock()
	defer s.mu.RUnlock()

	for k, v := range s.config {
		m[k] = v
	}
	return m
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

// DefaultSessionConfig returns default values for session variables
func DefaultSessionConfig() map[string]TypedValue {
	return map[string]TypedValue{
		"auto_increment_increment": TypedValue{Int64, int64(1)},
		"time_zone":                TypedValue{LongText, "SYSTEM"},
		"system_time_zone":         TypedValue{LongText, time.Now().UTC().Location().String()},
		"max_allowed_packet":       TypedValue{Int32, math.MaxInt32},
		"sql_mode":                 TypedValue{LongText, ""},
		"gtid_mode":                TypedValue{Int32, int32(0)},
		"collation_database":       TypedValue{LongText, "utf8_bin"},
		"ndbinfo_version":          TypedValue{LongText, ""},
		"sql_select_limit":         TypedValue{Int32, math.MaxInt32},
		"transaction_isolation":    TypedValue{LongText, "READ UNCOMMITTED"},
		"version":                  TypedValue{LongText, ""},
		"version_comment":          TypedValue{LongText, ""},
	}
}

// HasDefaultValue checks if session variable value is the default one.
func HasDefaultValue(s Session, key string) (bool, interface{}) {
	typ, val := s.Get(key)
	if cfg, ok := DefaultSessionConfig()[key]; ok {
		return (cfg.Typ == typ && cfg.Value == val), val
	}
	return false, val
}

// NewSession creates a new session with data.
func NewSession(server, client, user string, id uint32) Session {
	return &BaseSession{
		id:   id,
		addr: server,
		client: Client{
			Address: client,
			User:    user,
		},
		config: DefaultSessionConfig(),
	}
}

var autoSessionIDs uint32

// NewBaseSession creates a new empty session.
func NewBaseSession() Session {
	return &BaseSession{id: atomic.AddUint32(&autoSessionIDs, 1), config: DefaultSessionConfig()}
}

// Context of the query execution.
type Context struct {
	context.Context
	Session
	*IndexRegistry
	*ViewRegistry
	Memory   *MemoryManager
	pid      uint64
	query    string
	queryTime time.Time
	tracer   opentracing.Tracer
	rootSpan opentracing.Span
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
// childrens of this span.
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

	return span, &Context{ctx, c.Session, c.IndexRegistry, c.ViewRegistry, c.Memory, c.Pid(), c.Query(), c.queryTime, c.tracer, c.rootSpan}
}

func (c *Context) WithCurrentDB(db string) *Context {
	c.SetCurrentDatabase(db)
	return c
}

// WithContext returns a new context with the given underlying context.
func (c *Context) WithContext(ctx context.Context) *Context {
	return &Context{ctx, c.Session, c.IndexRegistry, c.ViewRegistry, c.Memory, c.Pid(), c.Query(), c.queryTime, c.tracer, c.rootSpan}
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
func NewSpanIter(span opentracing.Span, iter RowIter) RowIter {
	return &spanIter{
		span: span,
		iter: iter,
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

func (i *spanIter) Close() error {
	if !i.done {
		i.finish()
	}
	return i.iter.Close()
}
