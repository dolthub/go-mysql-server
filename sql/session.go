package sql

import (
	"context"
	"io"
	"sync"
	"time"

	opentracing "github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/log"
)

type key uint

const (
	// QueryKey to access query in the context.
	QueryKey key = iota
)

// Session holds the session data.
type Session interface {
	// Address of the server.
	Address() string
	// User of the session.
	User() string
	// Set session configuration.
	Set(key string, typ Type, value interface{})
	// Get session configuration.
	Get(key string) (Type, interface{})
}

// BaseSession is the basic session type.
type BaseSession struct {
	addr   string
	user   string
	mu     sync.RWMutex
	config map[string]typedValue
}

// User returns the current user of the session.
func (s *BaseSession) User() string { return s.user }

// Address returns the server address.
func (s *BaseSession) Address() string { return s.addr }

// Set implements the Session interface.
func (s *BaseSession) Set(key string, typ Type, value interface{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.config[key] = typedValue{typ, value}
}

// Get implements the Session interface.
func (s *BaseSession) Get(key string) (Type, interface{}) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.config[key]
	if !ok {
		return Null, nil
	}

	return v.typ, v.value
}

type typedValue struct {
	typ   Type
	value interface{}
}

// NewSession creates a new session with data.
func NewSession(address string, user string) Session {
	return &BaseSession{
		addr:   address,
		user:   user,
		config: make(map[string]typedValue),
	}
}

// NewBaseSession creates a new empty session.
func NewBaseSession() Session {
	return &BaseSession{config: make(map[string]typedValue)}
}

// Context of the query execution.
type Context struct {
	context.Context
	Session
	tracer opentracing.Tracer
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
func WithTracer(t opentracing.Tracer) ContextOption {
	return func(ctx *Context) {
		ctx.tracer = t
	}
}

// NewContext creates a new query context. Options can be passed to configure
// the context. If some aspect of the context is not configure, the default
// value will be used.
// By default, the context will have an empty base session and a noop tracer.
func NewContext(
	ctx context.Context,
	opts ...ContextOption,
) *Context {
	c := &Context{ctx, NewBaseSession(), opentracing.NoopTracer{}}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// NewEmptyContext returns a default context with default values.
func NewEmptyContext() *Context { return NewContext(context.TODO()) }

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

	return span, &Context{ctx, c.Session, c.tracer}
}

// WithQuery adds the query to the context.
func (c *Context) WithQuery(query string) *Context {
	nc := *c
	nc.Context = context.WithValue(c.Context, QueryKey, query)
	return &nc
}

// NewSpanIter creates a RowIter executed in the given span.
func NewSpanIter(span opentracing.Span, iter RowIter) RowIter {
	return &spanIter{span, iter, 0, false}
}

type spanIter struct {
	span  opentracing.Span
	iter  RowIter
	count int
	done  bool
}

func (i *spanIter) Next() (Row, error) {
	if i.done {
		return nil, io.EOF
	}

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
	return row, nil
}

func (i *spanIter) finish() {
	i.span.FinishWithOptions(opentracing.FinishOptions{
		LogRecords: []opentracing.LogRecord{
			{
				Timestamp: time.Now(),
				Fields:    []log.Field{log.Int("rows", i.count)},
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
