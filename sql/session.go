package sql

import (
	"context"

	opentracing "github.com/opentracing/opentracing-go"
)

// Session holds the session data.
type Session interface {
	// TODO: add config
}

// BaseSession is the basic session type.
type BaseSession struct {
	// TODO: add config
}

// NewBaseSession creates a new basic session.
func NewBaseSession() Session {
	return &BaseSession{}
}

// Context of the query execution.
type Context struct {
	context.Context
	Session
	tracer opentracing.Tracer
}

// NewContext creates a new query context.
func NewContext(
	ctx context.Context,
	session Session,
	tracer opentracing.Tracer,
) *Context {
	return &Context{ctx, session, tracer}
}

// NewEmptyContext create a new context that is completely empty, useful for
// testing.
func NewEmptyContext() *Context {
	return &Context{
		context.TODO(),
		NewBaseSession(),
		opentracing.NoopTracer{},
	}
}

// Span creates a new tracing span with the given context.
// It will return the span and a new context that should be passed to all
// childrens of this span.
func (c *Context) Span(
	opName string,
	opts ...opentracing.StartSpanOption,
) (opentracing.Span, *Context) {
	var span opentracing.Span
	if parentSpan := opentracing.SpanFromContext(c.Context); parentSpan != nil {
		opts = append(opts, opentracing.ChildOf(parentSpan.Context()))
		span = c.tracer.StartSpan(opName, opts...)
	} else {
		span = c.tracer.StartSpan(opName, opts...)
	}

	ctx := opentracing.ContextWithSpan(c.Context, span)
	return span, &Context{ctx, c.Session, c.tracer}
}
