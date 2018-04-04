package sql

import (
	"context"
	"io"
	"time"

	opentracing "github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/log"
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
