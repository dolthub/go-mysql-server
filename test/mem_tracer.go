package test

import (
	"sync"

	opentracing "github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/log"
)

// MemTracer implements a simple tracer in memory for testing.
type MemTracer struct {
	Spans []string
	sync.Mutex
}

type memSpan struct {
	opName string
}

// StartSpan implements opentracing.Tracer interface.
func (t *MemTracer) StartSpan(operationName string, opts ...opentracing.StartSpanOption) opentracing.Span {
	t.Lock()
	t.Spans = append(t.Spans, operationName)
	t.Unlock()
	return &memSpan{operationName}
}

// Inject implements opentracing.Tracer interface.
func (t *MemTracer) Inject(sm opentracing.SpanContext, format interface{}, carrier interface{}) error {
	panic("not implemented")
}

// Extract implements opentracing.Tracer interface.
func (t *MemTracer) Extract(format interface{}, carrier interface{}) (opentracing.SpanContext, error) {
	panic("not implemented")
}

func (m memSpan) Context() opentracing.SpanContext                      { return m }
func (m memSpan) SetBaggageItem(key, val string) opentracing.Span       { return m }
func (m memSpan) BaggageItem(key string) string                         { return "" }
func (m memSpan) SetTag(key string, value interface{}) opentracing.Span { return m }
func (m memSpan) LogFields(fields ...log.Field)                         {}
func (m memSpan) LogKV(keyVals ...interface{})                          {}
func (m memSpan) Finish()                                               {}
func (m memSpan) FinishWithOptions(opts opentracing.FinishOptions)      {}
func (m memSpan) SetOperationName(operationName string) opentracing.Span {
	return &memSpan{operationName}
}
func (m memSpan) Tracer() opentracing.Tracer                            { return &MemTracer{} }
func (m memSpan) LogEvent(event string)                                 {}
func (m memSpan) LogEventWithPayload(event string, payload interface{}) {}
func (m memSpan) Log(data opentracing.LogData)                          {}
func (m memSpan) ForeachBaggageItem(handler func(k, v string) bool)     {}
