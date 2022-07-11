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

package test

import (
	"context"
	"sync"

	"go.opentelemetry.io/otel/trace"
)

var _, noopSpan = trace.NewNoopTracerProvider().Tracer("").Start(context.Background(), "")

// MemTracer implements a simple tracer in memory for testing.
type MemTracer struct {
	Spans []string
	sync.Mutex
}

type memSpan struct {
	opName string
}

func (t *MemTracer) Start(ctx context.Context, operationName string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	t.Lock()
	t.Spans = append(t.Spans, operationName)
	t.Unlock()
	return ctx, noopSpan
}
