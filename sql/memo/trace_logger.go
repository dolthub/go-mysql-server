// Copyright 2025 Dolthub, Inc.
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

package memo

import (
	"strings"

	"github.com/sirupsen/logrus"
)

type TraceLogger struct {
	// A stack of debugger context. See PushDebugContext, PopDebugContext
	contextStack []string
	traceEnabled bool
}

var log = logrus.New()

// PushDebugContext pushes the given context string onto the context stack, to use when logging debug messages.
func (a *TraceLogger) PushDebugContext(msg string) {
	if a != nil && a.traceEnabled {
		a.contextStack = append(a.contextStack, msg)
	}
}

// PopDebugContext pops a context message off the context stack.
func (a *TraceLogger) PopDebugContext() {
	if a != nil && len(a.contextStack) > 0 {
		a.contextStack = a.contextStack[:len(a.contextStack)-1]
	}
}

// Log prints an INFO message to stdout with the given message and args
// if the analyzer is in debug mode.
func (a *TraceLogger) Log(msg string, args ...interface{}) {
	if a != nil && a.traceEnabled {
		if len(a.contextStack) > 0 {
			ctx := strings.Join(a.contextStack, "/")
			log.Infof("%s: "+msg, append([]interface{}{ctx}, args...)...)
		} else {
			log.Infof(msg, args...)
		}
	}
}