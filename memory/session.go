// Copyright 2023 Dolthub, Inc.
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

package memory

import (
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
)

type Session struct {
	*sql.BaseSession
	editAccumulators map[string]tableEditAccumulator	
}

var _ sql.Session = (*Session)(nil)

// NewSession returns the new session for this object
func NewSession(baseSession *sql.BaseSession) *Session {
	return &Session{
		BaseSession: baseSession,
		editAccumulators: make(map[string]tableEditAccumulator),
	}
}

func SessionFromContext(ctx *sql.Context) *Session {
	return ctx.Session.(*Session)
}

// editAccumulator returns the edit accumulator for the table provided for this session, creating one if it
// doesn't exist
func (s *Session) editAccumulator(t *Table) tableEditAccumulator {
	ea, ok := s.editAccumulators[strings.ToLower(t.name)]
	if !ok {
		ea = NewTableEditAccumulator(t)
		s.editAccumulators[strings.ToLower(t.name)] = ea
	}
	
	return ea
}