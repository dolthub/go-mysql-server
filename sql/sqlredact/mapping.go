// Copyright 2026 Dolthub, Inc.
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

package sqlredact

import (
	"fmt"
	"strconv"
	"sync"
)

// Mapping records a per-query substitution from an original identifier
// or value to a stable, low-entropy token. Tokens are minted on first
// lookup in lexer-emit order; subsequent lookups for the same original
// return the same token. Two independent namespaces are kept so an
// identifier and a literal sharing the same surface form get distinct
// tokens, but tables/columns/aliases all share the identifier
// namespace because the lexer cannot distinguish them.
//
// Tokens are intentionally short and repeating (n1, n2, ..., v1, v2,
// ...) so trace storage compresses well across many queries with the
// same shape. Hashes would be anti-compression.
//
// A Mapping is safe for concurrent use. The intended pattern is:
// populate during the initial parse pass on one goroutine, then read
// (with occasional mint-on-miss for synthetic names that never
// appeared in the parsed SQL) from many goroutines as parallel
// rowexec spans fire. Writes are guarded by a sync.RWMutex so a
// fast-path read on an existing token only takes the read lock.
type Mapping struct {
	mu sync.RWMutex

	idents map[string]string
	values map[string]string

	nCount int
	vCount int
}

// NewMapping returns an empty mapping with the two namespaces
// initialized.
func NewMapping() *Mapping {
	return &Mapping{
		idents: map[string]string{},
		values: map[string]string{},
	}
}

// RedactIdent returns a stable token for orig in the identifier
// namespace (table, column, alias, schema — the lexer does not
// distinguish). The empty string passes through.
func (m *Mapping) RedactIdent(orig string) string {
	if m == nil || orig == "" {
		return orig
	}
	m.mu.RLock()
	if t, ok := m.idents[orig]; ok {
		m.mu.RUnlock()
		return t
	}
	m.mu.RUnlock()
	m.mu.Lock()
	defer m.mu.Unlock()
	// Re-check after acquiring the write lock — another goroutine may
	// have minted the same token between RUnlock and Lock.
	if t, ok := m.idents[orig]; ok {
		return t
	}
	m.nCount++
	t := "n" + strconv.Itoa(m.nCount)
	m.idents[orig] = t
	return t
}

// RedactValue returns a stable token for orig in the literal-value
// namespace (string, integer, float, hex, bit literal). The token is
// a bare name; callers that want bind-arg syntax should prefix.
func (m *Mapping) RedactValue(orig string) string {
	if m == nil {
		return orig
	}
	m.mu.RLock()
	if t, ok := m.values[orig]; ok {
		m.mu.RUnlock()
		return t
	}
	m.mu.RUnlock()
	m.mu.Lock()
	defer m.mu.Unlock()
	if t, ok := m.values[orig]; ok {
		return t
	}
	m.vCount++
	t := "v" + strconv.Itoa(m.vCount)
	m.values[orig] = t
	return t
}

// Idents returns a snapshot copy of the original→token map for
// identifiers. Safe to call from any goroutine.
func (m *Mapping) Idents() map[string]string {
	if m == nil {
		return nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return copyMap(m.idents)
}

// Values returns a snapshot copy of the original→token map for
// literal values. Safe to call from any goroutine.
func (m *Mapping) Values() map[string]string {
	if m == nil {
		return nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return copyMap(m.values)
}

func copyMap(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

// String returns a debug-friendly summary, used in trace error events
// when redaction is partial.
func (m *Mapping) String() string {
	if m == nil {
		return "<nil mapping>"
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return fmt.Sprintf("sqlredact.Mapping{idents:%d values:%d}",
		m.nCount, m.vCount)
}
