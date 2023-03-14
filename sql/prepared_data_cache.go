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

package sql

import "sync"

// PreparedDataCacher is a node that contains a reference to a PreparedDataCache.
type PreparedDataCacher interface {
	Node
	// WithPreparedDataCache returns a new Node with the PreparedDataCache replaced with the one given as the parameter.
	WithPreparedDataCache(*PreparedDataCache) (Node, error)
}

// PreparedDataCache manages all the prepared data for every session for every query for an engine
type PreparedDataCache struct {
	data map[uint32]map[string]Node
	mu   *sync.Mutex
}

func NewPreparedDataCache() *PreparedDataCache {
	return &PreparedDataCache{
		data: make(map[uint32]map[string]Node),
		mu:   &sync.Mutex{},
	}
}

// GetCachedStmt will retrieve the prepared Node associated with the ctx.SessionId and query if it exists
// it will return nil, false if the query does not exist
func (p *PreparedDataCache) GetCachedStmt(sessId uint32, query string) (Node, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if sessData, ok := p.data[sessId]; ok {
		data, ok := sessData[query]
		return data, ok
	}
	return nil, false
}

// GetSessionData returns all the prepared queries for a particular session
func (p *PreparedDataCache) GetSessionData(sessId uint32) map[string]Node {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.data[sessId]
}

// DeleteSessionData clears a session along with all prepared queries for that session
func (p *PreparedDataCache) DeleteSessionData(sessId uint32) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.data, sessId)
}

// CacheStmt saves the prepared node and associates a ctx.SessionId and query to it
func (p *PreparedDataCache) CacheStmt(sessId uint32, query string, node Node) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.data[sessId]; !ok {
		p.data[sessId] = make(map[string]Node)
	}
	p.data[sessId][query] = node
}

// UncacheStmt removes the prepared node associated with a ctx.SessionId and query to it
func (p *PreparedDataCache) UncacheStmt(sessId uint32, query string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.data[sessId]; !ok {
		return
	}
	delete(p.data[sessId], query)
}
