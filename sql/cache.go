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

package sql

import (
	"fmt"
	"runtime"
	"sync"

	"github.com/cespare/xxhash/v2"

	lru "github.com/hashicorp/golang-lru"
)

// HashOf returns a hash of the given value to be used as key in a cache.
func HashOf(v Row) (uint64, error) {
	if v == nil {
		return 0, nil
	}
	hash := digestPool.Get().(*xxhash.Digest)
	hash.Reset()
	defer digestPool.Put(hash)
	for i, x := range v.Values() {
		if i > 0 {
			// separate each value in the row with a nil byte
			if _, err := hash.Write([]byte{0}); err != nil {
				return 0, err
			}
		}

		// TODO: probably much faster to do this with a type switch
		// TODO: we don't have the type info necessary to appropriately encode the value of a string with a non-standard
		//  collation, which means that two strings that differ only in their collations will hash to the same value.
		//  See rowexec/grouping_key()
		if _, err := fmt.Fprintf(hash, "%v,", x); err != nil {
			return 0, err
		}
	}
	return hash.Sum64(), nil
}

var digestPool = sync.Pool{
	New: func() any {
		return xxhash.New()
	},
}

// ErrKeyNotFound is returned when the key could not be found in the cache.
var ErrKeyNotFound = fmt.Errorf("memory: key not found in cache")

type lruCache struct {
	memory   Freeable
	reporter Reporter
	size     int
	cache    *lru.Cache
}

func (l *lruCache) Size() int {
	return l.size
}

func newLRUCache(memory Freeable, r Reporter, size uint) *lruCache {
	lru, _ := lru.New(int(size))
	return &lruCache{memory, r, int(size), lru}
}

func (l *lruCache) Put(k uint64, v interface{}) error {
	if releaseMemoryIfNeeded(l.reporter, l.Free, l.memory.Free) {
		l.cache.Add(k, v)
	}
	return nil
}

func (l *lruCache) Get(k uint64) (interface{}, error) {
	v, ok := l.cache.Get(k)
	if !ok {
		return nil, ErrKeyNotFound
	}

	return v, nil
}

func (l *lruCache) Free() {
	l.cache, _ = lru.New(l.size)
}

func (l *lruCache) Dispose() {
	l.memory = nil
	l.cache = nil
}

type rowsCache struct {
	memory   Freeable
	reporter Reporter
	rows     []Row
	rows2    []Row2
}

func newRowsCache(memory Freeable, r Reporter) *rowsCache {
	return &rowsCache{memory: memory, reporter: r}
}

func (c *rowsCache) Add(row Row) error {
	if !releaseMemoryIfNeeded(c.reporter, c.memory.Free) {
		return ErrNoMemoryAvailable.New()
	}

	c.rows = append(c.rows, row)
	return nil
}

func (c *rowsCache) Get() []Row { return c.rows }

func (c *rowsCache) Add2(row2 Row2) error {
	if !releaseMemoryIfNeeded(c.reporter, c.memory.Free) {
		return ErrNoMemoryAvailable.New()
	}

	c.rows2 = append(c.rows2, row2)
	return nil
}

func (c *rowsCache) Get2() []Row2 {
	return c.rows2
}

func (c *rowsCache) Dispose() {
	c.memory = nil
	c.rows = nil
}

// mapCache is a simple in-memory implementation of a cache
type mapCache struct {
	cache map[uint64]interface{}
}

func (m mapCache) Put(u uint64, i interface{}) error {
	m.cache[u] = i
	return nil
}

func (m mapCache) Get(u uint64) (interface{}, error) {
	v, ok := m.cache[u]
	if !ok {
		return nil, ErrKeyNotFound
	}
	return v, nil
}

func (m mapCache) Size() int {
	return len(m.cache)
}

func NewMapCache() mapCache {
	return mapCache{
		cache: make(map[uint64]interface{}),
	}
}

type historyCache struct {
	memory   Freeable
	reporter Reporter
	cache    map[uint64]interface{}
}

func (h *historyCache) Size() int {
	return len(h.cache)
}

func newHistoryCache(memory Freeable, r Reporter) *historyCache {
	return &historyCache{memory, r, make(map[uint64]interface{})}
}

func (h *historyCache) Put(k uint64, v interface{}) error {
	if !releaseMemoryIfNeeded(h.reporter, h.memory.Free) {
		return ErrNoMemoryAvailable.New()
	}
	h.cache[k] = v
	return nil
}

func (h *historyCache) Get(k uint64) (interface{}, error) {
	v, ok := h.cache[k]
	if !ok {
		return nil, ErrKeyNotFound
	}
	return v, nil
}

func (h *historyCache) Dispose() {
	h.memory = nil
	h.cache = nil
}

// releasesMemoryIfNeeded releases memory if needed using the following steps
// until there is available memory. It returns whether or not there was
// available memory after all the steps.
func releaseMemoryIfNeeded(r Reporter, steps ...func()) bool {
	for _, s := range steps {
		if HasAvailableMemory(r) {
			return true
		}

		s()
		runtime.GC()
	}

	return HasAvailableMemory(r)
}
