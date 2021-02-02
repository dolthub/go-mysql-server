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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLRUCache(t *testing.T) {
	t.Run("basic methods", func(t *testing.T) {
		require := require.New(t)

		cache := newLRUCache(mockMemory{}, fixedReporter(5, 50), 10)

		require.NoError(cache.Put(1, "foo"))
		v, err := cache.Get(1)
		require.NoError(err)
		require.Equal("foo", v)

		_, err = cache.Get(2)
		require.Error(err)
		require.True(ErrKeyNotFound.Is(err))

		// Free the cache and check previous entry disappeared.
		cache.Free()

		_, err = cache.Get(1)
		require.Error(err)
		require.True(ErrKeyNotFound.Is(err))

		cache.Dispose()
		require.Panics(func() {
			_, _ = cache.Get(1)
		})
	})

	t.Run("no memory available", func(t *testing.T) {
		require := require.New(t)
		cache := newLRUCache(mockMemory{}, fixedReporter(51, 50), 5)

		require.NoError(cache.Put(1, "foo"))
		_, err := cache.Get(1)
		require.Error(err)
		require.True(ErrKeyNotFound.Is(err))
	})

	t.Run("free required to add entry", func(t *testing.T) {
		require := require.New(t)
		var freed bool
		cache := newLRUCache(
			mockMemory{func() {
				freed = true
			}},
			mockReporter{func() uint64 {
				if freed {
					return 0
				}
				return 51
			}, 50},
			5,
		)
		require.NoError(cache.Put(1, "foo"))
		v, err := cache.Get(1)
		require.NoError(err)
		require.Equal("foo", v)
		require.True(freed)
	})
}

func TestHistoryCache(t *testing.T) {
	t.Run("basic methods", func(t *testing.T) {
		require := require.New(t)

		cache := newHistoryCache(mockMemory{}, fixedReporter(5, 50))

		require.NoError(cache.Put(1, "foo"))
		v, err := cache.Get(1)
		require.NoError(err)
		require.Equal("foo", v)

		_, err = cache.Get(2)
		require.Error(err)
		require.True(ErrKeyNotFound.Is(err))

		cache.Dispose()
		require.Panics(func() {
			_ = cache.Put(2, "foo")
		})
	})

	t.Run("no memory available", func(t *testing.T) {
		require := require.New(t)
		cache := newHistoryCache(mockMemory{}, fixedReporter(51, 50))

		err := cache.Put(1, "foo")
		require.Error(err)
		require.True(ErrNoMemoryAvailable.Is(err))
	})

	t.Run("free required to add entry", func(t *testing.T) {
		require := require.New(t)
		var freed bool
		cache := newHistoryCache(
			mockMemory{func() {
				freed = true
			}},
			mockReporter{func() uint64 {
				if freed {
					return 0
				}
				return 51
			}, 50},
		)
		require.NoError(cache.Put(1, "foo"))
		v, err := cache.Get(1)
		require.NoError(err)
		require.Equal("foo", v)
		require.True(freed)
	})
}

func TestRowsCache(t *testing.T) {
	t.Run("basic methods", func(t *testing.T) {
		require := require.New(t)

		cache := newRowsCache(mockMemory{}, fixedReporter(5, 50))

		require.NoError(cache.Add(Row{1}))
		require.Len(cache.Get(), 1)

		cache.Dispose()
		require.Panics(func() {
			_ = cache.Add(Row{2})
		})
	})

	t.Run("no memory available", func(t *testing.T) {
		require := require.New(t)
		cache := newRowsCache(mockMemory{}, fixedReporter(51, 50))

		err := cache.Add(Row{1, "foo"})
		require.Error(err)
		require.True(ErrNoMemoryAvailable.Is(err))
	})

	t.Run("free required to add entry", func(t *testing.T) {
		require := require.New(t)
		var freed bool
		cache := newRowsCache(
			mockMemory{func() {
				freed = true
			}},
			mockReporter{func() uint64 {
				if freed {
					return 0
				}
				return 51
			}, 50},
		)
		require.NoError(cache.Add(Row{1, "foo"}))
		require.Len(cache.Get(), 1)
		require.True(freed)
	})
}
