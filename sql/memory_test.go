package sql

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestManager(t *testing.T) {
	require := require.New(t)
	m := NewMemoryManager(nil)

	kv, dispose := m.NewLRUCache(5)
	_, ok := kv.(*lruCache)
	require.True(ok)
	require.Len(m.caches, 1)
	dispose()
	require.Len(m.caches, 0)

	kv, dispose = m.NewHistoryCache()
	_, ok = kv.(*historyCache)
	require.True(ok)
	require.Len(m.caches, 1)
	dispose()
	require.Len(m.caches, 0)

	rc, dispose := m.NewRowsCache()
	_, ok = rc.(*rowsCache)
	require.True(ok)
	require.Len(m.caches, 1)
	dispose()
	require.Len(m.caches, 0)

	m.addCache(disposableCache{})
	f := new(freeableCache)
	m.addCache(f)
	m.Free()
	require.True(f.freed)
}

type disposableCache struct{}

func (d disposableCache) Dispose() {}

type freeableCache struct {
	disposableCache
	freed bool
}

func (f *freeableCache) Free() { f.freed = true }

func TestHasAvailable(t *testing.T) {
	require.True(t, HasAvailableMemory(fixedReporter(2, 5)))
	require.False(t, HasAvailableMemory(fixedReporter(6, 5)))
}

type mockReporter struct {
	f   func() uint64
	max uint64
}

func (m mockReporter) UsedMemory() uint64 { return m.f() }
func (m mockReporter) MaxMemory() uint64  { return m.max }

func fixedReporter(v, max uint64) mockReporter {
	return mockReporter{func() uint64 {
		return v
	}, max}
}

type mockMemory struct {
	f func()
}

func (m mockMemory) Free() {
	if m.f != nil {
		m.f()
	}
}
