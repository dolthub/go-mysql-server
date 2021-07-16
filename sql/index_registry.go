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
	"strings"
	"sync"

	"github.com/sirupsen/logrus"

	"github.com/dolthub/go-mysql-server/internal/similartext"
)

// IndexRegistry keeps track of all driver-provided indexes in the engine.
type IndexRegistry struct {
	// Root path where all the data of the indexes is stored on disk.
	Root string

	mut        sync.RWMutex
	indexes    map[indexKey]DriverIndex
	indexOrder []indexKey
	statuses   map[indexKey]IndexStatus

	driversMut sync.RWMutex
	drivers    map[string]IndexDriver

	rcmut            sync.RWMutex
	refCounts        map[indexKey]int
	deleteIndexQueue map[indexKey]chan<- struct{}
	indexLoaders     map[dbTableTuple][]func(ctx *Context) error
}

// NewIndexRegistry returns a new Index Registry.
func NewIndexRegistry() *IndexRegistry {
	return &IndexRegistry{
		indexes:          make(map[indexKey]DriverIndex),
		statuses:         make(map[indexKey]IndexStatus),
		drivers:          make(map[string]IndexDriver),
		refCounts:        make(map[indexKey]int),
		deleteIndexQueue: make(map[indexKey]chan<- struct{}),
		indexLoaders:     make(map[dbTableTuple][]func(ctx *Context) error),
	}
}

// IndexDriver returns the IndexDriver with the given ID.
func (r *IndexRegistry) IndexDriver(id string) IndexDriver {
	r.driversMut.RLock()
	defer r.driversMut.RUnlock()
	return r.drivers[id]
}

// HasIndexes returns whether the index registry has any registered indexes. Not thread safe, so the answer is
// approximate in the face of drivers and indexes being added and removed.
func (r *IndexRegistry) HasIndexes() bool {
	return len(r.indexes) > 0 || len(r.drivers) > 0
}

// DefaultIndexDriver returns the default index driver, which is the only
// driver when there is 1 driver in the registry. If there are more than
// 1 drivers in the registry, this will return the empty string, as there
// is no clear default driver.
func (r *IndexRegistry) DefaultIndexDriver() IndexDriver {
	r.driversMut.RLock()
	defer r.driversMut.RUnlock()
	if len(r.drivers) == 1 {
		for _, d := range r.drivers {
			return d
		}
	}
	return nil
}

// RegisterIndexDriver registers a new index driver.
func (r *IndexRegistry) RegisterIndexDriver(driver IndexDriver) {
	r.driversMut.Lock()
	defer r.driversMut.Unlock()
	r.drivers[driver.ID()] = driver
}

// LoadIndexes creates load functions for all indexes for all dbs, tables and drivers.  These functions are called
// as needed by the query
func (r *IndexRegistry) LoadIndexes(ctx *Context, dbs []Database) error {
	r.driversMut.RLock()
	defer r.driversMut.RUnlock()
	r.mut.Lock()
	defer r.mut.Unlock()

	for drIdx := range r.drivers {
		driver := r.drivers[drIdx]
		for dbIdx := range dbs {
			db := dbs[dbIdx]
			tNames, err := db.GetTableNames(ctx)

			if err != nil {
				return err
			}

			for tIdx := range tNames {
				tName := tNames[tIdx]

				loadF := func(ctx *Context) error {
					t, ok, err := db.GetTableInsensitive(ctx, tName)

					if err != nil {
						return err
					} else if !ok {
						panic("Failed to find table in list of table names")
					}

					indexes, err := driver.LoadAll(ctx, db.Name(), t.Name())
					if err != nil {
						return err
					}

					var checksum string
					if c, ok := t.(Checksumable); ok && len(indexes) != 0 {
						checksum, err = c.Checksum()
						if err != nil {
							return err
						}
					}

					for _, idx := range indexes {
						k := indexKey{db.Name(), idx.ID()}
						r.indexes[k] = idx
						r.indexOrder = append(r.indexOrder, k)

						var idxChecksum string
						if c, ok := idx.(Checksumable); ok {
							idxChecksum, err = c.Checksum()
							if err != nil {
								return err
							}
						}

						if checksum == "" || checksum == idxChecksum {
							r.statuses[k] = IndexReady
						} else {
							logrus.Warnf(
								"index %q is outdated and will not be used, you can remove it using `DROP INDEX %s ON %s`",
								idx.ID(),
								idx.ID(),
								idx.Table(),
							)
							r.MarkOutdated(idx)
						}
					}

					return nil
				}

				dbTT := dbTableTuple{db.Name(), tName}
				r.indexLoaders[dbTT] = append(r.indexLoaders[dbTT], loadF)
			}
		}
	}

	return nil
}

func (r *IndexRegistry) registerIndexesForTable(ctx *Context, dbName, tName string) error {
	r.driversMut.RLock()
	defer r.driversMut.RUnlock()

	dbTT := dbTableTuple{dbName, tName}

	if loaders, ok := r.indexLoaders[dbTT]; ok {
		for _, loader := range loaders {
			err := loader(ctx)

			if err != nil {
				return err
			}
		}

		delete(r.indexLoaders, dbTT)
	}

	return nil
}

// MarkOutdated sets the index status as outdated. This method is not thread
// safe and should not be used directly except for testing.
func (r *IndexRegistry) MarkOutdated(idx Index) {
	r.statuses[indexKey{idx.Database(), idx.ID()}] = IndexOutdated
}

func (r *IndexRegistry) retainIndex(db, id string) {
	r.rcmut.Lock()
	defer r.rcmut.Unlock()
	key := indexKey{db, id}
	r.refCounts[key]++
}

// CanUseIndex returns whether the given index is ready to use or not.
func (r *IndexRegistry) CanUseIndex(idx Index) bool {
	r.mut.RLock()
	defer r.mut.RUnlock()
	return r.canUseIndex(idx)
}

// CanRemoveIndex returns whether the given index is ready to be removed.
func (r *IndexRegistry) CanRemoveIndex(idx Index) bool {
	if idx == nil {
		return false
	}

	r.mut.RLock()
	defer r.mut.RUnlock()
	status := r.statuses[indexKey{idx.Database(), idx.ID()}]
	return status == IndexReady || status == IndexOutdated
}

func (r *IndexRegistry) canUseIndex(idx Index) bool {
	if idx == nil {
		return false
	}
	return r.statuses[indexKey{idx.Database(), idx.ID()}].IsUsable()
}

// setStatus is not thread-safe, it should be guarded using mut.
func (r *IndexRegistry) setStatus(idx Index, status IndexStatus) {
	r.statuses[indexKey{idx.Database(), idx.ID()}] = status
}

// ReleaseIndex releases an index after it's been used.
func (r *IndexRegistry) ReleaseIndex(idx Index) {
	r.rcmut.Lock()
	defer r.rcmut.Unlock()
	key := indexKey{idx.Database(), idx.ID()}
	r.refCounts[key]--
	if r.refCounts[key] > 0 {
		return
	}

	if ch, ok := r.deleteIndexQueue[key]; ok {
		close(ch)
		delete(r.deleteIndexQueue, key)
	}
}

// Index returns the index with the given id. It may return nil if the index is
// not found.
func (r *IndexRegistry) Index(db, id string) DriverIndex {
	r.mut.RLock()
	defer r.mut.RUnlock()

	r.retainIndex(db, id)
	return r.indexes[indexKey{db, strings.ToLower(id)}]
}

// IndexesByTable returns a slice of all the indexes existing on the given table.
func (r *IndexRegistry) IndexesByTable(db, table string) []DriverIndex {
	r.mut.RLock()
	defer r.mut.RUnlock()

	var indexes []DriverIndex
	for _, key := range r.indexOrder {
		idx := r.indexes[key]
		if idx.Database() == db && idx.Table() == table {
			indexes = append(indexes, idx)
			r.retainIndex(db, idx.ID())
		}
	}

	return indexes
}

type exprWithTable interface {
	Table() string
}

// IndexByExpression returns an index by the given expression. It will return
// nil if the index is not found. If more than one expression is given, all
// of them must match for the index to be matched.
func (r *IndexRegistry) IndexByExpression(ctx *Context, db string, expr ...Expression) Index {
	r.mut.RLock()
	defer r.mut.RUnlock()

	expressions := make([]string, len(expr))
	for i, e := range expr {
		expressions[i] = e.String()

		Inspect(e, func(e Expression) bool {
			if e == nil {
				return true
			}

			if val, ok := e.(exprWithTable); ok {
				err := r.registerIndexesForTable(ctx, db, val.Table())

				// TODO: fix panics
				if err != nil {
					panic(err)
				}
			}

			return true
		})
	}

	for _, k := range r.indexOrder {
		idx := r.indexes[k]
		if !r.canUseIndex(idx) {
			continue
		}

		if idx.Database() == db {
			if exprListsEqual(idx.Expressions(), expressions) {
				r.retainIndex(db, idx.ID())
				return idx
			}
		}
	}

	return nil
}

// ExpressionsWithIndexes finds all the combinations of expressions with
// matching indexes. This only matches multi-column indexes.
func (r *IndexRegistry) ExpressionsWithIndexes(
	db string,
	exprs ...Expression,
) [][]Expression {
	r.mut.RLock()
	defer r.mut.RUnlock()

	var results [][]Expression
Indexes:
	for _, idx := range r.indexes {
		if !r.canUseIndex(idx) {
			continue
		}

		if ln := len(idx.Expressions()); ln <= len(exprs) && ln > 1 {
			var used = make(map[int]struct{})
			var matched []Expression
			for _, ie := range idx.Expressions() {
				var found bool
				for i, e := range exprs {
					if _, ok := used[i]; ok {
						continue
					}

					if ie == e.String() {
						used[i] = struct{}{}
						found = true
						matched = append(matched, e)
						break
					}
				}

				if !found {
					continue Indexes
				}
			}

			results = append(results, matched)
		}
	}

	return results
}

func (r *IndexRegistry) validateIndexToAdd(idx Index) error {
	r.mut.RLock()
	defer r.mut.RUnlock()

	for _, i := range r.indexes {
		if i.Database() != idx.Database() {
			continue
		}

		if i.ID() == idx.ID() {
			return ErrIndexIDAlreadyRegistered.New(idx.ID())
		}

		if exprListsEqual(i.Expressions(), idx.Expressions()) {
			return ErrIndexExpressionAlreadyRegistered.New(
				strings.Join(idx.Expressions(), ", "),
			)
		}
	}

	return nil
}

// exprListsEqual returns whether a and b have the same items.
func exprListsEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}

	var visited = make([]bool, len(b))

	for _, va := range a {
		found := false

		for j, vb := range b {
			if visited[j] {
				continue
			}

			if va == vb {
				visited[j] = true
				found = true
				break
			}
		}

		if !found {
			return false
		}
	}

	return true
}

// AddIndex adds the given index to the registry. The added index will be
// marked as creating, so nobody can register two indexes with the same
// expression or id while the other is still being created.
// When something is sent through the returned channel, it means the index has
// finished its creation and will be marked as ready.
// Another channel is returned to notify the user when the index is ready.
func (r *IndexRegistry) AddIndex(
	idx DriverIndex,
) (created chan<- struct{}, ready <-chan struct{}, err error) {
	if err := r.validateIndexToAdd(idx); err != nil {
		return nil, nil, err
	}

	r.mut.Lock()
	r.setStatus(idx, IndexNotReady)
	key := indexKey{idx.Database(), idx.ID()}
	r.indexes[key] = idx
	r.indexOrder = append(r.indexOrder, key)
	r.mut.Unlock()

	var _created = make(chan struct{})
	var _ready = make(chan struct{})
	go func() {
		<-_created
		r.mut.Lock()
		defer r.mut.Unlock()
		r.setStatus(idx, IndexReady)
		close(_ready)
	}()

	return _created, _ready, nil
}

// DeleteIndex deletes an index from the registry by its id. First, it marks
// the index for deletion but does not remove it, so queries that are using it
// may still do so. The returned channel will send a message when the index can
// be deleted from disk.
// If force is true, it will delete the index even if it's not ready for usage.
// Only use that parameter if you know what you're doing.
func (r *IndexRegistry) DeleteIndex(db, id string, force bool) (<-chan struct{}, error) {
	r.mut.RLock()
	var key indexKey

	if len(r.indexes) == 0 {
		return nil, ErrIndexNotFound.New(id)
	}

	var indexNames []string

	for k, idx := range r.indexes {
		if strings.ToLower(id) == idx.ID() {
			if !force && !r.CanRemoveIndex(idx) {
				r.mut.RUnlock()
				return nil, ErrIndexDeleteInvalidStatus.New(id)
			}
			r.setStatus(idx, IndexNotReady)
			key = k
			break
		}
		indexNames = append(indexNames, idx.ID())
	}
	r.mut.RUnlock()

	if key.id == "" {
		similar := similartext.Find(indexNames, id)
		return nil, ErrIndexNotFound.New(id + similar)
	}

	var done = make(chan struct{}, 1)

	r.rcmut.Lock()
	// If no query is using this index just delete it right away
	if force || r.refCounts[key] <= 0 {
		r.mut.Lock()
		defer r.mut.Unlock()
		defer r.rcmut.Unlock()

		delete(r.indexes, key)
		var pos = -1
		for i, k := range r.indexOrder {
			if k == key {
				pos = i
				break
			}
		}
		if pos >= 0 {
			r.indexOrder = append(r.indexOrder[:pos], r.indexOrder[pos+1:]...)
		}
		close(done)
		return done, nil
	}

	var onReadyToDelete = make(chan struct{})
	r.deleteIndexQueue[key] = onReadyToDelete
	r.rcmut.Unlock()

	go func() {
		<-onReadyToDelete
		r.mut.Lock()
		defer r.mut.Unlock()
		delete(r.indexes, key)

		done <- struct{}{}
	}()

	return done, nil
}

type indexKey struct {
	db, id string
}

type dbTableTuple struct {
	db, tbl string
}

// IndexStatus represents the current status in which the index is.
type IndexStatus byte

const (
	// IndexNotReady means the index is not ready to be used.
	IndexNotReady IndexStatus = iota
	// IndexReady means the index can be used.
	IndexReady
	// IndexOutdated means the index is loaded but will not be used because the
	// contents in it are outdated.
	IndexOutdated
)

// IsUsable returns whether the index can be used or not based on the status.
func (s IndexStatus) IsUsable() bool {
	return s == IndexReady
}

func (s IndexStatus) String() string {
	switch s {
	case IndexReady:
		return "ready"
	default:
		return "not ready"
	}
}
