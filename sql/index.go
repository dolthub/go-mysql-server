package sql

import (
	"bytes"
	"encoding/hex"
	"io"
	"strings"
	"sync"

	"gopkg.in/src-d/go-errors.v1"
)

// IndexBatchSize is the number of rows to save at a time when creating indexes.
var IndexBatchSize = uint64(10000)

// IndexKeyValueIter is an iterator of index key values, that is, a tuple of
// the values that will be index keys.
type IndexKeyValueIter interface {
	// Next returns the next tuple of index key values. The length of the
	// returned slice will be the same as the number of columns used to
	// create this iterator. The second returned parameter is a repo's location.
	Next() ([]interface{}, []byte, error)
	io.Closer
}

// IndexValueIter is an iterator of index values.
type IndexValueIter interface {
	// Next returns the next value (repo's location) - see IndexKeyValueIter.
	Next() ([]byte, error)
	io.Closer
}

// Index is the basic representation of an index. It can be extended with
// more functionality by implementing more specific interfaces.
type Index interface {
	// Get returns an IndexLookup for the given key in the index.
	Get(key ...interface{}) (IndexLookup, error)
	// Has checks if the given key is present in the index.
	Has(key ...interface{}) (bool, error)
	// ID returns the identifier of the index.
	ID() string
	// Database returns the database name this index belongs to.
	Database() string
	// Table returns the table name this index belongs to.
	Table() string
	// Expressions returns the indexed expressions. If the result is more than
	// one expression, it means the index has multiple columns indexed. If it's
	// just one, it means it may be an expression or a column.
	ExpressionHashes() []ExpressionHash
	// Driver ID of the index.
	Driver() string
}

// AscendIndex is an index that is sorted in ascending order.
type AscendIndex interface {
	// AscendGreaterOrEqual returns an IndexLookup for keys that are greater
	// or equal to the given keys.
	AscendGreaterOrEqual(keys ...interface{}) (IndexLookup, error)
	// AscendLessThan returns an IndexLookup for keys that are less than the
	// given keys.
	AscendLessThan(keys ...interface{}) (IndexLookup, error)
	// AscendRange returns an IndexLookup for keys that are within the given
	// range.
	AscendRange(greaterOrEqual, lessThan []interface{}) (IndexLookup, error)
}

// DescendIndex is an index that is sorted in descending order.
type DescendIndex interface {
	// DescendGreater returns an IndexLookup for keys that are greater
	// than the given keys.
	DescendGreater(keys ...interface{}) (IndexLookup, error)
	// DescendLessOrEqual returns an IndexLookup for keys that are less than or
	// equal to the given keys.
	DescendLessOrEqual(keys ...interface{}) (IndexLookup, error)
	// DescendRange returns an IndexLookup for keys that are within the given
	// range.
	DescendRange(lessOrEqual, greaterThan []interface{}) (IndexLookup, error)
}

// NegateIndex is an index that supports retrieving negated values.
type NegateIndex interface {
	// Not returns an IndexLookup for keys that are not equal
	// to the given keys.
	Not(keys ...interface{}) (IndexLookup, error)
}

// IndexLookup is a subset of an index. More specific interfaces can be
// implemented to grant more capabilities to the index lookup.
type IndexLookup interface {
	// Values returns the values in the subset of the index.
	Values() (IndexValueIter, error)
}

// SetOperations is a specialization of IndexLookup that enables set operations
// between several IndexLookups.
type SetOperations interface {
	// Intersection returns a new index subset with the intersection of the
	// current IndexLookup and the ones given.
	Intersection(...IndexLookup) IndexLookup
	// Union returns a new index subset with the union of the current
	// IndexLookup and the ones given.
	Union(...IndexLookup) IndexLookup
	// Difference returns a new index subset with the difference between the
	// current IndexLookup and the ones given.
	Difference(...IndexLookup) IndexLookup
}

// Mergeable is a specialization of IndexLookup to check if an IndexLookup can
// be merged with another one.
type Mergeable interface {
	// IsMergeable checks whether the current IndexLookup can be merged with
	// the given one.
	IsMergeable(IndexLookup) bool
}

// IndexDriver manages the coordination between the indexes and their
// representation on disk.
type IndexDriver interface {
	// ID returns the unique name of the driver.
	ID() string
	// Create a new index. If exprs is more than one expression, it means the
	// index has multiple columns indexed. If it's just one, it means it may
	// be an expression or a column.
	Create(db, table, id string, expressionHashes []ExpressionHash, config map[string]string) (Index, error)
	// LoadAll loads all indexes for given db and table
	LoadAll(db, table string) ([]Index, error)
	// Save the given index
	Save(ctx *Context, index Index, iter IndexKeyValueIter) error
	// Delete the given index.
	Delete(index Index) error
}

type indexKey struct {
	db, id string
}

// IndexRegistry keeps track of all indexes in the engine.
type IndexRegistry struct {
	// Root path where all the data of the indexes is stored on disk.
	Root string

	mut        sync.RWMutex
	indexes    map[indexKey]Index
	indexOrder []indexKey
	statuses   map[indexKey]IndexStatus

	driversMut sync.RWMutex
	drivers    map[string]IndexDriver

	rcmut            sync.RWMutex
	refCounts        map[indexKey]int
	deleteIndexQueue map[indexKey]chan<- struct{}
}

// NewIndexRegistry returns a new Index Registry.
func NewIndexRegistry() *IndexRegistry {
	return &IndexRegistry{
		indexes:          make(map[indexKey]Index),
		statuses:         make(map[indexKey]IndexStatus),
		drivers:          make(map[string]IndexDriver),
		refCounts:        make(map[indexKey]int),
		deleteIndexQueue: make(map[indexKey]chan<- struct{}),
	}
}

// IndexDriver returns the IndexDriver with the given ID.
func (r *IndexRegistry) IndexDriver(id string) IndexDriver {
	r.driversMut.RLock()
	defer r.driversMut.RUnlock()
	return r.drivers[id]
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

// LoadIndexes loads all indexes for all dbs, tables and drivers.
func (r *IndexRegistry) LoadIndexes(dbs Databases) error {
	r.driversMut.RLock()
	defer r.driversMut.RUnlock()
	r.mut.Lock()
	defer r.mut.Unlock()

	for _, driver := range r.drivers {
		for _, db := range dbs {
			for t := range db.Tables() {
				indexes, err := driver.LoadAll(db.Name(), t)
				if err != nil {
					return err
				}

				for _, idx := range indexes {
					k := indexKey{db.Name(), idx.ID()}
					r.indexes[k] = idx
					r.indexOrder = append(r.indexOrder, k)
					r.statuses[k] = IndexReady
				}
			}
		}
	}

	return nil
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
func (r *IndexRegistry) Index(db, id string) Index {
	r.mut.RLock()
	defer r.mut.RUnlock()

	r.retainIndex(db, id)
	idx := r.indexes[indexKey{db, strings.ToLower(id)}]
	if idx != nil && !r.canUseIndex(idx) {
		return nil
	}

	return idx
}

// IndexByExpression returns an index by the given expression. It will return
// nil it the index is not found. If more than one expression is given, all
// of them must match for the index to be matched.
func (r *IndexRegistry) IndexByExpression(db string, expr ...Expression) Index {
	r.mut.RLock()
	defer r.mut.RUnlock()

	var expressionHashes []ExpressionHash
	for _, e := range expr {
		expressionHashes = append(expressionHashes, NewExpressionHash(e))
	}

	for _, k := range r.indexOrder {
		idx := r.indexes[k]
		if !r.canUseIndex(idx) {
			continue
		}

		if idx.Database() == db {
			if exprListsMatch(idx.ExpressionHashes(), expressionHashes) {
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

		if ln := len(idx.ExpressionHashes()); ln <= len(exprs) && ln > 1 {
			var used = make(map[int]struct{})
			var matched []Expression
			for _, ie := range idx.ExpressionHashes() {
				var found bool
				for i, e := range exprs {
					if _, ok := used[i]; ok {
						continue
					}

					if expressionsEqual(ie, NewExpressionHash(e)) {
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

type withIndexer interface {
	WithIndex(int) Expression
}

func removeIndexes(e Expression) (Expression, error) {
	i, ok := e.(withIndexer)
	if !ok {
		return e, nil
	}

	return i.WithIndex(-1), nil
}

func expressionsEqual(a, b ExpressionHash) bool {
	return bytes.Compare(a, b) == 0
}

var (
	// ErrIndexIDAlreadyRegistered is the error returned when there is already
	// an index with the same ID.
	ErrIndexIDAlreadyRegistered = errors.NewKind("an index with id %q has already been registered")

	// ErrIndexExpressionAlreadyRegistered is the error returned when there is
	// already an index with the same expression.
	ErrIndexExpressionAlreadyRegistered = errors.NewKind("there is already an index registered for the expressions: %s")

	// ErrIndexNotFound is returned when the index could not be found.
	ErrIndexNotFound = errors.NewKind("index %q	was not found")

	// ErrIndexDeleteInvalidStatus is returned when the index trying to delete
	// does not have a ready state.
	ErrIndexDeleteInvalidStatus = errors.NewKind("can't delete index %q because it's not ready for usage")
)

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

		if exprListsEqual(i.ExpressionHashes(), idx.ExpressionHashes()) {
			var exprs = make([]string, len(idx.ExpressionHashes()))
			for i, e := range idx.ExpressionHashes() {
				exprs[i] = hex.EncodeToString(e)
			}
			return ErrIndexExpressionAlreadyRegistered.New(strings.Join(exprs, ", "))
		}
	}

	return nil
}

// exprListsMatch returns whether any subset of a is the entirety of b.
func exprListsMatch(a, b []ExpressionHash) bool {
	var visited = make([]bool, len(b))

	for _, va := range a {
		found := false

		for j, vb := range b {
			if visited[j] {
				continue
			}

			if bytes.Equal(va, vb) {
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

// exprListsEqual returns whether a and b have the same items.
func exprListsEqual(a, b []ExpressionHash) bool {
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

			if bytes.Equal(va, vb) {
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
// marked as creating, so nobody can't register two indexes with the same
// expression or id while the other is still being created.
// When something is sent through the returned channel, it means the index has
// finished it's creation and will be marked as ready.
// Another channel is returned to notify the user when the index is ready.
func (r *IndexRegistry) AddIndex(
	idx Index,
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
	for k, idx := range r.indexes {
		if strings.ToLower(id) == idx.ID() {
			if !force && !r.CanUseIndex(idx) {
				r.mut.RUnlock()
				return nil, ErrIndexDeleteInvalidStatus.New(id)
			}
			r.setStatus(idx, IndexNotReady)
			key = k
			break
		}
	}
	r.mut.RUnlock()

	if key.id == "" {
		return nil, ErrIndexNotFound.New(id)
	}

	var done = make(chan struct{}, 1)

	r.rcmut.Lock()
	// If no query is using this index just delete it right away
	if r.refCounts[key] == 0 {
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

// IndexStatus represents the current status in which the index is.
type IndexStatus bool

const (
	// IndexNotReady means the index is not ready to be used.
	IndexNotReady IndexStatus = false
	// IndexReady means the index can be used.
	IndexReady IndexStatus = true
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
