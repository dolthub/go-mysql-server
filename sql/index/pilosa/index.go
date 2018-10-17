package pilosa

import (
	"context"
	"sync"

	"github.com/pilosa/pilosa"
	errors "gopkg.in/src-d/go-errors.v1"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/index"
)

// concurrentPilosaIndex is a wrapper of pilosa.Index that can be opened and closed
// concurrently.
type concurrentPilosaIndex struct {
	*pilosa.Index
	m  sync.Mutex
	rc int
}

func newConcurrentPilosaIndex(idx *pilosa.Index) *concurrentPilosaIndex {
	return &concurrentPilosaIndex{Index: idx}
}

func (i *concurrentPilosaIndex) Open() error {
	i.m.Lock()
	defer i.m.Unlock()

	if i.rc == 0 {
		if err := i.Index.Open(); err != nil {
			return err
		}
	}

	i.rc++
	return nil
}

func (i *concurrentPilosaIndex) Close() error {
	i.m.Lock()
	defer i.m.Unlock()

	i.rc--
	if i.rc < 0 {
		i.rc = 0
	}

	if i.rc == 0 {
		return i.Index.Close()
	}

	return nil
}

var (
	errInvalidKeys = errors.NewKind("expecting %d keys for index %q, got %d")
)

// pilosaIndex is an pilosa implementation of sql.Index interface
type pilosaIndex struct {
	index   *concurrentPilosaIndex
	mapping *mapping
	cancel  context.CancelFunc
	wg      sync.WaitGroup

	db          string
	table       string
	id          string
	expressions []string
}

func newPilosaIndex(idx *pilosa.Index, mapping *mapping, cfg *index.Config) *pilosaIndex {
	return &pilosaIndex{
		index:       newConcurrentPilosaIndex(idx),
		db:          cfg.DB,
		table:       cfg.Table,
		id:          cfg.ID,
		expressions: cfg.Expressions,
		mapping:     mapping,
	}
}

// Get returns an IndexLookup for the given key in the index.
// If key parameter is not present then the returned iterator
// will go through all the locations on the index.
func (idx *pilosaIndex) Get(keys ...interface{}) (sql.IndexLookup, error) {
	if len(keys) != len(idx.expressions) {
		return nil, errInvalidKeys.New(len(idx.expressions), idx.ID(), len(keys))
	}

	return &indexLookup{
		id:          idx.ID(),
		index:       idx.index,
		mapping:     idx.mapping,
		keys:        keys,
		expressions: idx.expressions,
	}, nil
}

// Has checks if the given key is present in the index mapping
func (idx *pilosaIndex) Has(p sql.Partition, key ...interface{}) (bool, error) {
	if err := idx.mapping.open(); err != nil {
		return false, err
	}
	defer idx.mapping.close()

	n := len(key)
	if n > len(idx.expressions) {
		n = len(idx.expressions)
	}

	for i, expr := range idx.expressions {
		name := fieldName(idx.ID(), expr, p)

		val, err := idx.mapping.get(name, key[i])
		if err != nil || val == nil {
			return false, err
		}
	}

	return true, nil
}

// Database returns the database name this index belongs to.
func (idx *pilosaIndex) Database() string {
	return idx.db
}

// Table returns the table name this index belongs to.
func (idx *pilosaIndex) Table() string {
	return idx.table
}

// ID returns the identifier of the index.
func (idx *pilosaIndex) ID() string {
	return idx.id
}

// Expressions returns the indexed expressions. If the result is more than
// one expression, it means the index has multiple columns indexed. If it's
// just one, it means it may be an expression or a column.
func (idx *pilosaIndex) Expressions() []string {
	return idx.expressions
}

func (*pilosaIndex) Driver() string { return DriverID }

func (idx *pilosaIndex) AscendGreaterOrEqual(keys ...interface{}) (sql.IndexLookup, error) {
	if len(keys) != len(idx.expressions) {
		return nil, errInvalidKeys.New(len(idx.expressions), idx.ID(), len(keys))
	}

	return newAscendLookup(&filteredLookup{
		id:          idx.ID(),
		index:       idx.index,
		mapping:     idx.mapping,
		keys:        keys,
		expressions: idx.expressions,
	}, keys, nil), nil
}

func (idx *pilosaIndex) AscendLessThan(keys ...interface{}) (sql.IndexLookup, error) {
	if len(keys) != len(idx.expressions) {
		return nil, errInvalidKeys.New(len(idx.expressions), idx.ID(), len(keys))
	}

	return newAscendLookup(&filteredLookup{
		id:          idx.ID(),
		index:       idx.index,
		mapping:     idx.mapping,
		keys:        keys,
		expressions: idx.expressions,
	}, nil, keys), nil
}

func (idx *pilosaIndex) AscendRange(greaterOrEqual, lessThan []interface{}) (sql.IndexLookup, error) {
	if len(greaterOrEqual) != len(idx.expressions) {
		return nil, errInvalidKeys.New(len(idx.expressions), idx.ID(), len(greaterOrEqual))
	}

	if len(lessThan) != len(idx.expressions) {
		return nil, errInvalidKeys.New(len(idx.expressions), idx.ID(), len(lessThan))
	}

	return newAscendLookup(&filteredLookup{
		id:          idx.ID(),
		index:       idx.index,
		mapping:     idx.mapping,
		expressions: idx.expressions,
	}, greaterOrEqual, lessThan), nil
}

func (idx *pilosaIndex) DescendGreater(keys ...interface{}) (sql.IndexLookup, error) {
	if len(keys) != len(idx.expressions) {
		return nil, errInvalidKeys.New(len(idx.expressions), idx.ID(), len(keys))
	}

	return newDescendLookup(&filteredLookup{
		id:          idx.ID(),
		index:       idx.index,
		mapping:     idx.mapping,
		keys:        keys,
		expressions: idx.expressions,
		reverse:     true,
	}, keys, nil), nil
}

func (idx *pilosaIndex) DescendLessOrEqual(keys ...interface{}) (sql.IndexLookup, error) {
	if len(keys) != len(idx.expressions) {
		return nil, errInvalidKeys.New(len(idx.expressions), idx.ID(), len(keys))
	}

	return newDescendLookup(&filteredLookup{
		id:          idx.ID(),
		index:       idx.index,
		mapping:     idx.mapping,
		keys:        keys,
		expressions: idx.expressions,
		reverse:     true,
	}, nil, keys), nil
}

func (idx *pilosaIndex) DescendRange(lessOrEqual, greaterThan []interface{}) (sql.IndexLookup, error) {
	if len(lessOrEqual) != len(idx.expressions) {
		return nil, errInvalidKeys.New(len(idx.expressions), idx.ID(), len(lessOrEqual))
	}

	if len(greaterThan) != len(idx.expressions) {
		return nil, errInvalidKeys.New(len(idx.expressions), idx.ID(), len(greaterThan))
	}

	return newDescendLookup(&filteredLookup{
		id:          idx.ID(),
		index:       idx.index,
		mapping:     idx.mapping,
		expressions: idx.expressions,
		reverse:     true,
	}, greaterThan, lessOrEqual), nil
}

func (idx *pilosaIndex) Not(keys ...interface{}) (sql.IndexLookup, error) {
	if len(keys) != len(idx.expressions) {
		return nil, errInvalidKeys.New(len(idx.expressions), idx.ID(), len(keys))
	}

	return &negateLookup{
		id:          idx.ID(),
		index:       idx.index,
		mapping:     idx.mapping,
		keys:        keys,
		expressions: idx.expressions,
	}, nil
}

func newAscendLookup(f *filteredLookup, gte []interface{}, lt []interface{}) *ascendLookup {
	l := &ascendLookup{filteredLookup: f, gte: gte, lt: lt}
	if l.filter == nil {
		l.filter = func(i int, value []byte) (bool, error) {
			var v interface{}
			var err error
			if len(l.gte) > 0 {
				v, err = decodeGob(value, l.gte[i])
				if err != nil {
					return false, err
				}

				cmp, err := compare(v, l.gte[i])
				if err != nil {
					return false, err
				}

				if cmp < 0 {
					return false, nil
				}
			}

			if len(l.lt) > 0 {
				if v == nil {
					v, err = decodeGob(value, l.lt[i])
					if err != nil {
						return false, err
					}
				}

				cmp, err := compare(v, l.lt[i])
				if err != nil {
					return false, err
				}

				if cmp >= 0 {
					return false, nil
				}
			}

			return true, nil
		}
	}
	return l
}

func newDescendLookup(f *filteredLookup, gt []interface{}, lte []interface{}) *descendLookup {
	l := &descendLookup{filteredLookup: f, gt: gt, lte: lte}
	if l.filter == nil {
		l.filter = func(i int, value []byte) (bool, error) {
			var v interface{}
			var err error
			if len(l.gt) > 0 {
				v, err = decodeGob(value, l.gt[i])
				if err != nil {
					return false, err
				}

				cmp, err := compare(v, l.gt[i])
				if err != nil {
					return false, err
				}

				if cmp <= 0 {
					return false, nil
				}
			}

			if len(l.lte) > 0 {
				if v == nil {
					v, err = decodeGob(value, l.lte[i])
					if err != nil {
						return false, err
					}
				}

				cmp, err := compare(v, l.lte[i])
				if err != nil {
					return false, err
				}

				if cmp > 0 {
					return false, nil
				}
			}

			return true, nil
		}
	}
	return l
}
