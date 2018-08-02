package pilosalib

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io"
	"sort"
	"sync"

	"github.com/boltdb/bolt"
)

// mapping
// buckets:
// - index name: columndID uint64 -> location []byte
// - frame name: value []byte (gob encoding) -> rowID uint64
type mapping struct {
	path string

	mut sync.RWMutex
	db  *bolt.DB

	// in create mode there's only one transaction closed explicitly by
	// commit function
	create bool
	tx     *bolt.Tx

	clientMut sync.Mutex
	clients   int
}

func newMapping(path string) *mapping {
	return &mapping{path: path}
}

func (m *mapping) open() {
	m.openCreate(false)
}

// openCreate opens and sets creation mode in the database.
func (m *mapping) openCreate(create bool) {
	m.clientMut.Lock()
	defer m.clientMut.Unlock()
	m.clients++
	m.create = create
}

func (m *mapping) close() error {
	m.clientMut.Lock()
	defer m.clientMut.Unlock()

	if m.clients > 1 {
		m.clients--
		return nil
	}

	m.clients = 0

	m.mut.Lock()
	defer m.mut.Unlock()

	if m.db != nil {
		if err := m.db.Close(); err != nil {
			return err
		}
		m.db = nil
	}

	return nil
}

func (m *mapping) query(fn func() error) error {
	m.mut.Lock()
	if m.db == nil {
		var err error
		m.db, err = bolt.Open(m.path, 0640, nil)
		if err != nil {
			m.mut.Unlock()
			return err
		}
	}
	m.mut.Unlock()

	m.mut.RLock()
	defer m.mut.RUnlock()
	return fn()
}

func (m *mapping) rowID(frameName string, value interface{}) (uint64, error) {
	val, err := m.get(frameName, value)
	if err != nil {
		return 0, err
	}
	if val == nil {
		return 0, io.EOF
	}

	return binary.LittleEndian.Uint64(val), err
}

// commit saves current transaction, if cont is true a new transaction will be
// created again in the next query. Only for create mode.
func (m *mapping) commit(cont bool) error {
	m.clientMut.Lock()
	defer m.clientMut.Unlock()

	var err error
	if m.create && m.tx != nil {
		err = m.tx.Commit()
	}

	m.create = cont
	m.tx = nil

	return err
}

func (m *mapping) rollback() error {
	m.clientMut.Lock()
	defer m.clientMut.Unlock()

	var err error
	if m.create && m.tx != nil {
		err = m.tx.Rollback()
	}

	m.create = false
	m.tx = nil

	return err
}

func (m *mapping) transaction(writable bool, f func(*bolt.Tx) error) error {
	var tx *bolt.Tx
	var err error
	if m.create {
		m.clientMut.Lock()
		if m.tx == nil {
			m.tx, err = m.db.Begin(true)
			if err != nil {
				m.clientMut.Unlock()
				return err
			}
		}

		m.clientMut.Unlock()

		tx = m.tx
	} else {
		tx, err = m.db.Begin(writable)
		if err != nil {
			return err
		}
	}

	err = f(tx)

	if m.create {
		return err
	}

	if err != nil {
		tx.Rollback()
		return err
	}

	return tx.Commit()
}

func (m *mapping) getRowID(frameName string, value interface{}) (uint64, error) {
	var id uint64
	err := m.query(func() error {
		var buf bytes.Buffer
		enc := gob.NewEncoder(&buf)
		err := enc.Encode(value)
		if err != nil {
			return err
		}

		err = m.transaction(true, func(tx *bolt.Tx) error {
			b, err := tx.CreateBucketIfNotExists([]byte(frameName))
			if err != nil {
				return err
			}

			key := buf.Bytes()
			val := b.Get(key)
			if val != nil {
				id = binary.LittleEndian.Uint64(val)
				return nil
			}

			// the first NextSequence is 1 so the first id will be 1
			// this can only fail if the transaction is closed
			id, _ = b.NextSequence()

			val = make([]byte, 8)
			binary.LittleEndian.PutUint64(val, id)
			err = b.Put(key, val)
			return err
		})

		return err
	})

	return id, err
}

func (m *mapping) getMaxRowID(frameName string) (uint64, error) {
	var id uint64
	err := m.query(func() error {
		return m.transaction(true, func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte(frameName))
			if b == nil {
				return nil
			}

			id = b.Sequence()
			return nil
		})
	})

	return id, err
}

func (m *mapping) putLocation(indexName string, colID uint64, location []byte) error {
	return m.query(func() error {
		return m.transaction(true, func(tx *bolt.Tx) error {
			b, err := tx.CreateBucketIfNotExists([]byte(indexName))
			if err != nil {
				return err
			}

			key := make([]byte, 8)
			binary.LittleEndian.PutUint64(key, colID)

			return b.Put(key, location)
		})
	})
}

func (m *mapping) sortedLocations(indexName string, cols []uint64, reverse bool) ([][]byte, error) {
	var result [][]byte
	err := m.query(func() error {
		return m.db.View(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte(indexName))
			if b == nil {
				return fmt.Errorf("bucket %s not found", indexName)
			}

			for _, col := range cols {
				key := make([]byte, 8)
				binary.LittleEndian.PutUint64(key, col)
				val := b.Get(key)

				// val will point to mmap addresses, so we need to copy the slice
				dst := make([]byte, len(val))
				copy(dst, val)
				result = append(result, dst)
			}

			return nil
		})
	})

	if err != nil {
		return nil, err
	}

	if reverse {
		sort.Stable(sort.Reverse(byBytes(result)))
	} else {
		sort.Stable(byBytes(result))
	}

	return result, nil
}

type byBytes [][]byte

func (b byBytes) Len() int           { return len(b) }
func (b byBytes) Swap(i, j int)      { b[i], b[j] = b[j], b[i] }
func (b byBytes) Less(i, j int) bool { return bytes.Compare(b[i], b[j]) < 0 }

func (m *mapping) getLocation(indexName string, colID uint64) ([]byte, error) {
	var location []byte

	err := m.query(func() error {
		err := m.transaction(true, func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte(indexName))
			if b == nil {
				return fmt.Errorf("bucket %s not found", indexName)
			}

			key := make([]byte, 8)
			binary.LittleEndian.PutUint64(key, colID)

			location = b.Get(key)
			return nil
		})

		return err
	})

	return location, err
}

func (m *mapping) getLocationFromBucket(
	bucket *bolt.Bucket,
	colID uint64,
) ([]byte, error) {
	var location []byte

	err := m.query(func() error {
		key := make([]byte, 8)
		binary.LittleEndian.PutUint64(key, colID)

		location = bucket.Get(key)
		return nil
	})

	return location, err
}

func (m *mapping) getBucket(
	indexName string,
	writable bool,
) (*bolt.Bucket, error) {
	var bucket *bolt.Bucket

	err := m.query(func() error {
		tx, err := m.db.Begin(writable)
		if err != nil {
			return err
		}

		bucket = tx.Bucket([]byte(indexName))
		if bucket == nil {
			tx.Rollback()
			return fmt.Errorf("bucket %s not found", indexName)
		}

		return nil
	})

	return bucket, err
}

func (m *mapping) get(name string, key interface{}) ([]byte, error) {
	var value []byte

	err := m.query(func() error {
		var buf bytes.Buffer
		enc := gob.NewEncoder(&buf)
		err := enc.Encode(key)
		if err != nil {
			return err
		}

		err = m.transaction(true, func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte(name))
			if b != nil {
				value = b.Get(buf.Bytes())
				return nil
			}

			return fmt.Errorf("%s not found", name)
		})

		return err
	})
	return value, err
}

func (m *mapping) filter(name string, fn func([]byte) (bool, error)) ([]uint64, error) {
	var result []uint64

	err := m.query(func() error {
		return m.db.View(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte(name))
			if b == nil {
				return nil
			}

			return b.ForEach(func(k, v []byte) error {
				ok, err := fn(k)
				if err != nil {
					return err
				}

				if ok {
					result = append(result, binary.LittleEndian.Uint64(v))
				}

				return nil
			})
		})
	})

	return result, err
}
