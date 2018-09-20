package pilosa

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
// - field name: value []byte (gob encoding) -> rowID uint64
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

func (m *mapping) open() error {
	return m.openCreate(false)
}

// openCreate opens and sets creation mode in the database.
func (m *mapping) openCreate(create bool) error {
	m.clientMut.Lock()
	defer m.clientMut.Unlock()
	m.mut.Lock()
	defer m.mut.Unlock()

	if m.clients == 0 && m.db == nil {
		var err error
		m.db, err = bolt.Open(m.path, 0640, nil)
		if err != nil {
			return err
		}
	}

	m.clients++
	m.create = create
	return nil
}

func (m *mapping) close() error {
	m.clientMut.Lock()
	defer m.clientMut.Unlock()
	m.mut.Lock()
	defer m.mut.Unlock()

	if m.clients > 1 {
		m.clients--
		return nil
	}

	m.clients = 0

	if m.db != nil {
		if err := m.db.Close(); err != nil {
			return err
		}
		m.db = nil
	}

	return nil
}

func (m *mapping) rowID(fieldName string, value interface{}) (uint64, error) {
	val, err := m.get(fieldName, value)
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

func (m *mapping) getRowID(fieldName string, value interface{}) (uint64, error) {
	var id uint64
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(value)
	if err != nil {
		return 0, err
	}

	err = m.transaction(true, func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(fieldName))
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

	if err != nil {
		return 0, err
	}

	return id, err
}

func (m *mapping) getMaxRowID(fieldName string) (uint64, error) {
	var id uint64
	err := m.transaction(true, func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(fieldName))
		if b == nil {
			return nil
		}

		id = b.Sequence()
		return nil
	})

	return id, err
}

func (m *mapping) putLocation(indexName string, colID uint64, location []byte) error {
	return m.transaction(true, func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(indexName))
		if err != nil {
			return err
		}

		key := make([]byte, 8)
		binary.LittleEndian.PutUint64(key, colID)

		return b.Put(key, location)
	})
}

func (m *mapping) sortedLocations(indexName string, cols []uint64, reverse bool) ([][]byte, error) {
	var result [][]byte
	m.mut.RLock()
	defer m.mut.RUnlock()
	err := m.db.View(func(tx *bolt.Tx) error {
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

	return location, err
}

func (m *mapping) getLocationFromBucket(
	bucket *bolt.Bucket,
	colID uint64,
) ([]byte, error) {
	key := make([]byte, 8)
	binary.LittleEndian.PutUint64(key, colID)
	return bucket.Get(key), nil
}

func (m *mapping) getBucket(
	indexName string,
	writable bool,
) (*bolt.Bucket, error) {
	var bucket *bolt.Bucket

	tx, err := m.db.Begin(writable)
	if err != nil {
		return nil, err
	}

	bucket = tx.Bucket([]byte(indexName))
	if bucket == nil {
		_ = tx.Rollback()
		return nil, fmt.Errorf("bucket %s not found", indexName)
	}

	return bucket, err
}

func (m *mapping) get(name string, key interface{}) ([]byte, error) {
	var value []byte

	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(key)
	if err != nil {
		return nil, err
	}

	err = m.transaction(true, func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(name))
		if b != nil {
			value = b.Get(buf.Bytes())
			return nil
		}

		return fmt.Errorf("%s not found", name)
	})

	return value, err
}

func (m *mapping) filter(name string, fn func([]byte) (bool, error)) ([]uint64, error) {
	var result []uint64

	m.mut.RLock()
	defer m.mut.RUnlock()
	err := m.db.View(func(tx *bolt.Tx) error {
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

	return result, err
}
