package pilosa

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"path/filepath"
	"sync"

	"github.com/boltdb/bolt"
)

const (
	mappingFileName = DriverID + "-mapping.db"
)

// mapping
// buckets:
// - index name: columndID uint64 -> location []byte
// - frame name: value []byte (gob encoding) -> rowID uint64
type mapping struct {
	dir string

	mut sync.RWMutex
	db  *bolt.DB

	clientMut sync.Mutex
	clients   int
}

func newMapping(dir string) *mapping {
	return &mapping{dir: dir}
}

func (m *mapping) open() {
	m.clientMut.Lock()
	defer m.clientMut.Unlock()
	m.clients++
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
		m.db, err = bolt.Open(filepath.Join(m.dir, mappingFileName), 0640, nil)
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

func (m *mapping) getRowID(frameName string, value interface{}) (uint64, error) {
	var id uint64
	err := m.query(func() error {
		var buf bytes.Buffer
		enc := gob.NewEncoder(&buf)
		err := enc.Encode(value)
		if err != nil {
			return err
		}

		err = m.db.Update(func(tx *bolt.Tx) error {
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

			id = uint64(b.Stats().KeyN)
			val = make([]byte, 8)
			binary.LittleEndian.PutUint64(val, id)
			err = b.Put(key, val)
			return err
		})

		return err
	})

	return id, err
}

func (m *mapping) putLocation(indexName string, colID uint64, location []byte) error {
	return m.query(func() error {
		return m.db.Update(func(tx *bolt.Tx) error {
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

func (m *mapping) getLocation(indexName string, colID uint64) ([]byte, error) {
	var location []byte

	err := m.query(func() error {
		err := m.db.View(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte(indexName))
			if b == nil {
				return fmt.Errorf("Bucket %s not found", indexName)
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

func (m *mapping) getLocationN(indexName string) (int, error) {
	var n int
	err := m.query(func() error {
		err := m.db.View(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte(indexName))
			if b == nil {
				return fmt.Errorf("Bucket %s not found", indexName)
			}

			n = b.Stats().KeyN
			return nil
		})

		return err
	})
	return n, err
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

		err = m.db.View(func(tx *bolt.Tx) error {
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
