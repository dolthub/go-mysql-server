package pilosa

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"path/filepath"

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
	db *bolt.DB
}

func openMapping(dir string) (*mapping, error) {
	db, err := bolt.Open(filepath.Join(dir, mappingFileName), 0640, nil)
	if err != nil {
		return nil, err
	}

	return &mapping{db: db}, nil
}

func (m *mapping) close() error {
	if m.db != nil {
		return m.db.Close()
	}

	return nil
}

func (m *mapping) getRowID(frameName string, value interface{}) (uint64, error) {
	var (
		id  uint64
		buf bytes.Buffer
	)

	enc := gob.NewEncoder(&buf)
	err := enc.Encode(value)
	if err != nil {
		return id, err
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

	return id, err
}

func (m *mapping) putLocation(indexName string, colID uint64, location []byte) error {
	return m.db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(indexName))
		if err != nil {
			return err
		}

		key := make([]byte, 8)
		binary.LittleEndian.PutUint64(key, colID)

		return b.Put(key, location)
	})
}

func (m *mapping) getLocation(indexName string, colID uint64) ([]byte, error) {
	var location []byte

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

	return location, err
}

func (m *mapping) getLocationN(indexName string) (int, error) {
	n := 0
	err := m.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(indexName))
		if b == nil {
			return fmt.Errorf("Bucket %s not found", indexName)
		}

		n = b.Stats().KeyN
		return nil
	})

	return n, err
}

func (m *mapping) get(name string, key interface{}) ([]byte, error) {
	var (
		value []byte
		buf   bytes.Buffer
	)

	enc := gob.NewEncoder(&buf)
	err := enc.Encode(key)
	if err != nil {
		return nil, err
	}

	err = m.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(name))
		if b != nil {
			value = b.Get(buf.Bytes())
			return nil
		}

		return fmt.Errorf("%s not found", name)
	})

	return value, err
}
