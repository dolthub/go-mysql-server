// +build !windows

package pilosa

import (
	"io"

	"github.com/sirupsen/logrus"
	bolt "go.etcd.io/bbolt"
)

type locationValueIter struct {
	locations [][]byte
	pos       int
}

func (i *locationValueIter) Next() ([]byte, error) {
	if i.pos >= len(i.locations) {
		return nil, io.EOF
	}

	i.pos++
	return i.locations[i.pos-1], nil
}

func (i *locationValueIter) Close() error {
	i.locations = nil
	return nil
}

type indexValueIter struct {
	offset    uint64
	total     uint64
	bits      []uint64
	mapping   *mapping
	indexName string

	// share transaction and bucket on all getLocation calls
	bucket *bolt.Bucket
	tx     *bolt.Tx
	closed bool
}

func (it *indexValueIter) Next() ([]byte, error) {
	if it.bucket == nil {
		if err := it.mapping.open(); err != nil {
			return nil, err
		}

		bucket, err := it.mapping.getBucket(it.indexName, false)
		if err != nil {
			_ = it.Close()
			return nil, err
		}

		it.bucket = bucket
		it.tx = bucket.Tx()
	}

	if it.offset >= it.total {
		if err := it.Close(); err != nil {
			logrus.WithField("err", err.Error()).
				Error("unable to close the pilosa index value iterator")
		}

		if it.tx != nil {
			_ = it.tx.Rollback()
		}

		return nil, io.EOF
	}

	var colID uint64
	if it.bits == nil {
		colID = it.offset
	} else {
		colID = it.bits[it.offset]
	}

	it.offset++

	return it.mapping.getLocationFromBucket(it.bucket, colID)
}

func (it *indexValueIter) Close() error {
	if it.closed {
		return nil
	}

	it.closed = true
	if it.tx != nil {
		_ = it.tx.Rollback()
	}

	if it.bucket != nil {
		return it.mapping.close()
	}

	return nil
}
