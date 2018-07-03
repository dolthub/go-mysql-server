package pilosa

import (
	"bytes"
	"encoding/gob"
	"io"
	"strings"
	"time"

	"gopkg.in/src-d/go-errors.v1"

	"github.com/boltdb/bolt"
	pilosa "github.com/pilosa/go-pilosa"
	"github.com/sirupsen/logrus"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/index"
)

// pilosaIndex is an pilosa implementation of sql.Index interface
type pilosaIndex struct {
	path    string
	client  *pilosa.Client
	mapping *mapping

	db          string
	table       string
	id          string
	expressions []sql.ExpressionHash
}

func newPilosaIndex(path string, client *pilosa.Client, cfg *index.Config) *pilosaIndex {
	return &pilosaIndex{
		path:        path,
		client:      client,
		db:          cfg.DB,
		table:       cfg.Table,
		id:          cfg.ID,
		expressions: cfg.ExpressionHashes(),
		mapping:     newMapping(path),
	}
}

var (
	errInvalidKeys = errors.NewKind("expecting %d keys for index %q, got %d")
	errPilosaQuery = errors.NewKind("error executing pilosa query: %s")
)

// Get returns an IndexLookup for the given key in the index.
// If key parameter is not present then the returned iterator
// will go through all the locations on the index.
func (idx *pilosaIndex) Get(keys ...interface{}) (sql.IndexLookup, error) {
	if len(keys) != len(idx.expressions) {
		return nil, errInvalidKeys.New(len(idx.expressions), idx.ID(), len(keys))
	}

	return &indexLookup{
		indexName:   indexName(idx.Database(), idx.Table(), idx.ID()),
		mapping:     idx.mapping,
		keys:        keys,
		client:      idx.client,
		expressions: idx.expressions,
	}, nil
}

// Has checks if the given key is present in the index mapping
func (idx *pilosaIndex) Has(key ...interface{}) (bool, error) {
	idx.mapping.open()
	defer idx.mapping.close()

	n := len(key)
	if n > len(idx.expressions) {
		n = len(idx.expressions)
	}

	// We can make this loop parallel, but does it make sense?
	// For how many (maximum) keys will be asked by one function call?
	for i := 0; i < n; i++ {
		expr := idx.expressions[i]
		name := frameName(expr)

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
func (idx *pilosaIndex) ExpressionHashes() []sql.ExpressionHash {
	return idx.expressions
}

func (pilosaIndex) Driver() string { return DriverID }

func (idx *pilosaIndex) AscendGreaterOrEqual(keys ...interface{}) (sql.IndexLookup, error) {
	if len(keys) != len(idx.expressions) {
		return nil, errInvalidKeys.New(len(idx.expressions), idx.ID(), len(keys))
	}

	return &ascendLookup{
		filteredLookup: &filteredLookup{
			indexName:   indexName(idx.Database(), idx.Table(), idx.ID()),
			mapping:     idx.mapping,
			client:      idx.client,
			expressions: idx.expressions,
		},
		gte: keys,
		lt:  nil,
	}, nil
}

func (idx *pilosaIndex) AscendLessThan(keys ...interface{}) (sql.IndexLookup, error) {
	if len(keys) != len(idx.expressions) {
		return nil, errInvalidKeys.New(len(idx.expressions), idx.ID(), len(keys))
	}

	return &ascendLookup{
		filteredLookup: &filteredLookup{
			indexName:   indexName(idx.Database(), idx.Table(), idx.ID()),
			mapping:     idx.mapping,
			client:      idx.client,
			expressions: idx.expressions,
		},
		gte: nil,
		lt:  keys,
	}, nil
}

func (idx *pilosaIndex) AscendRange(greaterOrEqual, lessThan []interface{}) (sql.IndexLookup, error) {
	if len(greaterOrEqual) != len(idx.expressions) {
		return nil, errInvalidKeys.New(len(idx.expressions), idx.ID(), len(greaterOrEqual))
	}

	if len(lessThan) != len(idx.expressions) {
		return nil, errInvalidKeys.New(len(idx.expressions), idx.ID(), len(lessThan))
	}

	return &ascendLookup{
		filteredLookup: &filteredLookup{
			indexName:   indexName(idx.Database(), idx.Table(), idx.ID()),
			mapping:     idx.mapping,
			client:      idx.client,
			expressions: idx.expressions,
		},
		gte: greaterOrEqual,
		lt:  lessThan,
	}, nil
}

func (idx *pilosaIndex) DescendGreater(keys ...interface{}) (sql.IndexLookup, error) {
	if len(keys) != len(idx.expressions) {
		return nil, errInvalidKeys.New(len(idx.expressions), idx.ID(), len(keys))
	}

	return &descendLookup{
		filteredLookup: &filteredLookup{
			indexName:   indexName(idx.Database(), idx.Table(), idx.ID()),
			mapping:     idx.mapping,
			client:      idx.client,
			expressions: idx.expressions,
			reverse:     true,
		},
		gt:  keys,
		lte: nil,
	}, nil
}

func (idx *pilosaIndex) DescendLessOrEqual(keys ...interface{}) (sql.IndexLookup, error) {
	if len(keys) != len(idx.expressions) {
		return nil, errInvalidKeys.New(len(idx.expressions), idx.ID(), len(keys))
	}

	return &descendLookup{
		filteredLookup: &filteredLookup{
			indexName:   indexName(idx.Database(), idx.Table(), idx.ID()),
			mapping:     idx.mapping,
			client:      idx.client,
			expressions: idx.expressions,
			reverse:     true,
		},
		gt:  nil,
		lte: keys,
	}, nil
}

func (idx *pilosaIndex) DescendRange(lessOrEqual, greaterThan []interface{}) (sql.IndexLookup, error) {
	if len(lessOrEqual) != len(idx.expressions) {
		return nil, errInvalidKeys.New(len(idx.expressions), idx.ID(), len(lessOrEqual))
	}

	if len(greaterThan) != len(idx.expressions) {
		return nil, errInvalidKeys.New(len(idx.expressions), idx.ID(), len(greaterThan))
	}

	return &descendLookup{
		filteredLookup: &filteredLookup{
			indexName:   indexName(idx.Database(), idx.Table(), idx.ID()),
			mapping:     idx.mapping,
			client:      idx.client,
			expressions: idx.expressions,
			reverse:     true,
		},
		gt:  greaterThan,
		lte: lessOrEqual,
	}, nil
}

type indexLookup struct {
	keys        []interface{}
	indexName   string
	mapping     *mapping
	client      *pilosa.Client
	expressions []sql.ExpressionHash
}

func (l *indexLookup) Values() (sql.IndexValueIter, error) {
	l.mapping.open()

	schema, err := l.client.Schema()
	if err != nil {
		return nil, err
	}

	index, err := schema.Index(l.indexName)
	if err != nil {
		return nil, err
	}

	// Compute Intersection of bitmaps
	var bitmaps []*pilosa.PQLBitmapQuery
	for i := 0; i < len(l.keys); i++ {
		frm, err := index.Frame(frameName(l.expressions[i]))
		if err != nil {
			return nil, err
		}

		rowID, err := l.mapping.rowID(frm.Name(), l.keys[i])
		if err != nil {
			return nil, err
		}

		bitmaps = append(bitmaps, frm.Bitmap(rowID))
	}

	resp, err := l.client.Query(index.Intersect(bitmaps...))
	if err != nil {
		return nil, err
	}

	if !resp.Success {
		return nil, errPilosaQuery.New(resp.ErrorMessage)
	}

	if resp.Result() == nil {
		return &indexValueIter{mapping: l.mapping, indexName: l.indexName}, nil
	}

	bits := resp.Result().Bitmap().Bits
	return &indexValueIter{
		total:     uint64(len(bits)),
		bits:      bits,
		mapping:   l.mapping,
		indexName: l.indexName,
	}, nil
}

type filteredLookup struct {
	indexName   string
	mapping     *mapping
	client      *pilosa.Client
	expressions []sql.ExpressionHash
	reverse     bool
}

func (l *filteredLookup) values(filter func(int, []byte) (bool, error)) (sql.IndexValueIter, error) {
	l.mapping.open()
	defer l.mapping.close()

	schema, err := l.client.Schema()
	if err != nil {
		return nil, err
	}

	index, err := schema.Index(l.indexName)
	if err != nil {
		return nil, err
	}

	// Compute Intersection of bitmaps
	var bitmaps []*pilosa.PQLBitmapQuery
	for i := 0; i < len(l.expressions); i++ {
		frm, err := index.Frame(frameName(l.expressions[i]))
		if err != nil {
			return nil, err
		}

		rows, err := l.mapping.filter(frm.Name(), func(b []byte) (bool, error) {
			return filter(i, b)
		})

		if err != nil {
			return nil, err
		}

		var bs []*pilosa.PQLBitmapQuery
		for _, row := range rows {
			bs = append(bs, frm.Bitmap(row))
		}

		bitmaps = append(bitmaps, index.Union(bs...))
	}

	resp, err := l.client.Query(index.Intersect(bitmaps...))
	if err != nil {
		return nil, err
	}

	if !resp.Success {
		return nil, errPilosaQuery.New(resp.ErrorMessage)
	}

	if resp.Result() == nil {
		return &indexValueIter{mapping: l.mapping, indexName: l.indexName}, nil
	}

	bits := resp.Result().Bitmap().Bits
	locations, err := l.mapping.sortedLocations(l.indexName, bits, l.reverse)
	if err != nil {
		return nil, err
	}

	return &locationValueIter{locations: locations}, nil
}

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

type ascendLookup struct {
	*filteredLookup
	gte []interface{}
	lt  []interface{}
}

func (l *ascendLookup) Values() (sql.IndexValueIter, error) {
	return l.values(func(i int, value []byte) (bool, error) {
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
	})
}

type descendLookup struct {
	*filteredLookup
	gt  []interface{}
	lte []interface{}
}

func (l *descendLookup) Values() (sql.IndexValueIter, error) {
	return l.values(func(i int, value []byte) (bool, error) {
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
	})
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
}

func (it *indexValueIter) Next() ([]byte, error) {
	if it.bucket == nil {
		bucket, err := it.mapping.getBucket(it.indexName, false)
		if err != nil {
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
			it.tx.Rollback()
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
	if it.tx != nil {
		it.tx.Rollback()
	}

	return it.mapping.close()
}

var (
	errUnknownType  = errors.NewKind("unknown type %T received as value")
	errTypeMismatch = errors.NewKind("cannot compare type %T with type %T")
)

func decodeGob(k []byte, value interface{}) (interface{}, error) {
	decoder := gob.NewDecoder(bytes.NewBuffer(k))

	switch value.(type) {
	case string:
		var v string
		err := decoder.Decode(&v)
		return v, err
	case int32:
		var v int32
		err := decoder.Decode(&v)
		return v, err
	case int64:
		var v int64
		err := decoder.Decode(&v)
		return v, err
	case uint32:
		var v uint32
		err := decoder.Decode(&v)
		return v, err
	case uint64:
		var v uint64
		err := decoder.Decode(&v)
		return v, err
	case float64:
		var v float64
		err := decoder.Decode(&v)
		return v, err
	case time.Time:
		var v time.Time
		err := decoder.Decode(&v)
		return v, err
	case []byte:
		var v []byte
		err := decoder.Decode(&v)
		return v, err
	case bool:
		var v bool
		err := decoder.Decode(&v)
		return v, err
	case []interface{}:
		var v []interface{}
		err := decoder.Decode(&v)
		return v, err
	default:
		return nil, errUnknownType.New(value)
	}
}

// compare two values of the same underlying type. The values MUST be of the
// same type.
func compare(a, b interface{}) (int, error) {
	switch a := a.(type) {
	case bool:
		v, ok := b.(bool)
		if !ok {
			return 0, errTypeMismatch.New(a, b)
		}

		if a == v {
			return 0, nil
		}

		if a == false {
			return -1, nil
		}

		return 1, nil
	case string:
		v, ok := b.(string)
		if !ok {
			return 0, errTypeMismatch.New(a, b)
		}

		return strings.Compare(a, v), nil
	case int32:
		v, ok := b.(int32)
		if !ok {
			return 0, errTypeMismatch.New(a, b)
		}

		if a == v {
			return 0, nil
		}

		if a < v {
			return -1, nil
		}

		return 1, nil
	case int64:
		v, ok := b.(int64)
		if !ok {
			return 0, errTypeMismatch.New(a, b)
		}

		if a == v {
			return 0, nil
		}

		if a < v {
			return -1, nil
		}

		return 1, nil
	case uint32:
		v, ok := b.(uint32)
		if !ok {
			return 0, errTypeMismatch.New(a, b)
		}

		if a == v {
			return 0, nil
		}

		if a < v {
			return -1, nil
		}

		return 1, nil
	case uint64:
		v, ok := b.(uint64)
		if !ok {
			return 0, errTypeMismatch.New(a, b)
		}

		if a == v {
			return 0, nil
		}

		if a < v {
			return -1, nil
		}

		return 1, nil
	case float64:
		v, ok := b.(float64)
		if !ok {
			return 0, errTypeMismatch.New(a, b)
		}

		if a == v {
			return 0, nil
		}

		if a < v {
			return -1, nil
		}

		return 1, nil
	case []byte:
		v, ok := b.([]byte)
		if !ok {
			return 0, errTypeMismatch.New(a, b)
		}
		return bytes.Compare(a, v), nil
	case []interface{}:
		v, ok := b.([]interface{})
		if !ok {
			return 0, errTypeMismatch.New(a, b)
		}

		if len(a) < len(v) {
			return -1, nil
		}

		if len(a) > len(v) {
			return 1, nil
		}

		for i := range a {
			cmp, err := compare(a[i], v[i])
			if err != nil {
				return 0, err
			}

			if cmp != 0 {
				return cmp, nil
			}
		}

		return 0, nil
	case time.Time:
		v, ok := b.(time.Time)
		if !ok {
			return 0, errTypeMismatch.New(a, b)
		}

		if a.Equal(v) {
			return 0, nil
		}

		if a.Before(v) {
			return -1, nil
		}

		return 1, nil
	default:
		return 0, errUnknownType.New(a)
	}
}
