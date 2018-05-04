package pilosa

import (
	"io"

	"gopkg.in/src-d/go-errors.v1"

	pilosa "github.com/pilosa/go-pilosa"
	"github.com/sirupsen/logrus"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
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

		rowID, err := l.mapping.getRowID(frm.Name(), l.keys[i])
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

type indexValueIter struct {
	offset    uint64
	total     uint64
	bits      []uint64
	mapping   *mapping
	indexName string
}

func (it *indexValueIter) Next() ([]byte, error) {
	if it.offset >= it.total {
		if err := it.Close(); err != nil {
			logrus.WithField("err", err.Error()).
				Error("unable to close the pilosa index value iterator")
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

	return it.mapping.getLocation(it.indexName, colID)
}

func (it *indexValueIter) Close() error { return it.mapping.close() }
