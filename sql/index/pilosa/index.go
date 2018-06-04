package pilosa

import (
	"errors"
	"io"

	pilosa "github.com/pilosa/go-pilosa"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

// Index is an pilosa implementation of sql.Index interface
type sqlIndex struct {
	path   string
	client *pilosa.Client

	db          string
	table       string
	id          string
	expressions []sql.ExpressionHash
}

// Get returns an IndexLookup for the given key in the index.
// If key parameter is not present then the returned iterator
// will go through all the locations on the index.
func (idx *sqlIndex) Get(key ...interface{}) (sql.IndexLookup, error) {
	name := indexName(idx.Database(), idx.Table(), idx.ID())
	mapping, err := openMapping(idx.path)
	if err != nil {
		return nil, err
	}

	n := len(key)
	if n == 0 {
		// return all locations from mapping
		total, err := mapping.getLocationN(name)
		if err != nil {
			return nil, err
		}

		return &sqlIndexLookupIter{
			total:     uint64(total),
			mapping:   mapping,
			indexName: name,
		}, nil
	}

	// min(len(key), len(idx.expressions))
	if n > len(idx.expressions) {
		n = len(idx.expressions)
	}

	schema, err := idx.client.Schema()
	if err != nil {
		return nil, err
	}
	index, err := schema.Index(name)
	if err != nil {
		return nil, err
	}

	// Compute Intersection of bitmaps
	var bitmaps []*pilosa.PQLBitmapQuery
	for i := 0; i < n; i++ {
		frm, err := index.Frame(frameName(idx.expressions[i]))
		if err != nil {
			return nil, err
		}

		rowID, err := mapping.getRowID(frm.Name(), key[i])
		if err != nil {
			return nil, err
		}

		bitmaps = append(bitmaps, frm.Bitmap(rowID))
	}

	resp, err := idx.client.Query(index.Intersect(bitmaps...))
	if err != nil {
		return nil, err
	}
	if !resp.Success {
		return nil, errors.New(resp.ErrorMessage)
	}
	if resp.Result() == nil {
		return &sqlIndexLookupIter{mapping: mapping, indexName: name}, nil
	}

	bits := resp.Result().Bitmap().Bits
	return &sqlIndexLookupIter{
		total:     uint64(len(bits)),
		bits:      bits,
		mapping:   mapping,
		indexName: name,
	}, nil
}

// Has checks if the given key is present in the index mapping
func (idx *sqlIndex) Has(key ...interface{}) (bool, error) {
	m, err := openMapping(idx.path)
	if err != nil {
		return false, err
	}
	defer m.close()

	n := len(key)
	if n > len(idx.expressions) {
		n = len(idx.expressions)
	}

	// We can make this loop parallel, but does it make sense?
	// For how many (maximum) keys will be asked by one function call?
	for i := 0; i < n; i++ {
		expr := idx.expressions[i]
		name := frameName(expr)

		val, err := m.get(name, key[i])
		if err != nil || val == nil {
			return false, err
		}
	}

	return true, nil
}

// Database returns the database name this index belongs to.
func (idx *sqlIndex) Database() string {
	return idx.db
}

// Table returns the table name this index belongs to.
func (idx *sqlIndex) Table() string {
	return idx.table
}

// ID returns the identifier of the index.
func (idx *sqlIndex) ID() string {
	return idx.id
}

// Expressions returns the indexed expressions. If the result is more than
// one expression, it means the index has multiple columns indexed. If it's
// just one, it means it may be an expression or a column.
func (idx *sqlIndex) ExpressionHashes() []sql.ExpressionHash {
	return idx.expressions
}

// lookup implements sql.IndexLookup and sql.IndexValueIter interface
type sqlIndexLookupIter struct {
	offset    uint64
	total     uint64
	bits      []uint64
	mapping   *mapping
	indexName string
}

func (it *sqlIndexLookupIter) Values() sql.IndexValueIter {
	return it
}

func (it *sqlIndexLookupIter) Next() ([]byte, error) {
	if it.offset >= it.total {
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

func (it *sqlIndexLookupIter) Close() error {
	it.offset, it.total = uint64(0), uint64(0)
	it.bits = nil

	return it.mapping.close()
}
