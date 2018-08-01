package pilosa

import (
	errors "gopkg.in/src-d/go-errors.v1"

	pilosa "github.com/pilosa/go-pilosa"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/index"
)

// pilosaIndex is an pilosa implementation of sql.Index interface
type pilosaIndex struct {
	client  *pilosa.Client
	mapping *mapping

	db          string
	table       string
	id          string
	expressions []string
}

func newPilosaIndex(mappingfile string, client *pilosa.Client, cfg *index.Config) *pilosaIndex {
	return &pilosaIndex{
		client:      client,
		db:          cfg.DB,
		table:       cfg.Table,
		id:          cfg.ID,
		expressions: cfg.Expressions,
		mapping:     newMapping(mappingfile),
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

	schema, err := idx.client.Schema()
	if err != nil {
		return nil, err
	}
	index, err := schema.Index(indexName(idx.Database(), idx.Table()))
	if err != nil {
		return nil, err
	}

	return &indexLookup{
		id:          idx.ID(),
		client:      idx.client,
		index:       index,
		mapping:     idx.mapping,
		keys:        keys,
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
	for i, expr := range idx.expressions {
		name := frameName(idx.ID(), expr)

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

func (pilosaIndex) Driver() string { return DriverID }

func (idx *pilosaIndex) AscendGreaterOrEqual(keys ...interface{}) (sql.IndexLookup, error) {
	if len(keys) != len(idx.expressions) {
		return nil, errInvalidKeys.New(len(idx.expressions), idx.ID(), len(keys))
	}

	schema, err := idx.client.Schema()
	if err != nil {
		return nil, err
	}
	index, err := schema.Index(indexName(idx.Database(), idx.Table()))
	if err != nil {
		return nil, err
	}

	l := &ascendLookup{
		filteredLookup: &filteredLookup{
			id:          idx.ID(),
			client:      idx.client,
			index:       index,
			mapping:     idx.mapping,
			keys:        keys,
			expressions: idx.expressions,
		},
		gte: keys,
		lt:  nil,
	}
	l.initFilter()

	return l, nil
}

func (idx *pilosaIndex) AscendLessThan(keys ...interface{}) (sql.IndexLookup, error) {
	if len(keys) != len(idx.expressions) {
		return nil, errInvalidKeys.New(len(idx.expressions), idx.ID(), len(keys))
	}

	schema, err := idx.client.Schema()
	if err != nil {
		return nil, err
	}
	index, err := schema.Index(indexName(idx.Database(), idx.Table()))
	if err != nil {
		return nil, err
	}

	l := &ascendLookup{
		filteredLookup: &filteredLookup{
			id:          idx.ID(),
			client:      idx.client,
			index:       index,
			mapping:     idx.mapping,
			keys:        keys,
			expressions: idx.expressions,
		},
		gte: nil,
		lt:  keys,
	}
	l.initFilter()

	return l, nil
}

func (idx *pilosaIndex) AscendRange(greaterOrEqual, lessThan []interface{}) (sql.IndexLookup, error) {
	if len(greaterOrEqual) != len(idx.expressions) {
		return nil, errInvalidKeys.New(len(idx.expressions), idx.ID(), len(greaterOrEqual))
	}

	if len(lessThan) != len(idx.expressions) {
		return nil, errInvalidKeys.New(len(idx.expressions), idx.ID(), len(lessThan))
	}

	schema, err := idx.client.Schema()
	if err != nil {
		return nil, err
	}
	index, err := schema.Index(indexName(idx.Database(), idx.Table()))
	if err != nil {
		return nil, err
	}

	l := &ascendLookup{
		filteredLookup: &filteredLookup{
			id:          idx.ID(),
			client:      idx.client,
			index:       index,
			mapping:     idx.mapping,
			expressions: idx.expressions,
		},
		gte: greaterOrEqual,
		lt:  lessThan,
	}
	l.initFilter()

	return l, nil
}

func (idx *pilosaIndex) DescendGreater(keys ...interface{}) (sql.IndexLookup, error) {
	if len(keys) != len(idx.expressions) {
		return nil, errInvalidKeys.New(len(idx.expressions), idx.ID(), len(keys))
	}

	schema, err := idx.client.Schema()
	if err != nil {
		return nil, err
	}
	index, err := schema.Index(indexName(idx.Database(), idx.Table()))
	if err != nil {
		return nil, err
	}

	l := &descendLookup{
		filteredLookup: &filteredLookup{
			id:          idx.ID(),
			client:      idx.client,
			index:       index,
			mapping:     idx.mapping,
			keys:        keys,
			expressions: idx.expressions,
			reverse:     true,
		},
		gt:  keys,
		lte: nil,
	}
	l.initFilter()

	return l, nil
}

func (idx *pilosaIndex) DescendLessOrEqual(keys ...interface{}) (sql.IndexLookup, error) {
	if len(keys) != len(idx.expressions) {
		return nil, errInvalidKeys.New(len(idx.expressions), idx.ID(), len(keys))
	}

	schema, err := idx.client.Schema()
	if err != nil {
		return nil, err
	}
	index, err := schema.Index(indexName(idx.Database(), idx.Table()))
	if err != nil {
		return nil, err
	}

	l := &descendLookup{
		filteredLookup: &filteredLookup{
			id:          idx.ID(),
			client:      idx.client,
			index:       index,
			mapping:     idx.mapping,
			keys:        keys,
			expressions: idx.expressions,
			reverse:     true,
		},
		gt:  nil,
		lte: keys,
	}
	l.initFilter()

	return l, nil
}

func (idx *pilosaIndex) DescendRange(lessOrEqual, greaterThan []interface{}) (sql.IndexLookup, error) {
	if len(lessOrEqual) != len(idx.expressions) {
		return nil, errInvalidKeys.New(len(idx.expressions), idx.ID(), len(lessOrEqual))
	}

	if len(greaterThan) != len(idx.expressions) {
		return nil, errInvalidKeys.New(len(idx.expressions), idx.ID(), len(greaterThan))
	}

	schema, err := idx.client.Schema()
	if err != nil {
		return nil, err
	}
	index, err := schema.Index(indexName(idx.Database(), idx.Table()))
	if err != nil {
		return nil, err
	}

	l := &descendLookup{
		filteredLookup: &filteredLookup{
			id:          idx.ID(),
			client:      idx.client,
			index:       index,
			mapping:     idx.mapping,
			expressions: idx.expressions,
			reverse:     true,
		},
		gt:  greaterThan,
		lte: lessOrEqual,
	}
	l.initFilter()

	return l, nil
}

func (idx *pilosaIndex) Not(keys ...interface{}) (sql.IndexLookup, error) {
	if len(keys) != len(idx.expressions) {
		return nil, errInvalidKeys.New(len(idx.expressions), idx.ID(), len(keys))
	}

	schema, err := idx.client.Schema()
	if err != nil {
		return nil, err
	}

	index, err := schema.Index(indexName(idx.Database(), idx.Table()))
	if err != nil {
		return nil, err
	}

	return &negateLookup{
		id:          idx.ID(),
		client:      idx.client,
		index:       index,
		mapping:     idx.mapping,
		keys:        keys,
		expressions: idx.expressions,
	}, nil
}
