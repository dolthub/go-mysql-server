package sql

import "io"

// IndexKeyValueIter is an iterator of index key values, that is, a tuple of
// the values that will be index keys.
type IndexKeyValueIter interface {
	// Next returns the next tuple of index key values. The length of the
	// returned slice will be the same as the number of columns used to
	// create this iterator.
	Next() ([]interface{}, error)
	io.Closer
}

// IndexValueIter is an iterator of index values.
type IndexValueIter interface {
	// Next returns the next index value.
	Next() (interface{}, error)
	io.Closer
}

// Index is the basic representation of an index. It can be extended with
// more functionality by implementing more specific interfaces.
type Index interface {
	// Get returns an IndexLookup for the given key in the index.
	Get(key interface{}) (IndexLookup, error)
	// Has checks if the given key is present in the index.
	Has(key interface{}) (bool, error)
	// ID returns the identifier of the index.
	ID() string
	// Expression returns the indexed expression.
	Expression() Expression
}

// AscendIndex is an index that is sorted in ascending order.
type AscendIndex interface {
	// AscendGreaterOrEqual returns an IndexLookup for keys that are greater
	// or equal to the given key.
	AscendGreaterOrEqual(key interface{}) (IndexLookup, error)
	// AscendLessThan returns an IndexLookup for keys that are less than the
	// given key.
	AscendLessThan(key interface{}) (IndexLookup, error)
	// AscendRange returns an IndexLookup for keys that are within the given
	// range.
	AscendRange(greaterOrEqual, lessThan interface{}) (IndexLookup, error)
}

// DescendIndex is an index that is sorted in descending order.
type DescendIndex interface {
	// DescendGreater returns an IndexLookup for keys that are greater
	// than the given key.
	DescendGreater(key interface{}) (IndexLookup, error)
	// DescendLessOrEqual returns an IndexLookup for keys that are less than or
	// equal to the given key.
	DescendLessOrEqual(key interface{}) (IndexLookup, error)
	// DescendRange returns an IndexLookup for keys that are within the given
	// range.
	DescendRange(lessOrEqual, greaterThan interface{}) (IndexLookup, error)
}

// IndexLookup is a subset of an index. More specific interfaces can be
// implemented to grant more capabilities to the index lookup.
type IndexLookup interface {
	// Values returns the values in the subset of the index.
	Values() IndexValueIter
}

// SetOperations is a specialization of IndexLookup that enables set operations
// between several IndexLookups.
type SetOperations interface {
	// Intersection returns a new index subset with the intersection of the
	// current IndexLookup and the ones given.
	Intersection(...IndexLookup) IndexLookup
	// Union returns a new index subset with the union of the current
	// IndexLookup and the ones given.
	Union(...IndexLookup) IndexLookup
	// Difference returns a new index subset with the difference between the
	// current IndexLookup and the ones given.
	Difference(...IndexLookup) IndexLookup
}

// Mergeable is a specialization of IndexLookup to check if an IndexLookup can
// be merged with another one.
type Mergeable interface {
	// IsMergeable checks whether the current IndexLookup can be merged with
	// the given one.
	IsMergeable(IndexLookup) bool
}

// IndexLoader is the piece that loads indexes from disk.
type IndexLoader interface {
	// ID returns the unique name of the index loader.
	ID() string
	// Load the index at the given path.
	Load(path string) (Index, error)
}

// IndexSaver is the piece that stores indexes in disk.
type IndexSaver interface {
	// ID returns the unique name of the index saver.
	ID() string
	// Save the given index at the given path.
	Save(index Index, path string) error
}
