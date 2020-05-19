package sql

import "fmt"

// Index is the basic representation of an index. It can be extended with
// more functionality by implementing more specific interfaces.
type Index interface {
	// Get returns an IndexLookup for the given key in the index.
	Get(key ...interface{}) (IndexLookup, error)
	// Has checks if the given key is present in the index.
	Has(partition Partition, key ...interface{}) (bool, error)
	// ID returns the identifier of the index.
	ID() string
	// Database returns the database name this index belongs to.
	Database() string
	// Table returns the table name this index belongs to.
	Table() string
	// Expressions returns the indexed expressions. If the result is more than
	// one expression, it means the index has multiple columns indexed. If it's
	// just one, it means it may be an expression or a column.
	Expressions() []string
}

// AscendIndex is an index that is sorted in ascending order.
type AscendIndex interface {
	// AscendGreaterOrEqual returns an IndexLookup for keys that are greater
	// or equal to the given keys.
	AscendGreaterOrEqual(keys ...interface{}) (IndexLookup, error)
	// AscendLessThan returns an IndexLookup for keys that are less than the
	// given keys.
	AscendLessThan(keys ...interface{}) (IndexLookup, error)
	// AscendRange returns an IndexLookup for keys that are within the given
	// range.
	AscendRange(greaterOrEqual, lessThan []interface{}) (IndexLookup, error)
}

// DescendIndex is an index that is sorted in descending order.
type DescendIndex interface {
	// DescendGreater returns an IndexLookup for keys that are greater
	// than the given keys.
	DescendGreater(keys ...interface{}) (IndexLookup, error)
	// DescendLessOrEqual returns an IndexLookup for keys that are less than or
	// equal to the given keys.
	DescendLessOrEqual(keys ...interface{}) (IndexLookup, error)
	// DescendRange returns an IndexLookup for keys that are within the given
	// range.
	DescendRange(lessOrEqual, greaterThan []interface{}) (IndexLookup, error)
}

// NegateIndex is an index that supports retrieving negated values.
type NegateIndex interface {
	// Not returns an IndexLookup for keys that are not equal
	// to the given keys.
	Not(keys ...interface{}) (IndexLookup, error)
}

// IndexLookup is the implementation-specific definition of an index lookup, created by calls to Index.Get(). The
// IndexLookup must contain all necessary information to retrieve exactly the rows in the table specified by key(s)
// specified in Index.Get(). Implementors are responsible for all semantics of correctly returning rows that match an
// index lookup. By default, only a single index can be used for a given table access. Implementors can implement the
// Mergeable interface to optionally allow IndexLookups to be merged with others.
type IndexLookup interface {
	fmt.Stringer
}

// SetOperations is a specialization of IndexLookup that enables set operations
// between several IndexLookups.
type SetOperations interface {
	IndexLookup
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
// TODO: merge with SetOperations
type Mergeable interface {
	IndexLookup
	// IsMergeable checks whether the current IndexLookup can be merged with
	// the given one.
	IsMergeable(IndexLookup) bool
}