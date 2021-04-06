// Copyright 2020-2021 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
	// IsUnique returns whether this index is unique
	IsUnique() bool
	// Comment returns the comment for this index
	Comment() string
	// IndexType returns the type of this index, e.g. BTREE
	IndexType() string
	// IsGenerated returns whether this index was generated. Generated indexes
	// are used for index access, but are not displayed (such as with SHOW INDEXES).
	IsGenerated() bool
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

// MergeableIndexLookup is a specialization of IndexLookup that allows IndexLookups to be merged together. This allows
// multiple indexes to be used for a single table access, if the implementor can support it via merging the lookups of
// the two indexes.
type MergeableIndexLookup interface {
	IndexLookup
	// IsMergeable checks whether the current IndexLookup can be merged with the given one. If true, then all other
	// methods in the interface are expected to return a new IndexLookup that represents the given set operation of the
	// two operands.
	IsMergeable(IndexLookup) bool
	// Intersection returns a new IndexLookup with the intersection of the current IndexLookup and the ones given.
	Intersection(...IndexLookup) (IndexLookup, error)
	// Union returns a new IndexLookup with the union of the current IndexLookup and the ones given.
	Union(...IndexLookup) (IndexLookup, error)
}
