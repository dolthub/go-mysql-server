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

type IndexDef struct {
	Name       string
	Columns    []IndexColumn
	Constraint IndexConstraint
	Storage    IndexUsing
	Comment    string
}

// IndexColumn is the column by which to add to an index.
type IndexColumn struct {
	Name string
	// Length represents the index prefix length. If zero, then no length was specified.
	Length int64
}

// IndexConstraint represents any constraints that should be applied to the index.
type IndexConstraint byte

const (
	IndexConstraint_None IndexConstraint = iota
	IndexConstraint_Unique
	IndexConstraint_Fulltext
	IndexConstraint_Spatial
	IndexConstraint_Primary
)

// IndexUsing is the desired storage type.
type IndexUsing byte

const (
	IndexUsing_Default IndexUsing = iota
	IndexUsing_BTree
	IndexUsing_Hash
)

// Index is the representation of an index, and also creates an IndexLookup when given a collection of ranges.
type Index interface {
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
	// IsSpatial returns whether this index is a spatial index
	IsSpatial() bool
	// IsFullText returns whether this index is a Full-Text index
	IsFullText() bool
	// Comment returns the comment for this index
	Comment() string
	// IndexType returns the type of this index, e.g. BTREE
	IndexType() string
	// IsGenerated returns whether this index was generated. Generated indexes
	// are used for index access, but are not displayed (such as with SHOW INDEXES).
	IsGenerated() bool
	// ColumnExpressionTypes returns each expression and its associated Type.
	// Each expression string should exactly match the string returned from
	// Index.Expressions().
	ColumnExpressionTypes() []ColumnExpressionType
	// CanSupport returns whether this index supports lookups on the given
	// range filters.
	CanSupport(...Range) bool
	// PrefixLengths returns the prefix lengths for each column in this index
	PrefixLengths() []uint16
}

// ExtendedIndex is an extension of Index, that allows access to appended primary keys. MySQL internally represents an
// index as the collection of all explicitly referenced columns, while appending any unreferenced primary keys to the
// end (in order of their declaration). For full MySQL compatibility, integrators are encouraged to mimic this, however
// not all implementations may define their indexes (on tables with primary keys) in this way, therefore this interface
// is optional.
type ExtendedIndex interface {
	Index
	// ExtendedExpressions returns the same result as Expressions, but appends any primary keys that are implicitly in
	// the index. The appended primary keys are in declaration order.
	ExtendedExpressions() []string
	// ExtendedColumnExpressionTypes returns the same result as ColumnExpressionTypes, but appends the type of any
	// primary keys that are implicitly in the index. The appended primary keys are in declaration order.
	ExtendedColumnExpressionTypes() []ColumnExpressionType
}

// IndexLookup is the implementation-specific definition of an index lookup. The IndexLookup must contain all necessary
// information to retrieve exactly the rows in the table as specified by the ranges given to their parent index.
// Implementors are responsible for all semantics of correctly returning rows that match an index lookup.
type IndexLookup struct {
	Index  Index
	Ranges RangeCollection
	// IsPointLookup is true if the lookup will return one or zero
	// values; the range is null safe, the index is unique, every index
	// column has a range expression, and every range expression is an
	// exact equality.
	IsPointLookup   bool
	IsEmptyRange    bool
	IsSpatialLookup bool
	IsReverse       bool
}

var emptyLookup = IndexLookup{}

func NewIndexLookup(idx Index, ranges RangeCollection, isPointLookup, isEmptyRange, isSpatialLookup, isReverse bool) IndexLookup {
	if isReverse {
		for i, j := 0, len(ranges)-1; i < j; i, j = i+1, j-1 {
			ranges[i], ranges[j] = ranges[j], ranges[i]
		}
	}
	return IndexLookup{
		Index:           idx,
		Ranges:          ranges,
		IsPointLookup:   isPointLookup,
		IsEmptyRange:    isEmptyRange,
		IsSpatialLookup: isSpatialLookup,
		IsReverse:       isReverse,
	}
}

func (il IndexLookup) IsEmpty() bool {
	return il.Index == nil
}

func (il IndexLookup) String() string {
	pr := NewTreePrinter()
	_ = pr.WriteNode("IndexLookup")
	pr.WriteChildren(fmt.Sprintf("index: %s", il.Index), fmt.Sprintf("ranges: %s", il.Ranges.String()))
	return pr.String()
}

func (il IndexLookup) DebugString() string {
	pr := NewTreePrinter()
	_ = pr.WriteNode("IndexLookup")
	pr.WriteChildren(fmt.Sprintf("index: %s", il.Index), fmt.Sprintf("ranges: %s", il.Ranges.DebugString()))
	return pr.String()
}

// FilteredIndex is an extension of |Index| that allows an index to declare certain filter predicates handled,
// allowing them to be removed from the overall plan for greater execution efficiency
type FilteredIndex interface {
	Index
	// HandledFilters returns a subset of |filters| that are satisfied
	// by index lookups to this index.
	HandledFilters(filters []Expression) (handled []Expression)
}

type IndexOrder byte

const (
	IndexOrderNone IndexOrder = iota
	IndexOrderAsc
	IndexOrderDesc
)

// OrderedIndex is an extension of |Index| that allows indexes to declare their return order. The query engine can
// optimize certain queries if the order of an index is guaranteed, e.g. removing a sort operation.
type OrderedIndex interface {
	Index
	// Order returns the order of results for reads from this index
	Order() IndexOrder
	// Reversible returns whether or not this index can be iterated on backwards
	Reversible() bool
}

// ColumnExpressionType returns a column expression along with its Type.
type ColumnExpressionType struct {
	Expression string
	Type       Type
}
