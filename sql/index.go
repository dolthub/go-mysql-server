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
	IsPointLookup bool
	IsEmptyRange  bool
}

var emptyLookup = IndexLookup{}

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
}

// ColumnExpressionType returns a column expression along with its Type.
type ColumnExpressionType struct {
	Expression string
	Type       Type
}
