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

import (
	"fmt"
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
	// Comment returns the comment for this index
	Comment() string
	// IndexType returns the type of this index, e.g. BTREE
	IndexType() string
	// IsGenerated returns whether this index was generated. Generated indexes
	// are used for index access, but are not displayed (such as with SHOW INDEXES).
	IsGenerated() bool
	// NewLookup returns a new IndexLookup for the ranges given. Ranges represent filters over columns. Each Range
	// is ordered by the column expressions (as returned by Expressions) with the RangeColumnExpr representing the
	// searchable area for each column expression. Each Range given will not overlap with any other ranges. Additionally,
	// all ranges will have the same length, and may represent a partial index (matching a prefix rather than the entire
	// index). If an integrator is unable to process the given ranges, then a nil may be returned. An error should be
	// returned only in the event that an error occurred.
	NewLookup(ctx *Context, ranges ...Range) (IndexLookup, error)
	// ColumnExpressionTypes returns each expression and its associated Type. Each expression string should exactly
	// match the string returned from Index.Expressions().
	ColumnExpressionTypes(ctx *Context) []ColumnExpressionType
}

// IndexLookup is the implementation-specific definition of an index lookup. The IndexLookup must contain all necessary
// information to retrieve exactly the rows in the table as specified by the ranges given to their parent index.
// Implementors are responsible for all semantics of correctly returning rows that match an index lookup.
type IndexLookup interface {
	fmt.Stringer
	// Index returns the index that created this IndexLookup.
	Index() Index
	// Ranges returns each Range that created this IndexLookup.
	Ranges() RangeCollection
}

// ColumnExpressionType returns a column expression along with its Type.
type ColumnExpressionType struct {
	Expression string
	Type       Type
}
