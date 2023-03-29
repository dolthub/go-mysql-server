// Copyright 2022 Dolthub, Inc.
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

package plan

import (
	"errors"
	"fmt"
	"io"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

const cteRecursionLimit = 1000

// RecursiveCte is defined by two subqueries
// connected with a union:
//
//	ex => WITH RECURSIVE [name]([Columns]) as ([Init] UNION [Rec]) ...
//
// [Init] is a non-recursive select statement, and [Rec] selects from
// the recursive relation [name] until exhaustion. Note that if [Rec] is
// not recursive, the optimizer will fold the RecursiveCte into a
// SubqueryAlias.
//
// The node is executed as follows:
//  1. First, iterate the [Init] subquery.
//  2. Collect the outputs of [Init] in a [temporary] buffer.
//  3. When the iterator is exhausted, populate the recursive
//     [working] table with the [temporary] buffer.
//  4. Iterate [Rec], collecting outputs in the [temporary] buffer.
//  5. Repeat steps (3) and (4) until [temporary] is empty.
//
// A RecursiveCte, its [Init], and its [Rec] have the same
// projection count and types. [Init] will be resolved before
// [Rec] or [RecursiveCte] to share schema types.
type RecursiveCte struct {
	union *Union
	// Columns used to name lazily-loaded schema fields
	Columns []string
	// schema will match the types of [Init.Schema()], names of [Columns]
	schema sql.Schema
	// working is a handle to our refreshable intermediate table
	working *RecursiveTable
	name    string
}

var _ sql.Node = (*RecursiveCte)(nil)
var _ sql.Nameable = (*RecursiveCte)(nil)
var _ sql.Expressioner = (*RecursiveCte)(nil)
var _ sql.CollationCoercible = (*RecursiveCte)(nil)

func NewRecursiveCte(initial, recursive sql.Node, name string, outputCols []string, deduplicate bool, l sql.Expression, sf sql.SortFields) *RecursiveCte {
	return &RecursiveCte{
		Columns: outputCols,
		union: &Union{
			BinaryNode: BinaryNode{left: initial, right: recursive},
			Distinct:   deduplicate,
			Limit:      l,
			SortFields: sf,
		},
		name: name,
	}
}

// Name implements sql.Nameable
func (r *RecursiveCte) Name() string {
	return r.name
}

// Left implements sql.BinaryNode
func (r *RecursiveCte) Left() sql.Node {
	return r.union.left
}

// Right implements sql.BinaryNode
func (r *RecursiveCte) Right() sql.Node {
	return r.union.right
}

func (r *RecursiveCte) Union() *Union {
	return r.union
}

// WithSchema inherits [Init]'s schema at resolve time
func (r *RecursiveCte) WithSchema(s sql.Schema) *RecursiveCte {
	nr := *r
	nr.schema = s
	return &nr
}

// WithWorking populates the [working] table with a common schema
func (r *RecursiveCte) WithWorking(t *RecursiveTable) *RecursiveCte {
	nr := *r
	nr.working = t
	return &nr
}

// Schema implements sql.Node
func (r *RecursiveCte) Schema() sql.Schema {
	return r.schema
}

// RowIter implements sql.Node
func (r *RecursiveCte) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	var iter sql.RowIter = &recursiveCteIter{
		init:        r.Left(),
		rec:         r.Right(),
		row:         row,
		working:     r.working,
		temp:        make([]sql.Row, 0),
		deduplicate: r.union.Distinct,
	}
	if r.union.Limit != nil && len(r.union.SortFields) > 0 {
		limit, err := getInt64Value(ctx, r.union.Limit)
		if err != nil {
			return nil, err
		}
		iter = newTopRowsIter(r.union.SortFields, limit, false, iter)
	} else if r.union.Limit != nil {
		limit, err := getInt64Value(ctx, r.union.Limit)
		if err != nil {
			return nil, err
		}
		iter = &limitIter{limit: limit, childIter: iter}
	} else if len(r.union.SortFields) > 0 {
		iter = newSortIter(r.union.SortFields, iter)
	}
	return iter, nil
}

// WithChildren implements sql.Node
func (r *RecursiveCte) WithChildren(children ...sql.Node) (sql.Node, error) {
	ret := *r
	u, err := r.union.WithChildren(children...)
	if err != nil {
		return nil, err
	}
	ret.union = u.(*Union)
	return &ret, nil
}

func (r *RecursiveCte) Opaque() bool {
	return true
}

func (r *RecursiveCte) Resolved() bool {
	return r.union.Resolved()
}

func (r *RecursiveCte) Children() []sql.Node {
	return r.union.Children()
}

func (r *RecursiveCte) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return r.union.CheckPrivileges(ctx, opChecker)
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*RecursiveCte) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 7
}

func (r *RecursiveCte) Expressions() []sql.Expression {
	return r.union.Expressions()
}

func (r *RecursiveCte) WithExpressions(exprs ...sql.Expression) (sql.Node, error) {
	ret := *r
	u, err := r.union.WithExpressions(exprs...)
	if err != nil {
		return nil, err
	}
	ret.union = u.(*Union)
	return &ret, nil
}

// String implements sql.Node
func (r *RecursiveCte) String() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("RecursiveCTE")
	pr.WriteChildren(r.union.String())
	return pr.String()
}

// DebugString implements sql.Node
func (r *RecursiveCte) DebugString() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("RecursiveCTE")
	pr.WriteChildren(sql.DebugString(r.union))
	return pr.String()
}

// Type implements sql.Node
func (r *RecursiveCte) Type() sql.Type {
	cols := r.schema
	if len(cols) == 1 {
		return cols[0].Type
	}
	ts := make([]sql.Type, len(cols))
	for i, c := range cols {
		ts[i] = c.Type
	}
	return types.CreateTuple(ts...)
}

// IsNullable implements sql.Node
func (r *RecursiveCte) IsNullable() bool {
	return true
}

// recursiveCteIter exhaustively executes a recursive
// relation [rec] populated by an [init] base case.
// Refer to RecursiveCte for more details.
type recursiveCteIter struct {
	// base sql.Project
	init sql.Node
	// recursive sql.Project
	rec sql.Node
	// anchor to recursive table to repopulate with [temp]
	working *RecursiveTable
	// true if UNION, false if UNION ALL
	deduplicate bool
	// parent iter initialization state
	row sql.Row

	// active iterator, either [init].RowIter or [rec].RowIter
	iter sql.RowIter
	// number of recursive iterations finished
	cycle int
	// buffer to collect intermediate results for next recursion
	temp []sql.Row
	// duplicate lookup if [deduplicated] set
	cache sql.KeyValueCache
}

var _ sql.RowIter = (*recursiveCteIter)(nil)

// Next implements sql.RowIter
func (r *recursiveCteIter) Next(ctx *sql.Context) (sql.Row, error) {
	if r.iter == nil {
		// start with [Init].RowIter
		var err error
		if r.deduplicate {
			r.cache = sql.NewMapCache()

		}
		r.iter, err = r.init.RowIter(ctx, r.row)
		if err != nil {
			return nil, err
		}
	}

	var row sql.Row
	for {
		var err error
		row, err = r.iter.Next(ctx)
		if errors.Is(err, io.EOF) && len(r.temp) > 0 {
			// reset [Rec].RowIter
			err = r.resetIter(ctx)
			if err != nil {
				return nil, err
			}
			continue
		} else if err != nil {
			return nil, err
		}

		var key uint64
		if r.deduplicate {
			key, _ = sql.HashOf(row)
			if k, _ := r.cache.Get(key); k != nil {
				// skip duplicate
				continue
			}
		}
		r.store(row, key)
		if err != nil {
			return nil, err
		}
		break
	}
	return row, nil
}

// store saves a row to the [temp] buffer, and hashes if [deduplicated] = true
func (r *recursiveCteIter) store(row sql.Row, key uint64) {
	if r.deduplicate {
		r.cache.Put(key, struct{}{})
	}
	r.temp = append(r.temp, row)
	return
}

// resetIter creates a new [Rec].RowIter after refreshing the [working] RecursiveTable
func (r *recursiveCteIter) resetIter(ctx *sql.Context) error {
	if len(r.temp) == 0 {
		return io.EOF
	}
	r.cycle++
	if r.cycle > cteRecursionLimit {
		return sql.ErrCteRecursionLimitExceeded.New()
	}

	if r.working != nil {
		r.working.buf = r.temp
		r.temp = make([]sql.Row, 0)
	}

	err := r.iter.Close(ctx)
	if err != nil {
		return err
	}
	r.iter, err = r.rec.RowIter(ctx, r.row)
	if err != nil {
		return err
	}
	return nil
}

// Close implements sql.RowIter
func (r *recursiveCteIter) Close(ctx *sql.Context) error {
	r.working.buf = nil
	r.temp = nil
	if r.iter != nil {
		return r.iter.Close(ctx)
	}
	return nil
}

func NewRecursiveTable(n string, s sql.Schema) *RecursiveTable {
	return &RecursiveTable{
		name:   n,
		schema: s,
	}
}

// RecursiveTable is a thin wrapper around an in memory
// buffer for use with recursiveCteIter.
type RecursiveTable struct {
	name   string
	schema sql.Schema
	buf    []sql.Row
}

var _ sql.Node = (*RecursiveTable)(nil)
var _ sql.CollationCoercible = (*RecursiveTable)(nil)

func (r *RecursiveTable) Resolved() bool {
	return true
}

func (r *RecursiveTable) Name() string {
	return r.name
}

func (r *RecursiveTable) String() string {
	return fmt.Sprintf("RecursiveTable(%s)", r.name)
}

func (r *RecursiveTable) Schema() sql.Schema {
	return r.schema
}

func (r *RecursiveTable) Children() []sql.Node {
	return nil
}

func (r *RecursiveTable) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	return &recursiveTableIter{buf: r.buf}, nil
}

func (r *RecursiveTable) WithChildren(node ...sql.Node) (sql.Node, error) {
	return r, nil
}

// CheckPrivileges implements the interface sql.Node.
func (r *RecursiveTable) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return true
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*RecursiveTable) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 7
}

var _ sql.Node = (*RecursiveTable)(nil)

// TODO a queue is probably more optimal
type recursiveTableIter struct {
	pos int
	buf []sql.Row
}

var _ sql.RowIter = (*recursiveTableIter)(nil)

func (r *recursiveTableIter) Next(ctx *sql.Context) (sql.Row, error) {
	if r.buf == nil || r.pos >= len(r.buf) {
		return nil, io.EOF
	}
	r.pos++
	return r.buf[r.pos-1], nil
}

func (r *recursiveTableIter) Close(ctx *sql.Context) error {
	r.buf = nil
	return nil
}
