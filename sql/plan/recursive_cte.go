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
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

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
	union *SetOp
	// Columns used to name lazily-loaded schema fields
	Columns []string
	// schema will match the types of [Init.Schema()], names of [Columns]
	schema sql.Schema
	// Working is a handle to our refreshable intermediate table
	Working *RecursiveTable
	name    string
}

var _ sql.Node = (*RecursiveCte)(nil)
var _ sql.Nameable = (*RecursiveCte)(nil)
var _ sql.Expressioner = (*RecursiveCte)(nil)
var _ sql.CollationCoercible = (*RecursiveCte)(nil)

func NewRecursiveCte(initial, recursive sql.Node, name string, outputCols []string, deduplicate bool, l sql.Expression, sf sql.SortFields) *RecursiveCte {
	return &RecursiveCte{
		Columns: outputCols,
		union: &SetOp{
			SetOpType:  UnionType,
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

func (r *RecursiveCte) IsReadOnly() bool {
	return r.union.BinaryNode.left.IsReadOnly() && r.union.BinaryNode.right.IsReadOnly()
}

// Left implements sql.BinaryNode
func (r *RecursiveCte) Left() sql.Node {
	return r.union.left
}

// Right implements sql.BinaryNode
func (r *RecursiveCte) Right() sql.Node {
	return r.union.right
}

func (r *RecursiveCte) Union() *SetOp {
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
	nr.Working = t
	return &nr
}

// Schema implements sql.Node
func (r *RecursiveCte) Schema() sql.Schema {
	return r.schema
}

// WithChildren implements sql.Node
func (r *RecursiveCte) WithChildren(children ...sql.Node) (sql.Node, error) {
	ret := *r
	s, err := r.union.WithChildren(children...)
	if err != nil {
		return nil, err
	}
	ret.union = s.(*SetOp)
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
	s, err := r.union.WithExpressions(exprs...)
	if err != nil {
		return nil, err
	}
	ret.union = s.(*SetOp)
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
	Buf    []sql.Row
}

var _ sql.Node = (*RecursiveTable)(nil)
var _ sql.CollationCoercible = (*RecursiveTable)(nil)

func (r *RecursiveTable) Resolved() bool {
	return true
}

func (r *RecursiveTable) Name() string {
	return r.name
}

func (r *RecursiveTable) IsReadOnly() bool {
	return true
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
