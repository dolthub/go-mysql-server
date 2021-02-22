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

package plan

import (
	"io"

	"github.com/dolthub/go-mysql-server/sql"
)

// Distinct is a node that ensures all rows that come from it are unique.
type Distinct struct {
	UnaryNode
}

// NewDistinct creates a new Distinct node.
func NewDistinct(child sql.Node) *Distinct {
	return &Distinct{
		UnaryNode: UnaryNode{Child: child},
	}
}

// Resolved implements the Resolvable interface.
func (d *Distinct) Resolved() bool {
	return d.UnaryNode.Child.Resolved()
}

// RowIter implements the Node interface.
func (d *Distinct) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	span, ctx := ctx.Span("plan.Distinct")

	it, err := d.Child.RowIter(ctx, row)
	if err != nil {
		span.Finish()
		return nil, err
	}

	return sql.NewSpanIter(span, newDistinctIter(ctx, it)), nil
}

// WithChildren implements the Node interface.
func (d *Distinct) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(d, len(children), 1)
	}

	return NewDistinct(children[0]), nil
}

func (d Distinct) String() string {
	p := sql.NewTreePrinter()
	_ = p.WriteNode("Distinct")
	_ = p.WriteChildren(d.Child.String())
	return p.String()
}

func (d Distinct) DebugString() string {
	p := sql.NewTreePrinter()
	_ = p.WriteNode("Distinct")
	_ = p.WriteChildren(sql.DebugString(d.Child))
	return p.String()
}

// distinctIter keeps track of the hashes of all rows that have been emitted.
// It does not emit any rows whose hashes have been seen already.
// TODO: come up with a way to use less memory than keeping all hashes in memory.
// Even though they are just 64-bit integers, this could be a problem in large
// result sets.
type distinctIter struct {
	childIter sql.RowIter
	seen      sql.KeyValueCache
	dispose   sql.DisposeFunc
}

func newDistinctIter(ctx *sql.Context, child sql.RowIter) *distinctIter {
	cache, dispose := ctx.Memory.NewHistoryCache()
	return &distinctIter{
		childIter: child,
		seen:      cache,
		dispose:   dispose,
	}
}

func (di *distinctIter) Next() (sql.Row, error) {
	for {
		row, err := di.childIter.Next()
		if err != nil {
			if err == io.EOF {
				di.Dispose()
			}
			return nil, err
		}

		hash, err := sql.HashOf(row)
		if err != nil {
			return nil, err
		}

		if _, err := di.seen.Get(hash); err == nil {
			continue
		}

		if err := di.seen.Put(hash, struct{}{}); err != nil {
			return nil, err
		}

		return row, nil
	}
}

func (di *distinctIter) Close(ctx *sql.Context) error {
	di.Dispose()
	return di.childIter.Close(ctx)
}

func (di *distinctIter) Dispose() {
	if di.dispose != nil {
		di.dispose()
	}
}

// OrderedDistinct is a Distinct node optimized for sorted row sets.
// It's 2 orders of magnitude faster and uses 2 orders of magnitude less memory.
type OrderedDistinct struct {
	UnaryNode
}

// NewOrderedDistinct creates a new OrderedDistinct node.
func NewOrderedDistinct(child sql.Node) *OrderedDistinct {
	return &OrderedDistinct{
		UnaryNode: UnaryNode{Child: child},
	}
}

// Resolved implements the Resolvable interface.
func (d *OrderedDistinct) Resolved() bool {
	return d.UnaryNode.Child.Resolved()
}

// RowIter implements the Node interface.
func (d *OrderedDistinct) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	span, ctx := ctx.Span("plan.OrderedDistinct")

	it, err := d.Child.RowIter(ctx, nil)
	if err != nil {
		span.Finish()
		return nil, err
	}

	return sql.NewSpanIter(span, newOrderedDistinctIter(it, d.Child.Schema())), nil
}

// WithChildren implements the Node interface.
func (d *OrderedDistinct) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(d, len(children), 1)
	}

	return NewOrderedDistinct(children[0]), nil
}

func (d OrderedDistinct) String() string {
	p := sql.NewTreePrinter()
	_ = p.WriteNode("OrderedDistinct")
	_ = p.WriteChildren(d.Child.String())
	return p.String()
}

// orderedDistinctIter iterates the children iterator and skips all the
// repeated rows assuming the iterator has all rows sorted.
type orderedDistinctIter struct {
	childIter sql.RowIter
	schema    sql.Schema
	prevRow   sql.Row
}

func newOrderedDistinctIter(child sql.RowIter, schema sql.Schema) *orderedDistinctIter {
	return &orderedDistinctIter{childIter: child, schema: schema}
}

func (di *orderedDistinctIter) Next() (sql.Row, error) {
	for {
		row, err := di.childIter.Next()
		if err != nil {
			return nil, err
		}

		if di.prevRow != nil {
			ok, err := di.prevRow.Equals(row, di.schema)
			if err != nil {
				return nil, err
			}

			if ok {
				continue
			}
		}

		di.prevRow = row
		return row, nil
	}
}

func (di *orderedDistinctIter) Close(ctx *sql.Context) error {
	return di.childIter.Close(ctx)
}
