package plan

import (
	"fmt"

	"github.com/mitchellh/hashstructure"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
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
func (d *Distinct) RowIter(ctx *sql.Context) (sql.RowIter, error) {
	span, ctx := ctx.Span("plan.Distinct")

	it, err := d.Child.RowIter(ctx)
	if err != nil {
		span.Finish()
		return nil, err
	}

	return sql.NewSpanIter(span, newDistinctIter(it)), nil
}

// TransformUp implements the Transformable interface.
func (d *Distinct) TransformUp(f sql.TransformNodeFunc) (sql.Node, error) {
	child, err := d.Child.TransformUp(f)
	if err != nil {
		return nil, err
	}
	return f(NewDistinct(child))
}

// TransformExpressionsUp implements the Transformable interface.
func (d *Distinct) TransformExpressionsUp(f sql.TransformExprFunc) (sql.Node, error) {
	child, err := d.Child.TransformExpressionsUp(f)
	if err != nil {
		return nil, err
	}
	return NewDistinct(child), nil
}

func (d Distinct) String() string {
	p := sql.NewTreePrinter()
	_ = p.WriteNode("Distinct")
	_ = p.WriteChildren(d.Child.String())
	return p.String()
}

// distinctIter keeps track of the hashes of all rows that have been emitted.
// It does not emit any rows whose hashes have been seen already.
// TODO: come up with a way to use less memory than keeping all hashes in mem.
// Even though they are just 64-bit integers, this could be a problem in large
// result sets.
type distinctIter struct {
	childIter sql.RowIter
	seen      map[uint64]struct{}
}

func newDistinctIter(child sql.RowIter) *distinctIter {
	return &distinctIter{
		childIter: child,
		seen:      make(map[uint64]struct{}),
	}
}

func (di *distinctIter) Next() (sql.Row, error) {
	for {
		row, err := di.childIter.Next()
		if err != nil {
			return nil, err
		}

		hash, err := hashstructure.Hash(row, nil)
		if err != nil {
			return nil, fmt.Errorf("unable to hash row: %s", err)
		}

		if _, ok := di.seen[hash]; ok {
			continue
		}

		di.seen[hash] = struct{}{}
		return row, nil
	}
}

func (di *distinctIter) Close() error {
	return di.childIter.Close()
}

// OrderedDistinct is a Distinct node optimized for sorted row sets.
// It's 2 orders of magnitude faster and uses 2 orders of magnitude less mem.
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
func (d *OrderedDistinct) RowIter(ctx *sql.Context) (sql.RowIter, error) {
	span, ctx := ctx.Span("plan.OrderedDistinct")

	it, err := d.Child.RowIter(ctx)
	if err != nil {
		span.Finish()
		return nil, err
	}

	return sql.NewSpanIter(span, newOrderedDistinctIter(it, d.Child.Schema())), nil
}

// TransformUp implements the Transformable interface.
func (d *OrderedDistinct) TransformUp(f sql.TransformNodeFunc) (sql.Node, error) {
	child, err := d.Child.TransformUp(f)
	if err != nil {
		return nil, err
	}
	return f(NewOrderedDistinct(child))
}

// TransformExpressionsUp implements the Transformable interface.
func (d *OrderedDistinct) TransformExpressionsUp(f sql.TransformExprFunc) (sql.Node, error) {
	child, err := d.Child.TransformExpressionsUp(f)
	if err != nil {
		return nil, err
	}
	return NewOrderedDistinct(child), nil
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

func (di *orderedDistinctIter) Close() error {
	return di.childIter.Close()
}
