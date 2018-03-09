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
func (d *Distinct) RowIter(session sql.Session) (sql.RowIter, error) {
	it, err := d.Child.RowIter(session)
	if err != nil {
		return nil, err
	}
	return newDistinctIter(it), nil
}

// TransformUp implements the Transformable interface.
func (d *Distinct) TransformUp(f func(sql.Node) (sql.Node, error)) (sql.Node, error) {
	child, err := d.Child.TransformUp(f)
	if err != nil {
		return nil, err
	}
	return f(NewDistinct(child))
}

// TransformExpressionsUp implements the Transformable interface.
func (d *Distinct) TransformExpressionsUp(f func(sql.Expression) (sql.Expression, error)) (sql.Node, error) {
	child, err := d.Child.TransformExpressionsUp(f)
	if err != nil {
		return nil, err
	}
	return NewDistinct(child), nil
}

// distinctIter keeps track of the hashes of all rows that have been emitted.
// It does not emit any rows whose hashes have been seen already.
// TODO: come up with a way to use less memory than keeping all hashes in mem.
// Even though they are just 64-bit integers, this could be a problem in large
// result sets.
type distinctIter struct {
	currentPos int64
	childIter  sql.RowIter
	seen       map[uint64]struct{}
}

func newDistinctIter(child sql.RowIter) *distinctIter {
	return &distinctIter{
		currentPos: 0,
		childIter:  child,
		seen:       make(map[uint64]struct{}),
	}
}

func (di *distinctIter) Next() (sql.Row, error) {
	for {
		childRow, err := di.childIter.Next()
		di.currentPos++
		if err != nil {
			return nil, err
		}

		hash, err := hashstructure.Hash(childRow, nil)
		if err != nil {
			return nil, fmt.Errorf("unable to hash row: %s", err)
		}

		if _, ok := di.seen[hash]; ok {
			continue
		}

		di.seen[hash] = struct{}{}
		return childRow, nil
	}
}

func (di *distinctIter) Close() error {
	return di.childIter.Close()
}
