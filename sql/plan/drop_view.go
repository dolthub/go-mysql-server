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
	errors "gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/sql"
)

var errDropViewChild = errors.NewKind("any child of DropView must be of type SingleDropView")

type SingleDropView struct {
	database sql.Database
	viewName string
}

// NewSingleDropView creates a SingleDropView.
func NewSingleDropView(
	database sql.Database,
	viewName string,
) *SingleDropView {
	return &SingleDropView{database, viewName}
}

// Children implements the Node interface. It always returns nil.
func (dv *SingleDropView) Children() []sql.Node {
	return nil
}

// Resolved implements the Node interface. This node is resolved if and only if
// its database is resolved.
func (dv *SingleDropView) Resolved() bool {
	_, ok := dv.database.(sql.UnresolvedDatabase)
	return !ok
}

// RowIter implements the Node interface. It always returns an empty iterator.
func (dv *SingleDropView) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	return sql.RowsToRowIter(), nil
}

// Schema implements the Node interface. It always returns nil.
func (dv *SingleDropView) Schema() sql.Schema { return nil }

// String implements the fmt.Stringer interface, using sql.TreePrinter to
// generate the string.
func (dv *SingleDropView) String() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("SingleDropView(%s.%s)", dv.database.Name(), dv.viewName)

	return pr.String()
}

// WithChildren implements the Node interface. It only succeeds if the length
// of the specified children equals 0.
func (dv *SingleDropView) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(dv, len(children), 0)
	}

	return dv, nil
}

// Database implements the sql.Databaser interface. It returns the node's database.
func (dv *SingleDropView) Database() sql.Database {
	return dv.database
}

// WithDatabase implements the sql.Databaser interface, and it returns a copy of this
// node with the specified database.
func (dv *SingleDropView) WithDatabase(database sql.Database) (sql.Node, error) {
	newDrop := *dv
	newDrop.database = database
	return &newDrop, nil
}

// DropView is a node representing the removal of a list of views, defined by
// the children member. The flag ifExists represents whether the user wants the
// node to fail if any of the views in children does not exist.
type DropView struct {
	children []sql.Node
	ifExists bool
}

// NewDropView creates a DropView node with the specified parameters,
// setting its catalog to nil.
func NewDropView(children []sql.Node, ifExists bool) *DropView {
	return &DropView{children: children, ifExists: ifExists}
}

// Children implements the Node interface. It returns the children of the
// CreateView node; i.e., all the views that will be dropped.
func (dvs *DropView) Children() []sql.Node {
	return dvs.children
}

// Resolved implements the Node interface. This node is resolved if and only if
// all of its children are resolved.
func (dvs *DropView) Resolved() bool {
	for _, child := range dvs.children {
		if !child.Resolved() {
			return false
		}
	}
	return true
}

// RowIter implements the Node interface. When executed, this function drops
// all the views defined by the node's children. It errors if the flag ifExists
// is set to false and there is some view that does not exist.
func (dvs *DropView) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	for _, child := range dvs.children {
		drop, ok := child.(*SingleDropView)
		if !ok {
			return sql.RowsToRowIter(), errDropViewChild.New()
		}

		if dropper, ok := drop.database.(sql.ViewDatabase); ok {
			err := dropper.DropView(ctx, drop.viewName)
			if err != nil {
				allowedError := dvs.ifExists && sql.ErrViewDoesNotExist.Is(err)
				if !allowedError {
					return sql.RowsToRowIter(), err
				}
			}
		} else {
			err := ctx.GetViewRegistry().Delete(drop.database.Name(), drop.viewName)
			allowedError := dvs.ifExists && sql.ErrViewDoesNotExist.Is(err)
			if !allowedError {
				return sql.RowsToRowIter(), err
			}
		}
	}

	return sql.RowsToRowIter(), nil
}

// Schema implements the Node interface. It always returns nil.
func (dvs *DropView) Schema() sql.Schema { return nil }

// String implements the fmt.Stringer interface, using sql.TreePrinter to
// generate the string.
func (dvs *DropView) String() string {
	childrenStrings := make([]string, len(dvs.children))
	for i, child := range dvs.children {
		childrenStrings[i] = child.String()
	}

	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("DropView")
	_ = pr.WriteChildren(childrenStrings...)

	return pr.String()
}

// WithChildren implements the Node interface. It always suceeds, returning a
// copy of this node with the new array of nodes as children.
func (dvs *DropView) WithChildren(children ...sql.Node) (sql.Node, error) {
	newDrop := dvs
	newDrop.children = children
	return newDrop, nil
}
