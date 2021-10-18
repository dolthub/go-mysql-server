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
	"fmt"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
)

// CreateView is a node representing the creation (or replacement) of a view,
// which is defined by the Child node. The Columns member represent the
// explicit columns specified by the query, if any.
type CreateView struct {
	UnaryNode
	database   sql.Database
	Name       string
	Columns    []string
	IsReplace  bool
	Definition *SubqueryAlias
}

// NewCreateView creates a CreateView node with the specified parameters,
// setting its catalog to nil.
func NewCreateView(
	database sql.Database,
	name string,
	columns []string,
	definition *SubqueryAlias,
	isReplace bool,
) *CreateView {
	return &CreateView{
		UnaryNode:  UnaryNode{Child: definition},
		database:   database,
		Name:       name,
		Columns:    columns,
		IsReplace:  isReplace,
		Definition: definition,
	}
}

// View returns the view that will be created by this node.
func (cv *CreateView) View() *sql.View {
	return cv.Definition.AsView()
}

// Children implements the Node interface. It returns the Child of the
// CreateView node; i.e., the definition of the view that will be created.
func (cv *CreateView) Children() []sql.Node {
	return []sql.Node{cv.Child}
}

// Resolved implements the Node interface. This node is resolved if and only if
// the database and the Child are both resolved.
func (cv *CreateView) Resolved() bool {
	_, ok := cv.database.(sql.UnresolvedDatabase)
	return !ok && cv.Child.Resolved()
}

// RowIter implements the Node interface. When executed, this function creates
// (or replaces) the view. It can error if the CraeteView's IsReplace member is
// set to false and the view already exists. The RowIter returned is always
// empty.
func (cv *CreateView) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	view := cv.View()
	registry := ctx.GetViewRegistry()

	if cv.IsReplace {
		if dropper, ok := cv.database.(sql.ViewDatabase); ok {
			err := dropper.DropView(ctx, cv.Name)
			if err != nil && !sql.ErrViewDoesNotExist.Is(err) {
				return sql.RowsToRowIter(), err
			}
		} else {
			err := registry.Delete(cv.database.Name(), view.Name())
			if err != nil && !sql.ErrViewDoesNotExist.Is(err) {
				return sql.RowsToRowIter(), err
			}
		}
	}

	creator, ok := cv.database.(sql.ViewDatabase)
	if ok {
		return sql.RowsToRowIter(), creator.CreateView(ctx, cv.Name, cv.Definition.TextDefinition)
	} else {
		return sql.RowsToRowIter(), registry.Register(cv.database.Name(), view)
	}
}

// Schema implements the Node interface. It always returns nil.
func (cv *CreateView) Schema() sql.Schema { return nil }

// String implements the fmt.Stringer interface, using sql.TreePrinter to
// generate the string.
func (cv *CreateView) String() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("CreateView(%s)", cv.Name)
	_ = pr.WriteChildren(
		fmt.Sprintf("Columns (%s)", strings.Join(cv.Columns, ", ")),
		cv.Child.String(),
	)
	return pr.String()
}

// WithChildren implements the Node interface. It only succeeds if the length
// of the specified children equals 1.
func (cv *CreateView) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(cv, len(children), 1)
	}

	newCreate := *cv
	newCreate.Child = children[0]
	return &newCreate, nil
}

// Database implements the Databaser interface, and it returns the database in
// which CreateView will create the view.
func (cv *CreateView) Database() sql.Database {
	return cv.database
}

// WithDatabase implements the Databaser interface, and it returns a copy of this
// node with the specified database.
func (cv *CreateView) WithDatabase(database sql.Database) (sql.Node, error) {
	newCreate := *cv
	newCreate.database = database
	return &newCreate, nil
}
