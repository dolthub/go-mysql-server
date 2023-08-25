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
	"github.com/dolthub/go-mysql-server/sql"
)

// TableAlias is a node that acts as a table with a given name.
type TableAlias struct {
	*UnaryNode
	name    string
	comment string
}

var _ sql.RenameableNode = (*TableAlias)(nil)
var _ sql.CommentedNode = (*TableAlias)(nil)
var _ sql.CollationCoercible = (*TableAlias)(nil)

// NewTableAlias returns a new Table alias node.
func NewTableAlias(name string, node sql.Node) *TableAlias {
	return &TableAlias{UnaryNode: &UnaryNode{Child: node}, name: name}
}

// Name implements the Nameable interface.
func (t *TableAlias) Name() string {
	return t.name
}

func (t *TableAlias) IsReadOnly() bool {
	return t.Child.IsReadOnly()
}

func (t *TableAlias) WithComment(s string) sql.Node {
	ret := *t
	ret.comment = s
	return &ret
}

func (t *TableAlias) Comment() string {
	return t.comment
}

// Schema implements the Node interface. TableAlias alters the schema of its child element to rename the source of
// columns to the alias.
func (t *TableAlias) Schema() sql.Schema {
	childSchema := t.Child.Schema()
	copy := make(sql.Schema, len(childSchema))
	for i, col := range childSchema {
		colCopy := *col
		colCopy.Source = t.name
		copy[i] = &colCopy
	}
	return copy
}

// WithChildren implements the Node interface.
func (t *TableAlias) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(t, len(children), 1)
	}

	return NewTableAlias(t.name, children[0]).WithComment(t.Comment()), nil
}

// CheckPrivileges implements the interface sql.Node.
func (t *TableAlias) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	if t.UnaryNode != nil {
		return t.UnaryNode.Child.CheckPrivileges(ctx, opChecker)
	}
	return true
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (t *TableAlias) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	if t.UnaryNode != nil {
		return sql.GetCoercibility(ctx, t.UnaryNode.Child)
	}
	return sql.Collation_binary, 7
}

func (t TableAlias) String() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("TableAlias(%s)", t.name)
	_ = pr.WriteChildren(t.Child.String())
	return pr.String()
}

func (t TableAlias) DebugString() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("TableAlias(%s)", t.name)
	_ = pr.WriteChildren(sql.DebugString(t.Child))
	return pr.String()
}

func (t TableAlias) WithName(name string) sql.Node {
	t.name = name
	return &t
}
