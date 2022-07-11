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

type TransformedNamedNode struct {
	UnaryNode
	name string
}

// TransformedNamedNode is a wrapper for arbitrary logic to represent a table
// factor assembled from other nodes at some point in by the analyzer. See
// e.g., Concat.
func NewTransformedNamedNode(child sql.Node, name string) *TransformedNamedNode {
	return &TransformedNamedNode{UnaryNode{child}, name}
}

func (n *TransformedNamedNode) Name() string {
	return n.name
}

func (n *TransformedNamedNode) Schema() sql.Schema {
	return n.Child.Schema()
}

func (n *TransformedNamedNode) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	return n.Child.RowIter(ctx, row)
}

func (n *TransformedNamedNode) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(n, len(children), 1)
	}
	return NewTransformedNamedNode(children[0], n.name), nil
}

// CheckPrivileges implements the interface sql.Node.
func (n *TransformedNamedNode) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return n.Child.CheckPrivileges(ctx, opChecker)
}

func (n *TransformedNamedNode) String() string {
	return n.Child.String()
}

func (n *TransformedNamedNode) DebugString() string {
	return sql.DebugString(n.Child)
}
