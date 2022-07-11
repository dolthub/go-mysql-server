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
	"github.com/dolthub/go-mysql-server/sql/transform"
)

// IsUnary returns whether the node is unary or not.
func IsUnary(node sql.Node) bool {
	return len(node.Children()) == 1
}

// IsBinary returns whether the node is binary or not.
func IsBinary(node sql.Node) bool {
	return len(node.Children()) == 2
}

// NillaryNode is a node with no children. This is a common WithChildren implementation for all nodes that have none.
func NillaryWithChildren(node sql.Node, children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(node, len(children), 0)
	}
	return node, nil
}

// UnaryNode is a node that has only one child.
type UnaryNode struct {
	Child sql.Node
}

// Schema implements the Node interface.
func (n *UnaryNode) Schema() sql.Schema {
	return n.Child.Schema()
}

// Resolved implements the Resolvable interface.
func (n UnaryNode) Resolved() bool {
	return n.Child.Resolved()
}

// Children implements the Node interface.
func (n UnaryNode) Children() []sql.Node {
	return []sql.Node{n.Child}
}

// BinaryNode is a node with two children.
type BinaryNode struct {
	left  sql.Node
	right sql.Node
}

func (n BinaryNode) Left() sql.Node {
	return n.left
}

func (n BinaryNode) Right() sql.Node {
	return n.right
}

// Children implements the Node interface.
func (n BinaryNode) Children() []sql.Node {
	return []sql.Node{n.left, n.right}
}

// Resolved implements the Resolvable interface.
func (n BinaryNode) Resolved() bool {
	return n.left.Resolved() && n.right.Resolved()
}

// BlockRowIter is an iterator that produces rows. It is an extended interface over RowIter. This is primarily used
// by block statements. In order to track the schema of a sql.RowIter from nested blocks, this extended row iter returns
// the relevant information inside of the iter itself. In addition, the most specific top-level Node for that iter is
// returned, as stored procedures use that Node to determine whether the iter represents a SELECT statement.
type BlockRowIter interface {
	sql.RowIter
	// RepresentingNode returns the Node that most directly represents this RowIter. For example, in the case of
	// an IF/ELSE block, the RowIter represents the Node where the condition evaluated to true.
	RepresentingNode() sql.Node
	// Schema returns the schema of this RowIter.
	Schema() sql.Schema
}

// nodeRepresentsSelect attempts to walk a sql.Node to determine if it represents a SELECT statement.
func nodeRepresentsSelect(s sql.Node) bool {
	if s == nil {
		return false
	}
	isSelect := false
	// All SELECT statements, including those that do not specify a table (using "dual"), have a ResolvedTable.
	transform.Inspect(s, func(node sql.Node) bool {
		switch node.(type) {
		case *AlterAutoIncrement, *AlterIndex, *CreateForeignKey, *CreateIndex, *CreateTable, *CreateTrigger,
			*DeleteFrom, *DropForeignKey, *InsertInto, *ShowCreateTable, *ShowIndexes, *Truncate, *Update, *Into:
			return false
		case *ResolvedTable, *ProcedureResolvedTable:
			isSelect = true
			return false
		default:
			return true
		}
	})
	return isSelect
}

// getTableName attempts to fetch the table name from the node. If not found directly on the node, searches the
// children. Returns the first table name found, regardless of whether there are more, therefore this is only intended
// to be used in situations where only a single table is expected to be found.
func getTableName(nodeToSearch sql.Node) string {
	nodeStack := []sql.Node{nodeToSearch}
	for len(nodeStack) > 0 {
		node := nodeStack[len(nodeStack)-1]
		nodeStack = nodeStack[:len(nodeStack)-1]
		switch n := node.(type) {
		case *TableAlias:
			if n.UnaryNode != nil {
				nodeStack = append(nodeStack, n.UnaryNode.Child)
				continue
			}
		case *ResolvedTable:
			return n.Table.Name()
		case *UnresolvedTable:
			return n.name
		case *IndexedTableAccess:
			return n.Name()
		case sql.TableWrapper:
			return n.Underlying().Name()
		}
		nodeStack = append(nodeStack, node.Children()...)
	}
	return ""
}

// getDatabaseName attempts to fetch the database name from the node. If not found directly on the node, searches the
// children. Returns the first database name found, regardless of whether there are more, therefore this is only
// intended to be used in situations where only a single database is expected to be found. Unlike how tables are handled
// in most nodes, databases may be stored as a string field therefore there will be situations where a database name
// exists on a node, but cannot be found through inspection.
func getDatabaseName(nodeToSearch sql.Node) string {
	nodeStack := []sql.Node{nodeToSearch}
	for len(nodeStack) > 0 {
		node := nodeStack[len(nodeStack)-1]
		nodeStack = nodeStack[:len(nodeStack)-1]
		switch n := node.(type) {
		case sql.Databaser:
			return n.Database().Name()
		case *ResolvedTable:
			return n.Database.Name()
		case *UnresolvedTable:
			return n.Database()
		case *IndexedTableAccess:
			return n.Database().Name()
		}
		nodeStack = append(nodeStack, node.Children()...)
	}
	return ""
}
