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

package analyzer

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

// Scope of the analysis being performed, used when analyzing subqueries to give such analysis access to outer scope.
type Scope struct {
	// Stack of nested node scopes, with innermost scope first. A scope node is the node in which the subquery is
	// defined, or an appropriate sibling.
	nodes []sql.Node
	// Memo nodes are nodes in the execution context that shouldn't be considered for name resolution, but are still
	// important for analysis.
	memos []sql.Node
}

// newScope creates a new Scope object with the additional innermost Node context. When constructing with a subquery,
// the Node given should be the sibling Node of the subquery.
func (s *Scope) newScope(node sql.Node) *Scope {
	if s == nil {
		return &Scope{nodes: []sql.Node{node}}
	}
	var newNodes []sql.Node
	newNodes = append(newNodes, node)
	newNodes = append(newNodes, s.nodes...)
	return &Scope{nodes: newNodes, memos: s.memos}
}

// memo creates a new Scope object with the memo node given. Memo nodes don't affect name resolution, but are used in
// other parts of analysis, such as error handling for trigger / procedure execution.
func (s *Scope) memo(node sql.Node) *Scope {
	if s == nil {
		return &Scope{memos: []sql.Node{node}}
	}
	var newNodes []sql.Node
	newNodes = append(newNodes, node)
	newNodes = append(newNodes, s.memos...)
	return &Scope{memos: newNodes, nodes: s.nodes}
}

// withMemos returns a new scope object identical to the receiver, but with its memos replaced with the ones given.
func (s *Scope) withMemos(memoNodes []sql.Node) *Scope {
	if s == nil {
		return &Scope{memos: memoNodes}
	}
	return &Scope{memos: memoNodes, nodes: s.nodes}
}

func (s *Scope) MemoNodes() []sql.Node {
	if s == nil {
		return nil
	}
	return s.memos
}

// InnerToOuter returns the scope Nodes in order of innermost scope to outermost scope. When using these nodes for
// analysis, always inspect the children of the nodes, rather than the nodes themselves. The children define the schema
// of the rows being processed by the scope node itself.
func (s *Scope) InnerToOuter() []sql.Node {
	if s == nil {
		return nil
	}
	return s.nodes
}

// OuterToInner returns the scope nodes in order of outermost scope to innermost scope. When using these nodes for
// analysis, always inspect the children of the nodes, rather than the nodes themselves. The children define the schema
// of the rows being processed by the scope node itself.
func (s *Scope) OuterToInner() []sql.Node {
	if s == nil {
		return nil
	}
	reversed := make([]sql.Node, len(s.nodes))
	for i := range s.nodes {
		reversed[i] = s.nodes[len(s.nodes)-i-1]
	}
	return reversed
}

// Schema returns the equivalent schema of this scope, which consists of the schemas of all constituent scope nodes
// concatenated from outer to inner. Because we can only calculate the Schema() of nodes that are Resolved(), this
// method fills in place holder columns as necessary.
func (s *Scope) Schema() sql.Schema {
	var schema sql.Schema
	for _, n := range s.OuterToInner() {
		for _, n := range n.Children() {
			if n.Resolved() {
				schema = append(schema, n.Schema()...)
				continue
			}

			// If this scope node isn't resolved, we can't use Schema() on it. Instead, assemble an equivalent Schema, with
			// placeholder columns where necessary, for the purpose of analysis.
			switch n := n.(type) {
			case *plan.Project:
				for _, expr := range n.Projections {
					var col *sql.Column
					if expr.Resolved() {
						col = expression.ExpressionToColumn(expr)
					} else {
						// TODO: a new type here?
						col = &sql.Column{
							Name:   "",
							Source: "",
						}
					}
					schema = append(schema, col)
				}
			default:
				// TODO: log this
				// panic(fmt.Sprintf("Unsupported scope node %T", n))
			}
		}
	}
	return schema
}
