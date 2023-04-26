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
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
)

// Scope of the analysis being performed, used when analyzing subqueries to give such analysis access to outer scope.
type Scope struct {
	// Stack of nested node scopes, with innermost scope first. A scope node is the node in which the subquery is
	// defined, or an appropriate sibling, NOT the child node of the Subquery node.
	nodes []sql.Node
	// Memo nodes are nodes in the execution context that shouldn't be considered for name resolution, but are still
	// important for analysis.
	memos []sql.Node
	// recursionDepth tracks how many times we've recursed with analysis, to avoid stack overflows from infinite recursion
	recursionDepth int
	// currentNodeIsFromSubqueryExpression is true when the last scope (i.e. the most inner of the outer scope levels) has been
	// created by a subquery expression. This is needed in order to calculate outer scope visibility for derived tables.
	currentNodeIsFromSubqueryExpression bool
	// enforceReadOnly causes analysis to block all modification operations, as though a database is read only.
	enforceReadOnly bool

	procedures *ProcedureCache

	inJoin       bool
	joinSiblings []sql.Node
}

func (s *Scope) SetJoin(b bool) {
	if s == nil {
		return
	}
	s.inJoin = b
}

func (s *Scope) IsEmpty() bool {
	return s == nil || len(s.nodes) == 0
}

func (s *Scope) EnforcesReadOnly() bool {
	return s != nil && s.enforceReadOnly
}

// OuterRelUnresolved returns true if the relations in the
// outer scope are not qualified and resolved.
// note: a subquery in the outer scope is itself a scope,
// and by definition not an outer relation
func (s *Scope) OuterRelUnresolved() bool {
	return !s.IsEmpty() && s.Schema() == nil && len(s.nodes[0].Children()) > 0
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
	return &Scope{
		nodes:          newNodes,
		memos:          s.memos,
		recursionDepth: s.recursionDepth + 1,
		procedures:     s.procedures,
		joinSiblings:   s.joinSiblings,
	}
}

// newScopeFromSubqueryExpression returns a new subscope created from a subquery expression contained by the specified
// node.
func (s *Scope) newScopeFromSubqueryExpression(node sql.Node) *Scope {
	subScope := s.newScope(node)
	subScope.currentNodeIsFromSubqueryExpression = true
	return subScope
}

// newScopeFromSubqueryExpression returns a new subscope created from a subquery expression contained by the specified
// node.
func (s *Scope) newScopeInJoin(node sql.Node) *Scope {
	for {
		var done bool
		switch n := node.(type) {
		case *plan.StripRowNode:
			node = n.Child
		default:
			done = true
		}
		if done {
			break
		}
	}
	subScope := &Scope{
		nodes:          s.nodes,
		memos:          s.memos,
		recursionDepth: s.recursionDepth + 1,
		procedures:     s.procedures,
		joinSiblings:   s.joinSiblings,
	}
	subScope.joinSiblings = append(subScope.joinSiblings, node)
	return subScope
}

// newScopeFromSubqueryExpression returns a new subscope created from a subquery expression contained by the specified
// node.
func (s *Scope) newScopeNoJoin() *Scope {
	return &Scope{
		nodes:           s.nodes,
		memos:           s.memos,
		recursionDepth:  s.recursionDepth + 1,
		procedures:      s.procedures,
		enforceReadOnly: s.enforceReadOnly,
	}
}

// newScopeFromSubqueryAlias returns a new subscope created from the specified SubqueryAlias. Subquery aliases, or
// derived tables, generally do NOT have any visibility to outer scopes, but when they are nested inside a subquery
// expression, they may reference tables from the scopes outside the subquery expression's scope.
func (s *Scope) newScopeFromSubqueryAlias(sqa *plan.SubqueryAlias) *Scope {
	subScope := newScopeWithDepth(s.RecursionDepth() + 1)
	if s != nil && len(s.nodes) > 0 {
		// As of MySQL 8.0.14, MySQL provides OUTER scope visibility to derived tables. Unlike LATERAL scope visibility, which
		// gives a derived table visibility to the adjacent expressions where the subquery is defined, OUTER scope visibility
		// gives a derived table visibility to the OUTER scope where the subquery is defined.
		// https://dev.mysql.com/blog-archive/supporting-all-kinds-of-outer-references-in-derived-tables-lateral-or-not/
		// We don't include the current inner node so that the outer scope nodes are still present, but not the lateral nodes
		if s.currentNodeIsFromSubqueryExpression {
			sqa.OuterScopeVisibility = true
			subScope.joinSiblings = append(subScope.joinSiblings, s.joinSiblings...)
			subScope.nodes = append(subScope.nodes, s.InnerToOuter()...)
		} else if len(s.joinSiblings) > 0 {
			subScope.joinSiblings = append(subScope.joinSiblings, s.joinSiblings...)
			subScope.nodes = append(subScope.nodes, s.InnerToOuter()...)
		}
	}

	return subScope
}

// newScopeWithDepth returns a new scope object with the recursion depth given
func newScopeWithDepth(depth int) *Scope {
	return &Scope{recursionDepth: depth}
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
	return &Scope{
		memos:      newNodes,
		nodes:      s.nodes,
		procedures: s.procedures,
	}
}

// withMemos returns a new scope object identical to the receiver, but with its memos replaced with the ones given.
func (s *Scope) withMemos(memoNodes []sql.Node) *Scope {
	if s == nil {
		return &Scope{memos: memoNodes}
	}
	return &Scope{
		memos:      memoNodes,
		nodes:      s.nodes,
		procedures: s.procedures,
	}
}

func (s *Scope) MemoNodes() []sql.Node {
	if s == nil {
		return nil
	}
	return s.memos
}

func (s *Scope) RecursionDepth() int {
	if s == nil {
		return 0
	}
	return s.recursionDepth
}

func (s *Scope) procedureCache() *ProcedureCache {
	if s == nil {
		return nil
	}
	return s.procedures
}

func (s *Scope) withProcedureCache(cache *ProcedureCache) *Scope {
	if s == nil {
		return &Scope{procedures: cache}
	}
	return &Scope{
		memos:      s.memos,
		nodes:      s.nodes,
		procedures: cache,
	}
}

func (s *Scope) proceduresPopulating() bool {
	return s != nil && s.procedures != nil && s.procedures.IsPopulating
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
						col = transform.ExpressionToColumn(expr)
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
	if s != nil && s.inJoin {
		for _, n := range s.joinSiblings {
			schema = append(schema, n.Schema()...)
		}
	}
	return schema
}
