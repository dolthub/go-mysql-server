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

	"github.com/dolthub/go-mysql-server/sql"
)

// SubqueryAlias is a node that gives a subquery a name.
type SubqueryAlias struct {
	UnaryNode
	Columns        []string
	name           string
	TextDefinition string
	// OuterScopeVisibility is true when a SubqueryAlias (i.e. derived table) is contained in a subquery
	// expression and is eligible to have visibility to outer scopes of the query.
	OuterScopeVisibility bool
	Correlated           sql.ColSet
	Volatile             bool
	CacheableCTESource   bool
	IsLateral            bool
	ScopeMapping         map[sql.ColumnId]sql.Expression
}

var _ sql.Node = (*SubqueryAlias)(nil)
var _ sql.CollationCoercible = (*SubqueryAlias)(nil)

// NewSubqueryAlias creates a new SubqueryAlias node.
func NewSubqueryAlias(name, textDefinition string, node sql.Node) *SubqueryAlias {
	return &SubqueryAlias{
		UnaryNode:            UnaryNode{Child: node},
		name:                 name,
		TextDefinition:       textDefinition,
		OuterScopeVisibility: false,
	}
}

// AsView returns the view wrapper for this subquery
func (sq *SubqueryAlias) AsView(createViewStmt string) *sql.View {
	return sql.NewView(sq.Name(), sq, sq.TextDefinition, createViewStmt)
}

// Name implements the Table interface.
func (sq *SubqueryAlias) Name() string { return sq.name }

func (sq *SubqueryAlias) WithName(n string) *SubqueryAlias {
	ret := *sq
	ret.name = n
	return &ret
}

func (sq *SubqueryAlias) IsReadOnly() bool {
	return sq.Child.IsReadOnly()
}

// Schema implements the Node interface.
func (sq *SubqueryAlias) Schema() sql.Schema {
	childSchema := sq.Child.Schema()
	schema := make(sql.Schema, len(childSchema))
	for i, col := range childSchema {
		c := *col
		c.Source = sq.name
		if len(sq.Columns) > 0 {
			c.Name = sq.Columns[i]
		}
		schema[i] = &c
	}
	return schema
}

// WithChildren implements the Node interface.
func (sq *SubqueryAlias) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(sq, len(children), 1)
	}

	nn := *sq
	nn.Child = children[0]
	return &nn, nil
}

// CheckPrivileges implements the interface sql.Node.
func (sq *SubqueryAlias) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return sq.Child.CheckPrivileges(ctx, opChecker)
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (sq *SubqueryAlias) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.GetCoercibility(ctx, sq.Child)
}

func (sq *SubqueryAlias) WithChild(n sql.Node) *SubqueryAlias {
	ret := *sq
	ret.Child = n
	return &ret
}

func (sq *SubqueryAlias) CanCacheResults() bool {
	return sq.Correlated.Empty() && !sq.Volatile
}

func (sq *SubqueryAlias) WithCorrelated(cols sql.ColSet) *SubqueryAlias {
	ret := *sq
	ret.Correlated = cols
	return &ret
}

func (sq *SubqueryAlias) WithVolatile(v bool) *SubqueryAlias {
	ret := *sq
	ret.Volatile = v
	return &ret
}

func (sq *SubqueryAlias) WithScopeMapping(cols map[sql.ColumnId]sql.Expression) *SubqueryAlias {
	ret := *sq
	ret.ScopeMapping = cols
	return &ret
}

// Opaque implements the OpaqueNode interface.
func (sq *SubqueryAlias) Opaque() bool {
	return true
}

func (sq *SubqueryAlias) String() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("SubqueryAlias")
	children := make([]string, 4)
	children[0] = fmt.Sprintf("name: %s", sq.name)
	children[1] = fmt.Sprintf("outerVisibility: %t", sq.OuterScopeVisibility)
	children[2] = fmt.Sprintf("cacheable: %t", sq.CanCacheResults())
	children[3] = sq.Child.String()
	_ = pr.WriteChildren(children...)
	return pr.String()
}

func (sq *SubqueryAlias) DebugString() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("SubqueryAlias")
	children := make([]string, 4)
	children[0] = fmt.Sprintf("name: %s", sq.name)
	children[1] = fmt.Sprintf("outerVisibility: %t", sq.OuterScopeVisibility)
	children[2] = fmt.Sprintf("cacheable: %t", sq.CanCacheResults())
	children[3] = sql.DebugString(sq.Child)
	_ = pr.WriteChildren(children...)
	return pr.String()
}

func (sq *SubqueryAlias) WithColumns(columns []string) *SubqueryAlias {
	ret := *sq
	ret.Columns = columns
	return &ret
}
