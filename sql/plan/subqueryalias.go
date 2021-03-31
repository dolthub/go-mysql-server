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

// SubqueryAlias is a node that gives a subquery a name.
type SubqueryAlias struct {
	UnaryNode
	name           string
	schema         sql.Schema
	TextDefinition string
}

// NewSubqueryAlias creates a new SubqueryAlias node.
func NewSubqueryAlias(name, textDefinition string, node sql.Node) *SubqueryAlias {
	return &SubqueryAlias{UnaryNode{Child: node}, name, nil, textDefinition}
}

// Returns the view wrapper for this subquery
func (n *SubqueryAlias) AsView() sql.View {
	return sql.NewView(n.Name(), n, n.TextDefinition)
}

// Name implements the Table interface.
func (n *SubqueryAlias) Name() string { return n.name }

// Schema implements the Node interface.
func (n *SubqueryAlias) Schema() sql.Schema {
	schema := n.Child.Schema()
	n.schema = make(sql.Schema, len(schema))
	for i, col := range schema {
		c := *col
		c.Source = n.name
		n.schema[i] = &c
	}
	return n.schema
}

// RowIter implements the Node interface.
func (n *SubqueryAlias) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	span, ctx := ctx.Span("plan.SubqueryAlias")

	// subqueries do not have access to outer scope
	iter, err := n.Child.RowIter(ctx, nil)
	if err != nil {
		span.Finish()
		return nil, err
	}

	return sql.NewSpanIter(span, iter), nil
}

// WithChildren implements the Node interface.
func (n *SubqueryAlias) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(n, len(children), 1)
	}

	nn := *n
	nn.Child = children[0]
	return &nn, nil
}

func (n SubqueryAlias) WithName(name string) *SubqueryAlias {
	n.name = name
	return &n
}

// Opaque implements the OpaqueNode interface.
func (n *SubqueryAlias) Opaque() bool {
	return true
}

func (n SubqueryAlias) String() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("SubqueryAlias(%s)", n.name)
	_ = pr.WriteChildren(n.Child.String())
	return pr.String()
}

func (n SubqueryAlias) DebugString() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("SubqueryAlias(%s)", n.name)
	_ = pr.WriteChildren(sql.DebugString(n.Child))
	return pr.String()
}
