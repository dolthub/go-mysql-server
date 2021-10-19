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
	Columns        []string
	name           string
	TextDefinition string
}

// NewSubqueryAlias creates a new SubqueryAlias node.
func NewSubqueryAlias(name, textDefinition string, node sql.Node) *SubqueryAlias {
	return &SubqueryAlias{
		UnaryNode:      UnaryNode{Child: node},
		name:           name,
		TextDefinition: textDefinition,
	}
}

// Returns the view wrapper for this subquery
func (sq *SubqueryAlias) AsView() *sql.View {
	return sql.NewView(sq.Name(), sq, sq.TextDefinition)
}

// Name implements the Table interface.
func (sq *SubqueryAlias) Name() string { return sq.name }

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

// RowIter implements the Node interface.
func (sq *SubqueryAlias) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	span, ctx := ctx.Span("plan.SubqueryAlias")

	// subqueries do not have access to outer scope
	iter, err := sq.Child.RowIter(ctx, nil)
	if err != nil {
		span.Finish()
		return nil, err
	}

	return sql.NewSpanIter(span, iter), nil
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

func (sq SubqueryAlias) WithName(name string) *SubqueryAlias {
	sq.name = name
	return &sq
}

// Opaque implements the OpaqueNode interface.
func (sq *SubqueryAlias) Opaque() bool {
	return true
}

func (sq SubqueryAlias) String() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("SubqueryAlias(%s)", sq.name)
	_ = pr.WriteChildren(sq.Child.String())
	return pr.String()
}

func (sq SubqueryAlias) DebugString() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("SubqueryAlias(%s)", sq.name)
	_ = pr.WriteChildren(sql.DebugString(sq.Child))
	return pr.String()
}

func (sq SubqueryAlias) WithColumns(columns []string) *SubqueryAlias {
	sq.Columns = columns
	return &sq
}
