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
	opentracing "github.com/opentracing/opentracing-go"

	"github.com/dolthub/go-mysql-server/sql"
)

// Offset is a node that skips the first N rows.
type Offset struct {
	UnaryNode
	Offset sql.Expression
}

// NewOffset creates a new Offset node.
func NewOffset(n sql.Expression, child sql.Node) *Offset {
	return &Offset{
		UnaryNode: UnaryNode{Child: child},
		Offset:    n,
	}
}

// Expressions implements sql.Expressioner
func (o *Offset) Expressions() []sql.Expression {
	return []sql.Expression{o.Offset}
}

// WithExpressions implements sql.Expressioner
func (o *Offset) WithExpressions(exprs ...sql.Expression) (sql.Node, error) {
	if len(exprs) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(o, len(exprs), 1)
	}
	return NewOffset(exprs[0], o.Child), nil
}

// Resolved implements the Resolvable interface.
func (o *Offset) Resolved() bool {
	return o.Child.Resolved() && o.Offset.Resolved()
}

// RowIter implements the Node interface.
func (o *Offset) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	span, ctx := ctx.Span("plan.Offset", opentracing.Tag{Key: "offset", Value: o.Offset})

	offset, err := getInt64Value(ctx, o.Offset)
	if err != nil {
		return nil, err
	}

	it, err := o.Child.RowIter(ctx, row)
	if err != nil {
		span.Finish()
		return nil, err
	}
	return sql.NewSpanIter(span, &offsetIter{offset, it}), nil
}

// WithChildren implements the Node interface.
func (o *Offset) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(o, len(children), 1)
	}
	return NewOffset(o.Offset, children[0]), nil
}

func (o Offset) String() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("Offset(%s)", o.Offset)
	_ = pr.WriteChildren(o.Child.String())
	return pr.String()
}

type offsetIter struct {
	skip      int64
	childIter sql.RowIter
}

func (i *offsetIter) Next() (sql.Row, error) {
	if i.skip > 0 {
		for i.skip > 0 {
			_, err := i.childIter.Next()
			if err != nil {
				return nil, err
			}
			i.skip--
		}
	}

	row, err := i.childIter.Next()
	if err != nil {
		return nil, err
	}

	return row, nil
}

func (i *offsetIter) Close(ctx *sql.Context) error {
	return i.childIter.Close(ctx)
}
