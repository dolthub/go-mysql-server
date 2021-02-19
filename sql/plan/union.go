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
	"io"

	"github.com/dolthub/go-mysql-server/sql"
)

// Union is a node that returns everything in Left and then everything in Right
type Union struct {
	BinaryNode
}

// NewUnion creates a new Union node with the given children.
func NewUnion(left, right sql.Node) *Union {
	return &Union{
		BinaryNode: BinaryNode{left: left, right: right},
	}
}

func (u *Union) Schema() sql.Schema {
	ls := u.left.Schema()
	rs := u.right.Schema()
	ret := make([]*sql.Column, len(ls))
	for i := range ls {
		c := *ls[i]
		if i < len(rs) {
			c.Nullable = ls[i].Nullable || rs[i].Nullable
		}
		ret[i] = &c
	}
	return ret
}

// Opaque implements the sql.OpaqueNode interface.
// Like SubqueryAlias, the selects in a Union must be evaluated in isolation.
func (u *Union) Opaque() bool {
	return true
}

// RowIter implements the Node interface.
func (u *Union) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	span, ctx := ctx.Span("plan.Union")
	li, err := u.left.RowIter(ctx, row)
	if err != nil {
		span.Finish()
		return nil, err
	}
	ui := &unionIter{
		ctx,
		li,
		func() (sql.RowIter, error) {
			return u.right.RowIter(ctx, row)
		},
	}
	return sql.NewSpanIter(span, ui), nil
}

// WithChildren implements the Node interface.
func (u *Union) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 2 {
		return nil, sql.ErrInvalidChildrenNumber.New(u, len(children), 2)
	}
	return NewUnion(children[0], children[1]), nil
}

func (u Union) String() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("Union")
	_ = pr.WriteChildren(u.left.String(), u.right.String())
	return pr.String()
}

func (u Union) DebugString() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("Union")
	_ = pr.WriteChildren(sql.DebugString(u.left), sql.DebugString(u.right))
	return pr.String()
}

type unionIter struct {
	ctx      *sql.Context
	cur      sql.RowIter
	nextIter func() (sql.RowIter, error)
}

func (ui *unionIter) Next() (sql.Row, error) {
	res, err := ui.cur.Next()
	if err == io.EOF {
		if ui.nextIter == nil {
			return nil, io.EOF
		}
		err = ui.cur.Close(ui.ctx)
		if err != nil {
			return nil, err
		}
		ui.cur, err = ui.nextIter()
		ui.nextIter = nil
		if err != nil {
			return nil, err
		}
		return ui.cur.Next()
	}
	return res, err
}

func (ui *unionIter) Close(ctx *sql.Context) error {
	if ui.cur != nil {
		return ui.cur.Close(ctx)
	} else {
		return nil
	}
}
