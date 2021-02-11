// Copyright 2021 Dolthub, Inc.
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
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

type Window struct {
	SelectExprs []sql.Expression
	UnaryNode
}

var _ sql.Node = (*Window)(nil)

func NewWindow(selectExprs []sql.Expression, node sql.Node) *Window {
	return &Window{
		SelectExprs: selectExprs,
		UnaryNode:   UnaryNode{node},
	}
}

func (w *Window) Resolved() bool {
	return w.UnaryNode.Child.Resolved() &&
		expressionsResolved(w.SelectExprs...)
}

func (w *Window) String() string {
	pr := sql.NewTreePrinter()
	var exprs = make([]string, len(w.SelectExprs))
	for i, expr := range w.SelectExprs {
		exprs[i] = expr.String()
	}
	_ = pr.WriteNode("Window(%s)", strings.Join(exprs, ", "))
	_ = pr.WriteChildren(w.Child.String())
	return pr.String()
}

func (w *Window) DebugString() string {
	pr := sql.NewTreePrinter()
	var exprs = make([]string, len(w.SelectExprs))
	for i, expr := range w.SelectExprs {
		exprs[i] = sql.DebugString(expr)
	}
	_ = pr.WriteNode("Window(%s)", strings.Join(exprs, ", "))
	_ = pr.WriteChildren(sql.DebugString(w.Child))
	return pr.String()
}

func (w *Window) Schema() sql.Schema {
	var s = make(sql.Schema, len(w.SelectExprs))
	for i, e := range w.SelectExprs {
		s[i] = expression.ExpressionToColumn(e)
	}
	return s
}

func (w *Window) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	panic("implement me")
}

func (w *Window) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(w, len(children), 1)
	}

	return NewWindow(w.SelectExprs, children[0]), nil
}


