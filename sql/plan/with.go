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
)

// With is a node to wrap the top-level node in a query plan so that any common table expressions can be applied in
// analysis. It is removed during analysis.
type With struct {
	UnaryNode
	CTEs      []*CommonTableExpression
	Recursive bool
}

func NewWith(child sql.Node, ctes []*CommonTableExpression, recursive bool) *With {
	return &With{
		UnaryNode: UnaryNode{child},
		CTEs:      ctes,
		Recursive: recursive,
	}
}

func (w *With) String() string {
	cteStrings := make([]string, len(w.CTEs))
	for i, e := range w.CTEs {
		cteStrings[i] = e.String()
	}

	pr := sql.NewTreePrinter()
	if w.Recursive {
		_ = pr.WriteNode("with recursive (%s)", strings.Join(cteStrings, ", "))
	} else {
		_ = pr.WriteNode("with(%s)", strings.Join(cteStrings, ", "))
	}
	_ = pr.WriteChildren(w.Child.String())
	return pr.String()
}

func (w *With) DebugString() string {
	cteStrings := make([]string, len(w.CTEs))
	for i, e := range w.CTEs {
		cteStrings[i] = sql.DebugString(e)
	}

	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("With(%s)", strings.Join(cteStrings, ", "))
	_ = pr.WriteChildren(sql.DebugString(w.Child))
	return pr.String()
}

func (w *With) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	panic("Cannot call RowIter on With node")
}

func (w *With) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(w, len(children), 1)
	}

	return NewWith(children[0], w.CTEs, w.Recursive), nil
}

// CheckPrivileges implements the interface sql.Node.
func (w *With) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return w.Child.CheckPrivileges(ctx, opChecker)
}

type CommonTableExpression struct {
	Subquery *SubqueryAlias
	Columns  []string
}

func NewCommonTableExpression(subquery *SubqueryAlias, columns []string) *CommonTableExpression {
	return &CommonTableExpression{
		Subquery: subquery,
		Columns:  columns,
	}
}

func (e *CommonTableExpression) String() string {
	pr := sql.NewTreePrinter()
	if len(e.Columns) > 0 {
		_ = pr.WriteNode("%s (%s)", e.Subquery.name, strings.Join(e.Columns, ","))
	} else {
		_ = pr.WriteNode("%s", e.Subquery.name)
	}
	_ = pr.WriteChildren(sql.DebugString(e.Subquery.Child))
	return pr.String()
}

func (e *CommonTableExpression) DebugString() string {
	pr := sql.NewTreePrinter()
	if len(e.Columns) > 0 {
		_ = pr.WriteNode("%s (%s)", e.Subquery.name, strings.Join(e.Columns, ","))
	} else {
		_ = pr.WriteNode("%s", e.Subquery.name)
	}
	_ = pr.WriteChildren(e.Subquery.Child.String())
	return pr.String()
}
