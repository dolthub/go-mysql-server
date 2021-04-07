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
	"fmt"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
)

// With is a node to wrap the top-level node in a query plan so that any common table expressions can be applied in
// analysis. It is removed during analysis.
type With struct {
	UnaryNode
	CTEs []*CommonTableExpression
}

func NewWith(child sql.Node, ctes []*CommonTableExpression) *With {
	return &With{
		UnaryNode: UnaryNode{child},
		CTEs:      ctes,
	}
}

func (w *With) String() string {
	cteStrings := make([]string, len(w.CTEs))
	for i, e := range w.CTEs {
		cteStrings[i] = e.String()
	}

	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("With(%s)", strings.Join(cteStrings, ", "))
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

	return NewWith(children[0], w.CTEs), nil
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
	if len(e.Columns) > 0 {
		return fmt.Sprintf("%s (%s) AS %s", e.Subquery.name, strings.Join(e.Columns, ","), e.Subquery.Child)
	}
	return fmt.Sprintf("%s AS %s", e.Subquery.name, e.Subquery.Child)
}

func (e *CommonTableExpression) DebugString() string {
	if len(e.Columns) > 0 {
		return fmt.Sprintf("%s (%s) AS %s", e.Subquery.name, strings.Join(e.Columns, ","), sql.DebugString(e.Subquery))
	}
	return fmt.Sprintf("%s AS %s", e.Subquery.name, e)
}
