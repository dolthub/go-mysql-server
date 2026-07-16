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
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
)

type Sortable interface {
	sql.Node
	GetSortConditions() sql.SortConditions
}

// Sort is the sort node.
type Sort struct {
	UnaryNode
	SortConditions sql.SortConditions
}

// NewSort creates a new Sort node.
func NewSort(sortConditions sql.SortConditions, child sql.Node) *Sort {
	return &Sort{
		UnaryNode:      UnaryNode{child},
		SortConditions: sortConditions,
	}
}

var _ sql.Expressioner = (*Sort)(nil)
var _ sql.Node = (*Sort)(nil)
var _ sql.CollationCoercible = (*Sort)(nil)
var _ Sortable = (*Sort)(nil)
var _ sql.Describable = (*Sort)(nil)

// Resolved implements the Resolvable interface.
func (s *Sort) Resolved() bool {
	for _, f := range s.SortConditions {
		if !f.Expr.Resolved() {
			return false
		}
	}
	return s.Child.Resolved()
}

func (s *Sort) IsReadOnly() bool {
	return s.Child.IsReadOnly()
}

func (s *Sort) String() string {
	pr := sql.NewTreePrinter()
	var conds = make([]string, len(s.SortConditions))
	for i, c := range s.SortConditions {
		conds[i] = fmt.Sprintf("%s %s", c.Expr, c.Order)
	}
	_ = pr.WriteNode("Sort(%s)", strings.Join(conds, ", "))
	_ = pr.WriteChildren(s.Child.String())
	return pr.String()
}

// Describe implements the sql.Describable interface
func (s *Sort) Describe(ctx *sql.Context, options sql.DescribeOptions) string {
	pr := sql.NewTreePrinter()
	var conds = make([]string, len(s.SortConditions))
	for i, c := range s.SortConditions {
		conds[i] = sql.Describe(ctx, c, options)
	}
	_ = pr.WriteNode("Sort(%s)", strings.Join(conds, ", "))
	_ = pr.WriteChildren(sql.Describe(ctx, s.Child, options))
	return pr.String()
}

func (s *Sort) DebugString(ctx *sql.Context) string {
	pr := sql.NewTreePrinter()
	var conds = make([]string, len(s.SortConditions))
	for i, f := range s.SortConditions {
		conds[i] = sql.DebugString(ctx, f)
	}
	_ = pr.WriteNode("Sort(%s)", strings.Join(conds, ", "))
	_ = pr.WriteChildren(sql.DebugString(ctx, s.Child))
	return pr.String()
}

// Expressions implements the Expressioner interface.
func (s *Sort) Expressions() []sql.Expression {
	// TODO: use shared method
	return s.SortConditions.ToExpressions()
}

// WithChildren implements the Node interface.
func (s *Sort) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(s, len(children), 1)
	}

	return NewSort(s.SortConditions, children[0]), nil
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (s *Sort) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.GetCoercibility(ctx, s.Child)
}

// WithExpressions implements the Expressioner interface.
func (s *Sort) WithExpressions(ctx *sql.Context, exprs ...sql.Expression) (sql.Node, error) {
	if len(exprs) != len(s.SortConditions) {
		return nil, sql.ErrInvalidChildrenNumber.New(s, len(exprs), len(s.SortConditions))
	}

	conds := s.SortConditions.FromExpressions(ctx, exprs...)
	return NewSort(conds, s.Child), nil
}

func (s *Sort) GetSortConditions() sql.SortConditions {
	return s.SortConditions
}

// TopN was a sort node that has a limit. It doesn't need to buffer everything,
// but can calculate the top n on the fly.
type TopN struct {
	UnaryNode
	Limit          sql.Expression
	SortConditions sql.SortConditions
	CalcFoundRows  bool
}

// NewTopN creates a new TopN node.
func NewTopN(conds sql.SortConditions, limit sql.Expression, child sql.Node) *TopN {
	return &TopN{
		UnaryNode:      UnaryNode{child},
		Limit:          limit,
		SortConditions: conds,
	}
}

var _ sql.Node = (*TopN)(nil)
var _ sql.Expressioner = (*TopN)(nil)
var _ sql.CollationCoercible = (*TopN)(nil)
var _ Sortable = (*TopN)(nil)

// Resolved implements the Resolvable interface.
func (n *TopN) Resolved() bool {
	for _, f := range n.SortConditions {
		if !f.Expr.Resolved() {
			return false
		}
	}
	return n.Child.Resolved()
}

func (n *TopN) WithCalcFoundRows(v bool) *TopN {
	n.CalcFoundRows = v
	return n
}

func (n *TopN) IsReadOnly() bool {
	return n.Child.IsReadOnly()
}

func (n *TopN) String() string {
	pr := sql.NewTreePrinter()
	var conds = make([]string, len(n.SortConditions))
	for i, f := range n.SortConditions {
		conds[i] = fmt.Sprintf("%s %s", f.Expr, f.Order)
	}
	_ = pr.WriteNode("TopN(Limit: [%s]; %s)", n.Limit.String(), strings.Join(conds, ", "))
	_ = pr.WriteChildren(n.Child.String())
	return pr.String()
}

func (n *TopN) DebugString(ctx *sql.Context) string {
	pr := sql.NewTreePrinter()
	var conds = make([]string, len(n.SortConditions))
	for i, f := range n.SortConditions {
		conds[i] = sql.DebugString(ctx, f)
	}
	_ = pr.WriteNode("TopN(Limit: [%s]; %s)", sql.DebugString(ctx, n.Limit), strings.Join(conds, ", "))
	_ = pr.WriteChildren(sql.DebugString(ctx, n.Child))
	return pr.String()
}

// Expressions implements the Expressioner interface.
func (n *TopN) Expressions() []sql.Expression {
	exprs := []sql.Expression{n.Limit}
	exprs = append(exprs, n.SortConditions.ToExpressions()...)
	return exprs
}

// WithChildren implements the Node interface.
func (n *TopN) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(n, len(children), 1)
	}

	topn := NewTopN(n.SortConditions, n.Limit, children[0])
	topn.CalcFoundRows = n.CalcFoundRows
	return topn, nil
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (n *TopN) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.GetCoercibility(ctx, n.Child)
}

// WithExpressions implements the Expressioner interface.
func (n *TopN) WithExpressions(ctx *sql.Context, exprs ...sql.Expression) (sql.Node, error) {
	if len(exprs) != len(n.SortConditions)+1 {
		return nil, sql.ErrInvalidChildrenNumber.New(n, len(exprs), len(n.SortConditions)+1)
	}

	var limit = exprs[0]
	var conds = n.SortConditions.FromExpressions(ctx, exprs[1:]...)

	topn := NewTopN(conds, limit, n.Child)
	topn.CalcFoundRows = n.CalcFoundRows
	return topn, nil
}

func (n *TopN) GetSortConditions() sql.SortConditions {
	return n.SortConditions
}
