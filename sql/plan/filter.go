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

	"github.com/gabereiser/go-mysql-server/sql"
	"github.com/gabereiser/go-mysql-server/sql/expression"
	"github.com/gabereiser/go-mysql-server/sql/transform"
)

// Filter skips rows that don't match a certain expression.
type Filter struct {
	UnaryNode
	Expression sql.Expression
}

// NewFilter creates a new filter node.
func NewFilter(expression sql.Expression, child sql.Node) *Filter {
	return &Filter{
		UnaryNode:  UnaryNode{Child: child},
		Expression: expression,
	}
}

// Resolved implements the Resolvable interface.
func (f *Filter) Resolved() bool {
	return f.UnaryNode.Child.Resolved() && f.Expression.Resolved()
}

// RowIter implements the Node interface.
func (f *Filter) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	span, ctx := ctx.Span("plan.Filter")

	i, err := f.Child.RowIter(ctx, row)
	if err != nil {
		span.End()
		return nil, err
	}

	return sql.NewSpanIter(span, NewFilterIter(f.Expression, i)), nil
}

// WithChildren implements the Node interface.
func (f *Filter) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(f, len(children), 1)
	}

	return NewFilter(f.Expression, children[0]), nil
}

// CheckPrivileges implements the interface sql.Node.
func (f *Filter) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return f.Child.CheckPrivileges(ctx, opChecker)
}

// WithExpressions implements the Expressioner interface.
func (f *Filter) WithExpressions(exprs ...sql.Expression) (sql.Node, error) {
	if len(exprs) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(f, len(exprs), 1)
	}

	return NewFilter(exprs[0], f.Child), nil
}

func (f *Filter) String() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("Filter")
	children := []string{f.Expression.String(), f.Child.String()}
	_ = pr.WriteChildren(children...)
	return pr.String()
}

func (f *Filter) DebugString() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("Filter")
	children := []string{sql.DebugString(f.Expression), sql.DebugString(f.Child)}
	_ = pr.WriteChildren(children...)
	return pr.String()
}

// Expressions implements the Expressioner interface.
func (f *Filter) Expressions() []sql.Expression {
	return []sql.Expression{f.Expression}
}

// FilterIter is an iterator that filters another iterator and skips rows that
// don't match the given condition.
type FilterIter struct {
	cond      sql.Expression
	childIter sql.RowIter
}

// NewFilterIter creates a new FilterIter.
func NewFilterIter(
	cond sql.Expression,
	child sql.RowIter,
) *FilterIter {
	return &FilterIter{cond: cond, childIter: child}
}

// Next implements the RowIter interface.
func (i *FilterIter) Next(ctx *sql.Context) (sql.Row, error) {
	for {
		row, err := i.childIter.Next(ctx)
		if err != nil {
			return nil, err
		}

		res, err := sql.EvaluateCondition(ctx, i.cond, row)
		if err != nil {
			return nil, err
		}

		if sql.IsTrue(res) {
			return row, nil
		}
	}
}

// Close implements the RowIter interface.
func (i *FilterIter) Close(ctx *sql.Context) error {
	return i.childIter.Close(ctx)
}

func NewSelectSingleRel(sel []sql.Expression, rel sql.NameableNode) *SelectSingleRel {
	return &SelectSingleRel{
		Select: sel,
		Rel:    rel,
	}
}

// SelectSingleRel collapses table and filter nodes into one object.
// Strong optimizations can be made for scopes that only has one
// relation, compared to a join with multiple nodes. Additionally, filters
// than only operate on a single table are easy to index.
type SelectSingleRel struct {
	Select []sql.Expression
	Rel    sql.NameableNode
}

var _ sql.NameableNode = (*SelectSingleRel)(nil)

func (s *SelectSingleRel) Name() string {
	return s.Rel.Name()
}

func (s *SelectSingleRel) Resolved() bool {
	return s.Rel.Resolved() && expression.ExpressionsResolved(s.Select...)
}

func (s *SelectSingleRel) String() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("Select")
	filters := make([]string, len(s.Select))
	for i, e := range s.Select {
		filters[i] = e.String()
	}
	children := []string{fmt.Sprintf("filters: %s", strings.Join(filters, ", ")), s.Rel.String()}
	_ = pr.WriteChildren(children...)
	return pr.String()
}

func (s *SelectSingleRel) DebugString() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("Select")
	filters := make([]string, len(s.Select))
	for i, e := range s.Select {
		filters[i] = sql.DebugString(e)
	}
	children := []string{fmt.Sprintf("filters: %s", strings.Join(filters, ", ")), sql.DebugString(s.Rel)}
	_ = pr.WriteChildren(children...)
	return pr.String()
}

func (s *SelectSingleRel) Schema() sql.Schema {
	return s.Rel.Schema()
}

func (s *SelectSingleRel) Children() []sql.Node {
	return []sql.Node{s.Rel}
}

func (s *SelectSingleRel) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	return nil, fmt.Errorf("*plan.SelectSingleRel is not an executable node; rule: normalizeSelectSingleRel should have normalized")
}

// Expressions implements the sql.Expressioner interface.
func (s *SelectSingleRel) Expressions() []sql.Expression {
	return s.Select
}

// WithExpressions implements the sql.Expressioner interface.
func (s *SelectSingleRel) WithExpressions(exprs ...sql.Expression) (sql.Node, error) {
	if len(exprs) != len(s.Select) {
		return nil, sql.ErrInvalidChildrenNumber.New(s, len(exprs), len(s.Select))
	}

	return NewSelectSingleRel(exprs, s.Rel), nil
}

func (s *SelectSingleRel) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(s, len(children), 1)
	}

	nn, ok := children[0].(sql.NameableNode)
	if !ok {
		return nil, fmt.Errorf("SingleRelSelect.WithChildren() expected sql.NameableNode, found :%T", children[0])
	}

	return NewSelectSingleRel(s.Select, nn), nil
}

func (s *SelectSingleRel) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return s.Rel.CheckPrivileges(ctx, opChecker)
}

// RequalifyFields updates the |Select| filters source to a new
// table alias.
func (s *SelectSingleRel) RequalifyFields(to string) *SelectSingleRel {
	newSel, same, _ := transform.Exprs(s.Select, func(e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
		switch e := e.(type) {
		case *expression.GetField:
			return e.WithTable(to), transform.NewTree, nil
		default:
			return e, transform.SameTree, nil
		}
	})
	if same {
		return s
	}
	return NewSelectSingleRel(newSel, s.Rel)
}
