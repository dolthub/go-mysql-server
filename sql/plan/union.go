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
	"io"

	"github.com/gabereiser/go-mysql-server/sql"
)

// Union is a node that returns everything in Left and then everything in Right
type Union struct {
	BinaryNode
	Distinct   bool
	Limit      sql.Expression
	SortFields sql.SortFields
}

var _ sql.Expressioner = (*Union)(nil)

// NewUnion creates a new Union node with the given children.
func NewUnion(left, right sql.Node, distinct bool, limit sql.Expression, sortFields sql.SortFields) *Union {
	return &Union{
		BinaryNode: BinaryNode{left: left, right: right},
		Distinct:   distinct,
		Limit:      limit,
		SortFields: sortFields,
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

func (u *Union) Resolved() bool {
	res := u.Left().Resolved() && u.Right().Resolved()
	if u.Limit != nil {
		res = res && u.Limit.Resolved()
	}
	for _, sf := range u.SortFields {
		res = res && sf.Column.Resolved()
	}
	return res
}

func (u *Union) WithDistinct(b bool) *Union {
	ret := *u
	ret.Distinct = b
	return &ret
}

func (u *Union) WithLimit(e sql.Expression) *Union {
	ret := *u
	ret.Limit = e
	return &ret
}

func (u *Union) Expressions() []sql.Expression {
	var exprs []sql.Expression
	if u.Limit != nil {
		exprs = append(exprs, u.Limit)
	}
	if len(u.SortFields) > 0 {
		exprs = append(exprs, u.SortFields.ToExpressions()...)
	}
	return exprs
}

func (u *Union) WithExpressions(exprs ...sql.Expression) (sql.Node, error) {
	var expLim, expSort int
	if u.Limit != nil {
		expLim = 1
	}
	expSort = len(u.SortFields)

	if len(exprs) != expLim+expSort {
		return nil, fmt.Errorf("expected %d limit and %d sort fields", expLim, expSort)
	} else if len(exprs) == 0 {
		return u, nil
	}

	ret := *u
	if expLim == 1 {
		ret.Limit = exprs[0]
		exprs = exprs[1:]
	}
	ret.SortFields = u.SortFields.FromExpressions(exprs...)
	return &ret, nil
}

// RowIter implements the Node interface.
func (u *Union) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	span, ctx := ctx.Span("plan.Union")
	var iter sql.RowIter
	var err error
	iter, err = u.left.RowIter(ctx, row)
	if err != nil {
		span.End()
		return nil, err
	}
	iter = &unionIter{
		cur: iter,
		nextIter: func(ctx *sql.Context) (sql.RowIter, error) {
			return u.right.RowIter(ctx, row)
		},
	}
	if u.Distinct {
		iter = newDistinctIter(ctx, iter)
	}
	if u.Limit != nil && len(u.SortFields) > 0 {
		limit, err := getInt64Value(ctx, u.Limit)
		if err != nil {
			return nil, err
		}
		iter = newTopRowsIter(u.SortFields, limit, false, iter)
	} else if u.Limit != nil {
		limit, err := getInt64Value(ctx, u.Limit)
		if err != nil {
			return nil, err
		}
		iter = &limitIter{limit: limit, childIter: iter}
	} else if len(u.SortFields) > 0 {
		iter = newSortIter(u.SortFields, iter)
	}
	return sql.NewSpanIter(span, iter), nil
}

// WithChildren implements the Node interface.
func (u *Union) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 2 {
		return nil, sql.ErrInvalidChildrenNumber.New(u, len(children), 2)
	}
	return NewUnion(children[0], children[1], u.Distinct, u.Limit, u.SortFields), nil
}

// CheckPrivileges implements the interface sql.Node.
func (u *Union) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return u.left.CheckPrivileges(ctx, opChecker) && u.right.CheckPrivileges(ctx, opChecker)
}

func (u Union) String() string {
	pr := sql.NewTreePrinter()
	var distinct string
	if u.Distinct {
		distinct = "distinct"
	} else {
		distinct = "all"
	}
	_ = pr.WriteNode(fmt.Sprintf("Union %s", distinct))
	var children []string
	if len(u.SortFields) > 0 {
		children = append(children, fmt.Sprintf("sortFields: %s", u.SortFields.ToExpressions()))
	}
	if u.Limit != nil {
		children = append(children, fmt.Sprintf("limit: %s", u.Limit))
	}
	children = append(children, u.left.String(), u.right.String())
	_ = pr.WriteChildren(children...)
	return pr.String()
}

func (u Union) DebugString() string {
	pr := sql.NewTreePrinter()
	var distinct string
	if u.Distinct {
		distinct = "distinct"
	} else {
		distinct = "all"
	}
	_ = pr.WriteNode(fmt.Sprintf("Union %s", distinct))
	var children []string
	if len(u.SortFields) > 0 {
		children = append(children, fmt.Sprintf("sortFields: %s", u.SortFields.ToExpressions()))
	}
	if u.Limit != nil {
		children = append(children, fmt.Sprintf("limit: %s", u.Limit))
	}
	children = append(children, sql.DebugString(u.left), sql.DebugString(u.right))
	_ = pr.WriteChildren(children...)
	return pr.String()
}

type unionIter struct {
	cur      sql.RowIter
	nextIter func(ctx *sql.Context) (sql.RowIter, error)
}

func (ui *unionIter) Next(ctx *sql.Context) (sql.Row, error) {
	res, err := ui.cur.Next(ctx)
	if err == io.EOF {
		if ui.nextIter == nil {
			return nil, io.EOF
		}
		err = ui.cur.Close(ctx)
		if err != nil {
			return nil, err
		}
		ui.cur, err = ui.nextIter(ctx)
		ui.nextIter = nil
		if err != nil {
			return nil, err
		}
		return ui.cur.Next(ctx)
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
