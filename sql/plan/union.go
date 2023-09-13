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

const (
	UnionType = iota
	IntersectType
	ExceptType
)

// Union is a node that returns everything in Left and then everything in Right
type Union struct {
	BinaryNode
	Distinct   bool
	Limit      sql.Expression
	Offset     sql.Expression
	SortFields sql.SortFields
	Type       int
}

var _ sql.Node = (*Union)(nil)
var _ sql.Expressioner = (*Union)(nil)
var _ sql.CollationCoercible = (*Union)(nil)

// NewUnion creates a new Union node with the given children.
func NewUnion(unionType int, left, right sql.Node, distinct bool, limit, offset sql.Expression, sortFields sql.SortFields) *Union {
	return &Union{
		BinaryNode: BinaryNode{left: left, right: right},
		Distinct:   distinct,
		Limit:      limit,
		Offset:     offset,
		SortFields: sortFields,
		Type:       unionType,
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
	if u.Offset != nil {
		res = res && u.Offset.Resolved()
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

func (u *Union) WithOffset(e sql.Expression) *Union {
	ret := *u
	ret.Offset = e
	return &ret
}

func (u *Union) Expressions() []sql.Expression {
	var exprs []sql.Expression
	if u.Limit != nil {
		exprs = append(exprs, u.Limit)
	}
	if u.Offset != nil {
		exprs = append(exprs, u.Offset)
	}
	if len(u.SortFields) > 0 {
		exprs = append(exprs, u.SortFields.ToExpressions()...)
	}
	return exprs
}

func (u *Union) WithExpressions(exprs ...sql.Expression) (sql.Node, error) {
	var expLim, expOff, expSort int
	if u.Limit != nil {
		expLim = 1
	}
	if u.Offset != nil {
		expOff = 1
	}
	expSort = len(u.SortFields)

	if len(exprs) != expLim+expOff+expSort {
		return nil, fmt.Errorf("expected %d limit and %d sort fields", expLim, expSort)
	} else if len(exprs) == 0 {
		return u, nil
	}

	ret := *u
	if expLim == 1 {
		ret.Limit = exprs[0]
		exprs = exprs[1:]
	}
	if expOff == 1 {
		ret.Offset = exprs[0]
		exprs = exprs[1:]
	}
	ret.SortFields = u.SortFields.FromExpressions(exprs...)
	return &ret, nil
}

// WithChildren implements the Node interface.
func (u *Union) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 2 {
		return nil, sql.ErrInvalidChildrenNumber.New(u, len(children), 2)
	}
	return NewUnion(u.Type, children[0], children[1], u.Distinct, u.Limit, u.Offset, u.SortFields), nil
}

// CheckPrivileges implements the interface sql.Node.
func (u *Union) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return u.left.CheckPrivileges(ctx, opChecker) && u.right.CheckPrivileges(ctx, opChecker)
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*Union) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	// Unions are able to return differing values, therefore they cannot be used to determine coercibility
	return sql.Collation_binary, 7
}

func (u *Union) String() string {
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
	if u.Offset != nil {
		children = append(children, fmt.Sprintf("offset: %s", u.Offset))
	}
	children = append(children, u.left.String(), u.right.String())
	_ = pr.WriteChildren(children...)
	return pr.String()
}

func (u *Union) IsReadOnly() bool {
	return u.left.IsReadOnly() && u.right.IsReadOnly()
}

func (u *Union) DebugString() string {
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
		sFields := make([]string, len(u.SortFields))
		for i, e := range u.SortFields.ToExpressions() {
			sFields[i] = sql.DebugString(e)
		}
		children = append(children, fmt.Sprintf("sortFields: %s", strings.Join(sFields, ", ")))
	}
	if u.Limit != nil {
		children = append(children, fmt.Sprintf("limit: %s", u.Limit))
	}
	if u.Offset != nil {
		children = append(children, fmt.Sprintf("offset: %s", u.Offset))
	}
	children = append(children, sql.DebugString(u.left), sql.DebugString(u.right))
	_ = pr.WriteChildren(children...)
	return pr.String()
}
