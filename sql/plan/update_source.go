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
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/transform"
)

// UpdateSource is the source of updates for an Update node. Its schema is the concatenation of the old and new rows,
// before and after being updated.
type UpdateSource struct {
	UnaryNode
	UpdateExprs *UpdateExprs
	Ignore      bool
}

var _ sql.Node = (*UpdateSource)(nil)
var _ sql.CollationCoercible = (*UpdateSource)(nil)

// NewUpdateSource returns a new UpdateSource from the node and expressions given.
func NewUpdateSource(node sql.Node, ignore bool, updateExprs *UpdateExprs) *UpdateSource {
	return &UpdateSource{
		UnaryNode:   UnaryNode{node},
		UpdateExprs: updateExprs,
		Ignore:      ignore,
	}
}

// Expressions implements the sql.Expressioner interface.
func (u *UpdateSource) Expressions() []sql.Expression {
	return u.UpdateExprs.allExpressions()
}

func (u *UpdateSource) IsReadOnly() bool {
	return true
}

// WithExpressions implements the sql.Expressioner interface.
func (u *UpdateSource) WithExpressions(newExprs ...sql.Expression) (sql.Node, error) {
	var err error
	ret := *u
	ret.UpdateExprs, err = u.UpdateExprs.withExpressions(newExprs)
	return &ret, err
}

// Schema implements sql.Node. The schema of an update is a concatenation of the old and new rows.
func (u *UpdateSource) Schema() sql.Schema {
	return append(u.Child.Schema(), u.Child.Schema()...)
}

// Resolved implements the Resolvable interface.
func (u *UpdateSource) Resolved() bool {
	return u.Child.Resolved() && u.UpdateExprs.Resolved()
}

func (u *UpdateSource) String() string {
	tp := sql.NewTreePrinter()
	updateExprs := make([]string, len(u.UpdateExprs.explicitUpdateExprs))
	for i, e := range u.UpdateExprs.explicitUpdateExprs {
		updateExprs[i] = sql.DebugString(e)
	}
	_ = tp.WriteNode("UpdateSource(%s)", strings.Join(updateExprs, ","))
	_ = tp.WriteChildren(u.Child.String())
	return tp.String()
}

func (u *UpdateSource) DebugString() string {
	pr := sql.NewTreePrinter()
	updateExprs := make([]string, len(u.UpdateExprs.explicitUpdateExprs))
	for i, e := range u.UpdateExprs.explicitUpdateExprs {
		updateExprs[i] = sql.DebugString(e)
	}
	_ = pr.WriteNode("UpdateSource(%s)", strings.Join(updateExprs, ","))
	_ = pr.WriteChildren(sql.DebugString(u.Child))
	return pr.String()
}

func (u *UpdateSource) GetChildSchema() (sql.Schema, error) {
	if nodeHasJoin(u.Child) {
		return u.Child.Schema(), nil
	}

	table, err := GetUpdatable(u.Child)
	if err != nil {
		return nil, err
	}

	return table.Schema(), nil
}

func nodeHasJoin(node sql.Node) bool {
	hasJoinNode := false
	transform.InspectWithOpaque(node, func(node sql.Node) bool {
		switch node.(type) {
		case *JoinNode:
			hasJoinNode = true
			return false
		default:
			return true
		}
	})

	return hasJoinNode
}

func (u *UpdateSource) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(u, len(children), 1)
	}
	newU := *u
	newU.Child = children[0]
	return &newU, nil
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (u *UpdateSource) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.GetCoercibility(ctx, u.Child)
}

type UpdateExprs struct {
	// explicitUpdateExprs are update expressions that are explicitly part of a query
	explicitUpdateExprs []sql.Expression
	// derivedUpdateExprs are update expressions that are derived from the table's column expressions. This includes
	// updates on generated columns and ON UPDATE columns. derivedUpdateExprs should only be applied when updateExprs
	// actually yield a change in the row's values
	derivedUpdateExprs []sql.Expression
	len                int
}

func newUpdateExprs(explicitUpdateExprs []sql.Expression, updateExprs []sql.Expression) *UpdateExprs {
	return &UpdateExprs{
		explicitUpdateExprs: explicitUpdateExprs,
		derivedUpdateExprs:  updateExprs,
		len:                 len(explicitUpdateExprs) + len(updateExprs),
	}
}

func (u *UpdateExprs) allExpressions() []sql.Expression {
	return append(u.explicitUpdateExprs, u.derivedUpdateExprs...)
}

func (u *UpdateExprs) withExpressions(newExprs []sql.Expression) (*UpdateExprs, error) {
	// number of expressions must match
	if len(newExprs) != u.len {
		return nil, sql.ErrInvalidExpressionNumber.New(u, u.len, 1)
	}
	ret := *u
	numExplicitUpdateExprs := len(u.explicitUpdateExprs)
	ret.explicitUpdateExprs = newExprs[numExplicitUpdateExprs:]
	ret.derivedUpdateExprs = u.derivedUpdateExprs[:numExplicitUpdateExprs]
	return &ret, nil
}

func (u *UpdateExprs) Resolved() bool {
	return expression.ExpressionsResolved(u.explicitUpdateExprs...) &&
		expression.ExpressionsResolved(u.derivedUpdateExprs...)
}
