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
	return u.UpdateExprs.AllExpressions()
}

func (u *UpdateSource) IsReadOnly() bool {
	return true
}

// WithExpressions implements the sql.Expressioner interface.
func (u *UpdateSource) WithExpressions(ctx *sql.Context, exprs ...sql.Expression) (sql.Node, error) {
	var err error
	ret := *u
	ret.UpdateExprs, err = u.UpdateExprs.WithExpressions(exprs)
	return &ret, err
}

// Schema implements sql.Node. The schema of an update is a concatenation of the old and new rows.
func (u *UpdateSource) Schema(ctx *sql.Context) sql.Schema {
	return append(u.Child.Schema(ctx), u.Child.Schema(ctx)...)
}

// Resolved implements the Resolvable interface.
func (u *UpdateSource) Resolved() bool {
	return u.Child.Resolved() && u.UpdateExprs.Resolved()
}

func (u *UpdateSource) String() string {
	// To maintain compatibility with fmt.Stringer we have to use an empty context, but this will fail in any case that
	// requires a context to determine a string (such as an integrator using the context to contain type information).
	ctx := sql.NewEmptyContext()
	tp := sql.NewTreePrinter()
	updateExprs := make([]string, u.UpdateExprs.Length())
	for i, e := range u.UpdateExprs.AllExpressions() {
		updateExprs[i] = sql.DebugString(ctx, e)
	}
	_ = tp.WriteNode("UpdateSource(%s)", strings.Join(updateExprs, ","))
	_ = tp.WriteChildren(u.Child.String())
	return tp.String()
}

func (u *UpdateSource) DebugString(ctx *sql.Context) string {
	pr := sql.NewTreePrinter()
	updateExprs := make([]string, u.UpdateExprs.Length())
	for i, e := range u.UpdateExprs.AllExpressions() {
		updateExprs[i] = sql.DebugString(ctx, e)
	}
	_ = pr.WriteNode("UpdateSource(%s)", strings.Join(updateExprs, ","))
	_ = pr.WriteChildren(sql.DebugString(ctx, u.Child))
	return pr.String()
}

func (u *UpdateSource) GetChildSchema(ctx *sql.Context) (sql.Schema, error) {
	if nodeHasJoin(ctx, u.Child) {
		return u.Child.Schema(ctx), nil
	}

	table, err := GetUpdatable(u.Child)
	if err != nil {
		return nil, err
	}

	return table.Schema(ctx), nil
}

func nodeHasJoin(ctx *sql.Context, node sql.Node) bool {
	hasJoinNode := false
	transform.InspectWithOpaque(ctx, node, func(ctx *sql.Context, node sql.Node) bool {
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

func (u *UpdateSource) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
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
