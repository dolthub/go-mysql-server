// Copyright 2020 Liquidata, Inc.
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
	"github.com/liquidata-inc/go-mysql-server/sql"
)

// UpdateSource is the source of updates for an Update node. Its schema is the concatenation of the old and new rows,
// before and after being updated.
type UpdateSource struct {
	UnaryNode
	UpdateExprs []sql.Expression
}

// NewUpdateSource returns a new UpdateSource from the node and expressions given.
func NewUpdateSource(node sql.Node, updateExprs []sql.Expression) *UpdateSource {
	return &UpdateSource{
		UnaryNode:   UnaryNode{node},
		UpdateExprs: updateExprs,
	}
}

// Expressions implements the sql.Expressioner interface.
func (u *UpdateSource) Expressions() []sql.Expression {
	return u.UpdateExprs
}

// WithExpressions implements the sql.Expressioner interface.
func (u *UpdateSource) WithExpressions(newExprs ...sql.Expression) (sql.Node, error) {
	if len(newExprs) != len(u.UpdateExprs) {
		return nil, sql.ErrInvalidChildrenNumber.New(u, len(u.UpdateExprs), 1)
	}
	return NewUpdate(u.Child, newExprs), nil
}

// Schema implements sql.Node. The schema of an update is a concatenation of the old and new rows.
func (u *UpdateSource) Schema() sql.Schema {
	return append(u.Child.Schema(), u.Child.Schema()...)
}

// Resolved implements the Resolvable interface.
func (u *UpdateSource) Resolved() bool {
	if !u.Child.Resolved() {
		return false
	}
	for _, updateExpr := range u.UpdateExprs {
		if !updateExpr.Resolved() {
			return false
		}
	}
	return true
}

func (u *UpdateSource) String() string {
	tp := sql.NewTreePrinter()
	_ = tp.WriteNode("UpdateSource")
	_ = tp.WriteChildren(u.Child.String())
	return tp.String()
}

type updateSourceIter struct {
	childIter   sql.RowIter
	updateExprs []sql.Expression
	schema      sql.Schema
	ctx         *sql.Context
}

func (u *updateSourceIter) Next() (sql.Row, error) {
	oldRow, err := u.childIter.Next()
	if err != nil {
		return nil, err
	}

	newRow, err := applyUpdateExpressions(u.ctx, u.updateExprs, oldRow)
	if err != nil {
		return nil, err
	}

	// Reduce the row to the length of the schema. The length can differ when some update values come from an outer
	// scope, which will be the first N values in the row.
	// TODO: handle this in the analyzer instead?
	if len(u.schema) < len(newRow) {
		newRow = newRow[len(newRow)-len(u.schema):]
		oldRow = oldRow[len(oldRow)-len(u.schema):]
	}

	return oldRow.Append(newRow), nil
}

func (u *updateSourceIter) Close() error {
	return u.childIter.Close()
}

func (u *UpdateSource) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	rowIter, err := u.Child.RowIter(ctx, row)
	if err != nil {
		return nil, err
	}

	return &updateSourceIter{
		childIter:   rowIter,
		updateExprs: u.UpdateExprs,
		schema:      u.Schema(),
		ctx:         ctx,
	}, nil
}

func (u *UpdateSource) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(u, len(children), 1)
	}
	return NewUpdateSource(children[0], u.UpdateExprs), nil
}



