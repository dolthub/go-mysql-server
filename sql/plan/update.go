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

	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

var ErrUpdateNotSupported = errors.NewKind("table doesn't support UPDATE")
var ErrUpdateUnexpectedSetResult = errors.NewKind("attempted to set field but expression returned %T")

// Update is a node for updating rows on tables.
type Update struct {
	UnaryNode
	Checks sql.CheckConstraints
}

// NewUpdate creates an Update node.
func NewUpdate(n sql.Node, updateExprs []sql.Expression) *Update {
	return &Update{
		UnaryNode: UnaryNode{NewUpdateSource(
			n,
			updateExprs,
		)}}
}

func getUpdatable(node sql.Node) (sql.UpdatableTable, error) {
	switch node := node.(type) {
	case sql.UpdatableTable:
		return node, nil
	case *IndexedTableAccess:
		return getUpdatable(node.ResolvedTable)
	case *ResolvedTable:
		return getUpdatableTable(node.Table)
	case *SubqueryAlias:
		return nil, ErrUpdateNotSupported.New()
	case *TriggerExecutor:
		return getUpdatable(node.Left())
	case sql.TableWrapper:
		return getUpdatableTable(node.Underlying())
	}
	if len(node.Children()) > 1 {
		return nil, ErrUpdateNotSupported.New()
	}
	for _, child := range node.Children() {
		updater, _ := getUpdatable(child)
		if updater != nil {
			return updater, nil
		}
	}
	return nil, ErrUpdateNotSupported.New()
}

func getUpdatableTable(t sql.Table) (sql.UpdatableTable, error) {
	switch t := t.(type) {
	case sql.UpdatableTable:
		return t, nil
	case sql.TableWrapper:
		return getUpdatableTable(t.Underlying())
	default:
		return nil, ErrUpdateNotSupported.New()
	}
}

func updateDatabaseHelper(node sql.Node) string {
	switch node := node.(type) {
	case sql.UpdatableTable:
		return ""
	case *IndexedTableAccess:
		return updateDatabaseHelper(node.ResolvedTable)
	case *ResolvedTable:
		return node.Database.Name()
	case *UnresolvedTable:
		return node.Database
	}

	for _, child := range node.Children() {
		return updateDatabaseHelper(child)
	}

	return ""
}

func (u *Update) Database() string {
	return updateDatabaseHelper(u.Child)
}

func (u *Update) Expressions() []sql.Expression {
	return u.Checks.ToExpressions()
}

func (u *Update) Resolved() bool {
	return u.Child.Resolved() && expression.ExpressionsResolved(u.Checks.ToExpressions()...)
}

func (u Update) WithExpressions(newExprs ...sql.Expression) (sql.Node, error) {
	if len(newExprs) != len(u.Checks) {
		return nil, sql.ErrInvalidChildrenNumber.New(u, len(newExprs), len(u.Checks))
	}

	var err error
	u.Checks, err = u.Checks.FromExpressions(newExprs)
	if err != nil {
		return nil, err
	}

	return &u, nil
}

// UpdateInfo is the Info for OKResults returned by Update nodes.
type UpdateInfo struct {
	Matched, Updated, Warnings int
}

// String implements fmt.Stringer
func (ui UpdateInfo) String() string {
	return fmt.Sprintf("Rows matched: %d  Changed: %d  Warnings: %d", ui.Matched, ui.Updated, ui.Warnings)
}

type updateIter struct {
	childIter sql.RowIter
	schema    sql.Schema
	updater   sql.RowUpdater
	checks    sql.CheckConstraints
	ctx       *sql.Context
	closed    bool
}

func (u *updateIter) Next() (sql.Row, error) {
	oldAndNewRow, err := u.childIter.Next()
	if err != nil {
		return nil, err
	}

	oldRow, newRow := oldAndNewRow[:len(oldAndNewRow)/2], oldAndNewRow[len(oldAndNewRow)/2:]
	if equals, err := oldRow.Equals(newRow, u.schema); err == nil {
		// TODO: we aren't enforcing other kinds of constraints here, like nullability
		if !equals {
			// apply check constraints
			for _, check := range u.checks {
				if !check.Enforced {
					continue
				}

				res, err := sql.EvaluateCondition(u.ctx, check.Expr, newRow)
				if err != nil {
					return nil, err
				}

				if sql.IsFalse(res) {
					return nil, sql.ErrCheckConstraintViolated.New(check.Name)
				}
			}

			err = u.updater.Update(u.ctx, oldRow, newRow)
			if err != nil {
				return nil, err
			}
		}
	} else {
		return nil, err
	}

	return oldAndNewRow, nil
}

// Applies the update expressions given to the row given, returning the new resultant row.
// TODO: a set of update expressions should probably be its own expression type with an Eval method that does this
func applyUpdateExpressions(ctx *sql.Context, updateExprs []sql.Expression, row sql.Row) (sql.Row, error) {
	var ok bool
	prev := row
	for _, updateExpr := range updateExprs {
		val, err := updateExpr.Eval(ctx, prev)
		if err != nil {
			return nil, err
		}
		prev, ok = val.(sql.Row)
		if !ok {
			return nil, ErrUpdateUnexpectedSetResult.New(val)
		}
	}
	return prev, nil
}

func (u *updateIter) Close(ctx *sql.Context) error {
	if !u.closed {
		u.closed = true
		if err := u.updater.Close(ctx); err != nil {
			return err
		}
		return u.childIter.Close(ctx)
	}
	return nil
}

func newUpdateIter(
	ctx *sql.Context,
	childIter sql.RowIter,
	schema sql.Schema,
	updater sql.RowUpdater,
	checks sql.CheckConstraints,
) sql.RowIter {
	return NewTableEditorIter(ctx, updater, &updateIter{
		childIter: childIter,
		updater:   updater,
		schema:    schema,
		checks:    checks,
		ctx:       ctx,
	})
}

// RowIter implements the Node interface.
func (u *Update) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	updatable, err := getUpdatable(u.Child)
	if err != nil {
		return nil, err
	}
	updater := updatable.Updater(ctx)

	iter, err := u.Child.RowIter(ctx, row)
	if err != nil {
		return nil, err
	}

	return newUpdateIter(ctx, iter, updatable.Schema(), updater, u.Checks), nil
}

// WithChildren implements the Node interface.
func (u *Update) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(u, len(children), 1)
	}
	np := *u
	np.Child = children[0]
	return &np, nil
}

func (u *Update) String() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("Update")
	_ = pr.WriteChildren(u.Child.String())
	return pr.String()
}

func (u *Update) DebugString() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode(fmt.Sprintf("Update"))
	_ = pr.WriteChildren(sql.DebugString(u.Child))
	return pr.String()
}
