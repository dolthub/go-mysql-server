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
var ErrUpdateForTableNotSupported = errors.NewKind("The target table %s of the UPDATE is not updatable")
var ErrUpdateUnexpectedSetResult = errors.NewKind("attempted to set field but expression returned %T")

// Update is a node for updating rows on tables.
type Update struct {
	UnaryNode
	checks sql.CheckConstraints
	Ignore bool
}

var _ sql.Node = (*Update)(nil)
var _ sql.Databaseable = (*Update)(nil)
var _ sql.CollationCoercible = (*Update)(nil)
var _ sql.CheckConstraintNode = (*Update)(nil)

// NewUpdate creates an Update node.
func NewUpdate(n sql.Node, ignore bool, updateExprs []sql.Expression) *Update {
	return &Update{
		UnaryNode: UnaryNode{NewUpdateSource(
			n,
			ignore,
			updateExprs,
		)},
		Ignore: ignore,
	}
}

func GetUpdatable(node sql.Node) (sql.UpdatableTable, error) {
	switch node := node.(type) {
	case sql.UpdatableTable:
		return node, nil
	case *IndexedTableAccess:
		return GetUpdatable(node.TableNode)
	case *ResolvedTable:
		return getUpdatableTable(node.Table)
	case *SubqueryAlias:
		return nil, ErrUpdateNotSupported.New()
	case *TriggerExecutor:
		return GetUpdatable(node.Left())
	case sql.TableWrapper:
		return getUpdatableTable(node.Underlying())
	case *UpdateJoin:
		return node.GetUpdatable(), nil
	}
	if len(node.Children()) > 1 {
		return nil, ErrUpdateNotSupported.New()
	}
	for _, child := range node.Children() {
		updater, _ := GetUpdatable(child)
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

// GetDatabase returns the first database found in the node tree given
func GetDatabase(node sql.Node) sql.Database {
	switch node := node.(type) {
	case *IndexedTableAccess:
		return GetDatabase(node.TableNode)
	case *ResolvedTable:
		return node.Database()
	case *UnresolvedTable:
		return node.Database()
	}

	for _, child := range node.Children() {
		return GetDatabase(child)
	}

	return nil
}

func (u *Update) Checks() sql.CheckConstraints {
	return u.checks
}

func (u *Update) WithChecks(checks sql.CheckConstraints) sql.Node {
	ret := *u
	ret.checks = checks
	return &ret
}

// DB returns the database being updated. |Database| is already used by another interface we implement.
func (u *Update) DB() sql.Database {
	return GetDatabase(u.Child)
}

func (u *Update) IsReadOnly() bool {
	return false
}

func (u *Update) Database() string {
	db := GetDatabase(u.Child)
	if db == nil {
		return ""
	}
	return db.Name()
}

func (u *Update) Expressions() []sql.Expression {
	return u.checks.ToExpressions()
}

func (u *Update) Resolved() bool {
	return u.Child.Resolved() && expression.ExpressionsResolved(u.checks.ToExpressions()...)
}

func (u Update) WithExpressions(newExprs ...sql.Expression) (sql.Node, error) {
	if len(newExprs) != len(u.checks) {
		return nil, sql.ErrInvalidChildrenNumber.New(u, len(newExprs), len(u.checks))
	}

	var err error
	u.checks, err = u.checks.FromExpressions(newExprs)
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

// WithChildren implements the Node interface.
func (u *Update) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(u, len(children), 1)
	}
	np := *u
	np.Child = children[0]
	return &np, nil
}

// CheckPrivileges implements the interface sql.Node.
func (u *Update) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	//TODO: If column values are retrieved then the SELECT privilege is required
	// For example: "UPDATE table SET x = y + 1 WHERE z > 0"
	// We would need SELECT privileges on both the "y" and "z" columns as they're retrieving values
	db := u.DB()
	checkName := CheckPrivilegeNameForDatabase(db)

	return opChecker.UserHasPrivileges(ctx,
		// TODO: this needs a real database, fix it
		sql.NewPrivilegedOperation(checkName, getTableName(u.Child), "", sql.PrivilegeType_Update))
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*Update) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 7
}

func (u *Update) String() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("Update")
	_ = pr.WriteChildren(u.Child.String())
	return pr.String()
}

func (u *Update) DebugString() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("Update")
	_ = pr.WriteChildren(sql.DebugString(u.Child))
	return pr.String()
}
