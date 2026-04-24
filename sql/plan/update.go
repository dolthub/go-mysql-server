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
	"github.com/dolthub/go-mysql-server/sql/transform"
)

var ErrUpdateNotSupported = errors.NewKind("table doesn't support UPDATE")
var ErrUpdateForTableNotSupported = errors.NewKind("The target table %s of the UPDATE is not updatable")
var ErrUpdateUnexpectedSetResult = errors.NewKind("attempted to set field but expression returned %T")

// Update is a node for updating rows on tables.
type Update struct {
	UnaryNode
	checks sql.CheckConstraints
	// Returning is a list of expressions to return after the update operation. This feature is not
	// supported in MySQL's syntax, but is exposed through PostgreSQL's syntax.
	Returning []sql.Expression
	// IsJoin is true only for explicit UPDATE JOIN queries. It's possible for Update.IsJoin to be false and
	// Update.Child to be an UpdateJoin since subqueries are optimized as Joins
	IsJoin       bool
	HasSingleRel bool
	IsProcNested bool
	Ignore       bool
}

var _ sql.Node = (*Update)(nil)
var _ sql.Databaseable = (*Update)(nil)
var _ sql.CollationCoercible = (*Update)(nil)
var _ sql.CheckConstraintNode = (*Update)(nil)

// NewUpdate creates an Update node.
func NewUpdate(n sql.Node, ignore bool, updateExprs *UpdateExprs) *Update {
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

// Schema implements the sql.Node interface.
func (u *Update) Schema(ctx *sql.Context) sql.Schema {
	// Postgres allows the returned values of the update statement to be controlled, so if returning
	// expressions were specified, then we return a different schema.
	if u.Returning != nil {
		// We know that returning exprs are resolved here, because you can't call Schema()
		// safely until Resolved() is true.
		returningSchema := sql.Schema{}
		for _, expr := range u.Returning {
			returningSchema = append(returningSchema, transform.ExpressionToColumn(ctx, expr, ""))
		}

		return returningSchema
	}

	return u.Child.Schema(ctx)
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
	return append(u.checks.ToExpressions(), u.Returning...)
}

func (u *Update) Resolved() bool {
	return u.Child.Resolved() &&
		expression.ExpressionsResolved(u.checks.ToExpressions()...) &&
		expression.ExpressionsResolved(u.Returning...)

}

func (u *Update) WithExpressions(ctx *sql.Context, exprs ...sql.Expression) (sql.Node, error) {
	numChecks := len(u.checks)
	expectedLength := numChecks + len(u.Returning)
	if len(exprs) != expectedLength {
		return nil, sql.ErrInvalidExpressionNumber.New(u, len(exprs), expectedLength)
	}

	var err error
	ret := *u
	ret.checks, err = u.checks.FromExpressions(exprs[:numChecks])
	if err != nil {
		return nil, err
	}

	ret.Returning = exprs[numChecks:]

	return &ret, nil
}

// UpdateInfo is the Info for OKResults returned by Update nodes.
type UpdateInfo struct {
	Matched, Updated, Warnings int
}

// String implements sql.Stringer
func (ui UpdateInfo) String(ctx *sql.Context) string {
	return fmt.Sprintf("Rows matched: %d  Changed: %d  Warnings: %d", ui.Matched, ui.Updated, ui.Warnings)
}

// WithChildren implements the Node interface.
func (u *Update) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(u, len(children), 1)
	}
	np := *u
	np.Child = children[0]
	return &np, nil
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*Update) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 7
}

func (u *Update) String(ctx *sql.Context) string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("Update")
	_ = pr.WriteChildren(u.Child.String(ctx))
	return pr.String()
}

func (u *Update) DebugString(ctx *sql.Context) string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("Update")
	_ = pr.WriteChildren(sql.DebugString(ctx, u.Child))
	return pr.String()
}

type UpdateExprs struct {
	exprs []sql.Expression
	// numExplicitExprs is the number of explicit update expressions. Explicit updates are updates that are explicitly
	// part of a query, as opposed to derived updates, which are derived from the table's column definitions.
	// numExplicitExprs is used to index into exprs to separate explicit and derived expressions when needed
	numExplicitExprs int
}

func NewUpdateExprs(exprs []sql.Expression, numExplicitExprs int) *UpdateExprs {
	return &UpdateExprs{
		exprs:            exprs,
		numExplicitExprs: numExplicitExprs,
	}
}

func (ue *UpdateExprs) AllExpressions() []sql.Expression {
	if ue == nil {
		return nil
	}
	return ue.exprs
}

func (ue *UpdateExprs) WithExpressions(newExprs []sql.Expression) (*UpdateExprs, error) {
	length := ue.Length()
	if len(newExprs) != length {
		return nil, sql.ErrInvalidExpressionNumber.New(ue, length, 1)
	}
	if length == 0 {
		return ue, nil
	}
	ret := *ue
	ret.exprs = newExprs
	return &ret, nil
}

// ExplicitUpdateExprs returns update expressions that are explicitly part of a query.
func (ue *UpdateExprs) ExplicitUpdateExprs() []sql.Expression {
	return ue.exprs[:ue.numExplicitExprs]
}

// DerivedUpdateExprs returns update expressions derived from a table's column definition. This includes
// updates on generated columns and ON UPDATE columns. Derived update expressions should only be applied when explicit
// updates actually yield a change in the row's values
func (ue *UpdateExprs) DerivedUpdateExprs() []sql.Expression {
	return ue.exprs[ue.numExplicitExprs:]
}

func (ue *UpdateExprs) Resolved() bool {
	if ue == nil {
		return true
	}
	return expression.ExpressionsResolved(ue.exprs...)
}

func (ue *UpdateExprs) Length() int {
	if ue == nil {
		return 0
	}
	return len(ue.exprs)
}

func (ue *UpdateExprs) HasUpdates() bool {
	return ue != nil && len(ue.exprs) > 0
}

func (ue *UpdateExprs) HasDerivedUpdates() bool {
	return ue != nil && len(ue.exprs) > ue.numExplicitExprs
}
