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

	errors "gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/sql"
)

// ErrUnresolvedTable is thrown when a table cannot be resolved
var ErrUnresolvedTable = errors.NewKind("unresolved table")

// UnresolvedTable is a table that has not been resolved yet but whose name is known.
type UnresolvedTable struct {
	name     string
	database string
	asOf     sql.Expression
}

var _ sql.Node = (*UnresolvedTable)(nil)
var _ sql.Expressioner = (*UnresolvedTable)(nil)
var _ sql.UnresolvedTable = (*UnresolvedTable)(nil)

// NewUnresolvedTable creates a new Unresolved table.
func NewUnresolvedTable(name, db string) *UnresolvedTable {
	return &UnresolvedTable{name, db, nil}
}

// NewUnresolvedTableAsOf creates a new Unresolved table with an AS OF expression.
func NewUnresolvedTableAsOf(name, db string, asOf sql.Expression) *UnresolvedTable {
	return &UnresolvedTable{name, db, asOf}
}

// Name implements the Nameable interface.
func (t *UnresolvedTable) Name() string {
	return t.name
}

// Database implements sql.UnresolvedTable
func (t *UnresolvedTable) Database() string {
	return t.database
}

// Resolved implements the Resolvable interface.
func (*UnresolvedTable) Resolved() bool {
	return false
}

// Children implements the Node interface.
func (*UnresolvedTable) Children() []sql.Node { return nil }

// Schema implements the Node interface.
func (*UnresolvedTable) Schema() sql.Schema { return nil }

// RowIter implements the RowIter interface.
func (*UnresolvedTable) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	return nil, ErrUnresolvedTable.New()
}

// WithChildren implements the Node interface.
func (t *UnresolvedTable) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(t, len(children), 0)
	}

	return t, nil
}

// AsOf implements sql.UnresolvedTable
func (t *UnresolvedTable) AsOf() sql.Expression {
	return t.asOf
}

// CheckPrivileges implements the interface sql.Node.
func (t *UnresolvedTable) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return opChecker.UserHasPrivileges(ctx,
		sql.NewPrivilegedOperation(t.Database(), t.name, "", sql.PrivilegeType_Select))
}

// WithAsOf implements sql.UnresolvedTable
func (t *UnresolvedTable) WithAsOf(asOf sql.Expression) (sql.Node, error) {
	t2 := *t
	t2.asOf = asOf
	return &t2, nil
}

// WithDatabase returns a copy of this unresolved table with its Database field set to the given value. Analagous to
// WithChildren.
func (t *UnresolvedTable) WithDatabase(database string) (*UnresolvedTable, error) {
	t2 := *t
	t2.database = database
	return &t2, nil
}

func (t *UnresolvedTable) Expressions() []sql.Expression {
	if t.asOf != nil {
		return []sql.Expression{t.asOf}
	}
	return nil
}

func (t *UnresolvedTable) WithExpressions(expressions ...sql.Expression) (sql.Node, error) {
	if t.asOf == nil {
		if len(expressions) != 0 {
			return nil, sql.ErrInvalidChildrenNumber.New(t, len(expressions), 0)
		}
		return t, nil
	}

	if len(expressions) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(t, len(expressions), 1)
	}

	return t.WithAsOf(expressions[0])
}

func (t UnresolvedTable) String() string {
	return fmt.Sprintf("UnresolvedTable(%s)", t.name)
}

func NewDeferredAsOfTable(t *ResolvedTable, asOf sql.Expression) *DeferredAsOfTable {
	if asOf == nil {
		panic("Cannot create DeferredAsOfTable with nil asOf expression")
	}
	return &DeferredAsOfTable{
		ResolvedTable: t,
		asOf:          asOf,
	}
}

type DeferredAsOfTable struct {
	*ResolvedTable
	asOf sql.Expression
}

func (t *DeferredAsOfTable) Expressions() []sql.Expression {
	return []sql.Expression{t.asOf}
}

func (t *DeferredAsOfTable) WithExpressions(expressions ...sql.Expression) (sql.Node, error) {
	if t.asOf == nil {
		if len(expressions) != 0 {
			return nil, sql.ErrInvalidChildrenNumber.New(t, len(expressions), 0)
		}
		return t, nil
	}

	if len(expressions) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(t, len(expressions), 1)
	}

	return t.WithAsOf(expressions[0])
}

// Name implements the Nameable interface.
func (t *DeferredAsOfTable) Name() string {
	return t.ResolvedTable.Name()
}

// Database implements sql.UnresolvedTable
func (t *DeferredAsOfTable) Database() string {
	return t.ResolvedTable.Database.Name()
}

// WithAsOf implements sql.UnresolvedTable
func (t *DeferredAsOfTable) WithAsOf(asOf sql.Expression) (sql.Node, error) {
	t2 := *t
	t2.asOf = asOf
	return &t2, nil
}

// AsOf implements sql.UnresolvedTable
func (t *DeferredAsOfTable) AsOf() sql.Expression {
	return t.asOf
}

var _ sql.Node = (*DeferredAsOfTable)(nil)
var _ sql.Expressioner = (*DeferredAsOfTable)(nil)
var _ sql.UnresolvedTable = (*DeferredAsOfTable)(nil)

type DeferredTableFunction struct {
	tableFunction sql.TableFunction
	database      sql.Database
	arguments     []sql.Expression
}

var _ sql.TableFunction = (*DeferredTableFunction)(nil)

func NewDeferredTableFunction(tableFunction sql.TableFunction, database sql.Database, arguments []sql.Expression) *DeferredTableFunction {
	return &DeferredTableFunction{
		tableFunction: tableFunction,
		database:      database,
		arguments:     arguments,
	}
}

func (dtf *DeferredTableFunction) Resolved() bool {
	return true
}

func (dtf *DeferredTableFunction) String() string {
	return ""
}

func (dtf *DeferredTableFunction) Schema() sql.Schema {
	return dtf.tableFunction.Schema()
}

func (dtf *DeferredTableFunction) Children() []sql.Node {
	return nil
}

func (dtf *DeferredTableFunction) WithChildren(node ...sql.Node) (sql.Node, error) {
	return dtf, nil
}

func (dtf *DeferredTableFunction) RowIter(_ *sql.Context, _ sql.Row) (sql.RowIter, error) {
	return nil, nil
}

func (dtf *DeferredTableFunction) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return true
}

// Expressions implements the sql.Expressioner interface
func (dtf *DeferredTableFunction) Expressions() []sql.Expression {
	return dtf.arguments
}

// WithExpressions implements the sql.Expressioner interface
func (dtf *DeferredTableFunction) WithExpressions(expression ...sql.Expression) (sql.Node, error) {
	dtf.arguments = expression
	return dtf, nil
}

func (dtf *DeferredTableFunction) Database() sql.Database {
	return dtf.database
}

func (dtf *DeferredTableFunction) WithDatabase(database sql.Database) (sql.Node, error) {
	dtf.database = database
	return dtf, nil
}

func (dtf *DeferredTableFunction) NewInstance(ctx *sql.Context, database sql.Database, args []sql.Expression) (sql.Node, error) {
	panic("tried to create new instance of DeferredTableFunction")
	return nil, nil
}

func (dtf *DeferredTableFunction) FunctionName() string {
	return ""
}
