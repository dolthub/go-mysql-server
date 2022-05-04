// Copyright 2021 Dolthub, Inc.
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
)

type PKAction byte

const (
	PrimaryKeyAction_Create PKAction = iota
	PrimaryKeyAction_Drop
)

// ErrNotPrimaryKeyAlterable is return when a table cannot be determined to be primary key alterable
var ErrNotPrimaryKeyAlterable = errors.NewKind("error: table is not primary key alterable")

type AlterPK struct {
	ddlNode

	Action  PKAction
	Table   sql.Node
	Columns []sql.IndexColumn
	Catalog sql.Catalog
	targetSchema sql.Schema
}

var _ sql.Databaser = (*AlterPK)(nil)

func NewAlterCreatePk(db sql.Database, table sql.Node, columns []sql.IndexColumn) *AlterPK {
	return &AlterPK{
		Action:  PrimaryKeyAction_Create,
		ddlNode: ddlNode{db: db},
		Table:   table,
		Columns: columns,
	}
}

func NewAlterDropPk(db sql.Database, table sql.Node) *AlterPK {
	return &AlterPK{
		Action:  PrimaryKeyAction_Drop,
		Table:   table,
		ddlNode: ddlNode{db: db},
	}
}

func (a *AlterPK) Resolved() bool {
	return a.Table.Resolved() && a.ddlNode.Resolved()
}

func (a *AlterPK) String() string {
	action := "add"
	if a.Action == PrimaryKeyAction_Drop {
		action = "drop"
	}

	return fmt.Sprintf("alter table %s %s primary key", a.Table.String(), action)
}

func (a *AlterPK) Schema() sql.Schema {
	return sql.OkResultSchema
}

func (a AlterPK) WithTargetSchema(schema sql.Schema) (sql.Node, error) {
	a.targetSchema = schema
	return &a, nil
}

func (a *AlterPK) TargetSchema() sql.Schema {
	return a.targetSchema
}

func (a *AlterPK) Expressions() []sql.Expression {
	return wrappedColumnDefaults(a.targetSchema)
}

func (a AlterPK) WithExpressions(exprs ...sql.Expression) (sql.Node, error) {
	if len(exprs) != len(a.targetSchema) {
		return nil, sql.ErrInvalidChildrenNumber.New(a, len(exprs), len(a.targetSchema))
	}

	a.targetSchema = schemaWithDefaults(a.targetSchema, exprs[:len(a.targetSchema)])
	return &a, nil
}

func (a *AlterPK) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	// We grab the table from the database to ensure that state is properly refreshed, thereby preventing multiple keys
	// being defined.
	// Grab the table fresh from the database.
	table, err := getTableFromDatabase(ctx, a.Database(), a.Table)
	if err != nil {
		return nil, err
	}

	pkAlterable, ok := table.(sql.PrimaryKeyAlterableTable)
	if !ok {
		return nil, ErrNotPrimaryKeyAlterable.New(a.Table)
	}
	if err != nil {
		return nil, err
	}

	switch a.Action {
	case PrimaryKeyAction_Create:
		if hasPrimaryKeys(pkAlterable) {
			return sql.RowsToRowIter(), sql.ErrMultiplePrimaryKeysDefined.New()
		}

		for _, c := range a.Columns {
			if !pkAlterable.Schema().Contains(c.Name, pkAlterable.Name()) {
				return sql.RowsToRowIter(), sql.ErrKeyColumnDoesNotExist.New(c.Name)
			}
		}

		err = pkAlterable.CreatePrimaryKey(ctx, a.Columns)
	case PrimaryKeyAction_Drop:
		err = pkAlterable.DropPrimaryKey(ctx)
	}

	if err != nil {
		return nil, err
	}

	return sql.RowsToRowIter(sql.NewRow(sql.NewOkResult(0))), nil
}

func hasPrimaryKeys(table sql.Table) bool {
	for _, c := range table.Schema() {
		if c.PrimaryKey {
			return true
		}
	}

	return false
}

func (a *AlterPK) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(a, len(children), 1)
	}

	switch a.Action {
	case PrimaryKeyAction_Create:
		return NewAlterCreatePk(a.db, children[0], a.Columns), nil
	case PrimaryKeyAction_Drop:
		return NewAlterDropPk(a.db, children[0]), nil
	default:
		return nil, ErrIndexActionNotImplemented.New(a.Action)
	}
}

// Children implements the sql.Node interface.
func (a *AlterPK) Children() []sql.Node {
	return []sql.Node{a.Table}
}

// WithDatabase implements the sql.Databaser interface.
func (a *AlterPK) WithDatabase(database sql.Database) (sql.Node, error) {
	na := *a
	na.db = database
	return &na, nil
}

// CheckPrivileges implements the interface sql.Node.
func (a *AlterPK) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return opChecker.UserHasPrivileges(ctx,
		sql.NewPrivilegedOperation(a.Database().Name(), getTableName(a.Table), "", sql.PrivilegeType_Alter))
}
