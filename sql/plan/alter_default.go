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
	"strings"

	"github.com/gabereiser/go-mysql-server/sql"
	"github.com/gabereiser/go-mysql-server/sql/expression"
	"github.com/gabereiser/go-mysql-server/sql/transform"
)

// AlterDefaultSet represents the ALTER COLUMN SET DEFAULT statement.
type AlterDefaultSet struct {
	ddlNode
	Table        sql.Node
	ColumnName   string
	Default      *sql.ColumnDefaultValue
	targetSchema sql.Schema
}

var _ sql.Expressioner = (*AlterDefaultSet)(nil)
var _ sql.SchemaTarget = (*AlterDefaultSet)(nil)

// AlterDefaultDrop represents the ALTER COLUMN DROP DEFAULT statement.
type AlterDefaultDrop struct {
	ddlNode
	Table        sql.Node
	ColumnName   string
	targetSchema sql.Schema
}

var _ sql.Node = (*AlterDefaultDrop)(nil)
var _ sql.SchemaTarget = (*AlterDefaultDrop)(nil)

// NewAlterDefaultSet returns a *AlterDefaultSet node.
func NewAlterDefaultSet(database sql.Database, table sql.Node, columnName string, defVal *sql.ColumnDefaultValue) *AlterDefaultSet {
	return &AlterDefaultSet{
		ddlNode:    ddlNode{db: database},
		Table:      table,
		ColumnName: columnName,
		Default:    defVal,
	}
}

// String implements the sql.Node interface.
func (d *AlterDefaultSet) String() string {
	return fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s SET DEFAULT %s", d.Table.String(), d.ColumnName, d.Default.String())
}

// RowIter implements the sql.Node interface.
func (d *AlterDefaultSet) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	// Grab the table fresh from the database.
	table, err := getTableFromDatabase(ctx, d.Database(), d.Table)
	if err != nil {
		return nil, err
	}

	alterable, ok := table.(sql.AlterableTable)
	if !ok {
		return nil, sql.ErrAlterTableNotSupported.New(d.Table)
	}

	if err != nil {
		return nil, err
	}
	loweredColName := strings.ToLower(d.ColumnName)
	var col *sql.Column
	for _, schCol := range alterable.Schema() {
		if strings.ToLower(schCol.Name) == loweredColName {
			col = schCol
			break
		}
	}
	if col == nil {
		return nil, sql.ErrTableColumnNotFound.New(d.Table, d.ColumnName)
	}
	newCol := &(*col)
	newCol.Default = d.Default
	return sql.RowsToRowIter(), alterable.ModifyColumn(ctx, d.ColumnName, newCol, nil)
}

// WithChildren implements the sql.Node interface.
func (d *AlterDefaultSet) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(d, len(children), 1)
	}

	return NewAlterDefaultSet(d.db, children[0], d.ColumnName, d.Default), nil
}

// Children implements the sql.Node interface.
func (d *AlterDefaultSet) Children() []sql.Node {
	return []sql.Node{d.Table}
}

// CheckPrivileges implements the interface sql.Node.
func (d *AlterDefaultSet) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return opChecker.UserHasPrivileges(ctx,
		sql.NewPrivilegedOperation(d.Database().Name(), getTableName(d.Table), "", sql.PrivilegeType_Alter))
}

// Resolved implements the sql.Node interface.
func (d *AlterDefaultSet) Resolved() bool {
	return d.Table.Resolved() && d.ddlNode.Resolved() && d.Default.Resolved()
}

func (d *AlterDefaultSet) Expressions() []sql.Expression {
	return append(transform.WrappedColumnDefaults(d.targetSchema), expression.WrapExpressions(d.Default)...)
}

func (d AlterDefaultSet) WithExpressions(exprs ...sql.Expression) (sql.Node, error) {
	if len(exprs) != 1+len(d.targetSchema) {
		return nil, sql.ErrInvalidChildrenNumber.New(d, len(exprs), 1+len(d.targetSchema))
	}

	d.targetSchema = transform.SchemaWithDefaults(d.targetSchema, exprs[:len(d.targetSchema)])

	unwrappedColDefVal, ok := exprs[len(exprs)-1].(*expression.Wrapper).Unwrap().(*sql.ColumnDefaultValue)
	if ok {
		d.Default = unwrappedColDefVal
	} else { // nil fails type check
		d.Default = nil
	}
	return &d, nil
}

func (d AlterDefaultSet) WithTargetSchema(schema sql.Schema) (sql.Node, error) {
	d.targetSchema = schema
	return &d, nil
}

func (d *AlterDefaultSet) TargetSchema() sql.Schema {
	return d.targetSchema
}

func (d *AlterDefaultSet) WithDatabase(database sql.Database) (sql.Node, error) {
	na := *d
	na.db = database
	return &na, nil
}

func (d AlterDefaultSet) WithDefault(expr sql.Expression) (sql.Node, error) {
	newDefault := expr.(*expression.Wrapper).Unwrap().(*sql.ColumnDefaultValue)
	d.Default = newDefault
	return &d, nil
}

// NewAlterDefaultDrop returns a *AlterDefaultDrop node.
func NewAlterDefaultDrop(database sql.Database, table sql.Node, columnName string) *AlterDefaultDrop {
	return &AlterDefaultDrop{
		ddlNode:    ddlNode{db: database},
		Table:      table,
		ColumnName: columnName,
	}
}

// String implements the sql.Node interface.
func (d *AlterDefaultDrop) String() string {
	return fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s DROP DEFAULT", getTableName(d.Table), d.ColumnName)
}

// RowIter implements the sql.Node interface.
func (d *AlterDefaultDrop) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	table, ok, err := d.ddlNode.Database().GetTableInsensitive(ctx, getTableName(d.Table))
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, sql.ErrTableNotFound.New(d.Table)
	}

	alterable, ok := table.(sql.AlterableTable)
	loweredColName := strings.ToLower(d.ColumnName)
	var col *sql.Column
	for _, schCol := range alterable.Schema() {
		if strings.ToLower(schCol.Name) == loweredColName {
			col = schCol
			break
		}
	}

	if col == nil {
		return nil, sql.ErrTableColumnNotFound.New(getTableName(d.Table), d.ColumnName)
	}
	newCol := &(*col)
	newCol.Default = nil
	return sql.RowsToRowIter(), alterable.ModifyColumn(ctx, d.ColumnName, newCol, nil)
}

// WithChildren implements the sql.Node interface.
func (d *AlterDefaultDrop) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(d, len(children), 1)
	}
	return NewAlterDefaultDrop(d.Database(), children[0], d.ColumnName), nil
}

// Children implements the sql.Node interface.
func (d *AlterDefaultDrop) Children() []sql.Node {
	return []sql.Node{d.Table}
}

func (d AlterDefaultDrop) WithTargetSchema(schema sql.Schema) (sql.Node, error) {
	d.targetSchema = schema
	return &d, nil
}

func (d *AlterDefaultDrop) TargetSchema() sql.Schema {
	return d.targetSchema
}

func (d *AlterDefaultDrop) Expressions() []sql.Expression {
	return transform.WrappedColumnDefaults(d.targetSchema)
}

func (d AlterDefaultDrop) WithExpressions(exprs ...sql.Expression) (sql.Node, error) {
	if len(exprs) != len(d.targetSchema) {
		return nil, sql.ErrInvalidChildrenNumber.New(d, len(exprs), len(d.targetSchema))
	}

	d.targetSchema = transform.SchemaWithDefaults(d.targetSchema, exprs[:len(d.targetSchema)])
	return &d, nil
}

// CheckPrivileges implements the interface sql.Node.
func (d *AlterDefaultDrop) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return opChecker.UserHasPrivileges(ctx,
		sql.NewPrivilegedOperation(d.db.Name(), getTableName(d.Table), d.ColumnName, sql.PrivilegeType_Alter))
}

// WithDatabase implements the sql.Databaser interface.
func (d *AlterDefaultDrop) WithDatabase(db sql.Database) (sql.Node, error) {
	nd := *d
	nd.db = db
	return &nd, nil
}

// getTableFromDatabase returns the related sql.Table from a database in the case of a sql.Databasw
func getTableFromDatabase(ctx *sql.Context, db sql.Database, tableNode sql.Node) (sql.Table, error) {
	// Grab the table fresh from the database.
	tableName := getTableName(tableNode)

	table, ok, err := db.GetTableInsensitive(ctx, tableName)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, sql.ErrTableNotFound.New(tableName)
	}

	return table, nil
}
