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

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

type RenameTable struct {
	ddlNode
	oldNames []string
	newNames []string
}

var _ sql.Node = (*RenameTable)(nil)
var _ sql.Databaser = (*RenameTable)(nil)

// NewRenameTable creates a new RenameTable node
func NewRenameTable(db sql.Database, oldNames, newNames []string) *RenameTable {
	return &RenameTable{
		ddlNode:  ddlNode{db},
		oldNames: oldNames,
		newNames: newNames,
	}
}

func (r *RenameTable) WithDatabase(db sql.Database) (sql.Node, error) {
	nr := *r
	nr.db = db
	return &nr, nil
}

func (r *RenameTable) String() string {
	return fmt.Sprintf("Rename table %s to %s", r.oldNames, r.newNames)
}

func (r *RenameTable) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	renamer, ok := r.db.(sql.TableRenamer)
	if !ok {
		return nil, ErrRenameTableNotSupported.New(r.db.Name())
	}

	var err error
	for i, oldName := range r.oldNames {
		var tbl sql.Table
		var ok bool
		tbl, ok, err = r.db.GetTableInsensitive(ctx, oldName)
		if err != nil {
			return nil, err
		}

		if !ok {
			return nil, sql.ErrTableNotFound.New(oldName)
		}

		err = renamer.RenameTable(ctx, tbl.Name(), r.newNames[i])
		if err != nil {
			break
		}
	}

	return sql.RowsToRowIter(), err
}

func (r *RenameTable) WithChildren(children ...sql.Node) (sql.Node, error) {
	return NillaryWithChildren(r, children...)
}

type AddColumn struct {
	ddlNode
	tableName string
	column    *sql.Column
	order     *sql.ColumnOrder
}

var _ sql.Node = (*AddColumn)(nil)
var _ sql.Databaser = (*AddColumn)(nil)
var _ sql.Expressioner = (*AddColumn)(nil)

func NewAddColumn(db sql.Database, tableName string, column *sql.Column, order *sql.ColumnOrder) *AddColumn {
	return &AddColumn{
		ddlNode:   ddlNode{db},
		tableName: tableName,
		column:    column,
		order:     order,
	}
}

func (a *AddColumn) TableName() string {
	return a.tableName
}

func (a *AddColumn) Column() *sql.Column {
	return a.column
}

func (a *AddColumn) Order() *sql.ColumnOrder {
	return a.order
}

func (a *AddColumn) WithDatabase(db sql.Database) (sql.Node, error) {
	na := *a
	na.db = db
	return &na, nil
}

// Schema implements the sql.Node interface.
func (a *AddColumn) Schema() sql.Schema {
	return sql.Schema{a.column}
}

func (a *AddColumn) String() string {
	return fmt.Sprintf("add column %s", a.column.Name)
}

func (a *AddColumn) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	alterable, err := getAlterableTable(a.db, ctx, a.tableName)
	if err != nil {
		return nil, err
	}

	tbl := alterable.(sql.Table)
	tblSch := tbl.Schema()
	if a.order != nil && !a.order.First {
		idx := tblSch.IndexOf(a.order.AfterColumn, tbl.Name())
		if idx < 0 {
			return nil, sql.ErrTableColumnNotFound.New(tbl.Name(), a.order.AfterColumn)
		}
	}

	if err := a.validateDefaultPosition(tblSch); err != nil {
		return nil, err
	}

	return sql.RowsToRowIter(), alterable.AddColumn(ctx, a.column, a.order)
}

func (a *AddColumn) Expressions() []sql.Expression {
	return expression.WrapExpressions(a.column.Default)
}

func (a *AddColumn) WithExpressions(exprs ...sql.Expression) (sql.Node, error) {
	if len(exprs) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(a, len(exprs), 1)
	}
	na := *a
	unwrappedColDefVal, ok := exprs[0].(*expression.Wrapper).Unwrap().(*sql.ColumnDefaultValue)
	if ok {
		na.column.Default = unwrappedColDefVal
	} else { // nil fails type check
		na.column.Default = nil
	}
	return &na, nil
}

// Resolved implements the Resolvable interface.
func (a *AddColumn) Resolved() bool {
	return a.ddlNode.Resolved() && a.column.Default.Resolved()
}

func (a *AddColumn) validateDefaultPosition(tblSch sql.Schema) error {
	colsAfterThis := map[string]*sql.Column{a.column.Name: a.column}
	if a.order != nil {
		if a.order.First {
			for i := 0; i < len(tblSch); i++ {
				colsAfterThis[tblSch[i].Name] = tblSch[i]
			}
		} else {
			i := 1
			for ; i < len(tblSch); i++ {
				if tblSch[i-1].Name == a.order.AfterColumn {
					break
				}
			}
			for ; i < len(tblSch); i++ {
				colsAfterThis[tblSch[i].Name] = tblSch[i]
			}
		}
	}

	err := inspectDefaultForInvalidColumns(a.column, colsAfterThis)
	if err != nil {
		return err
	}

	return nil
}

func (a *AddColumn) WithChildren(children ...sql.Node) (sql.Node, error) {
	return NillaryWithChildren(a, children...)
}

type DropColumn struct {
	ddlNode
	tableName string
	column    string
	order     *sql.ColumnOrder
}

var _ sql.Node = (*DropColumn)(nil)
var _ sql.Databaser = (*DropColumn)(nil)

func NewDropColumn(db sql.Database, tableName string, column string) *DropColumn {
	return &DropColumn{
		ddlNode:   ddlNode{db},
		tableName: tableName,
		column:    column,
	}
}

func (d *DropColumn) WithDatabase(db sql.Database) (sql.Node, error) {
	nd := *d
	nd.db = db
	return &nd, nil
}

func (d *DropColumn) String() string {
	return fmt.Sprintf("drop column %s", d.column)
}

func (d *DropColumn) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	alterable, err := getAlterableTable(d.db, ctx, d.tableName)
	if err != nil {
		return nil, err
	}

	tbl := alterable.(sql.Table)
	found := false
	for _, column := range tbl.Schema() {
		if column.Name == d.column {
			found = true
			break
		}
	}

	if !found {
		return nil, sql.ErrTableColumnNotFound.New(tbl.Name(), d.column)
	}

	for _, col := range tbl.Schema() {
		if col.Default == nil {
			continue
		}
		var err error
		sql.Inspect(col.Default, func(expr sql.Expression) bool {
			switch expr := expr.(type) {
			case *expression.GetField:
				if expr.Name() == d.column {
					err = sql.ErrDropColumnReferencedInDefault.New(d.column, expr.Name())
					return false
				}
			}
			return true
		})
		if err != nil {
			return nil, err
		}
	}

	return sql.RowsToRowIter(), alterable.DropColumn(ctx, d.column)
}

func (d *DropColumn) WithChildren(children ...sql.Node) (sql.Node, error) {
	return NillaryWithChildren(d, children...)
}

type RenameColumn struct {
	ddlNode
	tableName     string
	columnName    string
	newColumnName string
}

var _ sql.Node = (*RenameColumn)(nil)
var _ sql.Databaser = (*RenameColumn)(nil)

func NewRenameColumn(db sql.Database, tableName string, columnName string, newColumnName string) *RenameColumn {
	return &RenameColumn{
		ddlNode:       ddlNode{db},
		tableName:     tableName,
		columnName:    columnName,
		newColumnName: newColumnName,
	}
}

func (r *RenameColumn) WithDatabase(db sql.Database) (sql.Node, error) {
	nr := *r
	nr.db = db
	return &nr, nil
}

func (r *RenameColumn) String() string {
	return fmt.Sprintf("rename column %s to %s", r.columnName, r.newColumnName)
}

func (r *RenameColumn) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	alterable, err := getAlterableTable(r.db, ctx, r.tableName)
	if err != nil {
		return nil, err
	}

	tbl := alterable.(sql.Table)
	idx := tbl.Schema().IndexOf(r.columnName, tbl.Name())
	if idx < 0 {
		return nil, sql.ErrTableColumnNotFound.New(tbl.Name(), r.columnName)
	}

	nc := *tbl.Schema()[idx]
	nc.Name = r.newColumnName
	col := &nc

	if err := updateDefaultsOnColumnRename(ctx, alterable, strings.ToLower(r.columnName), r.newColumnName); err != nil {
		return nil, err
	}

	return sql.RowsToRowIter(), alterable.ModifyColumn(ctx, r.columnName, col, nil)
}

func (r *RenameColumn) WithChildren(children ...sql.Node) (sql.Node, error) {
	return NillaryWithChildren(r, children...)
}

type ModifyColumn struct {
	ddlNode
	tableName  string
	columnName string
	column     *sql.Column
	order      *sql.ColumnOrder
}

var _ sql.Node = (*ModifyColumn)(nil)
var _ sql.Databaser = (*ModifyColumn)(nil)
var _ sql.Expressioner = (*ModifyColumn)(nil)

func NewModifyColumn(db sql.Database, tableName string, columnName string, column *sql.Column, order *sql.ColumnOrder) *ModifyColumn {
	return &ModifyColumn{
		ddlNode:    ddlNode{db},
		tableName:  tableName,
		columnName: columnName,
		column:     column,
		order:      order,
	}
}

func (m *ModifyColumn) WithDatabase(db sql.Database) (sql.Node, error) {
	nm := *m
	nm.db = db
	return &nm, nil
}

func (m *ModifyColumn) TableName() string {
	return m.tableName
}

func (m *ModifyColumn) Column() string {
	return m.columnName
}

func (m *ModifyColumn) NewColumn() *sql.Column {
	return m.column
}

func (m *ModifyColumn) Order() *sql.ColumnOrder {
	return m.order
}

// Schema implements the sql.Node interface.
func (m *ModifyColumn) Schema() sql.Schema {
	return sql.Schema{m.column}
}

func (m *ModifyColumn) String() string {
	return fmt.Sprintf("modify column %s", m.column.Name)
}

func (m *ModifyColumn) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	alterable, err := getAlterableTable(m.db, ctx, m.tableName)
	if err != nil {
		return nil, err
	}

	tbl := alterable.(sql.Table)
	tblSch := tbl.Schema()
	idx := tblSch.IndexOf(m.columnName, tbl.Name())
	if idx < 0 {
		return nil, sql.ErrTableColumnNotFound.New(tbl.Name(), m.columnName)
	}

	if m.order != nil && !m.order.First {
		idx = tblSch.IndexOf(m.order.AfterColumn, tbl.Name())
		if idx < 0 {
			return nil, sql.ErrTableColumnNotFound.New(tbl.Name(), m.order.AfterColumn)
		}
	}

	if err := m.validateDefaultPosition(tblSch); err != nil {
		return nil, err
	}
	if err := updateDefaultsOnColumnRename(ctx, alterable, m.columnName, m.column.Name); err != nil {
		return nil, err
	}

	return sql.RowsToRowIter(), alterable.ModifyColumn(ctx, m.columnName, m.column, m.order)
}

func (m *ModifyColumn) WithChildren(children ...sql.Node) (sql.Node, error) {
	return NillaryWithChildren(m, children...)
}

func (m *ModifyColumn) Expressions() []sql.Expression {
	return expression.WrapExpressions(m.column.Default)
}

func (m *ModifyColumn) WithExpressions(exprs ...sql.Expression) (sql.Node, error) {
	if len(exprs) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(m, len(exprs), 1)
	}
	nm := *m
	unwrappedColDefVal, ok := exprs[0].(*expression.Wrapper).Unwrap().(*sql.ColumnDefaultValue)
	if ok {
		nm.column.Default = unwrappedColDefVal
	} else { // nil fails type check
		nm.column.Default = nil
	}
	return &nm, nil
}

// Resolved implements the Resolvable interface.
func (m *ModifyColumn) Resolved() bool {
	return m.ddlNode.Resolved() && m.column.Default.Resolved()
}

// Gets an AlterableTable with the name given from the database, or an error if it cannot.
func getAlterableTable(db sql.Database, ctx *sql.Context, tableName string) (sql.AlterableTable, error) {
	tbl, ok, err := db.GetTableInsensitive(ctx, tableName)
	if err != nil {
		return nil, err
	}

	if !ok {
		return nil, sql.ErrTableNotFound.New(tableName)
	}

	alterable, ok := tbl.(sql.AlterableTable)
	if !ok {
		return nil, ErrAlterTableNotSupported.New(tableName, db.Name())
	}

	return alterable, nil
}

func (m *ModifyColumn) validateDefaultPosition(tblSch sql.Schema) error {
	colsBeforeThis := make(map[string]*sql.Column)
	colsAfterThis := make(map[string]*sql.Column) // includes the modified column
	if m.order == nil {
		i := 0
		for ; i < len(tblSch); i++ {
			if tblSch[i].Name == m.column.Name {
				colsAfterThis[m.column.Name] = m.column
				break
			}
			colsBeforeThis[tblSch[i].Name] = tblSch[i]
		}
		for ; i < len(tblSch); i++ {
			colsAfterThis[tblSch[i].Name] = tblSch[i]
		}
	} else if m.order.First {
		for i := 0; i < len(tblSch); i++ {
			colsAfterThis[tblSch[i].Name] = tblSch[i]
		}
	} else {
		i := 1
		for ; i < len(tblSch); i++ {
			colsBeforeThis[tblSch[i].Name] = tblSch[i]
			if tblSch[i-1].Name == m.order.AfterColumn {
				break
			}
		}
		for ; i < len(tblSch); i++ {
			colsAfterThis[tblSch[i].Name] = tblSch[i]
		}
		delete(colsBeforeThis, m.column.Name)
		colsAfterThis[m.column.Name] = m.column
	}

	err := inspectDefaultForInvalidColumns(m.column, colsAfterThis)
	if err != nil {
		return err
	}
	thisCol := map[string]*sql.Column{m.column.Name: m.column}
	for _, colBefore := range colsBeforeThis {
		err = inspectDefaultForInvalidColumns(colBefore, thisCol)
		if err != nil {
			return err
		}
	}

	return nil
}

// updateDefaultsOnColumnRename updates each column that references the old column name within its default value.
func updateDefaultsOnColumnRename(ctx *sql.Context, tbl sql.AlterableTable, oldName, newName string) error {
	if oldName == newName {
		return nil
	}
	var err error
	colsToModify := make(map[*sql.Column]struct{})
	for _, col := range tbl.Schema() {
		if col.Default == nil {
			continue
		}
		newCol := *col
		newCol.Default.Expression, err = expression.TransformUp(ctx, col.Default.Expression, func(e sql.Expression) (sql.Expression, error) {
			if expr, ok := e.(*expression.GetField); ok {
				if strings.ToLower(expr.Name()) == oldName {
					colsToModify[&newCol] = struct{}{}
					return expr.WithName(newName), nil
				}
			}
			return e, nil
		})
		if err != nil {
			return err
		}
	}
	for col := range colsToModify {
		err := tbl.ModifyColumn(ctx, col.Name, col, nil)
		if err != nil {
			return err
		}
	}
	return nil
}

func inspectDefaultForInvalidColumns(col *sql.Column, columnsAfterThis map[string]*sql.Column) error {
	if col.Default == nil {
		return nil
	}
	var err error
	sql.Inspect(col.Default, func(expr sql.Expression) bool {
		switch expr := expr.(type) {
		case *expression.GetField:
			if col, ok := columnsAfterThis[expr.Name()]; ok && col.Default != nil && !col.Default.IsLiteral() {
				err = sql.ErrInvalidDefaultValueOrder.New(col.Name)
				return false
			}
		}
		return true
	})
	return err
}
