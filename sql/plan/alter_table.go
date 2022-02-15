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
	"io"
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
		return nil, sql.ErrRenameTableNotSupported.New(r.db.Name())
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

// CheckPrivileges implements the interface sql.Node.
func (r *RenameTable) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	var operations []sql.PrivilegedOperation
	for _, oldName := range r.oldNames {
		operations = append(operations, sql.NewPrivilegedOperation(r.db.Name(), oldName, "", sql.PrivilegeType_Alter, sql.PrivilegeType_Drop))
	}
	for _, newName := range r.newNames {
		operations = append(operations, sql.NewPrivilegedOperation(r.db.Name(), newName, "", sql.PrivilegeType_Create, sql.PrivilegeType_Insert))
	}
	return opChecker.UserHasPrivileges(ctx, operations...)
}

type AddColumn struct {
	ddlNode
	UnaryNode
	column    *sql.Column
	order     *sql.ColumnOrder
	targetSch sql.Schema
}

var _ sql.Node = (*AddColumn)(nil)
var _ sql.Databaser = (*AddColumn)(nil)
var _ sql.Expressioner = (*AddColumn)(nil)

func NewAddColumn(db sql.Database, table *UnresolvedTable, column *sql.Column, order *sql.ColumnOrder) *AddColumn {
	return &AddColumn{
		ddlNode:   ddlNode{db},
		UnaryNode: UnaryNode{Child: table},
		column:    column,
		order:     order,
	}
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
	alterable, err := getAlterable(a.Child)
	if err != nil {
		return nil, err
	}

	tbl := alterable.(sql.Table)
	tblSch := a.targetSch
	if a.order != nil && !a.order.First {
		idx := tblSch.IndexOf(a.order.AfterColumn, tbl.Name())
		if idx < 0 {
			return nil, sql.ErrTableColumnNotFound.New(tbl.Name(), a.order.AfterColumn)
		}
	}

	if err := a.validateDefaultPosition(tblSch); err != nil {
		return nil, err
	}

	err = alterable.AddColumn(ctx, a.column, a.order)
	if err != nil {
		return nil, err
	}

	return sql.RowsToRowIter(), a.updateRowsWithDefaults(ctx, row)
}

// updateRowsWithDefaults iterates through an updatable table and applies an update to each row.
func (a *AddColumn) updateRowsWithDefaults(ctx *sql.Context, row sql.Row) error {
	updatable, err := getUpdatable(a.Child)
	if err != nil {
		return err
	}

	tableIter, err := a.Child.RowIter(ctx, row)
	if err != nil {
		return err
	}

	schema := updatable.Schema()
	idx := -1
	for i, col := range schema {
		if col.Name == a.column.Name {
			idx = i
		}
	}

	updater := updatable.Updater(ctx)

	for {
		r, err := tableIter.Next(ctx)
		if err == io.EOF {
			return updater.Close(ctx)
		}

		if err != nil {
			_ = updater.Close(ctx)
			return err
		}

		updatedRow, err := applyDefaults(ctx, schema, idx, r, a.column.Default)
		if err != nil {
			return err
		}

		err = updater.Update(ctx, r, updatedRow)
		if err != nil {
			return err
		}
	}
}

// applyDefaults applies the default value of the given column index to the given row, and returns a new row with the updated values.
// This assumes that the given row has placeholder `nil` values for the default entries, and also that each column in a table is
// present and in the order as represented by the schema.
func applyDefaults(ctx *sql.Context, tblSch sql.Schema, col int, row sql.Row, cd *sql.ColumnDefaultValue) (sql.Row, error) {
	newRow := row.Copy()
	if len(tblSch) != len(row) {
		return nil, fmt.Errorf("any row given to ApplyDefaults must be of the same length as the table it represents")
	}

	if col < 0 || col > len(tblSch) {
		return nil, fmt.Errorf("column index `%d` is out of bounds, table schema has `%d` number of columns", col, len(tblSch))
	}

	columnDefaultExpr := cd
	if columnDefaultExpr == nil && !tblSch[col].Nullable {
		val := tblSch[col].Type.Zero()
		var err error
		newRow[col], err = tblSch[col].Type.Convert(val)
		if err != nil {
			return nil, err
		}
	} else {
		val, err := columnDefaultExpr.Eval(ctx, newRow)
		if err != nil {
			return nil, err
		}
		newRow[col], err = tblSch[col].Type.Convert(val)
		if err != nil {
			return nil, err
		}
	}

	return newRow, nil
}

func (a *AddColumn) Expressions() []sql.Expression {
	return append(wrappedColumnDefaults(a.targetSch), expression.WrapExpressions(a.column.Default)...)
}

func (a AddColumn) WithExpressions(exprs ...sql.Expression) (sql.Node, error) {
	if len(exprs) != 1+len(a.targetSch) {
		return nil, sql.ErrInvalidChildrenNumber.New(a, len(exprs), 1+len(a.targetSch))
	}

	a.targetSch = schemaWithDefaults(a.targetSch, exprs[:len(a.targetSch)])

	unwrappedColDefVal, ok := exprs[len(exprs)-1].(*expression.Wrapper).Unwrap().(*sql.ColumnDefaultValue)
	if ok {
		a.column.Default = unwrappedColDefVal
	} else { // nil fails type check
		a.column.Default = nil
	}
	return &a, nil
}

// Resolved implements the Resolvable interface.
func (a *AddColumn) Resolved() bool {
	if !(a.ddlNode.Resolved() && a.column.Default.Resolved()) {
		return false
	}

	for _, col := range a.targetSch {
		if !col.Default.Resolved() {
			return false
		}
	}

	return true
}

// WithTargetSchema implements sql.SchemaTarget
func (a AddColumn) WithTargetSchema(schema sql.Schema) (sql.Node, error) {
	a.targetSch = schema
	return &a, nil
}

func (a *AddColumn) TargetSchema() sql.Schema {
	return a.targetSch
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

func (a AddColumn) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(a, len(children), 1)
	}
	a.UnaryNode = UnaryNode{Child: children[0]}
	return &a, nil
}

// CheckPrivileges implements the interface sql.Node.
func (a *AddColumn) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return opChecker.UserHasPrivileges(ctx,
		sql.NewPrivilegedOperation(a.db.Name(), getTableName(a.Child), "", sql.PrivilegeType_Alter))
}

func (a *AddColumn) Children() []sql.Node {
	return a.UnaryNode.Children()
}

type DropColumn struct {
	ddlNode
	UnaryNode
	Column       string
	Checks       sql.CheckConstraints
	targetSchema sql.Schema
}

var _ sql.Node = (*DropColumn)(nil)
var _ sql.Databaser = (*DropColumn)(nil)

func NewDropColumn(db sql.Database, table *UnresolvedTable, column string) *DropColumn {
	return &DropColumn{
		ddlNode:   ddlNode{db},
		UnaryNode: UnaryNode{Child: table},
		Column:    column,
	}
}

func (d *DropColumn) WithDatabase(db sql.Database) (sql.Node, error) {
	nd := *d
	nd.db = db
	return &nd, nil
}

func (d *DropColumn) String() string {
	return fmt.Sprintf("drop column %s", d.Column)
}

func (d *DropColumn) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	alterable, err := getAlterable(d.Child)
	if err != nil {
		return nil, err
	}

	tbl := alterable.(sql.Table)
	found := false
	for _, column := range tbl.Schema() {
		if column.Name == d.Column {
			found = true
			break
		}
	}

	if !found {
		return nil, sql.ErrTableColumnNotFound.New(tbl.Name(), d.Column)
	}

	for _, col := range d.targetSchema {
		if col.Default == nil {
			continue
		}
		var err error
		sql.Inspect(col.Default, func(expr sql.Expression) bool {
			switch expr := expr.(type) {
			case *expression.GetField:
				if expr.Name() == d.Column {
					err = sql.ErrDropColumnReferencedInDefault.New(d.Column, expr.Name())
					return false
				}
			}
			return true
		})
		if err != nil {
			return nil, err
		}
	}

	return sql.RowsToRowIter(), alterable.DropColumn(ctx, d.Column)
}

func (d *DropColumn) Schema() sql.Schema {
	return nil
}

func (d *DropColumn) Resolved() bool {
	if !(d.UnaryNode.Resolved() && d.ddlNode.Resolved()) {
		return false
	}

	for _, col := range d.targetSchema {
		if !col.Default.Resolved() {
			return false
		}
	}

	return true
}

func (d *DropColumn) Children() []sql.Node {
	return d.UnaryNode.Children()
}

func (d DropColumn) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(d, len(children), 1)
	}
	d.UnaryNode.Child = children[0]
	return &d, nil
}

// CheckPrivileges implements the interface sql.Node.
func (d *DropColumn) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return opChecker.UserHasPrivileges(ctx,
		sql.NewPrivilegedOperation(d.db.Name(), getTableName(d.Child), "", sql.PrivilegeType_Alter))
}

func (d DropColumn) WithTargetSchema(schema sql.Schema) (sql.Node, error) {
	d.targetSchema = schema
	return &d, nil
}

func (d *DropColumn) TargetSchema() sql.Schema {
	return d.targetSchema
}

func (d *DropColumn) Expressions() []sql.Expression {
	return wrappedColumnDefaults(d.targetSchema)
}

func (d DropColumn) WithExpressions(exprs ...sql.Expression) (sql.Node, error) {
	if len(exprs) != len(d.targetSchema) {
		return nil, sql.ErrInvalidChildrenNumber.New(d, len(exprs), len(d.targetSchema))
	}

	d.targetSchema = schemaWithDefaults(d.targetSchema, exprs)
	return &d, nil
}

type RenameColumn struct {
	ddlNode
	UnaryNode
	ColumnName    string
	NewColumnName string
	Checks        sql.CheckConstraints
	targetSchema  sql.Schema
}

var _ sql.Node = (*RenameColumn)(nil)
var _ sql.Databaser = (*RenameColumn)(nil)

func NewRenameColumn(db sql.Database, table *UnresolvedTable, columnName string, newColumnName string) *RenameColumn {
	return &RenameColumn{
		ddlNode:       ddlNode{db},
		UnaryNode:     UnaryNode{Child: table},
		ColumnName:    columnName,
		NewColumnName: newColumnName,
	}
}

func (r *RenameColumn) WithDatabase(db sql.Database) (sql.Node, error) {
	nr := *r
	nr.db = db
	return &nr, nil
}

func (r RenameColumn) WithTargetSchema(schema sql.Schema) (sql.Node, error) {
	r.targetSchema = schema
	return &r, nil
}

func (r *RenameColumn) TargetSchema() sql.Schema {
	return r.targetSchema
}

func (r *RenameColumn) String() string {
	return fmt.Sprintf("rename column %s to %s", r.ColumnName, r.NewColumnName)
}

func (r *RenameColumn) DebugString() string {
	pr := sql.NewTreePrinter()
	pr.WriteNode("rename column %s to %s", r.ColumnName, r.NewColumnName)

	var children []string
	for _, col := range r.targetSchema {
		children = append(children, sql.DebugString(col))
	}

	pr.WriteChildren(children...)
	return pr.String()
}

func (r *RenameColumn) Resolved() bool {
	if !r.UnaryNode.Resolved() || !r.ddlNode.Resolved() {
		return false
	}

	for _, col := range r.targetSchema {
		if !col.Default.Resolved() {
			return false
		}
	}

	return true
}

func (r *RenameColumn) Schema() sql.Schema {
	return nil
}

func (r *RenameColumn) Expressions() []sql.Expression {
	return wrappedColumnDefaults(r.targetSchema)
}

func (r RenameColumn) WithExpressions(exprs ...sql.Expression) (sql.Node, error) {
	if len(exprs) != len(r.targetSchema) {
		return nil, sql.ErrInvalidChildrenNumber.New(r, len(exprs), len(r.targetSchema))
	}

	r.targetSchema = schemaWithDefaults(r.targetSchema, exprs)
	return &r, nil
}

func (r *RenameColumn) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	alterable, err := getAlterable(r.Child)
	if err != nil {
		return nil, err
	}

	tbl := alterable.(sql.Table)
	idx := r.targetSchema.IndexOf(r.ColumnName, tbl.Name())
	if idx < 0 {
		return nil, sql.ErrTableColumnNotFound.New(tbl.Name(), r.ColumnName)
	}

	nc := *r.targetSchema[idx]
	nc.Name = r.NewColumnName
	col := &nc

	if err := updateDefaultsOnColumnRename(ctx, alterable, r.targetSchema, strings.ToLower(r.ColumnName), r.NewColumnName); err != nil {
		return nil, err
	}

	return sql.RowsToRowIter(), alterable.ModifyColumn(ctx, r.ColumnName, col, nil)
}

func (r *RenameColumn) Children() []sql.Node {
	return r.UnaryNode.Children()
}

func (r RenameColumn) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(r, len(children), 1)
	}
	r.UnaryNode.Child = children[0]
	return &r, nil
}

// CheckPrivileges implements the interface sql.Node.
func (r *RenameColumn) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return opChecker.UserHasPrivileges(ctx,
		sql.NewPrivilegedOperation(r.db.Name(), getTableName(r.Child), "", sql.PrivilegeType_Alter))
}

type ModifyColumn struct {
	ddlNode
	UnaryNode
	columnName   string
	column       *sql.Column
	order        *sql.ColumnOrder
	targetSchema sql.Schema
}

var _ sql.Node = (*ModifyColumn)(nil)
var _ sql.Databaser = (*ModifyColumn)(nil)
var _ sql.Expressioner = (*ModifyColumn)(nil)

func NewModifyColumn(db sql.Database, table *UnresolvedTable, columnName string, column *sql.Column, order *sql.ColumnOrder) *ModifyColumn {
	return &ModifyColumn{
		ddlNode: ddlNode{db},
		UnaryNode: UnaryNode{
			table,
		},
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
	return sql.Schema{}
}

func (m *ModifyColumn) String() string {
	return fmt.Sprintf("modify column %s", m.column.Name)
}

func (m ModifyColumn) WithTargetSchema(schema sql.Schema) (sql.Node, error) {
	m.targetSchema = schema
	return &m, nil
}

func (m *ModifyColumn) TargetSchema() sql.Schema {
	return m.targetSchema
}

func (m *ModifyColumn) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	alterable, err := getAlterable(m.Child)
	if err != nil {
		return nil, err
	}

	tbl := alterable.(sql.Table)
	tblSch := m.targetSchema
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
	// TODO: fix me
	if err := updateDefaultsOnColumnRename(ctx, alterable, tblSch, m.columnName, m.column.Name); err != nil {
		return nil, err
	}

	return sql.RowsToRowIter(), alterable.ModifyColumn(ctx, m.columnName, m.column, m.order)
}

func (m *ModifyColumn) Children() []sql.Node {
	return m.UnaryNode.Children()
}

func (m ModifyColumn) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(m, len(children), 1)
	}
	m.UnaryNode.Child = children[0]
	return &m, nil
}

// CheckPrivileges implements the interface sql.Node.
func (m *ModifyColumn) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return opChecker.UserHasPrivileges(ctx,
		sql.NewPrivilegedOperation(m.db.Name(), getTableName(m.Child), "", sql.PrivilegeType_Alter))
}

func (m *ModifyColumn) Expressions() []sql.Expression {
	return append(wrappedColumnDefaults(m.targetSchema), expression.WrapExpressions(m.column.Default)...)
}

func (m ModifyColumn) WithExpressions(exprs ...sql.Expression) (sql.Node, error) {
	if len(exprs) != 1+len(m.targetSchema) {
		return nil, sql.ErrInvalidChildrenNumber.New(m, len(exprs), 1+len(m.targetSchema))
	}

	m.targetSchema = schemaWithDefaults(m.targetSchema, exprs[:len(m.targetSchema)])

	unwrappedColDefVal, ok := exprs[len(exprs)-1].(*expression.Wrapper).Unwrap().(*sql.ColumnDefaultValue)
	if ok {
		m.column.Default = unwrappedColDefVal
	} else { // nil fails type check
		m.column.Default = nil
	}
	return &m, nil
}

// Resolved implements the Resolvable interface.
func (m *ModifyColumn) Resolved() bool {
	if !(m.UnaryNode.Resolved() && m.column.Default.Resolved()) {
		return false
	}

	for _, col := range m.targetSchema {
		if !col.Default.Resolved() {
			return false
		}
	}

	return true
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
func updateDefaultsOnColumnRename(ctx *sql.Context, tbl sql.AlterableTable, schema sql.Schema, oldName, newName string) error {
	if oldName == newName {
		return nil
	}
	var err error
	colsToModify := make(map[*sql.Column]struct{})
	for _, col := range schema {
		if col.Default == nil {
			continue
		}
		newCol := *col
		newCol.Default.Expression, err = expression.TransformUp(col.Default.Expression, func(e sql.Expression) (sql.Expression, error) {
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
