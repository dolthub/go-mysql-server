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

	"github.com/dolthub/vitess/go/sqltypes"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/transform"
	"github.com/dolthub/go-mysql-server/sql/types"
)

type RenameTable struct {
	ddlNode
	oldNames []string
	newNames []string
}

var _ sql.Node = (*RenameTable)(nil)
var _ sql.Databaser = (*RenameTable)(nil)
var _ sql.CollationCoercible = (*RenameTable)(nil)

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

		if fkTable, ok := tbl.(sql.ForeignKeyTable); ok {
			parentFks, err := fkTable.GetReferencedForeignKeys(ctx)
			if err != nil {
				return nil, err
			}
			for _, parentFk := range parentFks {
				//TODO: support renaming tables across databases for foreign keys
				if strings.ToLower(parentFk.Database) != strings.ToLower(parentFk.ParentDatabase) {
					return nil, fmt.Errorf("updating foreign key table names across databases is not yet supported")
				}
				parentFk.ParentTable = r.newNames[i]
				childTbl, ok, err := r.db.GetTableInsensitive(ctx, parentFk.Table)
				if err != nil {
					return nil, err
				}
				if !ok {
					return nil, sql.ErrTableNotFound.New(parentFk.Table)
				}
				childFkTbl, ok := childTbl.(sql.ForeignKeyTable)
				if !ok {
					return nil, fmt.Errorf("referenced table `%s` supports foreign keys but declaring table `%s` does not", parentFk.ParentTable, parentFk.Table)
				}
				err = childFkTbl.UpdateForeignKey(ctx, parentFk.Name, parentFk)
				if err != nil {
					return nil, err
				}
			}

			fks, err := fkTable.GetDeclaredForeignKeys(ctx)
			if err != nil {
				return nil, err
			}
			for _, fk := range fks {
				fk.Table = r.newNames[i]
				err = fkTable.UpdateForeignKey(ctx, fk.Name, fk)
				if err != nil {
					return nil, err
				}
			}
		}

		err = renamer.RenameTable(ctx, tbl.Name(), r.newNames[i])
		if err != nil {
			return nil, err
		}
	}

	return sql.RowsToRowIter(sql.NewRow(types.NewOkResult(0))), nil
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

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*RenameTable) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 7
}

type AddColumn struct {
	ddlNode
	Table     sql.Node
	column    *sql.Column
	order     *sql.ColumnOrder
	targetSch sql.Schema
}

var _ sql.Node = (*AddColumn)(nil)
var _ sql.Expressioner = (*AddColumn)(nil)
var _ sql.SchemaTarget = (*AddColumn)(nil)
var _ sql.CollationCoercible = (*AddColumn)(nil)

func (a *AddColumn) DebugString() string {
	pr := sql.NewTreePrinter()
	pr.WriteNode("add column %s to %s", a.column.Name, a.Table)

	var children []string
	children = append(children, sql.DebugString(a.column))
	for _, col := range a.targetSch {
		children = append(children, sql.DebugString(col))
	}

	pr.WriteChildren(children...)
	return pr.String()
}

func NewAddColumn(database sql.Database, table *UnresolvedTable, column *sql.Column, order *sql.ColumnOrder) *AddColumn {
	column.Source = table.name
	return &AddColumn{
		ddlNode: ddlNode{db: database},
		Table:   table,
		column:  column,
		order:   order,
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
	return types.OkResultSchema
}

func (a *AddColumn) String() string {
	return fmt.Sprintf("add column %s", a.column.Name)
}

func (a *AddColumn) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	table, err := getTableFromDatabase(ctx, a.Database(), a.Table)
	if err != nil {
		return nil, err
	}

	alterable, ok := table.(sql.AlterableTable)
	if !ok {
		return nil, sql.ErrAlterTableNotSupported.New(table.Name())
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
	// MySQL assigns the column's type (which contains the collation) at column creation/modification. If a column has
	// an invalid collation, then one has not been assigned at this point, so we assign it the table's collation. This
	// does not create a reference to the table's collation, which may change at any point, and therefore will have no
	// relation to this column after assignment.
	if collatedType, ok := a.column.Type.(sql.TypeWithCollation); ok && collatedType.Collation() == sql.Collation_Unspecified {
		a.column.Type, err = collatedType.WithNewCollation(alterable.Collation())
		if err != nil {
			return nil, err
		}
	}
	for _, col := range a.targetSch {
		if collatedType, ok := col.Type.(sql.TypeWithCollation); ok && collatedType.Collation() == sql.Collation_Unspecified {
			col.Type, err = collatedType.WithNewCollation(alterable.Collation())
			if err != nil {
				return nil, err
			}
		}
	}

	return &addColumnIter{
		a:         a,
		alterable: alterable,
	}, nil
}

// updateRowsWithDefaults iterates through an updatable table and applies an update to each row.
func (a *AddColumn) updateRowsWithDefaults(ctx *sql.Context, table sql.Table) error {
	rt := NewResolvedTable(table, a.db, nil)
	updatable, ok := table.(sql.UpdatableTable)
	if !ok {
		return ErrUpdateNotSupported.New(rt.Name())
	}

	tableIter, err := rt.RowIter(ctx, nil)
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
	return append(transform.WrappedColumnDefaults(a.targetSch), expression.WrapExpressions(a.column.Default)...)
}

func (a AddColumn) WithExpressions(exprs ...sql.Expression) (sql.Node, error) {
	if len(exprs) != 1+len(a.targetSch) {
		return nil, sql.ErrInvalidChildrenNumber.New(a, len(exprs), 1+len(a.targetSch))
	}

	a.targetSch = transform.SchemaWithDefaults(a.targetSch, exprs[:len(a.targetSch)])

	unwrappedColDefVal, ok := exprs[len(exprs)-1].(*expression.Wrapper).Unwrap().(*sql.ColumnDefaultValue)

	// *sql.Column is a reference type, make a copy before we modify it so we don't affect the original node
	a.column = a.column.Copy()
	if ok {
		a.column.Default = unwrappedColDefVal
	} else { // nil fails type check
		a.column.Default = nil
	}
	return &a, nil
}

// Resolved implements the Resolvable interface.
func (a *AddColumn) Resolved() bool {
	if !(a.ddlNode.Resolved() && a.Table.Resolved() && a.column.Default.Resolved()) {
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
	a.Table = children[0]
	return &a, nil
}

// CheckPrivileges implements the interface sql.Node.
func (a *AddColumn) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return opChecker.UserHasPrivileges(ctx,
		sql.NewPrivilegedOperation(a.db.Name(), getTableName(a.Table), "", sql.PrivilegeType_Alter))
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*AddColumn) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 7
}

func (a *AddColumn) Children() []sql.Node {
	return []sql.Node{a.Table}
}

type addColumnIter struct {
	a         *AddColumn
	alterable sql.AlterableTable
	runOnce   bool
}

func (i *addColumnIter) Next(ctx *sql.Context) (sql.Row, error) {
	if i.runOnce {
		return nil, io.EOF
	}
	i.runOnce = true

	rwt, ok := i.alterable.(sql.RewritableTable)
	if ok {
		rewritten, err := i.rewriteTable(ctx, rwt)
		if err != nil {
			return nil, err
		}
		if rewritten {
			return sql.NewRow(types.NewOkResult(0)), nil
		}
	}

	err := i.alterable.AddColumn(ctx, i.a.column, i.a.order)
	if err != nil {
		return nil, err
	}

	// We only need to update all table rows if the new column is non-nil
	if i.a.column.Nullable && i.a.column.Default == nil {
		return sql.NewRow(types.NewOkResult(0)), nil
	}

	err = i.a.updateRowsWithDefaults(ctx, i.alterable)
	if err != nil {
		return nil, err
	}

	return sql.NewRow(types.NewOkResult(0)), nil
}

func (i addColumnIter) Close(context *sql.Context) error {
	return nil
}

// rewriteTable rewrites the table given if required or requested, and returns the whether it was rewritten
func (i *addColumnIter) rewriteTable(ctx *sql.Context, rwt sql.RewritableTable) (bool, error) {
	newSch, projections, err := addColumnToSchema(i.a.targetSch, i.a.column, i.a.order)
	if err != nil {
		return false, err
	}

	oldPkSchema, newPkSchema := sql.SchemaToPrimaryKeySchema(rwt, rwt.Schema()), sql.SchemaToPrimaryKeySchema(rwt, newSch)

	rewriteRequired := false
	if i.a.column.Default != nil || !i.a.column.Nullable {
		rewriteRequired = true
	}

	rewriteRequested := rwt.ShouldRewriteTable(ctx, oldPkSchema, newPkSchema, nil, i.a.column)
	if !rewriteRequired && !rewriteRequested {
		return false, nil
	}

	inserter, err := rwt.RewriteInserter(ctx, oldPkSchema, newPkSchema, nil, i.a.column, nil)
	if err != nil {
		return false, err
	}

	partitions, err := rwt.Partitions(ctx)
	if err != nil {
		return false, err
	}

	rowIter := sql.NewTableRowIter(ctx, rwt, partitions)

	var val uint64
	autoIncColIdx := -1
	if newSch.HasAutoIncrement() && !i.a.targetSch.HasAutoIncrement() {
		t, ok := rwt.(sql.AutoIncrementTable)
		if !ok {
			return false, ErrAutoIncrementNotSupported.New()
		}

		autoIncColIdx = newSch.IndexOf(i.a.column.Name, i.a.column.Source)
		val, err = t.GetNextAutoIncrementValue(ctx, 0)
		if err != nil {
			return false, err
		}
	}

	for {
		r, err := rowIter.Next(ctx)
		if err == io.EOF {
			break
		} else if err != nil {
			return false, err
		}

		newRow, err := ProjectRow(ctx, projections, r)
		if err != nil {
			return false, err
		}

		if autoIncColIdx != -1 {
			v, err := i.a.column.Type.Convert(val)
			if err != nil {
				return false, err
			}
			newRow[autoIncColIdx] = v
			val++
		}

		err = inserter.Insert(ctx, newRow)
		if err != nil {
			return false, err
		}
	}

	// TODO: move this into iter.close, probably
	err = inserter.Close(ctx)
	if err != nil {
		return false, err
	}

	return true, nil
}

// addColumnToSchema returns a new schema and a set of projection expressions that when applied to rows from the old
// schema will result in rows in the new schema.
func addColumnToSchema(schema sql.Schema, column *sql.Column, order *sql.ColumnOrder) (sql.Schema, []sql.Expression, error) {
	idx := -1
	if order != nil && len(order.AfterColumn) > 0 {
		idx = schema.IndexOf(order.AfterColumn, column.Source)
		if idx == -1 {
			// Should be checked in the analyzer already
			return nil, nil, sql.ErrTableColumnNotFound.New(column.Source, order.AfterColumn)
		}
		idx++
	} else if order != nil && order.First {
		idx = 0
	}

	// Now build the new schema, keeping track of:
	// 1) the new result schema
	// 2) A set of projections to translate rows in the old schema to rows in the new schema
	newSch := make(sql.Schema, 0, len(schema)+1)
	projections := make([]sql.Expression, len(schema)+1)

	if idx >= 0 {
		newSch = append(newSch, schema[:idx]...)
		newSch = append(newSch, column)
		newSch = append(newSch, schema[idx:]...)

		for i := range schema[:idx] {
			projections[i] = expression.NewGetField(i, schema[i].Type, schema[i].Name, schema[i].Nullable)
		}
		projections[idx] = colDefaultExpression{column}
		for i := range schema[idx:] {
			schIdx := i + idx
			projections[schIdx+1] = expression.NewGetField(schIdx, schema[schIdx].Type, schema[schIdx].Name, schema[schIdx].Nullable)
		}
	} else { // new column at end
		newSch = append(newSch, schema...)
		newSch = append(newSch, column)
		for i := range schema {
			projections[i] = expression.NewGetField(i, schema[i].Type, schema[i].Name, schema[i].Nullable)
		}
		projections[len(schema)] = colDefaultExpression{column}
	}

	// Alter the new default if it refers to other columns. The column indexes computed during analysis refer to the
	// column indexes in the new result schema, which is not what we want here: we want the positions in the old
	// (current) schema, since that is what we'll be evaluating when we rewrite the table.
	for i := range projections {
		switch p := projections[i].(type) {
		case colDefaultExpression:
			if p.column.Default != nil {
				newExpr, _, err := transform.Expr(p.column.Default.Expression, func(s sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
					switch s := s.(type) {
					case *expression.GetField:
						idx := schema.IndexOf(s.Name(), schema[0].Source)
						if idx < 0 {
							return nil, transform.SameTree, sql.ErrTableColumnNotFound.New(schema[0].Source, s.Name())
						}
						return s.WithIndex(idx), transform.NewTree, nil
					default:
						return s, transform.SameTree, nil
					}
					return s, transform.SameTree, nil
				})
				if err != nil {
					return nil, nil, err
				}
				p.column.Default.Expression = newExpr
				projections[i] = p
			}
			break
		}
	}

	return newSch, projections, nil
}

// colDefault expression evaluates the column default for a row being inserted, correctly handling zero values and
// nulls
type colDefaultExpression struct {
	column *sql.Column
}

var _ sql.Expression = colDefaultExpression{}
var _ sql.CollationCoercible = colDefaultExpression{}

func (c colDefaultExpression) Resolved() bool   { return true }
func (c colDefaultExpression) String() string   { return "" }
func (c colDefaultExpression) Type() sql.Type   { return c.column.Type }
func (c colDefaultExpression) IsNullable() bool { return c.column.Default == nil }
func (c colDefaultExpression) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	if c.column != nil && c.column.Default != nil {
		return c.column.Default.CollationCoercibility(ctx)
	}
	return sql.Collation_binary, 6
}

func (c colDefaultExpression) Children() []sql.Expression {
	panic("colDefaultExpression is only meant for immediate evaluation and should never be modified")
}

func (c colDefaultExpression) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	panic("colDefaultExpression is only meant for immediate evaluation and should never be modified")
}

func (c colDefaultExpression) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	columnDefaultExpr := c.column.Default

	if columnDefaultExpr == nil && !c.column.Nullable {
		val := c.column.Type.Zero()
		return c.column.Type.Convert(val)
	} else if columnDefaultExpr != nil {
		val, err := columnDefaultExpr.Eval(ctx, row)
		if err != nil {
			return nil, err
		}
		return c.column.Type.Convert(val)
	}

	return nil, nil
}

var _ sql.RowIter = &addColumnIter{}

type DropColumn struct {
	ddlNode
	Table        sql.Node
	Column       string
	Checks       sql.CheckConstraints
	targetSchema sql.Schema
}

var _ sql.Node = (*DropColumn)(nil)
var _ sql.Databaser = (*DropColumn)(nil)
var _ sql.SchemaTarget = (*DropColumn)(nil)
var _ sql.CollationCoercible = (*DropColumn)(nil)

func NewDropColumn(database sql.Database, table *UnresolvedTable, column string) *DropColumn {
	return &DropColumn{
		ddlNode: ddlNode{db: database},
		Table:   table,
		Column:  column,
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

type dropColumnIter struct {
	d         *DropColumn
	alterable sql.AlterableTable
	runOnce   bool
}

func (i *dropColumnIter) Next(ctx *sql.Context) (sql.Row, error) {
	if i.runOnce {
		return nil, io.EOF
	}
	i.runOnce = true

	// drop constraints that reference the dropped column
	cat, ok := i.alterable.(sql.CheckAlterableTable)
	if ok {
		// note: validations done earlier ensure safety of dropping any constraint referencing the column
		err := dropConstraints(ctx, cat, i.d.Checks, i.d.Column)
		if err != nil {
			return nil, err
		}
	}

	rwt, ok := i.alterable.(sql.RewritableTable)
	if ok {
		rewritten, err := i.rewriteTable(ctx, rwt)
		if err != nil {
			return nil, err
		}
		if rewritten {
			return sql.NewRow(types.NewOkResult(0)), nil
		}
	}

	err := i.alterable.DropColumn(ctx, i.d.Column)
	if err != nil {
		return nil, err
	}

	return sql.NewRow(types.NewOkResult(0)), nil
}

// rewriteTable rewrites the table given if required or requested, and returns the whether it was rewritten
func (i *dropColumnIter) rewriteTable(ctx *sql.Context, rwt sql.RewritableTable) (bool, error) {
	newSch, projections, err := dropColumnFromSchema(i.d.targetSchema, i.d.Column, i.alterable.Name())
	if err != nil {
		return false, err
	}

	oldPkSchema, newPkSchema := sql.SchemaToPrimaryKeySchema(rwt, rwt.Schema()), sql.SchemaToPrimaryKeySchema(rwt, newSch)
	droppedColIdx := oldPkSchema.IndexOf(i.d.Column, i.alterable.Name())

	rewriteRequested := rwt.ShouldRewriteTable(ctx, oldPkSchema, newPkSchema, oldPkSchema.Schema[droppedColIdx], nil)
	if !rewriteRequested {
		return false, nil
	}

	inserter, err := rwt.RewriteInserter(ctx, oldPkSchema, newPkSchema, oldPkSchema.Schema[droppedColIdx], nil, nil)
	if err != nil {
		return false, err
	}

	partitions, err := rwt.Partitions(ctx)
	if err != nil {
		return false, err
	}

	rowIter := sql.NewTableRowIter(ctx, rwt, partitions)

	for {
		r, err := rowIter.Next(ctx)
		if err == io.EOF {
			break
		} else if err != nil {
			return false, err
		}

		newRow, err := ProjectRow(ctx, projections, r)
		if err != nil {
			return false, err
		}

		err = inserter.Insert(ctx, newRow)
		if err != nil {
			return false, err
		}
	}

	// TODO: move this into iter.close, probably
	err = inserter.Close(ctx)
	if err != nil {
		return false, err
	}

	return true, nil
}

func dropColumnFromSchema(schema sql.Schema, column string, tableName string) (sql.Schema, []sql.Expression, error) {
	idx := schema.IndexOf(column, tableName)
	if idx < 0 {
		return nil, nil, sql.ErrTableColumnNotFound.New(tableName, column)
	}

	newSch := make(sql.Schema, len(schema)-1)
	projections := make([]sql.Expression, len(schema)-1)

	i := 0
	for j := range schema[:idx] {
		newSch[i] = schema[j]
		projections[i] = expression.NewGetField(j, schema[j].Type, schema[j].Name, schema[j].Nullable)
		i++
	}

	for j := range schema[idx+1:] {
		schIdx := j + i + 1
		newSch[j+i] = schema[schIdx]
		projections[j+i] = expression.NewGetField(schIdx, schema[schIdx].Type, schema[schIdx].Name, schema[schIdx].Nullable)
	}

	return newSch, projections, nil
}

// dropConstraints drop constraints that reference the column to be dropped.
func dropConstraints(ctx *sql.Context, cat sql.CheckAlterableTable, checks sql.CheckConstraints, column string) error {
	var err error
	for _, check := range checks {
		_ = transform.InspectExpr(check.Expr, func(e sql.Expression) bool {
			if unresolvedColumn, ok := e.(*expression.UnresolvedColumn); ok {
				if column == unresolvedColumn.Name() {
					err = cat.DropCheck(ctx, check.Name)
					return true
				}
			}
			return false
		})

		if err != nil {
			return err
		}
	}
	return nil
}

func (i *dropColumnIter) Close(context *sql.Context) error {
	return nil
}

func (d *DropColumn) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	tbl, err := getTableFromDatabase(ctx, d.Database(), d.Table)
	if err != nil {
		return nil, err
	}

	err = d.validate(ctx, tbl)
	if err != nil {
		return nil, err
	}

	alterable, ok := tbl.(sql.AlterableTable)
	if !ok {
		return nil, sql.ErrAlterTableNotSupported.New(tbl.Name())
	}

	return &dropColumnIter{
		d:         d,
		alterable: alterable,
	}, nil
}

// validate returns an error if this drop column operation is invalid (because it would invalidate a column default
// or other constraint).
// TODO: move this check to analyzer
func (d *DropColumn) validate(ctx *sql.Context, tbl sql.Table) error {
	colIdx := d.targetSchema.IndexOfColName(d.Column)
	if colIdx < 0 {
		return sql.ErrTableColumnNotFound.New(tbl.Name(), d.Column)
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
			return err
		}
	}

	if fkTable, ok := tbl.(sql.ForeignKeyTable); ok {
		lowercaseColumn := strings.ToLower(d.Column)
		fks, err := fkTable.GetDeclaredForeignKeys(ctx)
		if err != nil {
			return err
		}
		for _, fk := range fks {
			for _, fkCol := range fk.Columns {
				if lowercaseColumn == strings.ToLower(fkCol) {
					return sql.ErrForeignKeyDropColumn.New(d.Column, fk.Name)
				}
			}
		}
		parentFks, err := fkTable.GetReferencedForeignKeys(ctx)
		if err != nil {
			return err
		}
		for _, parentFk := range parentFks {
			for _, parentFkCol := range parentFk.Columns {
				if lowercaseColumn == strings.ToLower(parentFkCol) {
					return sql.ErrForeignKeyDropColumn.New(d.Column, parentFk.Name)
				}
			}
		}
	}

	return nil
}

func (d *DropColumn) Schema() sql.Schema {
	return types.OkResultSchema
}

func (d *DropColumn) Resolved() bool {
	if !d.Table.Resolved() && !d.ddlNode.Resolved() {
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
	return []sql.Node{d.Table}
}

func (d DropColumn) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(d, len(children), 1)
	}
	d.Table = children[0]
	return &d, nil
}

// CheckPrivileges implements the interface sql.Node.
func (d *DropColumn) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return opChecker.UserHasPrivileges(ctx,
		sql.NewPrivilegedOperation(d.Database().Name(), getTableName(d.Table), "", sql.PrivilegeType_Alter))
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*DropColumn) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 7
}

func (d DropColumn) WithTargetSchema(schema sql.Schema) (sql.Node, error) {
	d.targetSchema = schema
	return &d, nil
}

func (d *DropColumn) TargetSchema() sql.Schema {
	return d.targetSchema
}

func (d *DropColumn) Expressions() []sql.Expression {
	return transform.WrappedColumnDefaults(d.targetSchema)
}

func (d DropColumn) WithExpressions(exprs ...sql.Expression) (sql.Node, error) {
	if len(exprs) != len(d.targetSchema) {
		return nil, sql.ErrInvalidChildrenNumber.New(d, len(exprs), len(d.targetSchema))
	}

	d.targetSchema = transform.SchemaWithDefaults(d.targetSchema, exprs)
	return &d, nil
}

type RenameColumn struct {
	ddlNode
	Table         sql.Node
	ColumnName    string
	NewColumnName string
	Checks        sql.CheckConstraints
	targetSchema  sql.Schema
}

var _ sql.Node = (*RenameColumn)(nil)
var _ sql.Databaser = (*RenameColumn)(nil)
var _ sql.SchemaTarget = (*RenameColumn)(nil)
var _ sql.CollationCoercible = (*RenameColumn)(nil)

func NewRenameColumn(database sql.Database, table *UnresolvedTable, columnName string, newColumnName string) *RenameColumn {
	return &RenameColumn{
		ddlNode:       ddlNode{db: database},
		Table:         table,
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
	if !r.Table.Resolved() && r.ddlNode.Resolved() {
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
	return types.OkResultSchema
}

func (r *RenameColumn) Expressions() []sql.Expression {
	return transform.WrappedColumnDefaults(r.targetSchema)
}

func (r RenameColumn) WithExpressions(exprs ...sql.Expression) (sql.Node, error) {
	if len(exprs) != len(r.targetSchema) {
		return nil, sql.ErrInvalidChildrenNumber.New(r, len(exprs), len(r.targetSchema))
	}

	r.targetSchema = transform.SchemaWithDefaults(r.targetSchema, exprs)
	return &r, nil
}

func (r *RenameColumn) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	tbl, err := getTableFromDatabase(ctx, r.Database(), r.Table)
	if err != nil {
		return nil, err
	}

	alterable, ok := tbl.(sql.AlterableTable)
	if !ok {
		return nil, sql.ErrAlterTableNotSupported.New(tbl.Name())
	}

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

	// Update the foreign key columns as well
	if fkTable, ok := alterable.(sql.ForeignKeyTable); ok {
		parentFks, err := fkTable.GetReferencedForeignKeys(ctx)
		if err != nil {
			return nil, err
		}
		fks, err := fkTable.GetDeclaredForeignKeys(ctx)
		if err != nil {
			return nil, err
		}
		if len(parentFks) > 0 || len(fks) > 0 {
			err = handleFkColumnRename(ctx, fkTable, r.db, r.ColumnName, r.NewColumnName)
			if err != nil {
				return nil, err
			}
		}
	}

	return sql.RowsToRowIter(sql.NewRow(types.NewOkResult(0))), alterable.ModifyColumn(ctx, r.ColumnName, col, nil)
}

func (r *RenameColumn) Children() []sql.Node {
	return []sql.Node{r.Table}
}

func (r RenameColumn) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(r, len(children), 1)
	}
	r.Table = children[0]
	return &r, nil
}

// CheckPrivileges implements the interface sql.Node.
func (r *RenameColumn) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return opChecker.UserHasPrivileges(ctx,
		sql.NewPrivilegedOperation(r.db.Name(), getTableName(r.Table), "", sql.PrivilegeType_Alter))
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*RenameColumn) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 7
}

type ModifyColumn struct {
	ddlNode
	Table        sql.Node
	columnName   string
	column       *sql.Column
	order        *sql.ColumnOrder
	targetSchema sql.Schema
}

var _ sql.Node = (*ModifyColumn)(nil)
var _ sql.Expressioner = (*ModifyColumn)(nil)
var _ sql.Databaser = (*ModifyColumn)(nil)
var _ sql.SchemaTarget = (*ModifyColumn)(nil)
var _ sql.CollationCoercible = (*ModifyColumn)(nil)

func NewModifyColumn(database sql.Database, table *UnresolvedTable, columnName string, column *sql.Column, order *sql.ColumnOrder) *ModifyColumn {
	column.Source = table.name
	return &ModifyColumn{
		ddlNode:    ddlNode{db: database},
		Table:      table,
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
	return types.OkResultSchema
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

type modifyColumnIter struct {
	m         *ModifyColumn
	alterable sql.AlterableTable
	runOnce   bool
}

func (m *ModifyColumn) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	tbl, err := getTableFromDatabase(ctx, m.Database(), m.Table)
	if err != nil {
		return nil, err
	}

	alterable, ok := tbl.(sql.AlterableTable)
	if !ok {
		return nil, sql.ErrAlterTableNotSupported.New(tbl.Name())
	}

	if err := m.validateDefaultPosition(m.targetSchema); err != nil {
		return nil, err
	}
	// MySQL assigns the column's type (which contains the collation) at column creation/modification. If a column has
	// an invalid collation, then one has not been assigned at this point, so we assign it the table's collation. This
	// does not create a reference to the table's collation, which may change at any point, and therefore will have no
	// relation to this column after assignment.
	if collatedType, ok := m.column.Type.(sql.TypeWithCollation); ok && collatedType.Collation() == sql.Collation_Unspecified {
		m.column.Type, err = collatedType.WithNewCollation(alterable.Collation())
		if err != nil {
			return nil, err
		}
	}
	for _, col := range m.targetSchema {
		if collatedType, ok := col.Type.(sql.TypeWithCollation); ok && collatedType.Collation() == sql.Collation_Unspecified {
			col.Type, err = collatedType.WithNewCollation(alterable.Collation())
			if err != nil {
				return nil, err
			}
		}
	}

	return &modifyColumnIter{
		m:         m,
		alterable: alterable,
	}, nil
}

func (i *modifyColumnIter) Next(ctx *sql.Context) (sql.Row, error) {
	if i.runOnce {
		return nil, io.EOF
	}
	i.runOnce = true

	idx := i.m.targetSchema.IndexOf(i.m.columnName, i.alterable.Name())
	if idx < 0 {
		return nil, sql.ErrTableColumnNotFound.New(i.alterable.Name(), i.m.columnName)
	}

	if i.m.order != nil && !i.m.order.First {
		idx = i.m.targetSchema.IndexOf(i.m.order.AfterColumn, i.alterable.Name())
		if idx < 0 {
			return nil, sql.ErrTableColumnNotFound.New(i.alterable.Name(), i.m.order.AfterColumn)
		}
	}

	lowerColName := strings.ToLower(i.m.columnName)

	// Update the foreign key columns as well
	if fkTable, ok := i.alterable.(sql.ForeignKeyTable); ok {
		// We only care if the column is used in a foreign key
		usedInFk := false
		fks, err := fkTable.GetDeclaredForeignKeys(ctx)
		if err != nil {
			return nil, err
		}
		parentFks, err := fkTable.GetReferencedForeignKeys(ctx)
		if err != nil {
			return nil, err
		}
	OuterChildFk:
		for _, foreignKey := range fks {
			for _, colName := range foreignKey.Columns {
				if strings.ToLower(colName) == lowerColName {
					usedInFk = true
					break OuterChildFk
				}
			}
		}
		if !usedInFk {
		OuterParentFk:
			for _, foreignKey := range parentFks {
				for _, colName := range foreignKey.ParentColumns {
					if strings.ToLower(colName) == lowerColName {
						usedInFk = true
						break OuterParentFk
					}
				}
			}
		}

		tblSch := i.m.targetSchema
		if usedInFk {
			if !i.m.targetSchema[idx].Type.Equals(i.m.column.Type) {
				// There seems to be a special case where you can lengthen a CHAR/VARCHAR/BINARY/VARBINARY.
				// Have not tested every type nor combination, but this seems specific to those 4 types.
				if tblSch[idx].Type.Type() == i.m.column.Type.Type() {
					switch i.m.column.Type.Type() {
					case sqltypes.Char, sqltypes.VarChar, sqltypes.Binary, sqltypes.VarBinary:
						oldType := tblSch[idx].Type.(sql.StringType)
						newType := i.m.column.Type.(sql.StringType)
						if oldType.Collation() != newType.Collation() || oldType.MaxCharacterLength() > newType.MaxCharacterLength() {
							return nil, sql.ErrForeignKeyTypeChange.New(i.m.columnName)
						}
					default:
						return nil, sql.ErrForeignKeyTypeChange.New(i.m.columnName)
					}
				} else {
					return nil, sql.ErrForeignKeyTypeChange.New(i.m.columnName)
				}
			}
			if !i.m.column.Nullable {
				lowerColName := strings.ToLower(i.m.columnName)
				for _, fk := range fks {
					if fk.OnUpdate == sql.ForeignKeyReferentialAction_SetNull || fk.OnDelete == sql.ForeignKeyReferentialAction_SetNull {
						for _, col := range fk.Columns {
							if lowerColName == strings.ToLower(col) {
								return nil, sql.ErrForeignKeyTypeChangeSetNull.New(i.m.columnName, fk.Name)
							}
						}
					}
				}
			}
			err = handleFkColumnRename(ctx, fkTable, i.m.db, i.m.columnName, i.m.column.Name)
			if err != nil {
				return nil, err
			}
		}
	}

	// TODO: replace with different node in analyzer
	rwt, ok := i.alterable.(sql.RewritableTable)
	if ok {
		rewritten, err := i.rewriteTable(ctx, rwt)
		if err != nil {
			return nil, err
		}
		if rewritten {
			return sql.NewRow(types.NewOkResult(0)), nil
		}
	}

	// TODO: fix me
	if err := updateDefaultsOnColumnRename(ctx, i.alterable, i.m.targetSchema, i.m.columnName, i.m.column.Name); err != nil {
		return nil, err
	}

	err := i.alterable.ModifyColumn(ctx, i.m.columnName, i.m.column, i.m.order)
	if err != nil {
		return nil, err
	}

	return sql.NewRow(types.NewOkResult(0)), nil
}

func (i *modifyColumnIter) Close(context *sql.Context) error {
	return nil
}

// rewriteTable rewrites the table given if required or requested, and returns the whether it was rewritten
func (i *modifyColumnIter) rewriteTable(ctx *sql.Context, rwt sql.RewritableTable) (bool, error) {
	oldColIdx := i.m.targetSchema.IndexOfColName(i.m.columnName)
	if oldColIdx < 0 {
		// Should be impossible, checked in analyzer
		return false, sql.ErrTableColumnNotFound.New(rwt.Name(), i.m.columnName)
	}

	newSch, projections, err := modifyColumnInSchema(i.m.targetSchema, i.m.columnName, i.m.column, i.m.order)
	if err != nil {
		return false, err
	}

	// Wrap any auto increment columns in auto increment expressions. This mirrors what happens to row sources for normal
	// INSERT statements during analysis.
	for i, col := range newSch {
		if col.AutoIncrement {
			projections[i], err = expression.NewAutoIncrementForColumn(ctx, rwt, col, projections[i])
			if err != nil {
				return false, err
			}
		}
	}

	oldPkSchema, newPkSchema := sql.SchemaToPrimaryKeySchema(rwt, rwt.Schema()), sql.SchemaToPrimaryKeySchema(rwt, newSch)

	rewriteRequired := false
	if i.m.targetSchema[oldColIdx].Nullable && !i.m.column.Nullable {
		rewriteRequired = true
	}

	// TODO: codify rewrite requirements
	rewriteRequested := rwt.ShouldRewriteTable(ctx, oldPkSchema, newPkSchema, i.m.targetSchema[oldColIdx], i.m.column)
	if !rewriteRequired && !rewriteRequested {
		return false, nil
	}

	inserter, err := rwt.RewriteInserter(ctx, oldPkSchema, newPkSchema, i.m.targetSchema[oldColIdx], i.m.column, nil)
	if err != nil {
		return false, err
	}

	partitions, err := rwt.Partitions(ctx)
	if err != nil {
		return false, err
	}

	rowIter := sql.NewTableRowIter(ctx, rwt, partitions)

	for {
		r, err := rowIter.Next(ctx)
		if err == io.EOF {
			break
		} else if err != nil {
			return false, err
		}

		newRow, err := projectRowWithTypes(ctx, newSch, projections, r)
		if err != nil {
			return false, err
		}

		err = i.validateNullability(ctx, newSch, newRow)
		if err != nil {
			return false, err
		}

		err = inserter.Insert(ctx, newRow)
		if err != nil {
			return false, err
		}
	}

	// TODO: move this into iter.close, probably
	err = inserter.Close(ctx)
	if err != nil {
		return false, err
	}

	return true, nil
}

// TODO: this shares logic with insert
func (i *modifyColumnIter) validateNullability(ctx *sql.Context, dstSchema sql.Schema, row sql.Row) error {
	for count, col := range dstSchema {
		if !col.Nullable && row[count] == nil {
			return sql.ErrInsertIntoNonNullableProvidedNull.New(col.Name)
		}
	}
	return nil
}

// projectRowWithTypes projects the row given with the projections given and additionally converts them to the
// corresponding types found in the schema given, using the standard type conversion logic.
func projectRowWithTypes(ctx *sql.Context, sch sql.Schema, projections []sql.Expression, r sql.Row) (sql.Row, error) {
	newRow, err := ProjectRow(ctx, projections, r)
	if err != nil {
		return nil, err
	}

	for i := range newRow {
		newRow[i], err = sch[i].Type.Convert(newRow[i])
		if err != nil {
			if sql.ErrNotMatchingSRID.Is(err) {
				err = sql.ErrNotMatchingSRIDWithColName.New(sch[i].Name, err)
			}
			return nil, err
		}
	}

	return newRow, nil
}

// modifyColumnInSchema modifies the given column in given schema and returns the new schema, along with a set of
// projections to adapt the old schema to the new one.
func modifyColumnInSchema(schema sql.Schema, name string, column *sql.Column, order *sql.ColumnOrder) (sql.Schema, []sql.Expression, error) {
	schema = schema.Copy()
	currIdx := schema.IndexOf(name, column.Source)
	if currIdx < 0 {
		// Should be checked in the analyzer already
		return nil, nil, sql.ErrTableColumnNotFound.New(column.Source, name)
	}

	// Primary key-ness isn't included in the column description as part of the ALTER statement, preserve it
	if schema[currIdx].PrimaryKey {
		column.PrimaryKey = true
	}

	newIdx := currIdx
	if order != nil && len(order.AfterColumn) > 0 {
		newIdx = schema.IndexOf(order.AfterColumn, column.Source)
		if newIdx == -1 {
			// Should be checked in the analyzer already
			return nil, nil, sql.ErrTableColumnNotFound.New(column.Source, order.AfterColumn)
		}
		// if we're moving left in the schema, shift everything over one
		if newIdx < currIdx {
			newIdx++
		}
	} else if order != nil && order.First {
		newIdx = 0
	}

	// establish a map from old column index to new column index
	oldToNewIdxMapping := make(map[int]int)
	var i, j int
	for j < len(schema) || i < len(schema) {
		if i == currIdx {
			oldToNewIdxMapping[i] = newIdx
			i++
		} else if j == newIdx {
			j++
		} else {
			oldToNewIdxMapping[i] = j
			i, j = i+1, j+1
		}
	}

	// Now build the new schema, keeping track of:
	// 1) The new result schema
	// 2) A set of projections to translate rows in the old schema to rows in the new schema
	newSch := make(sql.Schema, len(schema))
	projections := make([]sql.Expression, len(schema))

	for i := range schema {
		j := oldToNewIdxMapping[i]
		oldCol := schema[i]
		c := oldCol
		if j == newIdx {
			c = column
		}
		newSch[j] = c
		projections[j] = expression.NewGetField(i, oldCol.Type, oldCol.Name, oldCol.Nullable)
	}

	// If a column was renamed or moved, we need to update any column defaults that refer to it
	for i := range newSch {
		newCol := newSch[oldToNewIdxMapping[i]]

		if newCol.Default != nil {
			newDefault, _, err := transform.Expr(newCol.Default.Expression, func(e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
				gf, ok := e.(*expression.GetField)
				if !ok {
					return e, transform.SameTree, nil
				}

				colName := gf.Name()
				// handle column renames
				if strings.ToLower(colName) == strings.ToLower(name) {
					colName = column.Name
				}

				newSchemaIdx := newSch.IndexOfColName(colName)
				return expression.NewGetFieldWithTable(newSchemaIdx, gf.Type(), gf.Table(), colName, gf.IsNullable()), transform.NewTree, nil
			})
			if err != nil {
				return nil, nil, err
			}

			newDefault, err = newCol.Default.WithChildren(newDefault)
			if err != nil {
				return nil, nil, err
			}

			newCol.Default = newDefault.(*sql.ColumnDefaultValue)
		}
	}

	// TODO: do we need col defaults here? probably when changing a column to be non-null?
	return newSch, projections, nil
}

func (m *ModifyColumn) Children() []sql.Node {
	return []sql.Node{m.Table}
}

func (m ModifyColumn) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(m, len(children), 1)
	}
	m.Table = children[0]
	return &m, nil
}

// CheckPrivileges implements the interface sql.Node.
func (m *ModifyColumn) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return opChecker.UserHasPrivileges(ctx,
		sql.NewPrivilegedOperation(m.Database().Name(), getTableName(m.Table), "", sql.PrivilegeType_Alter))
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*ModifyColumn) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 7
}

func (m *ModifyColumn) Expressions() []sql.Expression {
	return append(transform.WrappedColumnDefaults(m.targetSchema), expression.WrapExpressions(m.column.Default)...)
}

func (m ModifyColumn) WithExpressions(exprs ...sql.Expression) (sql.Node, error) {
	if len(exprs) != 1+len(m.targetSchema) {
		return nil, sql.ErrInvalidChildrenNumber.New(m, len(exprs), 1+len(m.targetSchema))
	}

	m.targetSchema = transform.SchemaWithDefaults(m.targetSchema, exprs[:len(m.targetSchema)])

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
	if !(m.Table.Resolved() && m.column.Default.Resolved() && m.ddlNode.Resolved()) {
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

type AlterTableCollation struct {
	ddlNode
	Table     sql.Node
	Collation sql.CollationID
}

var _ sql.Node = (*AlterTableCollation)(nil)
var _ sql.Databaser = (*AlterTableCollation)(nil)

// NewAlterTableCollation returns a new *AlterTableCollation
func NewAlterTableCollation(database sql.Database, table *UnresolvedTable, collation sql.CollationID) *AlterTableCollation {
	return &AlterTableCollation{
		ddlNode:   ddlNode{db: database},
		Table:     table,
		Collation: collation,
	}
}

// WithDatabase implements the interface sql.Databaser.
func (atc *AlterTableCollation) WithDatabase(db sql.Database) (sql.Node, error) {
	natc := *atc
	natc.db = db
	return &natc, nil
}

// String implements the interface sql.Node.
func (atc *AlterTableCollation) String() string {
	return fmt.Sprintf("alter table %s collate %s", atc.Table.String(), atc.Collation.Name())
}

// DebugString implements the interface sql.Node.
func (atc *AlterTableCollation) DebugString() string {
	return atc.String()
}

// Resolved implements the interface sql.Node.
func (atc *AlterTableCollation) Resolved() bool {
	return atc.Table.Resolved() && atc.ddlNode.Resolved()
}

// Schema implements the interface sql.Node.
func (atc *AlterTableCollation) Schema() sql.Schema {
	return types.OkResultSchema
}

// RowIter implements the interface sql.Node.
func (atc *AlterTableCollation) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	tbl, err := getTableFromDatabase(ctx, atc.Database(), atc.Table)
	if err != nil {
		return nil, err
	}

	alterable, ok := tbl.(sql.CollationAlterableTable)
	if !ok {
		return nil, sql.ErrAlterTableCollationNotSupported.New(tbl.Name())
	}

	return sql.RowsToRowIter(sql.NewRow(types.NewOkResult(0))), alterable.ModifyDefaultCollation(ctx, atc.Collation)
}

// Children implements the interface sql.Node.
func (atc *AlterTableCollation) Children() []sql.Node {
	return []sql.Node{atc.Table}
}

// WithChildren implements the interface sql.Node.
func (atc *AlterTableCollation) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(atc, len(children), 1)
	}
	natc := *atc
	natc.Table = children[0]
	return &natc, nil
}

// CheckPrivileges implements the interface sql.Node.
func (atc *AlterTableCollation) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return opChecker.UserHasPrivileges(ctx,
		sql.NewPrivilegedOperation(atc.db.Name(), getTableName(atc.Table), "", sql.PrivilegeType_Alter))
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
		newCol.Default.Expression, _, err = transform.Expr(col.Default.Expression, func(e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
			if expr, ok := e.(*expression.GetField); ok {
				if strings.ToLower(expr.Name()) == oldName {
					colsToModify[&newCol] = struct{}{}
					return expr.WithName(newName), transform.NewTree, nil
				}
			}
			return e, transform.SameTree, nil
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

func handleFkColumnRename(ctx *sql.Context, fkTable sql.ForeignKeyTable, db sql.Database, oldName string, newName string) error {
	lowerOldName := strings.ToLower(oldName)
	if lowerOldName == strings.ToLower(newName) {
		return nil
	}

	parentFks, err := fkTable.GetReferencedForeignKeys(ctx)
	if err != nil {
		return err
	}
	if len(parentFks) > 0 {
		dbName := strings.ToLower(db.Name())
		for _, parentFk := range parentFks {
			//TODO: add support for multi db foreign keys
			if dbName != strings.ToLower(parentFk.ParentDatabase) {
				return fmt.Errorf("renaming columns involved in foreign keys referencing a different database" +
					" is not yet supported")
			}
			shouldUpdate := false
			for i, col := range parentFk.ParentColumns {
				if strings.ToLower(col) == lowerOldName {
					parentFk.ParentColumns[i] = newName
					shouldUpdate = true
				}
			}
			if shouldUpdate {
				childTable, ok, err := db.GetTableInsensitive(ctx, parentFk.Table)
				if err != nil {
					return err
				}
				if !ok {
					return sql.ErrTableNotFound.New(parentFk.Table)
				}
				err = childTable.(sql.ForeignKeyTable).UpdateForeignKey(ctx, parentFk.Name, parentFk)
				if err != nil {
					return err
				}
			}
		}
	}

	fks, err := fkTable.GetDeclaredForeignKeys(ctx)
	if err != nil {
		return err
	}
	for _, fk := range fks {
		shouldUpdate := false
		for i, col := range fk.Columns {
			if strings.ToLower(col) == lowerOldName {
				fk.Columns[i] = newName
				shouldUpdate = true
			}
		}
		if shouldUpdate {
			err = fkTable.UpdateForeignKey(ctx, fk.Name, fk)
			if err != nil {
				return err
			}
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
