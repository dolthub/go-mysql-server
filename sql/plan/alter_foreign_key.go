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
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
)

func getForeignKeyTable(t sql.Table) (sql.ForeignKeyTable, error) {
	switch t := t.(type) {
	case sql.ForeignKeyTable:
		return t, nil
	case sql.TableWrapper:
		return getForeignKeyTable(t.Underlying())
	default:
		return nil, sql.ErrNoForeignKeySupport.New(t.Name())
	}
}

type CreateForeignKey struct {
	// In the cases where we have multiple ALTER statements, we need to resolve the table at execution time rather than
	// during analysis. Otherwise, you could add a column in the preceding alter and we may have analyzed to a table
	// that did not yet have that column.
	dbProvider sql.DatabaseProvider
	FkDef      *sql.ForeignKeyConstraint
}

var _ sql.Node = (*CreateForeignKey)(nil)
var _ sql.MultiDatabaser = (*CreateForeignKey)(nil)

func NewAlterAddForeignKey(fkDef *sql.ForeignKeyConstraint) *CreateForeignKey {
	return &CreateForeignKey{
		dbProvider: nil,
		FkDef:      fkDef,
	}
}

// Resolved implements the interface sql.Node.
func (p *CreateForeignKey) Resolved() bool {
	return p.dbProvider != nil
}

// Children implements the interface sql.Node.
func (p *CreateForeignKey) Children() []sql.Node {
	return nil
}

// WithChildren implements the interface sql.Node.
func (p *CreateForeignKey) WithChildren(children ...sql.Node) (sql.Node, error) {
	return NillaryWithChildren(p, children...)
}

// CheckPrivileges implements the interface sql.Node.
func (p *CreateForeignKey) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return opChecker.UserHasPrivileges(ctx,
		sql.NewPrivilegedOperation(p.FkDef.ReferencedDatabase, p.FkDef.ReferencedTable, "", sql.PrivilegeType_References))
}

// Schema implements the interface sql.Node.
func (p *CreateForeignKey) Schema() sql.Schema {
	return nil
}

// DatabaseProvider implements the interface sql.MultiDatabaser.
func (p *CreateForeignKey) DatabaseProvider() sql.DatabaseProvider {
	return p.dbProvider
}

// WithDatabaseProvider implements the interface sql.MultiDatabaser.
func (p *CreateForeignKey) WithDatabaseProvider(provider sql.DatabaseProvider) (sql.Node, error) {
	np := *p
	np.dbProvider = provider
	return &np, nil
}

// RowIter implements the interface sql.Node.
func (p *CreateForeignKey) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	db, err := p.dbProvider.Database(ctx, p.FkDef.Database)
	if err != nil {
		return nil, err
	}
	tbl, ok, err := db.GetTableInsensitive(ctx, p.FkDef.Table)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, sql.ErrTableNotFound.New(p.FkDef.Table)
	}

	refDb, err := p.dbProvider.Database(ctx, p.FkDef.ReferencedDatabase)
	if err != nil {
		return nil, err
	}
	refTbl, ok, err := refDb.GetTableInsensitive(ctx, p.FkDef.ReferencedTable)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, sql.ErrTableNotFound.New(p.FkDef.ReferencedTable)
	}

	fkTbl, ok := tbl.(sql.ForeignKeyTable)
	if !ok {
		return nil, sql.ErrNoForeignKeySupport.New(p.FkDef.Table)
	}
	refFkTbl, ok := refTbl.(sql.ForeignKeyTable)
	if !ok {
		return nil, sql.ErrNoForeignKeySupport.New(p.FkDef.ReferencedTable)
	}

	err = fkTbl.AddForeignKey(ctx, *p.FkDef)
	if err != nil {
		return nil, err
	}
	err = resolveForeignKey(ctx, fkTbl, refFkTbl, p.FkDef)
	if err != nil {
		return nil, err
	}

	return sql.RowsToRowIter(), nil
}

// String implements the interface sql.Node.
func (p *CreateForeignKey) String() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("AddForeignKey(%s)", p.FkDef.Name)
	_ = pr.WriteChildren(
		fmt.Sprintf("Table(%s.%s)", p.FkDef.Database, p.FkDef.Table),
		fmt.Sprintf("Columns(%s)", strings.Join(p.FkDef.Columns, ", ")),
		fmt.Sprintf("ReferencedTable(%s.%s)", p.FkDef.ReferencedDatabase, p.FkDef.ReferencedTable),
		fmt.Sprintf("ReferencedColumns(%s)", strings.Join(p.FkDef.ReferencedColumns, ", ")),
		fmt.Sprintf("OnUpdate(%s)", p.FkDef.OnUpdate),
		fmt.Sprintf("OnDelete(%s)", p.FkDef.OnDelete))
	return pr.String()
}

// resolveForeignKey verifies the foreign key definition and resolves the foreign key, creating indexes and validating
// data as necessary.
func resolveForeignKey(ctx *sql.Context, tbl sql.ForeignKeyTable, refTbl sql.ForeignKeyTable, fkDef *sql.ForeignKeyConstraint) error {
	if t, ok := tbl.(sql.TemporaryTable); ok && t.IsTemporary() {
		return sql.ErrTemporaryTablesForeignKeySupport.New()
	}

	if fkDef.IsResolved {
		return fmt.Errorf("cannot resolve foreign key `%s` as it has already been resolved", fkDef.Name)
	}
	if len(fkDef.Columns) == 0 {
		return sql.ErrForeignKeyMissingColumns.New()
	}
	if len(fkDef.Columns) != len(fkDef.ReferencedColumns) {
		return sql.ErrForeignKeyColumnCountMismatch.New()
	}

	// Make sure that all columns are valid, in the table, and there are no duplicates
	cols := make(map[string]*sql.Column)
	seenCols := make(map[string]bool)
	actualColNames := make(map[string]string)
	for _, col := range tbl.Schema() {
		lowerColName := strings.ToLower(col.Name)
		cols[lowerColName] = col
		seenCols[lowerColName] = false
		actualColNames[lowerColName] = col.Name
	}
	for i, fkCol := range fkDef.Columns {
		lowerFkCol := strings.ToLower(fkCol)
		if seen, ok := seenCols[lowerFkCol]; ok {
			if !seen {
				seenCols[lowerFkCol] = true
				fkDef.Columns[i] = actualColNames[lowerFkCol]
			} else {
				return sql.ErrAddForeignKeyDuplicateColumn.New(fkCol)
			}
			// Non-nullable columns may not have SET NULL as a reference option
			if !cols[lowerFkCol].Nullable && (fkDef.OnUpdate == sql.ForeignKeyReferenceOption_SetNull ||
				fkDef.OnDelete == sql.ForeignKeyReferenceOption_SetNull) {

			}
		} else {
			return sql.ErrTableColumnNotFound.New(tbl.Name(), fkCol)
		}
	}

	// Do the same for the referenced columns
	seenCols = make(map[string]bool)
	actualColNames = make(map[string]string)
	for _, col := range refTbl.Schema() {
		lowerColName := strings.ToLower(col.Name)
		seenCols[lowerColName] = false
		actualColNames[lowerColName] = col.Name
	}
	for i, fkRefCol := range fkDef.ReferencedColumns {
		lowerFkRefCol := strings.ToLower(fkRefCol)
		if seen, ok := seenCols[lowerFkRefCol]; ok {
			if !seen {
				seenCols[lowerFkRefCol] = true
				fkDef.ReferencedColumns[i] = actualColNames[lowerFkRefCol]
			} else {
				return sql.ErrAddForeignKeyDuplicateColumn.New(fkRefCol)
			}
		} else {
			return sql.ErrTableColumnNotFound.New(fkDef.ReferencedTable, fkRefCol)
		}
	}

	//TODO: resolve foreign keys
	return tbl.SetForeignKeyResolved(ctx, fkDef.Name)
}

type DropForeignKey struct {
	// In the cases where we have multiple ALTER statements, we need to resolve the table at execution time rather than
	// during analysis. Otherwise, you could add a foreign key in the preceding alter and we may have analyzed to a
	// table that did not yet have that foreign key.
	dbProvider sql.DatabaseProvider
	Database   string
	Table      string
	Name       string
}

var _ sql.Node = (*DropForeignKey)(nil)
var _ sql.MultiDatabaser = (*DropForeignKey)(nil)

func NewAlterDropForeignKey(db, table, name string) *DropForeignKey {
	return &DropForeignKey{
		dbProvider: nil,
		Database:   db,
		Table:      table,
		Name:       name,
	}
}

// RowIter implements the interface sql.Node.
func (p *DropForeignKey) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	db, err := p.dbProvider.Database(ctx, p.Database)
	if err != nil {
		return nil, err
	}
	tbl, ok, err := db.GetTableInsensitive(ctx, p.Table)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, sql.ErrTableNotFound.New(p.Table)
	}
	fkTbl, ok := tbl.(sql.ForeignKeyTable)
	if !ok {
		return nil, sql.ErrNoForeignKeySupport.New(p.Name)
	}
	err = fkTbl.DropForeignKey(ctx, p.Name)
	if err != nil {
		return nil, err
	}

	return sql.RowsToRowIter(), nil
}

// WithChildren implements the interface sql.Node.
func (p *DropForeignKey) WithChildren(children ...sql.Node) (sql.Node, error) {
	return NillaryWithChildren(p, children...)
}

// CheckPrivileges implements the interface sql.Node.
func (p *DropForeignKey) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return opChecker.UserHasPrivileges(ctx,
		sql.NewPrivilegedOperation(p.Database, p.Table, "", sql.PrivilegeType_Alter))
}

// Schema implements the interface sql.Node.
func (p *DropForeignKey) Schema() sql.Schema {
	return nil
}

// DatabaseProvider implements the interface sql.MultiDatabaser.
func (p *DropForeignKey) DatabaseProvider() sql.DatabaseProvider {
	return p.dbProvider
}

// WithDatabaseProvider implements the interface sql.MultiDatabaser.
func (p *DropForeignKey) WithDatabaseProvider(provider sql.DatabaseProvider) (sql.Node, error) {
	np := *p
	np.dbProvider = provider
	return &np, nil
}

// Resolved implements the interface sql.Node.
func (p *DropForeignKey) Resolved() bool {
	return p.dbProvider != nil
}

// Children implements the interface sql.Node.
func (p *DropForeignKey) Children() []sql.Node {
	return nil
}

// String implements the interface sql.Node.
func (p *DropForeignKey) String() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("DropForeignKey(%s)", p.Name)
	_ = pr.WriteChildren(fmt.Sprintf("Table(%s.%s)", p.Database, p.Table))
	return pr.String()
}
