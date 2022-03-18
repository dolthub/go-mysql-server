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
	"sort"
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
	//TODO: look for foreign keys on the same columns

	// Ensure that a suitable index exists on the referenced table, and check the declaring table for a suitable index.
	refTblIndex, ok, err := FindIndexWithPrefix(ctx, refTbl, fkDef.ReferencedColumns)
	if err != nil {
		return err
	}
	if !ok {
		//TODO: make SQL error
		return fmt.Errorf("missing index for foreign key `%s` on the referenced table `%s`", fkDef.Name, fkDef.ReferencedTable)
	}
	_, ok, err = FindIndexWithPrefix(ctx, tbl, fkDef.Columns)
	if err != nil {
		return err
	}
	if !ok {
		indexColumns := make([]sql.IndexColumn, len(fkDef.Columns))
		for i, col := range fkDef.Columns {
			indexColumns[i] = sql.IndexColumn{
				Name:   col,
				Length: 0,
			}
		}
		//TODO: generate a non-colliding index name
		err := tbl.CreateIndexForForeignKey(ctx, fkDef.Name+"_idx", sql.IndexUsing_Default, sql.IndexConstraint_None, indexColumns)
		if err != nil {
			return err
		}
	}

	indexPositions, appendTypes, err := FindForeignKeyColMapping(ctx, fkDef.Name, tbl, fkDef.Columns, fkDef.ReferencedColumns, refTblIndex)
	if err != nil {
		return err
	}
	reference := &ForeignKeyReferenceHandler{
		Index:          refTblIndex,
		ForeignKey:     *fkDef,
		Editor:         refTbl.GetForeignKeyUpdater(ctx),
		IndexPositions: indexPositions,
		AppendTypes:    appendTypes,
	}
	if err := reference.CheckTable(ctx, tbl); err != nil {
		return err
	}
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

// FindForeignKeyColMapping returns the mapping from a given row to its equivalent index position, based on the matching
// foreign key columns. This also verifies that the column types match, as it is a prerequisite for mapping. For foreign
// keys that do not match the full index, also returns the types to append during the key mapping, as all index columns
// must have a column expression. All strings are case-insensitive.
func FindForeignKeyColMapping(
	ctx *sql.Context,
	fkName string,
	localTbl sql.ForeignKeyTable,
	localFKCols []string,
	destFKCols []string,
	index sql.Index,
) ([]int, []sql.Type, error) {
	localFKCols = lowercaseSlice(localFKCols)
	destFKCols = lowercaseSlice(destFKCols)
	destTblName := strings.ToLower(index.Table())

	localSchTypeMap := make(map[string]sql.Type)
	localSchPositionMap := make(map[string]int)
	for i, col := range localTbl.Schema() {
		colName := strings.ToLower(col.Name)
		localSchTypeMap[colName] = col.Type
		localSchPositionMap[colName] = i
	}
	var appendTypes []sql.Type
	indexTypeMap := make(map[string]sql.Type)
	indexColMap := make(map[string]int)
	for i, indexCol := range index.ColumnExpressionTypes(ctx) {
		indexColName := strings.ToLower(indexCol.Expression)
		indexTypeMap[indexColName] = indexCol.Type
		indexColMap[indexColName] = i
		if i >= len(destFKCols) {
			appendTypes = append(appendTypes, indexCol.Type)
		}
	}
	indexPositions := make([]int, len(destFKCols))

	for fkIdx, colName := range localFKCols {
		localRowPos, ok := localSchPositionMap[colName]
		if !ok {
			// Will happen if a column is renamed that is referenced by a foreign key
			//TODO: enforce that renaming a column referenced by a foreign key updates that foreign key
			return nil, nil, fmt.Errorf("column `%s` in foreign key `%s` cannot be found",
				colName, fkName)
		}
		expectedType := localSchTypeMap[colName]
		destFkCol := destTblName + "." + destFKCols[fkIdx]
		indexPos, ok := indexColMap[destFkCol]
		if !ok {
			// Same as above, renaming a referenced column would cause this error
			return nil, nil, fmt.Errorf("index column `%s` in foreign key `%s` cannot be found",
				destFKCols[fkIdx], fkName)
		}
		//TODO: add equality checks to types
		if indexTypeMap[destFkCol] != expectedType {
			return nil, nil, fmt.Errorf("mismatched types")
		}
		indexPositions[indexPos] = localRowPos
	}
	return indexPositions, appendTypes, nil
}

// FindIndexWithPrefix returns an index that has the given columns as a prefix. The returned index is deterministic and
// follows the given rules, from the highest priority in descending order:
//
// 1. Columns exactly match the index
// 2. Columns match as much of the index prefix as possible
// 3. Largest index by column count
// 4. Index ID in ascending order
//
// The prefix columns may be in any order, and the returned index will contain all of the prefix columns within its
// prefix. For example, the slices [col1, col2] and [col2, col1] will match the same index, as their ordering does not
// matter. The index [col1, col2, col3] would match, but the index [col1, col3] would not match as it is missing "col2".
// Prefix columns are case-insensitive.
func FindIndexWithPrefix(ctx *sql.Context, tbl sql.IndexedTable, prefixCols []string) (sql.Index, bool, error) {
	type idxWithLen struct {
		sql.Index
		colLen int
	}

	indexes, err := tbl.GetIndexes(ctx)
	if err != nil {
		return nil, false, err
	}
	tblName := strings.ToLower(tbl.Name())
	exprCols := make([]string, len(prefixCols))
	for i, prefixCol := range prefixCols {
		exprCols[i] = tblName + "." + strings.ToLower(prefixCol)
	}
	colLen := len(exprCols)
	var indexesWithLen []idxWithLen
	for _, idx := range indexes {
		indexExprs := lowercaseSlice(idx.Expressions())
		if ok, prefixCount := exprsAreIndexSubset(exprCols, indexExprs); ok && prefixCount == colLen {
			indexesWithLen = append(indexesWithLen, idxWithLen{idx, len(indexExprs)})
		}
	}
	if len(indexesWithLen) == 0 {
		return nil, false, nil
	}

	sort.Slice(indexesWithLen, func(i, j int) bool {
		idxI := indexesWithLen[i]
		idxJ := indexesWithLen[j]
		if idxI.colLen == colLen && idxJ.colLen != colLen {
			return true
		} else if idxI.colLen != colLen && idxJ.colLen == colLen {
			return false
		} else if idxI.colLen != idxJ.colLen {
			return idxI.colLen > idxJ.colLen
		} else {
			return idxI.Index.ID() < idxJ.Index.ID()
		}
	})
	sortedIndexes := make([]sql.Index, len(indexesWithLen))
	for i := 0; i < len(sortedIndexes); i++ {
		sortedIndexes[i] = indexesWithLen[i].Index
	}
	return sortedIndexes[0], true, nil
}

// TODO: copy of analyzer.exprsAreIndexSubset, need to shift stuff around to eliminate import cycle
func exprsAreIndexSubset(exprs, indexExprs []string) (ok bool, prefixCount int) {
	if len(exprs) > len(indexExprs) {
		return false, 0
	}

	visitedIndexExprs := make([]bool, len(indexExprs))
	for _, expr := range exprs {
		found := false
		for j, indexExpr := range indexExprs {
			if visitedIndexExprs[j] {
				continue
			}
			if expr == indexExpr {
				visitedIndexExprs[j] = true
				found = true
				break
			}
		}
		if !found {
			return false, 0
		}
	}

	// This checks the length of the prefix by checking how many true booleans are encountered before the first false
	for i, visitedExpr := range visitedIndexExprs {
		if visitedExpr {
			continue
		}
		return true, i
	}

	return true, len(exprs)
}

func lowercaseSlice(strs []string) []string {
	newStrs := make([]string, len(strs))
	for i, str := range strs {
		newStrs[i] = strings.ToLower(str)
	}
	return newStrs
}
