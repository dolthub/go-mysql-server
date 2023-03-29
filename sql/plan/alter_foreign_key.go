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

	"github.com/dolthub/vitess/go/sqltypes"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func getForeignKeyTable(t sql.Table) (sql.ForeignKeyTable, error) {
	switch t := t.(type) {
	case sql.ForeignKeyTable:
		return t, nil
	case sql.TableWrapper:
		return getForeignKeyTable(t.Underlying())
	case *ResolvedTable:
		return getForeignKeyTable(t.Table)
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
var _ sql.Databaseable = (*CreateForeignKey)(nil)
var _ sql.CollationCoercible = (*CreateForeignKey)(nil)

func NewAlterAddForeignKey(fkDef *sql.ForeignKeyConstraint) *CreateForeignKey {
	return &CreateForeignKey{
		dbProvider: nil,
		FkDef:      fkDef,
	}
}

func (p *CreateForeignKey) Database() string {
	return p.FkDef.Database
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
		sql.NewPrivilegedOperation(p.FkDef.ParentDatabase, p.FkDef.ParentTable, "", sql.PrivilegeType_References))
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*CreateForeignKey) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 7
}

// Schema implements the interface sql.Node.
func (p *CreateForeignKey) Schema() sql.Schema {
	return types.OkResultSchema
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
	if p.FkDef.OnUpdate == sql.ForeignKeyReferentialAction_SetDefault || p.FkDef.OnDelete == sql.ForeignKeyReferentialAction_SetDefault {
		return nil, sql.ErrForeignKeySetDefault.New()
	}
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

	refDb, err := p.dbProvider.Database(ctx, p.FkDef.ParentDatabase)
	if err != nil {
		return nil, err
	}
	refTbl, ok, err := refDb.GetTableInsensitive(ctx, p.FkDef.ParentTable)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, sql.ErrTableNotFound.New(p.FkDef.ParentTable)
	}

	fkTbl, ok := tbl.(sql.ForeignKeyTable)
	if !ok {
		return nil, sql.ErrNoForeignKeySupport.New(p.FkDef.Table)
	}
	refFkTbl, ok := refTbl.(sql.ForeignKeyTable)
	if !ok {
		return nil, sql.ErrNoForeignKeySupport.New(p.FkDef.ParentTable)
	}

	fkChecks, err := ctx.GetSessionVariable(ctx, "foreign_key_checks")
	if err != nil {
		return nil, err
	}

	err = ResolveForeignKey(ctx, fkTbl, refFkTbl, *p.FkDef, true, fkChecks.(int8) == 1)
	if err != nil {
		return nil, err
	}

	return sql.RowsToRowIter(sql.NewRow(types.NewOkResult(0))), nil
}

// String implements the interface sql.Node.
func (p *CreateForeignKey) String() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("AddForeignKey(%s)", p.FkDef.Name)
	_ = pr.WriteChildren(
		fmt.Sprintf("Table(%s.%s)", p.FkDef.Database, p.FkDef.Table),
		fmt.Sprintf("Columns(%s)", strings.Join(p.FkDef.Columns, ", ")),
		fmt.Sprintf("ParentTable(%s.%s)", p.FkDef.ParentDatabase, p.FkDef.ParentTable),
		fmt.Sprintf("ParentColumns(%s)", strings.Join(p.FkDef.ParentColumns, ", ")),
		fmt.Sprintf("OnUpdate(%s)", p.FkDef.OnUpdate),
		fmt.Sprintf("OnDelete(%s)", p.FkDef.OnDelete))
	return pr.String()
}

// ResolveForeignKey verifies the foreign key definition and resolves the foreign key, creating indexes and validating
// data as necessary.
func ResolveForeignKey(ctx *sql.Context, tbl sql.ForeignKeyTable, refTbl sql.ForeignKeyTable, fkDef sql.ForeignKeyConstraint, shouldAdd, fkChecks bool) error {
	if t, ok := tbl.(sql.TemporaryTable); ok && t.IsTemporary() {
		return sql.ErrTemporaryTablesForeignKeySupport.New()
	}
	if fkDef.IsResolved {
		return fmt.Errorf("cannot resolve foreign key `%s` as it has already been resolved", fkDef.Name)
	}
	if len(fkDef.Columns) == 0 {
		return sql.ErrForeignKeyMissingColumns.New()
	}
	if len(fkDef.Columns) != len(fkDef.ParentColumns) {
		return sql.ErrForeignKeyColumnCountMismatch.New()
	}

	// Make sure that all columns are valid, in the table, and there are no duplicates
	cols := make(map[string]*sql.Column)
	seenCols := make(map[string]struct{})
	for _, col := range tbl.Schema() {
		lowerColName := strings.ToLower(col.Name)
		cols[lowerColName] = col
	}
	for i, fkCol := range fkDef.Columns {
		lowerFkCol := strings.ToLower(fkCol)
		col, ok := cols[lowerFkCol]
		if !ok {
			return sql.ErrTableColumnNotFound.New(tbl.Name(), fkCol)
		}
		_, ok = seenCols[lowerFkCol]
		if ok {
			return sql.ErrAddForeignKeyDuplicateColumn.New(fkCol)
		}
		// Non-nullable columns may not have SET NULL as a reference option
		if !col.Nullable && (fkDef.OnUpdate == sql.ForeignKeyReferentialAction_SetNull || fkDef.OnDelete == sql.ForeignKeyReferentialAction_SetNull) {
			return sql.ErrForeignKeySetNullNonNullable.New(col.Name)
		}
		seenCols[lowerFkCol] = struct{}{}
		fkDef.Columns[i] = col.Name
	}

	// Do the same for the referenced columns
	if fkChecks {
		parentCols := make(map[string]*sql.Column)
		seenCols = make(map[string]struct{})
		for _, col := range refTbl.Schema() {
			lowerColName := strings.ToLower(col.Name)
			parentCols[lowerColName] = col
		}
		for i, fkParentCol := range fkDef.ParentColumns {
			lowerFkParentCol := strings.ToLower(fkParentCol)
			parentCol, ok := parentCols[lowerFkParentCol]
			if !ok {
				return sql.ErrTableColumnNotFound.New(fkDef.ParentTable, fkParentCol)
			}
			_, ok = seenCols[lowerFkParentCol]
			if ok {
				return sql.ErrAddForeignKeyDuplicateColumn.New(fkParentCol)
			}
			seenCols[lowerFkParentCol] = struct{}{}
			fkDef.ParentColumns[i] = parentCol.Name
		}

		// Check that the types align and are valid
		for i := range fkDef.Columns {
			col := cols[strings.ToLower(fkDef.Columns[i])]
			parentCol := parentCols[strings.ToLower(fkDef.ParentColumns[i])]
			if !foreignKeyComparableTypes(ctx, col.Type, parentCol.Type) {
				return sql.ErrForeignKeyColumnTypeMismatch.New(fkDef.Columns[i], fkDef.ParentColumns[i])
			}
			sqlParserType := col.Type.Type()
			if sqlParserType == sqltypes.Text || sqlParserType == sqltypes.Blob {
				return sql.ErrForeignKeyTextBlob.New()
			}
		}

		// Ensure that a suitable index exists on the referenced table, and check the declaring table for a suitable index.
		refTblIndex, ok, err := FindIndexWithPrefix(ctx, refTbl, fkDef.ParentColumns)
		if err != nil {
			return err
		}
		if !ok {
			return sql.ErrForeignKeyMissingReferenceIndex.New(fkDef.Name, fkDef.ParentTable)
		}

		indexPositions, appendTypes, err := FindForeignKeyColMapping(ctx, fkDef.Name, tbl, fkDef.Columns, fkDef.ParentColumns, refTblIndex)
		if err != nil {
			return err
		}
		var selfCols map[string]int
		if fkDef.IsSelfReferential() {
			selfCols = make(map[string]int)
			for i, col := range tbl.Schema() {
				selfCols[strings.ToLower(col.Name)] = i
			}
		}
		reference := &ForeignKeyReferenceHandler{
			ForeignKey: fkDef,
			SelfCols:   selfCols,
			RowMapper: ForeignKeyRowMapper{
				Index:          refTblIndex,
				Updater:        refTbl.GetForeignKeyEditor(ctx),
				SourceSch:      tbl.Schema(),
				IndexPositions: indexPositions,
				AppendTypes:    appendTypes,
			},
		}

		if err := reference.CheckTable(ctx, tbl); err != nil {
			return err
		}
	}

	// Check if the current foreign key name has already been used. Rather than checking the table first (which is the
	// highest cost part of creating a foreign key), we'll check the name if it needs to be checked. If the foreign key
	// was previously added, we don't need to check the name.
	if shouldAdd {
		existingFks, err := tbl.GetDeclaredForeignKeys(ctx)
		if err != nil {
			return err
		}
		fkLowerName := strings.ToLower(fkDef.Name)
		for _, existingFk := range existingFks {
			if fkLowerName == strings.ToLower(existingFk.Name) {
				return sql.ErrForeignKeyDuplicateName.New(fkDef.Name)
			}
		}
	}

	_, ok, err := FindIndexWithPrefix(ctx, tbl, fkDef.Columns)
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
		indexMap := make(map[string]struct{})
		indexes, err := tbl.GetIndexes(ctx)
		if err != nil {
			return err
		}
		for _, index := range indexes {
			indexMap[strings.ToLower(index.ID())] = struct{}{}
		}
		indexName := strings.Join(fkDef.Columns, "")
		if _, ok = indexMap[strings.ToLower(indexName)]; ok {
			for i := 0; true; i++ {
				newIndexName := fmt.Sprintf("%s_%d", indexName, i)
				if _, ok = indexMap[strings.ToLower(newIndexName)]; !ok {
					indexName = newIndexName
					break
				}
			}
		}
		err = tbl.CreateIndexForForeignKey(ctx, sql.IndexDef{
			Name:       indexName,
			Columns:    indexColumns,
			Constraint: sql.IndexConstraint_None,
			Storage:    sql.IndexUsing_Default,
		})
		if err != nil {
			return err
		}
	}

	if shouldAdd {
		fkDef.IsResolved = fkChecks
		return tbl.AddForeignKey(ctx, fkDef)
	} else {
		fkDef.IsResolved = fkChecks
		return tbl.UpdateForeignKey(ctx, fkDef.Name, fkDef)
	}
}

type DropForeignKey struct {
	// In the cases where we have multiple ALTER statements, we need to resolve the table at execution time rather than
	// during analysis. Otherwise, you could add a foreign key in the preceding alter and we may have analyzed to a
	// table that did not yet have that foreign key.
	dbProvider sql.DatabaseProvider
	database   string
	Table      string
	Name       string
}

var _ sql.Node = (*DropForeignKey)(nil)
var _ sql.MultiDatabaser = (*DropForeignKey)(nil)
var _ sql.Databaseable = (*DropForeignKey)(nil)
var _ sql.CollationCoercible = (*DropForeignKey)(nil)

func NewAlterDropForeignKey(db, table, name string) *DropForeignKey {
	return &DropForeignKey{
		dbProvider: nil,
		database:   db,
		Table:      table,
		Name:       name,
	}
}

func (p *DropForeignKey) Database() string {
	return p.database
}

// RowIter implements the interface sql.Node.
func (p *DropForeignKey) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	db, err := p.dbProvider.Database(ctx, p.database)
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

	return sql.RowsToRowIter(sql.NewRow(types.NewOkResult(0))), nil
}

// WithChildren implements the interface sql.Node.
func (p *DropForeignKey) WithChildren(children ...sql.Node) (sql.Node, error) {
	return NillaryWithChildren(p, children...)
}

// CheckPrivileges implements the interface sql.Node.
func (p *DropForeignKey) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return opChecker.UserHasPrivileges(ctx,
		sql.NewPrivilegedOperation(p.database, p.Table, "", sql.PrivilegeType_Alter))
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*DropForeignKey) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 7
}

// Schema implements the interface sql.Node.
func (p *DropForeignKey) Schema() sql.Schema {
	return types.OkResultSchema
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
	_ = pr.WriteChildren(fmt.Sprintf("Table(%s.%s)", p.Database(), p.Table))
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
	for i, indexCol := range index.ColumnExpressionTypes() {
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
		if !foreignKeyComparableTypes(ctx, indexTypeMap[destFkCol], expectedType) {
			return nil, nil, sql.ErrForeignKeyColumnTypeMismatch.New(colName, destFkCol)
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
// 3. Unique index before non-unique
// 4. Largest index by column count
// 5. Index ID in ascending order
//
// The prefix columns may be in any order, and the returned index will contain all of the prefix columns within its
// prefix. For example, the slices [col1, col2] and [col2, col1] will match the same index, as their ordering does not
// matter. The index [col1, col2, col3] would match, but the index [col1, col3] would not match as it is missing "col2".
// Prefix columns are case-insensitive.
func FindIndexWithPrefix(ctx *sql.Context, tbl sql.IndexAddressableTable, prefixCols []string, ignoredIndexes ...string) (sql.Index, bool, error) {
	type idxWithLen struct {
		sql.Index
		colLen int
	}

	ignoredIndexesMap := make(map[string]struct{})
	for _, ignoredIndex := range ignoredIndexes {
		ignoredIndexesMap[strings.ToLower(ignoredIndex)] = struct{}{}
	}

	indexes, err := tbl.GetIndexes(ctx)
	if err != nil {
		return nil, false, err
	}
	// ignore indexes with prefix lengths; they are unsupported in MySQL
	// https://dev.mysql.com/doc/refman/8.0/en/create-table-foreign-keys.html#:~:text=Index%20prefixes%20on%20foreign%20key%20columns%20are%20not%20supported.
	// ignore spatial indexes; MySQL will not pick them as the underlying secondary index for foreign keys
	for _, idx := range indexes {
		if len(idx.PrefixLengths()) > 0 || idx.IsSpatial() {
			ignoredIndexesMap[idx.ID()] = struct{}{}
		}
	}
	tblName := strings.ToLower(tbl.Name())
	exprCols := make([]string, len(prefixCols))
	for i, prefixCol := range prefixCols {
		exprCols[i] = tblName + "." + strings.ToLower(prefixCol)
	}
	colLen := len(exprCols)
	var indexesWithLen []idxWithLen
	for _, idx := range indexes {
		if _, ok := ignoredIndexesMap[strings.ToLower(idx.ID())]; ok {
			continue
		}
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
		} else if idxI.Index.IsUnique() != idxJ.Index.IsUnique() {
			return idxI.Index.IsUnique()
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

// foreignKeyComparableTypes returns whether the two given types are able to be used as parent/child columns in a
// foreign key.
func foreignKeyComparableTypes(ctx *sql.Context, type1 sql.Type, type2 sql.Type) bool {
	if !type1.Equals(type2) {
		// There seems to be a special case where CHAR/VARCHAR/BINARY/VARBINARY can have unequal lengths.
		// Have not tested every type nor combination, but this seems specific to those 4 types.
		if type1.Type() == type2.Type() {
			switch type1.Type() {
			case sqltypes.Char, sqltypes.VarChar, sqltypes.Binary, sqltypes.VarBinary:
				type1String := type1.(sql.StringType)
				type2String := type2.(sql.StringType)
				if type1String.Collation().CharacterSet() != type2String.Collation().CharacterSet() {
					return false
				}
			default:
				return false
			}
		} else {
			return false
		}
	}
	return true
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
