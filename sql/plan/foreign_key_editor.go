// Copyright 2022 Dolthub, Inc.
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
)

// ChildParentMapping is a mapping from the foreign key columns of a child schema to the parent schema. The position
// in the slice corresponds to the position in the child schema, while the value at a given position refers to the
// position in the parent schema. For all columns that are not in the foreign key definition, a value of -1 is returned.
//
// Here's an example:
// parent Schema: x1, x2, x3, x4, x5
// child Schema:  y1, y2, y3, y4
// FOREIGN KEY (y2) REFERENCES parent (x4)
//
// The slice for the above would be [-1, 3, -1, -1]. The foreign key uses the column "y2" on the child, which is the
// second position in the schema (and therefore the second position in the mapping). The equivalent parent column is
// "x4", which is in the fourth position (so 3 with zero-based indexed).
type ChildParentMapping []int

// ForeignKeyRefActionData contains the mapper, editor, and child to parent mapping for processing referential actions.
type ForeignKeyRefActionData struct {
	RowMapper          *ForeignKeyRowMapper
	Editor             *ForeignKeyEditor
	ForeignKey         sql.ForeignKeyConstraint
	ChildParentMapping ChildParentMapping
}

// ForeignKeyEditor handles update and delete operations, as they may have referential actions on other tables (such as
// cascading). If this editor is Cyclical, then that means that following the referential actions will eventually lead
// back to this same editor. Self-referential foreign keys are inherently cyclical.
type ForeignKeyEditor struct {
	Schema     sql.Schema
	Editor     sql.ForeignKeyEditor
	References []*ForeignKeyReferenceHandler
	RefActions []ForeignKeyRefActionData
	Cyclical   bool
}

// IsInitialized returns whether this editor has been initialized. The given map is used to prevent cycles, as editors
// will reference themselves if a cycle is formed between foreign keys.
func (fkEditor *ForeignKeyEditor) IsInitialized(editors map[*ForeignKeyEditor]struct{}) bool {
	if fkEditor == nil || fkEditor.Editor == nil {
		return false
	}
	if _, ok := editors[fkEditor]; ok {
		return true
	}
	editors[fkEditor] = struct{}{}
	for _, reference := range fkEditor.References {
		if !reference.IsInitialized() {
			return false
		}
	}
	for _, refAction := range fkEditor.RefActions {
		if !refAction.Editor.IsInitialized(editors) {
			return false
		}
	}
	return true
}

// Update handles both the standard UPDATE statement and propagated referential actions from a parent table's ON UPDATE.
func (fkEditor *ForeignKeyEditor) Update(ctx *sql.Context, old sql.Row, new sql.Row, depth int) error {
	for _, reference := range fkEditor.References {
		// Only check the reference for the columns that are updated
		hasChange := false
		for _, idx := range reference.RowMapper.IndexPositions {
			cmp, err := fkEditor.Schema[idx].Type.Compare(old.GetValue(idx), new.GetValue(idx))
			if err != nil {
				return err
			}
			if cmp != 0 {
				hasChange = true
				break
			}
		}
		if !hasChange {
			continue
		}
		if err := reference.CheckReference(ctx, new); err != nil {
			return err
		}
	}
	for _, refActionData := range fkEditor.RefActions {
		switch refActionData.ForeignKey.OnUpdate {
		default: // RESTRICT and friends
			if err := fkEditor.OnUpdateRestrict(ctx, refActionData, old, new); err != nil {
				return err
			}
		case sql.ForeignKeyReferentialAction_Cascade:
		case sql.ForeignKeyReferentialAction_SetNull:
		}
	}
	if err := fkEditor.Editor.Update(ctx, old, new); err != nil {
		return err
	}
	for _, refActionData := range fkEditor.RefActions {
		switch refActionData.ForeignKey.OnUpdate {
		case sql.ForeignKeyReferentialAction_Cascade:
			if err := fkEditor.OnUpdateCascade(ctx, refActionData, old, new, depth+1); err != nil {
				return err
			}
		case sql.ForeignKeyReferentialAction_SetNull:
			if err := fkEditor.OnUpdateSetNull(ctx, refActionData, old, new, depth+1); err != nil {
				return err
			}
		}
	}
	return nil
}

// OnUpdateRestrict handles the ON UPDATE RESTRICT referential action.
func (fkEditor *ForeignKeyEditor) OnUpdateRestrict(ctx *sql.Context, refActionData ForeignKeyRefActionData, old sql.Row, new sql.Row) error {
	if ok, err := fkEditor.ColumnsUpdated(refActionData, old, new); err != nil {
		return err
	} else if !ok {
		return nil
	}

	rowIter, err := refActionData.RowMapper.GetIter(ctx, old, false)
	if err != nil {
		return err
	}
	defer rowIter.Close(ctx)
	if _, err = rowIter.Next(ctx); err == nil {
		return sql.ErrForeignKeyParentViolation.New(refActionData.ForeignKey.Name,
			refActionData.ForeignKey.Table, refActionData.ForeignKey.ParentTable, refActionData.RowMapper.GetKeyString(old))
	}
	if err != io.EOF {
		return err
	}
	return nil
}

// OnUpdateCascade handles the ON UPDATE CASCADE referential action.
func (fkEditor *ForeignKeyEditor) OnUpdateCascade(ctx *sql.Context, refActionData ForeignKeyRefActionData, old sql.Row, new sql.Row, depth int) error {
	if ok, err := fkEditor.ColumnsUpdated(refActionData, old, new); err != nil {
		return err
	} else if !ok {
		return nil
	}

	rowIter, err := refActionData.RowMapper.GetIter(ctx, old, false)
	if err != nil {
		return err
	}
	defer rowIter.Close(ctx)
	var rowToUpdate sql.Row = sql.UntypedSqlRow{}
	for rowToUpdate, err = rowIter.Next(ctx); err == nil; rowToUpdate, err = rowIter.Next(ctx) {
		if depth > 15 {
			return sql.ErrForeignKeyDepthLimit.New()
		}
		updatedRow := sql.NewSqlRowWithLen(rowToUpdate.Len())
		for i := range rowToUpdate.Values() {
			mappedVal := refActionData.ChildParentMapping[i]
			if mappedVal == -1 {
				updatedRow.SetValue(i, rowToUpdate.GetValue(i))
			} else {
				updatedRow.SetValue(i, new.GetValue(mappedVal))
			}
		}
		err = refActionData.Editor.Update(ctx, rowToUpdate, updatedRow, depth)
		if err != nil {
			return err
		}
	}
	if err == io.EOF {
		return nil
	}
	return err
}

// OnUpdateSetNull handles the ON UPDATE SET NULL referential action.
func (fkEditor *ForeignKeyEditor) OnUpdateSetNull(ctx *sql.Context, refActionData ForeignKeyRefActionData, old sql.Row, new sql.Row, depth int) error {
	if ok, err := fkEditor.ColumnsUpdated(refActionData, old, new); err != nil {
		return err
	} else if !ok {
		return nil
	}

	rowIter, err := refActionData.RowMapper.GetIter(ctx, old, false)
	if err != nil {
		return err
	}
	defer rowIter.Close(ctx)
	var rowToUpdate sql.Row = sql.UntypedSqlRow{}
	for rowToUpdate, err = rowIter.Next(ctx); err == nil; rowToUpdate, err = rowIter.Next(ctx) {
		if depth > 15 {
			return sql.ErrForeignKeyDepthLimit.New()
		}
		updatedRow := sql.NewSqlRowWithLen(rowToUpdate.Len())
		for i := range rowToUpdate.Values() {
			// Row contents are nil by default, so we only need to assign the non-affected values
			if refActionData.ChildParentMapping[i] == -1 {
				updatedRow.SetValue(i, rowToUpdate.GetValue(i))
			}
		}
		err = refActionData.Editor.Update(ctx, rowToUpdate, updatedRow, depth)
		if err != nil {
			return err
		}
	}
	if err == io.EOF {
		return nil
	}
	return err
}

// Delete handles both the standard DELETE statement and propagated referential actions from a parent table's ON DELETE.
func (fkEditor *ForeignKeyEditor) Delete(ctx *sql.Context, row sql.Row, depth int) error {
	//TODO: may need to process some cascades after the update to avoid recursive violations, write some tests on this
	for _, refActionData := range fkEditor.RefActions {
		switch refActionData.ForeignKey.OnDelete {
		default: // RESTRICT and friends
			if err := fkEditor.OnDeleteRestrict(ctx, refActionData, row); err != nil {
				return err
			}
		case sql.ForeignKeyReferentialAction_Cascade:
		case sql.ForeignKeyReferentialAction_SetNull:
		}
	}
	if err := fkEditor.Editor.Delete(ctx, row); err != nil {
		return err
	}
	for _, refActionData := range fkEditor.RefActions {
		switch refActionData.ForeignKey.OnDelete {
		case sql.ForeignKeyReferentialAction_Cascade:
			if err := fkEditor.OnDeleteCascade(ctx, refActionData, row, depth+1); err != nil {
				return err
			}
		case sql.ForeignKeyReferentialAction_SetNull:
			if err := fkEditor.OnDeleteSetNull(ctx, refActionData, row, depth+1); err != nil {
				return err
			}
		}
	}
	return nil
}

// OnDeleteRestrict handles the ON DELETE RESTRICT referential action.
func (fkEditor *ForeignKeyEditor) OnDeleteRestrict(ctx *sql.Context, refActionData ForeignKeyRefActionData, row sql.Row) error {
	rowIter, err := refActionData.RowMapper.GetIter(ctx, row, false)
	if err != nil {
		return err
	}
	defer rowIter.Close(ctx)
	if _, err = rowIter.Next(ctx); err == nil {
		return sql.ErrForeignKeyParentViolation.New(refActionData.ForeignKey.Name,
			refActionData.ForeignKey.Table, refActionData.ForeignKey.ParentTable, refActionData.RowMapper.GetKeyString(row))
	}
	if err != io.EOF {
		return err
	}
	return nil
}

// OnDeleteCascade handles the ON DELETE CASCADE referential action.
func (fkEditor *ForeignKeyEditor) OnDeleteCascade(ctx *sql.Context, refActionData ForeignKeyRefActionData, row sql.Row, depth int) error {
	rowIter, err := refActionData.RowMapper.GetIter(ctx, row, false)
	if err != nil {
		return err
	}
	defer rowIter.Close(ctx)
	var rowToDelete sql.Row = sql.UntypedSqlRow{}
	for rowToDelete, err = rowIter.Next(ctx); err == nil; rowToDelete, err = rowIter.Next(ctx) {
		// MySQL seems to have a bug where cyclical foreign keys return an error at a depth of 15 instead of 16.
		// This replicates the observed behavior, regardless of whether we're replicating a bug or intentional behavior.
		if depth >= 15 {
			if fkEditor.Cyclical {
				return sql.ErrForeignKeyDepthLimit.New()
			} else if depth > 15 {
				return sql.ErrForeignKeyDepthLimit.New()
			}
		}
		err = refActionData.Editor.Delete(ctx, rowToDelete, depth)
		if err != nil {
			return err
		}
	}
	if err == io.EOF {
		return nil
	}
	return err
}

// OnDeleteSetNull handles the ON DELETE SET NULL referential action.
func (fkEditor *ForeignKeyEditor) OnDeleteSetNull(ctx *sql.Context, refActionData ForeignKeyRefActionData, row sql.Row, depth int) error {
	rowIter, err := refActionData.RowMapper.GetIter(ctx, row, false)
	if err != nil {
		return err
	}
	defer rowIter.Close(ctx)
	var rowToNull sql.Row = sql.UntypedSqlRow{}
	for rowToNull, err = rowIter.Next(ctx); err == nil; rowToNull, err = rowIter.Next(ctx) {
		// MySQL seems to have a bug where cyclical foreign keys return an error at a depth of 15 instead of 16.
		// This replicates the observed behavior, regardless of whether we're replicating a bug or intentional behavior.
		if depth >= 15 {
			if fkEditor.Cyclical {
				return sql.ErrForeignKeyDepthLimit.New()
			} else if depth > 15 {
				return sql.ErrForeignKeyDepthLimit.New()
			}
		}
		nulledRow := sql.NewSqlRowWithLen(rowToNull.Len())
		for i := range rowToNull.Values() {
			// Row contents are nil by default, so we only need to assign the non-affected values
			if refActionData.ChildParentMapping[i] == -1 {
				nulledRow.SetValue(i, rowToNull.GetValue(i))
			}
		}
		err = refActionData.Editor.Update(ctx, rowToNull, nulledRow, depth)
		if err != nil {
			return err
		}
	}
	if err == io.EOF {
		return nil
	}
	return err
}

// ColumnsUpdated returns whether the columns involved in the foreign key were updated. Some updates may only update
// columns that are not involved in a foreign key, and therefore we should ignore a CASCADE or SET NULL referential
// action in such cases.
func (fkEditor *ForeignKeyEditor) ColumnsUpdated(refActionData ForeignKeyRefActionData, old sql.Row, new sql.Row) (bool, error) {
	for _, mappedVal := range refActionData.ChildParentMapping {
		if mappedVal == -1 {
			continue
		}
		oldVal := old.GetValue(mappedVal)
		newVal := new.GetValue(mappedVal)
		cmp, err := fkEditor.Schema[mappedVal].Type.Compare(oldVal, newVal)
		if err != nil {
			return false, err
		}
		if cmp != 0 {
			return true, nil
		}
	}
	return false, nil
}

// Close closes this handler along with all child handlers.
func (fkEditor *ForeignKeyEditor) Close(ctx *sql.Context) error {
	err := fkEditor.Editor.Close(ctx)
	for _, child := range fkEditor.RefActions {
		nErr := child.Editor.Close(ctx)
		if err == nil {
			err = nErr
		}
	}
	return err
}

// ForeignKeyReferenceHandler handles references to any parent rows to verify they exist.
type ForeignKeyReferenceHandler struct {
	ForeignKey sql.ForeignKeyConstraint
	RowMapper  ForeignKeyRowMapper
	SelfCols   map[string]int // SelfCols are used for self-referential fks to refer to a col position given a col name
}

// IsInitialized returns whether this reference handler has been initialized.
func (reference *ForeignKeyReferenceHandler) IsInitialized() bool {
	return reference.RowMapper.IsInitialized()
}

// CheckReference checks that the given row has an index entry in the referenced table.
func (reference *ForeignKeyReferenceHandler) CheckReference(ctx *sql.Context, row sql.Row) error {
	// If even one of the values are NULL then we don't check the parent
	for _, pos := range reference.RowMapper.IndexPositions {
		if row.GetValue(pos) == nil {
			return nil
		}
	}

	rowIter, err := reference.RowMapper.GetIter(ctx, row, true)
	if err != nil {
		return err
	}
	defer rowIter.Close(ctx)

	_, err = rowIter.Next(ctx)
	if err != nil && err != io.EOF {
		return err
	}
	if err == nil {
		// We have a parent row so throw no error
		return nil
	}

	if reference.ForeignKey.IsSelfReferential() {
		allMatch := true
		for i := range reference.ForeignKey.Columns {
			colPos := reference.SelfCols[strings.ToLower(reference.ForeignKey.Columns[i])]
			refPos := reference.SelfCols[strings.ToLower(reference.ForeignKey.ParentColumns[i])]
			cmp, err := reference.RowMapper.SourceSch[colPos].Type.Compare(row.GetValue(colPos), row.GetValue(refPos))
			if err != nil {
				return err
			}
			if cmp != 0 {
				allMatch = false
				break
			}
		}
		if allMatch {
			return nil
		}
	}
	return sql.ErrForeignKeyChildViolation.New(reference.ForeignKey.Name, reference.ForeignKey.Table,
		reference.ForeignKey.ParentTable, reference.RowMapper.GetKeyString(row))
}

// CheckTable checks that every row in the table has an index entry in the referenced table.
func (reference *ForeignKeyReferenceHandler) CheckTable(ctx *sql.Context, tbl sql.ForeignKeyTable) error {
	partIter, err := tbl.Partitions(ctx)
	if err != nil {
		return err
	}
	rowIter := sql.NewTableRowIter(ctx, tbl, partIter)
	defer rowIter.Close(ctx)
	for row, err := rowIter.Next(ctx); err == nil; row, err = rowIter.Next(ctx) {
		err = reference.CheckReference(ctx, row)
		if err != nil {
			return err
		}
	}
	if err != io.EOF {
		return err
	}
	return nil
}

// ForeignKeyRowMapper takes a source row and returns all matching rows on the contained table according to the row
// mapping from the source columns to the contained index's columns.
type ForeignKeyRowMapper struct {
	Index     sql.Index
	Updater   sql.ForeignKeyEditor
	SourceSch sql.Schema
	// IndexPositions hold the mapping between an index's column position and the source row's column position. Given
	// an index (x1, x2) and a source row (y1, y2, y3) and the relation (x1->y3, x2->y1), this slice would contain
	// [2, 0]. The first index column "x1" maps to the third source column "y3" (so position 2 since it's zero-based),
	// and the second index column "x2" maps to the first source column "y1" (position 0).
	IndexPositions []int
	// AppendTypes hold any types that may be needed to complete an index range's generation. Foreign keys are allowed
	// to use an index's prefix, and indexes expect ranges to reference all of their columns (not just the prefix), so
	// we grab the types of the suffix index columns to append to the range after the prefix columns that we're
	// referencing.
	AppendTypes []sql.Type
}

// IsInitialized returns whether this mapper has been initialized.
func (mapper *ForeignKeyRowMapper) IsInitialized() bool {
	return mapper.Updater != nil && mapper.Index != nil
}

// GetIter returns a row iterator for all rows that match the given source row.
func (mapper *ForeignKeyRowMapper) GetIter(ctx *sql.Context, row sql.Row, refCheck bool) (sql.RowIter, error) {
	rang := make(sql.MySQLRange, len(mapper.IndexPositions)+len(mapper.AppendTypes))
	for rangPosition, rowPos := range mapper.IndexPositions {
		rowVal := row.GetValue(rowPos)
		// If any value is NULL then it is ignored by foreign keys
		if rowVal == nil {
			return sql.RowsToRowIter(), nil
		}
		rang[rangPosition] = sql.ClosedRangeColumnExpr(rowVal, rowVal, mapper.SourceSch[rowPos].Type)
	}
	for i, appendType := range mapper.AppendTypes {
		rang[i+len(mapper.IndexPositions)] = sql.AllRangeColumnExpr(appendType)
	}

	if !mapper.Index.CanSupport(rang) {
		return nil, ErrInvalidLookupForIndexedTable.New(rang.DebugString())
	}
	//TODO: profile this, may need to redesign this or add a fast path
	lookup := sql.IndexLookup{Ranges: sql.MySQLRangeCollection{rang}, Index: mapper.Index}

	editorData := mapper.Updater.IndexedAccess(lookup)

	if rc, ok := editorData.(sql.ReferenceChecker); refCheck && ok {
		err := rc.SetReferenceCheck()
		if err != nil {
			return nil, err
		}
	}

	partIter, err := editorData.LookupPartitions(ctx, lookup)
	if err != nil {
		return nil, err
	}
	return sql.NewTableRowIter(ctx, editorData, partIter), nil
}

// GetKeyString returns a string representing the key used to access the index.
func (mapper *ForeignKeyRowMapper) GetKeyString(row sql.Row) string {
	keyStrParts := make([]string, len(mapper.IndexPositions))
	for i, rowPos := range mapper.IndexPositions {
		keyStrParts[i] = fmt.Sprint(row.GetValue(rowPos))
	}
	return fmt.Sprintf("[%s]", strings.Join(keyStrParts, ","))
}

// GetChildParentMapping returns a mapping from the foreign key columns of a child schema to the parent schema.
func GetChildParentMapping(parentSch sql.Schema, childSch sql.Schema, fkDef sql.ForeignKeyConstraint) (ChildParentMapping, error) {
	parentMap := make(map[string]int)
	for i, col := range parentSch {
		parentMap[strings.ToLower(col.Name)] = i
	}
	childMap := make(map[string]int)
	for i, col := range childSch {
		childMap[strings.ToLower(col.Name)] = i
	}
	mapping := make(ChildParentMapping, len(childSch))
	for i := range mapping {
		mapping[i] = -1
	}
	for i := range fkDef.Columns {
		childIndex, ok := childMap[strings.ToLower(fkDef.Columns[i])]
		if !ok {
			return nil, fmt.Errorf("foreign key `%s` refers to column `%s` on table `%s` but it could not be found",
				fkDef.Name, fkDef.Columns[i], fkDef.Table)
		}
		parentIndex, ok := parentMap[strings.ToLower(fkDef.ParentColumns[i])]
		if !ok {
			return nil, fmt.Errorf("foreign key `%s` refers to column `%s` on referenced table `%s` but it could not be found",
				fkDef.Name, fkDef.ParentColumns[i], fkDef.ParentTable)
		}
		mapping[childIndex] = parentIndex
	}
	return mapping, nil
}
