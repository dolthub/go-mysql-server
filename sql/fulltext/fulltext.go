// Copyright 2023 Dolthub, Inc.
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

package fulltext

import (
	"fmt"
	"strings"

	"github.com/dolthub/vitess/go/sqltypes"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

//TODO: remove FULLTEXT before all of the functions/variables, leftover from when this was in the SQL package
//TODO: split this into multiple files, grouped by functionality

const (
	IDColumnName          = "FTS_DOC_ID"       // IDColumnName is the exact name of the column that is used as the document ID.
	IDColumnNameLowercase = "fts_doc_id"       // IDColumnNameLowercase is the lowercased name of the column that is used as the document ID.
	IDIndexName           = "FTS_DOC_ID_INDEX" // IDIndexName is the exact name of the index that is used for the document ID.
	IDIndexNameLowercase  = "fts_doc_id_index" // IDIndexNameLowercase is the lowercased name of the index that is used for the document ID.
)

var (
	// FulltextSchemaConfig is the schema for the config table, which is a pseudo-index implementation of a Full-Text index.
	FulltextSchemaConfig = sql.Schema{
		{Name: "ID", Type: types.Int32, Nullable: false, PrimaryKey: true},
		{Name: "STOPWORD_TABLE", Type: types.MustCreateStringWithDefaults(sqltypes.VarChar, 2048), Nullable: false, PrimaryKey: false},
		{Name: "USE_STOPWORD", Type: types.Boolean, Nullable: false, PrimaryKey: false},
	}
	// FulltextSchemaWordToPosMap is the schema for the "word to position" table, which is a pseudo-index implementation of a Full-Text index.
	FulltextSchemaWordToPosMap = sql.Schema{
		{Name: "WORD", Type: types.MustCreateString(sqltypes.VarChar, 84, sql.Collation_Default), Nullable: false, PrimaryKey: true},
		{Name: "DOC_ID", Type: types.Uint64, Nullable: false, PrimaryKey: true},
		{Name: "POSITION", Type: types.Uint64, Nullable: false, PrimaryKey: true},
	}
	// FulltextSchemaCount is the schema for the count table, which is a pseudo-index implementation of a Full-Text index.
	FulltextSchemaCount = sql.Schema{
		{Name: "WORD", Type: types.MustCreateString(sqltypes.VarChar, 84, sql.Collation_Default), Nullable: false, PrimaryKey: true},
		{Name: "FIRST_DOC_ID", Type: types.Uint64, Nullable: false, PrimaryKey: true}, //TODO: determine if anything after WORD should be in the PK
		{Name: "LAST_DOC_ID", Type: types.Uint64, Nullable: false, PrimaryKey: true},
		{Name: "DOC_COUNT", Type: types.Uint64, Nullable: false, PrimaryKey: true},
	}
	// IDColumnType is the type that the document ID should have.
	IDColumnType = types.Uint64
)

// EditableTable is a table that implements InsertableTable, UpdatableTable, and DeletableTable.
type EditableTable interface {
	sql.InsertableTable
	sql.UpdatableTable
	sql.DeletableTable
}

// IndexAlterableFulltextTable represents a table that supports the creation of FULLTEXT indexes. Renaming and deleting
// the FULLTEXT index are both handled by RenameIndex and DropIndex respectively.
type IndexAlterableFulltextTable interface {
	sql.IndexAlterableTable
	// CreateFulltextIndex creates a FULLTEXT index for this table. The index should not create a backing store, as the
	// names of the tables given have already been created and will be managed by GMS as pseudo-indexes.
	CreateFulltextIndex(ctx *sql.Context, indexDef sql.IndexDef, configTableName, wordToPosTableName, countTableName string) error
	//TODO: maybe handle renaming the representing tables if the parent table has been renamed?
	//TODO: maybe handle dropping the representing tables if the parent table has been dropped?
}

// FulltextIndex contains additional information regarding the FULLTEXT index.
type FulltextIndex interface {
	sql.Index
	// FullTextTableNames returns the names of the tables that represent the FULLTEXT index. Returns false if the index
	// is not FULLTEXT, or the tables do not exist.
	FullTextTableNames(ctx *sql.Context) (config string, wordToPos string, count string, exists bool)
}

// FullTextSearchModifier represents the search modifier when using MATCH ... AGAINST ...
type FullTextSearchModifier byte

const (
	FullTextSearchModifier_NaturalLanguage FullTextSearchModifier = iota
	FullTextSearchModifier_NaturalLangaugeQueryExpansion
	FullTextSearchModifier_Boolean
	FullTextSearchModifier_QueryExpansion
)

// FulltextTablePair contains the two tables that comprise a single FULLTEXT index. The config table is supplied
// separately, as it is shared among all pairs.
type FulltextTablePair struct {
	// Index contains information regarding the FULLTEXT index.
	Index FulltextIndex
	// WordToPosMap refers to the table that maps each word to an ID and position.
	WordToPosMap EditableTable
	// Count refers to the table that contains the word count.
	Count EditableTable
}

// FulltextEditor handles editors for FULLTEXT indexes. These indexes are implemented as standard tables, and therefore
// require the use of this editor to handle the transformation of rows.
type FulltextEditor struct {
	Config     sql.TableEditor
	Indexes    []FulltextEditorIndex
	DocIdIndex int
}

// FulltextEditorIndex represents an individual index's editors, along with the columns (and order) to source the text
// from.
type FulltextEditorIndex struct {
	WordToPos  sql.TableEditor
	Count      sql.TableEditor
	SourceCols []int
}

var _ sql.TableEditor = FulltextEditor{}

// CreateFulltextEditor returns a FulltextEditor that will handle the transformation of rows destined for the parent
// table to the FULLTEXT tables.
func CreateFulltextEditor(ctx *sql.Context, parent sql.Table, config EditableTable, indexes ...FulltextTablePair) (editor FulltextEditor, err error) {
	// Verify that the schema for each table is correct
	if err = fulltextSchemaCompare(config.Name(), config.Schema(), FulltextSchemaConfig); err != nil {
		return FulltextEditor{}, err
	}
	for _, index := range indexes {
		if !index.Index.IsFullText() {
			return FulltextEditor{}, fmt.Errorf("index `%s` is not a FULLTEXT index", index.Index.ID())
		}
		if err = fulltextSchemaCompare(index.WordToPosMap.Name(), index.WordToPosMap.Schema(), FulltextSchemaWordToPosMap); err != nil {
			return FulltextEditor{}, err
		}
		if err = fulltextSchemaCompare(index.Count.Name(), index.Count.Schema(), FulltextSchemaCount); err != nil {
			return FulltextEditor{}, err
		}
	}

	// Ensure that the parent table has the FTS_DOC_ID column
	docIdIndex, parentColMap, err := GetDocIdColPos(parent.Schema())
	if err != nil {
		return FulltextEditor{}, err
	}
	if docIdIndex == -1 {
		return FulltextEditor{}, fmt.Errorf("table `%s` declares a FULLTEXT index but is missing the `%s` column", parent.Name(), IDColumnName)
	}

	// Map each indexes' columns to their respective table columns, and create the editors
	editorIndexes := make([]FulltextEditorIndex, len(indexes))
	for i, index := range indexes {
		exprs := index.Index.Expressions()
		sourceCols := make([]int, len(exprs))
		for i, expr := range exprs {
			var ok bool
			sourceCols[i], ok = parentColMap[strings.ToLower(expr)]
			if !ok {
				return FulltextEditor{}, fmt.Errorf("table `%s` FULLTEXT index `%s` references the column `%s` but it could not be found",
					parent.Name(), index.Index.ID(), expr)
			}
		}

		editorIndexes[i] = FulltextEditorIndex{
			WordToPos:  index.WordToPosMap.Inserter(ctx).(sql.TableEditor),
			Count:      index.Count.Inserter(ctx).(sql.TableEditor),
			SourceCols: sourceCols,
		}
	}

	return FulltextEditor{
		Config:     config.Inserter(ctx).(sql.TableEditor),
		Indexes:    editorIndexes,
		DocIdIndex: docIdIndex,
	}, nil
}

// StatementBegin implements the interface TableEditor.
func (editor FulltextEditor) StatementBegin(ctx *sql.Context) {
	editor.Config.StatementBegin(ctx)
	for _, editorIndex := range editor.Indexes {
		editorIndex.WordToPos.StatementBegin(ctx)
		editorIndex.Count.StatementBegin(ctx)
	}
}

// DiscardChanges implements the interface TableEditor.
func (editor FulltextEditor) DiscardChanges(ctx *sql.Context, errorEncountered error) error {
	err := editor.Config.DiscardChanges(ctx, errorEncountered)
	for _, editorIndex := range editor.Indexes {
		if nErr := editorIndex.WordToPos.DiscardChanges(ctx, errorEncountered); err == nil {
			err = nErr
		}
		if nErr := editorIndex.Count.DiscardChanges(ctx, errorEncountered); err == nil {
			err = nErr
		}
	}
	return err
}

// StatementComplete implements the interface TableEditor.
func (editor FulltextEditor) StatementComplete(ctx *sql.Context) error {
	err := editor.Config.StatementComplete(ctx)
	for _, editorIndex := range editor.Indexes {
		if nErr := editorIndex.WordToPos.StatementComplete(ctx); err == nil {
			err = nErr
		}
		if nErr := editorIndex.Count.StatementComplete(ctx); err == nil {
			err = nErr
		}
	}
	return err
}

// Insert implements the interface TableEditor.
func (editor FulltextEditor) Insert(ctx *sql.Context, row sql.Row) error {
	ftsDocId := row[editor.DocIdIndex].(uint64)
	for _, index := range editor.Indexes {
		// A multi-column index treats a row as the concatenation of all columns, so we track our current position
		position := uint64(0)
		for _, sourceCol := range index.SourceCols {
			colStr := row[sourceCol].(string) //TODO: verify that this will always be a string and never a []byte
			words := strings.Split(colStr, " ")
			for _, word := range words {
				if err := index.WordToPos.Insert(ctx, sql.Row{word, ftsDocId, position}); err != nil {
					return err
				}
				// We always add 1 to account for the space between words. This also works for multi-column indexes.
				position += uint64(len(word)) + 1
				//TODO: write to the count table
				//TODO: write to the config table
			}
		}
	}
	return nil
}

// Update implements the interface TableEditor.
func (editor FulltextEditor) Update(ctx *sql.Context, old sql.Row, new sql.Row) error {
	// I'm sure a bespoke UPDATE routine would be more efficient, but this will work for now
	if err := editor.Delete(ctx, old); err != nil {
		return err
	}
	return editor.Insert(ctx, new)
}

// Delete implements the interface TableEditor.
func (editor FulltextEditor) Delete(ctx *sql.Context, row sql.Row) error {
	ftsDocId := row[editor.DocIdIndex].(uint64)
	for _, index := range editor.Indexes {
		// A multi-column index treats a row as the concatenation of all columns, so we track our current position
		position := uint64(0)
		for _, sourceCol := range index.SourceCols {
			colStr := row[sourceCol].(string) //TODO: verify that this will always be a string and never a []byte
			words := strings.Split(colStr, " ")
			for _, word := range words {
				if err := index.WordToPos.Delete(ctx, sql.Row{word, ftsDocId, position}); err != nil {
					return err
				}
				// We always add 1 to account for the space between words. This also works for multi-column indexes.
				position += uint64(len(word)) + 1
				//TODO: write to the count table
				//TODO: write to the config table
			}
		}
	}
	return nil
}

// Close implements the interface TableEditor.
func (editor FulltextEditor) Close(ctx *sql.Context) error {
	err := editor.Config.Close(ctx)
	for _, editorIndex := range editor.Indexes {
		if nErr := editorIndex.WordToPos.Close(ctx); err == nil {
			err = nErr
		}
		if nErr := editorIndex.Count.Close(ctx); err == nil {
			err = nErr
		}
	}
	return err
}

// MultiTableEditor wraps multiple table editors, allowing for a single function to handle writes across multiple tables.
type MultiTableEditor struct {
	primary     sql.TableEditor
	secondaries []sql.TableEditor
}

var _ sql.TableEditor = MultiTableEditor{}
var _ sql.ForeignKeyEditor = MultiTableEditor{}
var _ sql.AutoIncrementSetter = MultiTableEditor{}

// CreateMultiTableEditor creates a TableEditor that writes to both the primary and secondary editors. The primary
// editor must implement ForeignKeyEditor and AutoIncrementSetter in addition to TableEditor.
func CreateMultiTableEditor(ctx *sql.Context, primary sql.TableEditor, secondaries ...sql.TableEditor) (sql.TableEditor, error) {
	if _, ok := primary.(sql.ForeignKeyEditor); !ok {
		return nil, fmt.Errorf("cannot create a MultiTableEditor with a primary editor that does not implement ForeignKeyEditor")
	}
	if _, ok := primary.(sql.AutoIncrementSetter); !ok {
		return nil, fmt.Errorf("cannot create a MultiTableEditor with a primary editor that does not implement AutoIncrementSetter")
	}
	return MultiTableEditor{
		primary:     primary,
		secondaries: secondaries,
	}, nil
}

// StatementBegin implements the interface TableEditor.
func (editor MultiTableEditor) StatementBegin(ctx *sql.Context) {
	for _, secondary := range editor.secondaries {
		secondary.StatementBegin(ctx)
	}
	editor.primary.StatementBegin(ctx)
}

// DiscardChanges implements the interface TableEditor.
func (editor MultiTableEditor) DiscardChanges(ctx *sql.Context, errorEncountered error) error {
	var err error
	for _, secondary := range editor.secondaries {
		if nErr := secondary.DiscardChanges(ctx, errorEncountered); err == nil {
			err = nErr
		}
	}
	if nErr := editor.primary.DiscardChanges(ctx, errorEncountered); err == nil {
		err = nErr
	}
	return err
}

// StatementComplete implements the interface TableEditor.
func (editor MultiTableEditor) StatementComplete(ctx *sql.Context) error {
	var err error
	for _, secondary := range editor.secondaries {
		if nErr := secondary.StatementComplete(ctx); err == nil {
			err = nErr
		}
	}
	if nErr := editor.primary.StatementComplete(ctx); err == nil {
		err = nErr
	}
	return err
}

// Insert implements the interface TableEditor.
func (editor MultiTableEditor) Insert(ctx *sql.Context, row sql.Row) error {
	for _, secondary := range editor.secondaries {
		if err := secondary.Insert(ctx, row); err != nil {
			return err
		}
	}
	return editor.primary.Insert(ctx, row)
}

// Update implements the interface TableEditor.
func (editor MultiTableEditor) Update(ctx *sql.Context, old sql.Row, new sql.Row) error {
	for _, secondary := range editor.secondaries {
		if err := secondary.Update(ctx, old, new); err != nil {
			return err
		}
	}
	return editor.primary.Update(ctx, old, new)
}

// Delete implements the interface TableEditor.
func (editor MultiTableEditor) Delete(ctx *sql.Context, row sql.Row) error {
	for _, secondary := range editor.secondaries {
		if err := secondary.Delete(ctx, row); err != nil {
			return err
		}
	}
	return editor.primary.Delete(ctx, row)
}

// IndexedAccess implements the interface ForeignKeyEditor.
func (editor MultiTableEditor) IndexedAccess(lookup sql.IndexLookup) sql.IndexedTable {
	return editor.primary.(sql.ForeignKeyEditor).IndexedAccess(lookup)
}

// GetIndexes implements the interface ForeignKeyEditor.
func (editor MultiTableEditor) GetIndexes(ctx *sql.Context) ([]sql.Index, error) {
	return editor.primary.(sql.ForeignKeyEditor).GetIndexes(ctx)
}

// SetAutoIncrementValue implements the interface AutoIncrementSetter.
func (editor MultiTableEditor) SetAutoIncrementValue(ctx *sql.Context, u uint64) error {
	return editor.primary.(sql.AutoIncrementSetter).SetAutoIncrementValue(ctx, u)
}

// Close implements the interface TableEditor.
func (editor MultiTableEditor) Close(ctx *sql.Context) error {
	var err error
	for _, secondary := range editor.secondaries {
		if nErr := secondary.Close(ctx); err == nil {
			err = nErr
		}
	}
	if nErr := editor.primary.Close(ctx); err == nil {
		err = nErr
	}
	return err
}

// GetDocIdColPos returns the column position of FTS_DOC_ID, along with a map that associates column names with their
// index. Returns -1 if the index was not found. All names in the map have been lowercased.
func GetDocIdColPos(sch sql.Schema) (docIdIndex int, parentColMap map[string]int, err error) {
	parentColMap = make(map[string]int)
	docIdIndex = -1
	for i, col := range sch {
		found, err := FullTextVerifyFtsDocIdCol(col)
		if err != nil {
			return -1, nil, err
		}
		if found {
			docIdIndex = i
		}
		// We'll write all of the columns and their positions to a map for reference later
		parentColMap[strings.ToLower(col.Name)] = i
		parentColMap[strings.ToLower(col.Source)+"."+strings.ToLower(col.Name)] = i
	}
	return docIdIndex, parentColMap, nil
}

// FullTextVerifyFtsDocIdCol verifies that the given column matches the expected format of the FTS_DOC_ID column.
// Returns false if the column is not the correct column. Returns true with an error if it is the correct column, but
// its definition is incorrect.
func FullTextVerifyFtsDocIdCol(col *sql.Column) (bool, error) {
	if strings.ToLower(col.Name) != IDColumnNameLowercase {
		return false, nil
	}
	// The column name must be all uppercase.
	// The type must be BIGINT UNSIGNED.
	// The column cannot allow NULL values.
	// The column allows DEFAULT values, but maybe it shouldn't?
	if col.Name != IDColumnName || !types.Uint64.Equals(col.Type) || col.Nullable {
		return true, sql.ErrInvalidFullTextDocIDColumn.New(col.Name)
	}
	return true, nil
}

// MakeCopyOfSchema returns a copy of the given schema with all collated fields replaced with the given collation, and
// all columns set to the source given.
func MakeCopyOfSchema(sch sql.Schema, source string, collation sql.CollationID) (newSch sql.Schema, err error) {
	newSch = sch.Copy()
	for _, col := range newSch {
		if collatedType, ok := col.Type.(sql.TypeWithCollation); ok {
			if col.Type, err = collatedType.WithNewCollation(collation); err != nil {
				return nil, err
			}
		}
		col.Source = source
	}
	return newSch, nil
}

// fulltextSchemaCompare compares two schemas to make sure that they're compatible. This is used to verify that the
// given Full-Text tables have the correct schemas. In practice, this shouldn't fail unless the integrator allows the
// user to modify the tables' schemas.
func fulltextSchemaCompare(ftTblName string, sch sql.Schema, expected sql.Schema) (err error) {
	if len(sch) != len(expected) {
		return fmt.Errorf("Full-Text table `%s` has an unexpected number of columns", ftTblName)
	}
	for i := range sch {
		col := *sch[i]
		expectedCol := *expected[i]
		// The expected schemas use the default collation, so we set them to the given column's collation for comparison
		if expectedColStrType, ok := expectedCol.Type.(sql.TypeWithCollation); ok {
			colStrType, ok := col.Type.(sql.TypeWithCollation)
			if !ok {
				return fmt.Errorf("Full-Text table `%s` has an incorrect type for the column `%s`", ftTblName, col.Name)
			}
			expectedCol.Type, err = expectedColStrType.WithNewCollation(colStrType.Collation())
			if err != nil {
				return err
			}
		}
		// We can't just use the Equals() function on the columns as they care about fields that we do not.
		if col.Name != expectedCol.Name || !col.Type.Equals(expectedCol.Type) || col.PrimaryKey != expectedCol.PrimaryKey || col.Nullable != expectedCol.Nullable ||
			col.AutoIncrement != expectedCol.AutoIncrement || col.Default != expectedCol.Default {
			return fmt.Errorf("Full-Text table `%s` column `%s` has an incorrect definition", ftTblName, col.Name)
		}
	}
	return nil
}
