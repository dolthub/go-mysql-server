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

package analyzer

import (
	"fmt"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

// applyForeignKeys handles the application and resolution of foreign keys and their tables.
func applyForeignKeys(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	fkChecks, err := ctx.GetSessionVariable(ctx, "foreign_key_checks")
	if err != nil {
		return nil, err
	}
	if fkChecks.(int8) == 0 {
		return n, nil
	}
	return applyForeignKeysToNodes(ctx, a, n, newForeignKeyTableCache())
}

// applyForeignKeysToNodes handles the resolution and application of foreign key tables, along with recursive searching
// and caching of table editors.
//TODO: handle cycle detection for CASCADEs          https://dev.mysql.com/doc/refman/8.0/en/ansi-diff-foreign-keys.html
func applyForeignKeysToNodes(ctx *sql.Context, a *Analyzer, n sql.Node, cache *foreignKeyTableCache) (sql.Node, error) {
	fkChecks, err := ctx.GetSessionVariable(ctx, "foreign_key_checks")
	if err != nil {
		return nil, err
	}
	if fkChecks.(int8) == 0 {
		return n, nil
	}

	switch n := n.(type) {
	case *plan.CreateTable:
		fkDefs := n.ForeignKeys()
		fkParentTbls := make([]sql.ForeignKeyTable, len(fkDefs))
		for i, fkDef := range fkDefs {
			// This should never happen, but ensure that foreign keys are declared on the table being created
			if n.Database().Name() != fkDef.Database || n.Name() != fkDef.Table {
				return nil, fmt.Errorf("foreign key definition has a different database/table name than the declaring table: `%s`.`%s`",
					fkDef.Database, fkDef.Table)
			}
			// If the foreign key is self-referential then the table won't exist yet, so we put a nil here.
			// CreateTable knows to interpret all nil tables as the newly-created table.
			if fkDef.IsSelfReferential() {
				fkParentTbls[i] = nil
				continue
			}
			parentTbl, _, err := a.Catalog.Table(ctx, fkDef.ParentDatabase, fkDef.ParentTable)
			if err != nil {
				return nil, err
			}
			fkParentTbl, ok := parentTbl.(sql.ForeignKeyTable)
			if !ok {
				return nil, sql.ErrNoForeignKeySupport.New(fkDef.ParentTable)
			}
			fkParentTbls[i] = fkParentTbl
		}
		return n.WithParentForeignKeyTables(fkParentTbls)
	case *plan.InsertInto:
		if n.Destination == plan.EmptyTable {
			return n, nil
		}
		insertableDest, err := plan.GetInsertable(n.Destination)
		if err != nil {
			return nil, err
		}
		tbl, ok := insertableDest.(sql.ForeignKeyTable)
		// If foreign keys aren't supported then we return
		if !ok {
			return n, nil
		}
		var fkEditor *plan.ForeignKeyEditor
		if n.IsReplace {
			fkEditor, err = getForeignKeyEditor(ctx, a, tbl, cache, foreignKeyChain{})
			if err != nil {
				return nil, err
			}
		} else {
			fkEditor, err = getForeignKeyReferences(ctx, a, tbl, cache, foreignKeyChain{})
			if err != nil {
				return nil, err
			}
		}
		if fkEditor == nil {
			return n, nil
		}
		return n.WithChildren(&plan.ForeignKeyHandler{
			Table:        tbl,
			Sch:          insertableDest.Schema(),
			OriginalNode: n.Destination,
			Editor:       fkEditor,
		})
	case *plan.Update:
		if n.Child == plan.EmptyTable {
			return n, nil
		}
		updateDest, err := plan.GetUpdatable(n.Child)
		if err != nil {
			return nil, err
		}
		tbl, ok := updateDest.(sql.ForeignKeyTable)
		// If foreign keys aren't supported then we return
		if !ok {
			return n, nil
		}
		fkEditor, err := getForeignKeyEditor(ctx, a, tbl, cache, foreignKeyChain{})
		if err != nil {
			return nil, err
		}
		if fkEditor == nil {
			return n, nil
		}
		return n.WithChildren(&plan.ForeignKeyHandler{
			Table:        tbl,
			Sch:          updateDest.Schema(),
			OriginalNode: n.Child,
			Editor:       fkEditor,
		})
	case *plan.DeleteFrom:
		if n.Child == plan.EmptyTable {
			return n, nil
		}
		deleteDest, err := plan.GetDeletable(n.Child)
		if err != nil {
			return nil, err
		}
		tbl, ok := deleteDest.(sql.ForeignKeyTable)
		// If foreign keys aren't supported then we return
		if !ok {
			return n, nil
		}
		fkEditor, err := getForeignKeyRefActions(ctx, a, tbl, cache, foreignKeyChain{})
		if err != nil {
			return nil, err
		}
		if fkEditor == nil {
			return n, nil
		}
		return n.WithChildren(&plan.ForeignKeyHandler{
			Table:        tbl,
			Sch:          deleteDest.Schema(),
			OriginalNode: n.Child,
			Editor:       fkEditor,
		})
	case *plan.RowUpdateAccumulator:
		children := n.Children()
		newChildren := make([]sql.Node, len(children))
		for i, child := range children {
			newChildren[i], err = applyForeignKeysToNodes(ctx, a, child, cache)
			if err != nil {
				return nil, err
			}
		}
		return n.WithChildren(newChildren...)
	default:
		return n, nil
	}
}

// getForeignKeyEditor merges both getForeignKeyReferences and getForeignKeyRefActions and returns a single editor.
func getForeignKeyEditor(ctx *sql.Context, a *Analyzer, tbl sql.ForeignKeyTable, cache *foreignKeyTableCache, fkChain foreignKeyChain) (*plan.ForeignKeyEditor, error) {
	fkEditor, err := getForeignKeyReferences(ctx, a, tbl, cache, fkChain)
	if err != nil {
		return nil, err
	}
	fkEditor2, err := getForeignKeyRefActions(ctx, a, tbl, cache, fkChain)
	if err != nil {
		return nil, err
	}
	// Due to tables not containing their database, we have to grab the table's database from the foreign key definition.
	// This is so we may make proper use of cached updaters. Therefore, both calls return a full editor rather than just
	// their respective specialties (and may be used independently).
	//TODO: once we add a Database() function to sql.Table, this should be updated
	if fkEditor == nil && fkEditor2 == nil {
		return nil, nil
	} else if fkEditor != nil && fkEditor2 != nil {
		fkEditor.RefActions = fkEditor2.RefActions
		return fkEditor, nil
	} else if fkEditor != nil {
		return fkEditor, nil
	} else {
		return fkEditor2, nil
	}
}

// getForeignKeyReferences returns an editor containing only the references for the given table.
func getForeignKeyReferences(ctx *sql.Context, a *Analyzer, tbl sql.ForeignKeyTable, cache *foreignKeyTableCache, fkChain foreignKeyChain) (*plan.ForeignKeyEditor, error) {
	var editor sql.ForeignKeyUpdater
	fks, err := tbl.GetDeclaredForeignKeys(ctx)
	if err != nil {
		return nil, err
	}
	// We can ignore foreign keys that have been previously used as we can guarantee the parent table has the referenced data
	{
		newFks := make([]sql.ForeignKeyConstraint, 0, len(fks))
		for _, fk := range fks {
			if !fkChain.HasForeignKey(fk.Name) {
				newFks = append(newFks, fk)
			}
		}
		fks = newFks
	}
	// If there are no foreign keys (or we've ignored them all) then we can skip the rest of this
	if len(fks) == 0 {
		return nil, nil
	}

	fkEditor := &plan.ForeignKeyEditor{
		Schema:     tbl.Schema(),
		Editor:     nil,
		References: make([]*plan.ForeignKeyReferenceHandler, len(fks)),
		RefActions: nil,
	}
	for i, fk := range fks {
		// Tables do not include their database. As a workaround, we'll use one of foreign keys to tell us the database.
		if editor == nil {
			editor, err = cache.Add(ctx, tbl, fk.Database, fk.Table)
			if err != nil {
				return nil, err
			}
			fkEditor.Editor = editor
			fkChain = fkChain.AddTable(fk.ParentDatabase, fk.ParentTable)
		}

		parentTbl, parentUpdater, err := cache.Get(ctx, a, fk.ParentDatabase, fk.ParentTable)
		if err != nil {
			return nil, sql.ErrForeignKeyNotResolved.New(fk.Database, fk.Table, fk.Name,
				strings.Join(fk.Columns, "`, `"), fk.ParentTable, strings.Join(fk.ParentColumns, "`, `"))
		}

		// Resolve the foreign key if it has not been resolved yet
		if !fk.IsResolved {
			err = plan.ResolveForeignKey(ctx, tbl, parentTbl, fk, false)
			if err != nil {
				return nil, sql.ErrForeignKeyNotResolved.New(fk.Database, fk.Table, fk.Name,
					strings.Join(fk.Columns, "`, `"), fk.ParentTable, strings.Join(fk.ParentColumns, "`, `"))
			}
		}

		parentIndex, ok, err := plan.FindIndexWithPrefix(ctx, parentTbl, fk.ParentColumns)
		if err != nil {
			return nil, err
		}
		if !ok {
			// If this error is returned, it is due to an index deletion not properly checking for foreign key usage
			return nil, sql.ErrForeignKeyNotResolved.New(fk.Database, fk.Table, fk.Name,
				strings.Join(fk.Columns, "`, `"), fk.ParentTable, strings.Join(fk.ParentColumns, "`, `"))
		}
		indexPositions, appendTypes, err := plan.FindForeignKeyColMapping(ctx, fk.Name, tbl, fk.Columns,
			fk.ParentColumns, parentIndex)
		if err != nil {
			return nil, err
		}
		var selfCols map[string]int
		if fk.IsSelfReferential() {
			selfCols = make(map[string]int)
			for i, col := range tbl.Schema() {
				selfCols[strings.ToLower(col.Name)] = i
			}
		}
		fkEditor.References[i] = &plan.ForeignKeyReferenceHandler{
			ForeignKey: fk,
			SelfCols:   selfCols,
			RowMapper: plan.ForeignKeyRowMapper{
				Index:          parentIndex,
				Updater:        parentUpdater,
				SourceSch:      tbl.Schema(),
				IndexPositions: indexPositions,
				AppendTypes:    appendTypes,
			},
		}
	}
	return fkEditor, nil
}

// getForeignKeyRefActions returns an editor containing only the referential actions to enforce on the given table.
func getForeignKeyRefActions(ctx *sql.Context, a *Analyzer, tbl sql.ForeignKeyTable, cache *foreignKeyTableCache, fkChain foreignKeyChain) (*plan.ForeignKeyEditor, error) {
	var editor sql.ForeignKeyUpdater
	fks, err := tbl.GetReferencedForeignKeys(ctx)
	if err != nil {
		return nil, err
	}
	// MySQL has a CASCADE limit of a depth of 15
	//TODO: figure out if MySQL errors once 15 has been hit, or if it just stops cascading (if the former, add a node that always errors)
	if len(fks) == 0 || fkChain.Count() >= 15 {
		return nil, nil
	}

	tblSch := tbl.Schema()
	fkEditor := &plan.ForeignKeyEditor{
		Schema:     tbl.Schema(),
		Editor:     nil,
		References: nil,
		RefActions: make([]plan.ForeignKeyRefActionData, len(fks)),
	}
	for i, fk := range fks {
		// Tables do not include their database. As a workaround, we'll use one of foreign keys to tell us the database.
		if editor == nil {
			editor, err = cache.Add(ctx, tbl, fk.ParentDatabase, fk.ParentTable)
			if err != nil {
				return nil, err
			}
			fkEditor.Editor = editor
			fkChain = fkChain.AddTable(fk.ParentDatabase, fk.ParentTable)
		}

		childTbl, childUpdater, err := cache.Get(ctx, a, fk.Database, fk.Table)
		if err != nil {
			return nil, sql.ErrForeignKeyNotResolved.New(fk.Database, fk.Table, fk.Name,
				strings.Join(fk.Columns, "`, `"), fk.ParentTable, strings.Join(fk.ParentColumns, "`, `"))
		}

		// Resolve the foreign key if it has not been resolved yet
		if !fk.IsResolved {
			err = plan.ResolveForeignKey(ctx, childTbl, tbl, fk, false)
			if err != nil {
				return nil, sql.ErrForeignKeyNotResolved.New(fk.Database, fk.Table, fk.Name,
					strings.Join(fk.Columns, "`, `"), fk.ParentTable, strings.Join(fk.ParentColumns, "`, `"))
			}
		}

		childIndex, ok, err := plan.FindIndexWithPrefix(ctx, childTbl, fk.Columns)
		if err != nil {
			return nil, err
		}
		if !ok {
			// If this error is returned, it is due to an index deletion not properly checking for foreign key usage
			return nil, sql.ErrForeignKeyNotResolved.New(fk.Database, fk.Table, fk.Name,
				strings.Join(fk.Columns, "`, `"), fk.ParentTable, strings.Join(fk.ParentColumns, "`, `"))
		}
		indexPositions, appendTypes, err := plan.FindForeignKeyColMapping(ctx, fk.Name, tbl, fk.ParentColumns,
			fk.Columns, childIndex)
		if err != nil {
			return nil, err
		}
		childParentMapping, err := plan.GetChildParentMapping(tblSch, childTbl.Schema(), fk)
		if err != nil {
			return nil, err
		}

		childEditor, err := getForeignKeyEditor(ctx, a, childTbl, cache, fkChain.AddForeignKey(fk.Name))
		if err != nil {
			return nil, err
		}
		// May return nil if we recursively loop onto a foreign key previously declared
		if childEditor == nil {
			childEditor = &plan.ForeignKeyEditor{
				Schema:     childTbl.Schema(),
				Editor:     childUpdater,
				References: nil,
				RefActions: nil,
			}
		}
		// If "ON UPDATE CASCADE" or "ON UPDATE SET NULL" recurses onto the same table that has been previously updated
		// in the same cascade then it's treated like a RESTRICT (does not apply to "ON DELETE")
		if fkChain.HasTable(fk.Database, fk.Table) {
			fk.OnUpdate = sql.ForeignKeyReferentialAction_Restrict
		}
		fkEditor.RefActions[i] = plan.ForeignKeyRefActionData{
			RowMapper: &plan.ForeignKeyRowMapper{
				Index:          childIndex,
				Updater:        childUpdater,
				SourceSch:      tblSch,
				IndexPositions: indexPositions,
				AppendTypes:    appendTypes,
			},
			Editor:             childEditor,
			ForeignKey:         fk,
			ChildParentMapping: childParentMapping,
		}
	}
	return fkEditor, nil
}

// foreignKeyTableName is the combination of a table's database along with their name, both lowercased.
type foreignKeyTableName struct {
	dbName  string
	tblName string
}

// foreignKeyTableUpdater is a foreign key table along with its updater.
type foreignKeyTableUpdater struct {
	tbl     sql.ForeignKeyTable
	updater sql.ForeignKeyUpdater
}

// foreignKeyTableCache is a cache of table editors for foreign keys.
type foreignKeyTableCache struct {
	cache map[foreignKeyTableName]foreignKeyTableUpdater
}

// newForeignKeyTableCache returns a new *foreignKeyTableCache.
func newForeignKeyTableCache() *foreignKeyTableCache {
	return &foreignKeyTableCache{
		cache: make(map[foreignKeyTableName]foreignKeyTableUpdater),
	}
}

// Add will add the given foreign key table (and updater) to the cache and returns its updater. If it already exists, it
// is not added, and instead the cached updater is returned. This is so that the same updater is referenced by all
// foreign key instances.
func (cache *foreignKeyTableCache) Add(ctx *sql.Context, tbl sql.ForeignKeyTable, dbName string, tblName string) (sql.ForeignKeyUpdater, error) {
	fkTableName := foreignKeyTableName{
		dbName:  strings.ToLower(dbName),
		tblName: strings.ToLower(tblName),
	}
	if cachedEditor, ok := cache.cache[fkTableName]; ok {
		return cachedEditor.updater, nil
	}
	editor := foreignKeyTableUpdater{
		tbl:     tbl,
		updater: tbl.GetForeignKeyUpdater(ctx),
	}
	cache.cache[fkTableName] = editor
	return editor.updater, nil
}

// Get returns the given foreign key table updater.
func (cache *foreignKeyTableCache) Get(ctx *sql.Context, a *Analyzer, dbName string, tblName string) (sql.ForeignKeyTable, sql.ForeignKeyUpdater, error) {
	fkTableName := foreignKeyTableName{
		dbName:  strings.ToLower(dbName),
		tblName: strings.ToLower(tblName),
	}
	if fkTblEditor, ok := cache.cache[fkTableName]; ok {
		return fkTblEditor.tbl, fkTblEditor.updater, nil
	}
	tbl, _, err := a.Catalog.Table(ctx, dbName, tblName)
	if err != nil {
		return nil, nil, err
	}
	fkTbl, ok := tbl.(sql.ForeignKeyTable)
	if !ok {
		return nil, nil, sql.ErrNoForeignKeySupport.New(tblName)
	}
	editor := foreignKeyTableUpdater{
		tbl:     fkTbl,
		updater: fkTbl.GetForeignKeyUpdater(ctx),
	}
	cache.cache[fkTableName] = editor
	return editor.tbl, editor.updater, nil
}

// foreignKeyChain holds all previously used foreign keys and modified tables in the chain. Also keeps track of how many
// times a foreign key was added (or attempted to be added). This is all used for cycle detection.
type foreignKeyChain struct {
	fkNames  map[string]struct{}
	fkTables map[foreignKeyTableName]struct{}
	count    int
}

// AddTable returns a new chain with the added table.
func (chain foreignKeyChain) AddTable(dbName string, tblName string) foreignKeyChain {
	newFkNames := make(map[string]struct{})
	newFkTables := make(map[foreignKeyTableName]struct{})
	for fkName := range chain.fkNames {
		newFkNames[fkName] = struct{}{}
	}
	for fkTable := range chain.fkTables {
		newFkTables[fkTable] = struct{}{}
	}
	newFkTables[foreignKeyTableName{
		dbName:  strings.ToLower(dbName),
		tblName: strings.ToLower(tblName),
	}] = struct{}{}
	return foreignKeyChain{
		fkNames:  newFkNames,
		fkTables: newFkTables,
		count:    chain.count,
	}
}

// AddForeignKey returns a new chain with the added foreign key.
func (chain foreignKeyChain) AddForeignKey(fkName string) foreignKeyChain {
	newFkNames := make(map[string]struct{})
	newFkTables := make(map[foreignKeyTableName]struct{})
	for fkName := range chain.fkNames {
		newFkNames[fkName] = struct{}{}
	}
	for fkTable := range chain.fkTables {
		newFkTables[fkTable] = struct{}{}
	}
	newFkNames[strings.ToLower(fkName)] = struct{}{}
	return foreignKeyChain{
		fkNames:  newFkNames,
		fkTables: newFkTables,
		count:    chain.count + 1,
	}
}

// HasTable returns whether the chain contains the given table. Case-insensitive.
func (chain foreignKeyChain) HasTable(dbName string, tblName string) bool {
	if _, ok := chain.fkTables[foreignKeyTableName{
		dbName:  strings.ToLower(dbName),
		tblName: strings.ToLower(tblName),
	}]; ok {
		return true
	}
	return false
}

// HasForeignKey returns whether the chain contains the given foreign key. Case-insensitive.
func (chain foreignKeyChain) HasForeignKey(fkName string) bool {
	if _, ok := chain.fkNames[strings.ToLower(fkName)]; ok {
		return true
	}
	return false
}

// Count returns how many times a foreign key has been added (or an attempt was made). This is representative of the
// referential action depth.
func (chain foreignKeyChain) Count() int {
	return chain.count
}
