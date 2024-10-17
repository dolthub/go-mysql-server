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

	"github.com/dolthub/go-mysql-server/sql/transform"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

// applyForeignKeys handles the application and resolution of foreign keys and their tables.
func applyForeignKeys(ctx *sql.Context, a *Analyzer, n sql.Node, scope *plan.Scope, sel RuleSelector, qFlags *sql.QueryFlags) (sql.Node, transform.TreeIdentity, error) {
	fkChecks, err := ctx.GetSessionVariable(ctx, "foreign_key_checks")
	if err != nil {
		return nil, transform.SameTree, err
	}
	if fkChecks.(int8) == 0 {
		return n, transform.SameTree, nil
	}
	return applyForeignKeysToNodes(ctx, a, n, newForeignKeyCache())
}

// applyForeignKeysToNodes handles the resolution and application of foreign key tables, along with recursive searching
// and caching of table editors.
func applyForeignKeysToNodes(ctx *sql.Context, a *Analyzer, n sql.Node, cache *foreignKeyCache) (sql.Node, transform.TreeIdentity, error) {
	var err error
	fkChain := newForeignKeyChain()

	switch n := n.(type) {
	case *plan.CreateTable:
		fkDefs := n.ForeignKeys()
		fkParentTbls := make([]sql.ForeignKeyTable, len(fkDefs))
		for i, fkDef := range fkDefs {
			// This should never happen, but ensure that foreign keys are declared on the table being created
			if n.Database().Name() != fkDef.Database || n.Name() != fkDef.Table {
				return nil, transform.SameTree, fmt.Errorf("foreign key definition has a different database/table name than the declaring table: `%s`.`%s`",
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
				return nil, transform.SameTree, err
			}
			fkParentTbl, ok := parentTbl.(sql.ForeignKeyTable)
			if !ok {
				return nil, transform.SameTree, sql.ErrNoForeignKeySupport.New(fkDef.ParentTable)
			}
			fkParentTbls[i] = fkParentTbl
		}
		n, err = n.WithParentForeignKeyTables(fkParentTbls)
		return n, transform.NewTree, err
	case *plan.InsertInto:
		if plan.IsEmptyTable(n.Destination) {
			return n, transform.SameTree, nil
		}
		insertableDest, err := plan.GetInsertable(n.Destination)
		if err != nil {
			return nil, transform.SameTree, err
		}
		tbl, ok := insertableDest.(sql.ForeignKeyTable)
		// If foreign keys aren't supported then we return
		if !ok {
			return n, transform.SameTree, nil
		}
		var fkEditor *plan.ForeignKeyEditor
		if n.IsReplace || len(n.OnDupExprs) > 0 {
			fkEditor, err = getForeignKeyEditor(ctx, a, tbl, cache, fkChain, true)
			if err != nil {
				return nil, transform.SameTree, err
			}
		} else {
			fkEditor, err = getForeignKeyReferences(ctx, a, tbl, cache, fkChain, true)
			if err != nil {
				return nil, transform.SameTree, err
			}
		}
		if fkEditor == nil {
			return n, transform.SameTree, nil
		}
		nn, err := n.WithChildren(&plan.ForeignKeyHandler{
			Table:        tbl,
			Sch:          insertableDest.Schema(),
			OriginalNode: n.Destination,
			Editor:       fkEditor,
			AllUpdaters:  fkChain.GetUpdaters(),
		})
		return nn, transform.NewTree, err
	case *plan.Update:
		if plan.IsEmptyTable(n.Child) {
			return n, transform.SameTree, nil
		}
		updateDest, err := plan.GetUpdatable(n.Child)
		if err != nil {
			return nil, transform.SameTree, err
		}
		fkTbl, ok := updateDest.(sql.ForeignKeyTable)
		// If foreign keys aren't supported then we return
		if !ok {
			return n, transform.SameTree, nil
		}

		fkEditor, err := getForeignKeyEditor(ctx, a, fkTbl, cache, fkChain, false)
		if err != nil {
			return nil, transform.SameTree, err
		}
		if fkEditor == nil {
			return n, transform.SameTree, nil
		}
		nn, err := n.WithChildren(&plan.ForeignKeyHandler{
			Table:        fkTbl,
			Sch:          updateDest.Schema(),
			OriginalNode: n.Child,
			Editor:       fkEditor,
			AllUpdaters:  fkChain.GetUpdaters(),
		})
		return nn, transform.NewTree, err
	case *plan.DeleteFrom:
		if plan.IsEmptyTable(n.Child) {
			return n, transform.SameTree, nil
		}

		targets := n.GetDeleteTargets()
		foreignKeyHandlers := make([]sql.Node, len(targets))
		copy(foreignKeyHandlers, targets)

		for i, node := range targets {
			deleteDest, err := plan.GetDeletable(node)
			if err != nil {
				return nil, transform.SameTree, err
			}

			tbl, ok := deleteDest.(sql.ForeignKeyTable)
			// If foreign keys aren't supported then check the next node
			if !ok {
				continue
			}
			fkEditor, err := getForeignKeyRefActions(ctx, a, tbl, cache, fkChain, nil, false)
			if err != nil {
				return nil, transform.SameTree, err
			}
			if fkEditor == nil {
				continue
			}

			foreignKeyHandlers[i] = &plan.ForeignKeyHandler{
				Table:        tbl,
				Sch:          deleteDest.Schema(),
				OriginalNode: targets[i],
				Editor:       fkEditor,
				AllUpdaters:  fkChain.GetUpdaters(),
			}
		}
		if n.HasExplicitTargets() {
			return n.WithExplicitTargets(foreignKeyHandlers), transform.NewTree, nil
		} else {
			newNode, err := n.WithChildren(foreignKeyHandlers...)
			if err != nil {
				return nil, transform.SameTree, err
			}
			return newNode, transform.NewTree, nil
		}
	case *plan.RowUpdateAccumulator:
		children := n.Children()
		newChildren := make([]sql.Node, len(children))
		treeIdentity := transform.SameTree
		for i, child := range children {
			newIdentity := transform.SameTree
			newChildren[i], newIdentity, err = applyForeignKeysToNodes(ctx, a, child, cache)
			if err != nil {
				return nil, transform.SameTree, err
			}
			if newIdentity == transform.NewTree {
				treeIdentity = transform.NewTree
			}
		}
		nn, err := n.WithChildren(newChildren...)
		return nn, treeIdentity, err
	default:
		return n, transform.SameTree, nil
	}
}

// getForeignKeyEditor merges both getForeignKeyReferences and getForeignKeyRefActions and returns a single editor.
func getForeignKeyEditor(ctx *sql.Context, a *Analyzer, tbl sql.ForeignKeyTable, cache *foreignKeyCache, fkChain foreignKeyChain, checkRows bool) (*plan.ForeignKeyEditor, error) {
	fkEditor, err := getForeignKeyReferences(ctx, a, tbl, cache, fkChain, checkRows)
	if err != nil {
		return nil, err
	}
	fkEditor, err = getForeignKeyRefActions(ctx, a, tbl, cache, fkChain, fkEditor, checkRows)
	if err != nil {
		return nil, err
	}
	return fkEditor, err
}

// getForeignKeyReferences returns an editor containing only the references for the given table.
func getForeignKeyReferences(ctx *sql.Context, a *Analyzer, tbl sql.ForeignKeyTable, cache *foreignKeyCache, fkChain foreignKeyChain, checkRows bool) (*plan.ForeignKeyEditor, error) {
	var updater sql.ForeignKeyEditor
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
	// Tables do not include their database. As a workaround, we'll use the first foreign key to tell us the database.
	updater, err = cache.AddUpdater(ctx, tbl, fks[0].Database, fks[0].Table)
	if err != nil {
		return nil, err
	}
	fkChain = fkChain.AddTable(fks[0].Database, fks[0].Table).AddTableUpdater(fks[0].Database, fks[0].Table, updater)

	tblSch := tbl.Schema()
	fkEditor := &plan.ForeignKeyEditor{
		Schema:     tblSch,
		Editor:     updater,
		References: make([]*plan.ForeignKeyReferenceHandler, len(fks)),
		RefActions: nil,
		Cyclical:   false,
	}
	for i, fk := range fks {
		parentTbl, parentUpdater, err := cache.GetUpdater(ctx, a, fk.ParentDatabase, fk.ParentTable)
		if err != nil {
			return nil, sql.ErrForeignKeyNotResolved.New(fk.Database, fk.Table, fk.Name,
				strings.Join(fk.Columns, "`, `"), fk.ParentTable, strings.Join(fk.ParentColumns, "`, `"))
		}

		// Resolve the foreign key if it has not been resolved yet
		if !fk.IsResolved {
			err = plan.ResolveForeignKey(ctx, tbl, parentTbl, fk, false, true, checkRows)
			if err != nil {
				return nil, sql.ErrForeignKeyNotResolved.New(fk.Database, fk.Table, fk.Name,
					strings.Join(fk.Columns, "`, `"), fk.ParentTable, strings.Join(fk.ParentColumns, "`, `"))
			}
		}

		parentIndex, ok, err := plan.FindFKIndexWithPrefix(ctx, parentTbl, fk.ParentColumns, true)
		if err != nil {
			return nil, err
		}
		if !ok {
			// If this error is returned, it is due to an index deletion not properly checking for foreign key usage
			return nil, sql.ErrForeignKeyNotResolved.New(fk.Database, fk.Table, fk.Name,
				strings.Join(fk.Columns, "`, `"), fk.ParentTable, strings.Join(fk.ParentColumns, "`, `"))
		}
		indexPositions, appendTypes, err := plan.FindForeignKeyColMapping(ctx, fk.Name, tbl, fk.Columns, fk.ParentColumns, parentIndex)
		if err != nil {
			return nil, err
		}
		var selfCols map[string]int
		if fk.IsSelfReferential() {
			selfCols = make(map[string]int)
			for i, col := range tblSch {
				selfCols[strings.ToLower(col.Name)] = i
			}
		}
		fkEditor.References[i] = &plan.ForeignKeyReferenceHandler{
			ForeignKey: fk,
			SelfCols:   selfCols,
			RowMapper: plan.ForeignKeyRowMapper{
				Index:          parentIndex,
				Updater:        parentUpdater,
				SourceSch:      tblSch,
				IndexPositions: indexPositions,
				AppendTypes:    appendTypes,
			},
		}
	}
	return fkEditor, nil
}

// getForeignKeyRefActions adds referential actions to enforce on the given table. If this is being called after
// getForeignKeyReferences, then that foreign key editor should be passed in. Otherwise, nil should be passed in.
// This also handles caching of the foreign key editor (when the editor does not have any references).
func getForeignKeyRefActions(ctx *sql.Context, a *Analyzer, tbl sql.ForeignKeyTable, cache *foreignKeyCache, fkChain foreignKeyChain, fkEditor *plan.ForeignKeyEditor, checkRows bool) (*plan.ForeignKeyEditor, error) {
	fks, err := tbl.GetReferencedForeignKeys(ctx)
	if err != nil {
		return nil, err
	}
	// Return early if there are no foreign keys that reference the table
	if len(fks) == 0 {
		return fkEditor, nil
	}

	// Check if we already have an editor that we can reuse. If we can, we'll return that instead.
	// Tables do not include their database. As a workaround, we'll use the first foreign key to tell us the database.
	cachedFkEditor := cache.GetEditor(fkEditor, fks[0].ParentDatabase, fks[0].ParentTable)
	if cachedFkEditor != nil {
		// Reusing an editor means that we've hit a cycle, so we update the cached editor.
		cachedFkEditor.Cyclical = true
		return cachedFkEditor, nil
	}
	// No matching editor was cached, so we either create a new one or add to the existing one
	tblSch := tbl.Schema()
	if fkEditor == nil {
		fkEditor = &plan.ForeignKeyEditor{
			Schema:     tblSch,
			Editor:     nil,
			References: nil,
			RefActions: make([]plan.ForeignKeyRefActionData, len(fks)),
			Cyclical:   false,
		}
		fkEditor.Editor, err = cache.AddUpdater(ctx, tbl, fks[0].ParentDatabase, fks[0].ParentTable)
		if err != nil {
			return nil, err
		}
	} else {
		// The editor has already been created, so we need to create the referential actions array
		fkEditor.RefActions = make([]plan.ForeignKeyRefActionData, len(fks))
	}
	// Add the editor to the cache
	cache.AddEditor(fkEditor, fks[0].ParentDatabase, fks[0].ParentTable)
	// Ensure that the chain has the table and updater
	fkChain = fkChain.AddTable(fks[0].ParentDatabase, fks[0].ParentTable).AddTableUpdater(fks[0].ParentDatabase, fks[0].ParentTable, fkEditor.Editor)

	for i, fk := range fks {
		childTbl, childUpdater, err := cache.GetUpdater(ctx, a, fk.Database, fk.Table)
		if err != nil {
			return nil, sql.ErrForeignKeyNotResolved.New(fk.Database, fk.Table, fk.Name,
				strings.Join(fk.Columns, "`, `"), fk.ParentTable, strings.Join(fk.ParentColumns, "`, `"))
		}
		// If either referential action is not equivalent to RESTRICT, then the updater has the possibility of having
		// its contents modified, therefore we add it to the chain.
		if !fk.OnUpdate.IsEquivalentToRestrict() || !fk.OnDelete.IsEquivalentToRestrict() {
			fkChain = fkChain.AddTableUpdater(fk.Database, fk.Table, childUpdater)
		}

		// Resolve the foreign key if it has not been resolved yet
		if !fk.IsResolved {
			err = plan.ResolveForeignKey(ctx, childTbl, tbl, fk, false, true, checkRows)
			if err != nil {
				return nil, sql.ErrForeignKeyNotResolved.New(fk.Database, fk.Table, fk.Name,
					strings.Join(fk.Columns, "`, `"), fk.ParentTable, strings.Join(fk.ParentColumns, "`, `"))
			}
		}

		childIndex, ok, err := plan.FindFKIndexWithPrefix(ctx, childTbl, fk.Columns, false)
		if err != nil {
			return nil, err
		}
		if !ok {
			// If this error is returned, it is due to an index deletion not properly checking for foreign key usage
			return nil, sql.ErrForeignKeyNotResolved.New(fk.Database, fk.Table, fk.Name,
				strings.Join(fk.Columns, "`, `"), fk.ParentTable, strings.Join(fk.ParentColumns, "`, `"))
		}
		indexPositions, appendTypes, err := plan.FindForeignKeyColMapping(ctx, fk.Name, tbl, fk.ParentColumns, fk.Columns, childIndex)
		if err != nil {
			return nil, err
		}
		childTblSch := childTbl.Schema()
		childParentMapping, err := plan.GetChildParentMapping(tblSch, childTblSch, fk)
		if err != nil {
			return nil, err
		}

		childEditor, err := getForeignKeyEditor(ctx, a, childTbl, cache, fkChain.AddForeignKey(fk.Name), checkRows)
		if err != nil {
			return nil, err
		}
		// May return nil if we recursively loop onto a foreign key previously declared
		if childEditor == nil {
			childEditor = &plan.ForeignKeyEditor{
				Schema:     childTblSch,
				Editor:     childUpdater,
				References: nil,
				RefActions: nil,
				Cyclical:   false,
			}
		}
		// If a child editor is cyclical, then this editor is a part of that cycle
		fkEditor.Cyclical = fkEditor.Cyclical || childEditor.Cyclical
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

func newForeignKeyTableName(dbName, tblName string) foreignKeyTableName {
	return foreignKeyTableName{
		dbName:  strings.ToLower(dbName),
		tblName: strings.ToLower(tblName),
	}
}

// foreignKeyTableUpdater is a foreign key table along with its updater.
type foreignKeyTableUpdater struct {
	tbl     sql.ForeignKeyTable
	updater sql.ForeignKeyEditor
}

// foreignKeyCache is a cache of updaters and editors for foreign keys.
type foreignKeyCache struct {
	updaterCache map[foreignKeyTableName]foreignKeyTableUpdater
	editorsCache map[foreignKeyTableName][]*plan.ForeignKeyEditor
}

// newForeignKeyCache returns a new *foreignKeyCache.
func newForeignKeyCache() *foreignKeyCache {
	return &foreignKeyCache{
		updaterCache: make(map[foreignKeyTableName]foreignKeyTableUpdater),
		editorsCache: make(map[foreignKeyTableName][]*plan.ForeignKeyEditor),
	}
}

// AddUpdater will add the given foreign key table (and updater) to the cache and returns its updater. If it already
// exists, it is not added, and instead the cached updater is returned. This is so that the same updater is referenced
// by all foreign key instances.
func (cache *foreignKeyCache) AddUpdater(ctx *sql.Context, tbl sql.ForeignKeyTable, dbName string, tblName string) (sql.ForeignKeyEditor, error) {
	fkTableName := foreignKeyTableName{
		dbName:  strings.ToLower(dbName),
		tblName: strings.ToLower(tblName),
	}
	if cachedEditor, ok := cache.updaterCache[fkTableName]; ok {
		return cachedEditor.updater, nil
	}
	editor := foreignKeyTableUpdater{
		tbl:     tbl,
		updater: tbl.GetForeignKeyEditor(ctx),
	}
	cache.updaterCache[fkTableName] = editor
	return editor.updater, nil
}

// AddEditor will add the given foreign key editor to the cache. Does not validate that the editor is unique, therefore
// GetEditor should be called before this function.
func (cache *foreignKeyCache) AddEditor(editor *plan.ForeignKeyEditor, dbName string, tblName string) {
	if editor == nil {
		panic("cannot pass in nil editor") // Should never be hit
	}
	fkTableName := foreignKeyTableName{
		dbName:  strings.ToLower(dbName),
		tblName: strings.ToLower(tblName),
	}
	cache.editorsCache[fkTableName] = append(cache.editorsCache[fkTableName], editor)
}

// GetUpdater returns the given foreign key table updater.
func (cache *foreignKeyCache) GetUpdater(ctx *sql.Context, a *Analyzer, dbName string, tblName string) (sql.ForeignKeyTable, sql.ForeignKeyEditor, error) {
	fkTableName := foreignKeyTableName{
		dbName:  strings.ToLower(dbName),
		tblName: strings.ToLower(tblName),
	}
	if fkTblEditor, ok := cache.updaterCache[fkTableName]; ok {
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
		updater: fkTbl.GetForeignKeyEditor(ctx),
	}
	cache.updaterCache[fkTableName] = editor
	return editor.tbl, editor.updater, nil
}

// GetEditor returns a foreign key editor that matches the given editor in all ways except for the referential actions.
// Returns nil if no such editors have been cached.
func (cache *foreignKeyCache) GetEditor(fkEditor *plan.ForeignKeyEditor, dbName string, tblName string) *plan.ForeignKeyEditor {
	fkTableName := foreignKeyTableName{
		dbName:  strings.ToLower(dbName),
		tblName: strings.ToLower(tblName),
	}
	// It is safe to assume that the index and schema will match for a table that has the same name on the same database,
	// so we only need to check that the references match. As long as they refer to the same foreign key, they should
	// match, so we only need to check the names.
	for _, cachedEditor := range cache.editorsCache[fkTableName] {
		if fkEditor == nil {
			if len(cachedEditor.References) == 0 {
				return cachedEditor
			} else {
				continue
			}
		}
		if len(fkEditor.References) != len(cachedEditor.References) {
			continue
		}
		for i := range fkEditor.References {
			if fkEditor.References[i].ForeignKey.Name != cachedEditor.References[i].ForeignKey.Name {
				continue
			}
		}
		return cachedEditor
	}
	return nil
}

// foreignKeyChain holds all previously used foreign keys and modified tables in the chain. Also keeps track of all
// updaters that have been used in the chain. This differs from the updaters in the cache, as the cache may contain
// updaters that are not a part of this chain. In addition, any updaters that cannot be modified (such as those
// belonging to strictly RESTRICT referential actions) will not appear in the chain.
type foreignKeyChain struct {
	fkNames    map[string]struct{}
	fkTables   map[foreignKeyTableName]struct{}
	fkUpdaters map[foreignKeyTableName]sql.ForeignKeyEditor
}

func newForeignKeyChain() foreignKeyChain {
	return foreignKeyChain{
		fkNames:    make(map[string]struct{}),
		fkTables:   make(map[foreignKeyTableName]struct{}),
		fkUpdaters: make(map[foreignKeyTableName]sql.ForeignKeyEditor),
	}
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
	newFkTables[newForeignKeyTableName(dbName, tblName)] = struct{}{}
	return foreignKeyChain{
		fkNames:    newFkNames,
		fkTables:   newFkTables,
		fkUpdaters: chain.fkUpdaters,
	}
}

// AddTableUpdater returns a new chain with the added foreign key updater.
func (chain foreignKeyChain) AddTableUpdater(dbName string, tblName string, fkUpdater sql.ForeignKeyEditor) foreignKeyChain {
	chain.fkUpdaters[newForeignKeyTableName(dbName, tblName)] = fkUpdater
	return chain
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
		fkNames:    newFkNames,
		fkTables:   newFkTables,
		fkUpdaters: chain.fkUpdaters,
	}
}

// HasTable returns whether the chain contains the given table. Case-insensitive.
func (chain foreignKeyChain) HasTable(dbName string, tblName string) bool {
	_, ok := chain.fkTables[newForeignKeyTableName(dbName, tblName)]
	return ok
}

// HasForeignKey returns whether the chain contains the given foreign key. Case-insensitive.
func (chain foreignKeyChain) HasForeignKey(fkName string) bool {
	_, ok := chain.fkNames[strings.ToLower(fkName)]
	return ok
}

// GetUpdaters returns all foreign key updaters that have been added to the chain.
func (chain foreignKeyChain) GetUpdaters() []sql.ForeignKeyEditor {
	updaters := make([]sql.ForeignKeyEditor, 0, len(chain.fkUpdaters))
	for _, updater := range chain.fkUpdaters {
		updaters = append(updaters, updater)
	}
	return updaters
}
