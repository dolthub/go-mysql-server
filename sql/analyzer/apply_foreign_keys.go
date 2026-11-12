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
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/planbuilder"
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

			parentTbl, _, err := a.Catalog.TableSchema(ctx, fkDef.ParentDatabase, fkDef.ParentSchema, fkDef.ParentTable)
			if err != nil {
				return nil, transform.SameTree, err
			}

			// If we are working with a schema-enabled database, alter the foreign key defn to apply the schema name we
			// just resolved
			dst, ok := parentTbl.(sql.DatabaseSchemaTable)
			if ok {
				schemaName := dst.DatabaseSchema().SchemaName()
				fkDef.ParentSchema = schemaName
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
		if n.IsReplace || n.OnDup != nil {
			fkEditor, err = getForeignKeyEditor(ctx, a.Catalog, tbl, cache, fkChain, nil, true)
			if err != nil {
				return nil, transform.SameTree, err
			}
		} else {
			fkEditor, err = getForeignKeyReferences(ctx, a.Catalog, tbl, cache, fkChain, nil, true)
			if err != nil {
				return nil, transform.SameTree, err
			}
		}
		if fkEditor == nil {
			return n, transform.SameTree, nil
		}
		// Detect duplicate keys before checking the proposed insert references so
		// BEFORE UPDATE triggers can repair the row in the duplicate update path.
		nn, err := n.WithChildren(ctx, &plan.ForeignKeyHandler{
			Table:                      tbl,
			Sch:                        insertableDest.Schema(ctx),
			OriginalNode:               n.Destination,
			Editor:                     fkEditor,
			Tables:                     cache.TablesFor(fkEditor),
			CheckReferencesAfterInsert: n.OnDup != nil || (n.Ignore && n.IgnoreMode == sql.InsertIgnoreModeDuplicateKeysOnly),
		})
		return nn, transform.NewTree, err
	case *plan.Update:
		if plan.IsEmptyTable(n.Child) {
			return n, transform.SameTree, nil
		}
		updated := columnsUpdatedBy(n)
		if updated != nil {
			hasTrigger, err := hasUpdateTrigger(ctx, a, n)
			if err != nil {
				return nil, transform.SameTree, err
			}
			if hasTrigger {
				updated = nil
			}
		}

		if uj, ok := n.Child.(*plan.UpdateJoin); ok {
			updateTargets := uj.UpdateTargets
			fkHandlerMap := make(map[string]sql.Node, len(updateTargets))
			for tableName, updateTarget := range updateTargets {
				fkHandlerMap[tableName] = updateTarget
				fkHandler, err := getForeignKeyHandlerFromUpdateTarget(ctx, a.Catalog, updateTarget, cache, fkChain, updated)
				if err != nil {
					return nil, transform.SameTree, err
				}
				if fkHandler == nil {
					fkHandlerMap[tableName] = updateTarget
				} else {
					fkHandlerMap[tableName] = fkHandler
				}
			}
			uj = plan.NewUpdateJoin(fkHandlerMap, uj.Child)
			nn, err := n.WithChildren(ctx, uj)
			return nn, transform.NewTree, err
		}
		fkHandler, err := getForeignKeyHandlerFromUpdateTarget(ctx, a.Catalog, n.Child, cache, fkChain, updated)
		if err != nil {
			return nil, transform.SameTree, err
		}
		if fkHandler == nil {
			return n, transform.SameTree, nil
		}
		nn, err := n.WithChildren(ctx, fkHandler)
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
			fkEditor, err := getForeignKeyRefActions(ctx, a.Catalog, tbl, cache, fkChain, nil, nil, false)
			if err != nil {
				return nil, transform.SameTree, err
			}
			if fkEditor == nil {
				continue
			}

			foreignKeyHandlers[i] = &plan.ForeignKeyHandler{
				Table:        tbl,
				Sch:          deleteDest.Schema(ctx),
				OriginalNode: targets[i],
				Editor:       fkEditor,
				Tables:       cache.TablesFor(fkEditor),
			}
		}
		if n.HasExplicitTargets() {
			return n.WithExplicitTargets(foreignKeyHandlers), transform.NewTree, nil
		} else {
			// For simple DELETEs, update the targets array with foreign key handlers.
			return n.WithTargets(foreignKeyHandlers), transform.NewTree, nil
		}
	default:
		return n, transform.SameTree, nil
	}
}

// BuildForeignKeyEditor builds an instance of the ForeignKeyEditor for tbl, for callers outside the analyzer, along
// with the editors it was given, which the caller owns. Returns (nil, nil, nil) when tbl is neither a child nor parent
// in any foreign key.
func BuildForeignKeyEditor(ctx *sql.Context, catalog *Catalog, tbl sql.ForeignKeyTable) (*plan.ForeignKeyEditor, []sql.ForeignKeyEditor, error) {
	cache := newForeignKeyCache()
	chain := newForeignKeyChain()
	editor, err := getForeignKeyEditor(ctx, catalog, tbl, cache, chain, nil, false)
	if err != nil {
		return nil, nil, err
	}
	if editor == nil {
		return nil, nil, nil
	}
	bySlot := make([]sql.ForeignKeyEditor, len(cache.tables))
	var editors []sql.ForeignKeyEditor
	for i, tbl := range cache.TablesFor(editor) {
		if tbl != nil {
			bySlot[i] = tbl.GetForeignKeyEditor(ctx)
			editors = append(editors, bySlot[i])
		}
	}
	return editor.Instantiate(bySlot), editors, nil
}

// getForeignKeyEditor merges both getForeignKeyReferences and getForeignKeyRefActions and returns a single editor.
func getForeignKeyEditor(ctx *sql.Context, catalog *Catalog, tbl sql.ForeignKeyTable, cache *foreignKeyCache, fkChain foreignKeyChain, updated updatedColumns, checkRows bool) (*plan.ForeignKeyEditor, error) {
	fkEditor, err := getForeignKeyReferences(ctx, catalog, tbl, cache, fkChain, updated, checkRows)
	if err != nil {
		return nil, err
	}
	fkEditor, err = getForeignKeyRefActions(ctx, catalog, tbl, cache, fkChain, fkEditor, updated, checkRows)
	if err != nil {
		return nil, err
	}
	return fkEditor, err
}

// getForeignKeyReferences returns an editor containing only the references for the given table. If |updated| is not
// nil, the editor only contains the references whose columns it holds.
func getForeignKeyReferences(ctx *sql.Context, catalog *Catalog, tbl sql.ForeignKeyTable, cache *foreignKeyCache, fkChain foreignKeyChain, updated updatedColumns, checkRows bool) (*plan.ForeignKeyEditor, error) {
	var slot int
	fks, err := tbl.GetDeclaredForeignKeys(ctx)
	if err != nil {
		return nil, err
	}
	// We can ignore foreign keys that have been previously used as we can guarantee the parent table has the referenced
	// data, and those whose columns aren't updated, as only a change to them is checked
	{
		newFks := make([]sql.ForeignKeyConstraint, 0, len(fks))
		for _, fk := range fks {
			if !fkChain.HasForeignKey(fk.Name) && updated.containsAny(fk.Columns) {
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
	slot, err = cache.AddUpdater(ctx, tbl, fks[0].Database, fks[0].SchemaName, fks[0].Table)
	if err != nil {
		return nil, err
	}
	fkChain = fkChain.AddTable(fks[0].Database, fks[0].SchemaName, fks[0].Table)

	tblSch, err := resolveSchemaColumnExpressions(ctx, catalog, tbl)
	if err != nil {
		return nil, err
	}
	fkEditor := &plan.ForeignKeyEditor{
		Schema:     tblSch,
		Slot:       slot,
		References: make([]*plan.ForeignKeyReferenceHandler, len(fks)),
		RefActions: nil,
		Cyclical:   false,
	}
	for i, fk := range fks {
		parentTbl, parentSlot, err := cache.GetUpdater(ctx, catalog, fk.ParentDatabase, fk.ParentSchema, fk.ParentTable)
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

		typeConversions, err := plan.GetForeignKeyTypeConversions(parentTbl.Schema(ctx), tblSch, fk, plan.ChildToParent)
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
				Index:                 parentIndex,
				Slot:                  parentSlot,
				SourceSch:             tblSch,
				TargetTypeConversions: typeConversions,
				IndexPositions:        indexPositions,
				AppendTypes:           appendTypes,
			},
		}
	}
	return fkEditor, nil
}

// getForeignKeyRefActions adds referential actions to enforce on the given table. If this is being called after
// getForeignKeyReferences, then that foreign key editor should be passed in. Otherwise, nil should be passed in.
// This also handles caching of the foreign key editor (when the editor does not have any references). If |updated| is
// not nil, the editor only contains the referential actions whose parent columns it holds, and is neither cached nor
// taken from the cache, since it doesn't handle changes to other columns.
func getForeignKeyRefActions(ctx *sql.Context, catalog *Catalog, tbl sql.ForeignKeyTable, cache *foreignKeyCache, fkChain foreignKeyChain, fkEditor *plan.ForeignKeyEditor, updated updatedColumns, checkRows bool) (*plan.ForeignKeyEditor, error) {
	fks, err := tbl.GetReferencedForeignKeys(ctx)
	if err != nil {
		return nil, err
	}

	// Referential actions on update are only taken when the referenced columns change
	if updated != nil {
		newFks := make([]sql.ForeignKeyConstraint, 0, len(fks))
		for _, fk := range fks {
			if updated.containsAny(fk.ParentColumns) {
				newFks = append(newFks, fk)
			}
		}
		fks = newFks
	}

	// Return early if there are no foreign keys that reference the table
	if len(fks) == 0 {
		return fkEditor, nil
	}

	// Check if we already have an editor that we can reuse. If we can, we'll return that instead.
	// Tables do not include their database. As a workaround, we'll use the first foreign key to tell us the database.
	if updated == nil {
		cachedFkEditor := cache.GetEditor(fkEditor, fks[0].ParentDatabase, fks[0].ParentSchema, fks[0].ParentTable)
		if cachedFkEditor != nil {
			// Reusing an editor means that we've hit a cycle, so we update the cached editor.
			cachedFkEditor.Cyclical = true
			return cachedFkEditor, nil
		}
	}
	// No matching editor was cached, so we either create a new one or add to the existing one
	tblSch, err := resolveSchemaColumnExpressions(ctx, catalog, tbl)
	if err != nil {
		return nil, err
	}
	if fkEditor == nil {
		fkEditor = &plan.ForeignKeyEditor{
			Schema:     tblSch,
			References: nil,
			RefActions: make([]plan.ForeignKeyRefActionData, len(fks)),
			Cyclical:   false,
		}
		fkEditor.Slot, err = cache.AddUpdater(ctx, tbl, fks[0].ParentDatabase, fks[0].ParentSchema, fks[0].ParentTable)
		if err != nil {
			return nil, err
		}
	} else {
		// The editor has already been created, so we need to create the referential actions array
		fkEditor.RefActions = make([]plan.ForeignKeyRefActionData, len(fks))
	}
	// Add the editor to the cache
	if updated == nil {
		cache.AddEditor(fkEditor, fks[0].ParentDatabase, fks[0].ParentSchema, fks[0].ParentTable)
	}
	// Ensure that the chain has the table and updater
	fkChain = fkChain.AddTable(fks[0].ParentDatabase, fks[0].ParentSchema, fks[0].ParentTable)

	for i, fk := range fks {
		childTbl, childSlot, err := cache.GetUpdater(ctx, catalog, fk.Database, fk.SchemaName, fk.Table)
		if err != nil {
			return nil, sql.ErrForeignKeyNotResolved.New(fk.Database, fk.Table, fk.Name,
				strings.Join(fk.Columns, "`, `"), fk.ParentTable, strings.Join(fk.ParentColumns, "`, `"))
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

		// TODO: Foreign key information is not fully added to the plan node until these FK rules in the analyzer,
		//       but it should be added during the binding phase, in planbuilder. Because this information is added
		//       late, we have to do extra work here to resolve the schema defaults, which normally would happen
		//       during binding. This extra FK information also doesn't get its exec indexes fixed up, so we have to
		//       manually do that here. Moving all the FK information into the binding, planbuilder, phase would
		//       clean this up. We should also fix assignExecIndexes to find all these FK schema references and fix
		//       their exec indexes.
		childTblSch, err := resolveSchemaColumnExpressions(ctx, catalog, childTbl)
		if err != nil {
			return nil, err
		}

		childParentMapping, err := plan.GetChildParentMapping(tblSch, childTblSch, fk)
		if err != nil {
			return nil, err
		}

		typeConversions, err := plan.GetForeignKeyTypeConversions(tblSch, childTblSch, fk, plan.ParentToChild)
		if err != nil {
			return nil, err
		}

		childEditor, err := getForeignKeyEditor(ctx, catalog, childTbl, cache, fkChain.AddForeignKey(fk.Name), nil, checkRows)
		if err != nil {
			return nil, err
		}
		// May return nil if we recursively loop onto a foreign key previously declared
		if childEditor == nil {
			childEditor = &plan.ForeignKeyEditor{
				Schema:     childTblSch,
				Slot:       childSlot,
				References: nil,
				RefActions: nil,
				Cyclical:   false,
			}
		}
		// If a child editor is cyclical, then this editor is a part of that cycle
		fkEditor.Cyclical = fkEditor.Cyclical || childEditor.Cyclical
		// If "ON UPDATE CASCADE" or "ON UPDATE SET NULL" recurses onto the same table that has been previously updated
		// in the same cascade then it's treated like a RESTRICT (does not apply to "ON DELETE")
		if fkChain.HasTable(fk.Database, fk.SchemaName, fk.Table) {
			fk.OnUpdate = sql.ForeignKeyReferentialAction_Restrict
		}
		fkEditor.RefActions[i] = plan.ForeignKeyRefActionData{
			RowMapper: &plan.ForeignKeyRowMapper{
				Index:                 childIndex,
				Slot:                  childSlot,
				SourceSch:             tblSch,
				TargetTypeConversions: typeConversions,
				IndexPositions:        indexPositions,
				AppendTypes:           appendTypes,
			},
			Editor:               childEditor,
			ForeignKey:           fk,
			ChildParentMapping:   childParentMapping,
			GeneratedProjections: childEditor.Schema.GeneratedExpressions(),
		}
	}
	return fkEditor, nil
}

// getForeignKeyHandlerFromUpdateTarget creates a ForeignKeyHandler from a given update target Node. It is used for
// applying foreign key constraints to Update nodes. If |updated| is not nil, the handler only enforces the foreign keys
// whose columns it holds.
func getForeignKeyHandlerFromUpdateTarget(ctx *sql.Context, catalog *Catalog, updateTarget sql.Node,
	cache *foreignKeyCache, fkChain foreignKeyChain, updated updatedColumns) (*plan.ForeignKeyHandler, error) {
	updateDest, err := plan.GetUpdatable(updateTarget)
	if err != nil {
		return nil, err
	}
	fkTbl, ok := updateDest.(sql.ForeignKeyTable)
	if !ok {
		return nil, nil
	}

	fkEditor, err := getForeignKeyEditor(ctx, catalog, fkTbl, cache, fkChain, updated, false)
	if err != nil {
		return nil, err
	}
	if fkEditor == nil {
		return nil, nil
	}

	return &plan.ForeignKeyHandler{
		Table:        fkTbl,
		Sch:          updateDest.Schema(ctx),
		OriginalNode: updateTarget,
		Editor:       fkEditor,
		Tables:       cache.TablesFor(fkEditor),
	}, nil
}

// updatedColumns holds the lower case names of the columns that an UPDATE may change. A nil set means that any column
// may change.
type updatedColumns map[string]struct{}

// columnsUpdatedBy returns the columns that |update| may change, apart from those that a BEFORE UPDATE trigger may
// assign, for which see hasUpdateTrigger. These are the columns its update expressions assign, which include the
// generated columns and the columns with ON UPDATE expressions. So that any node an integrator places between the
// update expressions and the update, which may change the rows being written, is accounted for, this returns nil
// unless the update expressions feed the update directly. As tables are not distinguished, an UPDATE that joins tables
// may change a column of any of them that has the name of a column it assigns.
func columnsUpdatedBy(update *plan.Update) updatedColumns {
	source := update.Child
	if updateJoin, ok := source.(*plan.UpdateJoin); ok {
		source = updateJoin.Child
	}
	updateSource, ok := source.(*plan.UpdateSource)
	if !ok {
		return nil
	}

	updated := make(updatedColumns)
	for _, expr := range updateSource.UpdateExprs.AllExpressions() {
		setField, ok := expr.(*expression.SetField)
		if !ok {
			return nil
		}
		field, ok := setField.LeftChild.(*expression.GetField)
		if !ok {
			return nil
		}
		updated[strings.ToLower(field.Name())] = struct{}{}
	}
	return updated
}

// hasUpdateTrigger returns whether a table that |update| writes has an UPDATE trigger. A BEFORE UPDATE trigger may
// assign any column, and since triggers are applied after foreign keys, |update| doesn't show them yet. Telling BEFORE
// triggers from AFTER triggers would take building each trigger, so any UPDATE trigger is taken to assign any column. A
// trigger that can't be parsed is taken to be an UPDATE trigger on every table.
func hasUpdateTrigger(ctx *sql.Context, a *Analyzer, update *plan.Update) (bool, error) {
	var tables []string
	if updateJoin, ok := update.Child.(*plan.UpdateJoin); ok {
		for _, target := range updateJoin.UpdateTargets {
			tables = append(tables, getTableName(ctx, target))
		}
	} else {
		tables = append(tables, getTableName(ctx, update))
	}

	dbName := update.Database()
	if dbName == "" {
		dbName = ctx.GetCurrentDatabase()
	}
	db, err := a.Catalog.Database(ctx, dbName)
	if err != nil {
		return false, err
	}
	triggerDb, ok := db.(sql.TriggerDatabase)
	if !ok {
		return false, nil
	}
	triggers, err := triggerDb.GetTriggers(ctx)
	if err != nil {
		return false, err
	}
	if len(triggers) == 0 {
		return false, nil
	}

	b := planbuilder.New(ctx, a.Catalog, nil)
	for _, trigger := range triggers {
		b.SetParserOptions(sql.NewSqlModeFromString(trigger.SqlMode).ParserOptions())
		table, event, isTrigger, err := b.TriggerTarget(trigger.CreateStatement)
		b.Reset()
		if err != nil {
			return true, nil
		}
		if isTrigger && stringContains(tables, table) && triggerEventsMatch(plan.UpdateTrigger, event) {
			return true, nil
		}
	}
	return false, nil
}

// containsAny returns whether any of the given columns may be updated.
func (updated updatedColumns) containsAny(columns []string) bool {
	if updated == nil {
		return true
	}
	for _, column := range columns {
		if _, ok := updated[strings.ToLower(column)]; ok {
			return true
		}
	}
	return false
}

// resolveSchemaColumnExpressions returns the schema of |table| with its default and generated column
// expressions resolved, because the planbuilder phase does not resolve them for this foreign key
// path and assignExecIndexes does not traverse foreign key editors to find them. It also decrements their
// GetField indexes by one, which is safe because they are only evaluated against a single table's row.
func resolveSchemaColumnExpressions(ctx *sql.Context, catalog *Catalog, table sql.Table) (sql.Schema, error) {
	// Resolve the default and generated column expressions in tblSch
	builder := planbuilder.New(ctx, catalog, nil)
	childTblSch, err := builder.ResolveSchemaDefaults(ctx.GetCurrentDatabase(), table.Name(), table.Schema(ctx))
	if err != nil {
		return nil, err
	}

	// Field Indexes are off by one initially and don't fixed by assignExecIndexes because it doesn't traverse through
	// the ForeignKeyEditors and referential actions, so we correct them here. This is safe because we know these fields
	// will only ever be accessed within the scope of a single table, so all we have to do is decrement the index by 1.
	// TODO: rm correction once foreign key schemas are resolved during binding, where assignExecIndexes would number these expressions correctly.
	for i := range childTblSch {
		for _, cd := range []*sql.ColumnDefaultValue{childTblSch[i].Default, childTblSch[i].Generated} {
			if cd == nil {
				continue
			}
			expr, identity, err := transform.Expr(ctx, cd.Expr, func(ctx *sql.Context, e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
				if gf, ok := e.(*expression.GetField); ok {
					return gf.WithIndex(gf.Index() - 1), transform.NewTree, nil
				}
				return e, transform.SameTree, nil
			})
			if err != nil {
				return nil, err
			}
			if identity == transform.NewTree {
				cd.Expr = expr
			}
		}
	}

	return childTblSch, nil
}

// foreignKeyTableName is the combination of a table's database along with their name, both lowercased.
type foreignKeyTableName struct {
	dbName     string
	schemaName string
	tblName    string
}

func newForeignKeyTableName(dbName, schemaName, tblName string) foreignKeyTableName {
	return foreignKeyTableName{
		dbName:     strings.ToLower(dbName),
		schemaName: strings.ToLower(schemaName),
		tblName:    strings.ToLower(tblName),
	}
}

// foreignKeyCache is a cache of the tables and editors for foreign keys. It holds no table editors: each table the
// foreign key graph reads or edits gets a slot, and executing the plan opens an editor for each slot.
type foreignKeyCache struct {
	slots        map[foreignKeyTableName]int
	tables       []sql.ForeignKeyTable
	editorsCache map[foreignKeyTableName][]*plan.ForeignKeyEditor
}

// newForeignKeyCache returns a new *foreignKeyCache.
func newForeignKeyCache() *foreignKeyCache {
	return &foreignKeyCache{
		slots:        make(map[foreignKeyTableName]int),
		editorsCache: make(map[foreignKeyTableName][]*plan.ForeignKeyEditor),
	}
}

// AddUpdater adds the given foreign key table to the cache if it isn't there already, and returns its slot. All
// foreign key instances for a table share its slot, so they share its editor.
func (cache *foreignKeyCache) AddUpdater(ctx *sql.Context, tbl sql.ForeignKeyTable, dbName, schemaName, tblName string) (int, error) {
	fkTableName := newForeignKeyTableName(dbName, schemaName, tblName)
	if slot, ok := cache.slots[fkTableName]; ok {
		return slot, nil
	}
	return cache.addSlot(fkTableName, tbl), nil
}

func (cache *foreignKeyCache) addSlot(name foreignKeyTableName, tbl sql.ForeignKeyTable) int {
	slot := len(cache.tables)
	cache.slots[name] = slot
	cache.tables = append(cache.tables, tbl)
	return slot
}

// TablesFor returns the tables that |fkEditor| edits or reads, indexed by slot, with nil for the slots it doesn't use.
// The cache is shared by every foreign key handler of a statement, so a handler doesn't use every slot.
func (cache *foreignKeyCache) TablesFor(fkEditor *plan.ForeignKeyEditor) []sql.ForeignKeyTable {
	tables := make([]sql.ForeignKeyTable, len(cache.tables))
	for slot := range fkEditor.Slots() {
		tables[slot] = cache.tables[slot]
	}
	return tables
}

// AddEditor will add the given foreign key editor to the cache. Does not validate that the editor is unique, therefore
// GetEditor should be called before this function.
func (cache *foreignKeyCache) AddEditor(editor *plan.ForeignKeyEditor, dbName, schemaName, tblName string) {
	if editor == nil {
		panic("cannot pass in nil editor") // Should never be hit
	}
	fkTableName := newForeignKeyTableName(dbName, schemaName, tblName)
	cache.editorsCache[fkTableName] = append(cache.editorsCache[fkTableName], editor)
}

// GetUpdater returns the given foreign key table and its slot, adding it to the cache if it isn't there already.
func (cache *foreignKeyCache) GetUpdater(ctx *sql.Context, catalog *Catalog, dbName, schemaName, tblName string) (sql.ForeignKeyTable, int, error) {
	fkTableName := newForeignKeyTableName(dbName, schemaName, tblName)
	if slot, ok := cache.slots[fkTableName]; ok {
		return cache.tables[slot], slot, nil
	}
	tbl, _, err := catalog.TableSchema(ctx, dbName, schemaName, tblName)
	if err != nil {
		return nil, 0, err
	}
	fkTbl, ok := tbl.(sql.ForeignKeyTable)
	if !ok {
		return nil, 0, sql.ErrNoForeignKeySupport.New(tblName)
	}
	return fkTbl, cache.addSlot(fkTableName, fkTbl), nil
}

// GetEditor returns a foreign key editor that matches the given editor in all ways except for the referential actions.
// Returns nil if no such editors have been cached.
func (cache *foreignKeyCache) GetEditor(fkEditor *plan.ForeignKeyEditor, dbName, schemaName, tblName string) *plan.ForeignKeyEditor {
	fkTableName := newForeignKeyTableName(dbName, schemaName, tblName)
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
	fkNames  map[string]struct{}
	fkTables map[foreignKeyTableName]struct{}
}

func newForeignKeyChain() foreignKeyChain {
	return foreignKeyChain{
		fkNames:  make(map[string]struct{}),
		fkTables: make(map[foreignKeyTableName]struct{}),
	}
}

// AddTable returns a new chain with the added table.
func (chain foreignKeyChain) AddTable(dbName string, schemaName, tblName string) foreignKeyChain {
	newFkNames := make(map[string]struct{})
	newFkTables := make(map[foreignKeyTableName]struct{})
	for fkName := range chain.fkNames {
		newFkNames[fkName] = struct{}{}
	}
	for fkTable := range chain.fkTables {
		newFkTables[fkTable] = struct{}{}
	}
	newFkTables[newForeignKeyTableName(dbName, schemaName, tblName)] = struct{}{}
	return foreignKeyChain{
		fkNames:  newFkNames,
		fkTables: newFkTables,
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
	}
}

// HasTable returns whether the chain contains the given table. Case-insensitive.
func (chain foreignKeyChain) HasTable(dbName, schemaName, tblName string) bool {
	_, ok := chain.fkTables[newForeignKeyTableName(dbName, schemaName, tblName)]
	return ok
}

// HasForeignKey returns whether the chain contains the given foreign key. Case-insensitive.
func (chain foreignKeyChain) HasForeignKey(fkName string) bool {
	_, ok := chain.fkNames[strings.ToLower(fkName)]
	return ok
}
