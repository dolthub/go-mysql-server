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
		fkRefTbls := make([]sql.ForeignKeyTable, len(fkDefs))
		for i, fkDef := range fkDefs {
			// This should never happen, but ensure that foreign keys are declared on the table being created
			if n.Database().Name() != fkDef.Database || n.Name() != fkDef.Table {
				return nil, fmt.Errorf("foreign key definition has a different database/table name than the declaring table: `%s`.`%s`",
					fkDef.Database, fkDef.Table)
			}
			// If the foreign key is self-referential then the table won't exist yet, so we put a nil here.
			// CreateTable knows to interpret all nil tables as the newly-created table.
			if fkDef.IsSelfReferential() {
				fkRefTbls[i] = nil
				continue
			}
			refTbl, _, err := a.Catalog.Table(ctx, fkDef.ReferencedDatabase, fkDef.ReferencedTable)
			if err != nil {
				return nil, err
			}
			fkRefTbl, ok := refTbl.(sql.ForeignKeyTable)
			if !ok {
				return nil, sql.ErrNoForeignKeySupport.New(fkDef.ReferencedTable)
			}
			fkRefTbls[i] = fkRefTbl
		}
		return n.WithReferencedForeignKeyTables(fkRefTbls)
	case *plan.InsertInto:
		insertableDest, err := plan.GetInsertable(n.Destination)
		if err != nil {
			return nil, err
		}
		tbl, ok := insertableDest.(sql.ForeignKeyTable)
		// If foreign keys aren't supported then we can just return
		if !ok {
			return n, nil
		}
		var editor sql.ForeignKeyUpdater
		//TODO: GetReferencedForeignKeys as well for REPLACE
		declaredFks, err := tbl.GetDeclaredForeignKeys(ctx)
		if err != nil {
			return nil, err
		}
		// If there are no foreign keys then we can skip the rest of this
		//TODO: consider referenced keys for REPLACE
		if len(declaredFks) == 0 {
			return n, nil
		}

		fkCheck := &plan.ForeignKeyHandler{
			Table:      tbl,
			Sch:        insertableDest.Schema(),
			References: make([]*plan.ForeignKeyReferenceHandler, len(declaredFks)),
		}
		for i, declaredFk := range declaredFks {
			// As we can't guarantee that the table comes from a ResolvedTable, we can't guarantee that the database
			// name has been retained. As a workaround, we'll use one of foreign keys to tell us the database.
			if editor == nil {
				editor, err = cache.Add(ctx, tbl, declaredFk.Database, declaredFk.Table)
				if err != nil {
					return nil, err
				}
				fkCheck.Editor = editor
			}

			refTbl, refEditor, err := cache.Get(ctx, a, declaredFk.ReferencedDatabase, declaredFk.ReferencedTable)
			if err != nil {
				return nil, err
			}
			refIndex, ok, err := plan.FindIndexWithPrefix(ctx, refTbl, declaredFk.ReferencedColumns)
			if err != nil {
				return nil, err
			}
			if !ok {
				// If this error is returned, it is due to an index deletion not properly checking for foreign key usage
				//TODO: enforce that the last matching index cannot be removed if depended upon by a foreign key
				return nil, fmt.Errorf("no suitable index found for table `%s` in foreign key `%s` declared on table `%s`",
					declaredFk.ReferencedTable, declaredFk.Name, declaredFk.Table)
			}
			indexPositions, appendTypes, err := plan.FindForeignKeyColMapping(ctx, declaredFk.Name, tbl, declaredFk.Columns,
				declaredFk.ReferencedColumns, refIndex)
			if err != nil {
				return nil, err
			}
			fkCheck.References[i] = &plan.ForeignKeyReferenceHandler{
				Index:          refIndex,
				ForeignKey:     declaredFk,
				Editor:         refEditor,
				IndexPositions: indexPositions,
				AppendTypes:    appendTypes,
			}
		}
		return n.WithChildren(fkCheck)
	case *plan.Update:
		//TODO: implement me
		return n, nil
	case *plan.DeleteFrom:
		//TODO: implement me
		return n, nil
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

// foreignKeyTableName is the combination of a table's database along with their name, both lowercased.
type foreignKeyTableName struct {
	dbName  string
	tblName string
}

// foreignKeyTableEditor is a foreign key table along with its editor.
type foreignKeyTableEditor struct {
	tbl    sql.ForeignKeyTable
	editor sql.ForeignKeyUpdater
}

// foreignKeyTableCache is a cache of table editors for foreign keys.
type foreignKeyTableCache struct {
	cache map[foreignKeyTableName]foreignKeyTableEditor
}

// newForeignKeyTableCache returns a new *foreignKeyTableCache.
func newForeignKeyTableCache() *foreignKeyTableCache {
	return &foreignKeyTableCache{
		cache: make(map[foreignKeyTableName]foreignKeyTableEditor),
	}
}

// Add will add the given foreign key table (and editor) to the cache and returns its editor. If it already exists, it
// is not added, and instead the cached editor is returned. This is so that the same editor is referenced by all foreign
// key instances.
func (cache *foreignKeyTableCache) Add(ctx *sql.Context, tbl sql.ForeignKeyTable, dbName string, tblName string) (sql.ForeignKeyUpdater, error) {
	fkTableName := foreignKeyTableName{
		dbName:  strings.ToLower(dbName),
		tblName: strings.ToLower(tblName),
	}
	if cachedEditor, ok := cache.cache[fkTableName]; ok {
		return cachedEditor.editor, nil
	}
	editor := foreignKeyTableEditor{
		tbl:    tbl,
		editor: tbl.GetForeignKeyUpdater(ctx),
	}
	cache.cache[fkTableName] = editor
	return editor.editor, nil
}

// Get returns the given foreign key table editor.
func (cache *foreignKeyTableCache) Get(ctx *sql.Context, a *Analyzer, dbName string, tblName string) (sql.ForeignKeyTable, sql.ForeignKeyUpdater, error) {
	fkTableName := foreignKeyTableName{
		dbName:  strings.ToLower(dbName),
		tblName: strings.ToLower(tblName),
	}
	if fkTblEditor, ok := cache.cache[fkTableName]; ok {
		return fkTblEditor.tbl, fkTblEditor.editor, nil
	}
	tbl, _, err := a.Catalog.Table(ctx, dbName, tblName)
	if err != nil {
		return nil, nil, err
	}
	fkTbl, ok := tbl.(sql.ForeignKeyTable)
	if !ok {
		return nil, nil, sql.ErrNoForeignKeySupport.New(tblName)
	}
	editor := foreignKeyTableEditor{
		tbl:    fkTbl,
		editor: fkTbl.GetForeignKeyUpdater(ctx),
	}
	cache.cache[fkTableName] = editor
	return editor.tbl, editor.editor, nil
}
