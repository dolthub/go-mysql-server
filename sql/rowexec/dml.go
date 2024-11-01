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

package rowexec

import (
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/fulltext"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func (b *BaseBuilder) buildInsertInto(ctx *sql.Context, ii *plan.InsertInto, row sql.Row) (sql.RowIter, error) {
	dstSchema := ii.Destination.Schema()

	insertable, err := plan.GetInsertable(ii.Destination)
	if err != nil {
		return nil, err
	}

	var inserter sql.RowInserter

	var replacer sql.RowReplacer
	var updater sql.RowUpdater
	// These type casts have already been asserted in the analyzer
	if ii.IsReplace {
		replacer = insertable.(sql.ReplaceableTable).Replacer(ctx)
	} else {
		inserter = insertable.Inserter(ctx)
		if len(ii.OnDupExprs) > 0 {
			updater = insertable.(sql.UpdatableTable).Updater(ctx)
		}
	}

	rowIter, err := b.buildNodeExec(ctx, ii.Source, row)
	if err != nil {
		return nil, err
	}

	var unlocker func()
	insertExpressions := getInsertExpressions(ii.Source)
	if ii.FirstGeneratedAutoIncRowIdx >= 0 {
		_, i, _ := sql.SystemVariables.GetGlobal("innodb_autoinc_lock_mode")
		lockMode, ok := i.(int64)
		if !ok {
			return nil, errors.New(fmt.Sprintf("unexpected type for innodb_autoinc_lock_mode, expected int64, got %T", i))
		}
		// Lock modes "traditional" (0) and "consecutive" (1) require that a single lock is held for the entire iteration.
		// Lock mode "interleaved" (2) will acquire the lock only when inserting into the table.
		if lockMode != 2 {
			autoIncrementable, ok := sql.GetUnderlyingTable(insertable).(sql.AutoIncrementTable)
			if !ok {
				return nil, errors.New("auto increment expression on non-AutoIncrement table. This should not be possible")
			}

			unlocker, err = autoIncrementable.AutoIncrementSetter(ctx).AcquireAutoIncrementLock(ctx)
			if err != nil {
				return nil, err
			}
		}
	}
	insertIter := &insertIter{
		schema:                      dstSchema,
		tableNode:                   ii.Destination,
		inserter:                    inserter,
		replacer:                    replacer,
		updater:                     updater,
		rowSource:                   rowIter,
		unlocker:                    unlocker,
		updateExprs:                 ii.OnDupExprs,
		insertExprs:                 insertExpressions,
		checks:                      ii.Checks(),
		ctx:                         ctx,
		ignore:                      ii.Ignore,
		firstGeneratedAutoIncRowIdx: ii.FirstGeneratedAutoIncRowIdx,
	}

	var ed sql.EditOpenerCloser
	if replacer != nil {
		ed = replacer
	} else {
		ed = inserter
	}

	if ii.Ignore {
		return plan.NewCheckpointingTableEditorIter(insertIter, ed), nil
	} else {
		return plan.NewTableEditorIter(insertIter, ed), nil
	}
}

func (b *BaseBuilder) buildDeleteFrom(ctx *sql.Context, n *plan.DeleteFrom, row sql.Row) (sql.RowIter, error) {
	iter, err := b.buildNodeExec(ctx, n.Child, row)
	if err != nil {
		return nil, err
	}

	targets := n.GetDeleteTargets()
	schemaPositionDeleters := make([]schemaPositionDeleter, len(targets))
	schema := n.Child.Schema()

	for i, target := range targets {
		deletable, err := plan.GetDeletable(target)
		if err != nil {
			return nil, err
		}
		deleter := deletable.Deleter(ctx)

		// By default the sourceName in the schema is the table name, but if there is a
		// table alias applied, then use that instead.
		sourceName := deletable.Name()
		transform.Inspect(target, func(node sql.Node) bool {
			if tableAlias, ok := node.(*plan.TableAlias); ok {
				sourceName = tableAlias.Name()
				return false
			}
			return true
		})

		start, end, err := findSourcePosition(schema, sourceName)
		if err != nil {
			return nil, err
		}
		schemaPositionDeleters[i] = schemaPositionDeleter{deleter, int(start), int(end)}
	}
	return newDeleteIter(iter, schema, schemaPositionDeleters...), nil
}

func (b *BaseBuilder) buildForeignKeyHandler(ctx *sql.Context, n *plan.ForeignKeyHandler, row sql.Row) (sql.RowIter, error) {
	return b.buildNodeExec(ctx, n.OriginalNode, row)
}

func (b *BaseBuilder) buildUpdate(ctx *sql.Context, n *plan.Update, row sql.Row) (sql.RowIter, error) {
	updatable, err := plan.GetUpdatable(n.Child)
	if err != nil {
		return nil, err
	}
	updater := updatable.Updater(ctx)

	iter, err := b.buildNodeExec(ctx, n.Child, row)
	if err != nil {
		return nil, err
	}

	return newUpdateIter(iter, updatable.Schema(), updater, n.Checks(), n.Ignore), nil
}

func (b *BaseBuilder) buildDropForeignKey(ctx *sql.Context, n *plan.DropForeignKey, row sql.Row) (sql.RowIter, error) {
	db, err := n.DbProvider.Database(ctx, n.Database())
	if err != nil {
		return nil, err
	}
	tbl, ok, err := db.GetTableInsensitive(ctx, n.Table)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, sql.ErrTableNotFound.New(n.Table)
	}
	fkTbl, ok := tbl.(sql.ForeignKeyTable)
	if !ok {
		return nil, sql.ErrNoForeignKeySupport.New(n.Name)
	}
	err = fkTbl.DropForeignKey(ctx, n.Name)
	if err != nil {
		return nil, err
	}

	return rowIterWithOkResultWithZeroRowsAffected(), nil
}

func (b *BaseBuilder) buildDropTable(ctx *sql.Context, n *plan.DropTable, row sql.Row) (sql.RowIter, error) {
	var err error
	var curdb sql.Database

	for _, table := range n.Tables {
		tbl := table.(*plan.ResolvedTable)
		curdb = tbl.SqlDatabase

		droppable := tbl.SqlDatabase.(sql.TableDropper)

		if fkTable, err := getForeignKeyTable(tbl); err == nil {
			fkChecks, err := ctx.GetSessionVariable(ctx, "foreign_key_checks")
			if err != nil {
				return nil, err
			}
			if fkChecks.(int8) == 1 {
				parentFks, err := fkTable.GetReferencedForeignKeys(ctx)
				if err != nil {
					return nil, err
				}
				for i, fk := range parentFks {
					// ignore self referential foreign keys
					if fk.Table != fk.ParentTable {
						return nil, sql.ErrForeignKeyDropTable.New(fkTable.Name(), parentFks[i].Name)
					}
				}
			}
			fks, err := fkTable.GetDeclaredForeignKeys(ctx)
			if err != nil {
				return nil, err
			}
			for _, fk := range fks {
				if err = fkTable.DropForeignKey(ctx, fk.Name); err != nil {
					return nil, err
				}
			}
		}

		if hasFullText(ctx, tbl) {
			if err = fulltext.DropAllIndexes(ctx, tbl.Table.(sql.IndexAddressableTable), droppable.(fulltext.Database)); err != nil {
				return nil, err
			}
		}

		err = droppable.DropTable(ctx, tbl.Name())
		if err != nil {
			return nil, err
		}
	}

	if len(n.TriggerNames) > 0 {
		triggerDb, ok := curdb.(sql.TriggerDatabase)
		if !ok {
			tblNames, _ := n.TableNames()
			return nil, fmt.Errorf(`tables %v are referenced in triggers %v, but database does not support triggers`, tblNames, n.TriggerNames)
		}
		//TODO: if dropping any triggers fail, then we'll be left in a state where triggers exist for a table that was dropped
		for _, trigger := range n.TriggerNames {
			err = triggerDb.DropTrigger(ctx, trigger)
			if err != nil {
				return nil, err
			}
		}
	}

	return rowIterWithOkResultWithZeroRowsAffected(), nil
}

func (b *BaseBuilder) buildAlterIndex(ctx *sql.Context, n *plan.AlterIndex, row sql.Row) (sql.RowIter, error) {
	err := b.executeAlterIndex(ctx, n)
	if err != nil {
		return nil, err
	}

	return rowIterWithOkResultWithZeroRowsAffected(), nil
}

func (b *BaseBuilder) buildTriggerBeginEndBlock(ctx *sql.Context, n *plan.TriggerBeginEndBlock, row sql.Row) (sql.RowIter, error) {
	return &triggerBlockIter{
		statements: n.Children(),
		row:        row,
		once:       &sync.Once{},
		b:          b,
	}, nil
}

func (b *BaseBuilder) buildTriggerExecutor(ctx *sql.Context, n *plan.TriggerExecutor, row sql.Row) (sql.RowIter, error) {
	childIter, err := b.buildNodeExec(ctx, n.Left(), row)
	if err != nil {
		return nil, err
	}

	return &triggerIter{
		child:          childIter,
		triggerTime:    n.TriggerTime,
		triggerEvent:   n.TriggerEvent,
		executionLogic: n.Right(),
		ctx:            ctx,
		b:              b,
	}, nil
}

func (b *BaseBuilder) buildInsertDestination(ctx *sql.Context, n *plan.InsertDestination, row sql.Row) (sql.RowIter, error) {
	return b.buildNodeExec(ctx, n.Child, row)
}

func (b *BaseBuilder) buildTruncate(ctx *sql.Context, n *plan.Truncate, row sql.Row) (sql.RowIter, error) {
	truncatable, err := plan.GetTruncatable(n.Child)
	if err != nil {
		return nil, err
	}
	//TODO: when performance schema summary tables are added, reset the columns to 0/NULL rather than remove rows
	//TODO: close all handlers that were opened with "HANDLER OPEN"

	removed, err := truncatable.Truncate(ctx)
	if err != nil {
		return nil, err
	}
	for _, col := range truncatable.Schema() {
		if col.AutoIncrement {
			aiTable, ok := truncatable.(sql.AutoIncrementTable)
			if ok {
				setter := aiTable.AutoIncrementSetter(ctx)
				err = setter.SetAutoIncrementValue(ctx, uint64(1))
				if err != nil {
					return nil, err
				}
				err = setter.Close(ctx)
				if err != nil {
					return nil, err
				}
			}
			break
		}
	}
	// If we've got Full-Text indexes, then we also need to clear those tables
	if hasFullText(ctx, truncatable) {
		if err = rebuildFullText(ctx, truncatable.Name(), plan.GetDatabase(n.Child)); err != nil {
			return nil, err
		}
	}
	return sql.RowsToRowIter(sql.NewRow(types.NewOkResult(removed))), nil
}

func (b *BaseBuilder) buildUpdateSource(ctx *sql.Context, n *plan.UpdateSource, row sql.Row) (sql.RowIter, error) {
	rowIter, err := b.buildNodeExec(ctx, n.Child, row)
	if err != nil {
		return nil, err
	}

	schema, err := n.GetChildSchema()
	if err != nil {
		return nil, err
	}

	return &updateSourceIter{
		childIter:   rowIter,
		updateExprs: n.UpdateExprs,
		tableSchema: schema,
		ignore:      n.Ignore,
	}, nil
}

func (b *BaseBuilder) buildUpdateJoin(ctx *sql.Context, n *plan.UpdateJoin, row sql.Row) (sql.RowIter, error) {
	ji, err := b.buildNodeExec(ctx, n.Child, row)
	if err != nil {
		return nil, err
	}

	return &updateJoinIter{
		updateSourceIter: ji,
		joinSchema:       n.Child.(*plan.UpdateSource).Child.Schema(),
		updaters:         n.Updaters,
		caches:           make(map[string]sql.KeyValueCache),
		disposals:        make(map[string]sql.DisposeFunc),
		joinNode:         n.Child.(*plan.UpdateSource).Child,
	}, nil
}

func (b *BaseBuilder) buildRenameForeignKey(ctx *sql.Context, n *plan.RenameForeignKey, row sql.Row) (sql.RowIter, error) {
	db, err := n.DbProvider.Database(ctx, n.Database())
	if err != nil {
		return nil, err
	}
	tbl, ok, err := db.GetTableInsensitive(ctx, n.Table)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, sql.ErrTableNotFound.New(n.Table)
	}
	fkTbl, ok := tbl.(sql.ForeignKeyTable)
	if !ok {
		return nil, sql.ErrNoForeignKeySupport.New(n.OldName)
	}

	fkcs, err := fkTbl.GetDeclaredForeignKeys(ctx)
	if err != nil {
		return nil, err
	}

	var existingFk sql.ForeignKeyConstraint
	for _, fkc := range fkcs {
		if strings.EqualFold(fkc.Name, n.OldName) {
			existingFk = fkc
			break
		}
	}

	err = fkTbl.DropForeignKey(ctx, n.OldName)
	if err != nil {
		return nil, err
	}

	existingFk.Name = n.NewName
	err = fkTbl.AddForeignKey(ctx, existingFk)
	if err != nil {
		return nil, err
	}
	return rowIterWithOkResultWithZeroRowsAffected(), nil
}
