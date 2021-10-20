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
	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/sql"
)

type UpdateJoin struct {
	updaters map[string]sql.RowUpdater
	UnaryNode
}

// NewUpdateJoin returns an *UpdateJoin node.
func NewUpdateJoin(editorMap map[string]sql.RowUpdater, child sql.Node) *UpdateJoin {
	return &UpdateJoin{
		updaters:  editorMap,
		UnaryNode: UnaryNode{Child: child},
	}
}

var _ sql.Node = (*UpdateJoin)(nil)

// String implements the sql.Node interface.
func (u *UpdateJoin) String() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("Update Join")
	_ = pr.WriteChildren(u.Child.String())
	return pr.String()
}

// RowIter implements the sql.Node interface.
func (u *UpdateJoin) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	ji, err := u.Child.RowIter(ctx, row)
	if err != nil {
		return nil, err
	}

	return &updateJoinIter{
		ctx:              ctx,
		updateSourceIter: ji,
		joinSchema:       u.Child.(*UpdateSource).Child.Schema(),
		updaters:         u.updaters,
		caches:           make(map[string]sql.KeyValueCache),
		disposals:        make(map[string]sql.DisposeFunc),
	}, nil
}

// GetUpdatable returns an updateJoinTable which implements sql.UpdatableTable.
func (u *UpdateJoin) GetUpdatable() sql.UpdatableTable {
	return &updatableJoinTable{
		updaters: u.updaters,
		joinNode: u.Child.(*UpdateSource).Child,
	}
}

// WithChildren implements the sql.Node interface.
func (u *UpdateJoin) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(u, len(children), 1)
	}

	return NewUpdateJoin(u.updaters, children[0]), nil
}

// updateJoinIter wraps the child UpdateSource iter and returns join row in such a way that updates per table row are
// done once.
type updateJoinIter struct {
	ctx              *sql.Context
	updateSourceIter sql.RowIter
	joinSchema       sql.Schema
	updaters         map[string]sql.RowUpdater
	caches           map[string]sql.KeyValueCache
	disposals        map[string]sql.DisposeFunc
}

var _ sql.RowIter = (*updateJoinIter)(nil)

func (u *updateJoinIter) Next() (sql.Row, error) {
	for {
		oldAndNewRow, err := u.updateSourceIter.Next()
		if err != nil {
			return nil, err
		}

		oldJoinRow, newJoinRow := oldAndNewRow[:len(oldAndNewRow)/2], oldAndNewRow[len(oldAndNewRow)/2:]

		tableToOldRowMap := splitRowIntoTableRowMap(oldJoinRow, u.joinSchema)
		tableToNewRowMap := splitRowIntoTableRowMap(newJoinRow, u.joinSchema)

		for tableName, _ := range u.updaters {
			oldTableRow := tableToOldRowMap[tableName]

			cache := u.getOrCreateCache(u.ctx, tableName)
			hash, err := sql.HashOf(oldTableRow)
			if err != nil {
				return nil, err
			}

			_, err = cache.Get(hash)
			if errors.Is(err, sql.ErrKeyNotFound) {
				cache.Put(hash, struct{}{})
				continue
			} else if err != nil {
				return nil, err
			}

			// If this row for the table has already been updated we rewrite the newJoinRow counterpart to ensure that this
			// returned row is not incorrectly counted by the update accumulator.
			tableToNewRowMap[tableName] = oldTableRow
		}

		newJoinRow = recreateRowFromMap(tableToNewRowMap, u.joinSchema)
		equals, err := oldJoinRow.Equals(newJoinRow, u.joinSchema)
		if err != nil {
			return nil, err
		}
		if !equals {
			return append(oldJoinRow, newJoinRow...), nil
		}
	}
}

func (u *updateJoinIter) Close(context *sql.Context) error {
	for _, disposeF := range u.disposals {
		disposeF()
	}

	return u.updateSourceIter.Close(context)
}

func (u *updateJoinIter) getOrCreateCache(ctx *sql.Context, tableName string) sql.KeyValueCache {
	potential, exists := u.caches[tableName]
	if exists {
		return potential
	}

	cache, disposal := ctx.Memory.NewHistoryCache()
	u.caches[tableName] = cache
	u.disposals[tableName] = disposal

	return cache
}

// updatableJoinTable manages the update of multiple tables.
type updatableJoinTable struct {
	updaters map[string]sql.RowUpdater
	joinNode sql.Node
}

var _ sql.UpdatableTable = (*updatableJoinTable)(nil)

// Partitions implements the sql.UpdatableTable interface.
func (u *updatableJoinTable) Partitions(context *sql.Context) (sql.PartitionIter, error) {
	panic("this method should not be called")
}

// PartitionsRows implements the sql.UpdatableTable interface.
func (u *updatableJoinTable) PartitionRows(context *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	panic("this method should not be called")
}

// Name implements the sql.UpdatableTable interface.
func (u *updatableJoinTable) Name() string {
	panic("this method should not be called")
}

// String implements the sql.UpdatableTable interface.
func (u *updatableJoinTable) String() string {
	panic("this method should not be called")
}

// Schema implements the sql.UpdatableTable interface.
func (u *updatableJoinTable) Schema() sql.Schema {
	return u.joinNode.Schema()
}

// Updater implements the sql.UpdatableTable interface.
func (u *updatableJoinTable) Updater(ctx *sql.Context) sql.RowUpdater {
	return &updatableJoinUpdater{
		updaterMap: u.updaters,
		schemaMap:  recreateTableSchemaFromJoinSchema(u.joinNode.Schema()),
		joinSchema: u.joinNode.Schema(),
	}
}

// updatableJoinUpdater manages the process of taking a join row and allocating the respective updates to each updatable
// table.
type updatableJoinUpdater struct {
	updaterMap map[string]sql.RowUpdater
	schemaMap  map[string]sql.Schema
	joinSchema sql.Schema
}

var _ sql.RowUpdater = (*updatableJoinUpdater)(nil)

// StatementBegin implements the sql.TableEditor interface.
func (u *updatableJoinUpdater) StatementBegin(ctx *sql.Context) {
	for _, v := range u.updaterMap {
		v.StatementBegin(ctx)
	}
}

// DiscardChanges implements the sql.TableEditor interface.
func (u *updatableJoinUpdater) DiscardChanges(ctx *sql.Context, errorEncountered error) error {
	for _, v := range u.updaterMap {
		err := v.DiscardChanges(ctx, errorEncountered)
		if err != nil {
			return err
		}
	}

	return nil
}

// StatementComplete implements the sql.TableEditor interface.
func (u *updatableJoinUpdater) StatementComplete(ctx *sql.Context) error {
	for _, v := range u.updaterMap {
		err := v.StatementComplete(ctx)

		if err != nil {
			return err
		}
	}

	return nil
}

// Update implements the sql.RowUpdater interface.
func (u *updatableJoinUpdater) Update(ctx *sql.Context, old sql.Row, new sql.Row) error {
	tableToOldRowMap := splitRowIntoTableRowMap(old, u.joinSchema)
	tableToNewRowMap := splitRowIntoTableRowMap(new, u.joinSchema)

	for tableName, updater := range u.updaterMap {
		oldRow := tableToOldRowMap[tableName]
		newRow := tableToNewRowMap[tableName]
		schema := u.schemaMap[tableName]

		eq, err := oldRow.Equals(newRow, schema)
		if err != nil {
			return err
		}

		if !eq {
			err = updater.Update(ctx, oldRow, newRow)
		}

		if err != nil {
			return err
		}
	}

	return nil
}

// Close implements the sql.RowUpdater interface.
func (u *updatableJoinUpdater) Close(ctx *sql.Context) error {
	for _, updater := range u.updaterMap {
		err := updater.Close(ctx)
		if err != nil {
			return err
		}
	}

	return nil
}

// splitRowIntoTableRowMap takes a join table row and breaks into a map of tables and their respective row.
func splitRowIntoTableRowMap(row sql.Row, joinSchema sql.Schema) map[string]sql.Row {
	ret := make(map[string]sql.Row)

	if len(joinSchema) == 0 {
		return ret
	}

	currentTable := joinSchema[0].Source
	currentRow := sql.Row{row[0]}

	for i := 1; i < len(joinSchema); i++ {
		c := joinSchema[i]

		if c.Source != currentTable {
			ret[currentTable] = currentRow
			currentTable = c.Source
			currentRow = sql.Row{row[i]}
		} else {
			currentTable = c.Source
			currentRow = append(currentRow, row[i])
		}
	}

	ret[currentTable] = currentRow

	return ret
}

// recreateRowFromMap takes a join schema and row map and recreates the original join row.
func recreateRowFromMap(rowMap map[string]sql.Row, joinSchema sql.Schema) sql.Row {
	var ret sql.Row

	if len(joinSchema) == 0 {
		return ret
	}

	currentTable := joinSchema[0].Source
	ret = append(ret, rowMap[currentTable]...)

	for i := 1; i < len(joinSchema); i++ {
		c := joinSchema[i]

		if c.Source != currentTable {
			ret = append(ret, rowMap[c.Source]...)
			currentTable = c.Source
		}
	}

	return ret
}
