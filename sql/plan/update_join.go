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

// Resolved implements the sql.Node interface.
func (u *UpdateJoin) Resolved() bool {
	return true
}

// String implements the sql.Node interface.
func (u *UpdateJoin) String() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("Update Join")
	_ = pr.WriteChildren(u.Child.String())
	return pr.String()

}

// Schema implements the sql.Node interface.
func (u *UpdateJoin) Schema() sql.Schema {
	return u.Child.Schema()
}

// Children implements the sql.Node interface.
func (u *UpdateJoin) Children() []sql.Node {
	return []sql.Node{u.Child}
}

// RowIter implements the sql.Node interface.
func (u *UpdateJoin) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	return u.Child.RowIter(ctx, row)
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
		initialUpdaterMap: u.updaters,
		updatedUpdaterMap: u.updaters,
		joinSchema:        u.joinNode.Schema(),
		caches:            make(map[string]sql.KeyValueCache),
		disposals:         make(map[string]sql.DisposeFunc),
	}
}

// updatableJoinUpdater manages the process of taking a join row and allocating the respective updates to each updatable
// table.
type updatableJoinUpdater struct {
	initialUpdaterMap map[string]sql.RowUpdater
	updatedUpdaterMap map[string]sql.RowUpdater
	caches            map[string]sql.KeyValueCache
	disposals         map[string]sql.DisposeFunc
	joinSchema        sql.Schema
}

var _ sql.RowUpdater = (*updatableJoinUpdater)(nil)

// StatementBegin implements the sql.TableEditor interface.
func (u *updatableJoinUpdater) StatementBegin(ctx *sql.Context) {}

// DiscardChanges implements the sql.TableEditor interface.
func (u *updatableJoinUpdater) DiscardChanges(ctx *sql.Context, errorEncountered error) error {
	u.updatedUpdaterMap = u.initialUpdaterMap
	return nil
}

// StatementComplete implements the sql.TableEditor interface.
func (u *updatableJoinUpdater) StatementComplete(ctx *sql.Context) error {
	u.initialUpdaterMap = u.updatedUpdaterMap
	return nil
}

func (u *updatableJoinUpdater) getOrCreateCache(ctx *sql.Context, tableName string) sql.KeyValueCache {
	potential, exists := u.caches[tableName]
	if exists {
		return potential
	}

	cache, disposal := ctx.Memory.NewHistoryCache()
	u.caches[tableName] = cache
	u.disposals[tableName] = disposal

	return cache
}

// Update implements the sql.RowUpdater interface.
func (u *updatableJoinUpdater) Update(ctx *sql.Context, old sql.Row, new sql.Row) error {
	tableToOldRowMap := splitRowIntoTableRowMap(old, u.joinSchema)
	tableToNewRowMap := splitRowIntoTableRowMap(new, u.joinSchema)

	for tableName, updater := range u.updatedUpdaterMap {
		oldRow := tableToOldRowMap[tableName]
		newRow := tableToNewRowMap[tableName]

		// Check if the row has already been updated
		cache := u.getOrCreateCache(ctx, tableName)
		hash, err := sql.HashOf(oldRow)
		if err != nil {
			return err
		}
		val, err := cache.Get(hash)

		if val != nil && val.(bool) == true {
			continue
		}

		err = updater.Update(ctx, oldRow, newRow)
		if err != nil {
			return err
		}

		cache.Put(hash, true)
	}

	return nil
}

// Close implements the sql.RowUpdater interface.
func (u *updatableJoinUpdater) Close(ctx *sql.Context) error {
	for _, updater := range u.updatedUpdaterMap {
		err := updater.Close(ctx)
		if err != nil {
			return err
		}
	}

	for _, disposeF := range u.disposals {
		disposeF()
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
