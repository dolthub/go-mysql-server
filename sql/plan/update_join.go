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
	Updaters map[string]sql.RowUpdater
	UnaryNode
}

// NewUpdateJoin returns an *UpdateJoin node.
func NewUpdateJoin(editorMap map[string]sql.RowUpdater, child sql.Node) *UpdateJoin {
	return &UpdateJoin{
		Updaters:  editorMap,
		UnaryNode: UnaryNode{Child: child},
	}
}

var _ sql.Node = (*UpdateJoin)(nil)
var _ sql.CollationCoercible = (*UpdateJoin)(nil)

// String implements the sql.Node interface.
func (u *UpdateJoin) String() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("Update Join")
	_ = pr.WriteChildren(u.Child.String())
	return pr.String()
}

// DebugString implements the sql.Node interface.
func (u *UpdateJoin) DebugString() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("Update Join")
	_ = pr.WriteChildren(sql.DebugString(u.Child))
	return pr.String()
}

// GetUpdatable returns an updateJoinTable which implements sql.UpdatableTable.
func (u *UpdateJoin) GetUpdatable() sql.UpdatableTable {
	return &updatableJoinTable{
		updaters: u.Updaters,
		joinNode: u.Child.(*UpdateSource).Child,
	}
}

// WithChildren implements the sql.Node interface.
func (u *UpdateJoin) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(u, len(children), 1)
	}

	return NewUpdateJoin(u.Updaters, children[0]), nil
}

func (u *UpdateJoin) IsReadOnly() bool {
	return false
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (u *UpdateJoin) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.GetCoercibility(ctx, u.Child)
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

// Collation implements the sql.Table interface.
func (u *updatableJoinTable) Collation() sql.CollationID {
	return sql.Collation_Default
}

// Updater implements the sql.UpdatableTable interface.
func (u *updatableJoinTable) Updater(ctx *sql.Context) sql.RowUpdater {
	return &updatableJoinUpdater{
		updaterMap: u.updaters,
		schemaMap:  RecreateTableSchemaFromJoinSchema(u.joinNode.Schema()),
		joinSchema: u.joinNode.Schema(),
	}
}

// RecreateTableSchemaFromJoinSchema takes a join schema and recreates each individual tables schema.
func RecreateTableSchemaFromJoinSchema(joinSchema sql.Schema) map[string]sql.Schema {
	ret := make(map[string]sql.Schema, 0)

	for _, c := range joinSchema {
		potential, exists := ret[c.Source]
		if exists {
			ret[c.Source] = append(potential, c)
		} else {
			ret[c.Source] = sql.Schema{c}
		}
	}

	return ret
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
	tableToOldRowMap := SplitRowIntoTableRowMap(old, u.joinSchema)
	tableToNewRowMap := SplitRowIntoTableRowMap(new, u.joinSchema)

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

// SplitRowIntoTableRowMap takes a join table row and breaks into a map of tables and their respective row.
func SplitRowIntoTableRowMap(row sql.Row, joinSchema sql.Schema) map[string]sql.Row {
	ret := make(map[string]sql.Row)

	if len(joinSchema) == 0 {
		return ret
	}

	currentTable := joinSchema[0].Source
	currentRow := sql.UntypedSqlRow{row.GetValue(0)}

	for i := 1; i < len(joinSchema); i++ {
		c := joinSchema[i]

		if c.Source != currentTable {
			ret[currentTable] = currentRow
			currentTable = c.Source
			currentRow = sql.UntypedSqlRow{row.GetValue(i)}
		} else {
			currentTable = c.Source
			currentRow = append(currentRow, row.GetValue(i))
		}
	}

	ret[currentTable] = currentRow

	return ret
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
