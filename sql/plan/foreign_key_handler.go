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

	"github.com/dolthub/go-mysql-server/sql"
)

// ForeignKeyHandler handles all referencing and cascading operations that would need to be executed for an operation
// on a table.
type ForeignKeyHandler struct {
	Table        sql.ForeignKeyTable
	Sch          sql.Schema
	OriginalNode sql.Node
	// Editor describes the foreign key graph. The handler holds no table editors: each execution opens one for every
	// table in Tables, which is indexed by ForeignKeyEditor.Slot and is nil for slots the graph doesn't use.
	Editor                     *ForeignKeyEditor
	Tables                     []sql.ForeignKeyTable
	CheckReferencesAfterInsert bool // allows a duplicate-key error to take precedence over FK validation
}

func (n *ForeignKeyHandler) Underlying() sql.Table {
	return n.Table
}

var _ sql.Node = (*ForeignKeyHandler)(nil)
var _ sql.CollationCoercible = (*ForeignKeyHandler)(nil)
var _ sql.Table = (*ForeignKeyHandler)(nil)
var _ sql.InsertableTable = (*ForeignKeyHandler)(nil)
var _ sql.ReplaceableTable = (*ForeignKeyHandler)(nil)
var _ sql.UpdatableTable = (*ForeignKeyHandler)(nil)
var _ sql.DeletableTable = (*ForeignKeyHandler)(nil)
var _ sql.TableWrapper = (*ForeignKeyHandler)(nil)

// Resolved implements the interface sql.Node.
func (n *ForeignKeyHandler) Resolved() bool {
	return n.OriginalNode.Resolved() && n.Editor.IsInitialized(make(map[*ForeignKeyEditor]struct{}))
}

// String implements the interface sql.Node.
func (n *ForeignKeyHandler) String() string {
	return n.OriginalNode.String()
}

func (n *ForeignKeyHandler) DebugString(ctx *sql.Context) string {
	return sql.DebugString(ctx, n.OriginalNode)
}

// Schema implements the interface sql.Node.
func (n *ForeignKeyHandler) Schema(ctx *sql.Context) sql.Schema {
	return n.OriginalNode.Schema(ctx)
}

func (n *ForeignKeyHandler) IsReadOnly() bool {
	// false?
	return n.OriginalNode.IsReadOnly()
}

// Collation implements the interface sql.Node.
func (n *ForeignKeyHandler) Collation() sql.CollationID {
	originalTable, ok := n.OriginalNode.(sql.Table)
	if !ok {
		return sql.Collation_Default
	}
	return originalTable.Collation()
}

// Children implements the interface sql.Node.
func (n *ForeignKeyHandler) Children() []sql.Node {
	return []sql.Node{n.OriginalNode}
}

// WithChildren implements the interface sql.Node.
func (n *ForeignKeyHandler) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(n, len(children), 1)
	}
	nn := *n
	nn.OriginalNode = children[0]
	return &nn, nil
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*ForeignKeyHandler) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 7
}

// Name implements the interface sql.Table.
func (n *ForeignKeyHandler) Name() string {
	return n.Table.Name()
}

// Partitions implements the interface sql.Table.
func (n *ForeignKeyHandler) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	originalTable, ok := n.OriginalNode.(sql.Table)
	if !ok {
		return nil, fmt.Errorf("cannot partition foreign key handler due to the original node not being a table")
	}
	return originalTable.Partitions(ctx)
}

// PartitionRows implements the interface sql.Table.
func (n *ForeignKeyHandler) PartitionRows(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	originalTable, ok := n.OriginalNode.(sql.Table)
	if !ok {
		return nil, fmt.Errorf("cannot partition rows on foreign key handler due to the original node not being a table")
	}
	return originalTable.PartitionRows(ctx, partition)
}

// Inserter implements the interface sql.InsertableTable.
func (n *ForeignKeyHandler) Inserter(ctx *sql.Context) sql.RowInserter {
	return n.open(ctx)
}

// Replacer implements the interface sql.ReplaceableTable.
func (n *ForeignKeyHandler) Replacer(ctx *sql.Context) sql.RowReplacer {
	return n.open(ctx)
}

// Updater implements the interface sql.UpdatableTable.
func (n *ForeignKeyHandler) Updater(ctx *sql.Context) sql.RowUpdater {
	return n.open(ctx)
}

// Deleter implements the interface sql.DeletableTable.
func (n *ForeignKeyHandler) Deleter(ctx *sql.Context) sql.RowDeleter {
	return n.open(ctx)
}

// open opens an editor for each of Tables and returns the editor that runs this handler's foreign key graph through
// them. Closing it closes them.
func (n *ForeignKeyHandler) open(ctx *sql.Context) *ForeignKeyHandlerEditor {
	bySlot := make([]sql.ForeignKeyEditor, len(n.Tables))
	editors := make([]sql.ForeignKeyEditor, 0, len(n.Tables))
	for i, tbl := range n.Tables {
		if tbl != nil {
			bySlot[i] = tbl.GetForeignKeyEditor(ctx)
			editors = append(editors, bySlot[i])
		}
	}
	return &ForeignKeyHandlerEditor{
		Handler: n,
		editor:  n.Editor.Instantiate(bySlot),
		editors: editors,
	}
}

// ForeignKeyHandlerEditor is the editor for one execution of a ForeignKeyHandler. It owns an editor for each of the
// handler's Tables, from when the handler hands it out until it's closed.
type ForeignKeyHandlerEditor struct {
	Handler *ForeignKeyHandler
	editor  *ForeignKeyEditor
	editors []sql.ForeignKeyEditor
}

var _ sql.TableEditor = (*ForeignKeyHandlerEditor)(nil)
var _ sql.RowInserter = (*ForeignKeyHandlerEditor)(nil)
var _ sql.RowUpdater = (*ForeignKeyHandlerEditor)(nil)
var _ sql.RowDeleter = (*ForeignKeyHandlerEditor)(nil)
var _ sql.RowReplacer = (*ForeignKeyHandlerEditor)(nil)

// StatementBegin implements the interface sql.TableEditor.
func (e *ForeignKeyHandlerEditor) StatementBegin(ctx *sql.Context) {
	for _, updater := range e.editors {
		updater.StatementBegin(ctx)
	}
}

// DiscardChanges implements the interface sql.TableEditor.
func (e *ForeignKeyHandlerEditor) DiscardChanges(ctx *sql.Context, errorEncountered error) error {
	var err error
	for _, updater := range e.editors {
		nErr := updater.DiscardChanges(ctx, errorEncountered)
		if err == nil {
			err = nErr
		}
	}
	return err
}

// StatementComplete implements the interface sql.TableEditor.
func (e *ForeignKeyHandlerEditor) StatementComplete(ctx *sql.Context) error {
	var err error
	for _, updater := range e.editors {
		nErr := updater.StatementComplete(ctx)
		if err == nil {
			err = nErr
		}
	}
	return err
}

// Insert implements the interface sql.RowInserter.
func (e *ForeignKeyHandlerEditor) Insert(ctx *sql.Context, row sql.Row) error {
	if !e.Handler.CheckReferencesAfterInsert {
		for _, reference := range e.editor.References {
			if err := reference.CheckReference(ctx, row); err != nil {
				return err
			}
		}
	}
	if err := e.editor.Editor.Insert(ctx, row); err != nil {
		return err
	}
	if e.Handler.CheckReferencesAfterInsert {
		for _, reference := range e.editor.References {
			if err := reference.CheckReference(ctx, row); err != nil {
				return err
			}
		}
	}
	return nil
}

// HasUniqueKeyConflict implements sql.UniqueKeyConflictCheckingRowInserter.
func (e *ForeignKeyHandlerEditor) HasUniqueKeyConflict(ctx *sql.Context, row sql.Row, columns []string) (bool, error) {
	checker, ok := e.editor.Editor.(sql.UniqueKeyConflictCheckingRowInserter)
	if !ok {
		return false, nil
	}
	return checker.HasUniqueKeyConflict(ctx, row, columns)
}

// Update implements the interface sql.RowUpdater.
func (e *ForeignKeyHandlerEditor) Update(ctx *sql.Context, old sql.Row, new sql.Row) error {
	return e.editor.Update(ctx, old, new, 1)
}

// Delete implements the interface sql.RowDeleter.
func (e *ForeignKeyHandlerEditor) Delete(ctx *sql.Context, row sql.Row) error {
	return e.editor.Delete(ctx, row, 1)
}

// Close implements the interface sql.Closer.
func (e *ForeignKeyHandlerEditor) Close(ctx *sql.Context) error {
	var err error
	for _, updater := range e.editors {
		nErr := updater.Close(ctx)
		if err == nil {
			err = nErr
		}
	}
	return err
}
