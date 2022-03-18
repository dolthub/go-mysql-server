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
	"io"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
)

// ForeignKeyHandler handles all referencing and cascading operations that would need to be executed for an operation
// on a table.
type ForeignKeyHandler struct {
	Table      sql.ForeignKeyTable
	Sch        sql.Schema
	Editor     sql.ForeignKeyUpdater
	References []*ForeignKeyReferenceHandler
}

var _ sql.Node = (*ForeignKeyHandler)(nil)
var _ sql.Table = (*ForeignKeyHandler)(nil)
var _ sql.InsertableTable = (*ForeignKeyHandler)(nil)
var _ sql.ReplaceableTable = (*ForeignKeyHandler)(nil)
var _ sql.UpdatableTable = (*ForeignKeyHandler)(nil)
var _ sql.DeletableTable = (*ForeignKeyHandler)(nil)
var _ sql.TableEditor = (*ForeignKeyHandler)(nil)
var _ sql.RowInserter = (*ForeignKeyHandler)(nil)
var _ sql.RowUpdater = (*ForeignKeyHandler)(nil)
var _ sql.RowDeleter = (*ForeignKeyHandler)(nil)

// Resolved implements the interface sql.Node.
func (n *ForeignKeyHandler) Resolved() bool {
	if n.Table == nil || n.Editor == nil {
		return false
	}
	for _, reference := range n.References {
		if !reference.IsInitialized() {
			return false
		}
	}
	return true
}

// String implements the interface sql.Node.
func (n *ForeignKeyHandler) String() string {
	return n.Table.String()
}

// Schema implements the interface sql.Node.
func (n *ForeignKeyHandler) Schema() sql.Schema {
	return n.Sch
}

// Children implements the interface sql.Node.
func (n *ForeignKeyHandler) Children() []sql.Node {
	return nil
}

// RowIter implements the interface sql.Node.
func (n *ForeignKeyHandler) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	return nil, fmt.Errorf("ForeignKeyHandler should not have its RowIter called")
}

// WithChildren implements the interface sql.Node.
func (n *ForeignKeyHandler) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(n, len(children), 0)
	}
	return n, nil
}

// CheckPrivileges implements the interface sql.Node.
func (n *ForeignKeyHandler) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	// The parent node should have already checked for the appropriate privileges, so this should just return true.
	return true
}

// Name implements the interface sql.Table.
func (n *ForeignKeyHandler) Name() string {
	return n.Name()
}

// Partitions implements the interface sql.Table.
func (n *ForeignKeyHandler) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	return n.Partitions(ctx)
}

// PartitionRows implements the interface sql.Table.
func (n *ForeignKeyHandler) PartitionRows(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	return n.PartitionRows(ctx, partition)
}

// Inserter implements the interface sql.InsertableTable.
func (n *ForeignKeyHandler) Inserter(context *sql.Context) sql.RowInserter {
	return n
}

// Replacer implements the interface sql.ReplaceableTable.
func (n *ForeignKeyHandler) Replacer(ctx *sql.Context) sql.RowReplacer {
	return n
}

// Updater implements the interface sql.UpdatableTable.
func (n *ForeignKeyHandler) Updater(ctx *sql.Context) sql.RowUpdater {
	return n
}

// Deleter implements the interface sql.DeletableTable.
func (n *ForeignKeyHandler) Deleter(context *sql.Context) sql.RowDeleter {
	return n
}

// StatementBegin implements the interface sql.TableEditor.
func (n *ForeignKeyHandler) StatementBegin(ctx *sql.Context) {
	//TODO: need to propagate this to all tables that will update their rows
	n.Editor.StatementBegin(ctx)
}

// DiscardChanges implements the interface sql.TableEditor.
func (n *ForeignKeyHandler) DiscardChanges(ctx *sql.Context, errorEncountered error) error {
	return n.Editor.DiscardChanges(ctx, errorEncountered)
}

// StatementComplete implements the interface sql.TableEditor.
func (n *ForeignKeyHandler) StatementComplete(ctx *sql.Context) error {
	return n.Editor.StatementComplete(ctx)
}

// Insert implements the interface sql.RowInserter.
func (n *ForeignKeyHandler) Insert(ctx *sql.Context, row sql.Row) error {
	for _, reference := range n.References {
		if err := reference.CheckReference(ctx, n.Sch, row); err != nil {
			return err
		}
	}
	return n.Editor.Insert(ctx, row)
}

// Update implements the interface sql.RowUpdater.
func (n *ForeignKeyHandler) Update(ctx *sql.Context, old sql.Row, new sql.Row) error {
	for _, reference := range n.References {
		if err := reference.CheckReference(ctx, n.Sch, new); err != nil {
			return err
		}
	}
	err := n.Editor.Update(ctx, old, new)
	if err != nil {
		return err
	}
	return nil
}

// Delete implements the interface sql.RowDeleter.
func (n *ForeignKeyHandler) Delete(ctx *sql.Context, row sql.Row) error {
	return n.Editor.Delete(ctx, row)
}

// Close implements the interface sql.Closer.
func (n *ForeignKeyHandler) Close(ctx *sql.Context) error {
	return n.Editor.Close(ctx)
}

// ForeignKeyReferenceHandler handles references to any parent rows to verify they exist.
type ForeignKeyReferenceHandler struct {
	Index      sql.Index
	ForeignKey sql.ForeignKeyConstraint
	Editor     sql.ForeignKeyUpdater
	// IndexPositions hold the mapping between an index's column position and the source row's column position. Given
	// an index (x1, x2) and a source row (y1, y2, y3) and the relation (x1->y3, x2->y1), this slice would contain
	// [2, 0]. The first index column "x1" maps to the third source column "y3" (so position 2 since it's zero-based),
	// and the second index column "x2" maps to the first source column "y1" (position 0).
	IndexPositions []int
	// AppendTypes hold any types that may be needed to complete an index range's generation. Foreign keys are allowed
	// to use an index's prefix, and indexes expect ranges to reference all of their columns (not just the prefix), so
	// we grab the types of the suffix index columns to append to the range after the prefix columns that we're
	// referencing.
	AppendTypes []sql.Type
}

// IsInitialized returns whether this editor has been initialized.
func (reference *ForeignKeyReferenceHandler) IsInitialized() bool {
	return reference.Editor != nil && reference.Index != nil
}

// CheckReference checks that the given row has an index entry in the referenced table.
func (reference *ForeignKeyReferenceHandler) CheckReference(ctx *sql.Context, sch sql.Schema, row sql.Row) error {
	rang := make(sql.Range, len(reference.IndexPositions)+len(reference.AppendTypes))
	for rangPosition, rowPos := range reference.IndexPositions {
		rowVal := row[rowPos]
		rang[rangPosition] = sql.ClosedRangeColumnExpr(rowVal, rowVal, sch[rowPos].Type)
	}
	for i, appendType := range reference.AppendTypes {
		rang[i+len(reference.IndexPositions)] = sql.AllRangeColumnExpr(appendType)
	}

	lookup, err := reference.Index.NewLookup(ctx, rang)
	if err != nil {
		return err
	}
	editorData := reference.Editor.WithIndexLookup(lookup)
	//TODO: profile this, may need to redesign this or add a fast path
	partIter, err := editorData.Partitions(ctx)
	if err != nil {
		return err
	}
	rowIter := sql.NewTableRowIter(ctx, editorData, partIter)
	defer rowIter.Close(ctx)
	if _, err = rowIter.Next(ctx); err == nil {
		return nil
	}

	keyStrParts := make([]string, len(reference.IndexPositions))
	for i, rowPos := range reference.IndexPositions {
		keyStrParts[i] = fmt.Sprint(row[rowPos])
	}
	keyStr := fmt.Sprintf("[%s]", strings.Join(keyStrParts, ","))
	return sql.ErrForeignKeyChildViolation.New(reference.ForeignKey.Name, reference.ForeignKey.Table,
		reference.ForeignKey.ReferencedTable, keyStr)
}

// CheckTable checks that every row in the table has an index entry in the referenced table.
func (reference *ForeignKeyReferenceHandler) CheckTable(ctx *sql.Context, tbl sql.ForeignKeyTable) error {
	sch := tbl.Schema()
	partIter, err := tbl.Partitions(ctx)
	if err != nil {
		return err
	}
	rowIter := sql.NewTableRowIter(ctx, tbl, partIter)
	defer rowIter.Close(ctx)
	for row, err := rowIter.Next(ctx); err == nil; row, err = rowIter.Next(ctx) {
		err = reference.CheckReference(ctx, sch, row)
		if err != nil {
			return err
		}
	}
	if err != io.EOF {
		return err
	}
	return nil
}
