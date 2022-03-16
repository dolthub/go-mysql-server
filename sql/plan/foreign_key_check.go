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

//TODO: doc and check field names
type ForeignKeyReferenceChecker struct {
	Table      sql.ForeignKeyTable
	Sch        sql.Schema
	Editor     sql.ForeignKeyUpdater
	References []*ForeignKeyReference
}

var _ sql.Node = (*ForeignKeyReferenceChecker)(nil)
var _ sql.Table = (*ForeignKeyReferenceChecker)(nil)
var _ sql.InsertableTable = (*ForeignKeyReferenceChecker)(nil)
var _ sql.ReplaceableTable = (*ForeignKeyReferenceChecker)(nil)
var _ sql.UpdatableTable = (*ForeignKeyReferenceChecker)(nil)
var _ sql.DeletableTable = (*ForeignKeyReferenceChecker)(nil)
var _ sql.TableEditor = (*ForeignKeyReferenceChecker)(nil)
var _ sql.RowInserter = (*ForeignKeyReferenceChecker)(nil)
var _ sql.RowUpdater = (*ForeignKeyReferenceChecker)(nil)
var _ sql.RowDeleter = (*ForeignKeyReferenceChecker)(nil)

// Resolved implements the interface sql.Node.
func (n *ForeignKeyReferenceChecker) Resolved() bool {
	//TODO: figure out resolutions
	return true
}

// String implements the interface sql.Node.
func (n *ForeignKeyReferenceChecker) String() string {
	return n.Table.String()
}

// Schema implements the interface sql.Node.
func (n *ForeignKeyReferenceChecker) Schema() sql.Schema {
	return n.Sch
}

// Children implements the interface sql.Node.
func (n *ForeignKeyReferenceChecker) Children() []sql.Node {
	return nil
}

// RowIter implements the interface sql.Node.
func (n *ForeignKeyReferenceChecker) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	return nil, fmt.Errorf("ForeignKeyReferenceChecker should not have its RowIter called")
}

// WithChildren implements the interface sql.Node.
func (n *ForeignKeyReferenceChecker) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(n, len(children), 0)
	}
	return n, nil
}

// CheckPrivileges implements the interface sql.Node.
func (n *ForeignKeyReferenceChecker) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	//TODO: figure out the privileges
	return true
}

// Name implements the interface sql.Table.
func (n *ForeignKeyReferenceChecker) Name() string {
	return n.Name()
}

// Partitions implements the interface sql.Table.
func (n *ForeignKeyReferenceChecker) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	return n.Partitions(ctx)
}

// PartitionRows implements the interface sql.Table.
func (n *ForeignKeyReferenceChecker) PartitionRows(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	return n.PartitionRows(ctx, partition)
}

// Inserter implements the interface sql.InsertableTable.
func (n *ForeignKeyReferenceChecker) Inserter(context *sql.Context) sql.RowInserter {
	return n
}

// Replacer implements the interface sql.ReplaceableTable.
func (n *ForeignKeyReferenceChecker) Replacer(ctx *sql.Context) sql.RowReplacer {
	return n
}

// Updater implements the interface sql.UpdatableTable.
func (n *ForeignKeyReferenceChecker) Updater(ctx *sql.Context) sql.RowUpdater {
	return n
}

// Deleter implements the interface sql.DeletableTable.
func (n *ForeignKeyReferenceChecker) Deleter(context *sql.Context) sql.RowDeleter {
	return n
}

// StatementBegin implements the interface sql.TableEditor.
func (n *ForeignKeyReferenceChecker) StatementBegin(ctx *sql.Context) {
	n.Editor.StatementBegin(ctx)
}

// DiscardChanges implements the interface sql.TableEditor.
func (n *ForeignKeyReferenceChecker) DiscardChanges(ctx *sql.Context, errorEncountered error) error {
	return n.Editor.DiscardChanges(ctx, errorEncountered)
}

// StatementComplete implements the interface sql.TableEditor.
func (n *ForeignKeyReferenceChecker) StatementComplete(ctx *sql.Context) error {
	return n.Editor.StatementComplete(ctx)
}

// Insert implements the interface sql.RowInserter.
func (n *ForeignKeyReferenceChecker) Insert(ctx *sql.Context, row sql.Row) error {
	for _, reference := range n.References {
		if err := reference.CheckReference(ctx, n.Sch, row); err != nil {
			return err
		}
	}
	return n.Editor.Insert(ctx, row)
}

// Update implements the interface sql.RowUpdater.
func (n *ForeignKeyReferenceChecker) Update(ctx *sql.Context, old sql.Row, new sql.Row) error {
	err := n.Editor.Update(ctx, old, new)
	if err != nil {
		return err
	}
	//TODO: CASCADE stuff?
	return nil
}

// Delete implements the interface sql.RowDeleter.
func (n *ForeignKeyReferenceChecker) Delete(ctx *sql.Context, row sql.Row) error {
	err := n.Editor.Insert(ctx, row)
	if err != nil {
		return err
	}
	//TODO: CASCADE stuff?
	return nil
}

// Close implements the interface sql.Closer.
func (n *ForeignKeyReferenceChecker) Close(ctx *sql.Context) error {
	return n.Editor.Close(ctx)
}

//TODO: doc
type ForeignKeyReference struct {
	Index          sql.Index
	ForeignKey     sql.ForeignKeyConstraint
	Editor         sql.ForeignKeyUpdater
	IndexPositions []int //TODO: doc
	AppendTypes    []sql.Type
}

// IsInitialized returns whether this editor has been initialized.
func (reference *ForeignKeyReference) IsInitialized() bool {
	return reference.Editor != nil && reference.Index != nil
}

// CheckReference checks that the given row has an index entry in the referenced table.
func (reference *ForeignKeyReference) CheckReference(ctx *sql.Context, sch sql.Schema, row sql.Row) error {
	rang := make(sql.Range, len(reference.IndexPositions)+len(reference.AppendTypes))
	for rangPosition, rowPos := range reference.IndexPositions {
		rowVal := row[rowPos]
		//TODO: should I use schema type or index type? does it matter?
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
func (reference *ForeignKeyReference) CheckTable(ctx *sql.Context, tbl sql.ForeignKeyTable) error {
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
