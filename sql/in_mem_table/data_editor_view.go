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

package in_mem_table

import (
	"gopkg.in/src-d/go-errors.v1"

	"github.com/gabereiser/go-mysql-server/sql"
)

// ErrNoEntry is returned when an entry is required for the DataEditorView to work, however one cannot be found.
var ErrNoEntry = errors.NewKind("unable to update table due to a missing entry")

// DataEditorConverter handles the conversion of a row intended for one table to be processed on the entry of another
// table. This is used in the DataEditorView.
type DataEditorConverter interface {
	// RowToKey converts the given row into a Key. The Key must resolve to only a single entry.
	RowToKey(ctx *sql.Context, row sql.Row) (Key, error)
	// AddRowToEntry adds the given row to the given Entry, returning a new updated Entry. If the row's data already
	// exists, this should return a primary key violation error.
	AddRowToEntry(ctx *sql.Context, row sql.Row, entry Entry) (Entry, error)
	// RemoveRowFromEntry removes the given row from the given Entry, returning a new updated Entry. If the row's data
	// does not exist, the original Entry should be returned as-is since this is not an error.
	RemoveRowFromEntry(ctx *sql.Context, row sql.Row, entry Entry) (Entry, error)
	// EntryToRows converts the given entry to rows.
	EntryToRows(ctx *sql.Context, entry Entry) ([]sql.Row, error)
}

// DataEditorView allows for a table to process alteration statements on the data of another table, as though it was
// its own data. As a consequence, the DataEditorView may not create entirely new entries on the original table, as it
// may only modify existing ones. This allows for the fields of an entry to appear as entirely new entries for a
// different table, as this is simply a view into that data. If an Entry does not exist, then ErrNoEntry is returned.
type DataEditorView struct {
	data      *Data
	converter DataEditorConverter
}

var _ sql.RowInserter = (*DataEditorView)(nil)
var _ sql.RowUpdater = (*DataEditorView)(nil)
var _ sql.RowDeleter = (*DataEditorView)(nil)
var _ sql.RowReplacer = (*DataEditorView)(nil)

// NewDataEditorView returns a new *DataEditorView.
func NewDataEditorView(data *Data, converter DataEditorConverter) *DataEditorView {
	return &DataEditorView{data, converter}
}

// StatementBegin implements the sql.TableEditor interface.
func (editor *DataEditorView) StatementBegin(ctx *sql.Context) {
	//TODO: implement this
}

// DiscardChanges implements the sql.TableEditor interface.
func (editor *DataEditorView) DiscardChanges(ctx *sql.Context, errorEncountered error) error {
	//TODO: implement this
	return nil
}

// StatementComplete implements the sql.TableEditor interface.
func (editor *DataEditorView) StatementComplete(ctx *sql.Context) error {
	//TODO: implement this
	return nil
}

// Insert implements the sql.RowInserter interface.
func (editor *DataEditorView) Insert(ctx *sql.Context, row sql.Row) (retErr error) {
	key, err := editor.converter.RowToKey(ctx, row)
	if err != nil {
		return err
	}
	entries := editor.data.Get(key)
	if len(entries) != 1 {
		return ErrNoEntry.New()
	}
	entry := entries[0]
	newEntry, err := editor.converter.AddRowToEntry(ctx, row, entry)
	if err != nil {
		return err
	}
	err = editor.data.Remove(ctx, nil, entry)
	if err != nil {
		return err
	}
	defer func() {
		if retErr != nil {
			_ = editor.data.Put(ctx, entry)
		}
	}()
	return editor.data.Put(ctx, newEntry)
}

// Update implements the sql.RowUpdater interface.
func (editor *DataEditorView) Update(ctx *sql.Context, old sql.Row, new sql.Row) (retErr error) {
	// These defers will reverse each step if there is an error further in, preventing an update from leaving
	// the table in a half-updated state.
	oldKey, err := editor.converter.RowToKey(ctx, old)
	if err != nil {
		return err
	}
	oldEntries := editor.data.Get(oldKey)
	if len(oldEntries) != 1 {
		return ErrNoEntry.New()
	}
	oldEntry := oldEntries[0]

	updatedOldEntry, err := editor.converter.RemoveRowFromEntry(ctx, old, oldEntry)
	if err != nil {
		return err
	}
	err = editor.data.Remove(ctx, nil, oldEntry)
	if err != nil {
		return err
	}
	defer func() {
		if retErr != nil {
			_ = editor.data.Put(ctx, oldEntry)
		}
	}()
	err = editor.data.Put(ctx, updatedOldEntry)
	if err != nil {
		return err
	}
	defer func() {
		if retErr != nil {
			_ = editor.data.Remove(ctx, nil, updatedOldEntry)
		}
	}()

	newKey, err := editor.converter.RowToKey(ctx, new)
	if err != nil {
		return err
	}
	newEntries := editor.data.Get(newKey)
	if len(newEntries) != 1 {
		return ErrNoEntry.New()
	}
	newEntry := newEntries[0]

	updatedNewEntry, err := editor.converter.AddRowToEntry(ctx, new, newEntry)
	if err != nil {
		return err
	}
	err = editor.data.Remove(ctx, nil, newEntry)
	if err != nil {
		return err
	}
	defer func() {
		if retErr != nil {
			_ = editor.data.Put(ctx, newEntry)
		}
	}()
	return editor.data.Put(ctx, updatedNewEntry)
}

// Delete implements the sql.RowDeleter interface.
func (editor *DataEditorView) Delete(ctx *sql.Context, row sql.Row) (retErr error) {
	key, err := editor.converter.RowToKey(ctx, row)
	if err != nil {
		return err
	}
	entries := editor.data.Get(key)
	if len(entries) != 1 {
		return ErrNoEntry.New()
	}
	entry := entries[0]
	newEntry, err := editor.converter.RemoveRowFromEntry(ctx, row, entry)
	if err != nil {
		return err
	}
	err = editor.data.Remove(ctx, nil, entry)
	if err != nil {
		return err
	}
	defer func() {
		if retErr != nil {
			_ = editor.data.Put(ctx, entry)
		}
	}()
	return editor.data.Put(ctx, newEntry)
}

// Close implements the sql.Closer interface.
func (editor *DataEditorView) Close(ctx *sql.Context) error {
	return nil
}
