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

import "github.com/gabereiser/go-mysql-server/sql"

// DataEditor allows for a table to process alteration statements on its data.
type DataEditor struct {
	data *Data
}

var _ sql.TableEditor = (*DataEditor)(nil)

// NewDataEditor returns a new *DataEditor.
func NewDataEditor(data *Data) *DataEditor {
	return &DataEditor{data}
}

// StatementBegin implements the sql.TableEditor interface.
func (editor *DataEditor) StatementBegin(ctx *sql.Context) {
	//TODO: implement this
}

// DiscardChanges implements the sql.TableEditor interface.
func (editor *DataEditor) DiscardChanges(ctx *sql.Context, errorEncountered error) error {
	//TODO: implement this
	return nil
}

// StatementComplete implements the sql.TableEditor interface.
func (editor *DataEditor) StatementComplete(ctx *sql.Context) error {
	//TODO: implement this
	return nil
}

// Insert implements the sql.RowInserter interface.
func (editor *DataEditor) Insert(ctx *sql.Context, row sql.Row) error {
	entry, err := editor.data.entryReference.NewFromRow(ctx, row)
	if err != nil {
		return err
	}
	if editor.data.Has(ctx, entry) {
		return sql.ErrPrimaryKeyViolation.New()
	}
	return editor.data.Put(ctx, entry)
}

// Update implements the sql.RowUpdater interface.
func (editor *DataEditor) Update(ctx *sql.Context, old sql.Row, new sql.Row) (retErr error) {
	oldKey, err := editor.data.primaryReferenceKey.KeyFromRow(ctx, old)
	if err != nil {
		return err
	}
	oldEntries := editor.data.Get(oldKey)
	if len(oldEntries) == 1 {
		// Some entries may have additional data that cannot be represented in a row, and it is important to keep those
		// fields intact.
		oldEntry := oldEntries[0]
		newEntry, err := oldEntry.UpdateFromRow(ctx, new)
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
		return editor.data.Put(ctx, newEntry)
	} else {
		newEntry, err := editor.data.entryReference.NewFromRow(ctx, new)
		if err != nil {
			return err
		}
		err = editor.data.Remove(ctx, oldKey, nil)
		if err != nil {
			return err
		}
		defer func() {
			if retErr != nil {
				for _, oldEntry := range oldEntries {
					_ = editor.data.Put(ctx, oldEntry)
				}
			}
		}()
		return editor.data.Put(ctx, newEntry)
	}
}

// Delete implements the sql.RowDeleter interface.
func (editor *DataEditor) Delete(ctx *sql.Context, row sql.Row) error {
	key, err := editor.data.primaryReferenceKey.KeyFromRow(ctx, row)
	if err != nil {
		return err
	}
	return editor.data.Remove(ctx, key, nil)
}

// Close implements the sql.Closer interface.
func (editor *DataEditor) Close(ctx *sql.Context) error {
	return nil
}
