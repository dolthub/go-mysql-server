// Copyright 2021 Dolthub, Inc.
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
	"fmt"
	"reflect"
	"sync"

	"github.com/dolthub/go-mysql-server/sql"
)

// Eventually these structures should be replaced by memory tables, however at the time of writing this file, the
// memory tables strive for correctness rather than speed (as they're primarily used for testing functionality rather
// than for actual usage). They are also not built with concurrency in mind. Rather than rewriting them, these
// structures were made. For internal tables that need to quickly access data, such as the Grant Tables, this provides
// a more optimized alternative to the memory tables for rapid access (such as verifying every statement for the
// correct privileges). Although alteration is supported, it is not the focus.
//
// For clarification, these are "in-memory tables", which are different from the "memory tables". The "memory tables"
// reside in a folder in the project's root, under `/memory`. It contains a full table implementation, from auto-
// incrementing columns to foreign key support. For now, "in-memory tables" are to be used for tables that exist outside
// of an integrator's domain, while requiring alteration. This is unlike the information_schema tables, which do not
// allow any kind of direct alteration.

// Key represents a key that will be matched in order to retrieve a row. All implementations of this interface must NOT
// be a pointer, as pointers have different rules for determining equality.
type Key interface {
	// KeyFromEntry returns a new key from the given entry. The new key's type is expected to match the calling key's type.
	KeyFromEntry(ctx *sql.Context, entry Entry) (Key, error)
	// KeyFromRow returns a new key from the given sql.Row. The given row will match the schema of the parent table. The
	// new key's type is expected to match the calling key's type.
	KeyFromRow(ctx *sql.Context, row sql.Row) (Key, error)
}

// Entry is an entry in Data. It handles conversions to and from a sql.Row, so that the underlying
// representation does not have to be a sql.Row. As the sql.Row interface is only important in the context of table
// operations, the data type stored may be of any type as long as it can convert between itself and sql.Row, as well as
// testing equality against other entries. All rows will match the schema of the parent table.
type Entry interface {
	// NewFromRow takes the given row and returns a new instance of the Entry containing the properties of the given row.
	NewFromRow(ctx *sql.Context, row sql.Row) (Entry, error)
	// UpdateFromRow uses the given row to return a new Entry that is based on the calling Entry. This means that any
	// fields that do not have a direct mapping to or from a sql.Row should be preserved on the new Entry.
	UpdateFromRow(ctx *sql.Context, row sql.Row) (Entry, error)
	// ToRow returns this Entry as a sql.Row.
	ToRow(ctx *sql.Context) sql.Row
	// Equals returns whether the calling entry is equivalent to the given entry. Standard struct comparison may work
	// for some entries, however other implementations may have fields that should not be considered when checking for
	// equality, therefore such implementations can make the comparable fields explicit.
	Equals(ctx *sql.Context, otherEntry Entry) bool
}

// TODO: Whenever we update to Go 1.18 for generics, make this a generic type with a constraint for types inheriting the Entry interface.

// Data is used for in-memory tables to store their row data in a way that may be quickly retrieved for queries.
type Data struct {
	mutex               *sync.RWMutex
	count               int64
	primaryReferenceKey Key
	otherReferenceKeys  []Key
	entryReference      Entry
	entryType           reflect.Type
	data                map[reflect.Type]map[Key][]Entry
}

// NewData returns a new *Data. A primary key must be given, with additional keys (secondary) optional. If a key is
// given, it must not be defined as a pointer type. The given Entry will be used to in the TableEditor to interact with
// rows.
func NewData(entryReference Entry, primaryKey Key, secondaryKeys []Key) *Data {
	if primaryKey == nil {
		panic("in memory table primary key cannot be nil")
	}
	if reflect.TypeOf(primaryKey).Kind() == reflect.Ptr {
		panic("in memory table primary key must not be a pointer type")
	}
	for _, secondaryKey := range secondaryKeys {
		if secondaryKey == nil {
			panic("in memory table secondary keys cannot be nil")
		}
		if reflect.TypeOf(secondaryKey).Kind() == reflect.Ptr {
			panic("in memory table secondary keys must not be a pointer types")
		}
	}
	return &Data{
		&sync.RWMutex{},
		0,
		primaryKey,
		secondaryKeys,
		entryReference,
		reflect.TypeOf(entryReference),
		make(map[reflect.Type]map[Key][]Entry),
	}
}

// Count returns this table's number of rows.
func (data *Data) Count() int64 {
	data.mutex.RLock()
	defer data.mutex.RUnlock()
	return data.count
}

// Get returns the entries matching the given key.
func (data *Data) Get(key Key) []Entry {
	data.mutex.RLock()
	defer data.mutex.RUnlock()
	keyType := reflect.TypeOf(key)
	indexedData, ok := data.data[keyType]
	if !ok {
		return nil
	}
	return indexedData[key]
}

// Has returns whether the given Entry is found in the table.
func (data *Data) Has(ctx *sql.Context, entry Entry) bool {
	data.mutex.RLock()
	defer data.mutex.RUnlock()
	key, err := data.primaryReferenceKey.KeyFromEntry(ctx, entry)
	if err != nil {
		return false
	}

	keyType := reflect.TypeOf(key)
	indexedData, ok := data.data[keyType]
	if !ok {
		return false
	}
	entries, ok := indexedData[key]
	if !ok {
		return false
	}
	for _, ourEntry := range entries {
		if ourEntry.Equals(ctx, entry); ok {
			return true
		}
	}
	return false
}

// Put adds the given Entry to the data.
func (data *Data) Put(ctx *sql.Context, entry Entry) error {
	data.mutex.Lock()
	defer data.mutex.Unlock()
	if reflect.TypeOf(entry) != data.entryType {
		return fmt.Errorf("expected Entry of type `%T` but got `%T`", data.entryType, reflect.TypeOf(entry))
	}
	isDuplicateEntry := true
	for _, referenceKey := range append(data.otherReferenceKeys, data.primaryReferenceKey) {
		key, err := referenceKey.KeyFromEntry(ctx, entry)
		if err != nil {
			return err
		}

		keyType := reflect.TypeOf(key)
		indexedData, ok := data.data[keyType]
		if !ok {
			indexedData = make(map[Key][]Entry)
			data.data[keyType] = indexedData
			indexedData[key] = []Entry{entry}
		} else {
			existingEntries := indexedData[key]
			found := false
			for _, existingEntry := range existingEntries {
				if existingEntry.Equals(ctx, entry) {
					found = true
					break
				}
			}
			if !found {
				indexedData[key] = append(indexedData[key], entry)
				isDuplicateEntry = false
			}
		}
	}
	if !isDuplicateEntry {
		data.count++
	}
	return nil
}

// Remove will completely remove the given key and/or Entry. If the given key is not nil, then all matching entries are
// found and removed. If the given Entry is not nil, then only that Entry is removed.
func (data *Data) Remove(ctx *sql.Context, key Key, entry Entry) error {
	if key != nil {
		existingEntries := data.Get(key)
		for _, existingEntry := range existingEntries {
			if err := data.Remove(ctx, nil, existingEntry); err != nil {
				return err
			}
		}
	}
	if entry != nil {
		data.mutex.Lock()
		defer data.mutex.Unlock()
		entryExisted := false
		for _, referenceKey := range append(data.otherReferenceKeys, data.primaryReferenceKey) {
			key, err := referenceKey.KeyFromEntry(ctx, entry)
			if err != nil {
				return err
			}

			keyType := reflect.TypeOf(key)
			indexedData, ok := data.data[keyType]
			if ok {
				existingEntries, ok := indexedData[key]
				if ok {
					for i, existingEntry := range existingEntries {
						if existingEntry.Equals(ctx, entry) {
							indexedData[key] = append(existingEntries[:i], existingEntries[i+1:]...)
							entryExisted = true
							break
						}
					}
				}
			}
		}
		if entryExisted {
			data.count--
		}
	}
	return nil
}

// Clear removes all entries.
func (data *Data) Clear() {
	data.mutex.Lock()
	defer data.mutex.Unlock()
	data.count = 0
	data.data = make(map[reflect.Type]map[Key][]Entry)
}

// ToRowIter returns a RowIter containing all of this table's entries as rows.
func (data *Data) ToRowIter(ctx *sql.Context) sql.RowIter {
	data.mutex.RLock()
	defer data.mutex.RUnlock()
	var rows []sql.Row
	for _, indexedData := range data.data {
		for _, ourRows := range indexedData {
			for _, item := range ourRows {
				rows = append(rows, item.ToRow(ctx))
			}
		}
		break
	}
	return sql.RowsToRowIter(rows...)
}

// DataEditor allows for a table to process alteration statements on its data.
type DataEditor struct {
	data *Data
}

var _ sql.RowInserter = (*DataEditor)(nil)
var _ sql.RowUpdater = (*DataEditor)(nil)
var _ sql.RowDeleter = (*DataEditor)(nil)
var _ sql.RowReplacer = (*DataEditor)(nil)

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
func (editor *DataEditor) Update(ctx *sql.Context, old sql.Row, new sql.Row) error {
	oldKey, err := editor.data.primaryReferenceKey.KeyFromRow(ctx, old)
	if err != nil {
		return err
	}
	oldEntries := editor.data.Get(oldKey)
	if len(oldEntries) == 1 {
		// If an entry already exists then we just update it rather than creating a new one. Some entries may have
		// additional data that cannot be represented in a row, and it is important to keep those fields intact.
		oldEntry := oldEntries[0]
		newEntry, err := oldEntry.UpdateFromRow(ctx, new)
		if err != nil {
			return err
		}
		err = editor.data.Remove(ctx, nil, oldEntry)
		if err != nil {
			return err
		}
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
