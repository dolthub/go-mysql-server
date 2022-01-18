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

// InMemTableDataKey represents a key that will be matched in order to retrieve a row. All implementations of this
// interface must NOT be a pointer, as pointers have different rules for determining equality.
type InMemTableDataKey interface {
	// AssignValues returns a new key with the given values. These values will be in the same order as their represented
	// columns (as returned by RepresentedColumns). The new key's type is expected to match the calling key's type.
	AssignValues(vals ...interface{}) (InMemTableDataKey, error)
	// RepresentedColumns returns the index positions of the columns that represent this key, based on the table's schema.
	RepresentedColumns() []uint16
}

// InMemTableData is used for in-memory tables to store their row data in a way that may be quickly retrieved for queries.
type InMemTableData struct {
	sch                 sql.Schema
	mutex               *sync.RWMutex
	count               int64
	primaryReferenceKey InMemTableDataKey
	otherReferenceKeys  []InMemTableDataKey
	data                map[reflect.Type]map[InMemTableDataKey][]sql.Row
}

// NewInMemTableData returns a new *InMemTableData. A primary key must be given, with additional keys (secondary)
// optional. If a key is given, it must not be defined as a pointer type.
func NewInMemTableData(sch sql.Schema, primaryKey InMemTableDataKey, secondaryKeys []InMemTableDataKey) *InMemTableData {
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
	return &InMemTableData{
		sch,
		&sync.RWMutex{},
		0,
		primaryKey,
		secondaryKeys,
		make(map[reflect.Type]map[InMemTableDataKey][]sql.Row),
	}
}

// Count returns this table's number of rows.
func (imtd *InMemTableData) Count() int64 {
	imtd.mutex.RLock()
	defer imtd.mutex.RUnlock()
	return imtd.count
}

// Get returns the rows matching the given key.
func (imtd *InMemTableData) Get(key InMemTableDataKey) []sql.Row {
	imtd.mutex.RLock()
	defer imtd.mutex.RUnlock()
	keyType := reflect.TypeOf(key)
	indexedData, ok := imtd.data[keyType]
	if !ok {
		return nil
	}
	return indexedData[key]
}

// Has returns whether the given row is found in the table.
func (imtd *InMemTableData) Has(row sql.Row) bool {
	imtd.mutex.RLock()
	defer imtd.mutex.RUnlock()
	rowIndexes := imtd.primaryReferenceKey.RepresentedColumns()
	vals := make([]interface{}, len(rowIndexes))
	for i, rowIndex := range rowIndexes {
		vals[i] = row[rowIndex]
	}
	key, err := imtd.primaryReferenceKey.AssignValues(vals...)
	if err != nil {
		return false
	}

	keyType := reflect.TypeOf(key)
	indexedData, ok := imtd.data[keyType]
	if !ok {
		return false
	}
	rows, ok := indexedData[key]
	if !ok {
		return false
	}
	for _, ourRow := range rows {
		if ok, err := ourRow.Equals(row, imtd.sch); err == nil && ok {
			return true
		}
	}
	return false
}

// Put adds the given row to the data.
func (imtd *InMemTableData) Put(row sql.Row) error {
	imtd.mutex.Lock()
	defer imtd.mutex.Unlock()
	row = row.Copy()
	isDuplicateRow := true
	for _, referenceKey := range append(imtd.otherReferenceKeys, imtd.primaryReferenceKey) {
		rowIndexes := referenceKey.RepresentedColumns()
		vals := make([]interface{}, len(rowIndexes))
		for i, rowIndex := range rowIndexes {
			vals[i] = row[rowIndex]
		}
		key, err := referenceKey.AssignValues(vals...)
		if err != nil {
			return err
		}

		keyType := reflect.TypeOf(key)
		indexedData, ok := imtd.data[keyType]
		if !ok {
			indexedData = make(map[InMemTableDataKey][]sql.Row)
			imtd.data[keyType] = indexedData
			indexedData[key] = []sql.Row{row}
		} else {
			existingRows := indexedData[key]
			found := false
			for _, existingRow := range existingRows {
				if ok, err := existingRow.Equals(row, imtd.sch); err != nil {
					return err
				} else if ok {
					found = true
					break
				}
			}
			if !found {
				indexedData[key] = append(indexedData[key], row)
				isDuplicateRow = false
			}
		}
	}
	if !isDuplicateRow {
		imtd.count++
	}
	return nil
}

// Remove will completely remove the given key and/or row. If the given key is not nil, then all matching rows are
// found and removed. If the given row is not nil, then only that row is removed.
func (imtd *InMemTableData) Remove(key InMemTableDataKey, row sql.Row) error {
	if key != nil {
		existingRows := imtd.Get(key)
		for _, existingRow := range existingRows {
			if err := imtd.Remove(nil, existingRow); err != nil {
				return err
			}
		}
	}
	if row != nil {
		imtd.mutex.Lock()
		defer imtd.mutex.Unlock()
		rowExisted := false
		for _, referenceKey := range append(imtd.otherReferenceKeys, imtd.primaryReferenceKey) {
			rowIndexes := referenceKey.RepresentedColumns()
			vals := make([]interface{}, len(rowIndexes))
			for i, rowIndex := range rowIndexes {
				vals[i] = row[rowIndex]
			}
			key, err := referenceKey.AssignValues(vals...)
			if err != nil {
				return err
			}

			keyType := reflect.TypeOf(key)
			indexedData, ok := imtd.data[keyType]
			if ok {
				existingRows, ok := indexedData[key]
				if ok {
					for i, existingRow := range existingRows {
						if ok, err := existingRow.Equals(row, imtd.sch); err != nil {
							return err
						} else if ok {
							indexedData[key] = append(existingRows[:i], existingRows[i+1:]...)
							rowExisted = true
							break
						}
					}
				}
			}
		}
		if rowExisted {
			imtd.count--
		}
	}
	return nil
}

// Clear removes all rows.
func (imtd *InMemTableData) Clear() {
	imtd.mutex.Lock()
	defer imtd.mutex.Unlock()
	imtd.count = 0
	imtd.data = make(map[reflect.Type]map[InMemTableDataKey][]sql.Row)
}

// ToRowIter returns a RowIter containing all of this table's rows.
func (imtd *InMemTableData) ToRowIter() sql.RowIter {
	imtd.mutex.RLock()
	defer imtd.mutex.RUnlock()
	var rows []sql.Row
	for _, indexedData := range imtd.data {
		for _, ourRows := range indexedData {
			for _, ourRow := range ourRows {
				rows = append(rows, ourRow.Copy())
			}
		}
		break
	}
	return sql.RowsToRowIter(rows...)
}

// InMemTableDataEditor allows for a table to process alteration statements on its data.
type InMemTableDataEditor struct {
	data *InMemTableData
}

var _ sql.RowInserter = (*InMemTableDataEditor)(nil)
var _ sql.RowUpdater = (*InMemTableDataEditor)(nil)
var _ sql.RowDeleter = (*InMemTableDataEditor)(nil)
var _ sql.RowReplacer = (*InMemTableDataEditor)(nil)

// NewInMemTableDataEditor returns a new *InMemTableDataEditor.
func NewInMemTableDataEditor(data *InMemTableData) *InMemTableDataEditor {
	return &InMemTableDataEditor{data}
}

// StatementBegin implements the TableEditor interface.
func (editor *InMemTableDataEditor) StatementBegin(ctx *sql.Context) {
	//TODO: implement this
}

// DiscardChanges implements the TableEditor interface.
func (editor *InMemTableDataEditor) DiscardChanges(ctx *sql.Context, errorEncountered error) error {
	//TODO: implement this
	return nil
}

// StatementComplete implements the TableEditor interface.
func (editor *InMemTableDataEditor) StatementComplete(ctx *sql.Context) error {
	//TODO: implement this
	return nil
}

// Insert implements the RowInserter interface.
func (editor *InMemTableDataEditor) Insert(ctx *sql.Context, row sql.Row) error {
	if editor.data.Has(row) {
		return sql.ErrPrimaryKeyViolation.New()
	}
	return editor.data.Put(row)
}

// Update implements the RowUpdater interface.
func (editor *InMemTableDataEditor) Update(ctx *sql.Context, old sql.Row, new sql.Row) error {
	err := editor.data.Remove(nil, old)
	if err != nil {
		return err
	}
	return editor.data.Put(new)
}

// Delete implements the RowDeleter interface.
func (editor *InMemTableDataEditor) Delete(ctx *sql.Context, row sql.Row) error {
	return editor.data.Remove(nil, row)
}

// Close implements the Closer interface.
func (editor *InMemTableDataEditor) Close(ctx *sql.Context) error {
	return nil
}
