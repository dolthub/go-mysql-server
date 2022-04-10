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

package memory

import (
	"fmt"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
)

// tableEditor manages the edits that a table receives.
type tableEditor struct {
	table             *Table
	initialAutoIncVal uint64
	initialPartitions map[string][]sql.Row
	ea                tableEditAccumulator
	initialInsert     int
}

var _ sql.RowReplacer = (*tableEditor)(nil)
var _ sql.RowUpdater = (*tableEditor)(nil)
var _ sql.RowInserter = (*tableEditor)(nil)
var _ sql.RowDeleter = (*tableEditor)(nil)
var _ sql.ForeignKeyUpdater = (*tableEditor)(nil)

func (t *tableEditor) Close(ctx *sql.Context) error {
	return t.ea.ApplyEdits(ctx)
}

func (t *tableEditor) StatementBegin(ctx *sql.Context) {
	t.initialInsert = t.table.insertPartIdx
	t.initialAutoIncVal = t.table.autoIncVal
	t.initialPartitions = make(map[string][]sql.Row)
	for partStr, rowSlice := range t.table.partitions {
		newRowSlice := make([]sql.Row, len(rowSlice))
		for i, row := range rowSlice {
			newRowSlice[i] = row.Copy()
		}
		t.initialPartitions[partStr] = newRowSlice
	}
}

func (t *tableEditor) DiscardChanges(ctx *sql.Context, errorEncountered error) error {
	t.table.insertPartIdx = t.initialInsert
	t.table.autoIncVal = t.initialAutoIncVal
	t.table.partitions = t.initialPartitions
	t.ea.Clear()
	return nil
}

func (t *tableEditor) StatementComplete(ctx *sql.Context) error {
	return nil
}

// Insert a new row into the table.
func (t *tableEditor) Insert(ctx *sql.Context, row sql.Row) error {
	if err := checkRow(t.table.schema.Schema, row); err != nil {
		return err
	}

	partitionRow, added, err := t.ea.Get(row)
	if err != nil {
		return err
	}

	if added {
		pkColIdxes := t.pkColumnIndexes()
		vals := make([]interface{}, len(pkColIdxes))
		for i := range pkColIdxes {
			vals[i] = row[pkColIdxes[i]]
		}
		return sql.NewUniqueKeyErr(fmt.Sprint(vals), true, partitionRow)
	}

	err = t.ea.Insert(row)
	if err != nil {
		return err
	}

	idx := t.table.autoColIdx
	if idx >= 0 {
		autoCol := t.table.schema.Schema[idx]
		cmp, err := autoCol.Type.Compare(row[idx], t.table.autoIncVal)
		if err != nil {
			return err
		}
		if cmp > 0 {
			v, err := sql.Uint64.Convert(row[idx])
			if err != nil {
				return err
			}
			t.table.autoIncVal = v.(uint64)
		}
		t.table.autoIncVal++
	}

	return nil
}

// Delete the given row from the table.
func (t *tableEditor) Delete(ctx *sql.Context, row sql.Row) error {
	if err := checkRow(t.table.schema.Schema, row); err != nil {
		return err
	}

	err := t.ea.Delete(row)
	if err != nil {
		return err
	}

	return nil
}

// Update the given row from the table.
func (t *tableEditor) Update(ctx *sql.Context, oldRow sql.Row, newRow sql.Row) error {
	if err := checkRow(t.table.schema.Schema, oldRow); err != nil {
		return err
	}
	if err := checkRow(t.table.schema.Schema, newRow); err != nil {
		return err
	}

	err := t.ea.Delete(oldRow)
	if err != nil {
		return err
	}

	if t.pkColsDiffer(oldRow, newRow) {
		partitionRow, added, err := t.ea.Get(newRow)
		if err != nil {
			return err
		}

		if added {
			pkColIdxes := t.pkColumnIndexes()
			vals := make([]interface{}, len(pkColIdxes))
			for i := range pkColIdxes {
				vals[i] = newRow[pkColIdxes[i]]
			}
			return sql.NewUniqueKeyErr(fmt.Sprint(vals), true, partitionRow)
		}
	}

	err = t.ea.Insert(newRow)
	if err != nil {
		return err
	}

	return nil
}

// SetAutoIncrementValue sets a new AUTO_INCREMENT value
func (t *tableEditor) SetAutoIncrementValue(ctx *sql.Context, val uint64) error {
	t.table.autoIncVal = val
	return nil
}

// WithIndexLookup returns
func (t *tableEditor) WithIndexLookup(lookup sql.IndexLookup) sql.Table {
	//TODO: optimize this, should create some a struct that encloses the tableEditor and filters based on the lookup
	if pkTea, ok := t.ea.(*pkTableEditAccumulator); ok {
		newTable, err := copyTable(pkTea.table, pkTea.table.schema)
		if err != nil {
			panic(err)
		}
		adds := make(map[string]sql.Row)
		deletes := make(map[string]sql.Row)
		for key, val := range pkTea.adds {
			adds[key] = val
		}
		for key, val := range pkTea.deletes {
			deletes[key] = val
		}
		err = (&pkTableEditAccumulator{
			table:   newTable,
			adds:    adds,
			deletes: deletes,
		}).ApplyEdits(sql.NewEmptyContext())
		if err != nil {
			panic(err)
		}
		memoryLookup := lookup.(*IndexLookup)
		lookupIndex := *memoryLookup.idx.(*Index)
		lookupIndex.Tbl = newTable
		memoryLookup.idx = &lookupIndex
		return newTable.WithIndexLookup(memoryLookup)
	} else {
		nonPkTea := t.ea.(*keylessTableEditAccumulator)
		newTable, err := copyTable(nonPkTea.table, nonPkTea.table.schema)
		if err != nil {
			panic(err)
		}
		adds := make([]sql.Row, len(nonPkTea.adds))
		deletes := make([]sql.Row, len(nonPkTea.deletes))
		for i, val := range nonPkTea.adds {
			adds[i] = val
		}
		for i, val := range nonPkTea.deletes {
			deletes[i] = val
		}
		err = (&keylessTableEditAccumulator{
			table:   newTable,
			adds:    adds,
			deletes: deletes,
		}).ApplyEdits(sql.NewEmptyContext())
		if err != nil {
			panic(err)
		}
		memoryLookup := lookup.(*IndexLookup)
		lookupIndex := *memoryLookup.idx.(*Index)
		lookupIndex.Tbl = newTable
		memoryLookup.idx = &lookupIndex
		return newTable.WithIndexLookup(memoryLookup)
	}
}

func (t *tableEditor) pkColumnIndexes() []int {
	var pkColIdxes []int
	for _, column := range t.table.schema.Schema {
		if column.PrimaryKey {
			idx, _ := t.table.getField(column.Name)
			pkColIdxes = append(pkColIdxes, idx)
		}
	}
	return pkColIdxes
}

func (t *tableEditor) pkColsDiffer(row, row2 sql.Row) bool {
	pkColIdxes := t.pkColumnIndexes()
	return !columnsMatch(pkColIdxes, row, row2)
}

// Returns whether the values for the columns given match in the two rows provided
func columnsMatch(colIndexes []int, row sql.Row, row2 sql.Row) bool {
	for _, i := range colIndexes {
		if row[i] != row2[i] {
			return false
		}
	}
	return true
}

// tableEditAccumulator tracks the set of inserts and deletes and applies those edits to a initialTable.
type tableEditAccumulator interface {
	// Insert adds a row to the accumulator to be inserted in the future. Updates are modeled as a delete than an insertPartIdx.
	Insert(value sql.Row) error
	// Delete adds a row to the accumulator to be deleted in the future. Updates are modeled as a delete than an insertPartIdx.
	Delete(value sql.Row) error
	// Get returns a row if found along with two booleans added and deleted. Added is true if a row was inserted. Deleted
	// is true if a row was deleted.
	Get(value sql.Row) (sql.Row, bool, error)
	// ApplyEdits takes a initialTable and runs through a sequence of inserts and deletes that have been stored in the
	// accumulator.
	ApplyEdits(ctx *sql.Context) error
	// Clear wipes all of the stored inserts and deletes that may or may not have been applied.
	Clear()
}

// NewTableEditAccumulator returns a tableEditAccumulator based on the schema.
func NewTableEditAccumulator(t *Table) tableEditAccumulator {
	if sql.IsKeyless(t.schema.Schema) {
		return &keylessTableEditAccumulator{
			table:   t,
			adds:    make([]sql.Row, 0),
			deletes: make([]sql.Row, 0),
		}
	}

	return &pkTableEditAccumulator{
		table:   t,
		adds:    make(map[string]sql.Row),
		deletes: make(map[string]sql.Row),
	}
}

// pkTableEditAccumulator manages the updates of keyed tables. It uses a map to efficiently toggle edits.
type pkTableEditAccumulator struct {
	table   *Table
	adds    map[string]sql.Row
	deletes map[string]sql.Row
}

var _ tableEditAccumulator = (*pkTableEditAccumulator)(nil)

// Insert implements the tableEditAccumulator interface.
func (pke *pkTableEditAccumulator) Insert(value sql.Row) error {
	rowKey := pke.getRowKey(value)
	delete(pke.deletes, rowKey)
	pke.adds[rowKey] = value
	return nil
}

// Delete implements the tableEditAccumulator interface.
func (pke *pkTableEditAccumulator) Delete(value sql.Row) error {
	rowKey := pke.getRowKey(value)

	delete(pke.adds, rowKey)
	pke.deletes[rowKey] = value

	return nil
}

// Get implements the tableEditAccumulator interface.
func (pke *pkTableEditAccumulator) Get(value sql.Row) (sql.Row, bool, error) {
	rowKey := pke.getRowKey(value)

	r, exists := pke.adds[rowKey]
	if exists {
		return r, true, nil
	}

	r, exists = pke.deletes[rowKey]
	if exists {
		return r, false, nil
	}

	pkColIdxes := pke.pkColumnIndexes()
	for _, partition := range pke.table.partitions {
		for _, partitionRow := range partition {
			if columnsMatch(pkColIdxes, partitionRow, value) {
				return partitionRow, true, nil
			}
		}
	}

	return nil, false, nil
}

// ApplyEdits implements the tableEditAccumulator interface.
func (pke *pkTableEditAccumulator) ApplyEdits(ctx *sql.Context) error {
	for _, val := range pke.deletes {
		err := pke.deleteHelper(ctx, pke.table, val)
		if err != nil {
			return err
		}
	}

	for _, val := range pke.adds {
		err := pke.insertHelper(ctx, pke.table, val)
		if err != nil {
			return err
		}
	}

	return nil
}

// Clear implements the tableEditAccumulator interface.
func (pke *pkTableEditAccumulator) Clear() {
	pke.adds = make(map[string]sql.Row)
	pke.deletes = make(map[string]sql.Row)
}

// pkColumnIndexes returns the indexes of the primary partitionKeys in the initialized table.
func (pke *pkTableEditAccumulator) pkColumnIndexes() []int {
	return pke.table.schema.PkOrdinals
}

// getRowKey returns a sql.Row of the primary partitionKeys a row in relation with the initialized table.
func (pke *pkTableEditAccumulator) getRowKey(r sql.Row) string {
	var rowKey strings.Builder
	for _, i := range pke.table.schema.PkOrdinals {
		rowKey.WriteString(fmt.Sprintf("%v", r[i]))
	}
	return rowKey.String()
}

// deleteHelper deletes the given row from the table.
func (pke *pkTableEditAccumulator) deleteHelper(ctx *sql.Context, table *Table, row sql.Row) error {
	if err := checkRow(table.schema.Schema, row); err != nil {
		return err
	}

	matches := false
	for partitionIndex, partition := range table.partitions {
		for partitionRowIndex, partitionRow := range partition {
			matches = true

			// For DELETE queries, we will have previously selected the row in order to delete it. For REPLACE, we will just
			// have the row to be replaced, so we need to consider primary key information.
			pkColIdxes := pke.pkColumnIndexes()
			if len(pkColIdxes) > 0 {
				if columnsMatch(pkColIdxes, partitionRow, row) {
					table.partitions[partitionIndex] = append(partition[:partitionRowIndex], partition[partitionRowIndex+1:]...)
					break
				}
			}

			var err error
			matches, err = rowsAreEqual(ctx, table.schema.Schema, row, partitionRow)
			if err != nil {
				return err
			}

			if matches {
				table.partitions[partitionIndex] = append(partition[:partitionRowIndex], partition[partitionRowIndex+1:]...)
				break
			}
		}
		if matches {
			break
		}
	}

	return nil
}

// insertHelper inserts the given row into the given table.
func (pke *pkTableEditAccumulator) insertHelper(ctx *sql.Context, table *Table, row sql.Row) error {
	key := string(table.partitionKeys[table.insertPartIdx])
	table.insertPartIdx++
	if table.insertPartIdx == len(table.partitionKeys) {
		table.insertPartIdx = 0
	}

	pkColIdxes := pke.pkColumnIndexes()
	savedPartitionIndex := ""
	savedPartitionRowIndex := -1
	if len(pkColIdxes) > 0 {
		for partitionIndex, partition := range table.partitions {
			for partitionRowIndex, partitionRow := range partition {
				if columnsMatch(pkColIdxes, partitionRow, row) {
					// Instead of throwing a unique key error, we perform an update operation to essentially represent
					// map semantics for the keyed table.
					savedPartitionIndex = partitionIndex
					savedPartitionRowIndex = partitionRowIndex
					break
				}
			}
		}
	}

	if savedPartitionRowIndex > -1 {
		table.partitions[savedPartitionIndex][savedPartitionRowIndex] = row
	} else {
		table.partitions[key] = append(table.partitions[key], row)
	}

	return nil
}

// keylessTableEditAccumulator manages updates for a keyless table.
type keylessTableEditAccumulator struct {
	table   *Table
	adds    []sql.Row
	deletes []sql.Row
}

var _ tableEditAccumulator = (*keylessTableEditAccumulator)(nil)

// Insert implements the tableEditAccumulator interface.
func (k *keylessTableEditAccumulator) Insert(value sql.Row) error {
	for i, row := range k.deletes {
		eq, err := value.Equals(row, k.table.schema.Schema)
		if err != nil {
			return err
		}

		if eq {
			k.deletes = append(k.deletes[:i], k.deletes[i+1:]...)
		}
	}

	k.adds = append(k.adds, value)
	return nil
}

// Delete implements the tableEditAccumulator interface.
func (k *keylessTableEditAccumulator) Delete(value sql.Row) error {
	for i, row := range k.adds {
		eq, err := value.Equals(row, k.table.schema.Schema)
		if err != nil {
			return err
		}

		if eq {
			k.adds = append(k.adds[:i], k.adds[i+1:]...)
		}

	}

	k.deletes = append(k.deletes, value)
	return nil
}

// Get implements the tableEditAccumulator interface.
func (k *keylessTableEditAccumulator) Get(value sql.Row) (sql.Row, bool, error) {
	// Note: Keyless tables do not have to return an accurate answer here as any given row can be inserted or deleted
	// multiple times.
	return nil, false, nil
}

// ApplyEdits implements the tableEditAccumulator interface.
func (k *keylessTableEditAccumulator) ApplyEdits(ctx *sql.Context) error {
	for _, val := range k.deletes {
		err := k.deleteHelper(ctx, k.table, val)
		if err != nil {
			return err
		}
	}

	for _, val := range k.adds {
		err := k.insertHelper(ctx, k.table, val)
		if err != nil {
			return err
		}
	}

	return nil
}

// Clear implements the tableEditAccumulator interface.
func (k *keylessTableEditAccumulator) Clear() {
	k.adds = make([]sql.Row, 0)
	k.deletes = make([]sql.Row, 0)
}

// deleteHelper deletes a row from a keyless table, if it exists.
func (k *keylessTableEditAccumulator) deleteHelper(ctx *sql.Context, table *Table, row sql.Row) error {
	if err := checkRow(table.schema.Schema, row); err != nil {
		return err
	}

	matches := false
	for partitionIndex, partition := range table.partitions {
		for partitionRowIndex, partitionRow := range partition {
			matches = true
			var err error
			matches, err = rowsAreEqual(ctx, table.schema.Schema, row, partitionRow)
			if err != nil {
				return err
			}

			if matches {
				table.partitions[partitionIndex] = append(partition[:partitionRowIndex], partition[partitionRowIndex+1:]...)
				break
			}
		}
		if matches {
			break
		}
	}

	return nil
}

// insertHelper inserts into a keyless table.
func (k *keylessTableEditAccumulator) insertHelper(ctx *sql.Context, table *Table, row sql.Row) error {
	key := string(table.partitionKeys[table.insertPartIdx])
	table.insertPartIdx++
	if table.insertPartIdx == len(table.partitionKeys) {
		table.insertPartIdx = 0
	}

	table.partitions[key] = append(table.partitions[key], row)

	return nil
}
