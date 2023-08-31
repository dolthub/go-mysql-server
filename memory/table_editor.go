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
	"github.com/dolthub/go-mysql-server/sql/types"
)

// tableEditor manages the edits that a targetTable receives.
type tableEditor struct {
	editedTable       *Table
	targetTable       *Table
	discardChanges    bool
	initialAutoIncVal uint64
	initialPartitions map[string][]sql.Row
	initialPartIdx    int
	ea                tableEditAccumulator
	schema            sql.Schema
	initialInsert     int
	// array of key ordinals for each unique index defined on the targetTable
	uniqueIdxCols [][]int
	prefixLengths [][]uint16
	fkTable       *Table
}

var _ sql.Table = (*tableEditor)(nil)
var _ sql.RowReplacer = (*tableEditor)(nil)
var _ sql.RowUpdater = (*tableEditor)(nil)
var _ sql.RowInserter = (*tableEditor)(nil)
var _ sql.RowDeleter = (*tableEditor)(nil)
var _ sql.AutoIncrementSetter = (*tableEditor)(nil)
var _ sql.ForeignKeyEditor = (*tableEditor)(nil)

func (t *tableEditor) Name() string {
	return t.targetTable.name
}

func (t *tableEditor) String() string {
	return t.editedTable.String()
}

func (t *tableEditor) Schema() sql.Schema {
	return t.editedTable.Schema()
}

func (t *tableEditor) Collation() sql.CollationID {
	return t.editedTable.Collation()
}

func (t *tableEditor) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	return t.editedTable.Partitions(ctx)
}

func (t *tableEditor) PartitionRows(ctx *sql.Context, part sql.Partition) (sql.RowIter, error) {
	if t.fkTable != nil {
		return t.fkTable.PartitionRows(ctx, part)
	}
	return t.editedTable.PartitionRows(ctx, part)
}

func (t *tableEditor) GetIndexes(ctx *sql.Context) ([]sql.Index, error) {
	return t.editedTable.GetIndexes(ctx)
}

func (t *tableEditor) Close(ctx *sql.Context) error {
	defer func() {
		sess := SessionFromContext(ctx)
		sess.clearEditAccumulator(t.targetTable)
	}()
	
	if t.discardChanges {
		return nil
	}
	
	// On the normal INSERT / UPDATE / DELETE path this happens at StatementComplete time, but for table rewrites it 
	// only happens at Close
	err := t.ea.ApplyEdits(ctx)
	if err != nil {
		return err
	}
	t.ea.Clear()
	
	t.targetTable.replaceData(t.editedTable)
	
	// for various reasons, the pointer we're editing may not be the one recorded in the database map, so update it
	t.targetTable.db.putTable(t.targetTable)

	return nil
}

func (t *tableEditor) StatementBegin(ctx *sql.Context) {
	t.initialInsert = t.editedTable.insertPartIdx
	t.initialAutoIncVal = t.editedTable.autoIncVal
	t.initialPartitions = make(map[string][]sql.Row)
	for partStr, rowSlice := range t.editedTable.partitions {
		newRowSlice := make([]sql.Row, len(rowSlice))
		for i, row := range rowSlice {
			newRowSlice[i] = row.Copy()
		}
		t.initialPartitions[partStr] = newRowSlice
	}
}

func (t *tableEditor) DiscardChanges(ctx *sql.Context, errorEncountered error) error {
	t.editedTable.insertPartIdx = t.initialInsert
	t.editedTable.autoIncVal = t.initialAutoIncVal
	t.editedTable.partitions = t.initialPartitions
	t.ea.Clear()
	if _, ignore := errorEncountered.(sql.IgnorableError); !ignore {
		t.discardChanges = true	
	}
	return nil
}

func (t *tableEditor) StatementComplete(ctx *sql.Context) error {
	err := t.ea.ApplyEdits(ctx)
	if err != nil {
		return nil
	}
	
	t.ea.Clear()
	t.initialInsert = t.editedTable.insertPartIdx
	t.initialAutoIncVal = t.editedTable.autoIncVal
	t.initialPartitions = make(map[string][]sql.Row)
	for partStr, rowSlice := range t.editedTable.partitions {
		newRowSlice := make([]sql.Row, len(rowSlice))
		for i, row := range rowSlice {
			newRowSlice[i] = row.Copy()
		}
		t.initialPartitions[partStr] = newRowSlice
	}
	return nil
}

// Insert inserts a new row into the table.
func (t *tableEditor) Insert(ctx *sql.Context, row sql.Row) error {
	if err := checkRow(t.editedTable.schema.Schema, row); err != nil {
		return err
	}
	t.editedTable.verifyRowTypes(row)

	partitionRow, added, err := t.ea.Get(row)
	if err != nil {
		return err
	}

	if added {
		pkColIdxes := t.pkColumnIndexes()
		return sql.NewUniqueKeyErr(formatRow(row, pkColIdxes), true, partitionRow)
	}

	for i, cols := range t.uniqueIdxCols {
		if hasNullForAnyCols(row, cols) {
			continue
		}
		prefixLengths := t.prefixLengths[i]
		existing, found, err := t.ea.GetByCols(row, cols, prefixLengths)
		if err != nil {
			return err
		}

		if found {
			return sql.NewUniqueKeyErr(formatRow(row, cols), false, existing)
		}
	}

	err = t.ea.Insert(row)
	if err != nil {
		return err
	}

	idx := t.editedTable.autoColIdx
	if idx >= 0 {
		autoCol := t.editedTable.schema.Schema[idx]
		cmp, err := autoCol.Type.Compare(row[idx], t.editedTable.autoIncVal)
		if err != nil {
			return err
		}
		if cmp > 0 {
			// Provided value larger than autoIncVal, set autoIncVal to that value
			v, _, err := types.Uint64.Convert(row[idx])
			if err != nil {
				return err
			}
			t.editedTable.autoIncVal = v.(uint64)
			t.editedTable.autoIncVal++ // Move onto next autoIncVal
		} else if cmp == 0 {
			// Provided value equal to autoIncVal
			t.editedTable.autoIncVal++ // Move onto next autoIncVal
		}
	}

	return nil
}

// Delete the given row from the table.
func (t *tableEditor) Delete(ctx *sql.Context, row sql.Row) error {
	if err := checkRow(t.editedTable.Schema(), row); err != nil {
		return err
	}
	t.editedTable.verifyRowTypes(row)

	err := t.ea.Delete(row)
	if err != nil {
		return err
	}

	return nil
}

// Update the given row from the table.
func (t *tableEditor) Update(ctx *sql.Context, oldRow sql.Row, newRow sql.Row) error {
	if err := checkRow(t.editedTable.Schema(), oldRow); err != nil {
		return err
	}
	if err := checkRow(t.editedTable.Schema(), newRow); err != nil {
		return err
	}
	t.editedTable.verifyRowTypes(oldRow)
	t.editedTable.verifyRowTypes(newRow)

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

	// Throw a unique key error if any unique indexes are defined
	for i, cols := range t.uniqueIdxCols {
		if hasNullForAnyCols(newRow, cols) {
			continue
		}
		prefixLengths := t.prefixLengths[i]
		existing, found, err := t.ea.GetByCols(newRow, cols, prefixLengths)
		if err != nil {
			return err
		}

		if found {
			return sql.NewUniqueKeyErr(formatRow(newRow, cols), false, existing)
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
	t.editedTable.autoIncVal = val
	return nil
	// TODO: this seems like a bug, we should be closing this in the engine
	// return t.Close(ctx)
}

func (t *tableEditor) IndexedAccess(ctx *sql.Context, i sql.IndexLookup) (sql.IndexedTable, error) {
	// TODO: optimize this, should create some struct that encloses the tableEditor and filters based on the lookup
	if pkTea, ok := t.ea.(*pkTableEditAccumulator); ok {
		newTable, err := newTable(ctx, pkTea.table, pkTea.table.schema)
		if err != nil {
			return nil, err
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
		}).ApplyEdits(ctx)
		if err != nil {
			return nil, err
		}
		return &IndexedTable{Table: newTable, Lookup: i}, nil
	} else {
		nonPkTea := t.ea.(*keylessTableEditAccumulator)
		newTable, err := newTable(ctx, nonPkTea.table, nonPkTea.table.schema)
		if err != nil {
			return nil, err
		}
		adds := make([]sql.Row, len(nonPkTea.adds))
		deletes := make([]sql.Row, len(nonPkTea.deletes))
		copy(adds, nonPkTea.adds)
		copy(deletes, nonPkTea.deletes)
		err = (&keylessTableEditAccumulator{
			table:   newTable,
			adds:    adds,
			deletes: deletes,
		}).ApplyEdits(ctx)
		if err != nil {
			return nil, err
		}
		return &IndexedTable{Table: newTable, Lookup: i}, nil
	}
}

func (t *tableEditor) pkColumnIndexes() []int {
	var pkColIdxes []int
	for _, column := range t.editedTable.schema.Schema {
		if column.PrimaryKey {
			idx, _ := t.editedTable.getField(column.Name)
			pkColIdxes = append(pkColIdxes, idx)
		}
	}
	return pkColIdxes
}

func (t *tableEditor) pkColsDiffer(row, row2 sql.Row) bool {
	pkColIdxes := t.pkColumnIndexes()
	return !columnsMatch(pkColIdxes, nil, row, row2)
}

// Returns whether the values for the columns given match in the two rows provided
func columnsMatch(colIndexes []int, prefixLengths []uint16, row sql.Row, row2 sql.Row) bool {
	for i, idx := range colIndexes {
		v1 := row[idx]
		v2 := row2[idx]
		if len(prefixLengths) > i && prefixLengths[i] > 0 {
			prefixLength := prefixLengths[i]
			switch v := v1.(type) {
			case string:
				if prefixLength > uint16(len(v)) {
					prefixLength = uint16(len(v))
				}
				v1 = v[:prefixLength]
			case []byte:
				if prefixLength > uint16(len(v)) {
					prefixLength = uint16(len(v))
				}
				v1 = v[:prefixLength]
			}
			prefixLength = prefixLengths[i]
			switch v := v2.(type) {
			case string:
				if prefixLength > uint16(len(v)) {
					prefixLength = uint16(len(v))
				}
				v2 = v[:prefixLength]
			case []byte:
				if prefixLength > uint16(len(v)) {
					prefixLength = uint16(len(v))
				}
				v2 = v[:prefixLength]
			}
		}
		if v, ok := v1.([]byte); ok {
			v1 = string(v)
		}
		if v, ok := v2.([]byte); ok {
			v2 = string(v)
		}
		if v1 != v2 {
			return false
		}
	}
	return true
}

// tableEditAccumulator tracks the set of inserts and deletes and applies those edits to a initialTable.
type tableEditAccumulator interface {
	// Insert adds a row to the accumulator to be inserted in the future. Updates are modeled as a Delete then an insertPartIdx.
	Insert(value sql.Row) error
	// Delete adds a row to the accumulator to be deleted in the future. Updates are modeled as a Delete then an insertPartIdx.
	Delete(value sql.Row) error
	// Get returns a row if found along with a boolean added. Added is true if a row was inserted.
	Get(value sql.Row) (sql.Row, bool, error)
	// ApplyEdits takes a initialTable and runs through a sequence of inserts and deletes that have been stored in the
	// accumulator.
	ApplyEdits(ctx *sql.Context) error
	GetByCols(value sql.Row, cols []int, prefixLengths []uint16) (sql.Row, bool, error)
	// Clear wipes all of the stored inserts and deletes that may or may not have been applied.
	Clear()
	// Table returns the table being edited
	Table() *Table
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
			if columnsMatch(pkColIdxes, nil, partitionRow, value) {
				return partitionRow, true, nil
			}
		}
	}

	return nil, false, nil
}

// GetByCols finds a row that has the same |cols| values as |value|.
func (pke *pkTableEditAccumulator) GetByCols(value sql.Row, cols []int, prefixLengths []uint16) (sql.Row, bool, error) {
	// If we have this row in any delete, bail.
	for _, r := range pke.deletes {
		if columnsMatch(cols, prefixLengths, r, value) {
			return nil, false, nil
		}
	}

	for _, r := range pke.adds {
		if columnsMatch(cols, prefixLengths, r, value) {
			return r, true, nil
		}
	}

	for _, partition := range pke.table.partitions {
		for _, partitionRow := range partition {
			if columnsMatch(cols, prefixLengths, partitionRow, value) {
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

	pke.table.sortRows()

	return nil
}

// Clear implements the tableEditAccumulator interface.
func (pke *pkTableEditAccumulator) Clear() {
	pke.adds = make(map[string]sql.Row)
	pke.deletes = make(map[string]sql.Row)
}

func (pke *pkTableEditAccumulator) Table() *Table {
	return pke.table
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
	if err := checkRow(table.Schema(), row); err != nil {
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
				if columnsMatch(pkColIdxes, nil, partitionRow, row) {
					table.partitions[partitionIndex] = append(partition[:partitionRowIndex], partition[partitionRowIndex+1:]...)
					break
				}
			}

			var err error
			matches, err = rowsAreEqual(ctx, table.Schema(), row, partitionRow)
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
				if columnsMatch(pkColIdxes, nil, partitionRow, row) {
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

func (k *keylessTableEditAccumulator) Table() *Table {
	return k.table
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
			return nil
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
			return nil
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

func (k *keylessTableEditAccumulator) GetByCols(value sql.Row, cols []int, prefixLengths []uint16) (sql.Row, bool, error) {
	deleteCount := 0
	for _, r := range k.deletes {
		if columnsMatch(cols, prefixLengths, r, value) {
			deleteCount++
		}
	}

	for _, partition := range k.table.partitions {
		for _, partitionRow := range partition {
			if columnsMatch(cols, prefixLengths, partitionRow, value) {
				if deleteCount == 0 {
					return partitionRow, true, nil
				}
				deleteCount--
			}
		}
	}

	for _, r := range k.adds {
		if columnsMatch(cols, prefixLengths, r, value) {
			if deleteCount == 0 {
				return r, true, nil
			}
			deleteCount--
		}
	}

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

func formatRow(r sql.Row, idxs []int) string {
	b := &strings.Builder{}
	b.WriteString("[")
	var seenOne bool
	for _, idx := range idxs {
		if seenOne {
			_, _ = fmt.Fprintf(b, ",")
		}
		_, _ = fmt.Fprintf(b, "%v", r[idx])
	}
	b.WriteString("]")
	return b.String()
}
