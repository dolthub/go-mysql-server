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
	"github.com/dolthub/go-mysql-server/sql"
)

// tableEditAccumulator tracks the set of inserts and deletes and applies those edits to a initialTable.
type tableEditAccumulator interface {
	// Insert adds a row to the accumulator to be inserted in the future. Updates are modeled as a delete than an insert.
	Insert(value sql.Row) error
	// Delete adds a row to the accumulator to be deleted in the future. Updates are modeled as a delete than an insert.
	Delete(value sql.Row) error
	// Get returns a row if found along with two booleans added and deleted. Added is true if a row was inserted. Deleted
	// is true if a row was deleted.
	Get(value sql.Row) (sql.Row, bool, bool, error)
	// ApplyEdits takes a initialTable and runs through a sequence of inserts and deletes that have been stored in the
	// accumulator.
	ApplyEdits(ctx *sql.Context) (*Table, error)
	// Clear wipes all of the stored inserts and deletes that may or may not have been applied.
	Clear()
}

// NewTableEditAccumulator returns a tableEditAccumulator based on the schema.
func NewTableEditAccumulator(t *Table) tableEditAccumulator {
	if sql.IsKeyless(t.schema) {
		return &keylessTableEditAccumulator{
			table:   t,
			adds:    make([]sql.Row, 0),
			deletes: make([]sql.Row, 0),
		}
	}

	return &pkTableEditAccumulator{
		table:   t,
		adds:    make(map[uint64]sql.Row, 0),
		deletes: make(map[uint64]sql.Row, 0),
	}
}

// pkTableEditAccumulator manages the updates of keyed tables. It uses a map to efficiently toggle edits.
type pkTableEditAccumulator struct {
	table   *Table
	adds    map[uint64]sql.Row
	deletes map[uint64]sql.Row
}

var _ tableEditAccumulator = (*pkTableEditAccumulator)(nil)

// Insert implements the tableEditAccumulator interface.
func (pke *pkTableEditAccumulator) Insert(value sql.Row) error {
	pks := pke.getPks(value)

	pkHash, err := sql.HashOf(pks)
	if err != nil {
		return err
	}

	delete(pke.deletes, pkHash)
	pke.adds[pkHash] = value
	return nil
}

// Delete implements the tableEditAccumulator interface.
func (pke *pkTableEditAccumulator) Delete(value sql.Row) error {
	pks := pke.getPks(value)

	pkHash, err := sql.HashOf(pks)
	if err != nil {
		return err
	}

	delete(pke.adds, pkHash)
	pke.deletes[pkHash] = value

	return nil
}

// Get implements the tableEditAccumulator interface.
func (pke *pkTableEditAccumulator) Get(value sql.Row) (sql.Row, bool, bool, error) {
	pks := pke.getPks(value)
	pkHash, err := sql.HashOf(pks)
	if err != nil {
		return nil, false, false, err
	}

	r, exists := pke.adds[pkHash]
	if exists {
		return r, true, false, nil
	}

	r, exists = pke.deletes[pkHash]
	if exists {
		return r, false, true, nil
	}

	pkColIdxes := pke.pkColumnIndexes()
	for _, partition := range pke.table.partitions {
		for _, partitionRow := range partition {
			if columnsMatch(pkColIdxes, partitionRow, value) {
				return partitionRow, true, false, nil
			}
		}
	}

	return nil, false, false, nil
}

// ApplyEdits implements the tableEditAccumulator interface.
func (pke *pkTableEditAccumulator) ApplyEdits(ctx *sql.Context) (*Table, error) {
	for _, val := range pke.deletes {
		err := pke.deleteHelper(ctx, pke.table, val)
		if err != nil {
			return nil, err
		}
	}

	for _, val := range pke.adds {
		err := pke.insertHelper(ctx, pke.table, val)
		if err != nil {
			return nil, err
		}
	}

	return pke.table, nil
}

// Clear implements the tableEditAccumulator interface.
func (pke *pkTableEditAccumulator) Clear() {
	pke.adds = make(map[uint64]sql.Row, 0)
	pke.deletes = make(map[uint64]sql.Row, 0)
}

// pkColumnIndexes returns the indexes of the primary keys in the initialized table.
func (pke *pkTableEditAccumulator) pkColumnIndexes() []int {
	var pkColIdxes []int
	for _, column := range pke.table.schema {
		if column.PrimaryKey {
			idx, _ := pke.table.getField(column.Name)
			pkColIdxes = append(pkColIdxes, idx)
		}
	}
	return pkColIdxes
}

// getPks returns a sql.Row of the primary keys a row in relation with the initialized table.
func (pke *pkTableEditAccumulator) getPks(r sql.Row) sql.Row {
	pkIdxs := pke.pkColumnIndexes()
	ret := sql.Row{}

	for _, idx := range pkIdxs {
		ret = append(ret, r[idx])
	}

	return ret
}

// deleteHelper deletes the given row from the table.
func (pke *pkTableEditAccumulator) deleteHelper(ctx *sql.Context, table *Table, row sql.Row) error {
	if err := checkRow(table.schema, row); err != nil {
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
			matches, err = rowsAreEqual(ctx, table.schema, row, partitionRow)
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
	key := string(table.keys[table.insert])
	table.insert++
	if table.insert == len(table.keys) {
		table.insert = 0
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
		eq, err := value.Equals(row, k.table.schema)
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
		eq, err := value.Equals(row, k.table.schema)
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
func (k *keylessTableEditAccumulator) Get(value sql.Row) (sql.Row, bool, bool, error) {
	// Note: Keyless tables do not have to return an accurate answer here as any given row can be inserted or deleted
	// multiple times.
	return nil, false, false, nil
}

// ApplyEdits implements the tableEditAccumulator interface.
func (k *keylessTableEditAccumulator) ApplyEdits(ctx *sql.Context) (*Table, error) {
	for _, val := range k.deletes {
		err := k.deleteHelper(ctx, k.table, val)
		if err != nil {
			return nil, err
		}
	}

	for _, val := range k.adds {
		err := k.insertHelper(ctx, k.table, val)
		if err != nil {
			return nil, err
		}
	}

	return k.table, nil
}

// Clear implements the tableEditAccumulator interface.
func (k *keylessTableEditAccumulator) Clear() {
	k.adds = make([]sql.Row, 0)
	k.deletes = make([]sql.Row, 0)
}

// deleteHelper deletes a row from a keyless table, if it exists.
func (k *keylessTableEditAccumulator) deleteHelper(ctx *sql.Context, table *Table, row sql.Row) error {
	if err := checkRow(table.schema, row); err != nil {
		return err
	}

	matches := false
	for partitionIndex, partition := range table.partitions {
		for partitionRowIndex, partitionRow := range partition {
			matches = true
			var err error
			matches, err = rowsAreEqual(ctx, table.schema, row, partitionRow)
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

// insertHelper deletes a row from a keyless table, if it exists.
func (k *keylessTableEditAccumulator) insertHelper(ctx *sql.Context, table *Table, row sql.Row) error {
	key := string(table.keys[table.insert])
	table.insert++
	if table.insert == len(table.keys) {
		table.insert = 0
	}

	table.partitions[key] = append(table.partitions[key], row)

	return nil
}
