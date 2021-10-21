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

type tableEditAccumulator interface {
	Insert(value sql.Row) error
	Delete(value sql.Row) error
	Get(value sql.Row) (sql.Row, bool, bool)
	ApplyEdits(ctx *sql.Context, table *Table) error
	Clear()
}

func NewTableEditAccumulator(t *Table) tableEditAccumulator {
	if sql.IsKeyless(t.schema) {
		return &keylessTableEditAccumulator{
			table: t,
			adds: make([]sql.Row, 0),
			deletes: make([]sql.Row, 0),
		}
	}

	return &pkTableEditAccumulator{
		table: t,
		adds: make(map[uint64]sql.Row, 0),
		deletes: make(map[uint64]sql.Row, 0),
	}
}

type pkTableEditAccumulator struct {
	table             *Table
	adds map[uint64]sql.Row
	deletes map[uint64]sql.Row
}

var _ tableEditAccumulator = (*pkTableEditAccumulator)(nil)

func (pk *pkTableEditAccumulator) Insert(value sql.Row) error {
	pks := getPks(value, pk.table.schema)

	pkHash, err := sql.HashOf(pks)
	if err != nil {
		return err
	}

	delete(pk.deletes, pkHash)
	pk.adds[pkHash] = value
	return nil
}

func (pk *pkTableEditAccumulator) Delete(value sql.Row) error {
	pks := getPks(value, pk.table.schema)

	pkHash, err := sql.HashOf(pks)
	if err != nil {
		return err
	}

	delete(pk.adds, pkHash)
	pk.deletes[pkHash] = value

	return nil
}

func (pk *pkTableEditAccumulator) Get(value sql.Row) (sql.Row, bool, bool) {
	pks := getPks(value, pk.table.schema) // TODO: Move to use getPkIndexes

	pkHash, _ := sql.HashOf(pks)
	//if err != nil {
	//	return err
	//}
	//

	r, exists := pk.adds[pkHash]
	if exists {
		return r, true, false
	}

	r, exists = pk.deletes[pkHash]
	if exists {
		return r, false, true
	}

	pkColIdxes := pk.pkColumnIndexes()
	for _, partition := range pk.table.partitions {
		for _, partitionRow := range partition {
			if columnsMatch(pkColIdxes, partitionRow, value) {
				return partitionRow, true, false
			}
		}
	}

	return nil, false, false
}

func (pk *pkTableEditAccumulator) pkColumnIndexes() []int {
	var pkColIdxes []int
	for _, column := range pk.table.schema {
		if column.PrimaryKey {
			idx, _ := pk.table.getField(column.Name)
			pkColIdxes = append(pkColIdxes, idx)
		}
	}
	return pkColIdxes
}

func (pk *pkTableEditAccumulator) ApplyEdits(ctx *sql.Context, table *Table) error {
	for _, val := range pk.deletes {
		err := pk.deleteHelper(ctx, table, val)
		if err != nil {
			return err
		}
	}

	for _, val := range pk.adds {
		err := pk.insertHelper(ctx, table, val)
		if err != nil {
			return err
		}
	}

	return nil
}

func (pk *pkTableEditAccumulator) Clear() {
	pk.adds = make(map[uint64]sql.Row, 0)
	pk.deletes = make(map[uint64]sql.Row, 0)
}

func (pk *pkTableEditAccumulator) deleteHelper(ctx *sql.Context, table *Table, row sql.Row) error {
	if err := checkRow(table.schema, row); err != nil {
		return err
	}

	matches := false
	for partitionIndex, partition := range table.partitions {
		for partitionRowIndex, partitionRow := range partition {
			matches = true

			// For DELETE queries, we will have previously selected the row in order to delete it. For REPLACE, we will just
			// have the row to be replaced, so we need to consider primary key information.
			pkColIdxes := pk.pkColumnIndexes()
			if len(pkColIdxes) > 0 {
				if columnsMatch(pkColIdxes, partitionRow, row) {
					table.partitions[partitionIndex] = append(partition[:partitionRowIndex], partition[partitionRowIndex+1:]...)
					break
				}
			}

			// If we had no primary key match (or have no primary key), check each row for a total match
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

	//if !matches {
	//	return sql.ErrDeleteRowNotFound.New()
	//}

	return nil
}

func (pk *pkTableEditAccumulator) insertHelper(ctx *sql.Context, table *Table, row sql.Row) error {
	key := string(table.keys[table.insert])
	table.insert++
	if table.insert == len(table.keys) {
		table.insert = 0
	}

	pkColIdxes := pk.pkColumnIndexes()
	savedPartitionIndex := ""
	savedPartitionRowIndex := -1
	if len(pkColIdxes) > 0 {
		for partitionIndex, partition := range table.partitions {
			for partitionRowIndex, partitionRow := range partition {
				if columnsMatch(pkColIdxes, partitionRow, row) {
					// Implement map like semantics essentially
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

func getPks(r sql.Row, s sql.Schema) sql.Row {
	pkIdxs := make([]int, 0)
	for i, c := range s {
		if c.PrimaryKey {
			pkIdxs = append(pkIdxs, i)
		}
	}

	ret := sql.Row{}

	for _, idx := range pkIdxs {
		ret = append(ret, r[idx])
	}

	return ret
}

type keylessTableEditAccumulator struct {
	table *Table
	adds []sql.Row
	deletes []sql.Row
}

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

func (k *keylessTableEditAccumulator) Delete(value sql.Row) error {
	for i, row := range k.adds {
		eq, err := value.Equals(row, k.table.schema) // todo: keys?
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

func (k *keylessTableEditAccumulator) Get(value sql.Row) (sql.Row, bool, bool) {
	return nil, false, false
}

func (k *keylessTableEditAccumulator) ApplyEdits(ctx *sql.Context, table *Table) error {
	for _, val := range k.deletes {
		err := k.deleteHelper(ctx, table, val)
		if err != nil {
			return err
		}
	}

	for _, val := range k.adds {
		err := k.insertHelper(ctx, table, val)
		if err != nil {
			return err
		}
	}

	return nil
}

func (k *keylessTableEditAccumulator) Clear() {
	k.adds = make([]sql.Row, 0)
	k.deletes =  make([]sql.Row, 0)
}

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

	//if !matches {
	//	return sql.ErrDeleteRowNotFound.New()
	//}

	return nil
}

func (k *keylessTableEditAccumulator) insertHelper(ctx *sql.Context, table *Table, row sql.Row) error {
	key := string(table.keys[table.insert])
	table.insert++
	if table.insert == len(table.keys) {
		table.insert = 0
	}

	table.partitions[key] = append(table.partitions[key], row)

	return nil
}

var _ tableEditAccumulator = (*keylessTableEditAccumulator)(nil)

