package memory

import "github.com/dolthub/go-mysql-server/sql"

type tableEditAccumulator struct {
	table             *Table
	adds []sql.Row
	deletes []sql.Row
}

func NewTableEditAccumulator(t *Table) *tableEditAccumulator {
	return &tableEditAccumulator{
		table: t,
		adds: make([]sql.Row, 0),
		deletes: make([]sql.Row, 0),
	}
}

func (t *tableEditAccumulator) Insert(value sql.Row) error {
	for i, row := range t.deletes {
		var eq bool
		var err error
		if sql.IsKeyless(t.table.schema) {
			eq, err = value.Equals(row, t.table.schema) // todo: keys?
		} else {
			eq, err = value.Equals(row, t.table.schema) // todo: keys?
		}
		if err != nil {
			return err
		}

		if eq {
			t.deletes = append(t.deletes[:i], t.deletes[i+1:]...)
		}

	}

	t.adds = append(t.adds, value)
	return nil
}

func (t *tableEditAccumulator) Delete(value sql.Row) error {
	for i, row := range t.adds {
		var eq bool
		var err error
		if sql.IsKeyless(t.table.schema) {
			eq, err = value.Equals(row, t.table.schema) // todo: keys?
		} else {
			eq, err = value.Equals(row, t.table.schema) // todo: keys?
		}
		if err != nil {
			return err
		}

		if eq {
			t.adds = append(t.adds[:i], t.adds[i+1:]...)
		}

	}

	t.deletes = append(t.deletes, value)
	return nil
}

func (t *tableEditAccumulator) Get(value sql.Row) (sql.Row, bool, bool) {
	if sql.IsKeyless(t.table.schema) {
		return nil, false, false
	}

	pkColIdxes := t.pkColumnIndexes()

	for _, row := range t.adds {
		if columnsMatch(pkColIdxes, value, row) {
			return row, true, false
		}
	}

	for _, row := range t.deletes {
		if columnsMatch(pkColIdxes, value, row) {
			return row, false, true
		}
	}

	if len(pkColIdxes) > 0 {
		for _, partition := range t.table.partitions {
			for _, partitionRow := range partition {
				if columnsMatch(pkColIdxes, partitionRow, value) {
					return partitionRow, true, false
				}
			}
		}
	}

	return nil, false, false
}

func (t *tableEditAccumulator) pkColumnIndexes() []int {
	var pkColIdxes []int
	for _, column := range t.table.schema {
		if column.PrimaryKey {
			idx, _ := t.table.getField(column.Name)
			pkColIdxes = append(pkColIdxes, idx)
		}
	}
	return pkColIdxes
}


func (t *tableEditAccumulator) ApplyEdits(ctx *sql.Context, table *Table) error {
	for _, val := range t.deletes {
		err := t.deleteHelper(ctx, table, val)
		if err != nil {
			return err
		}
	}

	for _, val := range t.adds {
		err := t.insertHelper(ctx, table, val)
		if err != nil {
			return err
		}
	}

	return nil
}

func (t *tableEditAccumulator) deleteHelper(ctx *sql.Context, table *Table, row sql.Row) error {
	if err := checkRow(table.schema, row); err != nil {
		return err
	}

	matches := false
	for partitionIndex, partition := range table.partitions {
		for partitionRowIndex, partitionRow := range partition {
			matches = true

			// For DELETE queries, we will have previously selected the row in order to delete it. For REPLACE, we will just
			// have the row to be replaced, so we need to consider primary key information.
			pkColIdxes := t.pkColumnIndexes()
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

func (t *tableEditAccumulator) insertHelper(ctx *sql.Context, table *Table, row sql.Row) error {
	key := string(table.keys[table.insert])
	table.insert++
	if table.insert == len(table.keys) {
		table.insert = 0
	}

	pkColIdxes := t.pkColumnIndexes()
	if len(pkColIdxes) > 0 {
		for _, partition := range table.partitions {
			for _, partitionRow := range partition {
				if columnsMatch(pkColIdxes, partitionRow, row) {
					return nil
				}
			}
		}
	}


	table.partitions[key] = append(table.partitions[key], row)

	return nil
}