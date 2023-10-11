// Copyright 2023 Dolthub, Inc.
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
	"sort"
	"strconv"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

// TableData encapsulates all schema and data for a table's schema and rows. Other aspects of a table can change
// freely as needed for different views on a table (column projections, index lookups, filters, etc.) but the
// storage of underlying data lives here.
type TableData struct {
	dbName    string
	tableName string

	// Schema / config data
	schema                  sql.PrimaryKeySchema
	indexes                 map[string]sql.Index
	fkColl                  *ForeignKeyCollection
	checks                  []sql.CheckDefinition
	collation               sql.CollationID
	autoColIdx              int
	primaryKeyIndexes       bool
	fullTextConfigTableName string

	// Data storage
	partitions    map[string][]sql.Row
	partitionKeys [][]byte
	autoIncVal    uint64

	// Insert bookkeeping (spread inserts across partitions)
	insertPartIdx int
}

// Table returns a table with this data
func (td TableData) Table(database *BaseDatabase) *Table {
	return &Table{
		db:               database,
		name:             td.tableName,
		data:             &td,
		pkIndexesEnabled: td.primaryKeyIndexes,
	}
}

func (td TableData) copy() *TableData {
	sch := td.schema.Schema.Copy()
	pkSch := sql.NewPrimaryKeySchema(sch, td.schema.PkOrdinals...)
	td.schema = pkSch

	parts := make(map[string][]sql.Row, len(td.partitions))
	for k, v := range td.partitions {
		data := make([]sql.Row, len(v))
		copy(data, v)
		parts[k] = data
	}

	keys := make([][]byte, len(td.partitionKeys))
	for i := range td.partitionKeys {
		keys[i] = make([]byte, len(td.partitionKeys[i]))
		copy(keys[i], td.partitionKeys[i])
	}

	td.partitionKeys, td.partitions = keys, parts

	if td.checks != nil {
		checks := make([]sql.CheckDefinition, len(td.checks))
		copy(checks, td.checks)
		td.checks = checks
	}

	return &td
}

func (td *TableData) truncate(schema sql.PrimaryKeySchema) *TableData {
	var keys [][]byte
	var partitions = map[string][]sql.Row{}
	numParts := len(td.partitionKeys)

	for i := 0; i < numParts; i++ {
		key := strconv.Itoa(i)
		keys = append(keys, []byte(key))
		partitions[key] = []sql.Row{}
	}

	td.partitionKeys = keys
	td.partitions = partitions
	td.schema = schema
	td.insertPartIdx = 0

	td.autoIncVal = 0
	if schema.HasAutoIncrement() {
		td.autoIncVal = 1
	}

	return td
}

func allColumns(schema sql.PrimaryKeySchema) []int {
	columns := make([]int, len(schema.Schema))
	for i := range schema.Schema {
		columns[i] = i
	}
	return columns
}

func (td *TableData) columnIndexes(colNames []string) ([]int, error) {
	columns := make([]int, 0, len(colNames))

	for _, name := range colNames {
		i := td.schema.IndexOf(name, td.tableName)
		if i == -1 {
			return nil, errColumnNotFound.New(name)
		}

		columns = append(columns, i)
	}

	return columns, nil
}

func (td *TableData) numRows(ctx *sql.Context) (uint64, error) {
	var count uint64
	for _, rows := range td.partitions {
		count += uint64(len(rows))
	}

	return count, nil
}

// throws an error if any two or more rows share the same |cols| values.
func (td *TableData) errIfDuplicateEntryExist(cols []string, idxName string) error {
	columnMapping, err := td.columnIndexes(cols)
	if err != nil {
		return err
	}
	unique := make(map[uint64]struct{})
	for _, partition := range td.partitions {
		for _, row := range partition {
			idxPrefixKey := projectOnRow(columnMapping, row)
			if hasNulls(idxPrefixKey) {
				continue
			}
			h, err := sql.HashOf(idxPrefixKey)
			if err != nil {
				return err
			}
			if _, ok := unique[h]; ok {
				return sql.NewUniqueKeyErr(formatRow(row, columnMapping), false, nil)
			}
			unique[h] = struct{}{}
		}
	}
	return nil
}

func hasNulls(row sql.Row) bool {
	for _, v := range row {
		if v == nil {
			return true
		}
	}
	return false
}

// getColumnOrdinal returns the index in the schema and column with the name given, if it exists, or -1, nil otherwise.
func (td *TableData) getColumnOrdinal(col string) (int, *sql.Column) {
	i := td.schema.IndexOf(col, td.tableName)
	if i == -1 {
		return -1, nil
	}

	return i, td.schema.Schema[i]
}

func (td *TableData) generateCheckName() string {
	i := 1
Top:
	for {
		name := fmt.Sprintf("%s_chk_%d", td.tableName, i)
		for _, check := range td.checks {
			if check.Name == name {
				i++
				continue Top
			}
		}
		return name
	}
}

func (td *TableData) indexColsForTableEditor() ([][]int, [][]uint16) {
	var uniqIdxCols [][]int
	var prefixLengths [][]uint16
	for _, idx := range td.indexes {
		if !idx.IsUnique() {
			continue
		}
		var colNames []string
		expressions := idx.(*Index).Exprs
		for _, exp := range expressions {
			colNames = append(colNames, exp.(*expression.GetField).Name())
		}
		colIdxs, err := td.columnIndexes(colNames)
		if err != nil {
			// this means that the column names in this index aren't in the schema, which can happen in the case of a
			// table rewrite
			continue
		}
		uniqIdxCols = append(uniqIdxCols, colIdxs)
		prefixLengths = append(prefixLengths, idx.PrefixLengths())
	}
	return uniqIdxCols, prefixLengths
}

// Sorts the rows in the partitions of the table to be in primary key order.
func (td *TableData) sortRows() {
	type pkfield struct {
		i int
		c *sql.Column
	}
	var pk []pkfield
	for _, column := range td.schema.Schema {
		if column.PrimaryKey {
			idx, col := td.getColumnOrdinal(column.Name)
			pk = append(pk, pkfield{idx, col})
		}
	}

	less := func(l, r sql.Row) bool {
		for _, f := range pk {
			r, err := f.c.Type.Compare(l[f.i], r[f.i])
			if err != nil {
				panic(err)
			}
			if r != 0 {
				return r < 0
			}
		}
		return false
	}

	var idx []partidx
	for _, k := range td.partitionKeys {
		p := td.partitions[string(k)]
		for i := 0; i < len(p); i++ {
			idx = append(idx, partidx{string(k), i})
		}
	}

	sort.Sort(partitionssort{td.partitions, idx, less})
}

func (td TableData) virtualColIndexes() []int {
	var indexes []int
	for i, col := range td.schema.Schema {
		if col.Virtual {
			indexes = append(indexes, i)
		}
	}
	return indexes
}

func insertValueInRows(ctx *sql.Context, data *TableData, colIdx int, colDefault *sql.ColumnDefaultValue) error {
	for k, p := range data.partitions {
		newP := make([]sql.Row, len(p))
		for i, row := range p {
			var newRow sql.Row
			newRow = append(newRow, row[:colIdx]...)
			newRow = append(newRow, nil)
			newRow = append(newRow, row[colIdx:]...)
			var err error
			if !data.schema.Schema[colIdx].Nullable && colDefault == nil {
				newRow[colIdx] = data.schema.Schema[colIdx].Type.Zero()
			} else {
				newRow[colIdx], err = colDefault.Eval(ctx, newRow)
				if err != nil {
					return err
				}
			}
			newP[i] = newRow
		}
		data.partitions[k] = newP
	}
	return nil
}
