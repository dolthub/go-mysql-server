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
	"bytes"
	"encoding/gob"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"

	"github.com/dolthub/vitess/go/sqltypes"
	errors "gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

// Table represents an in-memory database table.
type Table struct {
	// Schema and related info
	name             string
	schema           sql.Schema
	indexes          map[string]sql.Index
	foreignKeys      []sql.ForeignKeyConstraint
	checks           []sql.CheckDefinition
	pkIndexesEnabled bool

	// pushdown info
	filters    []sql.Expression // currently unused, filter pushdown is significantly broken right now
	projection []string
	columns    []int

	// Data storage
	partitions map[string][]sql.Row
	keys       [][]byte

	// Insert bookkeeping
	insert int

	// Indexed lookups
	lookup sql.IndexLookup

	// AUTO_INCREMENT bookkeeping
	autoIncVal interface{}
	autoColIdx int
}

var _ sql.Table = (*Table)(nil)
var _ sql.InsertableTable = (*Table)(nil)
var _ sql.UpdatableTable = (*Table)(nil)
var _ sql.DeletableTable = (*Table)(nil)
var _ sql.ReplaceableTable = (*Table)(nil)
var _ sql.TruncateableTable = (*Table)(nil)
var _ sql.DriverIndexableTable = (*Table)(nil)
var _ sql.AlterableTable = (*Table)(nil)
var _ sql.IndexAlterableTable = (*Table)(nil)
var _ sql.IndexedTable = (*Table)(nil)
var _ sql.ForeignKeyAlterableTable = (*Table)(nil)
var _ sql.ForeignKeyTable = (*Table)(nil)
var _ sql.CheckAlterableTable = (*Table)(nil)
var _ sql.CheckTable = (*Table)(nil)
var _ sql.AutoIncrementTable = (*Table)(nil)
var _ sql.StatisticsTable = (*Table)(nil)
var _ sql.ProjectedTable = (*Table)(nil)

// NewTable creates a new Table with the given name and schema.
func NewTable(name string, schema sql.Schema) *Table {
	return NewPartitionedTable(name, schema, 0)
}

// NewPartitionedTable creates a new Table with the given name, schema and number of partitions.
func NewPartitionedTable(name string, schema sql.Schema, numPartitions int) *Table {
	var keys [][]byte
	var partitions = map[string][]sql.Row{}

	if numPartitions < 1 {
		numPartitions = 1
	}

	for i := 0; i < numPartitions; i++ {
		key := strconv.Itoa(i)
		keys = append(keys, []byte(key))
		partitions[key] = []sql.Row{}
	}

	var autoIncVal interface{}
	autoIncIdx := -1
	for i, c := range schema {
		if c.AutoIncrement {
			autoIncVal = sql.NumericUnaryValue(c.Type)
			autoIncIdx = i
			break
		}
	}

	return &Table{
		name:       name,
		schema:     schema,
		partitions: partitions,
		keys:       keys,
		autoIncVal: autoIncVal,
		autoColIdx: autoIncIdx,
	}
}

// Name implements the sql.Table interface.
func (t *Table) Name() string {
	return t.name
}

// Schema implements the sql.Table interface.
func (t *Table) Schema() sql.Schema {
	return t.schema
}

func (t *Table) GetPartition(key string) []sql.Row {
	rows, ok := t.partitions[string(key)]
	if ok {
		return rows
	}

	return nil
}

// Partitions implements the sql.Table interface.
func (t *Table) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	var keys [][]byte
	for _, k := range t.keys {
		if rows, ok := t.partitions[string(k)]; ok && len(rows) > 0 {
			keys = append(keys, k)
		}
	}
	return &partitionIter{keys: keys}, nil
}

// PartitionCount implements the sql.PartitionCounter interface.
func (t *Table) PartitionCount(ctx *sql.Context) (int64, error) {
	return int64(len(t.partitions)), nil
}

// PartitionRows implements the sql.PartitionRows interface.
func (t *Table) PartitionRows(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	rows, ok := t.partitions[string(partition.Key())]
	if !ok {
		return nil, sql.ErrPartitionNotFound.New(partition.Key())
	}

	var values sql.IndexValueIter
	if t.lookup != nil {
		var err error
		values, err = t.lookup.(sql.DriverIndexLookup).Values(partition)
		if err != nil {
			return nil, err
		}
	}

	// The slice could be altered by other operations taking place during iteration (such as deletion or insertion), so
	// make a copy of the values as they exist when execution begins.
	rowsCopy := make([]sql.Row, len(rows))
	copy(rowsCopy, rows)

	return &tableIter{
		rows:        rowsCopy,
		indexValues: values,
		columns:     t.columns,
		filters:     t.filters,
	}, nil
}

func (t *Table) NumRows(ctx *sql.Context) (uint64, error) {
	var count uint64 = 0
	for _, rows := range t.partitions {
		count += uint64(len(rows))
	}

	return count, nil
}

func (t *Table) DataLength(ctx *sql.Context) (uint64, error) {
	var numBytesPerRow uint64 = 0
	for _, col := range t.schema {
		switch n := col.Type.(type) {
		case sql.NumberType:
			numBytesPerRow += 8
		case sql.StringType:
			numBytesPerRow += uint64(n.MaxByteLength())
		case sql.BitType:
			numBytesPerRow += 1
		case sql.DatetimeType:
			numBytesPerRow += 8
		case sql.DecimalType:
			numBytesPerRow += uint64(n.MaximumScale())
		case sql.EnumType:
			numBytesPerRow += 2
		case sql.JsonType:
			numBytesPerRow += 20
		case sql.NullType:
			numBytesPerRow += 1
		case sql.TimeType:
			numBytesPerRow += 16
		case sql.YearType:
			numBytesPerRow += 8
		default:
			numBytesPerRow += 0
		}
	}

	numRows, err := t.NumRows(ctx)
	if err != nil {
		return 0, err
	}

	return numBytesPerRow * numRows, nil
}

func NewPartition(key []byte) *Partition {
	return &Partition{key: key}
}

type Partition struct {
	key []byte
}

func (p *Partition) Key() []byte { return p.key }

type partitionIter struct {
	keys [][]byte
	pos  int
}

func (p *partitionIter) Next() (sql.Partition, error) {
	if p.pos >= len(p.keys) {
		return nil, io.EOF
	}

	key := p.keys[p.pos]
	p.pos++
	return &Partition{key}, nil
}

func (p *partitionIter) Close(_ *sql.Context) error { return nil }

type tableIter struct {
	columns []int
	filters []sql.Expression

	rows        []sql.Row
	indexValues sql.IndexValueIter
	pos         int
}

var _ sql.RowIter = (*tableIter)(nil)

func (i *tableIter) Next() (sql.Row, error) {
	row, err := i.getRow()
	if err != nil {
		return nil, err
	}

	for _, f := range i.filters {
		result, err := f.Eval(sql.NewEmptyContext(), row)
		if err != nil {
			return nil, err
		}
		result, _ = sql.ConvertToBool(result)
		if result != true {
			return i.Next()
		}
	}

	resultRow := make(sql.Row, len(row))
	for j := range row {
		if len(i.columns) == 0 || i.colIsProjected(j) {
			resultRow[j] = row[j]
		}
	}

	return resultRow, nil
}

func (i *tableIter) colIsProjected(idx int) bool {
	for _, colIdx := range i.columns {
		if idx == colIdx {
			return true
		}
	}
	return false
}

func (i *tableIter) Close(ctx *sql.Context) error {
	if i.indexValues == nil {
		return nil
	}

	return i.indexValues.Close(ctx)
}

func (i *tableIter) getRow() (sql.Row, error) {
	if i.indexValues != nil {
		return i.getFromIndex()
	}

	if i.pos >= len(i.rows) {
		return nil, io.EOF
	}

	row := i.rows[i.pos]
	i.pos++
	return row, nil
}

func projectOnRow(columns []int, row sql.Row) sql.Row {
	if len(columns) < 1 {
		return row
	}

	projected := make([]interface{}, len(columns))
	for i, selected := range columns {
		projected[i] = row[selected]
	}

	return projected
}

func (i *tableIter) getFromIndex() (sql.Row, error) {
	data, err := i.indexValues.Next()
	if err != nil {
		return nil, err
	}

	value, err := DecodeIndexValue(data)
	if err != nil {
		return nil, err
	}

	return i.rows[value.Pos], nil
}

type IndexValue struct {
	Key string
	Pos int
}

func DecodeIndexValue(data []byte) (*IndexValue, error) {
	dec := gob.NewDecoder(bytes.NewReader(data))
	var value IndexValue
	if err := dec.Decode(&value); err != nil {
		return nil, err
	}

	return &value, nil
}

func EncodeIndexValue(value *IndexValue) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(value); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

type tableEditor struct {
	table             *Table
	initialAutoIncVal interface{}
	initialPartitions map[string][]sql.Row
	initialInsert     int
}

var _ sql.RowReplacer = (*tableEditor)(nil)
var _ sql.RowUpdater = (*tableEditor)(nil)
var _ sql.RowInserter = (*tableEditor)(nil)
var _ sql.RowDeleter = (*tableEditor)(nil)

func (t *tableEditor) Close(*sql.Context) error {
	// TODO: it would be nice to apply all pending updates here at once, rather than directly in the Insert / Update
	//  / Delete methods.
	return nil
}

func (t *tableEditor) StatementBegin(ctx *sql.Context) {
	t.initialInsert = t.table.insert
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
	t.table.insert = t.initialInsert
	t.table.autoIncVal = t.initialAutoIncVal
	t.table.partitions = t.initialPartitions
	return nil
}

func (t *tableEditor) StatementComplete(ctx *sql.Context) error {
	return nil
}

func (t *Table) Inserter(*sql.Context) sql.RowInserter {
	return &tableEditor{t, nil, nil, 0}
}

func (t *Table) Updater(*sql.Context) sql.RowUpdater {
	return &tableEditor{t, nil, nil, 0}
}

func (t *Table) Replacer(*sql.Context) sql.RowReplacer {
	return &tableEditor{t, nil, nil, 0}
}

func (t *Table) Deleter(*sql.Context) sql.RowDeleter {
	return &tableEditor{t, nil, nil, 0}
}

func (t *Table) AutoIncrementSetter(*sql.Context) sql.AutoIncrementSetter {
	return &tableEditor{t, nil, nil, 0}
}

func (t *Table) Truncate(ctx *sql.Context) (int, error) {
	count := 0
	for key := range t.partitions {
		count += len(t.partitions[key])
		t.partitions[key] = nil
	}
	return count, nil
}

// Convenience method to avoid having to create an inserter in test setup
func (t *Table) Insert(ctx *sql.Context, row sql.Row) error {
	inserter := t.Inserter(ctx)
	if err := inserter.Insert(ctx, row); err != nil {
		return err
	}
	return inserter.Close(ctx)
}

// Insert a new row into the table.
func (t *tableEditor) Insert(ctx *sql.Context, row sql.Row) error {
	if err := checkRow(t.table.schema, row); err != nil {
		return err
	}

	if err := t.checkUniquenessConstraints(row); err != nil {
		return err
	}

	key := string(t.table.keys[t.table.insert])
	t.table.insert++
	if t.table.insert == len(t.table.keys) {
		t.table.insert = 0
	}

	t.table.partitions[key] = append(t.table.partitions[key], row)

	idx := t.table.autoColIdx
	if idx >= 0 {
		// autoIncVal = max(autoIncVal, insertVal)
		autoCol := t.table.schema[idx]
		cmp, err := autoCol.Type.Compare(row[idx], t.table.autoIncVal)
		if err != nil {
			return err
		}
		if cmp > 0 {
			t.table.autoIncVal = row[idx]
		}
		t.table.autoIncVal = increment(t.table.autoIncVal)
	}

	return nil
}

func increment(v interface{}) interface{} {
	switch val := v.(type) {
	case int:
		return val + 1
	case uint:
		return val + 1
	case int8:
		return val + 1
	case int16:
		return val + 1
	case int32:
		return val + 1
	case int64:
		return val + 1
	case uint8:
		return val + 1
	case uint16:
		return val + 1
	case uint32:
		return val + 1
	case uint64:
		return val + 1
	case float32:
		return val + 1
	case float64:
		return val + 1
	}
	return v
}

func rowsAreEqual(ctx *sql.Context, schema sql.Schema, left, right sql.Row) (bool, error) {
	if len(left) != len(right) || len(left) != len(schema) {
		return false, nil
	}

	for index := range left {
		typ := schema[index].Type
		if typ.Type() != sqltypes.TypeJSON {
			if left[index] != right[index] {
				return false, nil
			}
			continue
		}

		// TODO should Type.Compare be used for all columns?
		cmp, err := typ.Compare(left[index], right[index])
		if err != nil {
			return false, err
		}
		if cmp != 0 {
			return false, nil
		}
	}
	return true, nil
}

// Delete the given row from the table.
func (t *tableEditor) Delete(ctx *sql.Context, row sql.Row) error {
	if err := checkRow(t.table.schema, row); err != nil {
		return err
	}

	matches := false
	for partitionIndex, partition := range t.table.partitions {
		for partitionRowIndex, partitionRow := range partition {
			matches = true

			// For DELETE queries, we will have previously selected the row in order to delete it. For REPLACE, we will just
			// have the row to be replaced, so we need to consider primary key information.
			pkColIdxes := t.pkColumnIndexes()
			if len(pkColIdxes) > 0 {
				if columnsMatch(pkColIdxes, partitionRow, row) {
					t.table.partitions[partitionIndex] = append(partition[:partitionRowIndex], partition[partitionRowIndex+1:]...)
					break
				}
			}

			// If we had no primary key match (or have no primary key), check each row for a total match
			var err error
			matches, err = rowsAreEqual(ctx, t.table.schema, row, partitionRow)
			if err != nil {
				return err
			}

			if matches {
				t.table.partitions[partitionIndex] = append(partition[:partitionRowIndex], partition[partitionRowIndex+1:]...)
				break
			}
		}
		if matches {
			break
		}
	}

	if !matches {
		return sql.ErrDeleteRowNotFound.New()
	}

	return nil
}

func (t *tableEditor) Update(ctx *sql.Context, oldRow sql.Row, newRow sql.Row) error {
	if err := checkRow(t.table.schema, oldRow); err != nil {
		return err
	}
	if err := checkRow(t.table.schema, newRow); err != nil {
		return err
	}

	if t.pkColsDiffer(oldRow, newRow) {
		if err := t.checkUniquenessConstraints(newRow); err != nil {
			return err
		}
	}

	matches := false
	for partitionIndex, partition := range t.table.partitions {
		for partitionRowIndex, partitionRow := range partition {
			var err error
			matches, err = rowsAreEqual(ctx, t.table.schema, oldRow, partitionRow)
			if err != nil {
				return err
			}
			if matches {
				t.table.partitions[partitionIndex][partitionRowIndex] = newRow
				break
			}
		}
		if matches {
			break
		}
	}

	return nil
}

// SetAutoIncrementValue sets a new AUTO_INCREMENT value
func (t *tableEditor) SetAutoIncrementValue(ctx *sql.Context, val interface{}) error {
	t.table.autoIncVal = val
	return nil
}

func (t *tableEditor) checkUniquenessConstraints(row sql.Row) error {
	pkColIdxes := t.pkColumnIndexes()

	if len(pkColIdxes) > 0 {
		for _, partition := range t.table.partitions {
			for _, partitionRow := range partition {
				if columnsMatch(pkColIdxes, partitionRow, row) {
					vals := make([]interface{}, len(pkColIdxes))
					for _, i := range pkColIdxes {
						vals[i] = row[pkColIdxes[i]]
					}
					return sql.NewUniqueKeyErr(fmt.Sprint(vals), true, partitionRow)
				}
			}
		}
	}

	return nil
}

func (t *tableEditor) pkColumnIndexes() []int {
	var pkColIdxes []int
	for _, column := range t.table.schema {
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

// PeekNextAutoIncrementValue peeks at the next AUTO_INCREMENT value
func (t *Table) PeekNextAutoIncrementValue(*sql.Context) (interface{}, error) {
	return t.autoIncVal, nil
}

// GetNextAutoIncrementValue gets the next auto increment value for the memory table the increment.
func (t *Table) GetNextAutoIncrementValue(ctx *sql.Context, insertVal interface{}) (interface{}, error) {
	autoIncCol := t.schema[t.autoColIdx]
	cmp, err := autoIncCol.Type.Compare(insertVal, t.autoIncVal)
	if err != nil {
		return nil, err
	}

	if cmp > 0 {
		t.autoIncVal = insertVal
	}

	return t.autoIncVal, nil
}

func (t *Table) AddColumn(ctx *sql.Context, column *sql.Column, order *sql.ColumnOrder) error {
	newColIdx := t.addColumnToSchema(ctx, column, order)
	return t.insertValueInRows(ctx, newColIdx, column.Default)
}

// addColumnToSchema adds the given column to the schema and returns the new index
func (t *Table) addColumnToSchema(ctx *sql.Context, newCol *sql.Column, order *sql.ColumnOrder) int {
	newCol.Source = t.Name()
	newSch := make(sql.Schema, len(t.schema)+1)

	newColIdx := 0
	var i int
	if order != nil && order.First {
		newSch[i] = newCol
		i++
	}

	for _, col := range t.schema {
		newSch[i] = col
		i++
		if (order != nil && order.AfterColumn == col.Name) || (order == nil && i == len(t.schema)) {
			newSch[i] = newCol
			newColIdx = i
			i++
		}
	}

	for i, newSchCol := range newSch {
		if i == newColIdx {
			continue
		}
		newDefault, _ := expression.TransformUp(ctx, newSchCol.Default, func(expr sql.Expression) (sql.Expression, error) {
			if expr, ok := expr.(*expression.GetField); ok {
				return expr.WithIndex(newSch.IndexOf(expr.Name(), t.name)), nil
			}
			return expr, nil
		})
		newSchCol.Default = newDefault.(*sql.ColumnDefaultValue)
	}

	t.schema = newSch
	return newColIdx
}

func (t *Table) insertValueInRows(ctx *sql.Context, idx int, colDefault *sql.ColumnDefaultValue) error {
	for k, p := range t.partitions {
		newP := make([]sql.Row, len(p))
		for i, row := range p {
			var newRow sql.Row
			newRow = append(newRow, row[:idx]...)
			newRow = append(newRow, nil)
			newRow = append(newRow, row[idx:]...)
			var err error
			if !t.schema[idx].Nullable && colDefault == nil {
				newRow[idx] = t.schema[idx].Type.Zero()
			} else {
				newRow[idx], err = colDefault.Eval(ctx, newRow)
				if err != nil {
					return err
				}
			}
			newP[i] = newRow
		}
		t.partitions[k] = newP
	}
	return nil
}

func (t *Table) DropColumn(ctx *sql.Context, columnName string) error {
	droppedCol := t.dropColumnFromSchema(ctx, columnName)
	for k, p := range t.partitions {
		newP := make([]sql.Row, len(p))
		for i, row := range p {
			var newRow sql.Row
			newRow = append(newRow, row[:droppedCol]...)
			newRow = append(newRow, row[droppedCol+1:]...)
			newP[i] = newRow
		}
		t.partitions[k] = newP
	}
	return nil
}

// dropColumnFromSchema drops the given column name from the schema and returns its old index.
func (t *Table) dropColumnFromSchema(ctx *sql.Context, columnName string) int {
	newSch := make(sql.Schema, len(t.schema)-1)
	var i int
	droppedCol := -1
	for _, col := range t.schema {
		if col.Name != columnName {
			newSch[i] = col
			i++
		} else {
			droppedCol = i
		}
	}
	t.schema = newSch
	return droppedCol
}

func (t *Table) ModifyColumn(ctx *sql.Context, columnName string, column *sql.Column, order *sql.ColumnOrder) error {
	oldIdx := -1
	newIdx := 0
	for i, col := range t.schema {
		if col.Name == columnName {
			oldIdx = i
			column.PrimaryKey = col.PrimaryKey
			break
		}
	}
	if order == nil {
		newIdx = oldIdx
		if newIdx == 0 {
			order = &sql.ColumnOrder{First: true}
		} else {
			order = &sql.ColumnOrder{AfterColumn: t.schema[newIdx-1].Name}
		}
	} else if !order.First {
		var oldSchemaWithoutCol sql.Schema
		oldSchemaWithoutCol = append(oldSchemaWithoutCol, t.schema[:oldIdx]...)
		oldSchemaWithoutCol = append(oldSchemaWithoutCol, t.schema[oldIdx+1:]...)
		for i, col := range oldSchemaWithoutCol {
			if col.Name == order.AfterColumn {
				newIdx = i + 1
				break
			}
		}
	}

	for k, p := range t.partitions {
		newP := make([]sql.Row, len(p))
		for i, row := range p {
			var oldRowWithoutVal sql.Row
			oldRowWithoutVal = append(oldRowWithoutVal, row[:oldIdx]...)
			oldRowWithoutVal = append(oldRowWithoutVal, row[oldIdx+1:]...)
			newVal, err := column.Type.Convert(row[oldIdx])
			if err != nil {
				return err
			}
			var newRow sql.Row
			newRow = append(newRow, oldRowWithoutVal[:newIdx]...)
			newRow = append(newRow, newVal)
			newRow = append(newRow, oldRowWithoutVal[newIdx:]...)
			newP[i] = newRow
		}
		t.partitions[k] = newP
	}

	_ = t.dropColumnFromSchema(ctx, columnName)
	t.addColumnToSchema(ctx, column, order)
	return nil
}

func checkRow(schema sql.Schema, row sql.Row) error {
	if len(row) != len(schema) {
		return sql.ErrUnexpectedRowLength.New(len(schema), len(row))
	}

	for i, value := range row {
		c := schema[i]
		if !c.Check(value) {
			return sql.ErrInvalidType.New(value)
		}
	}

	return nil
}

// String implements the sql.Table interface.
func (t *Table) String() string {
	return t.name
}

func (t *Table) DebugString() string {
	p := sql.NewTreePrinter()

	kind := ""

	if t.lookup != nil {
		kind += fmt.Sprintf("Indexed on %s", t.lookup)
	}

	if kind != "" {
		kind = ": " + kind
	}

	if len(t.columns) > 0 {
		var projections []string
		for _, column := range t.columns {
			projections = append(projections, fmt.Sprintf("%d", column))
		}
		kind += fmt.Sprintf("Projected on [%s] ", strings.Join(projections, ", "))
	}

	if len(t.filters) > 0 {
		var filters []string
		for _, filter := range t.filters {
			filters = append(filters, fmt.Sprintf("%s", sql.DebugString(filter)))
		}
		kind += fmt.Sprintf("Filtered on [%s]", strings.Join(filters, ", "))
	}

	if len(kind) == 0 {
		return t.name
	}

	_ = p.WriteNode("%s%s", t.name, kind)
	return p.String()
}

// HandledFilters implements the sql.FilteredTable interface.
func (t *Table) HandledFilters(filters []sql.Expression) []sql.Expression {
	var handled []sql.Expression
	for _, f := range filters {
		var hasOtherFields bool
		sql.Inspect(f, func(e sql.Expression) bool {
			if e, ok := e.(*expression.GetField); ok {
				if e.Table() != t.name || !t.schema.Contains(e.Name(), t.name) {
					hasOtherFields = true
					return false
				}
			}
			return true
		})

		if !hasOtherFields {
			handled = append(handled, f)
		}
	}

	return handled
}

// sql.FilteredTable functionality in the Table type was disabled for a long period of time, and has developed major
// issues with the current analyzer logic. It's only used in the pushdown unit tests, and sql.FilteredTable should be
// considered unstable until this situation is fixed.
type FilteredTable struct {
	*Table
}

var _ sql.FilteredTable = (*FilteredTable)(nil)

func NewFilteredTable(name string, schema sql.Schema) *FilteredTable {
	return &FilteredTable{
		Table: NewTable(name, schema),
	}
}

// WithFilters implements the sql.FilteredTable interface.
func (t *FilteredTable) WithFilters(ctx *sql.Context, filters []sql.Expression) sql.Table {
	if len(filters) == 0 {
		return t
	}

	nt := *t
	nt.filters = filters
	return &nt
}

// WithFilters implements the sql.FilteredTable interface.
func (t *FilteredTable) WithProjection(colNames []string) sql.Table {
	table := t.Table.WithProjection(colNames)

	nt := *t
	nt.Table = table.(*Table)
	return &nt
}

// WithProjection implements the sql.ProjectedTable interface.
func (t *Table) WithProjection(colNames []string) sql.Table {
	if len(colNames) == 0 {
		return t
	}

	nt := *t
	columns, err := nt.columnIndexes(colNames)
	if err != nil {
		panic(err)
	}

	nt.columns = columns
	nt.projection = colNames

	return &nt
}

func (t *Table) columnIndexes(colNames []string) ([]int, error) {
	var columns []int

	for _, name := range colNames {
		i := t.schema.IndexOf(name, t.name)
		if i == -1 {
			return nil, errColumnNotFound.New(name)
		}

		columns = append(columns, i)
	}

	return columns, nil
}

// EnablePrimaryKeyIndexes enables the use of primary key indexes on this table.
func (t *Table) EnablePrimaryKeyIndexes() {
	t.pkIndexesEnabled = true
}

// GetIndexes implements sql.IndexedTable
func (t *Table) GetIndexes(ctx *sql.Context) ([]sql.Index, error) {
	indexes := make([]sql.Index, 0)

	if t.pkIndexesEnabled {
		var pkCols []*sql.Column
		for _, col := range t.schema {
			if col.PrimaryKey {
				pkCols = append(pkCols, col)
			}
		}

		if len(pkCols) > 0 {
			exprs := make([]sql.Expression, len(pkCols))
			for i, column := range pkCols {
				idx, field := t.getField(column.Name)
				exprs[i] = expression.NewGetFieldWithTable(idx, field.Type, t.name, field.Name, field.Nullable)
			}
			indexes = append(indexes, &MergeableIndex{
				DB:         "",
				DriverName: "",
				Tbl:        t,
				TableName:  t.name,
				Exprs:      exprs,
				Name:       "PRIMARY",
				Unique:     true,
			})
		}
	}

	nonPrimaryIndexes := make([]sql.Index, len(t.indexes))
	var i int
	for _, index := range t.indexes {
		nonPrimaryIndexes[i] = index
		i++
	}
	sort.Slice(nonPrimaryIndexes, func(i, j int) bool {
		return nonPrimaryIndexes[i].ID() < nonPrimaryIndexes[j].ID()
	})

	return append(indexes, nonPrimaryIndexes...), nil
}

// GetForeignKeys implements sql.ForeignKeyTable
func (t *Table) GetForeignKeys(_ *sql.Context) ([]sql.ForeignKeyConstraint, error) {
	return t.foreignKeys, nil
}

// CreateForeignKey implements sql.ForeignKeyAlterableTable. Foreign keys are not enforced on update / delete.
func (t *Table) CreateForeignKey(_ *sql.Context, fkName string, columns []string, referencedTable string, referencedColumns []string, onUpdate, onDelete sql.ForeignKeyReferenceOption) error {
	for _, key := range t.foreignKeys {
		if key.Name == fkName {
			return fmt.Errorf("Constraint %s already exists", fkName)
		}
	}

	for _, key := range t.checks {
		if key.Name == fkName {
			return fmt.Errorf("constraint %s already exists", fkName)
		}
	}

	t.foreignKeys = append(t.foreignKeys, sql.ForeignKeyConstraint{
		Name:              fkName,
		Columns:           columns,
		ReferencedTable:   referencedTable,
		ReferencedColumns: referencedColumns,
		OnUpdate:          onUpdate,
		OnDelete:          onDelete,
	})

	return nil
}

// DropForeignKey implements sql.ForeignKeyAlterableTable.
func (t *Table) DropForeignKey(ctx *sql.Context, fkName string) error {
	return t.dropConstraint(ctx, fkName)
}

func (t *Table) dropConstraint(ctx *sql.Context, name string) error {
	for i, key := range t.foreignKeys {
		if key.Name == name {
			t.foreignKeys = append(t.foreignKeys[:i], t.foreignKeys[i+1:]...)
			return nil
		}
	}
	for i, key := range t.checks {
		if key.Name == name {
			t.checks = append(t.checks[:i], t.checks[i+1:]...)
			return nil
		}
	}
	return nil
}

// GetChecks implements sql.CheckTable
func (t *Table) GetChecks(_ *sql.Context) ([]sql.CheckDefinition, error) {
	return t.checks, nil
}

// CreateCheck implements sql.CheckAlterableTable
func (t *Table) CreateCheck(_ *sql.Context, check *sql.CheckDefinition) error {
	toInsert := *check

	if toInsert.Name == "" {
		toInsert.Name = t.generateCheckName()
	}

	for _, key := range t.checks {
		if key.Name == toInsert.Name {
			return fmt.Errorf("constraint %s already exists", toInsert.Name)
		}
	}

	for _, key := range t.foreignKeys {
		if key.Name == toInsert.Name {
			return fmt.Errorf("constraint %s already exists", toInsert.Name)
		}
	}

	t.checks = append(t.checks, toInsert)

	return nil
}

// func (t *Table) DropCheck(ctx *sql.Context, chName string) error {} implements sql.CheckAlterableTable.
func (t *Table) DropCheck(ctx *sql.Context, chName string) error {
	return t.dropConstraint(ctx, chName)
}

func (t *Table) createIndex(name string, columns []sql.IndexColumn, constraint sql.IndexConstraint, comment string) (sql.Index, error) {
	if t.indexes[name] != nil {
		// TODO: extract a standard error type for this
		return nil, fmt.Errorf("Error: index already exists")
	}

	exprs := make([]sql.Expression, len(columns))
	for i, column := range columns {
		idx, field := t.getField(column.Name)
		exprs[i] = expression.NewGetFieldWithTable(idx, field.Type, t.name, field.Name, field.Nullable)
	}

	return &UnmergeableIndex{
		MergeableIndex{
			DB:         "",
			DriverName: "",
			Tbl:        t,
			TableName:  t.name,
			Exprs:      exprs,
			Name:       name,
			Unique:     constraint == sql.IndexConstraint_Unique,
			CommentStr: comment,
		},
	}, nil
}

// getField returns the index and column index with the name given, if it exists, or -1, nil otherwise.
func (t *Table) getField(col string) (int, *sql.Column) {
	i := t.schema.IndexOf(col, t.name)
	if i == -1 {
		return -1, nil
	}

	return i, t.schema[i]
}

// CreateIndex implements sql.IndexAlterableTable
func (t *Table) CreateIndex(ctx *sql.Context, indexName string, using sql.IndexUsing, constraint sql.IndexConstraint, columns []sql.IndexColumn, comment string) error {
	if t.indexes == nil {
		t.indexes = make(map[string]sql.Index)
	}

	index, err := t.createIndex(indexName, columns, constraint, comment)
	if err != nil {
		return err
	}

	t.indexes[indexName] = index
	return nil
}

// DropIndex implements sql.IndexAlterableTable
func (t *Table) DropIndex(ctx *sql.Context, indexName string) error {
	for name := range t.indexes {
		if name == indexName {
			delete(t.indexes, name)
		}
	}
	return nil
}

// RenameIndex implements sql.IndexAlterableTable
func (t *Table) RenameIndex(ctx *sql.Context, fromIndexName string, toIndexName string) error {
	for name, index := range t.indexes {
		if name == fromIndexName {
			delete(t.indexes, name)
			t.indexes[toIndexName] = index
		}
	}
	return nil
}

// WithIndexLookup implements the sql.IndexAddressableTable interface.
func (t *Table) WithIndexLookup(lookup sql.IndexLookup) sql.Table {
	if lookup == nil {
		return t
	}

	nt := *t
	nt.lookup = lookup

	return &nt
}

// IndexKeyValues implements the sql.IndexableTable interface.
func (t *Table) IndexKeyValues(
	ctx *sql.Context,
	colNames []string,
) (sql.PartitionIndexKeyValueIter, error) {
	iter, err := t.Partitions(ctx)
	if err != nil {
		return nil, err
	}

	columns, err := t.columnIndexes(colNames)
	if err != nil {
		return nil, err
	}

	return &partitionIndexKeyValueIter{
		table:   t,
		iter:    iter,
		columns: columns,
		ctx:     ctx,
	}, nil
}

// Filters implements the sql.FilteredTable interface.
func (t *Table) Filters() []sql.Expression {
	return t.filters
}

func (t *Table) generateCheckName() string {
	i := 1
Top:
	for {
		name := fmt.Sprintf("%s_chk_%d", t.name, i)
		for _, check := range t.checks {
			if check.Name == name {
				i++
				continue Top
			}
		}
		return name
	}
}

type partitionIndexKeyValueIter struct {
	table   *Table
	iter    sql.PartitionIter
	columns []int
	ctx     *sql.Context
}

func (i *partitionIndexKeyValueIter) Next() (sql.Partition, sql.IndexKeyValueIter, error) {
	p, err := i.iter.Next()
	if err != nil {
		return nil, nil, err
	}

	iter, err := i.table.PartitionRows(i.ctx, p)
	if err != nil {
		return nil, nil, err
	}

	return p, &indexKeyValueIter{
		key:     string(p.Key()),
		iter:    iter,
		columns: i.columns,
	}, nil
}

func (i *partitionIndexKeyValueIter) Close(ctx *sql.Context) error {
	return i.iter.Close(ctx)
}

var errColumnNotFound = errors.NewKind("could not find column %s")

type indexKeyValueIter struct {
	key     string
	iter    sql.RowIter
	columns []int
	pos     int
}

func (i *indexKeyValueIter) Next() ([]interface{}, []byte, error) {
	row, err := i.iter.Next()
	if err != nil {
		return nil, nil, err
	}

	value := &IndexValue{Key: i.key, Pos: i.pos}
	data, err := EncodeIndexValue(value)
	if err != nil {
		return nil, nil, err
	}

	i.pos++
	return projectOnRow(i.columns, row), data, nil
}

func (i *indexKeyValueIter) Close(ctx *sql.Context) error {
	return i.iter.Close(ctx)
}
