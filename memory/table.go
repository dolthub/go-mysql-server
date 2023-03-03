// Copyright 2020-2022 Dolthub, Inc.
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
	"math"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/dolthub/vitess/go/sqltypes"
	errors "gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/transform"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// Table represents an in-memory database table.
type Table struct {
	// Schema and related info
	name             string
	schema           sql.PrimaryKeySchema
	indexes          map[string]sql.Index
	fkColl           *ForeignKeyCollection
	checks           []sql.CheckDefinition
	collation        sql.CollationID
	pkIndexesEnabled bool

	// pushdown info
	filters         []sql.Expression // currently unused, filter pushdown is significantly broken right now
	projection      []string
	projectedSchema sql.Schema
	columns         []int

	// Data storage
	partitions    map[string][]sql.Row
	partitionKeys [][]byte

	// Insert bookkeeping
	insertPartIdx int

	// Indexed lookups
	lookup sql.DriverIndexLookup

	// AUTO_INCREMENT bookkeeping
	autoIncVal uint64
	autoColIdx int

	tableStats *sql.TableStatistics
}

var _ sql.Table = (*Table)(nil)
var _ sql.Table2 = (*Table)(nil)
var _ sql.InsertableTable = (*Table)(nil)
var _ sql.UpdatableTable = (*Table)(nil)
var _ sql.DeletableTable = (*Table)(nil)
var _ sql.ReplaceableTable = (*Table)(nil)
var _ sql.TruncateableTable = (*Table)(nil)
var _ sql.DriverIndexableTable = (*Table)(nil)
var _ sql.AlterableTable = (*Table)(nil)
var _ sql.IndexAlterableTable = (*Table)(nil)

var _ sql.ForeignKeyTable = (*Table)(nil)
var _ sql.CheckAlterableTable = (*Table)(nil)
var _ sql.CheckTable = (*Table)(nil)
var _ sql.AutoIncrementTable = (*Table)(nil)
var _ sql.StatisticsTable = (*Table)(nil)
var _ sql.ProjectedTable = (*Table)(nil)
var _ sql.PrimaryKeyAlterableTable = (*Table)(nil)
var _ sql.PrimaryKeyTable = (*Table)(nil)

// NewTable creates a new Table with the given name and schema. Assigns the default collation, therefore if a different
// collation is desired, please use NewTableWithCollation.
func NewTable(name string, schema sql.PrimaryKeySchema, fkColl *ForeignKeyCollection) *Table {
	return NewPartitionedTableWithCollation(name, schema, fkColl, 0, sql.Collation_Default)
}

// NewTableWithCollation creates a new Table with the given name, schema, and collation.
func NewTableWithCollation(name string, schema sql.PrimaryKeySchema, fkColl *ForeignKeyCollection, collation sql.CollationID) *Table {
	return NewPartitionedTableWithCollation(name, schema, fkColl, 0, collation)
}

// NewPartitionedTable creates a new Table with the given name, schema and number of partitions. Assigns the default
// collation, therefore if a different collation is desired, please use NewPartitionedTableWithCollation.
func NewPartitionedTable(name string, schema sql.PrimaryKeySchema, fkColl *ForeignKeyCollection, numPartitions int) *Table {
	return NewPartitionedTableWithCollation(name, schema, fkColl, numPartitions, sql.Collation_Default)
}

// NewPartitionedTableWithCollation creates a new Table with the given name, schema, number of partitions, and collation.
func NewPartitionedTableWithCollation(name string, schema sql.PrimaryKeySchema, fkColl *ForeignKeyCollection, numPartitions int, collation sql.CollationID) *Table {
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

	var autoIncVal uint64
	autoIncIdx := -1
	for i, c := range schema.Schema {
		if c.AutoIncrement {
			autoIncVal = uint64(1)
			autoIncIdx = i
			break
		}
	}

	return &Table{
		name:          name,
		schema:        schema,
		fkColl:        fkColl,
		collation:     collation,
		partitions:    partitions,
		partitionKeys: keys,
		autoIncVal:    autoIncVal,
		autoColIdx:    autoIncIdx,
	}
}

// Name implements the sql.Table interface.
func (t Table) Name() string {
	return t.name
}

// Schema implements the sql.Table interface.
func (t *Table) Schema() sql.Schema {
	if t.projectedSchema != nil {
		return t.projectedSchema
	}
	return t.schema.Schema
}

// Collation implements the sql.Table interface.
func (t *Table) Collation() sql.CollationID {
	return t.collation
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
	for _, k := range t.partitionKeys {
		if rows, ok := t.partitions[string(k)]; ok && len(rows) > 0 {
			keys = append(keys, k)
		}
	}
	return &partitionIter{keys: keys}, nil
}

// rangePartitionIter returns a partition that has range and table data access
type rangePartitionIter struct {
	child  *partitionIter
	ranges sql.Expression
}

var _ sql.PartitionIter = (*rangePartitionIter)(nil)

func (i rangePartitionIter) Close(ctx *sql.Context) error {
	return i.child.Close(ctx)
}

func (i rangePartitionIter) Next(ctx *sql.Context) (sql.Partition, error) {
	part, err := i.child.Next(ctx)
	if err != nil {
		return nil, err
	}
	return &rangePartition{
		Partition: part.(*Partition),
		rang:      i.ranges,
	}, nil
}

type rangePartition struct {
	*Partition
	rang sql.Expression
}

// spatialRangePartitionIter returns a partition that has range and table data access
type spatialRangePartitionIter struct {
	child                  *partitionIter
	ord                    int
	minX, minY, maxX, maxY float64
}

var _ sql.PartitionIter = (*spatialRangePartitionIter)(nil)

func (i spatialRangePartitionIter) Close(ctx *sql.Context) error {
	return i.child.Close(ctx)
}

func (i spatialRangePartitionIter) Next(ctx *sql.Context) (sql.Partition, error) {
	part, err := i.child.Next(ctx)
	if err != nil {
		return nil, err
	}
	return &spatialRangePartition{
		Partition: part.(*Partition),
		ord:       i.ord,
		minX:      i.minX,
		minY:      i.minY,
		maxX:      i.maxX,
		maxY:      i.maxY,
	}, nil
}

type spatialRangePartition struct {
	*Partition
	ord                    int
	minX, minY, maxX, maxY float64
}

// PartitionCount implements the sql.PartitionCounter interface.
func (t *Table) PartitionCount(ctx *sql.Context) (int64, error) {
	return int64(len(t.partitions)), nil
}

// PartitionRows implements the sql.PartitionRows interface.
func (t *Table) PartitionRows(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	filters := t.filters
	if r, ok := partition.(*rangePartition); ok {
		// index lookup is currently a single filter applied to a full table scan
		filters = append(t.filters, r.rang)
	}

	rows, ok := t.partitions[string(partition.Key())]
	if !ok {
		return nil, sql.ErrPartitionNotFound.New(partition.Key())
	}
	// The slice could be altered by other operations taking place during iteration (such as deletion or insertion), so
	// make a copy of the values as they exist when execution begins.
	rowsCopy := make([]sql.Row, len(rows))
	copy(rowsCopy, rows)

	if r, ok := partition.(*spatialRangePartition); ok {
		return &spatialTableIter{
			columns: t.columns,
			ord:     r.ord,
			minX:    r.minX,
			minY:    r.minY,
			maxX:    r.maxX,
			maxY:    r.maxY,
			rows:    rowsCopy,
		}, nil
	}

	return &tableIter{
		rows:    rowsCopy,
		columns: t.columns,
		filters: filters,
	}, nil
}

func (t *Table) numRows(ctx *sql.Context) (uint64, error) {
	var count uint64
	for _, rows := range t.partitions {
		count += uint64(len(rows))
	}

	return count, nil
}

func (t *Table) DataLength(ctx *sql.Context) (uint64, error) {
	var numBytesPerRow uint64
	for _, col := range t.schema.Schema {
		switch n := col.Type.(type) {
		case sql.NumberType:
			numBytesPerRow += 8
		case sql.StringType:
			numBytesPerRow += uint64(n.MaxByteLength())
		case types.BitType:
			numBytesPerRow += 1
		case sql.DatetimeType:
			numBytesPerRow += 8
		case sql.DecimalType:
			numBytesPerRow += uint64(n.MaximumScale())
		case sql.EnumType:
			numBytesPerRow += 2
		case types.JsonType:
			numBytesPerRow += 20
		case sql.NullType:
			numBytesPerRow += 1
		case types.TimeType:
			numBytesPerRow += 16
		case sql.YearType:
			numBytesPerRow += 8
		default:
			numBytesPerRow += 0
		}
	}

	numRows, err := t.numRows(ctx)
	if err != nil {
		return 0, err
	}

	return numBytesPerRow * numRows, nil
}

// AnalyzeTable implements the sql.StatisticsTable interface.
func (t *Table) AnalyzeTable(ctx *sql.Context) error {
	// initialize histogram map
	t.tableStats = &sql.TableStatistics{
		CreatedAt: time.Now(),
	}

	histMap, err := NewHistogramMapFromTable(ctx, t)
	if err != nil {
		return err
	}

	t.tableStats.Histograms = histMap
	for _, v := range histMap {
		t.tableStats.RowCount = v.Count + v.NullCount
		break
	}

	return nil
}

func (t *Table) RowCount(ctx *sql.Context) (uint64, error) {
	return t.numRows(ctx)
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

func (p *partitionIter) Next(*sql.Context) (sql.Partition, error) {
	if p.pos >= len(p.keys) {
		return nil, io.EOF
	}

	key := p.keys[p.pos]
	p.pos++
	return &Partition{key}, nil
}

func (p *partitionIter) Close(*sql.Context) error { return nil }

type tableIter struct {
	columns []int
	filters []sql.Expression

	rows        []sql.Row
	indexValues sql.IndexValueIter
	pos         int
}

var _ sql.RowIter = (*tableIter)(nil)
var _ sql.RowIter2 = (*tableIter)(nil)

func (i *tableIter) Next(ctx *sql.Context) (sql.Row, error) {
	row, err := i.getRow(ctx)
	if err != nil {
		return nil, err
	}

	for _, f := range i.filters {
		result, err := f.Eval(ctx, row)
		if err != nil {
			return nil, err
		}
		result, _ = types.ConvertToBool(result)
		if result != true {
			return i.Next(ctx)
		}
	}

	if i.columns != nil {
		resultRow := make(sql.Row, len(i.columns))
		for i, j := range i.columns {
			resultRow[i] = row[j]
		}
		return resultRow, nil
	}

	return row, nil
}

func (i *tableIter) Next2(ctx *sql.Context, frame *sql.RowFrame) error {
	r, err := i.Next(ctx)
	if err != nil {
		return err
	}

	for _, v := range r {
		x, err := sql.ConvertToValue(v)
		if err != nil {
			return err
		}
		frame.Append(x)
	}

	return nil
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

func (i *tableIter) getRow(ctx *sql.Context) (sql.Row, error) {
	if i.indexValues != nil {
		return i.getFromIndex(ctx)
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

func (i *tableIter) getFromIndex(ctx *sql.Context) (sql.Row, error) {
	data, err := i.indexValues.Next(ctx)
	if err != nil {
		return nil, err
	}

	value, err := DecodeIndexValue(data)
	if err != nil {
		return nil, err
	}

	return i.rows[value.Pos], nil
}

type spatialTableIter struct {
	columns                []int
	rows                   []sql.Row
	pos                    int
	ord                    int
	minX, minY, maxX, maxY float64
}

var _ sql.RowIter = (*spatialTableIter)(nil)
var _ sql.RowIter2 = (*spatialTableIter)(nil)

func (i *spatialTableIter) Next(ctx *sql.Context) (sql.Row, error) {
	row, err := i.getRow(ctx)
	if err != nil {
		return nil, err
	}

	if len(i.columns) == 0 {
		return row, nil
	}

	// check if bounding boxes of geometry and range intersect
	// if the range [i.minX, i.maxX] and [gMinX, gMaxX] overlap and
	// if the range [i.minY, i.maxY] and [gMinY, gMaxY] overlap
	// then, the bounding boxes intersect
	g, ok := row[i.ord].(types.GeometryValue)
	if !ok {
		return nil, fmt.Errorf("spatial index over non-geometry column")
	}
	gMinX, gMinY, gMaxX, gMaxY := g.BBox()
	xInt := (gMinX <= i.minX && i.minX <= gMaxX) ||
		(gMinX <= i.maxX && i.maxX <= gMaxX) ||
		(i.minX <= gMinX && gMinX <= i.maxX) ||
		(i.minX <= gMaxX && gMaxX <= i.maxX)
	yInt := (gMinY <= i.minY && i.minY <= gMaxY) ||
		(gMinY <= i.maxY && i.maxY <= gMaxY) ||
		(i.minY <= gMinY && gMinY <= i.maxY) ||
		(i.minY <= gMaxY && gMaxY <= i.maxY)
	if !(xInt && yInt) {
		return i.Next(ctx)
	}

	resultRow := make(sql.Row, len(i.columns))
	for i, j := range i.columns {
		resultRow[i] = row[j]
	}
	return resultRow, nil
}

func (i *spatialTableIter) Next2(ctx *sql.Context, frame *sql.RowFrame) error {
	r, err := i.Next(ctx)
	if err != nil {
		return err
	}

	for _, v := range r {
		x, err := sql.ConvertToValue(v)
		if err != nil {
			return err
		}
		frame.Append(x)
	}

	return nil
}

func (i *spatialTableIter) Close(ctx *sql.Context) error {
	return nil
}

func (i *spatialTableIter) getRow(ctx *sql.Context) (sql.Row, error) {
	if i.pos >= len(i.rows) {
		return nil, io.EOF
	}

	row := i.rows[i.pos]
	i.pos++
	return row, nil
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

func (t *Table) Inserter(*sql.Context) sql.RowInserter {
	return t.newTableEditor()
}

func (t *Table) Updater(*sql.Context) sql.RowUpdater {
	return t.newTableEditor()
}

func (t *Table) Replacer(*sql.Context) sql.RowReplacer {
	return t.newTableEditor()
}

func (t *Table) Deleter(*sql.Context) sql.RowDeleter {
	return t.newTableEditor()
}

func (t *Table) AutoIncrementSetter(*sql.Context) sql.AutoIncrementSetter {
	return t.newTableEditor()
}

func (t *Table) newTableEditor() *tableEditor {
	var uniqIdxCols [][]int
	var prefixLengths [][]uint16
	for _, idx := range t.indexes {
		if !idx.IsUnique() {
			continue
		}
		var colNames []string
		expressions := idx.(*Index).Exprs
		for _, exp := range expressions {
			colNames = append(colNames, exp.(*expression.GetField).Name())
		}
		colIdxs, err := t.columnIndexes(colNames)
		if err != nil {
			panic("failed to get column indexes")
		}
		uniqIdxCols = append(uniqIdxCols, colIdxs)
		prefixLengths = append(prefixLengths, idx.PrefixLengths())
	}
	return &tableEditor{
		table:             t,
		initialAutoIncVal: 1,
		initialPartitions: nil,
		ea:                NewTableEditAccumulator(t),
		initialInsert:     0,
		uniqueIdxCols:     uniqIdxCols,
		prefixLengths:     prefixLengths,
	}
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

// PeekNextAutoIncrementValue peeks at the next AUTO_INCREMENT value
func (t *Table) PeekNextAutoIncrementValue(*sql.Context) (uint64, error) {
	return t.autoIncVal, nil
}

// GetNextAutoIncrementValue gets the next auto increment value for the memory table the increment.
func (t *Table) GetNextAutoIncrementValue(ctx *sql.Context, insertVal interface{}) (uint64, error) {
	cmp, err := types.Uint64.Compare(insertVal, t.autoIncVal)
	if err != nil {
		return 0, err
	}

	if cmp > 0 && insertVal != nil {
		v, err := types.Uint64.Convert(insertVal)
		if err != nil {
			return 0, err
		}
		t.autoIncVal = v.(uint64)
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
	newSch := make(sql.Schema, len(t.schema.Schema)+1)

	// TODO: need to fix this in the engine itself
	if newCol.PrimaryKey {
		newCol.Nullable = false
	}

	newColIdx := 0
	var i int
	if order != nil && order.First {
		newSch[i] = newCol
		i++
	}

	for _, col := range t.schema.Schema {
		newSch[i] = col
		i++
		if (order != nil && order.AfterColumn == col.Name) || (order == nil && i == len(t.schema.Schema)) {
			newSch[i] = newCol
			newColIdx = i
			i++
		}
	}

	for i, newSchCol := range newSch {
		if i == newColIdx {
			continue
		}
		newDefault, _, _ := transform.Expr(newSchCol.Default, func(expr sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
			if expr, ok := expr.(*expression.GetField); ok {
				return expr.WithIndex(newSch.IndexOf(expr.Name(), t.name)), transform.NewTree, nil
			}
			return expr, transform.SameTree, nil
		})
		newSchCol.Default = newDefault.(*sql.ColumnDefaultValue)
	}

	if newCol.AutoIncrement {
		t.autoColIdx = newColIdx
		t.autoIncVal = 0

		if newColIdx < len(t.schema.Schema) {
			for _, p := range t.partitions {
				for _, row := range p {
					if row[newColIdx] == nil {
						continue
					}

					cmp, err := newCol.Type.Compare(row[newColIdx], t.autoIncVal)
					if err != nil {
						panic(err)
					}

					if cmp > 0 {
						var val interface{}
						val, err = types.Uint64.Convert(row[newColIdx])
						if err != nil {
							panic(err)
						}
						t.autoIncVal = val.(uint64)
					}
				}
			}
		} else {
			t.autoIncVal = 0
		}

		t.autoIncVal++
	}

	newPkOrds := t.schema.PkOrdinals
	for i := 0; i < len(newPkOrds); i++ {
		// added column shifts the index of every column after
		// all ordinals above addIdx will be bumped
		if newColIdx <= newPkOrds[i] {
			newPkOrds[i]++
		}
	}

	t.schema = sql.NewPrimaryKeySchema(newSch, newPkOrds...)

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
			if !t.schema.Schema[idx].Nullable && colDefault == nil {
				newRow[idx] = t.schema.Schema[idx].Type.Zero()
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
	newSch := make(sql.Schema, len(t.schema.Schema)-1)
	var i int
	droppedCol := -1
	for _, col := range t.schema.Schema {
		if col.Name != columnName {
			newSch[i] = col
			i++
		} else {
			droppedCol = i
		}
	}

	newPkOrds := t.schema.PkOrdinals
	for i := 0; i < len(newPkOrds); i++ {
		// deleting a column will shift subsequent column indices left
		// PK ordinals after dropIdx bumped down
		if droppedCol <= newPkOrds[i] {
			newPkOrds[i]--
		}
	}

	t.schema = sql.NewPrimaryKeySchema(newSch, newPkOrds...)
	return droppedCol
}

func (t *Table) ModifyColumn(ctx *sql.Context, columnName string, column *sql.Column, order *sql.ColumnOrder) error {
	oldIdx := -1
	newIdx := 0
	for i, col := range t.schema.Schema {
		if col.Name == columnName {
			oldIdx = i
			column.PrimaryKey = col.PrimaryKey
			// We've removed auto increment through this modification so we need to do some bookkeeping
			if col.AutoIncrement && !column.AutoIncrement {
				t.autoColIdx = -1
				t.autoIncVal = 0
			}
			break
		}
	}
	if order == nil {
		newIdx = oldIdx
		if newIdx == 0 {
			order = &sql.ColumnOrder{First: true}
		} else {
			order = &sql.ColumnOrder{AfterColumn: t.schema.Schema[newIdx-1].Name}
		}
	} else if !order.First {
		var oldSchemaWithoutCol sql.Schema
		oldSchemaWithoutCol = append(oldSchemaWithoutCol, t.schema.Schema[:oldIdx]...)
		oldSchemaWithoutCol = append(oldSchemaWithoutCol, t.schema.Schema[oldIdx+1:]...)
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
				if sql.ErrNotMatchingSRID.Is(err) {
					err = sql.ErrNotMatchingSRIDWithColName.New(columnName, err)
				}
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

	pkNameToOrdIdx := make(map[string]int)
	for i, ord := range t.schema.PkOrdinals {
		pkNameToOrdIdx[t.schema.Schema[ord].Name] = i
	}

	_ = t.dropColumnFromSchema(ctx, columnName)
	t.addColumnToSchema(ctx, column, order)

	newPkOrds := make([]int, len(t.schema.PkOrdinals))
	for ord, col := range t.schema.Schema {
		if col.PrimaryKey {
			i := pkNameToOrdIdx[col.Name]
			newPkOrds[i] = ord
		}
	}

	t.schema.PkOrdinals = newPkOrds

	for _, index := range t.indexes {
		memIndex := index.(*Index)
		nameLowercase := strings.ToLower(columnName)
		for i, expr := range memIndex.Exprs {
			getField := expr.(*expression.GetField)
			if strings.ToLower(getField.Name()) == nameLowercase {
				memIndex.Exprs[i] = expression.NewGetFieldWithTable(newIdx, column.Type, getField.Table(), column.Name, column.Nullable)
			}
		}
	}

	return nil
}

// PrimaryKeySchema implements sql.PrimaryKeyAlterableTable
func (t *Table) PrimaryKeySchema() sql.PrimaryKeySchema {
	return t.schema
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

	children := []string{fmt.Sprintf("name: %s", t.name)}
	if t.lookup != nil {
		children = append(children, fmt.Sprintf("index: %s", t.lookup))
	}

	if len(t.columns) > 0 {
		var projections []string
		for _, column := range t.columns {
			projections = append(projections, fmt.Sprintf("%d", column))
		}
		children = append(children, fmt.Sprintf("projections: %s", projections))

	}

	if len(t.filters) > 0 {
		var filters []string
		for _, filter := range t.filters {
			filters = append(filters, fmt.Sprintf("%s", sql.DebugString(filter)))
		}
		children = append(children, fmt.Sprintf("filters: %s", filters))
	}
	_ = p.WriteNode("Table")
	p.WriteChildren(children...)
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

// FilteredTable functionality in the Table type was disabled for a long period of time, and has developed major
// issues with the current analyzer logic. It's only used in the pushdown unit tests, and sql.FilteredTable should be
// considered unstable until this situation is fixed.
type FilteredTable struct {
	*Table
}

var _ sql.FilteredTable = (*FilteredTable)(nil)

func NewFilteredTable(name string, schema sql.PrimaryKeySchema, fkColl *ForeignKeyCollection) *FilteredTable {
	return &FilteredTable{
		Table: NewTable(name, schema, fkColl),
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

// WithProjections implements sql.ProjectedTable
func (t *FilteredTable) WithProjections(schema []string) sql.Table {
	table := t.Table.WithProjections(schema)

	nt := *t
	nt.Table = table.(*Table)
	return &nt
}

// Projections implements sql.ProjectedTable
func (t *FilteredTable) Projections() []string {
	return t.projection
}

// IndexedTable is a table that expects to return one or more partitions
// for range lookups.
type IndexedTable struct {
	*Table
	Idx *Index
}

func (t *IndexedTable) LookupPartitions(ctx *sql.Context, lookup sql.IndexLookup) (sql.PartitionIter, error) {
	filter, err := lookup.Index.(*Index).rangeFilterExpr(lookup.Ranges...)
	if err != nil {
		return nil, err
	}
	child, err := t.Table.Partitions(ctx)
	if err != nil {
		return nil, err
	}

	if lookup.Index.IsSpatial() {
		lower := sql.GetRangeCutKey(lookup.Ranges[0][0].LowerBound)
		upper := sql.GetRangeCutKey(lookup.Ranges[0][0].UpperBound)
		minPoint, ok := lower.(types.Point)
		if !ok {
			return nil, sql.ErrInvalidGISData.New()
		}
		maxPoint, ok := upper.(types.Point)
		if !ok {
			return nil, sql.ErrInvalidGISData.New()
		}

		ord := lookup.Index.(*Index).Exprs[0].(*expression.GetField).Index()
		return spatialRangePartitionIter{
			child: child.(*partitionIter),
			ord:   ord,
			minX:  minPoint.X,
			minY:  minPoint.Y,
			maxX:  maxPoint.X,
			maxY:  maxPoint.Y,
		}, nil
	}

	return rangePartitionIter{child: child.(*partitionIter), ranges: filter}, nil
}

// PartitionRows implements the sql.PartitionRows interface.
func (t *IndexedTable) PartitionRows(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	iter, err := t.Table.PartitionRows(ctx, partition)
	if err != nil {
		return nil, err
	}
	if t.Idx != nil {
		sf := make(sql.SortFields, len(t.Idx.Exprs))
		for i, e := range t.Idx.Exprs {
			sf[i] = sql.SortField{Column: e}
		}
		var sorter *expression.Sorter
		if i, ok := iter.(*tableIter); ok {
			sorter = &expression.Sorter{
				SortFields: sf,
				Rows:       i.rows,
				LastError:  nil,
				Ctx:        ctx,
			}
		} else if i, ok := iter.(*spatialTableIter); ok {
			sorter = &expression.Sorter{
				SortFields: sf,
				Rows:       i.rows,
				LastError:  nil,
				Ctx:        ctx,
			}
		}

		sort.Stable(sorter)
	}
	return iter, nil
}

func (t *Table) IndexedAccess(i sql.IndexLookup) sql.IndexedTable {
	return &IndexedTable{Table: t, Idx: i.Index.(*Index)}
}

// WithProjections implements sql.ProjectedTable
func (t *Table) WithProjections(cols []string) sql.Table {
	nt := *t
	columns, err := nt.columnIndexes(cols)
	if err != nil {
		panic(err)
	}

	nt.columns = columns

	projectedSchema := make(sql.Schema, len(columns))
	for i, j := range columns {
		projectedSchema[i] = nt.schema.Schema[j]
	}
	nt.projectedSchema = projectedSchema
	nt.projection = cols

	return &nt
}

// Projections implements sql.ProjectedTable
func (t *Table) Projections() []string {
	return t.projection
}

func (t *Table) columnIndexes(colNames []string) ([]int, error) {
	columns := make([]int, 0, len(colNames))

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
		if len(t.schema.PkOrdinals) > 0 {
			exprs := make([]sql.Expression, len(t.schema.PkOrdinals))
			for i, ord := range t.schema.PkOrdinals {
				column := t.schema.Schema[ord]
				idx, field := t.getField(column.Name)
				exprs[i] = expression.NewGetFieldWithTable(idx, field.Type, t.name, field.Name, field.Nullable)
			}
			indexes = append(indexes, &Index{
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

// GetDeclaredForeignKeys implements the interface sql.ForeignKeyTable.
func (t *Table) GetDeclaredForeignKeys(ctx *sql.Context) ([]sql.ForeignKeyConstraint, error) {
	//TODO: may not be the best location, need to handle db as well
	var fks []sql.ForeignKeyConstraint
	lowerName := strings.ToLower(t.name)
	for _, fk := range t.fkColl.Keys() {
		if strings.ToLower(fk.Table) == lowerName {
			fks = append(fks, fk)
		}
	}
	return fks, nil
}

// GetReferencedForeignKeys implements the interface sql.ForeignKeyTable.
func (t *Table) GetReferencedForeignKeys(ctx *sql.Context) ([]sql.ForeignKeyConstraint, error) {
	//TODO: may not be the best location, need to handle db as well
	var fks []sql.ForeignKeyConstraint
	lowerName := strings.ToLower(t.name)
	for _, fk := range t.fkColl.Keys() {
		if strings.ToLower(fk.ParentTable) == lowerName {
			fks = append(fks, fk)
		}
	}
	return fks, nil
}

// AddForeignKey implements sql.ForeignKeyTable. Foreign partitionKeys are not enforced on update / delete.
func (t *Table) AddForeignKey(ctx *sql.Context, fk sql.ForeignKeyConstraint) error {
	lowerName := strings.ToLower(fk.Name)
	for _, key := range t.fkColl.Keys() {
		if strings.ToLower(key.Name) == lowerName {
			return fmt.Errorf("Constraint %s already exists", fk.Name)
		}
	}
	t.fkColl.AddFK(fk)
	return nil
}

// DropForeignKey implements sql.ForeignKeyTable.
func (t *Table) DropForeignKey(ctx *sql.Context, fkName string) error {
	if t.fkColl.DropFK(fkName) {
		return nil
	}
	return sql.ErrForeignKeyNotFound.New(fkName, t.name)
}

// UpdateForeignKey implements sql.ForeignKeyTable.
func (t *Table) UpdateForeignKey(ctx *sql.Context, fkName string, fk sql.ForeignKeyConstraint) error {
	t.fkColl.DropFK(fkName)
	return t.AddForeignKey(ctx, fk)
}

// CreateIndexForForeignKey implements sql.ForeignKeyTable.
func (t *Table) CreateIndexForForeignKey(ctx *sql.Context, idx sql.IndexDef) error {
	return t.CreateIndex(ctx, idx)
}

// SetForeignKeyResolved implements sql.ForeignKeyTable.
func (t *Table) SetForeignKeyResolved(ctx *sql.Context, fkName string) error {
	if !t.fkColl.SetResolved(fkName) {
		return sql.ErrForeignKeyNotFound.New(fkName, t.name)
	}
	return nil
}

// GetForeignKeyEditor implements sql.ForeignKeyTable.
func (t *Table) GetForeignKeyEditor(ctx *sql.Context) sql.ForeignKeyEditor {
	return t.newTableEditor()
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

	t.checks = append(t.checks, toInsert)

	return nil
}

// DropCheck implements sql.CheckAlterableTable.
func (t *Table) DropCheck(ctx *sql.Context, chName string) error {
	lowerName := strings.ToLower(chName)
	for i, key := range t.checks {
		if strings.ToLower(key.Name) == lowerName {
			t.checks = append(t.checks[:i], t.checks[i+1:]...)
			return nil
		}
	}
	//TODO: add SQL error
	return fmt.Errorf("check '%s' was not found on the table", chName)
}

func (t *Table) createIndex(name string, columns []sql.IndexColumn, constraint sql.IndexConstraint, comment string) (sql.Index, error) {
	if name == "" {
		for _, column := range columns {
			name += column.Name + "_"
		}
	}
	if t.indexes[name] != nil {
		// TODO: extract a standard error type for this
		return nil, fmt.Errorf("Error: index already exists")
	}

	exprs := make([]sql.Expression, len(columns))
	colNames := make([]string, len(columns))
	for i, column := range columns {
		idx, field := t.getField(column.Name)
		exprs[i] = expression.NewGetFieldWithTable(idx, field.Type, t.name, field.Name, field.Nullable)
		colNames[i] = column.Name
	}

	var hasNonZeroLengthColumn bool
	for _, column := range columns {
		if column.Length > 0 {
			hasNonZeroLengthColumn = true
			break
		}
	}
	var prefixLengths []uint16
	if hasNonZeroLengthColumn {
		prefixLengths = make([]uint16, len(columns))
		for i, column := range columns {
			prefixLengths[i] = uint16(column.Length)
		}
	}

	if constraint == sql.IndexConstraint_Unique {
		err := t.errIfDuplicateEntryExist(colNames, name)
		if err != nil {
			return nil, err
		}
	}

	return &Index{
		DB:         "",
		DriverName: "",
		Tbl:        t,
		TableName:  t.name,
		Exprs:      exprs,
		Name:       name,
		Unique:     constraint == sql.IndexConstraint_Unique,
		Spatial:    constraint == sql.IndexConstraint_Spatial,
		CommentStr: comment,
		PrefixLens: prefixLengths,
	}, nil
}

// throws an error if any two or more rows share the same |cols| values.
func (t *Table) errIfDuplicateEntryExist(cols []string, idxName string) error {
	columnMapping, err := t.columnIndexes(cols)
	if err != nil {
		return err
	}
	unique := make(map[uint64]struct{})
	for _, partition := range t.partitions {
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

func hasNullForAnyCols(row sql.Row, cols []int) bool {
	for _, idx := range cols {
		if row[idx] == nil {
			return true
		}
	}
	return false
}

// getField returns the index and column index with the name given, if it exists, or -1, nil otherwise.
func (t *Table) getField(col string) (int, *sql.Column) {
	i := t.schema.IndexOf(col, t.name)
	if i == -1 {
		return -1, nil
	}

	return i, t.schema.Schema[i]
}

// CreateIndex implements sql.IndexAlterableTable
func (t *Table) CreateIndex(ctx *sql.Context, idx sql.IndexDef) error {
	if t.indexes == nil {
		t.indexes = make(map[string]sql.Index)
	}

	index, err := t.createIndex(idx.Name, idx.Columns, idx.Constraint, idx.Comment)
	if err != nil {
		return err
	}

	t.indexes[index.ID()] = index // We should store the computed index name in the case of an empty index name being passed in
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
	if fromIndexName == toIndexName {
		return nil
	}
	if idx, ok := t.indexes[fromIndexName]; ok {
		delete(t.indexes, fromIndexName)
		t.indexes[toIndexName] = idx
	}
	return nil
}

// WithDriverIndexLookup implements the sql.IndexAddressableTable interface.
func (t *Table) WithDriverIndexLookup(lookup sql.DriverIndexLookup) sql.Table {
	if t.lookup != nil {
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

// CreatePrimaryKey implements the PrimaryKeyAlterableTable
func (t *Table) CreatePrimaryKey(ctx *sql.Context, columns []sql.IndexColumn) error {
	// First check that a primary key already exists
	for _, col := range t.schema.Schema {
		if col.PrimaryKey {
			return sql.ErrMultiplePrimaryKeysDefined.New()
		}
	}

	potentialSchema := copyschema(t.schema.Schema)

	pkOrdinals := make([]int, len(columns))
	for i, newCol := range columns {
		found := false
		for j, currCol := range potentialSchema {
			if strings.ToLower(currCol.Name) == strings.ToLower(newCol.Name) {
				if types.IsText(currCol.Type) && newCol.Length > 0 {
					return sql.ErrUnsupportedIndexPrefix.New(currCol.Name)
				}
				currCol.PrimaryKey = true
				currCol.Nullable = false
				found = true
				pkOrdinals[i] = j
				break
			}
		}

		if !found {
			return sql.ErrKeyColumnDoesNotExist.New(newCol.Name)
		}
	}

	pkSchema := sql.NewPrimaryKeySchema(potentialSchema, pkOrdinals...)
	newTable, err := newTable(t, pkSchema)
	if err != nil {
		return err
	}

	t.schema = pkSchema
	t.partitions = newTable.partitions
	t.partitionKeys = newTable.partitionKeys

	return nil
}

// Sorts the rows in the partitions of the table to be in primary key order.
func (t *Table) sortRows() {
	type pkfield struct {
		i int
		c *sql.Column
	}
	var pk []pkfield
	for _, column := range t.schema.Schema {
		if column.PrimaryKey {
			idx, col := t.getField(column.Name)
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
	for _, k := range t.partitionKeys {
		p := t.partitions[string(k)]
		for i := 0; i < len(p); i++ {
			idx = append(idx, partidx{string(k), i})
		}
	}

	sort.Sort(partitionssort{t.partitions, idx, less})
}

type partidx struct {
	key string
	i   int
}

type partitionssort struct {
	ps   map[string][]sql.Row
	idx  []partidx
	less func(l, r sql.Row) bool
}

func (ps partitionssort) Len() int {
	return len(ps.idx)
}

func (ps partitionssort) Less(i, j int) bool {
	lidx := ps.idx[i]
	ridx := ps.idx[j]
	lr := ps.ps[lidx.key][lidx.i]
	rr := ps.ps[ridx.key][ridx.i]
	return ps.less(lr, rr)
}

func (ps partitionssort) Swap(i, j int) {
	lidx := ps.idx[i]
	ridx := ps.idx[j]
	ps.ps[lidx.key][lidx.i], ps.ps[ridx.key][ridx.i] = ps.ps[ridx.key][ridx.i], ps.ps[lidx.key][lidx.i]
}

func copyschema(sch sql.Schema) sql.Schema {
	potentialSchema := make(sql.Schema, len(sch))

	for i, c := range sch {
		potentialSchema[i] = &sql.Column{
			Name:          c.Name,
			Type:          c.Type,
			Default:       c.Default,
			AutoIncrement: c.AutoIncrement,
			Nullable:      c.Nullable,
			Source:        c.Source,
			PrimaryKey:    c.PrimaryKey,
			Comment:       c.Comment,
			Extra:         c.Extra,
		}
	}

	return potentialSchema
}

func newTable(t *Table, newSch sql.PrimaryKeySchema) (*Table, error) {
	newTable := NewPartitionedTableWithCollation(t.name, newSch, t.fkColl, len(t.partitions), t.collation)
	for _, partition := range t.partitions {
		for _, partitionRow := range partition {
			err := newTable.Insert(sql.NewEmptyContext(), partitionRow)
			if err != nil {
				return nil, err
			}
		}
	}

	return newTable, nil
}

// DropPrimaryKey implements the PrimaryKeyAlterableTable
func (t *Table) DropPrimaryKey(ctx *sql.Context) error {
	// Must drop auto increment property before dropping primary key
	if t.schema.HasAutoIncrement() {
		return sql.ErrWrongAutoKey.New()
	}

	pks := make([]*sql.Column, 0)
	for _, col := range t.schema.Schema {
		if col.PrimaryKey {
			pks = append(pks, col)
		}
	}

	if len(pks) == 0 {
		return sql.ErrCantDropFieldOrKey.New("PRIMARY")
	}

	// Check for foreign key relationships
	for _, pk := range pks {
		if columnInFkRelationship(pk.Name, t.fkColl.Keys()) {
			return sql.ErrCantDropIndex.New("PRIMARY")
		}
	}

	for _, c := range pks {
		c.PrimaryKey = false
	}

	delete(t.indexes, "PRIMARY")

	t.schema.PkOrdinals = []int{}

	return nil
}

func columnInFkRelationship(col string, fkc []sql.ForeignKeyConstraint) bool {
	colsInFks := make(map[string]bool)
	for _, fk := range fkc {
		allCols := append(fk.Columns, fk.ParentColumns...)
		for _, ac := range allCols {
			colsInFks[ac] = true
		}
	}

	return colsInFks[col]
}

type partitionIndexKeyValueIter struct {
	table   *Table
	iter    sql.PartitionIter
	columns []int
}

func (i *partitionIndexKeyValueIter) Next(ctx *sql.Context) (sql.Partition, sql.IndexKeyValueIter, error) {
	p, err := i.iter.Next(ctx)
	if err != nil {
		return nil, nil, err
	}

	iter, err := i.table.PartitionRows(ctx, p)
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

func (i *indexKeyValueIter) Next(ctx *sql.Context) ([]interface{}, []byte, error) {
	row, err := i.iter.Next(ctx)
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

func (t *Table) PartitionRows2(ctx *sql.Context, partition sql.Partition) (sql.RowIter2, error) {
	iter, err := t.PartitionRows(ctx, partition)
	if err != nil {
		return nil, err
	}

	return iter.(*tableIter), nil
}

func (t *Table) verifyRowTypes(row sql.Row) {
	//TODO: only run this when in testing mode
	if len(row) == len(t.schema.Schema) {
		for i := range t.schema.Schema {
			col := t.schema.Schema[i]
			rowVal := row[i]
			valType := reflect.TypeOf(rowVal)
			expectedType := col.Type.ValueType()
			if valType != expectedType && rowVal != nil && !valType.AssignableTo(expectedType) {
				panic(fmt.Errorf("Actual Value Type: %s, Expected Value Type: %s", valType.String(), expectedType.String()))
			}
		}
	}
}

// NewHistogramMapFromTable will construct a HistogramMap given a Table
// TODO: this is copied from the information_schema package, and should be moved to a more general location
func NewHistogramMapFromTable(ctx *sql.Context, t sql.Table) (sql.HistogramMap, error) {
	// initialize histogram map
	histMap := make(sql.HistogramMap)
	cols := t.Schema()
	for _, col := range cols {
		hist := new(sql.Histogram)
		hist.Min = math.MaxFloat64
		hist.Max = -math.MaxFloat64
		histMap[col.Name] = hist
	}

	// freqMap can be adapted to a histogram with any number of buckets
	freqMap := make(map[string]map[float64]uint64)
	for _, col := range cols {
		freqMap[col.Name] = make(map[float64]uint64)
	}

	partIter, err := t.Partitions(ctx)
	if err != nil {
		return nil, err
	}

	for {
		part, err := partIter.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		iter, err := t.PartitionRows(ctx, part)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		for {
			row, err := iter.Next(ctx)
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, err
			}

			for i, col := range cols {
				hist, ok := histMap[col.Name]
				if !ok {
					panic("histogram was not initialized for this column; shouldn't be possible")
				}

				if row[i] == nil {
					hist.NullCount++
					continue
				}

				val, err := types.Float64.Convert(row[i])
				if err != nil {
					continue // silently skip unsupported column types for now
				}
				v := val.(float64)

				if freq, ok := freqMap[col.Name][v]; ok {
					freq++
				} else {
					freqMap[col.Name][v] = 1
					hist.DistinctCount++
				}

				hist.Mean += v
				hist.Min = math.Min(hist.Min, v)
				hist.Max = math.Max(hist.Max, v)
				hist.Count++
			}
		}
	}

	// add buckets to histogram in sorted order
	for colName, freqs := range freqMap {
		keys := make([]float64, 0)
		for k := range freqs {
			keys = append(keys, k)
		}
		sort.Float64s(keys)

		hist := histMap[colName]
		if hist.Count == 0 {
			hist.Min = 0
			hist.Max = 0
			continue
		}

		hist.Mean /= float64(hist.Count)
		for _, k := range keys {
			bucket := &sql.HistogramBucket{
				LowerBound: k,
				UpperBound: k,
				Frequency:  float64(freqs[k]) / float64(hist.Count),
			}
			hist.Buckets = append(hist.Buckets, bucket)
		}
	}

	return histMap, nil
}
