package memory

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io"
	"sort"
	"strconv"

	errors "gopkg.in/src-d/go-errors.v1"

	"github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/liquidata-inc/go-mysql-server/sql/expression"
)

// Table represents an in-memory database table.
type Table struct {
	name       string
	schema     sql.Schema
	partitions map[string][]sql.Row
	keys       [][]byte

	insert int

	filters          []sql.Expression
	projection       []string
	columns          []int
	lookup           sql.IndexLookup
	indexes          map[string]sql.Index
	pkIndexesEnabled bool
	foreignKeys      []sql.ForeignKeyConstraint
}

var _ sql.Table = (*Table)(nil)
var _ sql.InsertableTable = (*Table)(nil)
var _ sql.UpdatableTable = (*Table)(nil)
var _ sql.DeletableTable = (*Table)(nil)
var _ sql.ReplaceableTable = (*Table)(nil)
var _ sql.FilteredTable = (*Table)(nil)
var _ sql.ProjectedTable = (*Table)(nil)
var _ sql.DriverIndexableTable = (*Table)(nil)
var _ sql.AlterableTable = (*Table)(nil)
var _ sql.IndexAlterableTable = (*Table)(nil)
var _ sql.IndexedTable = (*Table)(nil)
var _ sql.ForeignKeyAlterableTable = (*Table)(nil)
var _ sql.ForeignKeyTable = (*Table)(nil)

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

	return &Table{
		name:       name,
		schema:     schema,
		partitions: partitions,
		keys:       keys,
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
		return nil, fmt.Errorf(
			"partition not found: %q", partition.Key(),
		)
	}

	var values sql.IndexValueIter
	if t.lookup != nil {
		var err error
		values, err = t.lookup.(sql.DriverIndexLookup).Values(partition)
		if err != nil {
			return nil, err
		}
	}

	return &tableIter{
		rows:        rows,
		columns:     t.columns,
		filters:     t.filters,
		indexValues: values,
	}, nil
}

type partition struct {
	key []byte
}

func (p *partition) Key() []byte { return p.key }

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
	return &partition{key}, nil
}

func (p *partitionIter) Close() error { return nil }

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

	return projectOnRow(i.columns, row), nil
}

func (i *tableIter) Close() error {
	if i.indexValues == nil {
		return nil
	}

	return i.indexValues.Close()
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

	value, err := decodeIndexValue(data)
	if err != nil {
		return nil, err
	}

	return i.rows[value.Pos], nil
}

type indexValue struct {
	Key string
	Pos int
}

func decodeIndexValue(data []byte) (*indexValue, error) {
	dec := gob.NewDecoder(bytes.NewReader(data))
	var value indexValue
	if err := dec.Decode(&value); err != nil {
		return nil, err
	}

	return &value, nil
}

func encodeIndexValue(value *indexValue) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(value); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

type tableEditor struct {
	table *Table
}

var _ sql.RowReplacer = (*tableEditor)(nil)
var _ sql.RowUpdater = (*tableEditor)(nil)
var _ sql.RowInserter = (*tableEditor)(nil)
var _ sql.RowDeleter = (*tableEditor)(nil)

func (t tableEditor) Close(*sql.Context) error {
	// TODO: it would be nice to apply all pending updates here at once, rather than directly in the Insert / Update
	//  / Delete methods.
	return nil
}

func (t *Table) Inserter(*sql.Context) sql.RowInserter {
	return &tableEditor{t}
}

func (t *Table) Updater(*sql.Context) sql.RowUpdater {
	return &tableEditor{t}
}

func (t *Table) Replacer(*sql.Context) sql.RowReplacer {
	return &tableEditor{t}
}

func (t *Table) Deleter(*sql.Context) sql.RowDeleter {
	return &tableEditor{t}
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
	return nil
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
			for rIndex, val := range row {
				if val != partitionRow[rIndex] {
					matches = false
					break
				}
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
			matches = true
			for rIndex, val := range oldRow {
				if val != partitionRow[rIndex] {
					matches = false
					break
				}
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

func (t *tableEditor) checkUniquenessConstraints(row sql.Row) error {
	pkColIdxes := t.pkColumnIndexes()

	if len(pkColIdxes) > 0 {
		for _, partition := range t.table.partitions {
			for _, partitionRow := range partition {
				if columnsMatch(pkColIdxes, partitionRow, row) {
					return sql.ErrUniqueKeyViolation.New(pkColIdxes)
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

func (t *Table) AddColumn(ctx *sql.Context, column *sql.Column, order *sql.ColumnOrder) error {
	column.Source = t.Name()
	newSch := make(sql.Schema, len(t.schema)+1)

	newColIdx := 0
	var i int
	if order != nil && order.First {
		newSch[i] = column
		i++
	}

	for _, col := range t.schema {
		newSch[i] = col
		i++
		if (order != nil && order.AfterColumn == col.Name) || (order == nil && i == len(t.schema)) {
			newSch[i] = column
			newColIdx = i
			i++
		}
	}

	t.schema = newSch
	return t.insertValueInRows(ctx, newColIdx, column.Default)
}

func (t *Table) insertValueInRows(ctx *sql.Context, idx int, colDefault *sql.ColumnDefaultValue) error {
	for k, p := range t.partitions {
		newP := make([]sql.Row, len(p))
		for i, row := range p {
			var newRow sql.Row
			newRow = append(newRow, row[:idx]...)
			val, err := colDefault.Eval(ctx, newRow)
			if err != nil {
				return err
			}
			newRow = append(newRow, val)
			if idx < len(row) {
				newRow = append(newRow, row[idx:]...)
			}
			newP[i] = newRow
		}
		t.partitions[k] = newP
	}
	return nil
}

func (t *Table) DropColumn(ctx *sql.Context, columnName string) error {
	newSch := make(sql.Schema, len(t.schema)-1)
	var i int
	for _, col := range t.schema {
		if col.Name != columnName {
			newSch[i] = col
			i++
		}
	}
	t.schema = newSch
	return nil
}

func (t *Table) ModifyColumn(ctx *sql.Context, columnName string, column *sql.Column, order *sql.ColumnOrder) error {
	if order == nil {
		colIdx := -1
		for i, col := range t.schema {
			if col.Name == columnName {
				colIdx = i
				break
			}
		}
		if colIdx <= 0 {
			order = &sql.ColumnOrder{
				First: true,
			}
		} else {
			order = &sql.ColumnOrder{
				AfterColumn: t.schema[colIdx-1].Name,
			}
		}
	}
	if err := t.DropColumn(ctx, columnName); err != nil {
		return err
	}
	return t.AddColumn(ctx, column, order)
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

// String implements the sql.Table inteface.
func (t *Table) String() string {
	p := sql.NewTreePrinter()

	kind := ""
	if len(t.columns) > 0 {
		kind += "Projected "
	}

	if len(t.filters) > 0 {
		kind += "Filtered "
	}

	if t.lookup != nil {
		kind += "Indexed"
	}

	if kind != "" {
		kind = ": " + kind
	}

	_ = p.WriteNode("Table(%s)%s", t.name, kind)
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

// WithFilters implements the sql.FilteredTable interface.
func (t *Table) WithFilters(filters []sql.Expression) sql.Table {
	if len(filters) == 0 {
		return t
	}

	nt := *t
	nt.filters = filters
	return &nt
}

// WithProjection implements the sql.ProjectedTable interface.
func (t *Table) WithProjection(colNames []string) sql.Table {
	if len(colNames) == 0 {
		return t
	}

	nt := *t
	columns, schema, err := nt.newColumnIndexesAndSchema(colNames)
	if err != nil {
		panic(err)
	}

	nt.columns = columns
	nt.projection = colNames
	nt.schema = schema

	return &nt
}

func (t *Table) newColumnIndexesAndSchema(colNames []string) ([]int, sql.Schema, error) {
	var columns []int
	var schema []*sql.Column

	for _, name := range colNames {
		i := t.schema.IndexOf(name, t.name)
		if i == -1 {
			return nil, nil, errColumnNotFound.New(name)
		}

		if len(t.columns) == 0 {
			// if the table hasn't been projected before
			// match against the original schema
			columns = append(columns, i)
		} else {
			// get indexes for the new projections from
			// the original indexes.
			columns = append(columns, t.columns[i])
		}

		schema = append(schema, t.schema[i])
	}

	return columns, schema, nil
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
	for i, key := range t.foreignKeys {
		if key.Name == fkName {
			t.foreignKeys = append(t.foreignKeys[:i], t.foreignKeys[i+1:]...)
			return nil
		}
	}
	return nil
}

func (t *Table) createIndex(name string, columns []sql.IndexColumn, constraint sql.IndexConstraint) (sql.Index, error) {
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
		},
	}, nil
}

// getField returns the index and column index with the name given, if it exists, or -1, nil otherwise.
func (t *Table) getField(col string) (int, *sql.Column) {
	i := t.schema.IndexOf(col, t.name)
	if i == -1 {
		return -1, nil
	}

	if len(t.columns) == 0 {
		// if the table hasn't been projected before
		// match against the original schema
		return i, t.schema[i]
	} else {
		return t.columns[i], t.schema[i]
	}
}

// CreateIndex implements sql.IndexAlterableTable
func (t *Table) CreateIndex(ctx *sql.Context, indexName string, using sql.IndexUsing, constraint sql.IndexConstraint, columns []sql.IndexColumn, comment string) error {
	if t.indexes == nil {
		t.indexes = make(map[string]sql.Index)
	}

	index, err := t.createIndex(indexName, columns, constraint)
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

	columns, _, err := t.newColumnIndexesAndSchema(colNames)
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

// Projection implements the sql.ProjectedTable interface.
func (t *Table) Projection() []string {
	return t.projection
}

// Filters implements the sql.FilteredTable interface.
func (t *Table) Filters() []sql.Expression {
	return t.filters
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

func (i *partitionIndexKeyValueIter) Close() error {
	return i.iter.Close()
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

	value := &indexValue{Key: i.key, Pos: i.pos}
	data, err := encodeIndexValue(value)
	if err != nil {
		return nil, nil, err
	}

	i.pos++
	return projectOnRow(i.columns, row), data, nil
}

func (i *indexKeyValueIter) Close() error {
	return i.iter.Close()
}
