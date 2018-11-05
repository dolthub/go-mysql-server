package sql // import "gopkg.in/src-d/go-mysql-server.v0/sql"

import (
	"bytes"
	"fmt"
	"io"
)

const (
	// InformationSchemaDatabaseName is the name of the information schema database.
	InformationSchemaDatabaseName = "information_schema"
	// FilesTableName is the name of the files table.
	FilesTableName = "files"
	// ColumnStatisticsTableName is the name of the column statistics table.
	ColumnStatisticsTableName = "column_statistics"
	// TablesTableName is the name of tables table.
	TablesTableName = "tables"
	// ColumnsTableName is the name of columns table.
	ColumnsTableName = "columns"
	// SchemataTableName is the name of the schemata table.
	SchemataTableName = "schemata"
)

type informationSchemaDatabase struct {
	name   string
	tables map[string]Table
}

type informationSchemaTable struct {
	name    string
	schema  Schema
	catalog *Catalog
	rowIter func(*Catalog) RowIter
}

type informationSchemaPartition struct {
	key []byte
}

type informationSchemaPartitionIter struct {
	informationSchemaPartition
	pos int
}

var (
	_ Database      = (*informationSchemaDatabase)(nil)
	_ Table         = (*informationSchemaTable)(nil)
	_ Partition     = (*informationSchemaPartition)(nil)
	_ PartitionIter = (*informationSchemaPartitionIter)(nil)
)

var filesSchema = Schema{
	{Name: "file_id", Type: Int64, Source: FilesTableName, Nullable: true},
	{Name: "file_name", Type: Text, Source: FilesTableName, Nullable: true},
	{Name: "file_type", Type: Text, Source: FilesTableName, Nullable: true},
	{Name: "tablespace_name", Type: Text, Source: FilesTableName},
	{Name: "table_catalog", Type: Text, Source: FilesTableName},
	{Name: "table_schema", Type: Blob, Source: FilesTableName, Nullable: true},
	{Name: "table_name", Type: Blob, Source: FilesTableName, Nullable: true},
	{Name: "logfile_group_name", Type: Text, Source: FilesTableName, Nullable: true},
	{Name: "logfile_group_number", Type: Int64, Source: FilesTableName, Nullable: true},
	{Name: "engine", Type: Text, Source: FilesTableName},
	{Name: "fulltext_keys", Type: Blob, Source: FilesTableName, Nullable: true},
	{Name: "deleted_rows", Type: Blob, Source: FilesTableName, Nullable: true},
	{Name: "update_count", Type: Blob, Source: FilesTableName, Nullable: true},
	{Name: "free_extents", Type: Int64, Source: FilesTableName, Nullable: true},
	{Name: "total_extents", Type: Int64, Source: FilesTableName, Nullable: true},
	{Name: "extent_size", Type: Int64, Source: FilesTableName, Nullable: true},
	{Name: "initial_size", Type: Int64, Source: FilesTableName, Nullable: true},
	{Name: "maximum_size", Type: Int64, Source: FilesTableName, Nullable: true},
	{Name: "autoextend_size", Type: Int64, Source: FilesTableName, Nullable: true},
	{Name: "creation_time", Type: Blob, Source: FilesTableName, Nullable: true},
	{Name: "last_update_time", Type: Blob, Source: FilesTableName, Nullable: true},
	{Name: "last_access_time", Type: Blob, Source: FilesTableName, Nullable: true},
	{Name: "recover_time", Type: Blob, Source: FilesTableName, Nullable: true},
	{Name: "transaction_counter", Type: Blob, Source: FilesTableName, Nullable: true},
	{Name: "version", Type: Int64, Source: FilesTableName, Nullable: true},
	{Name: "row_format", Type: Text, Source: FilesTableName, Nullable: true},
	{Name: "table_rows", Type: Blob, Source: FilesTableName, Nullable: true},
	{Name: "avg_row_length", Type: Blob, Source: FilesTableName, Nullable: true},
	{Name: "data_length", Type: Blob, Source: FilesTableName, Nullable: true},
	{Name: "max_data_length", Type: Blob, Source: FilesTableName, Nullable: true},
	{Name: "index_length", Type: Blob, Source: FilesTableName, Nullable: true},
	{Name: "data_free", Type: Int64, Source: FilesTableName, Nullable: true},
	{Name: "create_time", Type: Blob, Source: FilesTableName, Nullable: true},
	{Name: "update_time", Type: Blob, Source: FilesTableName, Nullable: true},
	{Name: "check_time", Type: Blob, Source: FilesTableName, Nullable: true},
	{Name: "checksum", Type: Blob, Source: FilesTableName, Nullable: true},
	{Name: "status", Type: Text, Source: FilesTableName, Nullable: true},
	{Name: "extra", Type: Blob, Source: FilesTableName, Nullable: true},
}

var columnStatisticsSchema = Schema{
	{Name: "schema_name", Type: Text, Source: ColumnStatisticsTableName},
	{Name: "table_name", Type: Text, Source: ColumnStatisticsTableName},
	{Name: "column_name", Type: Text, Source: ColumnStatisticsTableName},
	{Name: "histogram", Type: JSON, Source: ColumnStatisticsTableName},
}

var tablesSchema = Schema{
	{Name: "table_catalog", Type: Text, Default: "", Nullable: false, Source: TablesTableName},
	{Name: "table_schema", Type: Text, Default: "", Nullable: false, Source: TablesTableName},
	{Name: "table_name", Type: Text, Default: "", Nullable: false, Source: TablesTableName},
	{Name: "table_type", Type: Text, Default: "", Nullable: false, Source: TablesTableName},
	{Name: "engine", Type: Text, Default: nil, Nullable: true, Source: TablesTableName},
	{Name: "version", Type: Uint64, Default: nil, Nullable: true, Source: TablesTableName},
	{Name: "row_format", Type: Text, Default: nil, Nullable: true, Source: TablesTableName},
	{Name: "table_rows", Type: Uint64, Default: nil, Nullable: true, Source: TablesTableName},
	{Name: "avg_row_length", Type: Uint64, Default: nil, Nullable: true, Source: TablesTableName},
	{Name: "data_length", Type: Uint64, Default: nil, Nullable: true, Source: TablesTableName},
	{Name: "max_data_length", Type: Uint64, Default: nil, Nullable: true, Source: TablesTableName},
	{Name: "index_length", Type: Uint64, Default: nil, Nullable: true, Source: TablesTableName},
	{Name: "data_free", Type: Uint64, Default: nil, Nullable: true, Source: TablesTableName},
	{Name: "auto_increment", Type: Uint64, Default: nil, Nullable: true, Source: TablesTableName},
	{Name: "create_time", Type: Date, Default: nil, Nullable: true, Source: TablesTableName},
	{Name: "update_time", Type: Date, Default: nil, Nullable: true, Source: TablesTableName},
	{Name: "check_time", Type: Date, Default: nil, Nullable: true, Source: TablesTableName},
	{Name: "table_collation", Type: Text, Default: nil, Nullable: true, Source: TablesTableName},
	{Name: "checksum", Type: Uint64, Default: nil, Nullable: true, Source: TablesTableName},
	{Name: "create_options", Type: Text, Default: nil, Nullable: true, Source: TablesTableName},
	{Name: "table_comment", Type: Text, Default: "", Nullable: false, Source: TablesTableName},
}

var columnsSchema = Schema{
	{Name: "table_catalog", Type: Text, Default: "", Nullable: false, Source: ColumnsTableName},
	{Name: "table_schema", Type: Text, Default: "", Nullable: false, Source: ColumnsTableName},
	{Name: "table_name", Type: Text, Default: "", Nullable: false, Source: ColumnsTableName},
	{Name: "column_name", Type: Text, Default: "", Nullable: false, Source: ColumnsTableName},
	{Name: "ordinal_position", Type: Uint64, Default: 0, Nullable: false, Source: ColumnsTableName},
	{Name: "column_default", Type: Text, Default: nil, Nullable: true, Source: ColumnsTableName},
	{Name: "is_nullable", Type: Text, Default: "", Nullable: false, Source: ColumnsTableName},
	{Name: "data_type", Type: Text, Default: "", Nullable: false, Source: ColumnsTableName},
	{Name: "character_maximum_length", Type: Uint64, Default: nil, Nullable: true, Source: ColumnsTableName},
	{Name: "character_octet_length", Type: Uint64, Default: nil, Nullable: true, Source: ColumnsTableName},
	{Name: "numeric_precision", Type: Uint64, Default: nil, Nullable: true, Source: ColumnsTableName},
	{Name: "numeric_scale", Type: Uint64, Default: nil, Nullable: true, Source: ColumnsTableName},
	{Name: "datetime_precision", Type: Uint64, Default: nil, Nullable: true, Source: ColumnsTableName},
	{Name: "character_set_name", Type: Text, Default: nil, Nullable: true, Source: ColumnsTableName},
	{Name: "collation_name", Type: Text, Default: nil, Nullable: true, Source: ColumnsTableName},
	{Name: "column_type", Type: Text, Default: "", Nullable: false, Source: ColumnsTableName},
	{Name: "column_key", Type: Text, Default: "", Nullable: false, Source: ColumnsTableName},
	{Name: "extra", Type: Text, Default: "", Nullable: false, Source: ColumnsTableName},
	{Name: "privileges", Type: Text, Default: "", Nullable: false, Source: ColumnsTableName},
	{Name: "column_comment", Type: Text, Default: "", Nullable: false, Source: ColumnsTableName},
	{Name: "generation_expression", Type: Text, Default: "", Nullable: false, Source: ColumnsTableName},
}

var schemataSchema = Schema{
	{Name: "catalog_name", Type: Text, Default: nil, Nullable: false, Source: SchemataTableName},
	{Name: "schema_name", Type: Text, Default: nil, Nullable: false, Source: SchemataTableName},
	{Name: "default_character_set_name", Type: Text, Default: nil, Nullable: false, Source: SchemataTableName},
	{Name: "default_collation_name", Type: Text, Default: nil, Nullable: false, Source: SchemataTableName},
	{Name: "sql_path", Type: Text, Default: nil, Nullable: true, Source: SchemataTableName},
}

func tablesRowIter(cat *Catalog) RowIter {
	var rows []Row
	for _, db := range cat.AllDatabases() {
		tableType := "BASE TABLE"
		engine := "INNODB"
		rowFormat := "Dynamic"
		if db.Name() == InformationSchemaDatabaseName {
			tableType = "SYSTEM VIEW"
			engine = "MEMORY"
			rowFormat = "Fixed"
		}
		for _, t := range db.Tables() {
			rows = append(rows, Row{
				"def",      //table_catalog
				db.Name(),  // table_schema
				t.Name(),   // table_name
				tableType,  // table_type
				engine,     // engine
				10,         //version (protocol, always 10)
				rowFormat,  //row_format
				nil,        //table_rows
				nil,        //avg_row_length
				nil,        //data_length
				nil,        //max_data_length
				nil,        //max_data_length
				nil,        //data_free
				nil,        //auto_increment
				nil,        //create_time
				nil,        //update_time
				nil,        //check_time
				"utf8_bin", //table_collation
				nil,        //checksum
				nil,        //create_options
				"",         //table_comment
			})
		}
	}

	return RowsToRowIter(rows...)
}

func columnsRowIter(cat *Catalog) RowIter {
	var rows []Row
	for _, db := range cat.AllDatabases() {
		for _, t := range db.Tables() {
			for i, c := range t.Schema() {
				var (
					nullable string
					charName interface{}
					collName interface{}
				)
				if c.Nullable {
					nullable = "YES"
				} else {
					nullable = "NO"
				}
				if IsText(c.Type) {
					charName = "utf8mb4"
					collName = "utf8_bin"
				}
				rows = append(rows, Row{
					"def",           // table_catalog
					db.Name(),       // table_schema
					t.Name(),        // table_name
					c.Name,          // column_name
					uint64(i),       // ordinal_position
					c.Default,       // column_default
					nullable,        // is_nullable
					c.Type.String(), // data_type
					nil,             // character_maximum_length
					nil,             // character_octet_length
					nil,             // numeric_precision
					nil,             // numeric_scale
					nil,             // datetime_precision
					charName,        // character_set_name
					collName,        // collation_name
					c.Type.String(), // column_type
					"",              // column_key
					"",              // extra
					"select",        // privileges
					"",              // column_comment
					"",              // generation_expression
				})
			}
		}
	}
	return RowsToRowIter(rows...)
}

func schemataRowIter(c *Catalog) RowIter {
	dbs := c.AllDatabases()

	var rows []Row
	for _, db := range dbs {
		if db.Name() == InformationSchemaDatabaseName {
			continue
		}

		rows = append(rows, Row{
			"def",
			db.Name(),
			"utf8mb4",
			"utf8_bin",
			nil,
		})
	}

	return RowsToRowIter(rows...)
}

// NewInformationSchemaDatabase creates a new INFORMATION_SCHEMA Database.
func NewInformationSchemaDatabase(cat *Catalog) Database {
	return &informationSchemaDatabase{
		name: InformationSchemaDatabaseName,
		tables: map[string]Table{
			FilesTableName: &informationSchemaTable{
				name:    FilesTableName,
				schema:  filesSchema,
				catalog: cat,
			},
			ColumnStatisticsTableName: &informationSchemaTable{
				name:    ColumnStatisticsTableName,
				schema:  columnStatisticsSchema,
				catalog: cat,
			},
			TablesTableName: &informationSchemaTable{
				name:    TablesTableName,
				schema:  tablesSchema,
				catalog: cat,
				rowIter: tablesRowIter,
			},
			ColumnsTableName: &informationSchemaTable{
				name:    ColumnsTableName,
				schema:  columnsSchema,
				catalog: cat,
				rowIter: columnsRowIter,
			},
			SchemataTableName: &informationSchemaTable{
				name:    SchemataTableName,
				schema:  schemataSchema,
				catalog: cat,
				rowIter: schemataRowIter,
			},
		},
	}
}

// Name implements the sql.Database interface.
func (db *informationSchemaDatabase) Name() string { return db.name }

// Tables implements the sql.Database interface.
func (db *informationSchemaDatabase) Tables() map[string]Table { return db.tables }

// Name implements the sql.Table interface.
func (t *informationSchemaTable) Name() string {
	return t.name
}

// Schema implements the sql.Table interface.
func (t *informationSchemaTable) Schema() Schema {
	return t.schema
}

// Partitions implements the sql.Table interface.
func (t *informationSchemaTable) Partitions(ctx *Context) (PartitionIter, error) {
	return &informationSchemaPartitionIter{informationSchemaPartition: informationSchemaPartition{partitionKey(t.Name())}}, nil
}

// PartitionRows implements the sql.PartitionRows interface.
func (t *informationSchemaTable) PartitionRows(ctx *Context, partition Partition) (RowIter, error) {
	if !bytes.Equal(partition.Key(), partitionKey(t.Name())) {
		return nil, fmt.Errorf(
			"partition not found: %q", partition.Key(),
		)
	}
	if t.rowIter == nil {
		return RowsToRowIter(), nil
	}

	return t.rowIter(t.catalog), nil
}

// PartitionCount implements the sql.PartitionCounter interface.
func (t *informationSchemaTable) String() string {
	return printTable(t.Name(), t.Schema())
}

// Key implements Partition  interface
func (p *informationSchemaPartition) Key() []byte { return p.key }

// Next implements single PartitionIter interface
func (pit *informationSchemaPartitionIter) Next() (Partition, error) {
	if pit.pos == 0 {
		pit.pos++
		return pit, nil
	}
	return nil, io.EOF
}

// Close implements single PartitionIter interface
func (pit *informationSchemaPartitionIter) Close() error {
	pit.pos = 0
	return nil
}

func printTable(name string, tableSchema Schema) string {
	p := NewTreePrinter()
	_ = p.WriteNode("Table(%s)", name)
	var schema = make([]string, len(tableSchema))
	for i, col := range tableSchema {
		schema[i] = fmt.Sprintf(
			"Column(%s, %s, nullable=%v)",
			col.Name,
			col.Type.Type().String(),
			col.Nullable,
		)
	}
	_ = p.WriteChildren(schema...)
	return p.String()
}

func partitionKey(tableName string) []byte {
	return []byte(InformationSchemaDatabaseName + "." + tableName)
}
