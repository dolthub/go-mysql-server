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
)

type (
	informationSchemaDatabase struct {
		name   string
		tables map[string]Table
	}

	informationSchemaTable struct {
		name    string
		schema  Schema
		catalog *Catalog
		rowIter func(*Catalog) RowIter
	}

	informationSchemaPartition struct {
		key []byte
	}

	informationSchemaPartitionIter struct {
		informationSchemaPartition
		pos int
	}
)

var (
	_ Database      = (*informationSchemaDatabase)(nil)
	_ Table         = (*informationSchemaTable)(nil)
	_ Partition     = (*informationSchemaPartition)(nil)
	_ PartitionIter = (*informationSchemaPartitionIter)(nil)

	filesSchema = Schema{
		&Column{Name: "file_id", Type: Int64, Source: FilesTableName, Nullable: true},
		&Column{Name: "file_name", Type: Text, Source: FilesTableName, Nullable: true},
		&Column{Name: "file_type", Type: Text, Source: FilesTableName, Nullable: true},
		&Column{Name: "tablespace_name", Type: Text, Source: FilesTableName},
		&Column{Name: "table_catalog", Type: Text, Source: FilesTableName},
		&Column{Name: "table_schema", Type: Blob, Source: FilesTableName, Nullable: true},
		&Column{Name: "table_name", Type: Blob, Source: FilesTableName, Nullable: true},
		&Column{Name: "logfile_group_name", Type: Text, Source: FilesTableName, Nullable: true},
		&Column{Name: "logfile_group_number", Type: Int64, Source: FilesTableName, Nullable: true},
		&Column{Name: "engine", Type: Text, Source: FilesTableName},
		&Column{Name: "fulltext_keys", Type: Blob, Source: FilesTableName, Nullable: true},
		&Column{Name: "deleted_rows", Type: Blob, Source: FilesTableName, Nullable: true},
		&Column{Name: "update_count", Type: Blob, Source: FilesTableName, Nullable: true},
		&Column{Name: "free_extents", Type: Int64, Source: FilesTableName, Nullable: true},
		&Column{Name: "total_extents", Type: Int64, Source: FilesTableName, Nullable: true},
		&Column{Name: "extent_size", Type: Int64, Source: FilesTableName, Nullable: true},
		&Column{Name: "initial_size", Type: Int64, Source: FilesTableName, Nullable: true},
		&Column{Name: "maximum_size", Type: Int64, Source: FilesTableName, Nullable: true},
		&Column{Name: "autoextend_size", Type: Int64, Source: FilesTableName, Nullable: true},
		&Column{Name: "creation_time", Type: Blob, Source: FilesTableName, Nullable: true},
		&Column{Name: "last_update_time", Type: Blob, Source: FilesTableName, Nullable: true},
		&Column{Name: "last_access_time", Type: Blob, Source: FilesTableName, Nullable: true},
		&Column{Name: "recover_time", Type: Blob, Source: FilesTableName, Nullable: true},
		&Column{Name: "transaction_counter", Type: Blob, Source: FilesTableName, Nullable: true},
		&Column{Name: "version", Type: Int64, Source: FilesTableName, Nullable: true},
		&Column{Name: "row_format", Type: Text, Source: FilesTableName, Nullable: true},
		&Column{Name: "table_rows", Type: Blob, Source: FilesTableName, Nullable: true},
		&Column{Name: "avg_row_length", Type: Blob, Source: FilesTableName, Nullable: true},
		&Column{Name: "data_length", Type: Blob, Source: FilesTableName, Nullable: true},
		&Column{Name: "max_data_length", Type: Blob, Source: FilesTableName, Nullable: true},
		&Column{Name: "index_length", Type: Blob, Source: FilesTableName, Nullable: true},
		&Column{Name: "data_free", Type: Int64, Source: FilesTableName, Nullable: true},
		&Column{Name: "create_time", Type: Blob, Source: FilesTableName, Nullable: true},
		&Column{Name: "update_time", Type: Blob, Source: FilesTableName, Nullable: true},
		&Column{Name: "check_time", Type: Blob, Source: FilesTableName, Nullable: true},
		&Column{Name: "checksum", Type: Blob, Source: FilesTableName, Nullable: true},
		&Column{Name: "status", Type: Text, Source: FilesTableName, Nullable: true},
		&Column{Name: "extra", Type: Blob, Source: FilesTableName, Nullable: true},
	}

	columnStatisticsSchema = Schema{
		&Column{Name: "schema_name", Type: Text, Source: ColumnStatisticsTableName},
		&Column{Name: "table_name", Type: Text, Source: ColumnStatisticsTableName},
		&Column{Name: "column_name", Type: Text, Source: ColumnStatisticsTableName},
		&Column{Name: "histogram", Type: JSON, Source: ColumnStatisticsTableName},
	}

	tablesSchema = Schema{
		&Column{Name: "table_catalog", Type: Text, Default: "", Nullable: false, Source: TablesTableName},
		&Column{Name: "table_schema", Type: Text, Default: "", Nullable: false, Source: TablesTableName},
		&Column{Name: "table_name", Type: Text, Default: "", Nullable: false, Source: TablesTableName},
		&Column{Name: "table_type", Type: Text, Default: "", Nullable: false, Source: TablesTableName},
		&Column{Name: "engine", Type: Text, Default: nil, Nullable: true, Source: TablesTableName},
		&Column{Name: "version", Type: Uint64, Default: nil, Nullable: true, Source: TablesTableName},
		&Column{Name: "row_format", Type: Text, Default: nil, Nullable: true, Source: TablesTableName},
		&Column{Name: "table_rows", Type: Uint64, Default: nil, Nullable: true, Source: TablesTableName},
		&Column{Name: "avg_row_length", Type: Uint64, Default: nil, Nullable: true, Source: TablesTableName},
		&Column{Name: "data_length", Type: Uint64, Default: nil, Nullable: true, Source: TablesTableName},
		&Column{Name: "max_data_length", Type: Uint64, Default: nil, Nullable: true, Source: TablesTableName},
		&Column{Name: "index_length", Type: Uint64, Default: nil, Nullable: true, Source: TablesTableName},
		&Column{Name: "data_free", Type: Uint64, Default: nil, Nullable: true, Source: TablesTableName},
		&Column{Name: "auto_increment", Type: Uint64, Default: nil, Nullable: true, Source: TablesTableName},
		&Column{Name: "create_time", Type: Date, Default: nil, Nullable: true, Source: TablesTableName},
		&Column{Name: "update_time", Type: Date, Default: nil, Nullable: true, Source: TablesTableName},
		&Column{Name: "check_time", Type: Date, Default: nil, Nullable: true, Source: TablesTableName},
		&Column{Name: "table_collation", Type: Text, Default: nil, Nullable: true, Source: TablesTableName},
		&Column{Name: "checksum", Type: Uint64, Default: nil, Nullable: true, Source: TablesTableName},
		&Column{Name: "create_options", Type: Text, Default: nil, Nullable: true, Source: TablesTableName},
		&Column{Name: "table_comment", Type: Text, Default: "", Nullable: false, Source: TablesTableName},
	}

	columnsSchema = Schema{
		&Column{Name: "table_catalog", Type: Text, Default: "", Nullable: false, Source: ColumnsTableName},
		&Column{Name: "table_schema", Type: Text, Default: "", Nullable: false, Source: ColumnsTableName},
		&Column{Name: "table_name", Type: Text, Default: "", Nullable: false, Source: ColumnsTableName},
		&Column{Name: "column_name", Type: Text, Default: "", Nullable: false, Source: ColumnsTableName},
		&Column{Name: "ordinal_position", Type: Uint64, Default: 0, Nullable: false, Source: ColumnsTableName},
		&Column{Name: "column_default", Type: Text, Default: nil, Nullable: true, Source: ColumnsTableName},
		&Column{Name: "is_nullable", Type: Text, Default: "", Nullable: false, Source: ColumnsTableName},
		&Column{Name: "data_type", Type: Text, Default: "", Nullable: false, Source: ColumnsTableName},
		&Column{Name: "character_maximum_length", Type: Uint64, Default: nil, Nullable: true, Source: ColumnsTableName},
		&Column{Name: "character_octet_length", Type: Uint64, Default: nil, Nullable: true, Source: ColumnsTableName},
		&Column{Name: "numeric_precision", Type: Uint64, Default: nil, Nullable: true, Source: ColumnsTableName},
		&Column{Name: "numeric_scale", Type: Uint64, Default: nil, Nullable: true, Source: ColumnsTableName},
		&Column{Name: "datetime_precision", Type: Uint64, Default: nil, Nullable: true, Source: ColumnsTableName},
		&Column{Name: "character_set_name", Type: Text, Default: nil, Nullable: true, Source: ColumnsTableName},
		&Column{Name: "collation_name", Type: Text, Default: nil, Nullable: true, Source: ColumnsTableName},
		&Column{Name: "column_type", Type: Text, Default: "", Nullable: false, Source: ColumnsTableName},
		&Column{Name: "column_key", Type: Text, Default: "", Nullable: false, Source: ColumnsTableName},
		&Column{Name: "extra", Type: Text, Default: "", Nullable: false, Source: ColumnsTableName},
		&Column{Name: "privileges", Type: Text, Default: "", Nullable: false, Source: ColumnsTableName},
		&Column{Name: "column_comment", Type: Text, Default: "", Nullable: false, Source: ColumnsTableName},
		&Column{Name: "generation_expression", Type: Text, Default: "", Nullable: false, Source: ColumnsTableName},
	}

	tablesRowIter = func(cat *Catalog) RowIter {
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

	columnsRowIter = func(cat *Catalog) RowIter {
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
						charName = "utf-8"
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
)

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
