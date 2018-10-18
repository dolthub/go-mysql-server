package sql // import "gopkg.in/src-d/go-mysql-server.v0/sql"

import (
	"fmt"
	"io"
)

const (
	// InformationSchemaDBName is the name of the information schema table.
	InformationSchemaDBName = "information_schema"
	// FilesTableName is the name of the files table.
	FilesTableName = "files"
	// ColumnStatisticsTableName is the name of the column statistics table.
	ColumnStatisticsTableName = "column_statistics"
)

type informationSchemaDB struct {
	name   string
	tables map[string]Table
}

var _ Database = (*informationSchemaDB)(nil)

// NewInformationSchemaDB creates a new INFORMATION_SCHEMA Database.
func NewInformationSchemaDB() Database {
	return &informationSchemaDB{
		name: InformationSchemaDBName,
		tables: map[string]Table{
			FilesTableName:            newFilesTable(),
			ColumnStatisticsTableName: newColumnStatisticsTable(),
		},
	}
}

// Name implements the Database interface.
func (i *informationSchemaDB) Name() string { return i.name }

// Tables implements the Database interface.
func (i *informationSchemaDB) Tables() map[string]Table { return i.tables }

func newFilesTable() Table {
	return &emptyTable{
		name:   FilesTableName,
		schema: filesSchema,
	}
}

var filesSchema = Schema{
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

func newColumnStatisticsTable() Table {
	return &emptyTable{
		name:   ColumnStatisticsTableName,
		schema: columnStatisticsSchema,
	}
}

var columnStatisticsSchema = Schema{
	&Column{Name: "schema_name", Type: Text, Source: ColumnStatisticsTableName},
	&Column{Name: "table_name", Type: Text, Source: ColumnStatisticsTableName},
	&Column{Name: "column_name", Type: Text, Source: ColumnStatisticsTableName},
	&Column{Name: "histogram", Type: JSON, Source: ColumnStatisticsTableName},
}

type emptyTable struct {
	name   string
	schema Schema
}

var _ Table = (*emptyTable)(nil)

// Name implements the Table interface.
func (t *emptyTable) Name() string { return t.name }

// String implements the Table interface.
func (t *emptyTable) String() string { return printTable(t.name, t.schema) }

// Schema implements the Table interface.
func (t *emptyTable) Schema() Schema { return t.schema }

// Partitions implements the Table interface.
func (t *emptyTable) Partitions(_ *Context) (PartitionIter, error) {
	return new(partitionIter), nil
}

// PartitionRows implements the Table interface.
func (t *emptyTable) PartitionRows(_ *Context, _ Partition) (RowIter, error) {
	return RowsToRowIter(), nil
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

type partitionIter struct{}

var _ PartitionIter = (*partitionIter)(nil)

func (p *partitionIter) Next() (Partition, error) { return nil, io.EOF }

func (p *partitionIter) Close() error { return nil }
