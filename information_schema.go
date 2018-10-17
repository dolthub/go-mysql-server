package sqle // import "gopkg.in/src-d/go-mysql-server.v0"

import (
	"fmt"
	"io"

	"gopkg.in/src-d/go-mysql-server.v0/sql"
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
	tables map[string]sql.Table
}

var _ sql.Database = (*informationSchemaDB)(nil)

// NewInformationSchemaDB creates a new INFORMATION_SCHEMA Database.
func NewInformationSchemaDB() sql.Database {
	return &informationSchemaDB{
		name: InformationSchemaDBName,
		tables: map[string]sql.Table{
			FilesTableName:            newFilesTable(),
			ColumnStatisticsTableName: newColumnStatisticsTable(),
		},
	}
}

// Name implements the sql.Database interface.
func (i *informationSchemaDB) Name() string { return i.name }

// Tables implements the sql.Database interface.
func (i *informationSchemaDB) Tables() map[string]sql.Table { return i.tables }

func newFilesTable() sql.Table {
	return &emptyTable{
		name:   FilesTableName,
		schema: filesSchema,
	}
}

var filesSchema = sql.Schema{
	&sql.Column{Name: "file_id", Type: sql.Int64, Source: FilesTableName, Nullable: true},
	&sql.Column{Name: "file_name", Type: sql.Text, Source: FilesTableName, Nullable: true},
	&sql.Column{Name: "file_type", Type: sql.Text, Source: FilesTableName, Nullable: true},
	&sql.Column{Name: "tablespace_name", Type: sql.Text, Source: FilesTableName},
	&sql.Column{Name: "table_catalog", Type: sql.Text, Source: FilesTableName},
	&sql.Column{Name: "table_schema", Type: sql.Blob, Source: FilesTableName, Nullable: true},
	&sql.Column{Name: "table_name", Type: sql.Blob, Source: FilesTableName, Nullable: true},
	&sql.Column{Name: "logfile_group_name", Type: sql.Text, Source: FilesTableName, Nullable: true},
	&sql.Column{Name: "logfile_group_number", Type: sql.Int64, Source: FilesTableName, Nullable: true},
	&sql.Column{Name: "engine", Type: sql.Text, Source: FilesTableName},
	&sql.Column{Name: "fulltext_keys", Type: sql.Blob, Source: FilesTableName, Nullable: true},
	&sql.Column{Name: "deleted_rows", Type: sql.Blob, Source: FilesTableName, Nullable: true},
	&sql.Column{Name: "update_count", Type: sql.Blob, Source: FilesTableName, Nullable: true},
	&sql.Column{Name: "free_extents", Type: sql.Int64, Source: FilesTableName, Nullable: true},
	&sql.Column{Name: "total_extents", Type: sql.Int64, Source: FilesTableName, Nullable: true},
	&sql.Column{Name: "extent_size", Type: sql.Int64, Source: FilesTableName, Nullable: true},
	&sql.Column{Name: "initial_size", Type: sql.Int64, Source: FilesTableName, Nullable: true},
	&sql.Column{Name: "maximum_size", Type: sql.Int64, Source: FilesTableName, Nullable: true},
	&sql.Column{Name: "autoextend_size", Type: sql.Int64, Source: FilesTableName, Nullable: true},
	&sql.Column{Name: "creation_time", Type: sql.Blob, Source: FilesTableName, Nullable: true},
	&sql.Column{Name: "last_update_time", Type: sql.Blob, Source: FilesTableName, Nullable: true},
	&sql.Column{Name: "last_access_time", Type: sql.Blob, Source: FilesTableName, Nullable: true},
	&sql.Column{Name: "recover_time", Type: sql.Blob, Source: FilesTableName, Nullable: true},
	&sql.Column{Name: "transaction_counter", Type: sql.Blob, Source: FilesTableName, Nullable: true},
	&sql.Column{Name: "version", Type: sql.Int64, Source: FilesTableName, Nullable: true},
	&sql.Column{Name: "row_format", Type: sql.Text, Source: FilesTableName, Nullable: true},
	&sql.Column{Name: "table_rows", Type: sql.Blob, Source: FilesTableName, Nullable: true},
	&sql.Column{Name: "avg_row_length", Type: sql.Blob, Source: FilesTableName, Nullable: true},
	&sql.Column{Name: "data_length", Type: sql.Blob, Source: FilesTableName, Nullable: true},
	&sql.Column{Name: "max_data_length", Type: sql.Blob, Source: FilesTableName, Nullable: true},
	&sql.Column{Name: "index_length", Type: sql.Blob, Source: FilesTableName, Nullable: true},
	&sql.Column{Name: "data_free", Type: sql.Int64, Source: FilesTableName, Nullable: true},
	&sql.Column{Name: "create_time", Type: sql.Blob, Source: FilesTableName, Nullable: true},
	&sql.Column{Name: "update_time", Type: sql.Blob, Source: FilesTableName, Nullable: true},
	&sql.Column{Name: "check_time", Type: sql.Blob, Source: FilesTableName, Nullable: true},
	&sql.Column{Name: "checksum", Type: sql.Blob, Source: FilesTableName, Nullable: true},
	&sql.Column{Name: "status", Type: sql.Text, Source: FilesTableName, Nullable: true},
	&sql.Column{Name: "extra", Type: sql.Blob, Source: FilesTableName, Nullable: true},
}

func newColumnStatisticsTable() sql.Table {
	return &emptyTable{
		name:   ColumnStatisticsTableName,
		schema: columnStatisticsSchema,
	}
}

var columnStatisticsSchema = sql.Schema{
	&sql.Column{Name: "schema_name", Type: sql.Text, Source: ColumnStatisticsTableName},
	&sql.Column{Name: "table_name", Type: sql.Text, Source: ColumnStatisticsTableName},
	&sql.Column{Name: "column_name", Type: sql.Text, Source: ColumnStatisticsTableName},
	&sql.Column{Name: "histogram", Type: sql.JSON, Source: ColumnStatisticsTableName},
}

type emptyTable struct {
	name   string
	schema sql.Schema
}

var _ sql.Table = (*emptyTable)(nil)

// Name implements the sql.Table interface.
func (t *emptyTable) Name() string { return t.name }

// String implements the sql.Table interface.
func (t *emptyTable) String() string { return printTable(t.name, t.schema) }

// Schema implements the sql.Table interface.
func (t *emptyTable) Schema() sql.Schema { return t.schema }

// Partitions implements the sql.Table interface.
func (t *emptyTable) Partitions(_ *sql.Context) (sql.PartitionIter, error) {
	return new(partitionIter), nil
}

// PartitionRows implements the sql.Table interface.
func (t *emptyTable) PartitionRows(_ *sql.Context, _ sql.Partition) (sql.RowIter, error) {
	return sql.RowsToRowIter(), nil
}

func printTable(name string, tableSchema sql.Schema) string {
	p := sql.NewTreePrinter()
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

var _ sql.PartitionIter = (*partitionIter)(nil)

func (p *partitionIter) Next() (sql.Partition, error) { return nil, io.EOF }

func (p *partitionIter) Close() error { return nil }
