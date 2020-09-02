package information_schema

import (
	"bytes"
	"fmt"
	"io"
	"strings"

	. "github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/liquidata-inc/go-mysql-server/sql/parse"
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
	// CollationsTableName is the name of the collations table.
	CollationsTableName = "collations"
	// StatisticsTableName is the name of the statistics table.
	StatisticsTableName = "statistics"
	// TableConstraintsTableName is the name of the table_constraints table.
	TableConstraintsTableName = "table_constraints"
	// ReferentialConstraintsTableName is the name of the table_constraints table.
	ReferentialConstraintsTableName = "referential_constraints"
	// KeyColumnUsageTableName is the name of the key_column_usage table.
	KeyColumnUsageTableName = "key_column_usage"
	// TriggersTableName is the name of the triggers table.
	TriggersTableName = "triggers"
	// EventsTableName is the name of the events table.
	EventsTableName = "events"
	// RoutinesTableName is the name of the routines table.
	RoutinesTableName = "routines"
	// ViewsTableName is the name of the views table.
	ViewsTableName = "views"
	// UserPrivilegesTableName is the name of the user_privileges table
	UserPrivilegesTableName = "user_privileges"
)

var _ Database = (*informationSchemaDatabase)(nil)

type informationSchemaDatabase struct {
	name   string
	tables map[string]Table
}

type informationSchemaTable struct {
	name    string
	schema  Schema
	catalog *Catalog
	rowIter func(*Context, *Catalog) RowIter
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
	{Name: "file_name", Type: LongText, Source: FilesTableName, Nullable: true},
	{Name: "file_type", Type: LongText, Source: FilesTableName, Nullable: true},
	{Name: "tablespace_name", Type: LongText, Source: FilesTableName},
	{Name: "table_catalog", Type: LongText, Source: FilesTableName},
	{Name: "table_schema", Type: LongBlob, Source: FilesTableName, Nullable: true},
	{Name: "table_name", Type: LongBlob, Source: FilesTableName, Nullable: true},
	{Name: "logfile_group_name", Type: LongText, Source: FilesTableName, Nullable: true},
	{Name: "logfile_group_number", Type: Int64, Source: FilesTableName, Nullable: true},
	{Name: "engine", Type: LongText, Source: FilesTableName},
	{Name: "fulltext_keys", Type: LongBlob, Source: FilesTableName, Nullable: true},
	{Name: "deleted_rows", Type: LongBlob, Source: FilesTableName, Nullable: true},
	{Name: "update_count", Type: LongBlob, Source: FilesTableName, Nullable: true},
	{Name: "free_extents", Type: Int64, Source: FilesTableName, Nullable: true},
	{Name: "total_extents", Type: Int64, Source: FilesTableName, Nullable: true},
	{Name: "extent_size", Type: Int64, Source: FilesTableName, Nullable: true},
	{Name: "initial_size", Type: Int64, Source: FilesTableName, Nullable: true},
	{Name: "maximum_size", Type: Int64, Source: FilesTableName, Nullable: true},
	{Name: "autoextend_size", Type: Int64, Source: FilesTableName, Nullable: true},
	{Name: "creation_time", Type: LongBlob, Source: FilesTableName, Nullable: true},
	{Name: "last_update_time", Type: LongBlob, Source: FilesTableName, Nullable: true},
	{Name: "last_access_time", Type: LongBlob, Source: FilesTableName, Nullable: true},
	{Name: "recover_time", Type: LongBlob, Source: FilesTableName, Nullable: true},
	{Name: "transaction_counter", Type: LongBlob, Source: FilesTableName, Nullable: true},
	{Name: "version", Type: Int64, Source: FilesTableName, Nullable: true},
	{Name: "row_format", Type: LongText, Source: FilesTableName, Nullable: true},
	{Name: "table_rows", Type: LongBlob, Source: FilesTableName, Nullable: true},
	{Name: "avg_row_length", Type: LongBlob, Source: FilesTableName, Nullable: true},
	{Name: "data_length", Type: LongBlob, Source: FilesTableName, Nullable: true},
	{Name: "max_data_length", Type: LongBlob, Source: FilesTableName, Nullable: true},
	{Name: "index_length", Type: LongBlob, Source: FilesTableName, Nullable: true},
	{Name: "data_free", Type: Int64, Source: FilesTableName, Nullable: true},
	{Name: "create_time", Type: LongBlob, Source: FilesTableName, Nullable: true},
	{Name: "update_time", Type: LongBlob, Source: FilesTableName, Nullable: true},
	{Name: "check_time", Type: LongBlob, Source: FilesTableName, Nullable: true},
	{Name: "checksum", Type: LongBlob, Source: FilesTableName, Nullable: true},
	{Name: "status", Type: LongText, Source: FilesTableName, Nullable: true},
	{Name: "extra", Type: LongBlob, Source: FilesTableName, Nullable: true},
}

var columnStatisticsSchema = Schema{
	{Name: "schema_name", Type: LongText, Source: ColumnStatisticsTableName},
	{Name: "table_name", Type: LongText, Source: ColumnStatisticsTableName},
	{Name: "column_name", Type: LongText, Source: ColumnStatisticsTableName},
	{Name: "histogram", Type: JSON, Source: ColumnStatisticsTableName},
}

var tablesSchema = Schema{
	{Name: "table_catalog", Type: LongText, Default: parse.MustStringToColumnDefaultValue(NewEmptyContext(), `""`, LongText, false), Nullable: false, Source: TablesTableName},
	{Name: "table_schema", Type: LongText, Default: parse.MustStringToColumnDefaultValue(NewEmptyContext(), `""`, LongText, false), Nullable: false, Source: TablesTableName},
	{Name: "table_name", Type: LongText, Default: parse.MustStringToColumnDefaultValue(NewEmptyContext(), `""`, LongText, false), Nullable: false, Source: TablesTableName},
	{Name: "table_type", Type: LongText, Default: parse.MustStringToColumnDefaultValue(NewEmptyContext(), `""`, LongText, false), Nullable: false, Source: TablesTableName},
	{Name: "engine", Type: LongText, Default: nil, Nullable: true, Source: TablesTableName},
	{Name: "version", Type: Uint64, Default: nil, Nullable: true, Source: TablesTableName},
	{Name: "row_format", Type: LongText, Default: nil, Nullable: true, Source: TablesTableName},
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
	{Name: "table_collation", Type: LongText, Default: nil, Nullable: true, Source: TablesTableName},
	{Name: "checksum", Type: Uint64, Default: nil, Nullable: true, Source: TablesTableName},
	{Name: "create_options", Type: LongText, Default: nil, Nullable: true, Source: TablesTableName},
	{Name: "table_comment", Type: LongText, Default: parse.MustStringToColumnDefaultValue(NewEmptyContext(), `""`, LongText, false), Nullable: false, Source: TablesTableName},
}

var columnsSchema = Schema{
	{Name: "table_catalog", Type: LongText, Default: parse.MustStringToColumnDefaultValue(NewEmptyContext(), `""`, LongText, false), Nullable: false, Source: ColumnsTableName},
	{Name: "table_schema", Type: LongText, Default: parse.MustStringToColumnDefaultValue(NewEmptyContext(), `""`, LongText, false), Nullable: false, Source: ColumnsTableName},
	{Name: "table_name", Type: LongText, Default: parse.MustStringToColumnDefaultValue(NewEmptyContext(), `""`, LongText, false), Nullable: false, Source: ColumnsTableName},
	{Name: "column_name", Type: LongText, Default: parse.MustStringToColumnDefaultValue(NewEmptyContext(), `""`, LongText, false), Nullable: false, Source: ColumnsTableName},
	{Name: "ordinal_position", Type: Uint64, Default: parse.MustStringToColumnDefaultValue(NewEmptyContext(), "0", Uint64, false), Nullable: false, Source: ColumnsTableName},
	{Name: "column_default", Type: LongText, Default: nil, Nullable: true, Source: ColumnsTableName},
	{Name: "is_nullable", Type: LongText, Default: parse.MustStringToColumnDefaultValue(NewEmptyContext(), `""`, LongText, false), Nullable: false, Source: ColumnsTableName},
	{Name: "data_type", Type: LongText, Default: parse.MustStringToColumnDefaultValue(NewEmptyContext(), `""`, LongText, false), Nullable: false, Source: ColumnsTableName},
	{Name: "character_maximum_length", Type: Uint64, Default: nil, Nullable: true, Source: ColumnsTableName},
	{Name: "character_octet_length", Type: Uint64, Default: nil, Nullable: true, Source: ColumnsTableName},
	{Name: "numeric_precision", Type: Uint64, Default: nil, Nullable: true, Source: ColumnsTableName},
	{Name: "numeric_scale", Type: Uint64, Default: nil, Nullable: true, Source: ColumnsTableName},
	{Name: "datetime_precision", Type: Uint64, Default: nil, Nullable: true, Source: ColumnsTableName},
	{Name: "character_set_name", Type: LongText, Default: nil, Nullable: true, Source: ColumnsTableName},
	{Name: "collation_name", Type: LongText, Default: nil, Nullable: true, Source: ColumnsTableName},
	{Name: "column_type", Type: LongText, Default: parse.MustStringToColumnDefaultValue(NewEmptyContext(), `""`, LongText, false), Nullable: false, Source: ColumnsTableName},
	{Name: "column_key", Type: LongText, Default: parse.MustStringToColumnDefaultValue(NewEmptyContext(), `""`, LongText, false), Nullable: false, Source: ColumnsTableName},
	{Name: "extra", Type: LongText, Default: parse.MustStringToColumnDefaultValue(NewEmptyContext(), `""`, LongText, false), Nullable: false, Source: ColumnsTableName},
	{Name: "privileges", Type: LongText, Default: parse.MustStringToColumnDefaultValue(NewEmptyContext(), `""`, LongText, false), Nullable: false, Source: ColumnsTableName},
	{Name: "column_comment", Type: LongText, Default: parse.MustStringToColumnDefaultValue(NewEmptyContext(), `""`, LongText, false), Nullable: false, Source: ColumnsTableName},
	{Name: "generation_expression", Type: LongText, Default: parse.MustStringToColumnDefaultValue(NewEmptyContext(), `""`, LongText, false), Nullable: false, Source: ColumnsTableName},
}

var schemataSchema = Schema{
	{Name: "catalog_name", Type: LongText, Default: nil, Nullable: false, Source: SchemataTableName},
	{Name: "schema_name", Type: LongText, Default: nil, Nullable: false, Source: SchemataTableName},
	{Name: "default_character_set_name", Type: LongText, Default: nil, Nullable: false, Source: SchemataTableName},
	{Name: "default_collation_name", Type: LongText, Default: nil, Nullable: false, Source: SchemataTableName},
	{Name: "sql_path", Type: LongText, Default: nil, Nullable: true, Source: SchemataTableName},
}

var collationsSchema = Schema{
	{Name: "collation_name", Type: LongText, Default: nil, Nullable: false, Source: CollationsTableName},
	{Name: "character_set_name", Type: LongText, Default: nil, Nullable: false, Source: CollationsTableName},
	{Name: "id", Type: LongText, Default: nil, Nullable: false, Source: CollationsTableName},
	{Name: "is_default", Type: LongText, Default: nil, Nullable: false, Source: CollationsTableName},
	{Name: "is_compiled", Type: LongText, Default: nil, Nullable: true, Source: CollationsTableName},
	{Name: "sortlen", Type: LongText, Default: nil, Nullable: false, Source: CollationsTableName},
	{Name: "pad_attribute", Type: LongText, Default: nil, Nullable: false, Source: CollationsTableName},
}

var statisticsSchema = Schema{
	{Name: "table_catalog", Type: LongText, Default: nil, Nullable: true, Source: StatisticsTableName},
	{Name: "table_schema", Type: LongText, Default: nil, Nullable: true, Source: StatisticsTableName},
	{Name: "table_name", Type: LongText, Default: nil, Nullable: true, Source: StatisticsTableName},
	{Name: "non_unique", Type: Int64, Default: nil, Nullable: false, Source: StatisticsTableName},
	{Name: "index_schema", Type: LongText, Default: nil, Nullable: true, Source: StatisticsTableName},
	{Name: "index_name", Type: LongText, Default: nil, Nullable: true, Source: StatisticsTableName},
	{Name: "seq_in_index", Type: Int64, Default: nil, Nullable: false, Source: StatisticsTableName},
	{Name: "column_name", Type: LongText, Default: nil, Nullable: true, Source: StatisticsTableName},
	{Name: "collation", Type: LongText, Default: nil, Nullable: true, Source: StatisticsTableName},
	{Name: "cardinality", Type: Int64, Default: nil, Nullable: true, Source: StatisticsTableName},
	{Name: "sub_part", Type: Int64, Default: nil, Nullable: true, Source: StatisticsTableName},
	{Name: "packed", Type: Int64, Default: nil, Nullable: true, Source: StatisticsTableName},
	{Name: "nullable", Type: LongText, Default: nil, Nullable: false, Source: StatisticsTableName},
	{Name: "index_type", Type: LongText, Default: nil, Nullable: false, Source: StatisticsTableName},
	{Name: "comment", Type: LongText, Default: nil, Nullable: false, Source: StatisticsTableName},
	{Name: "index_comment", Type: LongText, Default: nil, Nullable: false, Source: StatisticsTableName},
	{Name: "is_visible", Type: LongText, Default: nil, Nullable: false, Source: StatisticsTableName},
	{Name: "expression", Type: LongText, Default: nil, Nullable: true, Source: StatisticsTableName},
}

var tableConstraintsSchema = Schema{
	{Name: "constraint_catalog", Type: LongText, Default: nil, Nullable: true, Source: TableConstraintsTableName},
	{Name: "constraint_schema", Type: LongText, Default: nil, Nullable: true, Source: TableConstraintsTableName},
	{Name: "constraint_name", Type: LongText, Default: nil, Nullable: true, Source: TableConstraintsTableName},
	{Name: "table_schema", Type: LongText, Default: nil, Nullable: true, Source: TableConstraintsTableName},
	{Name: "table_name", Type: LongText, Default: nil, Nullable: true, Source: TableConstraintsTableName},
	{Name: "constraint_type", Type: LongText, Default: nil, Nullable: false, Source: TableConstraintsTableName},
	{Name: "enforced", Type: LongText, Default: nil, Nullable: false, Source: TableConstraintsTableName},
}

var referentialConstraintsSchema = Schema{
	{Name: "constraint_catalog", Type: LongText, Default: nil, Nullable: false, Source: ReferentialConstraintsTableName},
	{Name: "constraint_schema", Type: LongText, Default: nil, Nullable: false, Source: ReferentialConstraintsTableName},
	{Name: "constraint_name", Type: LongText, Default: nil, Nullable: true, Source: ReferentialConstraintsTableName},
	{Name: "unique_constraint_catalog", Type: LongText, Default: nil, Nullable: false, Source: ReferentialConstraintsTableName},
	{Name: "unique_constraint_schema", Type: LongText, Default: nil, Nullable: false, Source: ReferentialConstraintsTableName},
	{Name: "unique_constraint_name", Type: LongText, Default: nil, Nullable: true, Source: ReferentialConstraintsTableName},
	{Name: "match_option", Type: LongText, Default: nil, Nullable: false, Source: ReferentialConstraintsTableName},
	{Name: "update_rule", Type: LongText, Default: nil, Nullable: false, Source: ReferentialConstraintsTableName},
	{Name: "delete_rule", Type: LongText, Default: nil, Nullable: false, Source: ReferentialConstraintsTableName},
	{Name: "table_name", Type: LongText, Default: nil, Nullable: false, Source: ReferentialConstraintsTableName},
	{Name: "referenced_table_name", Type: LongText, Default: nil, Nullable: false, Source: ReferentialConstraintsTableName},
}

var keyColumnUsageSchema = Schema{
	{Name: "constraint_catalog", Type: LongText, Default: nil, Nullable: true, Source: KeyColumnUsageTableName},
	{Name: "constraint_schema", Type: LongText, Default: nil, Nullable: true, Source: KeyColumnUsageTableName},
	{Name: "constraint_name", Type: LongText, Default: nil, Nullable: true, Source: KeyColumnUsageTableName},
	{Name: "table_catalog", Type: LongText, Default: nil, Nullable: true, Source: KeyColumnUsageTableName},
	{Name: "table_schema", Type: LongText, Default: nil, Nullable: true, Source: KeyColumnUsageTableName},
	{Name: "table_name", Type: LongText, Default: nil, Nullable: true, Source: KeyColumnUsageTableName},
	{Name: "column_name", Type: LongText, Default: nil, Nullable: true, Source: KeyColumnUsageTableName},
	{Name: "ordinal_position", Type: LongText, Default: nil, Nullable: false, Source: KeyColumnUsageTableName},
	{Name: "position_in_unique_constraint", Type: LongText, Default: nil, Nullable: true, Source: KeyColumnUsageTableName},
	{Name: "referenced_table_schema", Type: LongText, Default: nil, Nullable: true, Source: KeyColumnUsageTableName},
	{Name: "referenced_table_name", Type: LongText, Default: nil, Nullable: true, Source: KeyColumnUsageTableName},
	{Name: "referenced_column_name", Type: LongText, Default: nil, Nullable: true, Source: KeyColumnUsageTableName},
}

var triggersSchema = Schema{
	{Name: "trigger_catalog", Type: LongText, Default: nil, Nullable: true, Source: TriggersTableName},
	{Name: "trigger_schema", Type: LongText, Default: nil, Nullable: true, Source: TriggersTableName},
	{Name: "trigger_name", Type: LongText, Default: nil, Nullable: false, Source: TriggersTableName},
	{Name: "event_manipulation", Type: LongText, Default: nil, Nullable: false, Source: TriggersTableName},
	{Name: "event_object_catalog", Type: LongText, Default: nil, Nullable: true, Source: TriggersTableName},
	{Name: "event_object_schema", Type: LongText, Default: nil, Nullable: true, Source: TriggersTableName},
	{Name: "event_object_table", Type: LongText, Default: nil, Nullable: true, Source: TriggersTableName},
	{Name: "action_order", Type: Int64, Default: nil, Nullable: false, Source: TriggersTableName},
	{Name: "action_condition", Type: Int64, Default: nil, Nullable: true, Source: TriggersTableName},
	{Name: "action_statement", Type: LongText, Default: nil, Nullable: false, Source: TriggersTableName},
	{Name: "action_orientation", Type: LongText, Default: nil, Nullable: false, Source: TriggersTableName},
	{Name: "action_timing", Type: LongText, Default: nil, Nullable: false, Source: TriggersTableName},
	{Name: "action_reference_old_table", Type: Int64, Default: nil, Nullable: true, Source: TriggersTableName},
	{Name: "action_reference_new_table", Type: Int64, Default: nil, Nullable: true, Source: TriggersTableName},
	{Name: "action_reference_old_row", Type: LongText, Default: nil, Nullable: false, Source: TriggersTableName},
	{Name: "action_reference_new_row", Type: LongText, Default: nil, Nullable: false, Source: TriggersTableName},
	{Name: "created", Type: Timestamp, Default: nil, Nullable: false, Source: TriggersTableName},
	{Name: "sql_mode", Type: LongText, Default: nil, Nullable: false, Source: TriggersTableName},
	{Name: "definer", Type: LongText, Default: nil, Nullable: false, Source: TriggersTableName},
	{Name: "character_set_client", Type: LongText, Default: nil, Nullable: false, Source: TriggersTableName},
	{Name: "collation_connection", Type: LongText, Default: nil, Nullable: false, Source: TriggersTableName},
	{Name: "database_collation", Type: LongText, Default: nil, Nullable: false, Source: TriggersTableName},
}

var eventsSchema = Schema{
	{Name: "event_catalog", Type: LongText, Default: nil, Nullable: true, Source: EventsTableName},
	{Name: "event_schema", Type: LongText, Default: nil, Nullable: true, Source: EventsTableName},
	{Name: "event_name", Type: LongText, Default: nil, Nullable: false, Source: EventsTableName},
	{Name: "definer", Type: LongText, Default: nil, Nullable: false, Source: EventsTableName},
	{Name: "time_zone", Type: LongText, Default: nil, Nullable: true, Source: EventsTableName},
	{Name: "event_body", Type: LongText, Default: nil, Nullable: true, Source: EventsTableName},
	{Name: "event_definition", Type: LongText, Default: nil, Nullable: true, Source: EventsTableName},
	{Name: "event_type", Type: LongText, Default: nil, Nullable: false, Source: EventsTableName},
	{Name: "execute_at", Type: Datetime, Default: nil, Nullable: true, Source: EventsTableName},
	{Name: "interval_value", Type: LongText, Default: nil, Nullable: false, Source: EventsTableName},
	{Name: "interval_field", Type: LongText, Default: nil, Nullable: false, Source: EventsTableName},
	{Name: "sql_mode", Type: LongText, Default: nil, Nullable: false, Source: EventsTableName},
	{Name: "starts", Type: Datetime, Default: nil, Nullable: true, Source: EventsTableName},
	{Name: "ends", Type: Datetime, Default: nil, Nullable: true, Source: EventsTableName},
	{Name: "status", Type: LongText, Default: nil, Nullable: false, Source: EventsTableName},
	{Name: "on_completion", Type: LongText, Default: nil, Nullable: false, Source: EventsTableName},
	{Name: "created", Type: Timestamp, Default: nil, Nullable: false, Source: EventsTableName},
	{Name: "last_altered", Type: Timestamp, Default: nil, Nullable: false, Source: EventsTableName},
	{Name: "last_executed", Type: Datetime, Default: nil, Nullable: false, Source: EventsTableName},
	{Name: "event_comment", Type: LongText, Default: nil, Nullable: false, Source: EventsTableName},
	{Name: "originator", Type: Int64, Default: nil, Nullable: false, Source: EventsTableName},
	{Name: "character_set_client", Type: LongText, Default: nil, Nullable: false, Source: EventsTableName},
	{Name: "collation_connection", Type: LongText, Default: nil, Nullable: false, Source: EventsTableName},
	{Name: "database_collation", Type: LongText, Default: nil, Nullable: false, Source: EventsTableName},
}

var routinesSchema = Schema{
	{Name: "specific_name", Type: LongText, Default: nil, Nullable: false, Source: RoutinesTableName},
	{Name: "routine_catalog", Type: LongText, Default: nil, Nullable: true, Source: RoutinesTableName},
	{Name: "routine_schema", Type: LongText, Default: nil, Nullable: true, Source: RoutinesTableName},
	{Name: "routine_name", Type: LongText, Default: nil, Nullable: false, Source: RoutinesTableName},
	{Name: "routine_type", Type: LongText, Default: nil, Nullable: false, Source: RoutinesTableName},
	{Name: "data_type", Type: LongText, Default: nil, Nullable: true, Source: RoutinesTableName},
	{Name: "character_maximum_length", Type: Int64, Default: nil, Nullable: true, Source: RoutinesTableName},
	{Name: "character_octet_length", Type: Int64, Default: nil, Nullable: true, Source: RoutinesTableName},
	{Name: "numeric_precision", Type: Int64, Default: nil, Nullable: true, Source: RoutinesTableName},
	{Name: "numeric_scale", Type: Int64, Default: nil, Nullable: true, Source: RoutinesTableName},
	{Name: "datetime_precision", Type: Int64, Default: nil, Nullable: true, Source: RoutinesTableName},
	{Name: "character_set_name", Type: LongText, Default: nil, Nullable: true, Source: RoutinesTableName},
	{Name: "collation_name", Type: LongText, Default: nil, Nullable: true, Source: RoutinesTableName},
	{Name: "dtd_identifier", Type: LongText, Default: nil, Nullable: true, Source: RoutinesTableName},
	{Name: "routine_body", Type: LongText, Default: nil, Nullable: false, Source: RoutinesTableName},
	{Name: "routine_definition", Type: LongText, Default: nil, Nullable: true, Source: RoutinesTableName},
	{Name: "external_name", Type: Int64, Default: nil, Nullable: true, Source: RoutinesTableName},
	{Name: "external_language", Type: LongText, Default: nil, Nullable: false, Source: RoutinesTableName},
	{Name: "parameter_style", Type: LongText, Default: nil, Nullable: false, Source: RoutinesTableName},
	{Name: "is_deterministic", Type: LongText, Default: nil, Nullable: false, Source: RoutinesTableName},
	{Name: "sql_data_access", Type: LongText, Default: nil, Nullable: false, Source: RoutinesTableName},
	{Name: "sql_path", Type: Int64, Default: nil, Nullable: true, Source: RoutinesTableName},
	{Name: "security_type", Type: LongText, Default: nil, Nullable: false, Source: RoutinesTableName},
	{Name: "created", Type: Timestamp, Default: nil, Nullable: false, Source: RoutinesTableName},
	{Name: "last_altered", Type: Timestamp, Default: nil, Nullable: false, Source: RoutinesTableName},
	{Name: "sql_mode", Type: LongText, Default: nil, Nullable: false, Source: RoutinesTableName},
	{Name: "routine_comment", Type: LongText, Default: nil, Nullable: false, Source: RoutinesTableName},
	{Name: "definer", Type: LongText, Default: nil, Nullable: false, Source: RoutinesTableName},
	{Name: "character_set_client", Type: LongText, Default: nil, Nullable: false, Source: RoutinesTableName},
	{Name: "collation_connection", Type: LongText, Default: nil, Nullable: false, Source: RoutinesTableName},
	{Name: "database_collation", Type: LongText, Default: nil, Nullable: false, Source: RoutinesTableName},
}

var viewsSchema = Schema{
	{Name: "table_catalog", Type: LongText, Default: nil, Nullable: true, Source: ViewsTableName},
	{Name: "table_schema", Type: LongText, Default: nil, Nullable: true, Source: ViewsTableName},
	{Name: "table_name", Type: LongText, Default: nil, Nullable: true, Source: ViewsTableName},
	{Name: "view_definition", Type: LongText, Default: nil, Nullable: true, Source: ViewsTableName},
	{Name: "check_option", Type: LongText, Default: nil, Nullable: true, Source: ViewsTableName},
	{Name: "is_updatable", Type: LongText, Default: nil, Nullable: true, Source: ViewsTableName},
	{Name: "definer", Type: LongText, Default: nil, Nullable: true, Source: ViewsTableName},
	{Name: "security_type", Type: LongText, Default: nil, Nullable: true, Source: ViewsTableName},
	{Name: "character_set_client", Type: LongText, Default: nil, Nullable: false, Source: ViewsTableName},
	{Name: "collation_connection", Type: LongText, Default: nil, Nullable: false, Source: ViewsTableName},
}

var userPrivilegesSchema = Schema{
	{Name: "grantee", Type: LongText, Default: nil, Nullable: false, Source: UserPrivilegesTableName},
	{Name: "table_catalog", Type: LongText, Default: nil, Nullable: false, Source: UserPrivilegesTableName},
	{Name: "privilege_type", Type: LongText, Default: nil, Nullable: false, Source: UserPrivilegesTableName},
	{Name: "is_grantable", Type: LongText, Default: nil, Nullable: false, Source: UserPrivilegesTableName},
}

func tablesRowIter(ctx *Context, cat *Catalog) RowIter {
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

		err := DBTableIter(ctx, db, func(t Table) (cont bool, err error) {
			rows = append(rows, Row{
				"def",                      // table_catalog
				db.Name(),                  // table_schema
				t.Name(),                   // table_name
				tableType,                  // table_type
				engine,                     // engine
				10,                         // version (protocol, always 10)
				rowFormat,                  // row_format
				nil,                        // table_rows
				nil,                        // avg_row_length
				nil,                        // data_length
				nil,                        // max_data_length
				nil,                        // max_data_length
				nil,                        // data_free
				nil,                        // auto_increment
				nil,                        // create_time
				nil,                        // update_time
				nil,                        // check_time
				Collation_Default.String(), // table_collation
				nil,                        // checksum
				nil,                        // create_options
				"",                         // table_comment
			})

			return true, nil
		})

		for _, view := range ctx.ViewsInDatabase(db.Name()) {
			rows = append(rows, Row{
				"def",                      // table_catalog
				db.Name(),                  // table_schema
				view.Name(),                // table_name
				"VIEW",                     // table_type
				engine,                     // engine
				10,                         // version (protocol, always 10)
				rowFormat,                  // row_format
				nil,                        // table_rows
				nil,                        // avg_row_length
				nil,                        // data_length
				nil,                        // max_data_length
				nil,                        // max_data_length
				nil,                        // data_free
				nil,                        // auto_increment
				nil,                        // create_time
				nil,                        // update_time
				nil,                        // check_time
				Collation_Default.String(), // table_collation
				nil,                        // checksum
				nil,                        // create_options
				"",                         // table_comment
			})
		}

		// TODO: fix panics
		if err != nil {
			panic(err)
		}
	}

	return RowsToRowIter(rows...)
}

func columnsRowIter(ctx *Context, cat *Catalog) RowIter {
	var rows []Row
	for _, db := range cat.AllDatabases() {
		err := DBTableIter(ctx, db, func(t Table) (cont bool, err error) {
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
					charName = Collation_Default.CharacterSet().String()
					collName = Collation_Default.String()
				}
				rows = append(rows, Row{
					"def",                            // table_catalog
					db.Name(),                        // table_schema
					t.Name(),                         // table_name
					c.Name,                           // column_name
					uint64(i),                        // ordinal_position
					c.Default.String(),               // column_default
					nullable,                         // is_nullable
					strings.ToLower(c.Type.String()), // data_type
					nil,                              // character_maximum_length
					nil,                              // character_octet_length
					nil,                              // numeric_precision
					nil,                              // numeric_scale
					nil,                              // datetime_precision
					charName,                         // character_set_name
					collName,                         // collation_name
					strings.ToLower(c.Type.String()), // column_type
					"",                               // column_key
					"",                               // extra
					"select",                         // privileges
					"",                               // column_comment
					"",                               // generation_expression
				})
			}
			return true, nil
		})

		// TODO: fix panics
		if err != nil {
			panic(err)
		}
	}
	return RowsToRowIter(rows...)
}

func schemataRowIter(ctx *Context, c *Catalog) RowIter {
	dbs := c.AllDatabases()

	var rows []Row
	for _, db := range dbs {
		rows = append(rows, Row{
			"def",
			db.Name(),
			Collation_Default.CharacterSet().String(),
			Collation_Default.String(),
			nil,
		})
	}

	return RowsToRowIter(rows...)
}

func collationsRowIter(ctx *Context, c *Catalog) RowIter {
	return RowsToRowIter(Row{
		Collation_Default.String(),
		Collation_Default.CharacterSet().String(),
		int64(1),
		"Yes",
		"Yes",
		int64(1),
		"PAD SPACE",
	})
}

func emptyRowIter(ctx *Context, c *Catalog) RowIter {
	return RowsToRowIter()
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
			CollationsTableName: &informationSchemaTable{
				name:    CollationsTableName,
				schema:  collationsSchema,
				catalog: cat,
				rowIter: collationsRowIter,
			},
			StatisticsTableName: &informationSchemaTable{
				name:    StatisticsTableName,
				schema:  statisticsSchema,
				catalog: cat,
				rowIter: emptyRowIter,
			},
			TableConstraintsTableName: &informationSchemaTable{
				name:    TableConstraintsTableName,
				schema:  tableConstraintsSchema,
				catalog: cat,
				rowIter: emptyRowIter,
			},
			ReferentialConstraintsTableName: &informationSchemaTable{
				name:    ReferentialConstraintsTableName,
				schema:  referentialConstraintsSchema,
				catalog: cat,
				rowIter: emptyRowIter,
			},
			KeyColumnUsageTableName: &informationSchemaTable{
				name:    KeyColumnUsageTableName,
				schema:  keyColumnUsageSchema,
				catalog: cat,
				rowIter: emptyRowIter,
			},
			TriggersTableName: &informationSchemaTable{
				name:    TriggersTableName,
				schema:  triggersSchema,
				catalog: cat,
				rowIter: emptyRowIter,
			},
			EventsTableName: &informationSchemaTable{
				name:    EventsTableName,
				schema:  eventsSchema,
				catalog: cat,
				rowIter: emptyRowIter,
			},
			RoutinesTableName: &informationSchemaTable{
				name:    RoutinesTableName,
				schema:  routinesSchema,
				catalog: cat,
				rowIter: emptyRowIter,
			},
			ViewsTableName: &informationSchemaTable{
				name:    ViewsTableName,
				schema:  viewsSchema,
				catalog: cat,
				rowIter: viewRowIter,
			},
			UserPrivilegesTableName: &informationSchemaTable{
				name:    UserPrivilegesTableName,
				schema:  userPrivilegesSchema,
				catalog: cat,
				rowIter: emptyRowIter,
			},
		},
	}
}

func viewRowIter(context *Context, catalog *Catalog) RowIter {
	var rows []Row
	for _, db := range catalog.AllDatabases() {
		database := db.Name()
		for _, view := range context.ViewRegistry.ViewsInDatabase(database) {
			rows = append(rows, Row{
				"def",
				database,
				view.Name(),
				view.TextDefinition(),
				"NONE",
				"YES",
				"",
				"DEFINER",
				Collation_Default.CharacterSet().String(),
				Collation_Default.String(),
			})
		}
	}
	return RowsToRowIter(rows...)
}

// Name implements the sql.Database interface.
func (db *informationSchemaDatabase) Name() string { return db.name }

// Tables implements the sql.Database interface.
func (db *informationSchemaDatabase) Tables() map[string]Table { return db.tables }

func (db *informationSchemaDatabase) GetTableInsensitive(ctx *Context, tblName string) (Table, bool, error) {
	tbl, ok := GetTableInsensitive(tblName, db.tables)
	return tbl, ok, nil
}

func (db *informationSchemaDatabase) GetTableNames(ctx *Context) ([]string, error) {
	tblNames := make([]string, 0, len(db.tables))
	for k := range db.tables {
		tblNames = append(tblNames, k)
	}

	return tblNames, nil
}

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

	return t.rowIter(ctx, t.catalog), nil
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
			col.Type.String(),
			col.Nullable,
		)
	}
	_ = p.WriteChildren(schema...)
	return p.String()
}

func partitionKey(tableName string) []byte {
	return []byte(InformationSchemaDatabaseName + "." + tableName)
}
