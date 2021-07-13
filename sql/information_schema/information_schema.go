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

package information_schema

import (
	"bytes"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/dolthub/vitess/go/vt/sqlparser"

	. "github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/dolthub/go-mysql-server/sql/parse"
	"github.com/dolthub/go-mysql-server/sql/plan"
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
	// CharacterSetsTableName is the name of the character_sets table
	CharacterSetsTableName = "character_sets"
	// EnginesTableName is the name of the engines table
	EnginesTableName = "engines"
	// CheckConstraintsTableName is the name of check_constraints table
	CheckConstraintsTableName = "check_constraints"
	// PartitionsTableName is the name of the partitions table
	PartitionsTableName = "partitions"
	// InnoDBTempTableName is the name of the INNODB_TEMP_TABLE_INFO table
	InnoDBTempTableName = "innodb_temp_table_info"
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
	rowIter func(*Context, *Catalog) (RowIter, error)
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
	{Name: "create_time", Type: Timestamp, Default: nil, Nullable: true, Source: TablesTableName},
	{Name: "update_time", Type: Timestamp, Default: nil, Nullable: true, Source: TablesTableName},
	{Name: "check_time", Type: Timestamp, Default: nil, Nullable: true, Source: TablesTableName},
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

var characterSetSchema = Schema{
	{Name: "character_set_name", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: CharacterSetsTableName},
	{Name: "default_collate_name", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: CharacterSetsTableName},
	{Name: "description", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 2048), Default: nil, Nullable: false, Source: CharacterSetsTableName},
	{Name: "maxlen", Type: Uint8, Default: nil, Nullable: false, Source: CharacterSetsTableName},
}

var enginesSchema = Schema{
	{Name: "engine", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: EnginesTableName},
	{Name: "support", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 8), Default: nil, Nullable: false, Source: EnginesTableName},
	{Name: "comment", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 80), Default: nil, Nullable: false, Source: EnginesTableName},
	{Name: "transactions", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 3), Default: nil, Nullable: false, Source: EnginesTableName},
	{Name: "xa", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 3), Default: nil, Nullable: false, Source: EnginesTableName},
	{Name: "savepoints", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 3), Default: nil, Nullable: false, Source: EnginesTableName},
}

var checkConstraintsSchema = Schema{
	{Name: "constraint_catalog", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: CheckConstraintsTableName},
	{Name: "constraint_schema", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: CheckConstraintsTableName},
	{Name: "constraint_name", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: CheckConstraintsTableName},
	{Name: "check_clause", Type: LongText, Default: nil, Nullable: false, Source: CheckConstraintsTableName},
}

var partitionSchema = Schema{
	{Name: "table_catalog", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: PartitionsTableName},
	{Name: "table_schema", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: PartitionsTableName},
	{Name: "table_name", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: PartitionsTableName},
	{Name: "partition_name", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: PartitionsTableName},
	{Name: "subpartition_name", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: PartitionsTableName},
	{Name: "partition_ordinal_position", Type: Uint64, Default: nil, Nullable: true, Source: PartitionsTableName},
	{Name: "subpartition_ordinal_position", Type: Uint64, Default: nil, Nullable: true, Source: PartitionsTableName},
	{Name: "partition_method", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 13), Default: nil, Nullable: true, Source: PartitionsTableName},
	{Name: "subpartition_method", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 13), Default: nil, Nullable: true, Source: PartitionsTableName},
	{Name: "partition_expression", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 2048), Default: nil, Nullable: true, Source: PartitionsTableName},
	{Name: "subpartition_expression", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 2048), Default: nil, Nullable: true, Source: PartitionsTableName},
	{Name: "partition_description", Type: LongText, Default: nil, Nullable: true, Source: PartitionsTableName},
	{Name: "table_rows", Type: Uint64, Default: nil, Nullable: true, Source: PartitionsTableName},
	{Name: "avg_row_length", Type: Uint64, Default: nil, Nullable: true, Source: PartitionsTableName},
	{Name: "data_length", Type: Uint64, Default: nil, Nullable: true, Source: PartitionsTableName},
	{Name: "max_data_length", Type: Uint64, Default: nil, Nullable: true, Source: PartitionsTableName},
	{Name: "index_length", Type: Uint64, Default: nil, Nullable: true, Source: PartitionsTableName},
	{Name: "data_free", Type: Uint64, Default: nil, Nullable: true, Source: PartitionsTableName},
	{Name: "create_time", Type: Timestamp, Default: nil, Nullable: false, Source: PartitionsTableName},
	{Name: "update_time", Type: Datetime, Default: nil, Nullable: true, Source: PartitionsTableName},
	{Name: "check_time", Type: Datetime, Default: nil, Nullable: true, Source: PartitionsTableName},
	{Name: "checksum", Type: Uint64, Default: nil, Nullable: true, Source: PartitionsTableName},
	{Name: "partition_comment", Type: LongText, Default: nil, Nullable: false, Source: PartitionsTableName},
	{Name: "nodegroup", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 256), Default: nil, Nullable: true, Source: PartitionsTableName},
	{Name: "tablespace_name", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 258), Default: nil, Nullable: true, Source: PartitionsTableName},
}

var innoDBTempTableSchema = Schema{
	{Name: "table_id", Type: Int64, Default: nil, Nullable: false, Source: InnoDBTempTableName},
	{Name: "name", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: InnoDBTempTableName},
	{Name: "n_cols", Type: Uint64, Default: nil, Nullable: false, Source: InnoDBTempTableName},
	{Name: "space", Type: Uint64, Default: nil, Nullable: false, Source: InnoDBTempTableName},
}

func tablesRowIter(ctx *Context, cat *Catalog) (RowIter, error) {
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

		y2k, _ := Timestamp.Convert("2000-01-01 00:00:00")
		err := DBTableIter(ctx, db, func(t Table) (cont bool, err error) {

			autoVal := getAutoIncrementValue(ctx, t)
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
				autoVal,                    // auto_increment
				y2k,                        // create_time
				y2k,                        // update_time
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

		if err != nil {
			return nil, err
		}
	}

	return RowsToRowIter(rows...), nil
}

func columnsRowIter(ctx *Context, cat *Catalog) (RowIter, error) {
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
					c.Extra,                          // extra
					"select",                         // privileges
					c.Comment,                        // column_comment
					"",                               // generation_expression
				})
			}
			return true, nil
		})

		if err != nil {
			return nil, err
		}
	}
	return RowsToRowIter(rows...), nil
}

func schemataRowIter(ctx *Context, c *Catalog) (RowIter, error) {
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

	return RowsToRowIter(rows...), nil
}

func collationsRowIter(ctx *Context, c *Catalog) (RowIter, error) {
	var rows []Row
	for cName := range CollationToMySQLVals {
		c := Collations[cName]
		rows = append(rows, Row{
			c.String(),
			c.CharacterSet().String(),
			c.ID(),
			c.IsDefault(),
			c.IsCompiled(),
			c.SortLen(),
			c.PadSpace(),
		})
	}
	return RowsToRowIter(rows...), nil
}

func charsetRowIter(ctx *Context, c *Catalog) (RowIter, error) {
	var rows []Row
	for _, c := range SupportedCharsets {
		rows = append(rows, Row{
			c.String(),
			c.DefaultCollation().String(),
			c.Description(),
			c.MaxLength(),
		})
	}
	return RowsToRowIter(rows...), nil
}

func engineRowIter(ctx *Context, c *Catalog) (RowIter, error) {
	var rows []Row
	for _, c := range SupportedEngines {
		rows = append(rows, Row{
			c.String(),
			c.Support(),
			c.Comment(),
			c.Transactions(),
			c.XA(),
			c.Savepoints(),
		})
	}
	return RowsToRowIter(rows...), nil
}

func triggersRowIter(ctx *Context, c *Catalog) (RowIter, error) {
	var rows []Row
	for _, db := range c.AllDatabases() {
		triggerDb, ok := db.(TriggerDatabase)
		if ok {
			triggers, err := triggerDb.GetTriggers(ctx)
			if err != nil {
				return nil, err
			}
			var triggerPlans []*plan.CreateTrigger
			for _, trigger := range triggers {
				parsedTrigger, err := parse.Parse(ctx, trigger.CreateStatement)
				if err != nil {
					return nil, err
				}
				triggerPlan, ok := parsedTrigger.(*plan.CreateTrigger)
				if !ok {
					return nil, ErrTriggerCreateStatementInvalid.New(trigger.CreateStatement)
				}
				triggerPlans = append(triggerPlans, triggerPlan)
			}

			beforeTriggers, afterTriggers := analyzer.OrderTriggers(triggerPlans)
			var beforeDelete []*plan.CreateTrigger
			var beforeInsert []*plan.CreateTrigger
			var beforeUpdate []*plan.CreateTrigger
			var afterDelete []*plan.CreateTrigger
			var afterInsert []*plan.CreateTrigger
			var afterUpdate []*plan.CreateTrigger
			for _, triggerPlan := range beforeTriggers {
				switch triggerPlan.TriggerEvent {
				case sqlparser.DeleteStr:
					beforeDelete = append(beforeDelete, triggerPlan)
				case sqlparser.InsertStr:
					beforeInsert = append(beforeInsert, triggerPlan)
				case sqlparser.UpdateStr:
					beforeUpdate = append(beforeUpdate, triggerPlan)
				}
			}
			for _, triggerPlan := range afterTriggers {
				switch triggerPlan.TriggerEvent {
				case sqlparser.DeleteStr:
					afterDelete = append(afterDelete, triggerPlan)
				case sqlparser.InsertStr:
					afterInsert = append(afterInsert, triggerPlan)
				case sqlparser.UpdateStr:
					afterUpdate = append(afterUpdate, triggerPlan)
				}
			}

			// These are grouped as such just to use the index as the action order. No special importance on the arrangement,
			// or the fact that these are slices in a larger slice rather than separate counts.
			for _, planGroup := range [][]*plan.CreateTrigger{beforeDelete, beforeInsert, beforeUpdate, afterDelete, afterInsert, afterUpdate} {
				for order, triggerPlan := range planGroup {
					triggerEvent := strings.ToUpper(triggerPlan.TriggerEvent)
					triggerTime := strings.ToUpper(triggerPlan.TriggerTime)
					tableName := triggerPlan.Table.(*plan.UnresolvedTable).Name()
					characterSetClient, err := ctx.GetSessionVariable(ctx, "character_set_client")
					if err != nil {
						return nil, err
					}
					collationConnection, err := ctx.GetSessionVariable(ctx, "collation_connection")
					if err != nil {
						return nil, err
					}
					collationServer, err := ctx.GetSessionVariable(ctx, "collation_server")
					if err != nil {
						return nil, err
					}
					rows = append(rows, Row{
						"def",                   // trigger_catalog
						triggerDb.Name(),        // trigger_schema
						triggerPlan.TriggerName, // trigger_name
						triggerEvent,            // event_manipulation
						"def",                   // event_object_catalog
						triggerDb.Name(),        // event_object_schema //TODO: table may be in a different db
						tableName,               // event_object_table
						int64(order + 1),        // action_order
						nil,                     // action_condition
						triggerPlan.BodyString,  // action_statement
						"ROW",                   // action_orientation
						triggerTime,             // action_timing
						nil,                     // action_reference_old_table
						nil,                     // action_reference_new_table
						"OLD",                   // action_reference_old_row
						"NEW",                   // action_reference_new_row
						time.Unix(0, 0).UTC(),   // created
						"",                      // sql_mode
						"",                      // definer
						characterSetClient,      // character_set_client
						collationConnection,     // collation_connection
						collationServer,         // database_collation
					})
				}
			}
		}
	}
	return RowsToRowIter(rows...), nil
}

func checkConstraintsRowIter(ctx *Context, c *Catalog) (RowIter, error) {
	var rows []Row
	for _, db := range c.AllDatabases() {
		tableNames, err := db.GetTableNames(ctx)
		if err != nil {
			return nil, err
		}

		for _, tableName := range tableNames {
			tbl, _, err := c.Table(ctx, db.Name(), tableName)
			if err != nil {
				return nil, err
			}

			checkTbl, ok := tbl.(CheckTable)
			if ok {
				checkDefinitions, err := checkTbl.GetChecks(ctx)
				if err != nil {
					return nil, err
				}

				for _, checkDefinition := range checkDefinitions {
					rows = append(rows, Row{"def", db.Name(), checkDefinition.Name, checkDefinition.CheckExpression})
				}
			}
		}
	}

	return RowsToRowIter(rows...), nil
}

func tableConstraintRowIter(ctx *Context, c *Catalog) (RowIter, error) {
	var rows []Row
	for _, db := range c.AllDatabases() {
		tableNames, err := db.GetTableNames(ctx)
		if err != nil {
			return nil, err
		}

		for _, tableName := range tableNames {
			tbl, _, err := c.Table(ctx, db.Name(), tableName)
			if err != nil {
				return nil, err
			}

			// Get all the CHECKs
			checkTbl, ok := tbl.(CheckTable)
			if ok {
				checkDefinitions, err := checkTbl.GetChecks(ctx)
				if err != nil {
					return nil, err
				}

				for _, checkDefinition := range checkDefinitions {
					enforced := "YES"
					if !checkDefinition.Enforced {
						enforced = "NO"
					}
					rows = append(rows, Row{"def", db.Name(), checkDefinition.Name, db.Name(), tbl.Name(), "CHECK", enforced})
				}
			}

			// Get UNIQUEs, PRIMARY KEYs
			// TODO: Doesn't correctly consider primary keys from table implementations that don't implement sql.IndexedTable
			indexTable, ok := tbl.(IndexedTable)
			if ok {
				indexes, err := indexTable.GetIndexes(ctx)
				if err != nil {
					return nil, err
				}

				for _, index := range indexes {
					outputType := "PRIMARY KEY"
					if index.ID() != "PRIMARY" {
						if index.IsUnique() {
							outputType = "UNIQUE"
						} else {
							// In this case we have a multi-index which is not represented in this table
							continue
						}

					}

					rows = append(rows, Row{"def", db.Name(), index.ID(), db.Name(), tbl.Name(), outputType, "YES"})
				}
			}

			// Get FKs
			fkTable, ok := tbl.(ForeignKeyTable)
			if ok {
				fks, err := fkTable.GetForeignKeys(ctx)
				if err != nil {
					return nil, err
				}

				for _, fk := range fks {
					rows = append(rows, Row{"def", db.Name(), fk.Name, db.Name(), tbl.Name(), "FOREIGN KEY", "YES"})
				}
			}
		}
	}

	return RowsToRowIter(rows...), nil
}

func getColumnNamesFromIndex(idx Index, table Table) []string {
	var indexCols []string
	for _, expr := range idx.Expressions() {
		col := plan.GetColumnFromIndexExpr(expr, table)
		if col != nil {
			indexCols = append(indexCols, fmt.Sprintf("`%s`", col.Name))
		}
	}

	return indexCols
}

func keyColumnConstraintRowIter(ctx *Context, c *Catalog) (RowIter, error) {
	var rows []Row
	for _, db := range c.AllDatabases() {
		tableNames, err := db.GetTableNames(ctx)
		if err != nil {
			return nil, err
		}

		for _, tableName := range tableNames {
			tbl, _, err := c.Table(ctx, db.Name(), tableName)
			if err != nil {
				return nil, err
			}

			// Get UNIQUEs, PRIMARY KEYs
			// TODO: Doesn't correctly consider primary keys from table implementations that don't implement sql.IndexedTable
			indexTable, ok := tbl.(IndexedTable)
			if ok {
				indexes, err := indexTable.GetIndexes(ctx)
				if err != nil {
					return nil, err
				}

				for _, index := range indexes {
					// In this case we have a multi-index which is not represented in this table
					if index.ID() != "PRIMARY" && !index.IsUnique() {
						continue
					}

					colNames := getColumnNamesFromIndex(index, tbl)

					// Create a Row for each column this index refers too.
					for i, colName := range colNames {
						colName = strings.Replace(colName, "`", "", -1) // get rid of backticks
						ordinalPosition := i + 1                        // Ordinal Positions starts at one

						rows = append(rows, Row{"def", db.Name(), index.ID(), "def", db.Name(), tbl.Name(), colName, ordinalPosition, nil, nil, nil, nil})
					}
				}
			}

			// Get FKs
			fkTable, ok := tbl.(ForeignKeyTable)
			if ok {
				fks, err := fkTable.GetForeignKeys(ctx)
				if err != nil {
					return nil, err
				}

				for _, fk := range fks {
					for j, colName := range fk.Columns {
						ordinalPosition := j + 1

						referencedSchema := db.Name()
						referencedTableName := fk.ReferencedTable
						referencedColumnName := strings.Replace(fk.ReferencedColumns[j], "`", "", -1) // get rid of backticks

						rows = append(rows, Row{"def", db.Name(), fk.Name, "def", db.Name(), tbl.Name(), colName, ordinalPosition, ordinalPosition, referencedSchema, referencedTableName, referencedColumnName})
					}
				}
			}
		}
	}

	return RowsToRowIter(rows...), nil
}

// innoDBTempTableIter returns info on the temporary tables stored in the session.
// TODO: Since Table ids and Space are not yet supported this table is not completely accurate yet.
func innoDBTempTableIter(ctx *Context, c *Catalog) (RowIter, error) {
	var rows []Row
	for _, db := range c.AllDatabases() {
		tb, ok := db.(TemporaryTableDatabase)
		if !ok {
			continue
		}

		tables, err := tb.GetAllTemporaryTables(ctx)
		if err != nil {
			return nil, err
		}

		for i, table := range tables {
			rows = append(rows, Row{i, table.String(), len(table.Schema()), 0})
		}
	}

	return RowsToRowIter(rows...), nil
}

func emptyRowIter(ctx *Context, c *Catalog) (RowIter, error) {
	return RowsToRowIter(), nil
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
			CharacterSetsTableName: &informationSchemaTable{
				name:    CharacterSetsTableName,
				schema:  characterSetSchema,
				catalog: cat,
				rowIter: charsetRowIter,
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
				rowIter: tableConstraintRowIter,
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
				rowIter: keyColumnConstraintRowIter,
			},
			TriggersTableName: &informationSchemaTable{
				name:    TriggersTableName,
				schema:  triggersSchema,
				catalog: cat,
				rowIter: triggersRowIter,
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
			EnginesTableName: &informationSchemaTable{
				name:    EnginesTableName,
				schema:  enginesSchema,
				catalog: cat,
				rowIter: engineRowIter,
			},
			CheckConstraintsTableName: &informationSchemaTable{
				name:    CheckConstraintsTableName,
				schema:  checkConstraintsSchema,
				catalog: cat,
				rowIter: checkConstraintsRowIter,
			},
			PartitionsTableName: &informationSchemaTable{
				name:    PartitionsTableName,
				schema:  partitionSchema,
				catalog: cat,
				rowIter: emptyRowIter,
			},
			InnoDBTempTableName: &informationSchemaTable{
				name:    InnoDBTempTableName,
				schema:  innoDBTempTableSchema,
				catalog: cat,
				rowIter: innoDBTempTableIter,
			},
		},
	}
}

func viewRowIter(context *Context, catalog *Catalog) (RowIter, error) {
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
	return RowsToRowIter(rows...), nil
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
		return nil, ErrPartitionNotFound.New(partition.Key())
	}
	if t.rowIter == nil {
		return RowsToRowIter(), nil
	}

	return t.rowIter(ctx, t.catalog)
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
func (pit *informationSchemaPartitionIter) Close(_ *Context) error {
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

func getAutoIncrementValue(ctx *Context, t Table) (val interface{}) {
	for _, c := range t.Schema() {
		if c.AutoIncrement {
			val, _ = t.(AutoIncrementTable).PeekNextAutoIncrementValue(ctx)
			// ignore errors
			break
		}
	}
	return
}
