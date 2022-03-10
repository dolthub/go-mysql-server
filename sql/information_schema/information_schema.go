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

package information_schema

import (
	"bytes"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/dolthub/vitess/go/vt/sqlparser"

	. "github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/dolthub/go-mysql-server/sql/grant_tables"
	"github.com/dolthub/go-mysql-server/sql/parse"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

const (
	// InformationSchemaDatabaseName is the name of the information schema database.
	InformationSchemaDatabaseName = "information_schema"
	// AdministrableRoleAuthorizationsTableName is the name of the ADMINISTRABLE_ROLE_AUTHORIZATIONS table.
	AdministrableRoleAuthorizationsTableName = "administrable_role_authorizations"
	// ApplicableRolesTableName is the name of the APPLICABLE_ROLES table.
	ApplicableRolesTableName = "applicable_roles"
	// CharacterSetsTableName is the name of the CHARACTER_SETS table
	CharacterSetsTableName = "character_sets"
	// CheckConstraintsTableName is the name of CHECK_CONSTRAINTS table
	CheckConstraintsTableName = "check_constraints"
	// CollationCharSetApplicabilityTableName is the name of COLLATION_CHARACTER_SET_APPLICABILITY table.
	CollationCharSetApplicabilityTableName = "collation_character_set_applicability"
	// CollationsTableName is the name of the COLLATIONS table.
	CollationsTableName = "collations"
	// ColumnPrivilegesTableName is the name of the COLUMN_PRIVILEGES table.
	ColumnPrivilegesTableName = "column_privileges"
	// ColumnStatisticsTableName is the name of the COLUMN_STATISTICS table.
	ColumnStatisticsTableName = "column_statistics"
	// ColumnsTableName is the name of columns table.
	ColumnsTableName = "columns"
	// ColumnsExtensionsTableName is the name of the COLUMN_EXTENSIONS table.
	ColumnsExtensionsTableName = "columns_extensions"
	// ConnectionControlFailedLoginAttemptsTableName is the name of the CONNECTION_CONTROL_FAILED_LOGIN_ATTEMPTS.
	ConnectionControlFailedLoginAttemptsTableName = "connection_control_failed_login_attempts"
	// EnabledRolesTablesName is the name of the ENABLED_ROLES table.
	EnabledRolesTablesName = "enabled_roles"
	// EnginesTableName is the name of the ENGINES table
	EnginesTableName = "engines"
	// EventsTableName is the name of the EVENTS table.
	EventsTableName = "events"
	// FilesTableName is the name of the FILES table.
	FilesTableName = "files"
	// KeyColumnUsageTableName is the name of the KEY_COLUMN_USAGE table.
	KeyColumnUsageTableName = "key_column_usage"
	// KeywordsTableName is the name of the KEYWORDS table.
	KeywordsTableName = "keywords"
	// MysqlFirewallUsersTableName is the name of the MYSQL_FIREWALL_USERS table.
	MysqlFirewallUsersTableName = "mysql_firewall_users"
	// MysqlFirewallWhitelistTableName is the name of the MYSQL_FIREWALL_WHITELIST table.
	MysqlFirewallWhitelistTableName = "mysql_firewall_whitelist"
	// OptimizerTraceTableName is the name of the OPTIMIZER_TRACE table.
	OptimizerTraceTableName = "optimizer_trace"
	// PartitionsTableName is the name of the PARTITIONS table
	PartitionsTableName = "partitions"
	// PluginsTableName is the name of the PLUGINS table.
	PluginsTableName = "plugins"
	// ProcessListTableName is the name of PROCESSLIST table
	ProcessListTableName = "processlist"
	// ProfilingTableName is the name of PROFILING table.
	ProfilingTableName = "profiling"
	// ReferentialConstraintsTableName is the name of the TABLE_CONSTRAINTS table.
	ReferentialConstraintsTableName = "referential_constraints"
	// ResourceGroupsTableName is the name of the RESOURCE_GROUPS table.
	ResourceGroupsTableName = "resource_groups"
	// RoleColumnGrantsTableName is the name of the ROLE_COLUMNS_GRANTS table.
	RoleColumnGrantsTableName = "role_column_grants"
	// RoleRoutineGrantsTableName is the name of the ROLE_ROUTINE_GRANTS table.
	RoleRoutineGrantsTableName = "role_routine_grants"
	// RoleTableGrantsTableName is the name of the ROLE_TABLE_GRANTS table.
	RoleTableGrantsTableName = "role_table_grants"
	// RoutinesTableName is the name of the routines table.
	RoutinesTableName = "routines"
	// SchemaPrivilegesTableName is the name of the schema_privileges table.
	SchemaPrivilegesTableName = "schema_privileges"
	// SchemataTableName is the name of the SCHEMATA table.
	SchemataTableName = "schemata"
	// SchemataExtensionsTableName is the name of the SCHEMATA_EXTENSIONS table.
	SchemataExtensionsTableName = "schemata_extensions"
	// StGeometryColumnsTableName is the name of the ST_GEOMETRY_COLUMNS table.
	StGeometryColumnsTableName = "st_geometry_columns"
	// StSpatialReferenceSystemsTableName is the name of ST_SPATIAL_REFERENCE_SYSTEMS table.
	StSpatialReferenceSystemsTableName = "st_spatial_reference_systems"
	// StUnitsOfMeasureTableName is the name of the ST_UNITS_OF_MEASURE
	StUnitsOfMeasureTableName = "st_units_of_measure"
	// StatisticsTableName is the name of the STATISTICS table.
	StatisticsTableName = "statistics"
	// TableConstraintsTableName is the name of the TABLE_CONSTRAINTS table.
	TableConstraintsTableName = "table_constraints"
	// TableConstraintsExtensionsTableName is the name of the TABLE_CONSTRAINTS_EXTENSIONS table.
	TableConstraintsExtensionsTableName = "table_constraints_extensions"
	// TablePrivilegesTableName is the name of TABLE_PRIVILEGES table.
	TablePrivilegesTableName = "table_privileges"
	// TablesTableName is the name of TABLES table.
	TablesTableName = "tables"
	// TablesExtensionsTableName is the name of TABLE_EXTENSIONS table.
	TablesExtensionsTableName = "tables_extensions"
	// TablespacesTableName is the names of the TABLESPACES table.
	TablespacesTableName = "tablespaces"
	// TablespacesExtensionsTableName is the name of the TABLESPACES_EXTENSIONS table.
	TablespacesExtensionsTableName = "tablespaces_extensions"
	// TriggersTableName is the name of the TRIGGERS table.
	TriggersTableName = "triggers"
	// UserAttributesTableName is the name of the USER_ATTRIBUTES table.
	UserAttributesTableName = "user_attributes"
	// UserPrivilegesTableName is the name of the USER_PRIVILEGES table
	UserPrivilegesTableName = "user_privileges"
	// ViewRoutineUsageTableName is the name of VIEW_ROUTINE_USAGE table.
	ViewRoutineUsageTableName = "view_routine_usage"
	// ViewTableUsageTableName is the name of the VIEW_TABLE_USAGE table.
	ViewTableUsageTableName = "view_table_usage"
	// ViewsTableName is the name of the VIEWS table.
	ViewsTableName = "views"
)

var _ Database = (*informationSchemaDatabase)(nil)

type informationSchemaDatabase struct {
	name   string
	tables map[string]Table
}

type informationSchemaTable struct {
	name    string
	schema  Schema
	catalog Catalog
	rowIter func(*Context, Catalog) (RowIter, error)
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
	{Name: "maxlen", Type: Uint64, Default: nil, Nullable: false, Source: CharacterSetsTableName},
}

var enginesSchema = Schema{
	{Name: "engine", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: EnginesTableName},
	{Name: "support", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 8), Default: nil, Nullable: false, Source: EnginesTableName},
	{Name: "comment", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 80), Default: nil, Nullable: false, Source: EnginesTableName},
	{Name: "transactions", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 3), Default: nil, Nullable: true, Source: EnginesTableName},
	{Name: "xa", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 3), Default: nil, Nullable: true, Source: EnginesTableName},
	{Name: "savepoints", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 3), Default: nil, Nullable: true, Source: EnginesTableName},
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

var processListSchema = Schema{
	{Name: "id", Type: Int64, Default: nil, Nullable: false, Source: ProcessListTableName},
	{Name: "user", Type: LongText, Default: nil, Nullable: false, Source: ProcessListTableName},
	{Name: "host", Type: LongText, Default: nil, Nullable: false, Source: ProcessListTableName},
	{Name: "db", Type: LongText, Default: nil, Nullable: true, Source: ProcessListTableName},
	{Name: "command", Type: LongText, Default: nil, Nullable: false, Source: ProcessListTableName},
	{Name: "time", Type: Int64, Default: nil, Nullable: false, Source: ProcessListTableName},
	{Name: "state", Type: LongText, Default: nil, Nullable: true, Source: ProcessListTableName},
	{Name: "info", Type: LongText, Default: nil, Nullable: true, Source: ProcessListTableName},
}

var collationCharSetApplicabilitySchema = Schema{
	{Name: "collation_name", Type: LongText, Default: nil, Nullable: false, Source: CollationCharSetApplicabilityTableName},
	{Name: "character_set_name", Type: LongText, Default: nil, Nullable: false, Source: CollationCharSetApplicabilityTableName},
}

var administrableRoleAuthorizationsSchema = Schema{
	{Name: "user", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 97), Default: nil, Nullable: true, Source: AdministrableRoleAuthorizationsTableName},
	{Name: "host", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 256), Default: nil, Nullable: true, Source: AdministrableRoleAuthorizationsTableName},
	{Name: "grantee", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 97), Default: nil, Nullable: true, Source: AdministrableRoleAuthorizationsTableName},
	{Name: "grantee_host", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 256), Default: nil, Nullable: true, Source: AdministrableRoleAuthorizationsTableName},
	{Name: "role_name", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 255), Default: nil, Nullable: true, Source: AdministrableRoleAuthorizationsTableName},
	{Name: "role_host", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 256), Default: nil, Nullable: true, Source: AdministrableRoleAuthorizationsTableName},
	{Name: "is_grantable", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 3), Default: nil, Nullable: false, Source: AdministrableRoleAuthorizationsTableName},
	{Name: "is_default", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 3), Default: nil, Nullable: true, Source: AdministrableRoleAuthorizationsTableName},
	{Name: "is_mandatory", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 3), Default: nil, Nullable: false, Source: AdministrableRoleAuthorizationsTableName},
}

var applicableRolesSchema = Schema{
	{Name: "user", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 97), Default: nil, Nullable: true, Source: ApplicableRolesTableName},
	{Name: "host", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 256), Default: nil, Nullable: true, Source: ApplicableRolesTableName},
	{Name: "grantee", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 97), Default: nil, Nullable: true, Source: ApplicableRolesTableName},
	{Name: "grantee_host", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 256), Default: nil, Nullable: true, Source: ApplicableRolesTableName},
	{Name: "role_name", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 255), Default: nil, Nullable: true, Source: ApplicableRolesTableName},
	{Name: "role_host", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 256), Default: nil, Nullable: true, Source: ApplicableRolesTableName},
	{Name: "is_grantable", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 3), Default: nil, Nullable: false, Source: ApplicableRolesTableName},
	{Name: "is_default", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 3), Default: nil, Nullable: true, Source: ApplicableRolesTableName},
	{Name: "is_mandatory", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 3), Default: nil, Nullable: false, Source: ApplicableRolesTableName},
}

var columnPrivilegesSchema = Schema{
	{Name: "grantee", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 292), Default: nil, Nullable: false, Source: ColumnPrivilegesTableName},
	{Name: "table_catalog", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 512), Default: nil, Nullable: false, Source: ColumnPrivilegesTableName},
	{Name: "table_schema", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: ColumnPrivilegesTableName},
	{Name: "table_name", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: ColumnPrivilegesTableName},
	{Name: "column_name", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: ColumnPrivilegesTableName},
	{Name: "privilege_type", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: ColumnPrivilegesTableName},
	{Name: "is_grantable", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 3), Default: nil, Nullable: false, Source: ColumnPrivilegesTableName},
}

var columnExtensionsSchema = Schema{
	{Name: "table_catalog", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: ColumnsExtensionsTableName},
	{Name: "table_schema", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: ColumnsExtensionsTableName},
	{Name: "table_name", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: ColumnsExtensionsTableName},
	{Name: "column_name", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: ColumnsExtensionsTableName},
	{Name: "engine_attribute", Type: JSON, Default: nil, Nullable: true, Source: ColumnsExtensionsTableName},
	{Name: "secondary_engine_attribute", Type: JSON, Default: nil, Nullable: true, Source: ColumnsExtensionsTableName},
}

var connectionControlFailedLoginAttemptsSchema = Schema{
	{Name: "userhost", Type: LongText, Default: nil, Nullable: false, Source: ConnectionControlFailedLoginAttemptsTableName},
	{Name: "failed_attempts", Type: Uint64, Default: nil, Nullable: false, Source: ConnectionControlFailedLoginAttemptsTableName},
}

var enabledRolesSchema = Schema{
	{Name: "role_name", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 255), Default: nil, Nullable: true, Source: EnabledRolesTablesName},
	{Name: "role_host", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 255), Default: nil, Nullable: true, Source: EnabledRolesTablesName},
	{Name: "is_default", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 3), Default: nil, Nullable: true, Source: EnabledRolesTablesName},
	{Name: "is_mandatory", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 3), Default: nil, Nullable: false, Source: EnabledRolesTablesName},
}

var keywordsSchema = Schema{
	{Name: "word", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 128), Default: nil, Nullable: true, Source: KeywordsTableName},
	{Name: "reserved", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: KeywordsTableName},
}

var mysqlFirewallUsersSchema = Schema{
	{Name: "userhost", Type: LongText, Default: nil, Nullable: true, Source: MysqlFirewallUsersTableName},
	{Name: "mode", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: MysqlFirewallUsersTableName},
}

var mysqlFirewallWhitelistSchema = Schema{
	{Name: "userhost", Type: LongText, Default: nil, Nullable: true, Source: MysqlFirewallWhitelistTableName},
	{Name: "rule", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: MysqlFirewallWhitelistTableName},
}

var optimizerTraceSchema = Schema{
	{Name: "query", Type: Text, Default: nil, Nullable: false, Source: OptimizerTraceTableName},
	{Name: "trace", Type: Text, Default: nil, Nullable: false, Source: OptimizerTraceTableName},
	{Name: "missing_bytes_beyond_max_mem_size", Type: Int64, Default: nil, Nullable: false, Source: OptimizerTraceTableName},
	{Name: "insufficient_privileges", Type: MustCreateBitType(1), Default: nil, Nullable: false, Source: OptimizerTraceTableName},
}

var pluginsSchema = Schema{
	{Name: "plugin_name", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: PluginsTableName},
	{Name: "plugin_version", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 20), Default: nil, Nullable: true, Source: PluginsTableName},
	{Name: "plugin_status", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 10), Default: nil, Nullable: true, Source: PluginsTableName},
	{Name: "plugin_type", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 80), Default: nil, Nullable: true, Source: PluginsTableName},
	{Name: "plugin_type_version", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 20), Default: nil, Nullable: true, Source: PluginsTableName},
	{Name: "plugin_library", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: PluginsTableName},
	{Name: "plugin_library_version", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 20), Default: nil, Nullable: false, Source: PluginsTableName},
	{Name: "plugin_author", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: PluginsTableName},
	{Name: "plugin_description", Type: Text, Default: nil, Nullable: false, Source: PluginsTableName},
	{Name: "plugin_license", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 80), Default: nil, Nullable: false, Source: PluginsTableName},
	{Name: "load_option", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: PluginsTableName},
}

var profilingSchema = Schema{
	{Name: "query_id", Type: Int64, Default: nil, Nullable: false, Source: ProfilingTableName},
	{Name: "seq", Type: Int64, Default: nil, Nullable: false, Source: ProfilingTableName},
	{Name: "state", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 30), Default: nil, Nullable: false, Source: ProfilingTableName},
	{Name: "duration", Type: MustCreateDecimalType(DecimalTypeMaxPrecision, DecimalTypeMaxScale), Default: nil, Nullable: false, Source: ProfilingTableName},
	{Name: "cpu_user", Type: MustCreateDecimalType(DecimalTypeMaxPrecision, DecimalTypeMaxScale), Default: nil, Nullable: true, Source: ProfilingTableName},
	{Name: "cpu_system", Type: MustCreateDecimalType(DecimalTypeMaxPrecision, DecimalTypeMaxScale), Default: nil, Nullable: true, Source: ProfilingTableName},
	{Name: "context_voluntary", Type: Int64, Default: nil, Nullable: true, Source: ProfilingTableName},
	{Name: "context_involuntary", Type: Int64, Default: nil, Nullable: true, Source: ProfilingTableName},
	{Name: "block_ops_in", Type: Int64, Default: nil, Nullable: true, Source: ProfilingTableName},
	{Name: "block_ops_out", Type: Int64, Default: nil, Nullable: true, Source: ProfilingTableName},
	{Name: "messages_sent", Type: Int64, Default: nil, Nullable: true, Source: ProfilingTableName},
	{Name: "messages_received", Type: Int64, Default: nil, Nullable: true, Source: ProfilingTableName},
	{Name: "page_faults_major", Type: Int64, Default: nil, Nullable: true, Source: ProfilingTableName},
	{Name: "page_faults_minor", Type: Int64, Default: nil, Nullable: true, Source: ProfilingTableName},
	{Name: "swaps", Type: Int64, Default: nil, Nullable: true, Source: ProfilingTableName},
	{Name: "source_function", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 30), Default: nil, Nullable: true, Source: ProfilingTableName},
	{Name: "source_file", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 20), Default: nil, Nullable: true, Source: ProfilingTableName},
	{Name: "source_line", Type: Int64, Default: nil, Nullable: true, Source: ProfilingTableName},
}

var resourceGroupSchema = Schema{
	{Name: "resource_group_name", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: ResourceGroupsTableName},
	{Name: "resource_group_type", Type: MustCreateEnumType([]string{"SYSTEM", "USER"}, Collation_Default), Default: nil, Nullable: false, Source: ResourceGroupsTableName},
	{Name: "resource_group_enable", Type: MustCreateBitType(1), Default: nil, Nullable: false, Source: ResourceGroupsTableName},
	{Name: "vpcus_ids", Type: LongText, Default: nil, Nullable: true, Source: ResourceGroupsTableName},
	{Name: "thread_priority", Type: Int8, Default: nil, Nullable: false, Source: ResourceGroupsTableName},
}

var roleColumnGrantsSchema = Schema{
	{Name: "grantor", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 97), Default: nil, Nullable: true, Source: RoleColumnGrantsTableName},
	{Name: "grantor_host", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 256), Default: nil, Nullable: true, Source: RoleColumnGrantsTableName},
	{Name: "grantee", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 32), Default: nil, Nullable: false, Source: RoleColumnGrantsTableName},
	{Name: "grantee_host", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 255), Default: nil, Nullable: false, Source: RoleColumnGrantsTableName},
	{Name: "table_catalog", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 3), Default: nil, Nullable: false, Source: RoleColumnGrantsTableName},
	{Name: "table_schema", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: RoleColumnGrantsTableName},
	{Name: "table_name", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: RoleColumnGrantsTableName},
	{Name: "column_name", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: RoleColumnGrantsTableName},
	{Name: "privilege_type", Type: MustCreateSetType([]string{"Select", "Insert", "Update", "References"}, Collation_Default), Default: nil, Nullable: false, Source: RoleColumnGrantsTableName},
	{Name: "is_grantable", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 3), Default: nil, Nullable: false, Source: RoleColumnGrantsTableName},
}

var roleRoutineGrantsSchema = Schema{
	{Name: "grantor", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 97), Default: nil, Nullable: true, Source: RoleRoutineGrantsTableName},
	{Name: "grantor_host", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 256), Default: nil, Nullable: true, Source: RoleRoutineGrantsTableName},
	{Name: "grantee", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 32), Default: nil, Nullable: false, Source: RoleRoutineGrantsTableName},
	{Name: "grantee_host", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 255), Default: nil, Nullable: false, Source: RoleRoutineGrantsTableName},
	{Name: "specific_catalog", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 3), Default: nil, Nullable: false, Source: RoleRoutineGrantsTableName},
	{Name: "specific_schema", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: RoleRoutineGrantsTableName},
	{Name: "specific_name", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: RoleRoutineGrantsTableName},
	{Name: "routine_catalog", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 3), Default: nil, Nullable: false, Source: RoleRoutineGrantsTableName},
	{Name: "routine_schema", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: RoleRoutineGrantsTableName},
	{Name: "routine_name", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: RoleRoutineGrantsTableName},
	{Name: "privilege_type", Type: MustCreateSetType([]string{"Execute", "Alter Routine", "Grant"}, Collation_Default), Default: nil, Nullable: false, Source: RoleRoutineGrantsTableName},
	{Name: "is_grantable", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 3), Default: nil, Nullable: false, Source: RoleRoutineGrantsTableName},
}

var roleTableGrantsSchema = Schema{
	{Name: "grantor", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 97), Default: nil, Nullable: true, Source: RoleTableGrantsTableName},
	{Name: "grantor_host", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 256), Default: nil, Nullable: true, Source: RoleTableGrantsTableName},
	{Name: "grantee", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 32), Default: nil, Nullable: false, Source: RoleTableGrantsTableName},
	{Name: "grantee_host", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 255), Default: nil, Nullable: false, Source: RoleTableGrantsTableName},
	{Name: "table_catalog", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 3), Default: nil, Nullable: false, Source: RoleTableGrantsTableName},
	{Name: "table_schema", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: RoleTableGrantsTableName},
	{Name: "table_name", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: RoleTableGrantsTableName},
	{Name: "privilege_type", Type: MustCreateSetType([]string{"Select", "Insert", "Update", "Delete", "Create", "Drop", "Grant", "References", "Index", "Alter", "Create View", "Show view", "Trigger"}, Collation_Default), Default: nil, Nullable: false, Source: RoleTableGrantsTableName},
	{Name: "is_grantable", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 3), Default: nil, Nullable: false, Source: RoleTableGrantsTableName},
}

var schemaPrivilegesTableName = Schema{
	{Name: "grantee", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 292), Default: nil, Nullable: false, Source: SchemaPrivilegesTableName},
	{Name: "table_catalog", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 3), Default: nil, Nullable: false, Source: SchemaPrivilegesTableName},
	{Name: "table_schema", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: SchemaPrivilegesTableName},
	{Name: "privilege_type", Type: MustCreateSetType([]string{"Select", "Insert", "Update", "Delete", "Create", "Drop", "Grant", "References", "Index", "Alter", "Create View", "Show view", "Trigger"}, Collation_Default), Default: nil, Nullable: false, Source: SchemaPrivilegesTableName},
	{Name: "is_grantable", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 3), Default: nil, Nullable: false, Source: SchemaPrivilegesTableName},
}

var schemataExtensionTableName = Schema{
	{Name: "catalog_name", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: SchemataExtensionsTableName},
	{Name: "schema_name", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: SchemataExtensionsTableName},
	{Name: "options", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 256), Default: nil, Nullable: true, Source: SchemataExtensionsTableName},
}

var stGeometryColumnsSchema = Schema{
	{Name: "table_catalog", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: StGeometryColumnsTableName},
	{Name: "table_schema", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: StGeometryColumnsTableName},
	{Name: "table_name", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: StGeometryColumnsTableName},
	{Name: "column_name", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: StGeometryColumnsTableName},
	{Name: "srs_name", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: StGeometryColumnsTableName},
	{Name: "srs_id", Type: Uint64, Default: nil, Nullable: true, Source: StGeometryColumnsTableName},
	{Name: "geometry_type_name", Type: LongText, Default: nil, Nullable: true, Source: StGeometryColumnsTableName},
}

var stSpatialReferenceSystemsSchema = Schema{
	{Name: "srs_name", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 80), Default: nil, Nullable: false, Source: StSpatialReferenceSystemsTableName},
	{Name: "srs_id", Type: Uint64, Default: nil, Nullable: false, Source: StSpatialReferenceSystemsTableName},
	{Name: "organization", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 256), Default: nil, Nullable: true, Source: StSpatialReferenceSystemsTableName},
	{Name: "organization_coordsys_id", Type: Uint64, Default: nil, Nullable: true, Source: StSpatialReferenceSystemsTableName},
	{Name: "definition", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 4096), Default: nil, Nullable: false, Source: StSpatialReferenceSystemsTableName},
	{Name: "description", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 2048), Default: nil, Nullable: true, Source: StSpatialReferenceSystemsTableName},
}

var stUnitsOfMeasureSchema = Schema{
	{Name: "unit_name", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 255), Default: nil, Nullable: true, Source: StUnitsOfMeasureTableName},
	{Name: "unit_type", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 7), Default: nil, Nullable: true, Source: StUnitsOfMeasureTableName},
	{Name: "conversion_factor", Type: Float64, Default: nil, Nullable: true, Source: StUnitsOfMeasureTableName},
	{Name: "description", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 255), Default: nil, Nullable: true, Source: StUnitsOfMeasureTableName},
}

var tableConstraintsExtensionsSchema = Schema{
	{Name: "constraint_catalog", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: TableConstraintsExtensionsTableName},
	{Name: "constraint_schema", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: TableConstraintsExtensionsTableName},
	{Name: "constraint_name", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: TableConstraintsExtensionsTableName},
	{Name: "table_name", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: TableConstraintsExtensionsTableName},
	{Name: "engine_attribute", Type: JSON, Default: nil, Nullable: true, Source: TableConstraintsExtensionsTableName},
	{Name: "secondary_engine_attribute", Type: JSON, Default: nil, Nullable: true, Source: TableConstraintsExtensionsTableName},
}

var tablePrivilegesSchema = Schema{
	{Name: "grantee", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 292), Default: nil, Nullable: false, Source: TablePrivilegesTableName},
	{Name: "table_catalog", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 512), Default: nil, Nullable: false, Source: TablePrivilegesTableName},
	{Name: "table_schema", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: TablePrivilegesTableName},
	{Name: "table_name", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: TablePrivilegesTableName},
	{Name: "privilege_type", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: TablePrivilegesTableName},
	{Name: "is_grantable", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 3), Default: nil, Nullable: false, Source: TablePrivilegesTableName},
}

var tablesExtensionsSchema = Schema{
	{Name: "table_catalog", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: TablesExtensionsTableName},
	{Name: "table_schema", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: TablesExtensionsTableName},
	{Name: "table_name", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: TablesExtensionsTableName},
	{Name: "engine_attribute", Type: JSON, Default: nil, Nullable: true, Source: TablesExtensionsTableName},
	{Name: "secondary_engine_attribute", Type: JSON, Default: nil, Nullable: true, Source: TablesExtensionsTableName},
}

var tablespacesSchema = Schema{
	{Name: "tablespace_name", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: TablespacesTableName},
	{Name: "engine", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: TablespacesTableName},
	{Name: "tablespace_type", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: TablespacesTableName},
	{Name: "logfile_group_name", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: TablespacesTableName},
	{Name: "extent_size", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: TablespacesTableName},
	{Name: "autoextend_size", Type: Int64, Default: nil, Nullable: true, Source: TablespacesTableName},
	{Name: "maximum_size", Type: Int64, Default: nil, Nullable: true, Source: TablespacesTableName},
	{Name: "nodegroup_id", Type: Int64, Default: nil, Nullable: true, Source: TablespacesTableName},
	{Name: "tablespace_comment", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 2048), Default: nil, Nullable: true, Source: TablespacesTableName},
}

var tablespacesExtensionsSchema = Schema{
	{Name: "tablespace_name", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 268), Default: nil, Nullable: false, Source: TablespacesExtensionsTableName},
	{Name: "engine_attribute", Type: JSON, Default: nil, Nullable: true, Source: TablespacesExtensionsTableName},
}

var userAttributesSchema = Schema{
	{Name: "user", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 32), Default: nil, Nullable: false, Source: UserAttributesTableName},
	{Name: "host", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 255), Default: nil, Nullable: false, Source: UserAttributesTableName},
	{Name: "attribute", Type: LongText, Default: nil, Nullable: true, Source: UserAttributesTableName},
}

var viewRoutineUsageSchema = Schema{
	{Name: "table_catalog", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: ViewRoutineUsageTableName},
	{Name: "table_schema", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: ViewRoutineUsageTableName},
	{Name: "table_name", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: ViewRoutineUsageTableName},
	{Name: "specific_catalog", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: ViewRoutineUsageTableName},
	{Name: "specific_catalog", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: ViewRoutineUsageTableName},
	{Name: "specific_table", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: ViewRoutineUsageTableName},
}

var viewTableUsageSchema = Schema{
	{Name: "view_catalog", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: ViewTableUsageTableName},
	{Name: "view_schema", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: ViewTableUsageTableName},
	{Name: "view_name", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: ViewTableUsageTableName},
	{Name: "table_catalog", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: ViewTableUsageTableName},
	{Name: "table_schema", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: ViewTableUsageTableName},
	{Name: "table_name", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: ViewTableUsageTableName},
}

func tablesRowIter(ctx *Context, cat Catalog) (RowIter, error) {
	var rows []Row
	for _, db := range cat.AllDatabases(ctx) {
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

		if err != nil {
			return nil, err
		}

		views, err := viewsInDatabase(ctx, db)
		if err != nil {
			return nil, err
		}

		for _, view := range views {
			rows = append(rows, Row{
				"def",                      // table_catalog
				db.Name(),                  // table_schema
				view.Name,                  // table_name
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
	}

	return RowsToRowIter(rows...), nil
}

func columnsRowIter(ctx *Context, cat Catalog) (RowIter, error) {
	var rows []Row
	for _, db := range cat.AllDatabases(ctx) {
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

func schemataRowIter(ctx *Context, c Catalog) (RowIter, error) {
	dbs := c.AllDatabases(ctx)

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

func collationsRowIter(ctx *Context, c Catalog) (RowIter, error) {
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

func charsetRowIter(ctx *Context, c Catalog) (RowIter, error) {
	var rows []Row
	for _, c := range SupportedCharsets {
		rows = append(rows, Row{
			c.String(),
			c.DefaultCollation().String(),
			c.Description(),
			uint64(c.MaxLength()),
		})
	}
	return RowsToRowIter(rows...), nil
}

func statisticsRowIter(ctx *Context, c Catalog) (RowIter, error) {
	var rows []Row
	dbs := c.AllDatabases(ctx)

	for _, db := range dbs {
		tableNames, tErr := db.GetTableNames(ctx)
		if tErr != nil {
			return nil, tErr
		}

		for _, tableName := range tableNames {
			tbl, _, err := c.Table(ctx, db.Name(), tableName)
			if err != nil {
				return nil, err
			}

			indexTable, ok := tbl.(IndexedTable)
			if ok {
				indexes, iErr := indexTable.GetIndexes(ctx)
				if iErr != nil {
					return nil, iErr
				}

				for _, index := range indexes {
					var (
						nonUnique    int
						indexComment string
						indexName    string
						comment      = ""
						isVisible    string
					)
					indexName = index.ID()
					if index.IsUnique() {
						nonUnique = 1
					} else {
						nonUnique = 0
					}
					indexType := index.IndexType()
					indexComment = index.Comment()
					// setting `VISIBLE` is not supported, so defaulting it to "YES"
					isVisible = "YES"

					// Create a Row for each column this index refers too.
					i := 0
					for _, expr := range index.Expressions() {
						col := plan.GetColumnFromIndexExpr(expr, tbl)
						if col != nil {
							i += 1
							var (
								collation string
								nullable  string

								cardinality uint64
							)

							seqInIndex := i
							colName := strings.Replace(col.Name, "`", "", -1) // get rid of backticks

							// collation is "A" for ASC ; "D" for DESC ; "NULL" for not sorted
							collation = "A"

							// TODO : cardinality should be an estimate of the number of unique values in the index.
							// it is currently set to total number of rows in the table
							if st, ok := tbl.(StatisticsTable); ok {
								cardinality, err = st.NumRows(ctx)
								if err != nil {
									return nil, err
								}
							}

							// if nullable, 'YES'; if not, ''
							if col.Nullable {
								nullable = "YES"
							} else {
								nullable = ""
							}

							rows = append(rows, Row{
								"def",        // table_catalog
								db.Name(),    // table_schema
								tbl.Name(),   // table_name
								nonUnique,    // non_unique		NOT NULL
								db.Name(),    // index_schema
								indexName,    // index_name
								seqInIndex,   // seq_in_index	NOT NULL
								colName,      // column_name
								collation,    // collation
								cardinality,  // cardinality
								nil,          // sub_part
								nil,          // packed
								nullable,     // is_nullable	NOT NULL
								indexType,    // index_type		NOT NULL
								comment,      // comment		NOT NULL
								indexComment, // index_comment	NOT NULL
								isVisible,    // is_visible		NOT NULL
								nil,          // expression
							})
						}
					}
				}
			}
		}
	}

	return RowsToRowIter(rows...), nil
}

func engineRowIter(ctx *Context, c Catalog) (RowIter, error) {
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

func triggersRowIter(ctx *Context, c Catalog) (RowIter, error) {
	var rows []Row
	for _, db := range c.AllDatabases(ctx) {
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

func checkConstraintsRowIter(ctx *Context, c Catalog) (RowIter, error) {
	var rows []Row
	for _, db := range c.AllDatabases(ctx) {
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

func tableConstraintRowIter(ctx *Context, c Catalog) (RowIter, error) {
	var rows []Row
	for _, db := range c.AllDatabases(ctx) {
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

func keyColumnConstraintRowIter(ctx *Context, c Catalog) (RowIter, error) {
	var rows []Row
	for _, db := range c.AllDatabases(ctx) {
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

// processListRowIter returns info on all processes in the session
func processListRowIter(ctx *Context, c Catalog) (RowIter, error) {
	processes := ctx.ProcessList.Processes()
	var rows = make([]Row, len(processes))

	db := ctx.GetCurrentDatabase()
	if db == "" {
		db = "NULL"
	}

	for i, proc := range processes {
		var status []string
		for name, progress := range proc.Progress {
			status = append(status, fmt.Sprintf("%s(%s)", name, progress))
		}
		if len(status) == 0 {
			status = []string{"running"}
		}
		sort.Strings(status)
		rows[i] = Row{
			int64(proc.Connection),       // id
			proc.User,                    // user
			ctx.Session.Client().Address, // host
			db,                           // db
			"Query",                      // command
			int64(proc.Seconds()),        // time
			strings.Join(status, ", "),   // state
			proc.Query,                   // info
		}
	}

	return RowsToRowIter(rows...), nil
}

func collationCharSetApplicabilityRowIter(ctx *Context, c Catalog) (RowIter, error) {
	var rows []Row
	for cName := range CollationToMySQLVals {
		c := Collations[cName]
		rows = append(rows, Row{
			c.String(),
			c.CharacterSet().String(),
		})
	}
	return RowsToRowIter(rows...), nil
}

func emptyRowIter(ctx *Context, c Catalog) (RowIter, error) {
	return RowsToRowIter(), nil
}

// NewInformationSchemaDatabase creates a new INFORMATION_SCHEMA Database.
func NewInformationSchemaDatabase() Database {
	return &informationSchemaDatabase{
		name: InformationSchemaDatabaseName,
		tables: map[string]Table{
			FilesTableName: &informationSchemaTable{
				name:   FilesTableName,
				schema: filesSchema,
			},
			ColumnStatisticsTableName: &informationSchemaTable{
				name:   ColumnStatisticsTableName,
				schema: columnStatisticsSchema,
			},
			TablesTableName: &informationSchemaTable{
				name:    TablesTableName,
				schema:  tablesSchema,
				rowIter: tablesRowIter,
			},
			ColumnsTableName: &informationSchemaTable{
				name:    ColumnsTableName,
				schema:  columnsSchema,
				rowIter: columnsRowIter,
			},
			SchemataTableName: &informationSchemaTable{
				name:    SchemataTableName,
				schema:  schemataSchema,
				rowIter: schemataRowIter,
			},
			CollationsTableName: &informationSchemaTable{
				name:    CollationsTableName,
				schema:  collationsSchema,
				rowIter: collationsRowIter,
			},
			CharacterSetsTableName: &informationSchemaTable{
				name:    CharacterSetsTableName,
				schema:  characterSetSchema,
				rowIter: charsetRowIter,
			},
			StatisticsTableName: &informationSchemaTable{
				name:    StatisticsTableName,
				schema:  statisticsSchema,
				rowIter: statisticsRowIter,
			},
			TableConstraintsTableName: &informationSchemaTable{
				name:    TableConstraintsTableName,
				schema:  tableConstraintsSchema,
				rowIter: tableConstraintRowIter,
			},
			ReferentialConstraintsTableName: &informationSchemaTable{
				name:    ReferentialConstraintsTableName,
				schema:  referentialConstraintsSchema,
				rowIter: emptyRowIter,
			},
			KeyColumnUsageTableName: &informationSchemaTable{
				name:    KeyColumnUsageTableName,
				schema:  keyColumnUsageSchema,
				rowIter: keyColumnConstraintRowIter,
			},
			TriggersTableName: &informationSchemaTable{
				name:    TriggersTableName,
				schema:  triggersSchema,
				rowIter: triggersRowIter,
			},
			EventsTableName: &informationSchemaTable{
				name:    EventsTableName,
				schema:  eventsSchema,
				rowIter: emptyRowIter,
			},
			RoutinesTableName: &routineTable{
				name:    RoutinesTableName,
				schema:  routinesSchema,
				rowIter: routinesRowIter,
			},
			ViewsTableName: &informationSchemaTable{
				name:    ViewsTableName,
				schema:  viewsSchema,
				rowIter: viewRowIter,
			},
			UserPrivilegesTableName: &informationSchemaTable{
				name:    UserPrivilegesTableName,
				schema:  userPrivilegesSchema,
				rowIter: emptyRowIter,
			},
			EnginesTableName: &informationSchemaTable{
				name:    EnginesTableName,
				schema:  enginesSchema,
				rowIter: engineRowIter,
			},
			CheckConstraintsTableName: &informationSchemaTable{
				name:    CheckConstraintsTableName,
				schema:  checkConstraintsSchema,
				rowIter: checkConstraintsRowIter,
			},
			PartitionsTableName: &informationSchemaTable{
				name:    PartitionsTableName,
				schema:  partitionSchema,
				rowIter: emptyRowIter,
			},
			ProcessListTableName: &informationSchemaTable{
				name:    ProcessListTableName,
				schema:  processListSchema,
				rowIter: processListRowIter,
			},
			CollationCharSetApplicabilityTableName: &informationSchemaTable{
				name:    CollationCharSetApplicabilityTableName,
				schema:  collationCharSetApplicabilitySchema,
				rowIter: collationCharSetApplicabilityRowIter,
			},
			AdministrableRoleAuthorizationsTableName: &informationSchemaTable{
				name:    AdministrableRoleAuthorizationsTableName,
				schema:  administrableRoleAuthorizationsSchema,
				rowIter: emptyRowIter,
			},
			ApplicableRolesTableName: &informationSchemaTable{
				name:    ApplicableRolesTableName,
				schema:  applicableRolesSchema,
				rowIter: emptyRowIter,
			},
			ColumnPrivilegesTableName: &informationSchemaTable{
				name:    ColumnPrivilegesTableName,
				schema:  columnPrivilegesSchema,
				rowIter: emptyRowIter,
			},
			ColumnsExtensionsTableName: &informationSchemaTable{
				name:    ColumnsExtensionsTableName,
				schema:  columnExtensionsSchema,
				rowIter: emptyRowIter,
			},
			ConnectionControlFailedLoginAttemptsTableName: &informationSchemaTable{
				name:    ConnectionControlFailedLoginAttemptsTableName,
				schema:  connectionControlFailedLoginAttemptsSchema,
				rowIter: emptyRowIter,
			},
			EnabledRolesTablesName: &informationSchemaTable{
				name:    EnabledRolesTablesName,
				schema:  enabledRolesSchema,
				rowIter: emptyRowIter,
			},
			KeywordsTableName: &informationSchemaTable{
				name:    KeywordsTableName,
				schema:  keywordsSchema,
				rowIter: emptyRowIter,
			},
			MysqlFirewallUsersTableName: &informationSchemaTable{
				name:    MysqlFirewallUsersTableName,
				schema:  mysqlFirewallUsersSchema,
				rowIter: emptyRowIter,
			},
			MysqlFirewallWhitelistTableName: &informationSchemaTable{
				name:    MysqlFirewallUsersTableName,
				schema:  mysqlFirewallWhitelistSchema,
				rowIter: emptyRowIter,
			},
			OptimizerTraceTableName: &informationSchemaTable{
				name:    OptimizerTraceTableName,
				schema:  optimizerTraceSchema,
				rowIter: emptyRowIter,
			},
			PluginsTableName: &informationSchemaTable{
				name:    PluginsTableName,
				schema:  pluginsSchema,
				rowIter: emptyRowIter,
			},
			ProfilingTableName: &informationSchemaTable{
				name:    ProfilingTableName,
				schema:  profilingSchema,
				rowIter: emptyRowIter,
			},
			ResourceGroupsTableName: &informationSchemaTable{
				name:    ResourceGroupsTableName,
				schema:  resourceGroupSchema,
				rowIter: emptyRowIter,
			},
			RoleColumnGrantsTableName: &informationSchemaTable{
				name:    RoleColumnGrantsTableName,
				schema:  roleColumnGrantsSchema,
				rowIter: emptyRowIter,
			},
			RoleRoutineGrantsTableName: &informationSchemaTable{
				name:    RoleRoutineGrantsTableName,
				schema:  roleRoutineGrantsSchema,
				rowIter: emptyRowIter,
			},
			RoleTableGrantsTableName: &informationSchemaTable{
				name:    RoleTableGrantsTableName,
				schema:  roleTableGrantsSchema,
				rowIter: emptyRowIter,
			},
			SchemaPrivilegesTableName: &informationSchemaTable{
				name:    SchemaPrivilegesTableName,
				schema:  schemaPrivilegesTableName,
				rowIter: emptyRowIter,
			},
			SchemataExtensionsTableName: &informationSchemaTable{
				name:    SchemataExtensionsTableName,
				schema:  schemataExtensionTableName,
				rowIter: emptyRowIter,
			},
			StGeometryColumnsTableName: &informationSchemaTable{
				name:    StGeometryColumnsTableName,
				schema:  stGeometryColumnsSchema,
				rowIter: emptyRowIter,
			},
			StSpatialReferenceSystemsTableName: &informationSchemaTable{
				name:    StSpatialReferenceSystemsTableName,
				schema:  stSpatialReferenceSystemsSchema,
				rowIter: emptyRowIter,
			},
			StUnitsOfMeasureTableName: &informationSchemaTable{
				name:    StUnitsOfMeasureTableName,
				schema:  stUnitsOfMeasureSchema,
				rowIter: emptyRowIter,
			},
			TableConstraintsExtensionsTableName: &informationSchemaTable{
				name:    RoleColumnGrantsTableName,
				schema:  tableConstraintsExtensionsSchema,
				rowIter: emptyRowIter,
			},
			TablePrivilegesTableName: &informationSchemaTable{
				name:    TablePrivilegesTableName,
				schema:  tablePrivilegesSchema,
				rowIter: emptyRowIter,
			},
			TablesExtensionsTableName: &informationSchemaTable{
				name:    TablesExtensionsTableName,
				schema:  tablesExtensionsSchema,
				rowIter: emptyRowIter,
			},
			TablespacesTableName: &informationSchemaTable{
				name:    TablespacesTableName,
				schema:  tablespacesSchema,
				rowIter: emptyRowIter,
			},
			TablespacesExtensionsTableName: &informationSchemaTable{
				name:    TablespacesExtensionsTableName,
				schema:  tablespacesExtensionsSchema,
				rowIter: emptyRowIter,
			},
			UserAttributesTableName: &informationSchemaTable{
				name:    UserAttributesTableName,
				schema:  userAttributesSchema,
				rowIter: emptyRowIter,
			},
			ViewRoutineUsageTableName: &informationSchemaTable{
				name:    ViewRoutineUsageTableName,
				schema:  viewRoutineUsageSchema,
				rowIter: emptyRowIter,
			},
			ViewTableUsageTableName: &informationSchemaTable{
				name:    ViewTableUsageTableName,
				schema:  viewTableUsageSchema,
				rowIter: emptyRowIter,
			},
			InnoDBBufferPageName: &informationSchemaTable{
				name:    InnoDBBufferPageName,
				schema:  innoDBBufferPageSchema,
				rowIter: emptyRowIter,
			},
			InnoDBBufferPageLRUName: &informationSchemaTable{
				name:    InnoDBBufferPageLRUName,
				schema:  innoDBBufferPageLRUSchema,
				rowIter: emptyRowIter,
			},
			InnoDBBufferPoolStatsName: &informationSchemaTable{
				name:    InnoDBBufferPoolStatsName,
				schema:  innoDBBufferPoolStatsSchema,
				rowIter: emptyRowIter,
			},
			InnoDBCachedIndexesName: &informationSchemaTable{
				name:    InnoDBCachedIndexesName,
				schema:  innoDBCachedIndexesSchema,
				rowIter: emptyRowIter,
			},
			InnoDBCmpName: &informationSchemaTable{
				name:    InnoDBCmpName,
				schema:  innoDBCmpSchema,
				rowIter: emptyRowIter,
			},
			InnoDBCmpResetName: &informationSchemaTable{
				name:    InnoDBCmpResetName,
				schema:  innoDBCmpResetSchema,
				rowIter: emptyRowIter,
			},
			InnoDBCmpmemName: &informationSchemaTable{
				name:    InnoDBCmpmemName,
				schema:  innoDBCmpmemSchema,
				rowIter: emptyRowIter,
			},
			InnoDBCmpmemResetName: &informationSchemaTable{
				name:    InnoDBCmpmemResetName,
				schema:  innoDBCmpmemResetSchema,
				rowIter: emptyRowIter,
			},
			InnoDBCmpPerIndexName: &informationSchemaTable{
				name:    InnoDBCmpPerIndexName,
				schema:  innoDBCmpPerIndexSchema,
				rowIter: emptyRowIter,
			},
			InnoDBCmpPerIndexResetName: &informationSchemaTable{
				name:    InnoDBCmpPerIndexResetName,
				schema:  innoDBCmpPerIndexResetSchema,
				rowIter: emptyRowIter,
			},
			InnoDBColumnsName: &informationSchemaTable{
				name:    InnoDBColumnsName,
				schema:  innoDBColumnsSchema,
				rowIter: emptyRowIter,
			},
			InnoDBDatafilesName: &informationSchemaTable{
				name:    InnoDBDatafilesName,
				schema:  innoDBDatafilesSchema,
				rowIter: emptyRowIter,
			},
			InnoDBFieldsName: &informationSchemaTable{
				name:    InnoDBFieldsName,
				schema:  innoDBFieldsSchema,
				rowIter: emptyRowIter,
			},
			InnoDBForeignName: &informationSchemaTable{
				name:    InnoDBForeignName,
				schema:  innoDBForeignSchema,
				rowIter: emptyRowIter,
			},
			InnoDBForeignColsName: &informationSchemaTable{
				name:    InnoDBForeignColsName,
				schema:  innoDBForeignColsSchema,
				rowIter: emptyRowIter,
			},
			InnoDBFtBeingDeletedName: &informationSchemaTable{
				name:    InnoDBFtBeingDeletedName,
				schema:  innoDBFtBeingDeletedSchema,
				rowIter: emptyRowIter,
			},
			InnoDBFtConfigName: &informationSchemaTable{
				name:    InnoDBFtConfigName,
				schema:  innoDBFtConfigSchema,
				rowIter: emptyRowIter,
			},
			InnoDBFtDefaultStopwordName: &informationSchemaTable{
				name:    InnoDBFtDefaultStopwordName,
				schema:  innoDBFtDefaultStopwordSchema,
				rowIter: emptyRowIter,
			},
			InnoDBFtDeletedName: &informationSchemaTable{
				name:    InnoDBFtDeletedName,
				schema:  innoDBFtDeletedSchema,
				rowIter: emptyRowIter,
			},
			InnoDBFtIndexCacheName: &informationSchemaTable{
				name:    InnoDBFtIndexCacheName,
				schema:  innoDBFtIndexCacheSchema,
				rowIter: emptyRowIter,
			},
			InnoDBFtIndexTableName: &informationSchemaTable{
				name:    InnoDBFtIndexTableName,
				schema:  innoDBFtIndexTableSchema,
				rowIter: emptyRowIter,
			},
			InnoDBIndexesName: &informationSchemaTable{
				name:    InnoDBIndexesName,
				schema:  innoDBIndexesSchema,
				rowIter: emptyRowIter,
			},
			InnoDBMetricsName: &informationSchemaTable{
				name:    InnoDBMetricsName,
				schema:  innoDBMetricsSchema,
				rowIter: emptyRowIter,
			},
			InnoDBSessionTempTablespacesName: &informationSchemaTable{
				name:    InnoDBSessionTempTablespacesName,
				schema:  innoDBSessionTempTablespacesSchema,
				rowIter: emptyRowIter,
			},
			InnoDBTablesName: &informationSchemaTable{
				name:    InnoDBTablesName,
				schema:  innoDBTablesSchema,
				rowIter: emptyRowIter,
			},
			InnoDBTablespacesName: &informationSchemaTable{
				name:    InnoDBTablespacesName,
				schema:  innoDBTablespacesSchema,
				rowIter: emptyRowIter,
			},
			InnoDBTablespacesBriefName: &informationSchemaTable{
				name:    InnoDBTablespacesBriefName,
				schema:  innoDBTablespacesBriefSchema,
				rowIter: emptyRowIter,
			},
			InnoDBTablestatsName: &informationSchemaTable{
				name:    InnoDBTablestatsName,
				schema:  innoDBTablestatsSchema,
				rowIter: emptyRowIter,
			},
			InnoDBTempTableInfoName: &informationSchemaTable{
				name:    InnoDBTempTableInfoName,
				schema:  innoDBTempTableSchema,
				rowIter: innoDBTempTableRowIter,
			},
			InnoDBTrxName: &informationSchemaTable{
				name:    InnoDBTrxName,
				schema:  innoDBTrxSchema,
				rowIter: emptyRowIter,
			},
			InnoDBVirtualName: &informationSchemaTable{
				name:    InnoDBVirtualName,
				schema:  innoDBVirtualSchema,
				rowIter: emptyRowIter,
			},
		},
	}
}

func viewRowIter(ctx *Context, catalog Catalog) (RowIter, error) {
	var rows []Row
	for _, db := range catalog.AllDatabases(ctx) {
		dbName := db.Name()

		views, err := viewsInDatabase(ctx, db)
		if err != nil {
			return nil, err
		}

		for _, view := range views {
			rows = append(rows, Row{
				"def",
				dbName,
				view.Name,
				view.TextDefinition,
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

func routinesRowIter(ctx *Context, c Catalog, p []*plan.Procedure) (RowIter, error) {
	var rows []Row
	var (
		securityType    = "DEFINER"
		isDeterministic = ""    // YES or NO
		sqlMode         = "SQL" // SQL, NO SQL, READS SQL DATA, or MODIFIES SQL DATA.
	)

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

	for _, procedure := range p {
		if procedure.SecurityContext == plan.ProcedureSecurityContext_Invoker {
			securityType = "INVOKER"
		}
		rows = append(rows, Row{
			procedure.Name,             // specific_name NOT NULL
			"def",                      // routine_catalog
			"sys",                      // routine_schema
			procedure.Name,             // routine_name NOT NULL
			"PROCEDURE",                // routine_type NOT NULL
			"",                         // data_type
			nil,                        // character_maximum_length
			nil,                        // character_octet_length
			nil,                        // numeric_precision
			nil,                        // numeric_scale
			nil,                        // datetime_precision
			nil,                        // character_set_name
			nil,                        // collation_name
			"",                         // dtd_identifier
			"SQL",                      // routine_body NOT NULL
			procedure.Body.String(),    // routine_definition
			nil,                        // external_name
			"SQL",                      // external_language NOT NULL
			"SQL",                      // parameter_style NOT NULL
			isDeterministic,            // is_deterministic NOT NULL
			"",                         // sql_data_access NOT NULL
			nil,                        // sql_path
			securityType,               // security_type NOT NULL
			procedure.CreatedAt.UTC(),  // created NOT NULL
			procedure.ModifiedAt.UTC(), // last_altered NOT NULL
			sqlMode,                    // sql_mode NOT NULL
			procedure.Comment,          // routine_comment NOT NULL
			procedure.Definer,          // definer NOT NULL
			characterSetClient,         // character_set_client NOT NULL
			collationConnection,        // collation_connection NOT NULL
			collationServer,            // database_collation NOT NULL
		})
	}

	// TODO: need to add FUNCTIONS routine_type

	return RowsToRowIter(rows...), nil
}

// viewsInDatabase returns all views defined on the database given, consulting both the database itself as well as any
// views defined in session memory. Typically there will not be both types of views on a single database, but the
// interfaces do make it possible.
func viewsInDatabase(ctx *Context, db Database) ([]ViewDefinition, error) {
	var views []ViewDefinition
	dbName := db.Name()

	if privilegedDatabase, ok := db.(grant_tables.PrivilegedDatabase); ok {
		db = privilegedDatabase.Unwrap()
	}
	if vdb, ok := db.(ViewDatabase); ok {
		dbViews, err := vdb.AllViews(ctx)
		if err != nil {
			return nil, err
		}

		for _, view := range dbViews {
			views = append(views, view)
		}
	}

	for _, view := range ctx.GetViewRegistry().ViewsInDatabase(dbName) {
		views = append(views, ViewDefinition{
			Name:           view.Name(),
			TextDefinition: view.TextDefinition(),
		})
	}

	return views, nil
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

func (t *informationSchemaTable) AssignCatalog(cat Catalog) Table {
	t.catalog = cat
	return t
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
	if t.catalog == nil {
		return nil, fmt.Errorf("nil catalog for info schema table %s", t.name)
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
func (pit *informationSchemaPartitionIter) Next(ctx *Context) (Partition, error) {
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
