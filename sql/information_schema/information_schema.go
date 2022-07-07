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

	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/dolthub/vitess/go/vt/sqlparser"

	. "github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/mysql_db"
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
	{Name: "FILE_ID", Type: Int64, Source: FilesTableName, Nullable: true},
	{Name: "FILE_NAME", Type: LongText, Source: FilesTableName, Nullable: true},
	{Name: "FILE_TYPE", Type: LongText, Source: FilesTableName, Nullable: true},
	{Name: "TABLESPACE_NAME", Type: LongText, Source: FilesTableName},
	{Name: "TABLE_CATALOG", Type: LongText, Source: FilesTableName},
	{Name: "TABLE_SCHEMA", Type: LongBlob, Source: FilesTableName, Nullable: true},
	{Name: "TABLE_NAME", Type: LongBlob, Source: FilesTableName, Nullable: true},
	{Name: "LOGFILE_GROUP_NAME", Type: LongText, Source: FilesTableName, Nullable: true},
	{Name: "LOGFILE_GROUP_NUMBER", Type: Int64, Source: FilesTableName, Nullable: true},
	{Name: "ENGINE", Type: LongText, Source: FilesTableName},
	{Name: "FULLTEXT_KEYS", Type: LongBlob, Source: FilesTableName, Nullable: true},
	{Name: "DELETED_ROWS", Type: LongBlob, Source: FilesTableName, Nullable: true},
	{Name: "UPDATE_COUNT", Type: LongBlob, Source: FilesTableName, Nullable: true},
	{Name: "FREE_EXTENTS", Type: Int64, Source: FilesTableName, Nullable: true},
	{Name: "TOTAL_EXTENTS", Type: Int64, Source: FilesTableName, Nullable: true},
	{Name: "EXTENT_SIZE", Type: Int64, Source: FilesTableName, Nullable: true},
	{Name: "INITIAL_SIZE", Type: Int64, Source: FilesTableName, Nullable: true},
	{Name: "MAXIMUM_SIZE", Type: Int64, Source: FilesTableName, Nullable: true},
	{Name: "AUTOEXTEND_SIZE", Type: Int64, Source: FilesTableName, Nullable: true},
	{Name: "CREATION_TIME", Type: LongBlob, Source: FilesTableName, Nullable: true},
	{Name: "LAST_UPDATE_TIME", Type: LongBlob, Source: FilesTableName, Nullable: true},
	{Name: "LAST_ACCESS_TIME", Type: LongBlob, Source: FilesTableName, Nullable: true},
	{Name: "RECOVER_TIME", Type: LongBlob, Source: FilesTableName, Nullable: true},
	{Name: "TRANSACTION_COUNTER", Type: LongBlob, Source: FilesTableName, Nullable: true},
	{Name: "VERSION", Type: Int64, Source: FilesTableName, Nullable: true},
	{Name: "ROW_FORMAT", Type: LongText, Source: FilesTableName, Nullable: true},
	{Name: "TABLE_ROWS", Type: LongBlob, Source: FilesTableName, Nullable: true},
	{Name: "AVG_ROW_LENGTH", Type: LongBlob, Source: FilesTableName, Nullable: true},
	{Name: "DATA_LENGTH", Type: LongBlob, Source: FilesTableName, Nullable: true},
	{Name: "MAX_DATA_LENGTH", Type: LongBlob, Source: FilesTableName, Nullable: true},
	{Name: "INDEX_LENGTH", Type: LongBlob, Source: FilesTableName, Nullable: true},
	{Name: "DATA_FREE", Type: Int64, Source: FilesTableName, Nullable: true},
	{Name: "CREATE_TIME", Type: LongBlob, Source: FilesTableName, Nullable: true},
	{Name: "UPDATE_TIME", Type: LongBlob, Source: FilesTableName, Nullable: true},
	{Name: "CHECK_TIME", Type: LongBlob, Source: FilesTableName, Nullable: true},
	{Name: "CHECKSUM", Type: LongBlob, Source: FilesTableName, Nullable: true},
	{Name: "STATUS", Type: LongText, Source: FilesTableName, Nullable: true},
	{Name: "EXTRA", Type: LongBlob, Source: FilesTableName, Nullable: true},
}

var columnStatisticsSchema = Schema{
	{Name: "SCHEMA_NAME", Type: LongText, Source: ColumnStatisticsTableName},
	{Name: "TABLE_NAME", Type: LongText, Source: ColumnStatisticsTableName},
	{Name: "COLUMN_NAME", Type: LongText, Source: ColumnStatisticsTableName},
	{Name: "MEAN", Type: Float64, Source: ColumnStatisticsTableName},
	{Name: "MIN", Type: Float64, Source: ColumnStatisticsTableName},
	{Name: "MAX", Type: Float64, Source: ColumnStatisticsTableName},
	{Name: "COUNT", Type: Uint64, Source: ColumnStatisticsTableName},
	{Name: "NULL_COUNT", Type: Uint64, Source: ColumnStatisticsTableName},
	{Name: "DISTINCT_COUNT", Type: Uint64, Source: ColumnStatisticsTableName},
	{Name: "BUCKETS", Type: LongText, Source: ColumnStatisticsTableName},
	// TODO: mysql just has histogram
	//{Name: "HISTOGRAM", Type: JSON, Source: ColumnStatisticsTableName},

}

var tablesSchema = Schema{
	{Name: "TABLE_CATALOG", Type: LongText, Default: parse.MustStringToColumnDefaultValue(NewEmptyContext(), `""`, LongText, false), Nullable: false, Source: TablesTableName},
	{Name: "TABLE_SCHEMA", Type: LongText, Default: parse.MustStringToColumnDefaultValue(NewEmptyContext(), `""`, LongText, false), Nullable: false, Source: TablesTableName},
	{Name: "TABLE_NAME", Type: LongText, Default: parse.MustStringToColumnDefaultValue(NewEmptyContext(), `""`, LongText, false), Nullable: false, Source: TablesTableName},
	{Name: "TABLE_TYPE", Type: LongText, Default: parse.MustStringToColumnDefaultValue(NewEmptyContext(), `""`, LongText, false), Nullable: false, Source: TablesTableName},
	{Name: "ENGINE", Type: LongText, Default: nil, Nullable: true, Source: TablesTableName},
	{Name: "VERSION", Type: Uint64, Default: nil, Nullable: true, Source: TablesTableName},
	{Name: "ROW_FORMAT", Type: LongText, Default: nil, Nullable: true, Source: TablesTableName},
	{Name: "TABLE_ROWS", Type: Uint64, Default: nil, Nullable: true, Source: TablesTableName},
	{Name: "AVG_ROW_LENGTH", Type: Uint64, Default: nil, Nullable: true, Source: TablesTableName},
	{Name: "DATA_LENGTH", Type: Uint64, Default: nil, Nullable: true, Source: TablesTableName},
	{Name: "MAX_DATA_LENGTH", Type: Uint64, Default: nil, Nullable: true, Source: TablesTableName},
	{Name: "INDEX_LENGTH", Type: Uint64, Default: nil, Nullable: true, Source: TablesTableName},
	{Name: "DATA_FREE", Type: Uint64, Default: nil, Nullable: true, Source: TablesTableName},
	{Name: "AUTO_INCREMENT", Type: Uint64, Default: nil, Nullable: true, Source: TablesTableName},
	{Name: "CREATE_TIME", Type: Timestamp, Default: nil, Nullable: true, Source: TablesTableName},
	{Name: "UPDATE_TIME", Type: Timestamp, Default: nil, Nullable: true, Source: TablesTableName},
	{Name: "CHECK_TIME", Type: Timestamp, Default: nil, Nullable: true, Source: TablesTableName},
	{Name: "TABLE_COLLATION", Type: LongText, Default: nil, Nullable: true, Source: TablesTableName},
	{Name: "CHECKSUM", Type: Uint64, Default: nil, Nullable: true, Source: TablesTableName},
	{Name: "CREATE_OPTIONS", Type: LongText, Default: nil, Nullable: true, Source: TablesTableName},
	{Name: "TABLE_COMMENT", Type: LongText, Default: parse.MustStringToColumnDefaultValue(NewEmptyContext(), `""`, LongText, false), Nullable: false, Source: TablesTableName},
}

var columnsSchema = Schema{
	{Name: "TABLE_CATALOG", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: ColumnsTableName},
	{Name: "TABLE_SCHEMA", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: ColumnsTableName},
	{Name: "TABLE_NAME", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: ColumnsTableName},
	{Name: "COLUMN_NAME", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: ColumnsTableName},
	{Name: "ORDINAL_POSITION", Type: Uint32, Default: nil, Nullable: false, Source: ColumnsTableName},
	{Name: "COLUMN_DEFAULT", Type: LongText, Default: nil, Nullable: true, Source: ColumnsTableName},
	{Name: "IS_NULLABLE", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 3), Default: parse.MustStringToColumnDefaultValue(NewEmptyContext(), `""`, MustCreateStringWithDefaults(sqltypes.VarChar, 3), false), Nullable: false, Source: ColumnsTableName},
	{Name: "DATA_TYPE", Type: LongText, Default: nil, Nullable: true, Source: ColumnsTableName},
	{Name: "CHARACTER_MAXIMUM_LENGTH", Type: Int64, Default: nil, Nullable: true, Source: ColumnsTableName},
	{Name: "CHARACTER_OCTET_LENGTH", Type: Int64, Default: nil, Nullable: true, Source: ColumnsTableName},
	{Name: "NUMERIC_PRECISION", Type: Uint64, Default: nil, Nullable: true, Source: ColumnsTableName},
	{Name: "NUMERIC_SCALE", Type: Uint64, Default: nil, Nullable: true, Source: ColumnsTableName},
	{Name: "DATETIME_PRECISION", Type: Uint32, Default: nil, Nullable: true, Source: ColumnsTableName},
	{Name: "CHARACTER_SET_NAME", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: ColumnsTableName},
	{Name: "COLLATION_NAME", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: ColumnsTableName},
	{Name: "COLUMN_TYPE", Type: MediumText, Default: nil, Nullable: false, Source: ColumnsTableName},
	{Name: "COLUMN_KEY", Type: MustCreateEnumType([]string{"", "PRI", "UNI", "MUL"}, Collation_utf8mb4_0900_bin), Default: nil, Nullable: false, Source: ColumnsTableName},
	{Name: "EXTRA", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 256), Default: nil, Nullable: true, Source: ColumnsTableName},
	{Name: "PRIVILEGES", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 154), Default: nil, Nullable: true, Source: ColumnsTableName},
	{Name: "COLUMN_COMMENT", Type: Text, Default: nil, Nullable: false, Source: ColumnsTableName},
	{Name: "GENERATION_EXPRESSION", Type: LongText, Default: nil, Nullable: false, Source: ColumnsTableName},
	{Name: "SRS_ID", Type: Uint32, Default: nil, Nullable: true, Source: ColumnsTableName},
}

var schemataSchema = Schema{
	{Name: "CATALOG_NAME", Type: LongText, Default: nil, Nullable: false, Source: SchemataTableName},
	{Name: "SCHEMA_NAME", Type: LongText, Default: nil, Nullable: false, Source: SchemataTableName},
	{Name: "DEFAULT_CHARACTER_SET_NAME", Type: LongText, Default: nil, Nullable: false, Source: SchemataTableName},
	{Name: "DEFAULT_COLLATION_NAME", Type: LongText, Default: nil, Nullable: false, Source: SchemataTableName},
	{Name: "SQL_PATH", Type: LongText, Default: nil, Nullable: true, Source: SchemataTableName},
}

var collationsSchema = Schema{
	{Name: "COLLATION_NAME", Type: LongText, Default: nil, Nullable: false, Source: CollationsTableName},
	{Name: "CHARACTER_SET_NAME", Type: LongText, Default: nil, Nullable: false, Source: CollationsTableName},
	{Name: "ID", Type: LongText, Default: nil, Nullable: false, Source: CollationsTableName},
	{Name: "IS_DEFAULT", Type: LongText, Default: nil, Nullable: false, Source: CollationsTableName},
	{Name: "IS_COMPILED", Type: LongText, Default: nil, Nullable: true, Source: CollationsTableName},
	{Name: "SORTLEN", Type: LongText, Default: nil, Nullable: false, Source: CollationsTableName},
	{Name: "PAD_ATTRIBUTE", Type: LongText, Default: nil, Nullable: false, Source: CollationsTableName},
}

var statisticsSchema = Schema{
	{Name: "TABLE_CATALOG", Type: LongText, Default: nil, Nullable: true, Source: StatisticsTableName},
	{Name: "TABLE_SCHEMA", Type: LongText, Default: nil, Nullable: true, Source: StatisticsTableName},
	{Name: "TABLE_NAME", Type: LongText, Default: nil, Nullable: true, Source: StatisticsTableName},
	{Name: "NON_UNIQUE", Type: Int64, Default: nil, Nullable: false, Source: StatisticsTableName},
	{Name: "INDEX_SCHEMA", Type: LongText, Default: nil, Nullable: true, Source: StatisticsTableName},
	{Name: "INDEX_NAME", Type: LongText, Default: nil, Nullable: true, Source: StatisticsTableName},
	{Name: "SEQ_IN_INDEX", Type: Int64, Default: nil, Nullable: false, Source: StatisticsTableName},
	{Name: "COLUMN_NAME", Type: LongText, Default: nil, Nullable: true, Source: StatisticsTableName},
	{Name: "COLLATION", Type: LongText, Default: nil, Nullable: true, Source: StatisticsTableName},
	{Name: "CARDINALITY", Type: Int64, Default: nil, Nullable: true, Source: StatisticsTableName},
	{Name: "SUB_PART", Type: Int64, Default: nil, Nullable: true, Source: StatisticsTableName},
	{Name: "PACKED", Type: Int64, Default: nil, Nullable: true, Source: StatisticsTableName},
	{Name: "NULLABLE", Type: LongText, Default: nil, Nullable: false, Source: StatisticsTableName},
	{Name: "INDEX_TYPE", Type: LongText, Default: nil, Nullable: false, Source: StatisticsTableName},
	{Name: "COMMENT", Type: LongText, Default: nil, Nullable: false, Source: StatisticsTableName},
	{Name: "INDEX_COMMENT", Type: LongText, Default: nil, Nullable: false, Source: StatisticsTableName},
	{Name: "IS_VISIBLE", Type: LongText, Default: nil, Nullable: false, Source: StatisticsTableName},
	{Name: "EXPRESSION", Type: LongText, Default: nil, Nullable: true, Source: StatisticsTableName},
}

var tableConstraintsSchema = Schema{
	{Name: "CONSTRAINT_CATALOG", Type: LongText, Default: nil, Nullable: true, Source: TableConstraintsTableName},
	{Name: "CONSTRAINT_SCHEMA", Type: LongText, Default: nil, Nullable: true, Source: TableConstraintsTableName},
	{Name: "CONSTRAINT_NAME", Type: LongText, Default: nil, Nullable: true, Source: TableConstraintsTableName},
	{Name: "TABLE_SCHEMA", Type: LongText, Default: nil, Nullable: true, Source: TableConstraintsTableName},
	{Name: "TABLE_NAME", Type: LongText, Default: nil, Nullable: true, Source: TableConstraintsTableName},
	{Name: "CONSTRAINT_TYPE", Type: LongText, Default: nil, Nullable: false, Source: TableConstraintsTableName},
	{Name: "ENFORCED", Type: LongText, Default: nil, Nullable: false, Source: TableConstraintsTableName},
}

var referentialConstraintsSchema = Schema{
	{Name: "CONSTRAINT_CATALOG", Type: LongText, Default: nil, Nullable: false, Source: ReferentialConstraintsTableName},
	{Name: "CONSTRAINT_SCHEMA", Type: LongText, Default: nil, Nullable: false, Source: ReferentialConstraintsTableName},
	{Name: "CONSTRAINT_NAME", Type: LongText, Default: nil, Nullable: true, Source: ReferentialConstraintsTableName},
	{Name: "UNIQUE_CONSTRAINT_CATALOG", Type: LongText, Default: nil, Nullable: false, Source: ReferentialConstraintsTableName},
	{Name: "UNIQUE_CONSTRAINT_SCHEMA", Type: LongText, Default: nil, Nullable: false, Source: ReferentialConstraintsTableName},
	{Name: "UNIQUE_CONSTRAINT_NAME", Type: LongText, Default: nil, Nullable: true, Source: ReferentialConstraintsTableName},
	{Name: "MATCH_OPTION", Type: LongText, Default: nil, Nullable: false, Source: ReferentialConstraintsTableName},
	{Name: "UPDATE_RULE", Type: LongText, Default: nil, Nullable: false, Source: ReferentialConstraintsTableName},
	{Name: "DELETE_RULE", Type: LongText, Default: nil, Nullable: false, Source: ReferentialConstraintsTableName},
	{Name: "TABLE_NAME", Type: LongText, Default: nil, Nullable: false, Source: ReferentialConstraintsTableName},
	{Name: "REFERENCED_TABLE_NAME", Type: LongText, Default: nil, Nullable: false, Source: ReferentialConstraintsTableName},
}

var keyColumnUsageSchema = Schema{
	{Name: "CONSTRAINT_CATALOG", Type: LongText, Default: nil, Nullable: true, Source: KeyColumnUsageTableName},
	{Name: "CONSTRAINT_SCHEMA", Type: LongText, Default: nil, Nullable: true, Source: KeyColumnUsageTableName},
	{Name: "CONSTRAINT_NAME", Type: LongText, Default: nil, Nullable: true, Source: KeyColumnUsageTableName},
	{Name: "TABLE_CATALOG", Type: LongText, Default: nil, Nullable: true, Source: KeyColumnUsageTableName},
	{Name: "TABLE_SCHEMA", Type: LongText, Default: nil, Nullable: true, Source: KeyColumnUsageTableName},
	{Name: "TABLE_NAME", Type: LongText, Default: nil, Nullable: true, Source: KeyColumnUsageTableName},
	{Name: "COLUMN_NAME", Type: LongText, Default: nil, Nullable: true, Source: KeyColumnUsageTableName},
	{Name: "ORDINAL_POSITION", Type: LongText, Default: nil, Nullable: false, Source: KeyColumnUsageTableName},
	{Name: "POSITION_IN_UNIQUE_CONSTRAINT", Type: LongText, Default: nil, Nullable: true, Source: KeyColumnUsageTableName},
	{Name: "REFERENCED_TABLE_SCHEMA", Type: LongText, Default: nil, Nullable: true, Source: KeyColumnUsageTableName},
	{Name: "REFERENCED_TABLE_NAME", Type: LongText, Default: nil, Nullable: true, Source: KeyColumnUsageTableName},
	{Name: "REFERENCED_COLUMN_NAME", Type: LongText, Default: nil, Nullable: true, Source: KeyColumnUsageTableName},
}

var triggersSchema = Schema{
	{Name: "TRIGGER_CATALOG", Type: LongText, Default: nil, Nullable: true, Source: TriggersTableName},
	{Name: "TRIGGER_SCHEMA", Type: LongText, Default: nil, Nullable: true, Source: TriggersTableName},
	{Name: "TRIGGER_NAME", Type: LongText, Default: nil, Nullable: false, Source: TriggersTableName},
	{Name: "EVENT_MANIPULATION", Type: LongText, Default: nil, Nullable: false, Source: TriggersTableName},
	{Name: "EVENT_OBJECT_CATALOG", Type: LongText, Default: nil, Nullable: true, Source: TriggersTableName},
	{Name: "EVENT_OBJECT_SCHEMA", Type: LongText, Default: nil, Nullable: true, Source: TriggersTableName},
	{Name: "EVENT_OBJECT_TABLE", Type: LongText, Default: nil, Nullable: true, Source: TriggersTableName},
	{Name: "ACTION_ORDER", Type: Int64, Default: nil, Nullable: false, Source: TriggersTableName},
	{Name: "ACTION_CONDITION", Type: Int64, Default: nil, Nullable: true, Source: TriggersTableName},
	{Name: "ACTION_STATEMENT", Type: LongText, Default: nil, Nullable: false, Source: TriggersTableName},
	{Name: "ACTION_ORIENTATION", Type: LongText, Default: nil, Nullable: false, Source: TriggersTableName},
	{Name: "ACTION_TIMING", Type: LongText, Default: nil, Nullable: false, Source: TriggersTableName},
	{Name: "ACTION_REFERENCE_OLD_TABLE", Type: Int64, Default: nil, Nullable: true, Source: TriggersTableName},
	{Name: "ACTION_REFERENCE_NEW_TABLE", Type: Int64, Default: nil, Nullable: true, Source: TriggersTableName},
	{Name: "ACTION_REFERENCE_OLD_ROW", Type: LongText, Default: nil, Nullable: false, Source: TriggersTableName},
	{Name: "ACTION_REFERENCE_NEW_ROW", Type: LongText, Default: nil, Nullable: false, Source: TriggersTableName},
	{Name: "CREATED", Type: Timestamp, Default: nil, Nullable: false, Source: TriggersTableName},
	{Name: "SQL_MODE", Type: LongText, Default: nil, Nullable: false, Source: TriggersTableName},
	{Name: "DEFINER", Type: LongText, Default: nil, Nullable: false, Source: TriggersTableName},
	{Name: "CHARACTER_SET_CLIENT", Type: LongText, Default: nil, Nullable: false, Source: TriggersTableName},
	{Name: "COLLATION_CONNECTION", Type: LongText, Default: nil, Nullable: false, Source: TriggersTableName},
	{Name: "DATABASE_COLLATION", Type: LongText, Default: nil, Nullable: false, Source: TriggersTableName},
}

var eventsSchema = Schema{
	{Name: "EVENT_CATALOG", Type: LongText, Default: nil, Nullable: true, Source: EventsTableName},
	{Name: "EVENT_SCHEMA", Type: LongText, Default: nil, Nullable: true, Source: EventsTableName},
	{Name: "EVENT_NAME", Type: LongText, Default: nil, Nullable: false, Source: EventsTableName},
	{Name: "DEFINER", Type: LongText, Default: nil, Nullable: false, Source: EventsTableName},
	{Name: "TIME_ZONE", Type: LongText, Default: nil, Nullable: true, Source: EventsTableName},
	{Name: "EVENT_BODY", Type: LongText, Default: nil, Nullable: true, Source: EventsTableName},
	{Name: "EVENT_DEFINITION", Type: LongText, Default: nil, Nullable: true, Source: EventsTableName},
	{Name: "EVENT_TYPE", Type: LongText, Default: nil, Nullable: false, Source: EventsTableName},
	{Name: "EXECUTE_AT", Type: Datetime, Default: nil, Nullable: true, Source: EventsTableName},
	{Name: "INTERVAL_VALUE", Type: LongText, Default: nil, Nullable: false, Source: EventsTableName},
	{Name: "INTERVAL_FIELD", Type: LongText, Default: nil, Nullable: false, Source: EventsTableName},
	{Name: "SQL_MODE", Type: LongText, Default: nil, Nullable: false, Source: EventsTableName},
	{Name: "STARTS", Type: Datetime, Default: nil, Nullable: true, Source: EventsTableName},
	{Name: "ENDS", Type: Datetime, Default: nil, Nullable: true, Source: EventsTableName},
	{Name: "STATUS", Type: LongText, Default: nil, Nullable: false, Source: EventsTableName},
	{Name: "ON_COMPLETION", Type: LongText, Default: nil, Nullable: false, Source: EventsTableName},
	{Name: "CREATED", Type: Timestamp, Default: nil, Nullable: false, Source: EventsTableName},
	{Name: "LAST_ALTERED", Type: Timestamp, Default: nil, Nullable: false, Source: EventsTableName},
	{Name: "LAST_EXECUTED", Type: Datetime, Default: nil, Nullable: false, Source: EventsTableName},
	{Name: "EVENT_COMMENT", Type: LongText, Default: nil, Nullable: false, Source: EventsTableName},
	{Name: "ORIGINATOR", Type: Int64, Default: nil, Nullable: false, Source: EventsTableName},
	{Name: "CHARACTER_SET_CLIENT", Type: LongText, Default: nil, Nullable: false, Source: EventsTableName},
	{Name: "COLLATION_CONNECTION", Type: LongText, Default: nil, Nullable: false, Source: EventsTableName},
	{Name: "DATABASE_COLLATION", Type: LongText, Default: nil, Nullable: false, Source: EventsTableName},
}

var routinesSchema = Schema{
	{Name: "SPECIFIC_NAME", Type: LongText, Default: nil, Nullable: false, Source: RoutinesTableName},
	{Name: "ROUTINE_CATALOG", Type: LongText, Default: nil, Nullable: true, Source: RoutinesTableName},
	{Name: "ROUTINE_SCHEMA", Type: LongText, Default: nil, Nullable: true, Source: RoutinesTableName},
	{Name: "ROUTINE_NAME", Type: LongText, Default: nil, Nullable: false, Source: RoutinesTableName},
	{Name: "ROUTINE_TYPE", Type: LongText, Default: nil, Nullable: false, Source: RoutinesTableName},
	{Name: "DATA_TYPE", Type: LongText, Default: nil, Nullable: true, Source: RoutinesTableName},
	{Name: "CHARACTER_MAXIMUM_LENGTH", Type: Int64, Default: nil, Nullable: true, Source: RoutinesTableName},
	{Name: "CHARACTER_OCTET_LENGTH", Type: Int64, Default: nil, Nullable: true, Source: RoutinesTableName},
	{Name: "NUMERIC_PRECISION", Type: Int64, Default: nil, Nullable: true, Source: RoutinesTableName},
	{Name: "NUMERIC_SCALE", Type: Int64, Default: nil, Nullable: true, Source: RoutinesTableName},
	{Name: "DATETIME_PRECISION", Type: Int64, Default: nil, Nullable: true, Source: RoutinesTableName},
	{Name: "CHARACTER_SET_NAME", Type: LongText, Default: nil, Nullable: true, Source: RoutinesTableName},
	{Name: "COLLATION_NAME", Type: LongText, Default: nil, Nullable: true, Source: RoutinesTableName},
	{Name: "DTD_IDENTIFIER", Type: LongText, Default: nil, Nullable: true, Source: RoutinesTableName},
	{Name: "ROUTINE_BODY", Type: LongText, Default: nil, Nullable: false, Source: RoutinesTableName},
	{Name: "ROUTINE_DEFINITION", Type: LongText, Default: nil, Nullable: true, Source: RoutinesTableName},
	{Name: "EXTERNAL_NAME", Type: Int64, Default: nil, Nullable: true, Source: RoutinesTableName},
	{Name: "EXTERNAL_LANGUAGE", Type: LongText, Default: nil, Nullable: false, Source: RoutinesTableName},
	{Name: "PARAMETER_STYLE", Type: LongText, Default: nil, Nullable: false, Source: RoutinesTableName},
	{Name: "IS_DETERMINISTIC", Type: LongText, Default: nil, Nullable: false, Source: RoutinesTableName},
	{Name: "SQL_DATA_ACCESS", Type: LongText, Default: nil, Nullable: false, Source: RoutinesTableName},
	{Name: "SQL_PATH", Type: Int64, Default: nil, Nullable: true, Source: RoutinesTableName},
	{Name: "SECURITY_TYPE", Type: LongText, Default: nil, Nullable: false, Source: RoutinesTableName},
	{Name: "CREATED", Type: Timestamp, Default: nil, Nullable: false, Source: RoutinesTableName},
	{Name: "LAST_ALTERED", Type: Timestamp, Default: nil, Nullable: false, Source: RoutinesTableName},
	{Name: "SQL_MODE", Type: LongText, Default: nil, Nullable: false, Source: RoutinesTableName},
	{Name: "ROUTINE_COMMENT", Type: LongText, Default: nil, Nullable: false, Source: RoutinesTableName},
	{Name: "DEFINER", Type: LongText, Default: nil, Nullable: false, Source: RoutinesTableName},
	{Name: "CHARACTER_SET_CLIENT", Type: LongText, Default: nil, Nullable: false, Source: RoutinesTableName},
	{Name: "COLLATION_CONNECTION", Type: LongText, Default: nil, Nullable: false, Source: RoutinesTableName},
	{Name: "DATABASE_COLLATION", Type: LongText, Default: nil, Nullable: false, Source: RoutinesTableName},
}

var viewsSchema = Schema{
	{Name: "TABLE_CATALOG", Type: LongText, Default: nil, Nullable: true, Source: ViewsTableName},
	{Name: "TABLE_SCHEMA", Type: LongText, Default: nil, Nullable: true, Source: ViewsTableName},
	{Name: "TABLE_NAME", Type: LongText, Default: nil, Nullable: true, Source: ViewsTableName},
	{Name: "VIEW_DEFINITION", Type: LongText, Default: nil, Nullable: true, Source: ViewsTableName},
	{Name: "CHECK_OPTION", Type: LongText, Default: nil, Nullable: true, Source: ViewsTableName},
	{Name: "IS_UPDATABLE", Type: LongText, Default: nil, Nullable: true, Source: ViewsTableName},
	{Name: "DEFINER", Type: LongText, Default: nil, Nullable: true, Source: ViewsTableName},
	{Name: "SECURITY_TYPE", Type: LongText, Default: nil, Nullable: true, Source: ViewsTableName},
	{Name: "CHARACTER_SET_CLIENT", Type: LongText, Default: nil, Nullable: false, Source: ViewsTableName},
	{Name: "COLLATION_CONNECTION", Type: LongText, Default: nil, Nullable: false, Source: ViewsTableName},
}

var userPrivilegesSchema = Schema{
	{Name: "GRANTEE", Type: LongText, Default: nil, Nullable: false, Source: UserPrivilegesTableName},
	{Name: "TABLE_CATALOG", Type: LongText, Default: nil, Nullable: false, Source: UserPrivilegesTableName},
	{Name: "PRIVILEGE_TYPE", Type: LongText, Default: nil, Nullable: false, Source: UserPrivilegesTableName},
	{Name: "IS_GRANTABLE", Type: LongText, Default: nil, Nullable: false, Source: UserPrivilegesTableName},
}

var characterSetSchema = Schema{
	{Name: "CHARACTER_SET_NAME", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: CharacterSetsTableName},
	{Name: "DEFAULT_COLLATE_NAME", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: CharacterSetsTableName},
	{Name: "DESCRIPTION", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 2048), Default: nil, Nullable: false, Source: CharacterSetsTableName},
	{Name: "MAXLEN", Type: Uint64, Default: nil, Nullable: false, Source: CharacterSetsTableName},
}

var enginesSchema = Schema{
	{Name: "ENGINE", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: EnginesTableName},
	{Name: "SUPPORT", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 8), Default: nil, Nullable: false, Source: EnginesTableName},
	{Name: "COMMENT", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 80), Default: nil, Nullable: false, Source: EnginesTableName},
	{Name: "TRANSACTIONS", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 3), Default: nil, Nullable: true, Source: EnginesTableName},
	{Name: "XA", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 3), Default: nil, Nullable: true, Source: EnginesTableName},
	{Name: "SAVEPOINTS", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 3), Default: nil, Nullable: true, Source: EnginesTableName},
}

var checkConstraintsSchema = Schema{
	{Name: "CONSTRAINT_CATALOG", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: CheckConstraintsTableName},
	{Name: "CONSTRAINT_SCHEMA", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: CheckConstraintsTableName},
	{Name: "CONSTRAINT_NAME", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: CheckConstraintsTableName},
	{Name: "CHECK_CLAUSE", Type: LongText, Default: nil, Nullable: false, Source: CheckConstraintsTableName},
}

var partitionSchema = Schema{
	{Name: "TABLE_CATALOG", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: PartitionsTableName},
	{Name: "TABLE_SCHEMA", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: PartitionsTableName},
	{Name: "TABLE_NAME", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: PartitionsTableName},
	{Name: "PARTITION_NAME", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: PartitionsTableName},
	{Name: "SUBPARTITION_NAME", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: PartitionsTableName},
	{Name: "PARTITION_ORDINAL_POSITION", Type: Uint64, Default: nil, Nullable: true, Source: PartitionsTableName},
	{Name: "SUBPARTITION_ORDINAL_POSITION", Type: Uint64, Default: nil, Nullable: true, Source: PartitionsTableName},
	{Name: "PARTITION_METHOD", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 13), Default: nil, Nullable: true, Source: PartitionsTableName},
	{Name: "SUBPARTITION_METHOD", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 13), Default: nil, Nullable: true, Source: PartitionsTableName},
	{Name: "PARTITION_EXPRESSION", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 2048), Default: nil, Nullable: true, Source: PartitionsTableName},
	{Name: "SUBPARTITION_EXPRESSION", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 2048), Default: nil, Nullable: true, Source: PartitionsTableName},
	{Name: "PARTITION_DESCRIPTION", Type: LongText, Default: nil, Nullable: true, Source: PartitionsTableName},
	{Name: "TABLE_ROWS", Type: Uint64, Default: nil, Nullable: true, Source: PartitionsTableName},
	{Name: "AVG_ROW_LENGTH", Type: Uint64, Default: nil, Nullable: true, Source: PartitionsTableName},
	{Name: "DATA_LENGTH", Type: Uint64, Default: nil, Nullable: true, Source: PartitionsTableName},
	{Name: "MAX_DATA_LENGTH", Type: Uint64, Default: nil, Nullable: true, Source: PartitionsTableName},
	{Name: "INDEX_LENGTH", Type: Uint64, Default: nil, Nullable: true, Source: PartitionsTableName},
	{Name: "DATA_FREE", Type: Uint64, Default: nil, Nullable: true, Source: PartitionsTableName},
	{Name: "CREATE_TIME", Type: Timestamp, Default: nil, Nullable: false, Source: PartitionsTableName},
	{Name: "UPDATE_TIME", Type: Datetime, Default: nil, Nullable: true, Source: PartitionsTableName},
	{Name: "CHECK_TIME", Type: Datetime, Default: nil, Nullable: true, Source: PartitionsTableName},
	{Name: "CHECKSUM", Type: Uint64, Default: nil, Nullable: true, Source: PartitionsTableName},
	{Name: "PARTITION_COMMENT", Type: LongText, Default: nil, Nullable: false, Source: PartitionsTableName},
	{Name: "NODEGROUP", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 256), Default: nil, Nullable: true, Source: PartitionsTableName},
	{Name: "TABLESPACE_NAME", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 258), Default: nil, Nullable: true, Source: PartitionsTableName},
}

var processListSchema = Schema{
	{Name: "ID", Type: Int64, Default: nil, Nullable: false, Source: ProcessListTableName},
	{Name: "USER", Type: LongText, Default: nil, Nullable: false, Source: ProcessListTableName},
	{Name: "HOST", Type: LongText, Default: nil, Nullable: false, Source: ProcessListTableName},
	{Name: "DB", Type: LongText, Default: nil, Nullable: true, Source: ProcessListTableName},
	{Name: "COMMAND", Type: LongText, Default: nil, Nullable: false, Source: ProcessListTableName},
	{Name: "TIME", Type: Int64, Default: nil, Nullable: false, Source: ProcessListTableName},
	{Name: "STATE", Type: LongText, Default: nil, Nullable: true, Source: ProcessListTableName},
	{Name: "INFO", Type: LongText, Default: nil, Nullable: true, Source: ProcessListTableName},
}

var collationCharSetApplicabilitySchema = Schema{
	{Name: "COLLATION_NAME", Type: LongText, Default: nil, Nullable: false, Source: CollationCharSetApplicabilityTableName},
	{Name: "CHARACTER_SET_NAME", Type: LongText, Default: nil, Nullable: false, Source: CollationCharSetApplicabilityTableName},
}

var administrableRoleAuthorizationsSchema = Schema{
	{Name: "USER", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 97), Default: nil, Nullable: true, Source: AdministrableRoleAuthorizationsTableName},
	{Name: "HOST", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 256), Default: nil, Nullable: true, Source: AdministrableRoleAuthorizationsTableName},
	{Name: "GRANTEE", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 97), Default: nil, Nullable: true, Source: AdministrableRoleAuthorizationsTableName},
	{Name: "GRANTEE_HOST", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 256), Default: nil, Nullable: true, Source: AdministrableRoleAuthorizationsTableName},
	{Name: "ROLE_NAME", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 255), Default: nil, Nullable: true, Source: AdministrableRoleAuthorizationsTableName},
	{Name: "ROLE_HOST", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 256), Default: nil, Nullable: true, Source: AdministrableRoleAuthorizationsTableName},
	{Name: "IS_GRANTABLE", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 3), Default: nil, Nullable: false, Source: AdministrableRoleAuthorizationsTableName},
	{Name: "IS_DEFAULT", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 3), Default: nil, Nullable: true, Source: AdministrableRoleAuthorizationsTableName},
	{Name: "IS_MANDATORY", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 3), Default: nil, Nullable: false, Source: AdministrableRoleAuthorizationsTableName},
}

var applicableRolesSchema = Schema{
	{Name: "USER", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 97), Default: nil, Nullable: true, Source: ApplicableRolesTableName},
	{Name: "HOST", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 256), Default: nil, Nullable: true, Source: ApplicableRolesTableName},
	{Name: "GRANTEE", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 97), Default: nil, Nullable: true, Source: ApplicableRolesTableName},
	{Name: "GRANTEE_HOST", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 256), Default: nil, Nullable: true, Source: ApplicableRolesTableName},
	{Name: "ROLE_NAME", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 255), Default: nil, Nullable: true, Source: ApplicableRolesTableName},
	{Name: "ROLE_HOST", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 256), Default: nil, Nullable: true, Source: ApplicableRolesTableName},
	{Name: "IS_GRANTABLE", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 3), Default: nil, Nullable: false, Source: ApplicableRolesTableName},
	{Name: "IS_DEFAULT", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 3), Default: nil, Nullable: true, Source: ApplicableRolesTableName},
	{Name: "IS_MANDATORY", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 3), Default: nil, Nullable: false, Source: ApplicableRolesTableName},
}

var columnPrivilegesSchema = Schema{
	{Name: "GRANTEE", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 292), Default: nil, Nullable: false, Source: ColumnPrivilegesTableName},
	{Name: "TABLE_CATALOG", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 512), Default: nil, Nullable: false, Source: ColumnPrivilegesTableName},
	{Name: "TABLE_SCHEMA", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: ColumnPrivilegesTableName},
	{Name: "TABLE_NAME", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: ColumnPrivilegesTableName},
	{Name: "COLUMN_NAME", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: ColumnPrivilegesTableName},
	{Name: "PRIVILEGE_TYPE", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: ColumnPrivilegesTableName},
	{Name: "IS_GRANTABLE", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 3), Default: nil, Nullable: false, Source: ColumnPrivilegesTableName},
}

var columnExtensionsSchema = Schema{
	{Name: "TABLE_CATALOG", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: ColumnsExtensionsTableName},
	{Name: "TABLE_SCHEMA", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: ColumnsExtensionsTableName},
	{Name: "TABLE_NAME", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: ColumnsExtensionsTableName},
	{Name: "COLUMN_NAME", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: ColumnsExtensionsTableName},
	{Name: "ENGINE_ATTRIBUTE", Type: JSON, Default: nil, Nullable: true, Source: ColumnsExtensionsTableName},
	{Name: "SECONDARY_ENGINE_ATTRIBUTE", Type: JSON, Default: nil, Nullable: true, Source: ColumnsExtensionsTableName},
}

var connectionControlFailedLoginAttemptsSchema = Schema{
	{Name: "USERHOST", Type: LongText, Default: nil, Nullable: false, Source: ConnectionControlFailedLoginAttemptsTableName},
	{Name: "FAILED_ATTEMPTS", Type: Uint64, Default: nil, Nullable: false, Source: ConnectionControlFailedLoginAttemptsTableName},
}

var enabledRolesSchema = Schema{
	{Name: "ROLE_NAME", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 255), Default: nil, Nullable: true, Source: EnabledRolesTablesName},
	{Name: "ROLE_HOST", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 255), Default: nil, Nullable: true, Source: EnabledRolesTablesName},
	{Name: "IS_DEFAULT", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 3), Default: nil, Nullable: true, Source: EnabledRolesTablesName},
	{Name: "IS_MANDATORY", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 3), Default: nil, Nullable: false, Source: EnabledRolesTablesName},
}

var keywordsSchema = Schema{
	{Name: "WORD", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 128), Default: nil, Nullable: true, Source: KeywordsTableName},
	{Name: "RESERVED", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: KeywordsTableName},
}

var mysqlFirewallUsersSchema = Schema{
	{Name: "USERHOST", Type: LongText, Default: nil, Nullable: true, Source: MysqlFirewallUsersTableName},
	{Name: "MODE", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: MysqlFirewallUsersTableName},
}

var mysqlFirewallWhitelistSchema = Schema{
	{Name: "USERHOST", Type: LongText, Default: nil, Nullable: true, Source: MysqlFirewallWhitelistTableName},
	{Name: "RULE", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: MysqlFirewallWhitelistTableName},
}

var optimizerTraceSchema = Schema{
	{Name: "QUERY", Type: Text, Default: nil, Nullable: false, Source: OptimizerTraceTableName},
	{Name: "TRACE", Type: Text, Default: nil, Nullable: false, Source: OptimizerTraceTableName},
	{Name: "MISSING_BYTES_BEYOND_MAX_MEM_SIZE", Type: Int64, Default: nil, Nullable: false, Source: OptimizerTraceTableName},
	{Name: "INSUFFICIENT_PRIVILEGES", Type: MustCreateBitType(1), Default: nil, Nullable: false, Source: OptimizerTraceTableName},
}

var pluginsSchema = Schema{
	{Name: "PLUGIN_NAME", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: PluginsTableName},
	{Name: "PLUGIN_VERSION", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 20), Default: nil, Nullable: true, Source: PluginsTableName},
	{Name: "PLUGIN_STATUS", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 10), Default: nil, Nullable: true, Source: PluginsTableName},
	{Name: "PLUGIN_TYPE", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 80), Default: nil, Nullable: true, Source: PluginsTableName},
	{Name: "PLUGIN_TYPE_VERSION", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 20), Default: nil, Nullable: true, Source: PluginsTableName},
	{Name: "PLUGIN_LIBRARY", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: PluginsTableName},
	{Name: "PLUGIN_LIBRARY_VERSION", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 20), Default: nil, Nullable: false, Source: PluginsTableName},
	{Name: "PLUGIN_AUTHOR", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: PluginsTableName},
	{Name: "PLUGIN_DESCRIPTION", Type: Text, Default: nil, Nullable: false, Source: PluginsTableName},
	{Name: "PLUGIN_LICENSE", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 80), Default: nil, Nullable: false, Source: PluginsTableName},
	{Name: "LOAD_OPTION", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: PluginsTableName},
}

var profilingSchema = Schema{
	{Name: "QUERY_ID", Type: Int64, Default: nil, Nullable: false, Source: ProfilingTableName},
	{Name: "SEQ", Type: Int64, Default: nil, Nullable: false, Source: ProfilingTableName},
	{Name: "STATE", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 30), Default: nil, Nullable: false, Source: ProfilingTableName},
	{Name: "DURATION", Type: MustCreateDecimalType(DecimalTypeMaxPrecision, DecimalTypeMaxScale), Default: nil, Nullable: false, Source: ProfilingTableName},
	{Name: "CPU_USER", Type: MustCreateDecimalType(DecimalTypeMaxPrecision, DecimalTypeMaxScale), Default: nil, Nullable: true, Source: ProfilingTableName},
	{Name: "CPU_SYSTEM", Type: MustCreateDecimalType(DecimalTypeMaxPrecision, DecimalTypeMaxScale), Default: nil, Nullable: true, Source: ProfilingTableName},
	{Name: "CONTEXT_VOLUNTARY", Type: Int64, Default: nil, Nullable: true, Source: ProfilingTableName},
	{Name: "CONTEXT_INVOLUNTARY", Type: Int64, Default: nil, Nullable: true, Source: ProfilingTableName},
	{Name: "BLOCK_OPS_IN", Type: Int64, Default: nil, Nullable: true, Source: ProfilingTableName},
	{Name: "BLOCK_OPS_OUT", Type: Int64, Default: nil, Nullable: true, Source: ProfilingTableName},
	{Name: "MESSAGES_SENT", Type: Int64, Default: nil, Nullable: true, Source: ProfilingTableName},
	{Name: "MESSAGES_RECEIVED", Type: Int64, Default: nil, Nullable: true, Source: ProfilingTableName},
	{Name: "PAGE_FAULTS_MAJOR", Type: Int64, Default: nil, Nullable: true, Source: ProfilingTableName},
	{Name: "PAGE_FAULTS_MINOR", Type: Int64, Default: nil, Nullable: true, Source: ProfilingTableName},
	{Name: "SWAPS", Type: Int64, Default: nil, Nullable: true, Source: ProfilingTableName},
	{Name: "SOURCE_FUNCTION", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 30), Default: nil, Nullable: true, Source: ProfilingTableName},
	{Name: "SOURCE_FILE", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 20), Default: nil, Nullable: true, Source: ProfilingTableName},
	{Name: "SOURCE_LINE", Type: Int64, Default: nil, Nullable: true, Source: ProfilingTableName},
}

var resourceGroupSchema = Schema{
	{Name: "RESOURCE_GROUP_NAME", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: ResourceGroupsTableName},
	{Name: "RESOURCE_GROUP_TYPE", Type: MustCreateEnumType([]string{"SYSTEM", "USER"}, Collation_Default), Default: nil, Nullable: false, Source: ResourceGroupsTableName},
	{Name: "RESOURCE_GROUP_ENABLE", Type: MustCreateBitType(1), Default: nil, Nullable: false, Source: ResourceGroupsTableName},
	{Name: "VPCUS_IDS", Type: LongText, Default: nil, Nullable: true, Source: ResourceGroupsTableName},
	{Name: "THREAD_PRIORITY", Type: Int8, Default: nil, Nullable: false, Source: ResourceGroupsTableName},
}

var roleColumnGrantsSchema = Schema{
	{Name: "GRANTOR", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 97), Default: nil, Nullable: true, Source: RoleColumnGrantsTableName},
	{Name: "GRANTOR_HOST", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 256), Default: nil, Nullable: true, Source: RoleColumnGrantsTableName},
	{Name: "GRANTEE", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 32), Default: nil, Nullable: false, Source: RoleColumnGrantsTableName},
	{Name: "GRANTEE_HOST", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 255), Default: nil, Nullable: false, Source: RoleColumnGrantsTableName},
	{Name: "TABLE_CATALOG", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 3), Default: nil, Nullable: false, Source: RoleColumnGrantsTableName},
	{Name: "TABLE_SCHEMA", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: RoleColumnGrantsTableName},
	{Name: "TABLE_NAME", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: RoleColumnGrantsTableName},
	{Name: "COLUMN_NAME", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: RoleColumnGrantsTableName},
	{Name: "PRIVILEGE_TYPE", Type: MustCreateSetType([]string{"Select", "Insert", "Update", "References"}, Collation_Default), Default: nil, Nullable: false, Source: RoleColumnGrantsTableName},
	{Name: "IS_GRANTABLE", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 3), Default: nil, Nullable: false, Source: RoleColumnGrantsTableName},
}

var roleRoutineGrantsSchema = Schema{
	{Name: "GRANTOR", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 97), Default: nil, Nullable: true, Source: RoleRoutineGrantsTableName},
	{Name: "GRANTOR_HOST", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 256), Default: nil, Nullable: true, Source: RoleRoutineGrantsTableName},
	{Name: "GRANTEE", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 32), Default: nil, Nullable: false, Source: RoleRoutineGrantsTableName},
	{Name: "GRANTEE_HOST", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 255), Default: nil, Nullable: false, Source: RoleRoutineGrantsTableName},
	{Name: "SPECIFIC_CATALOG", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 3), Default: nil, Nullable: false, Source: RoleRoutineGrantsTableName},
	{Name: "SPECIFIC_SCHEMA", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: RoleRoutineGrantsTableName},
	{Name: "SPECIFIC_NAME", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: RoleRoutineGrantsTableName},
	{Name: "ROUTINE_CATALOG", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 3), Default: nil, Nullable: false, Source: RoleRoutineGrantsTableName},
	{Name: "ROUTINE_SCHEMA", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: RoleRoutineGrantsTableName},
	{Name: "ROUTINE_NAME", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: RoleRoutineGrantsTableName},
	{Name: "PRIVILEGE_TYPE", Type: MustCreateSetType([]string{"Execute", "Alter Routine", "Grant"}, Collation_Default), Default: nil, Nullable: false, Source: RoleRoutineGrantsTableName},
	{Name: "IS_GRANTABLE", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 3), Default: nil, Nullable: false, Source: RoleRoutineGrantsTableName},
}

var roleTableGrantsSchema = Schema{
	{Name: "GRANTOR", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 97), Default: nil, Nullable: true, Source: RoleTableGrantsTableName},
	{Name: "GRANTOR_HOST", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 256), Default: nil, Nullable: true, Source: RoleTableGrantsTableName},
	{Name: "GRANTEE", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 32), Default: nil, Nullable: false, Source: RoleTableGrantsTableName},
	{Name: "GRANTEE_HOST", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 255), Default: nil, Nullable: false, Source: RoleTableGrantsTableName},
	{Name: "TABLE_CATALOG", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 3), Default: nil, Nullable: false, Source: RoleTableGrantsTableName},
	{Name: "TABLE_SCHEMA", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: RoleTableGrantsTableName},
	{Name: "TABLE_NAME", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: RoleTableGrantsTableName},
	{Name: "PRIVILEGE_TYPE", Type: MustCreateSetType([]string{"Select", "Insert", "Update", "Delete", "Create", "Drop", "Grant", "References", "Index", "Alter", "Create View", "Show view", "Trigger"}, Collation_Default), Default: nil, Nullable: false, Source: RoleTableGrantsTableName},
	{Name: "IS_GRANTABLE", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 3), Default: nil, Nullable: false, Source: RoleTableGrantsTableName},
}

var schemaPrivilegesTableName = Schema{
	{Name: "GRANTEE", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 292), Default: nil, Nullable: false, Source: SchemaPrivilegesTableName},
	{Name: "TABLE_CATALOG", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 3), Default: nil, Nullable: false, Source: SchemaPrivilegesTableName},
	{Name: "TABLE_SCHEMA", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: SchemaPrivilegesTableName},
	{Name: "PRIVILEGE_TYPE", Type: MustCreateSetType([]string{"Select", "Insert", "Update", "Delete", "Create", "Drop", "Grant", "References", "Index", "Alter", "Create View", "Show view", "Trigger"}, Collation_Default), Default: nil, Nullable: false, Source: SchemaPrivilegesTableName},
	{Name: "IS_GRANTABLE", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 3), Default: nil, Nullable: false, Source: SchemaPrivilegesTableName},
}

var schemataExtensionTableName = Schema{
	{Name: "CATALOG_NAME", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: SchemataExtensionsTableName},
	{Name: "SCHEMA_NAME", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: SchemataExtensionsTableName},
	{Name: "OPTIONS", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 256), Default: nil, Nullable: true, Source: SchemataExtensionsTableName},
}

var stGeometryColumnsSchema = Schema{
	{Name: "TABLE_CATALOG", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: StGeometryColumnsTableName},
	{Name: "TABLE_SCHEMA", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: StGeometryColumnsTableName},
	{Name: "TABLE_NAME", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: StGeometryColumnsTableName},
	{Name: "COLUMN_NAME", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: StGeometryColumnsTableName},
	{Name: "SRS_NAME", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: StGeometryColumnsTableName},
	{Name: "SRS_ID", Type: Uint64, Default: nil, Nullable: true, Source: StGeometryColumnsTableName},
	{Name: "GEOMETRY_TYPE_NAME", Type: LongText, Default: nil, Nullable: true, Source: StGeometryColumnsTableName},
}

var stSpatialReferenceSystemsSchema = Schema{
	{Name: "SRS_NAME", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 80), Default: nil, Nullable: false, Source: StSpatialReferenceSystemsTableName},
	{Name: "SRS_ID", Type: Uint64, Default: nil, Nullable: false, Source: StSpatialReferenceSystemsTableName},
	{Name: "ORGANIZATION", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 256), Default: nil, Nullable: true, Source: StSpatialReferenceSystemsTableName},
	{Name: "ORGANIZATION_COORDSYS_ID", Type: Uint64, Default: nil, Nullable: true, Source: StSpatialReferenceSystemsTableName},
	{Name: "DEFINITION", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 4096), Default: nil, Nullable: false, Source: StSpatialReferenceSystemsTableName},
	{Name: "DESCRIPTION", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 2048), Default: nil, Nullable: true, Source: StSpatialReferenceSystemsTableName},
}

var stUnitsOfMeasureSchema = Schema{
	{Name: "UNIT_NAME", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 255), Default: nil, Nullable: true, Source: StUnitsOfMeasureTableName},
	{Name: "UNIT_TYPE", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 7), Default: nil, Nullable: true, Source: StUnitsOfMeasureTableName},
	{Name: "CONVERSION_FACTOR", Type: Float64, Default: nil, Nullable: true, Source: StUnitsOfMeasureTableName},
	{Name: "DESCRIPTION", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 255), Default: nil, Nullable: true, Source: StUnitsOfMeasureTableName},
}

var tableConstraintsExtensionsSchema = Schema{
	{Name: "CONSTRAINT_CATALOG", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: TableConstraintsExtensionsTableName},
	{Name: "CONSTRAINT_SCHEMA", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: TableConstraintsExtensionsTableName},
	{Name: "CONSTRAINT_NAME", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: TableConstraintsExtensionsTableName},
	{Name: "TABLE_NAME", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: TableConstraintsExtensionsTableName},
	{Name: "ENGINE_ATTRIBUTE", Type: JSON, Default: nil, Nullable: true, Source: TableConstraintsExtensionsTableName},
	{Name: "SECONDARY_ENGINE_ATTRIBUTE", Type: JSON, Default: nil, Nullable: true, Source: TableConstraintsExtensionsTableName},
}

var tablePrivilegesSchema = Schema{
	{Name: "GRANTEE", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 292), Default: nil, Nullable: false, Source: TablePrivilegesTableName},
	{Name: "TABLE_CATALOG", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 512), Default: nil, Nullable: false, Source: TablePrivilegesTableName},
	{Name: "TABLE_SCHEMA", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: TablePrivilegesTableName},
	{Name: "TABLE_NAME", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: TablePrivilegesTableName},
	{Name: "PRIVILEGE_TYPE", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: TablePrivilegesTableName},
	{Name: "IS_GRANTABLE", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 3), Default: nil, Nullable: false, Source: TablePrivilegesTableName},
}

var tablesExtensionsSchema = Schema{
	{Name: "TABLE_CATALOG", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: TablesExtensionsTableName},
	{Name: "TABLE_SCHEMA", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: TablesExtensionsTableName},
	{Name: "TABLE_NAME", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: TablesExtensionsTableName},
	{Name: "ENGINE_ATTRIBUTE", Type: JSON, Default: nil, Nullable: true, Source: TablesExtensionsTableName},
	{Name: "SECONDARY_ENGINE_ATTRIBUTE", Type: JSON, Default: nil, Nullable: true, Source: TablesExtensionsTableName},
}

var tablespacesSchema = Schema{
	{Name: "TABLESPACE_NAME", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: TablespacesTableName},
	{Name: "ENGINE", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: TablespacesTableName},
	{Name: "TABLESPACE_TYPE", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: TablespacesTableName},
	{Name: "LOGFILE_GROUP_NAME", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: TablespacesTableName},
	{Name: "EXTENT_SIZE", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: TablespacesTableName},
	{Name: "AUTOEXTEND_SIZE", Type: Int64, Default: nil, Nullable: true, Source: TablespacesTableName},
	{Name: "MAXIMUM_SIZE", Type: Int64, Default: nil, Nullable: true, Source: TablespacesTableName},
	{Name: "NODEGROUP_ID", Type: Int64, Default: nil, Nullable: true, Source: TablespacesTableName},
	{Name: "TABLESPACE_COMMENT", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 2048), Default: nil, Nullable: true, Source: TablespacesTableName},
}

var tablespacesExtensionsSchema = Schema{
	{Name: "TABLESPACE_NAME", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 268), Default: nil, Nullable: false, Source: TablespacesExtensionsTableName},
	{Name: "ENGINE_ATTRIBUTE", Type: JSON, Default: nil, Nullable: true, Source: TablespacesExtensionsTableName},
}

var userAttributesSchema = Schema{
	{Name: "USER", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 32), Default: nil, Nullable: false, Source: UserAttributesTableName},
	{Name: "HOST", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 255), Default: nil, Nullable: false, Source: UserAttributesTableName},
	{Name: "ATTRIBUTE", Type: LongText, Default: nil, Nullable: true, Source: UserAttributesTableName},
}

var viewRoutineUsageSchema = Schema{
	{Name: "TABLE_CATALOG", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: ViewRoutineUsageTableName},
	{Name: "TABLE_SCHEMA", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: ViewRoutineUsageTableName},
	{Name: "TABLE_NAME", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: ViewRoutineUsageTableName},
	{Name: "SPECIFIC_CATALOG", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: ViewRoutineUsageTableName},
	{Name: "SPECIFIC_CATALOG", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: ViewRoutineUsageTableName},
	{Name: "SPECIFIC_TABLE", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: ViewRoutineUsageTableName},
}

var viewTableUsageSchema = Schema{
	{Name: "VIEW_CATALOG", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: ViewTableUsageTableName},
	{Name: "VIEW_SCHEMA", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: ViewTableUsageTableName},
	{Name: "VIEW_NAME", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: ViewTableUsageTableName},
	{Name: "TABLE_CATALOG", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: ViewTableUsageTableName},
	{Name: "TABLE_SCHEMA", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: ViewTableUsageTableName},
	{Name: "TABLE_NAME", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: ViewTableUsageTableName},
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
				nil,                        // auto_increment (always nil)
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
	collIter := NewCollationsIterator()
	for c, ok := collIter.Next(); ok; c, ok = collIter.Next() {
		rows = append(rows, Row{
			c.Name,
			c.CharacterSet.Name(),
			int64(c.ID),
			c.ID.IsDefault(),
			c.ID.IsCompiled(),
			c.ID.SortLength(),
			c.ID.PadAttribute(),
		})
	}
	return RowsToRowIter(rows...), nil
}

func charsetRowIter(ctx *Context, c Catalog) (RowIter, error) {
	var rows []Row
	for _, c := range SupportedCharsets {
		rows = append(rows, Row{
			c.String(),
			c.DefaultCollation().Name(),
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
						nonUnique = 0
					} else {
						nonUnique = 1
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
								collation   string
								nullable    string
								cardinality int64
							)

							seqInIndex := i
							colName := strings.Replace(col.Name, "`", "", -1) // get rid of backticks

							// collation is "A" for ASC ; "D" for DESC ; "NULL" for not sorted
							collation = "A"

							// TODO : cardinality should be an estimate of the number of unique values in the index.
							// it is currently set to total number of rows in the table
							if st, ok := tbl.(StatisticsTable); ok {
								cardinality, err = getTotalNumRows(ctx, st)
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

// columnStatisticsRowIter implements the custom sql.RowIter for the information_schema.columns table.
func columnStatisticsRowIter(ctx *Context, c Catalog) (RowIter, error) {
	var rows []Row
	for _, db := range c.AllDatabases(ctx) {
		err := DBTableIter(ctx, db, func(t Table) (cont bool, err error) {
			statsTbl, ok := t.(StatisticsTable)
			if !ok {
				return true, nil
			}

			stats, err := statsTbl.Statistics(ctx)
			if err != nil {
				return false, err
			}

			if stats.HistogramMap() == nil {
				return true, nil
			}

			for _, col := range t.Schema() {
				if _, ok := col.Type.(StringType); ok {
					continue
				}

				hist, err := stats.Histogram(col.Name)
				if err != nil {
					return false, err
				}

				buckets := make([]string, len(hist.Buckets))
				for i, b := range hist.Buckets {
					buckets[i] = fmt.Sprintf("[%.2f, %.2f, %.2f]", b.LowerBound, b.UpperBound, b.Frequency)
				}

				bucketStrings := fmt.Sprintf("[%s]", strings.Join(buckets, ","))

				rows = append(rows, Row{
					db.Name(),          // table_schema
					statsTbl.Name(),    // table_name
					col.Name,           // column_name
					hist.Mean,          // mean
					hist.Min,           // min
					hist.Max,           // max
					hist.Count,         // count
					hist.NullCount,     // null_count
					hist.DistinctCount, // distinct_count
					bucketStrings,      // buckets
					//sql.JSONDocument{Val: jsonHist}, // histogram
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
				triggerPlan.CreatedAt = trigger.CreatedAt // Keep stored created time
				triggerPlans = append(triggerPlans, triggerPlan)
			}

			beforeTriggers, afterTriggers := plan.OrderTriggers(triggerPlans)
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
						triggerPlan.CreatedAt,   // created
						"",                      // sql_mode
						triggerPlan.Definer,     // definer
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
				fks, err := fkTable.GetDeclaredForeignKeys(ctx)
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
			indexCols = append(indexCols, col.Name)
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
						ordinalPosition := i + 1 // Ordinal Positions starts at one

						rows = append(rows, Row{"def", db.Name(), index.ID(), "def", db.Name(), tbl.Name(), colName, ordinalPosition, nil, nil, nil, nil})
					}
				}
			}

			// Get FKs
			fkTable, ok := tbl.(ForeignKeyTable)
			if ok {
				fks, err := fkTable.GetDeclaredForeignKeys(ctx)
				if err != nil {
					return nil, err
				}

				for _, fk := range fks {
					for j, colName := range fk.Columns {
						ordinalPosition := j + 1

						referencedSchema := db.Name()
						referencedTableName := fk.ParentTable
						referencedColumnName := strings.Replace(fk.ParentColumns[j], "`", "", -1) // get rid of backticks

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
	collIter := NewCollationsIterator()
	for c, ok := collIter.Next(); ok; c, ok = collIter.Next() {
		rows = append(rows, Row{
			c.Name,
			c.CharacterSet.String(),
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
				name:    ColumnStatisticsTableName,
				schema:  columnStatisticsSchema,
				rowIter: columnStatisticsRowIter,
			},
			TablesTableName: &informationSchemaTable{
				name:    TablesTableName,
				schema:  tablesSchema,
				rowIter: tablesRowIter,
			},
			ColumnsTableName: &ColumnsTable{
				name: ColumnsTableName,
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
				name:    MysqlFirewallWhitelistTableName,
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
				name:    TableConstraintsExtensionsTableName,
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

// viewsInDatabase returns all views defined on the database given, consulting both the database itself as well as any
// views defined in session memory. Typically there will not be both types of views on a single database, but the
// interfaces do make it possible.
func viewsInDatabase(ctx *Context, db Database) ([]ViewDefinition, error) {
	var views []ViewDefinition
	dbName := db.Name()

	if privilegedDatabase, ok := db.(mysql_db.PrivilegedDatabase); ok {
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

func getTotalNumRows(ctx *Context, st StatisticsTable) (int64, error) {
	stats, err := st.Statistics(ctx)
	if err != nil {
		return 0, err
	}
	var c uint64
	if stats != nil {
		c = stats.RowCount()
	}

	// cardinality is int64 type, but NumRows return uint64
	// so casting it to int64 with a check for negative number
	cardinality := int64(c)
	if cardinality < 0 {
		cardinality = int64(0)
	}

	return cardinality, nil
}
