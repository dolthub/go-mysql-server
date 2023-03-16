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
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/dolthub/vitess/go/vt/sqlparser"

	. "github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/mysql_db"
	"github.com/dolthub/go-mysql-server/sql/parse"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

const (
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
	// ColumnsTableName is the name of the COLUMNS table.
	ColumnsTableName = "columns"
	// ColumnsExtensionsTableName is the name of the COLUMN_EXTENSIONS table.
	ColumnsExtensionsTableName = "columns_extensions"
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
	// OptimizerTraceTableName is the name of the OPTIMIZER_TRACE table.
	OptimizerTraceTableName = "optimizer_trace"
	// ParametersTableName is the name of the PARAMETERS table.
	ParametersTableName = "parameters"
	// PartitionsTableName is the name of the PARTITIONS table
	PartitionsTableName = "partitions"
	// PluginsTableName is the name of the PLUGINS table.
	PluginsTableName = "plugins"
	// ProcessListTableName is the name of the PROCESSLIST table
	ProcessListTableName = "processlist"
	// ProfilingTableName is the name of the PROFILING table.
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
	// RoutinesTableName is the name of the ROUTINES table.
	RoutinesTableName = "routines"
	// SchemaPrivilegesTableName is the name of the SCHEMA_PRIVILEGES table.
	SchemaPrivilegesTableName = "schema_privileges"
	// SchemataTableName is the name of the SCHEMATA table.
	SchemataTableName = "schemata"
	// SchemataExtensionsTableName is the name of the SCHEMATA_EXTENSIONS table.
	SchemataExtensionsTableName = "schemata_extensions"
	// StGeometryColumnsTableName is the name of the ST_GEOMETRY_COLUMNS table.
	StGeometryColumnsTableName = "st_geometry_columns"
	// StSpatialReferenceSystemsTableName is the name of the ST_SPATIAL_REFERENCE_SYSTEMS table.
	StSpatialReferenceSystemsTableName = "st_spatial_reference_systems"
	// StUnitsOfMeasureTableName is the name of the ST_UNITS_OF_MEASURE
	StUnitsOfMeasureTableName = "st_units_of_measure"
	// StatisticsTableName is the name of the STATISTICS table.
	StatisticsTableName = "statistics"
	// TableConstraintsTableName is the name of the TABLE_CONSTRAINTS table.
	TableConstraintsTableName = "table_constraints"
	// TableConstraintsExtensionsTableName is the name of the TABLE_CONSTRAINTS_EXTENSIONS table.
	TableConstraintsExtensionsTableName = "table_constraints_extensions"
	// TablePrivilegesTableName is the name of the TABLE_PRIVILEGES table.
	TablePrivilegesTableName = "table_privileges"
	// TablesTableName is the name of the TABLES table.
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

var sqlModeSetType = MustCreateSetType([]string{
	"REAL_AS_FLOAT", "PIPES_AS_CONCAT", "ANSI_QUOTES", "IGNORE_SPACE", "NOT_USED", "ONLY_FULL_GROUP_BY",
	"NO_UNSIGNED_SUBTRACTION", "NO_DIR_IN_CREATE", "NOT_USED_9", "NOT_USED_10", "NOT_USED_11", "NOT_USED_12",
	"NOT_USED_13", "NOT_USED_14", "NOT_USED_15", "NOT_USED_16", "NOT_USED_17", "NOT_USED_18", "ANSI",
	"NO_AUTO_VALUE_ON_ZERO", "NO_BACKSLASH_ESCAPES", "STRICT_TRANS_TABLES", "STRICT_ALL_TABLES", "NO_ZERO_IN_DATE",
	"NO_ZERO_DATE", "ALLOW_INVALID_DATES", "ERROR_FOR_DIVISION_BY_ZERO", "TRADITIONAL", "NOT_USED_29",
	"HIGH_NOT_PRECEDENCE", "NO_ENGINE_SUBSTITUTION", "PAD_CHAR_TO_FULL_LENGTH", "TIME_TRUNCATE_FRACTIONAL"}, Collation_Default)

var _ Database = (*informationSchemaDatabase)(nil)

type informationSchemaDatabase struct {
	name   string
	tables map[string]Table
}

type informationSchemaTable struct {
	name    string
	schema  Schema
	catalog Catalog
	reader  func(*Context, Catalog) (RowIter, error)
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

var administrableRoleAuthorizationsSchema = Schema{
	{Name: "USER", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 97), Default: nil, Nullable: true, Source: AdministrableRoleAuthorizationsTableName},
	{Name: "HOST", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 256), Default: nil, Nullable: true, Source: AdministrableRoleAuthorizationsTableName},
	{Name: "GRANTEE", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 97), Default: nil, Nullable: true, Source: AdministrableRoleAuthorizationsTableName},
	{Name: "GRANTEE_HOST", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 256), Default: nil, Nullable: true, Source: AdministrableRoleAuthorizationsTableName},
	{Name: "ROLE_NAME", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 255), Default: nil, Nullable: true, Source: AdministrableRoleAuthorizationsTableName},
	{Name: "ROLE_HOST", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 256), Default: nil, Nullable: true, Source: AdministrableRoleAuthorizationsTableName},
	{Name: "IS_GRANTABLE", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 3), Default: parse.MustStringToColumnDefaultValue(NewEmptyContext(), `""`, LongText, false), Nullable: false, Source: AdministrableRoleAuthorizationsTableName},
	{Name: "IS_DEFAULT", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 3), Default: nil, Nullable: true, Source: AdministrableRoleAuthorizationsTableName},
	{Name: "IS_MANDATORY", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 3), Default: parse.MustStringToColumnDefaultValue(NewEmptyContext(), `""`, LongText, false), Nullable: false, Source: AdministrableRoleAuthorizationsTableName},
}

var applicableRolesSchema = Schema{
	{Name: "USER", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 97), Default: nil, Nullable: true, Source: ApplicableRolesTableName},
	{Name: "HOST", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 256), Default: nil, Nullable: true, Source: ApplicableRolesTableName},
	{Name: "GRANTEE", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 97), Default: nil, Nullable: true, Source: ApplicableRolesTableName},
	{Name: "GRANTEE_HOST", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 256), Default: nil, Nullable: true, Source: ApplicableRolesTableName},
	{Name: "ROLE_NAME", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 255), Default: nil, Nullable: true, Source: ApplicableRolesTableName},
	{Name: "ROLE_HOST", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 256), Default: nil, Nullable: true, Source: ApplicableRolesTableName},
	{Name: "IS_GRANTABLE", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 3), Default: parse.MustStringToColumnDefaultValue(NewEmptyContext(), `""`, LongText, false), Nullable: false, Source: ApplicableRolesTableName},
	{Name: "IS_DEFAULT", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 3), Default: nil, Nullable: true, Source: ApplicableRolesTableName},
	{Name: "IS_MANDATORY", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 3), Default: parse.MustStringToColumnDefaultValue(NewEmptyContext(), `""`, LongText, false), Nullable: false, Source: ApplicableRolesTableName},
}

var characterSetsSchema = Schema{
	{Name: "CHARACTER_SET_NAME", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: CharacterSetsTableName},
	{Name: "DEFAULT_COLLATE_NAME", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: CharacterSetsTableName},
	{Name: "DESCRIPTION", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 2048), Default: nil, Nullable: false, Source: CharacterSetsTableName},
	{Name: "MAXLEN", Type: Uint32, Default: nil, Nullable: false, Source: CharacterSetsTableName},
}

var checkConstraintsSchema = Schema{
	{Name: "CONSTRAINT_CATALOG", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: CheckConstraintsTableName},
	{Name: "CONSTRAINT_SCHEMA", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: CheckConstraintsTableName},
	{Name: "CONSTRAINT_NAME", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: CheckConstraintsTableName},
	{Name: "CHECK_CLAUSE", Type: LongText, Default: nil, Nullable: false, Source: CheckConstraintsTableName},
}

var collationCharacterSetApplicabilitySchema = Schema{
	{Name: "COLLATION_NAME", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: CollationCharSetApplicabilityTableName},
	{Name: "CHARACTER_SET_NAME", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: CollationCharSetApplicabilityTableName},
}

var collationsSchema = Schema{
	{Name: "COLLATION_NAME", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: CollationsTableName},
	{Name: "CHARACTER_SET_NAME", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: CollationsTableName},
	{Name: "ID", Type: Uint64, Default: parse.MustStringToColumnDefaultValue(NewEmptyContext(), "0", Uint64, false), Nullable: false, Source: CollationsTableName},
	{Name: "IS_DEFAULT", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 3), Default: parse.MustStringToColumnDefaultValue(NewEmptyContext(), `""`, LongText, false), Nullable: false, Source: CollationsTableName},
	{Name: "IS_COMPILED", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 3), Default: parse.MustStringToColumnDefaultValue(NewEmptyContext(), `""`, LongText, false), Nullable: true, Source: CollationsTableName},
	{Name: "SORTLEN", Type: Uint32, Default: nil, Nullable: false, Source: CollationsTableName},
	{Name: "PAD_ATTRIBUTE", Type: MustCreateEnumType([]string{"PAD SPACE", "NO PAD"}, Collation_Default), Default: nil, Nullable: false, Source: CollationsTableName},
}

var columnPrivilegesSchema = Schema{
	{Name: "GRANTEE", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 292), Default: parse.MustStringToColumnDefaultValue(NewEmptyContext(), `""`, LongText, false), Nullable: false, Source: ColumnPrivilegesTableName},
	{Name: "TABLE_CATALOG", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 512), Default: parse.MustStringToColumnDefaultValue(NewEmptyContext(), `""`, LongText, false), Nullable: false, Source: ColumnPrivilegesTableName},
	{Name: "TABLE_SCHEMA", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: parse.MustStringToColumnDefaultValue(NewEmptyContext(), `""`, LongText, false), Nullable: false, Source: ColumnPrivilegesTableName},
	{Name: "TABLE_NAME", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: parse.MustStringToColumnDefaultValue(NewEmptyContext(), `""`, LongText, false), Nullable: false, Source: ColumnPrivilegesTableName},
	{Name: "COLUMN_NAME", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: parse.MustStringToColumnDefaultValue(NewEmptyContext(), `""`, LongText, false), Nullable: false, Source: ColumnPrivilegesTableName},
	{Name: "PRIVILEGE_TYPE", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: parse.MustStringToColumnDefaultValue(NewEmptyContext(), `""`, LongText, false), Nullable: false, Source: ColumnPrivilegesTableName},
	{Name: "IS_GRANTABLE", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 3), Default: parse.MustStringToColumnDefaultValue(NewEmptyContext(), `""`, LongText, false), Nullable: false, Source: ColumnPrivilegesTableName},
}

var columnStatisticsSchema = Schema{
	{Name: "SCHEMA_NAME", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: ColumnStatisticsTableName},
	{Name: "TABLE_NAME", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: ColumnStatisticsTableName},
	{Name: "COLUMN_NAME", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: ColumnStatisticsTableName},
	{Name: "HISTOGRAM", Type: JSON, Default: nil, Nullable: false, Source: ColumnStatisticsTableName},
}

var columnsSchema = Schema{
	{Name: "TABLE_CATALOG", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: ColumnsTableName},
	{Name: "TABLE_SCHEMA", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: ColumnsTableName},
	{Name: "TABLE_NAME", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: ColumnsTableName},
	{Name: "COLUMN_NAME", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: ColumnsTableName},
	{Name: "ORDINAL_POSITION", Type: Uint32, Default: nil, Nullable: false, Source: ColumnsTableName},
	{Name: "COLUMN_DEFAULT", Type: Text, Default: nil, Nullable: true, Source: ColumnsTableName},
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
	{Name: "COLUMN_KEY", Type: MustCreateEnumType([]string{"", "PRI", "UNI", "MUL"}, Collation_Default), Default: nil, Nullable: false, Source: ColumnsTableName},
	{Name: "EXTRA", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 256), Default: nil, Nullable: true, Source: ColumnsTableName},
	{Name: "PRIVILEGES", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 154), Default: nil, Nullable: true, Source: ColumnsTableName},
	{Name: "COLUMN_COMMENT", Type: Text, Default: nil, Nullable: false, Source: ColumnsTableName},
	{Name: "GENERATION_EXPRESSION", Type: LongText, Default: nil, Nullable: false, Source: ColumnsTableName},
	{Name: "SRS_ID", Type: Uint32, Default: nil, Nullable: true, Source: ColumnsTableName},
}

var columnsExtensionsSchema = Schema{
	{Name: "TABLE_CATALOG", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: ColumnsExtensionsTableName},
	{Name: "TABLE_SCHEMA", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: ColumnsExtensionsTableName},
	{Name: "TABLE_NAME", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: ColumnsExtensionsTableName},
	{Name: "COLUMN_NAME", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: ColumnsExtensionsTableName},
	{Name: "ENGINE_ATTRIBUTE", Type: JSON, Default: nil, Nullable: true, Source: ColumnsExtensionsTableName},
	{Name: "SECONDARY_ENGINE_ATTRIBUTE", Type: JSON, Default: nil, Nullable: true, Source: ColumnsExtensionsTableName},
}

var enabledRolesSchema = Schema{
	{Name: "ROLE_NAME", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 255), Default: nil, Nullable: true, Source: EnabledRolesTablesName},
	{Name: "ROLE_HOST", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 255), Default: nil, Nullable: true, Source: EnabledRolesTablesName},
	{Name: "IS_DEFAULT", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 3), Default: nil, Nullable: true, Source: EnabledRolesTablesName},
	{Name: "IS_MANDATORY", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 3), Default: parse.MustStringToColumnDefaultValue(NewEmptyContext(), `""`, LongText, false), Nullable: false, Source: EnabledRolesTablesName},
}

var enginesSchema = Schema{
	{Name: "ENGINE", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: parse.MustStringToColumnDefaultValue(NewEmptyContext(), `""`, LongText, false), Nullable: false, Source: EnginesTableName},
	{Name: "SUPPORT", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 8), Default: parse.MustStringToColumnDefaultValue(NewEmptyContext(), `""`, LongText, false), Nullable: false, Source: EnginesTableName},
	{Name: "COMMENT", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 80), Default: parse.MustStringToColumnDefaultValue(NewEmptyContext(), `""`, LongText, false), Nullable: false, Source: EnginesTableName},
	{Name: "TRANSACTIONS", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 3), Default: parse.MustStringToColumnDefaultValue(NewEmptyContext(), `""`, LongText, false), Nullable: true, Source: EnginesTableName},
	{Name: "XA", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 3), Default: parse.MustStringToColumnDefaultValue(NewEmptyContext(), `""`, LongText, false), Nullable: true, Source: EnginesTableName},
	{Name: "SAVEPOINTS", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 3), Default: parse.MustStringToColumnDefaultValue(NewEmptyContext(), `""`, LongText, false), Nullable: true, Source: EnginesTableName},
}

var eventsSchema = Schema{
	{Name: "EVENT_CATALOG", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: EventsTableName},
	{Name: "EVENT_SCHEMA", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: EventsTableName},
	{Name: "EVENT_NAME", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: EventsTableName},
	{Name: "DEFINER", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 288), Default: nil, Nullable: false, Source: EventsTableName},
	{Name: "TIME_ZONE", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: EventsTableName},
	{Name: "EVENT_BODY", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 3), Default: parse.MustStringToColumnDefaultValue(NewEmptyContext(), `""`, LongText, false), Nullable: false, Source: EventsTableName},
	{Name: "EVENT_DEFINITION", Type: LongText, Default: nil, Nullable: false, Source: EventsTableName},
	{Name: "EVENT_TYPE", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 9), Default: parse.MustStringToColumnDefaultValue(NewEmptyContext(), `""`, LongText, false), Nullable: false, Source: EventsTableName},
	{Name: "EXECUTE_AT", Type: Datetime, Default: nil, Nullable: true, Source: EventsTableName},
	{Name: "INTERVAL_VALUE", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 256), Default: nil, Nullable: true, Source: EventsTableName},
	{Name: "INTERVAL_FIELD", Type: MustCreateEnumType([]string{
		"YEAR", "QUARTER", "MONTH", "DAY", "HOUR", "MINUTE", "WEEK", "SECOND", "MICROSECOND", "YEAR_MONTH",
		"DAY_HOUR", "DAY_MINUTE", "DAY_SECOND", "HOUR_MINUTE", "HOUR_SECOND", "MINUTE_SECOND",
		"DAY_MICROSECOND", "HOUR_MICROSECOND", "MINUTE_MICROSECOND", "SECOND_MICROSECOND"}, Collation_Default), Default: nil, Nullable: true, Source: EventsTableName},
	{Name: "SQL_MODE", Type: sqlModeSetType, Default: nil, Nullable: false, Source: EventsTableName},
	{Name: "STARTS", Type: Datetime, Default: nil, Nullable: true, Source: EventsTableName},
	{Name: "ENDS", Type: Datetime, Default: nil, Nullable: true, Source: EventsTableName},
	{Name: "STATUS", Type: MustCreateEnumType([]string{"ENABLED", "DISABLED", "SLAVESIDE_DISABLED"}, Collation_Default), Default: nil, Nullable: false, Source: EventsTableName},
	{Name: "ON_COMPLETION", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 12), Default: parse.MustStringToColumnDefaultValue(NewEmptyContext(), `""`, MustCreateStringWithDefaults(sqltypes.VarChar, 12), false), Nullable: false, Source: EventsTableName},
	{Name: "CREATED", Type: Timestamp, Default: nil, Nullable: false, Source: EventsTableName},
	{Name: "LAST_ALTERED", Type: Timestamp, Default: nil, Nullable: false, Source: EventsTableName},
	{Name: "LAST_EXECUTED", Type: Datetime, Default: nil, Nullable: true, Source: EventsTableName},
	{Name: "EVENT_COMMENT", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 2048), Default: nil, Nullable: false, Source: EventsTableName},
	{Name: "ORIGINATOR", Type: Uint32, Default: nil, Nullable: false, Source: EventsTableName},
	{Name: "CHARACTER_SET_CLIENT", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: EventsTableName},
	{Name: "COLLATION_CONNECTION", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: EventsTableName},
	{Name: "DATABASE_COLLATION", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: EventsTableName},
}

var filesSchema = Schema{
	{Name: "FILE_ID", Type: Int64, Default: nil, Nullable: true, Source: FilesTableName},
	{Name: "FILE_NAME", Type: Text, Default: nil, Nullable: true, Source: FilesTableName},
	{Name: "FILE_TYPE", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 256), Default: nil, Nullable: true, Source: FilesTableName},
	{Name: "TABLESPACE_NAME", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 268), Default: nil, Nullable: true, Source: FilesTableName},
	{Name: "TABLE_CATALOG", Type: MustCreateStringWithDefaults(sqltypes.Char, 0), Default: parse.MustStringToColumnDefaultValue(NewEmptyContext(), `""`, MustCreateStringWithDefaults(sqltypes.Char, 0), false), Nullable: true, Source: FilesTableName},
	{Name: "TABLE_SCHEMA", Type: MustCreateBinary(sqltypes.Binary, 0), Default: nil, Nullable: true, Source: FilesTableName},
	{Name: "TABLE_NAME", Type: MustCreateBinary(sqltypes.Binary, 0), Default: nil, Nullable: true, Source: FilesTableName},
	{Name: "LOGFILE_GROUP_NAME", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 256), Default: nil, Nullable: true, Source: FilesTableName},
	{Name: "LOGFILE_GROUP_NUMBER", Type: Int64, Default: nil, Nullable: true, Source: FilesTableName},
	{Name: "ENGINE", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: FilesTableName},
	{Name: "FULLTEXT_KEYS", Type: MustCreateBinary(sqltypes.Binary, 0), Default: nil, Nullable: true, Source: FilesTableName},
	{Name: "DELETED_ROWS", Type: MustCreateBinary(sqltypes.Binary, 0), Default: nil, Nullable: true, Source: FilesTableName},
	{Name: "UPDATE_COUNT", Type: MustCreateBinary(sqltypes.Binary, 0), Default: nil, Nullable: true, Source: FilesTableName},
	{Name: "FREE_EXTENTS", Type: Int64, Default: nil, Nullable: true, Source: FilesTableName},
	{Name: "TOTAL_EXTENTS", Type: Int64, Default: nil, Nullable: true, Source: FilesTableName},
	{Name: "EXTENT_SIZE", Type: Int64, Default: nil, Nullable: true, Source: FilesTableName},
	{Name: "INITIAL_SIZE", Type: Int64, Default: nil, Nullable: true, Source: FilesTableName},
	{Name: "MAXIMUM_SIZE", Type: Int64, Default: nil, Nullable: true, Source: FilesTableName},
	{Name: "AUTOEXTEND_SIZE", Type: Int64, Default: nil, Nullable: true, Source: FilesTableName},
	{Name: "CREATION_TIME", Type: MustCreateBinary(sqltypes.Binary, 0), Default: nil, Nullable: true, Source: FilesTableName},
	{Name: "LAST_UPDATE_TIME", Type: MustCreateBinary(sqltypes.Binary, 0), Default: nil, Nullable: true, Source: FilesTableName},
	{Name: "LAST_ACCESS_TIME", Type: MustCreateBinary(sqltypes.Binary, 0), Default: nil, Nullable: true, Source: FilesTableName},
	{Name: "RECOVER_TIME", Type: MustCreateBinary(sqltypes.Binary, 0), Default: nil, Nullable: true, Source: FilesTableName},
	{Name: "TRANSACTION_COUNTER", Type: MustCreateBinary(sqltypes.Binary, 0), Default: nil, Nullable: true, Source: FilesTableName},
	{Name: "VERSION", Type: Int64, Default: nil, Nullable: true, Source: FilesTableName},
	{Name: "ROW_FORMAT", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 256), Default: nil, Nullable: true, Source: FilesTableName},
	{Name: "TABLE_ROWS", Type: MustCreateBinary(sqltypes.Binary, 0), Default: nil, Nullable: true, Source: FilesTableName},
	{Name: "AVG_ROW_LENGTH", Type: MustCreateBinary(sqltypes.Binary, 0), Default: nil, Nullable: true, Source: FilesTableName},
	{Name: "DATA_LENGTH", Type: MustCreateBinary(sqltypes.Binary, 0), Default: nil, Nullable: true, Source: FilesTableName},
	{Name: "MAX_DATA_LENGTH", Type: MustCreateBinary(sqltypes.Binary, 0), Default: nil, Nullable: true, Source: FilesTableName},
	{Name: "INDEX_LENGTH", Type: MustCreateBinary(sqltypes.Binary, 0), Default: nil, Nullable: true, Source: FilesTableName},
	{Name: "DATA_FREE", Type: Int64, Default: nil, Nullable: true, Source: FilesTableName},
	{Name: "CREATE_TIME", Type: MustCreateBinary(sqltypes.Binary, 0), Default: nil, Nullable: true, Source: FilesTableName},
	{Name: "UPDATE_TIME", Type: MustCreateBinary(sqltypes.Binary, 0), Default: nil, Nullable: true, Source: FilesTableName},
	{Name: "CHECK_TIME", Type: MustCreateBinary(sqltypes.Binary, 0), Default: nil, Nullable: true, Source: FilesTableName},
	{Name: "CHECKSUM", Type: MustCreateBinary(sqltypes.Binary, 0), Default: nil, Nullable: true, Source: FilesTableName},
	{Name: "STATUS", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 256), Default: nil, Nullable: true, Source: FilesTableName},
	{Name: "EXTRA", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 256), Default: nil, Nullable: true, Source: FilesTableName},
}

var keyColumnUsageSchema = Schema{
	{Name: "CONSTRAINT_CATALOG", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: KeyColumnUsageTableName},
	{Name: "CONSTRAINT_SCHEMA", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: KeyColumnUsageTableName},
	{Name: "CONSTRAINT_NAME", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: KeyColumnUsageTableName},
	{Name: "TABLE_CATALOG", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: KeyColumnUsageTableName},
	{Name: "TABLE_SCHEMA", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: KeyColumnUsageTableName},
	{Name: "TABLE_NAME", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: KeyColumnUsageTableName},
	{Name: "COLUMN_NAME", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: KeyColumnUsageTableName},
	{Name: "ORDINAL_POSITION", Type: Uint32, Default: parse.MustStringToColumnDefaultValue(NewEmptyContext(), "0", Uint32, false), Nullable: false, Source: KeyColumnUsageTableName},
	{Name: "POSITION_IN_UNIQUE_CONSTRAINT", Type: Uint32, Default: nil, Nullable: true, Source: KeyColumnUsageTableName},
	{Name: "REFERENCED_TABLE_SCHEMA", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: KeyColumnUsageTableName},
	{Name: "REFERENCED_TABLE_NAME", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: KeyColumnUsageTableName},
	{Name: "REFERENCED_COLUMN_NAME", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: KeyColumnUsageTableName},
}

var keywordsSchema = Schema{
	{Name: "WORD", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 128), Default: nil, Nullable: true, Source: KeywordsTableName},
	{Name: "RESERVED", Type: Int32, Default: nil, Nullable: true, Source: KeywordsTableName},
}

var optimizerTraceSchema = Schema{
	{Name: "QUERY", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 65535), Default: nil, Nullable: false, Source: OptimizerTraceTableName},
	{Name: "TRACE", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 65535), Default: nil, Nullable: false, Source: OptimizerTraceTableName},
	{Name: "MISSING_BYTES_BEYOND_MAX_MEM_SIZE", Type: Int32, Default: nil, Nullable: false, Source: OptimizerTraceTableName},
	{Name: "INSUFFICIENT_PRIVILEGES", Type: MustCreateBitType(1), Default: nil, Nullable: false, Source: OptimizerTraceTableName},
}

var parametersSchema = Schema{
	{Name: "SPECIFIC_CATALOG", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: ParametersTableName},
	{Name: "SPECIFIC_SCHEMA", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: ParametersTableName},
	{Name: "SPECIFIC_NAME", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: ParametersTableName},
	{Name: "ORDINAL_POSITION", Type: Uint64, Default: parse.MustStringToColumnDefaultValue(NewEmptyContext(), "0", Uint64, false), Nullable: false, Source: ParametersTableName},
	{Name: "PARAMETER_MODE", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 5), Default: nil, Nullable: true, Source: ParametersTableName},
	{Name: "PARAMETER_NAME", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: ParametersTableName},
	{Name: "DATA_TYPE", Type: LongText, Default: nil, Nullable: true, Source: ParametersTableName},
	{Name: "CHARACTER_MAXIMUM_LENGTH", Type: Int64, Default: nil, Nullable: true, Source: ParametersTableName},
	{Name: "CHARACTER_OCTET_LENGTH", Type: Int64, Default: nil, Nullable: true, Source: ParametersTableName},
	{Name: "NUMERIC_PRECISION", Type: Uint32, Default: nil, Nullable: true, Source: ParametersTableName},
	{Name: "NUMERIC_SCALE", Type: Int64, Default: nil, Nullable: true, Source: ParametersTableName},
	{Name: "CHARACTER_SET_NAME", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: ParametersTableName},
	{Name: "COLLATION_NAME", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: ParametersTableName},
	{Name: "DTD_IDENTIFIER", Type: MediumText, Default: nil, Nullable: false, Source: ParametersTableName},
	{Name: "RESOURCE_GROUP_TYPE", Type: MustCreateEnumType([]string{"FUNCTION", "PROCEDURE"}, Collation_Default), Default: nil, Nullable: false, Source: ParametersTableName},
}

var partitionsSchema = Schema{
	{Name: "TABLE_CATALOG", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: PartitionsTableName},
	{Name: "TABLE_SCHEMA", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: PartitionsTableName},
	{Name: "TABLE_NAME", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: PartitionsTableName},
	{Name: "PARTITION_NAME", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: PartitionsTableName},
	{Name: "SUBPARTITION_NAME", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: PartitionsTableName},
	{Name: "PARTITION_ORDINAL_POSITION", Type: Uint32, Default: nil, Nullable: true, Source: PartitionsTableName},
	{Name: "SUBPARTITION_ORDINAL_POSITION", Type: Uint32, Default: nil, Nullable: true, Source: PartitionsTableName},
	{Name: "PARTITION_METHOD", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 13), Default: nil, Nullable: true, Source: PartitionsTableName},
	{Name: "SUBPARTITION_METHOD", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 13), Default: nil, Nullable: true, Source: PartitionsTableName},
	{Name: "PARTITION_EXPRESSION", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 2048), Default: nil, Nullable: true, Source: PartitionsTableName},
	{Name: "SUBPARTITION_EXPRESSION", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 2048), Default: nil, Nullable: true, Source: PartitionsTableName},
	{Name: "PARTITION_DESCRIPTION", Type: Text, Default: nil, Nullable: true, Source: PartitionsTableName},
	{Name: "TABLE_ROWS", Type: Uint64, Default: nil, Nullable: true, Source: PartitionsTableName},
	{Name: "AVG_ROW_LENGTH", Type: Uint64, Default: nil, Nullable: true, Source: PartitionsTableName},
	{Name: "DATA_LENGTH", Type: Uint64, Default: nil, Nullable: true, Source: PartitionsTableName},
	{Name: "MAX_DATA_LENGTH", Type: Uint64, Default: nil, Nullable: true, Source: PartitionsTableName},
	{Name: "INDEX_LENGTH", Type: Uint64, Default: nil, Nullable: true, Source: PartitionsTableName},
	{Name: "DATA_FREE", Type: Uint64, Default: nil, Nullable: true, Source: PartitionsTableName},
	{Name: "CREATE_TIME", Type: Timestamp, Default: nil, Nullable: false, Source: PartitionsTableName},
	{Name: "UPDATE_TIME", Type: Datetime, Default: nil, Nullable: true, Source: PartitionsTableName},
	{Name: "CHECK_TIME", Type: Datetime, Default: nil, Nullable: true, Source: PartitionsTableName},
	{Name: "CHECKSUM", Type: Int64, Default: nil, Nullable: true, Source: PartitionsTableName},
	{Name: "PARTITION_COMMENT", Type: Text, Default: nil, Nullable: false, Source: PartitionsTableName},
	{Name: "NODEGROUP", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 256), Default: nil, Nullable: true, Source: PartitionsTableName},
	{Name: "TABLESPACE_NAME", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 268), Default: nil, Nullable: true, Source: PartitionsTableName},
}

var pluginsSchema = Schema{
	{Name: "PLUGIN_NAME", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: PluginsTableName},
	{Name: "PLUGIN_VERSION", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 20), Default: nil, Nullable: false, Source: PluginsTableName},
	{Name: "PLUGIN_STATUS", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 10), Default: nil, Nullable: false, Source: PluginsTableName},
	{Name: "PLUGIN_TYPE", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 80), Default: nil, Nullable: false, Source: PluginsTableName},
	{Name: "PLUGIN_TYPE_VERSION", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 20), Default: nil, Nullable: false, Source: PluginsTableName},
	{Name: "PLUGIN_LIBRARY", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: PluginsTableName},
	{Name: "PLUGIN_LIBRARY_VERSION", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 20), Default: nil, Nullable: true, Source: PluginsTableName},
	{Name: "PLUGIN_AUTHOR", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: PluginsTableName},
	{Name: "PLUGIN_DESCRIPTION", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 65535), Default: nil, Nullable: true, Source: PluginsTableName},
	{Name: "PLUGIN_LICENSE", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 80), Default: nil, Nullable: true, Source: PluginsTableName},
	{Name: "LOAD_OPTION", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: PluginsTableName},
}

var processListSchema = Schema{
	{Name: "ID", Type: Uint64, Default: nil, Nullable: false, Source: ProcessListTableName},
	{Name: "USER", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 32), Default: nil, Nullable: false, Source: ProcessListTableName},
	{Name: "HOST", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 261), Default: nil, Nullable: false, Source: ProcessListTableName},
	{Name: "DB", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: ProcessListTableName},
	{Name: "COMMAND", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 16), Default: nil, Nullable: false, Source: ProcessListTableName},
	{Name: "TIME", Type: Int32, Default: nil, Nullable: false, Source: ProcessListTableName},
	{Name: "STATE", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: ProcessListTableName},
	{Name: "INFO", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 65535), Default: nil, Nullable: true, Source: ProcessListTableName},
}

var profilingSchema = Schema{
	{Name: "QUERY_ID", Type: Int32, Default: nil, Nullable: false, Source: ProfilingTableName},
	{Name: "SEQ", Type: Int32, Default: nil, Nullable: false, Source: ProfilingTableName},
	{Name: "STATE", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 30), Default: nil, Nullable: false, Source: ProfilingTableName},
	{Name: "DURATION", Type: MustCreateDecimalType(DecimalTypeMaxPrecision, 0), Default: nil, Nullable: false, Source: ProfilingTableName},
	{Name: "CPU_USER", Type: MustCreateDecimalType(DecimalTypeMaxPrecision, 0), Default: nil, Nullable: true, Source: ProfilingTableName},
	{Name: "CPU_SYSTEM", Type: MustCreateDecimalType(DecimalTypeMaxPrecision, 0), Default: nil, Nullable: true, Source: ProfilingTableName},
	{Name: "CONTEXT_VOLUNTARY", Type: Int32, Default: nil, Nullable: true, Source: ProfilingTableName},
	{Name: "CONTEXT_INVOLUNTARY", Type: Int32, Default: nil, Nullable: true, Source: ProfilingTableName},
	{Name: "BLOCK_OPS_IN", Type: Int32, Default: nil, Nullable: true, Source: ProfilingTableName},
	{Name: "BLOCK_OPS_OUT", Type: Int32, Default: nil, Nullable: true, Source: ProfilingTableName},
	{Name: "MESSAGES_SENT", Type: Int32, Default: nil, Nullable: true, Source: ProfilingTableName},
	{Name: "MESSAGES_RECEIVED", Type: Int32, Default: nil, Nullable: true, Source: ProfilingTableName},
	{Name: "PAGE_FAULTS_MAJOR", Type: Int32, Default: nil, Nullable: true, Source: ProfilingTableName},
	{Name: "PAGE_FAULTS_MINOR", Type: Int32, Default: nil, Nullable: true, Source: ProfilingTableName},
	{Name: "SWAPS", Type: Int32, Default: nil, Nullable: true, Source: ProfilingTableName},
	{Name: "SOURCE_FUNCTION", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 30), Default: nil, Nullable: true, Source: ProfilingTableName},
	{Name: "SOURCE_FILE", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 20), Default: nil, Nullable: true, Source: ProfilingTableName},
	{Name: "SOURCE_LINE", Type: Int32, Default: nil, Nullable: true, Source: ProfilingTableName},
}

var referentialConstraintsSchema = Schema{
	{Name: "CONSTRAINT_CATALOG", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: ReferentialConstraintsTableName},
	{Name: "CONSTRAINT_SCHEMA", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: ReferentialConstraintsTableName},
	{Name: "CONSTRAINT_NAME", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: ReferentialConstraintsTableName},
	{Name: "UNIQUE_CONSTRAINT_CATALOG", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: ReferentialConstraintsTableName},
	{Name: "UNIQUE_CONSTRAINT_SCHEMA", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: ReferentialConstraintsTableName},
	{Name: "UNIQUE_CONSTRAINT_NAME", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: ReferentialConstraintsTableName},
	{Name: "MATCH_OPTION", Type: MustCreateEnumType([]string{"NONE", "PARTIAL", "FULL"}, Collation_Default), Default: nil, Nullable: false, Source: ReferentialConstraintsTableName},
	{Name: "UPDATE_RULE", Type: MustCreateEnumType([]string{"NO ACTION", "RESTRICT", "CASCADE", "SET NULL", "SET DEFAULT"}, Collation_Default), Default: nil, Nullable: false, Source: ReferentialConstraintsTableName},
	{Name: "DELETE_RULE", Type: MustCreateEnumType([]string{"NO ACTION", "RESTRICT", "CASCADE", "SET NULL", "SET DEFAULT"}, Collation_Default), Default: nil, Nullable: false, Source: ReferentialConstraintsTableName},
	{Name: "TABLE_NAME", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: ReferentialConstraintsTableName},
	{Name: "REFERENCED_TABLE_NAME", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: ReferentialConstraintsTableName},
}

var resourceGroupsSchema = Schema{
	{Name: "RESOURCE_GROUP_NAME", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: ResourceGroupsTableName},
	{Name: "RESOURCE_GROUP_TYPE", Type: MustCreateEnumType([]string{"SYSTEM", "USER"}, Collation_Default), Default: nil, Nullable: false, Source: ResourceGroupsTableName},
	{Name: "RESOURCE_GROUP_ENABLE", Type: MustCreateBitType(1), Default: nil, Nullable: false, Source: ResourceGroupsTableName},
	{Name: "VPCUS_IDS", Type: Blob, Default: nil, Nullable: true, Source: ResourceGroupsTableName},
	{Name: "THREAD_PRIORITY", Type: Int32, Default: nil, Nullable: false, Source: ResourceGroupsTableName},
}

var roleColumnGrantsSchema = Schema{
	{Name: "GRANTOR", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 97), Default: nil, Nullable: true, Source: RoleColumnGrantsTableName},
	{Name: "GRANTOR_HOST", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 256), Default: nil, Nullable: true, Source: RoleColumnGrantsTableName},
	{Name: "GRANTEE", Type: MustCreateStringWithDefaults(sqltypes.Char, 32), Default: nil, Nullable: false, Source: RoleColumnGrantsTableName},
	{Name: "GRANTEE_HOST", Type: MustCreateStringWithDefaults(sqltypes.Char, 255), Default: nil, Nullable: false, Source: RoleColumnGrantsTableName},
	{Name: "TABLE_CATALOG", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 3), Default: nil, Nullable: false, Source: RoleColumnGrantsTableName},
	{Name: "TABLE_SCHEMA", Type: MustCreateStringWithDefaults(sqltypes.Char, 64), Default: nil, Nullable: false, Source: RoleColumnGrantsTableName},
	{Name: "TABLE_NAME", Type: MustCreateStringWithDefaults(sqltypes.Char, 64), Default: nil, Nullable: false, Source: RoleColumnGrantsTableName},
	{Name: "COLUMN_NAME", Type: MustCreateStringWithDefaults(sqltypes.Char, 64), Default: nil, Nullable: false, Source: RoleColumnGrantsTableName},
	{Name: "PRIVILEGE_TYPE", Type: MustCreateSetType([]string{"Select", "Insert", "Update", "References"}, Collation_Default), Default: nil, Nullable: false, Source: RoleColumnGrantsTableName},
	{Name: "IS_GRANTABLE", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 3), Default: nil, Nullable: false, Source: RoleColumnGrantsTableName},
}

var roleRoutineGrantsSchema = Schema{
	{Name: "GRANTOR", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 97), Default: nil, Nullable: true, Source: RoleRoutineGrantsTableName},
	{Name: "GRANTOR_HOST", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 256), Default: nil, Nullable: true, Source: RoleRoutineGrantsTableName},
	{Name: "GRANTEE", Type: MustCreateStringWithDefaults(sqltypes.Char, 32), Default: nil, Nullable: false, Source: RoleRoutineGrantsTableName},
	{Name: "GRANTEE_HOST", Type: MustCreateStringWithDefaults(sqltypes.Char, 255), Default: nil, Nullable: false, Source: RoleRoutineGrantsTableName},
	{Name: "SPECIFIC_CATALOG", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 3), Default: nil, Nullable: false, Source: RoleRoutineGrantsTableName},
	{Name: "SPECIFIC_SCHEMA", Type: MustCreateStringWithDefaults(sqltypes.Char, 64), Default: nil, Nullable: false, Source: RoleRoutineGrantsTableName},
	{Name: "SPECIFIC_NAME", Type: MustCreateStringWithDefaults(sqltypes.Char, 64), Default: nil, Nullable: false, Source: RoleRoutineGrantsTableName},
	{Name: "ROUTINE_CATALOG", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 3), Default: nil, Nullable: false, Source: RoleRoutineGrantsTableName},
	{Name: "ROUTINE_SCHEMA", Type: MustCreateStringWithDefaults(sqltypes.Char, 64), Default: nil, Nullable: false, Source: RoleRoutineGrantsTableName},
	{Name: "ROUTINE_NAME", Type: MustCreateStringWithDefaults(sqltypes.Char, 64), Default: nil, Nullable: false, Source: RoleRoutineGrantsTableName},
	{Name: "PRIVILEGE_TYPE", Type: MustCreateSetType([]string{"Execute", "Alter Routine", "Grant"}, Collation_Default), Default: nil, Nullable: false, Source: RoleRoutineGrantsTableName},
	{Name: "IS_GRANTABLE", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 3), Default: nil, Nullable: false, Source: RoleRoutineGrantsTableName},
}

var roleTableGrantsSchema = Schema{
	{Name: "GRANTOR", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 97), Default: nil, Nullable: true, Source: RoleTableGrantsTableName},
	{Name: "GRANTOR_HOST", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 256), Default: nil, Nullable: true, Source: RoleTableGrantsTableName},
	{Name: "GRANTEE", Type: MustCreateStringWithDefaults(sqltypes.Char, 32), Default: nil, Nullable: false, Source: RoleTableGrantsTableName},
	{Name: "GRANTEE_HOST", Type: MustCreateStringWithDefaults(sqltypes.Char, 255), Default: nil, Nullable: false, Source: RoleTableGrantsTableName},
	{Name: "TABLE_CATALOG", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 3), Default: nil, Nullable: false, Source: RoleTableGrantsTableName},
	{Name: "TABLE_SCHEMA", Type: MustCreateStringWithDefaults(sqltypes.Char, 64), Default: nil, Nullable: false, Source: RoleTableGrantsTableName},
	{Name: "TABLE_NAME", Type: MustCreateStringWithDefaults(sqltypes.Char, 64), Default: nil, Nullable: false, Source: RoleTableGrantsTableName},
	{Name: "PRIVILEGE_TYPE", Type: MustCreateSetType([]string{"Select", "Insert", "Update", "Delete", "Create", "Drop", "Grant", "References", "Index", "Alter", "Create View", "Show view", "Trigger"}, Collation_Default), Default: nil, Nullable: false, Source: RoleTableGrantsTableName},
	{Name: "IS_GRANTABLE", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 3), Default: nil, Nullable: false, Source: RoleTableGrantsTableName},
}

var routinesSchema = Schema{
	{Name: "SPECIFIC_NAME", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: RoutinesTableName},
	{Name: "ROUTINE_CATALOG", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: RoutinesTableName},
	{Name: "ROUTINE_SCHEMA", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: RoutinesTableName},
	{Name: "ROUTINE_NAME", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: RoutinesTableName},
	{Name: "ROUTINE_TYPE", Type: MustCreateEnumType([]string{"FUNCTION", "PROCEDURE"}, Collation_Default), Default: nil, Nullable: false, Source: RoutinesTableName},
	{Name: "DATA_TYPE", Type: LongText, Default: nil, Nullable: true, Source: RoutinesTableName},
	{Name: "CHARACTER_MAXIMUM_LENGTH", Type: Int64, Default: nil, Nullable: true, Source: RoutinesTableName},
	{Name: "CHARACTER_OCTET_LENGTH", Type: Int64, Default: nil, Nullable: true, Source: RoutinesTableName},
	{Name: "NUMERIC_PRECISION", Type: Uint32, Default: nil, Nullable: true, Source: RoutinesTableName},
	{Name: "NUMERIC_SCALE", Type: Uint32, Default: nil, Nullable: true, Source: RoutinesTableName},
	{Name: "DATETIME_PRECISION", Type: Uint32, Default: nil, Nullable: true, Source: RoutinesTableName},
	{Name: "CHARACTER_SET_NAME", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: RoutinesTableName},
	{Name: "COLLATION_NAME", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: RoutinesTableName},
	{Name: "DTD_IDENTIFIER", Type: LongText, Default: nil, Nullable: true, Source: RoutinesTableName},
	{Name: "ROUTINE_BODY", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 3), Default: parse.MustStringToColumnDefaultValue(NewEmptyContext(), `""`, MustCreateStringWithDefaults(sqltypes.VarChar, 3), false), Nullable: false, Source: RoutinesTableName},
	{Name: "ROUTINE_DEFINITION", Type: LongText, Default: nil, Nullable: true, Source: RoutinesTableName},
	{Name: "EXTERNAL_NAME", Type: MustCreateBinary(sqltypes.Binary, 0), Default: nil, Nullable: true, Source: RoutinesTableName},
	{Name: "EXTERNAL_LANGUAGE", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: parse.MustStringToColumnDefaultValue(NewEmptyContext(), `"SQL"`, MustCreateStringWithDefaults(sqltypes.VarChar, 64), false), Nullable: false, Source: RoutinesTableName},
	{Name: "PARAMETER_STYLE", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 3), Default: nil, Nullable: false, Source: RoutinesTableName},
	{Name: "IS_DETERMINISTIC", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 3), Default: nil, Nullable: false, Source: RoutinesTableName},
	{Name: "SQL_DATA_ACCESS", Type: MustCreateEnumType([]string{"CONTAINS SQL", "NO SQL", "READS SQL DATA", "MODIFIES SQL DATA"}, Collation_Default), Default: nil, Nullable: false, Source: RoutinesTableName},
	{Name: "SQL_PATH", Type: MustCreateBinary(sqltypes.Binary, 0), Default: nil, Nullable: true, Source: RoutinesTableName},
	{Name: "SECURITY_TYPE", Type: MustCreateEnumType([]string{"DEFAULT", "INVOKER", "DEFINER"}, Collation_Default), Default: nil, Nullable: false, Source: RoutinesTableName},
	{Name: "CREATED", Type: Timestamp, Default: nil, Nullable: false, Source: RoutinesTableName},
	{Name: "LAST_ALTERED", Type: Timestamp, Default: nil, Nullable: false, Source: RoutinesTableName},
	{Name: "SQL_MODE", Type: sqlModeSetType, Default: nil, Nullable: false, Source: RoutinesTableName},
	{Name: "ROUTINE_COMMENT", Type: Text, Default: nil, Nullable: false, Source: RoutinesTableName},
	{Name: "DEFINER", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 288), Default: nil, Nullable: false, Source: RoutinesTableName},
	{Name: "CHARACTER_SET_CLIENT", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: RoutinesTableName},
	{Name: "COLLATION_CONNECTION", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: RoutinesTableName},
	{Name: "DATABASE_COLLATION", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: RoutinesTableName},
}

var schemaPrivilegesTableName = Schema{
	{Name: "GRANTEE", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 292), Default: nil, Nullable: false, Source: SchemaPrivilegesTableName},
	{Name: "TABLE_CATALOG", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 512), Default: nil, Nullable: false, Source: SchemaPrivilegesTableName},
	{Name: "TABLE_SCHEMA", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: SchemaPrivilegesTableName},
	{Name: "PRIVILEGE_TYPE", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: SchemaPrivilegesTableName},
	{Name: "IS_GRANTABLE", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 3), Default: nil, Nullable: false, Source: SchemaPrivilegesTableName},
}

var schemataSchema = Schema{
	{Name: "CATALOG_NAME", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: SchemataTableName},
	{Name: "SCHEMA_NAME", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: SchemataTableName},
	{Name: "DEFAULT_CHARACTER_SET_NAME", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: SchemataTableName},
	{Name: "DEFAULT_COLLATION_NAME", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: SchemataTableName},
	{Name: "SQL_PATH", Type: MustCreateBinary(sqltypes.Binary, 0), Default: nil, Nullable: true, Source: SchemataTableName},
	{Name: "DEFAULT_ENCRYPTION", Type: MustCreateEnumType([]string{"NO", "YES"}, Collation_Default), Default: nil, Nullable: false, Source: SchemataTableName},
}

var schemataExtensionsTableName = Schema{
	{Name: "CATALOG_NAME", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: SchemataExtensionsTableName},
	{Name: "SCHEMA_NAME", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: SchemataExtensionsTableName},
	{Name: "OPTIONS", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 256), Default: nil, Nullable: true, Source: SchemataExtensionsTableName},
}

var stGeometryColumnsSchema = Schema{
	{Name: "TABLE_CATALOG", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: StGeometryColumnsTableName},
	{Name: "TABLE_SCHEMA", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: StGeometryColumnsTableName},
	{Name: "TABLE_NAME", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: StGeometryColumnsTableName},
	{Name: "COLUMN_NAME", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: StGeometryColumnsTableName},
	{Name: "SRS_NAME", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 80), Default: nil, Nullable: true, Source: StGeometryColumnsTableName},
	{Name: "SRS_ID", Type: Uint32, Default: nil, Nullable: true, Source: StGeometryColumnsTableName},
	{Name: "GEOMETRY_TYPE_NAME", Type: LongText, Default: nil, Nullable: true, Source: StGeometryColumnsTableName},
}

var stSpatialReferenceSystemsSchema = Schema{
	{Name: "SRS_NAME", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 80), Default: nil, Nullable: false, Source: StSpatialReferenceSystemsTableName},
	{Name: "SRS_ID", Type: Uint32, Default: nil, Nullable: false, Source: StSpatialReferenceSystemsTableName},
	{Name: "ORGANIZATION", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 256), Default: nil, Nullable: true, Source: StSpatialReferenceSystemsTableName},
	{Name: "ORGANIZATION_COORDSYS_ID", Type: Uint32, Default: nil, Nullable: true, Source: StSpatialReferenceSystemsTableName},
	{Name: "DEFINITION", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 4096), Default: nil, Nullable: false, Source: StSpatialReferenceSystemsTableName},
	{Name: "DESCRIPTION", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 2048), Default: nil, Nullable: true, Source: StSpatialReferenceSystemsTableName},
}

var stUnitsOfMeasureSchema = Schema{
	{Name: "UNIT_NAME", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 255), Default: nil, Nullable: true, Source: StUnitsOfMeasureTableName},
	{Name: "UNIT_TYPE", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 7), Default: nil, Nullable: true, Source: StUnitsOfMeasureTableName},
	{Name: "CONVERSION_FACTOR", Type: Float64, Default: nil, Nullable: true, Source: StUnitsOfMeasureTableName},
	{Name: "DESCRIPTION", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 255), Default: nil, Nullable: true, Source: StUnitsOfMeasureTableName},
}

var statisticsSchema = Schema{
	{Name: "TABLE_CATALOG", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: StatisticsTableName},
	{Name: "TABLE_SCHEMA", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: StatisticsTableName},
	{Name: "TABLE_NAME", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: StatisticsTableName},
	{Name: "NON_UNIQUE", Type: Int32, Default: nil, Nullable: false, Source: StatisticsTableName},
	{Name: "INDEX_SCHEMA", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: StatisticsTableName},
	{Name: "INDEX_NAME", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: StatisticsTableName},
	{Name: "SEQ_IN_INDEX", Type: Uint32, Default: nil, Nullable: false, Source: StatisticsTableName},
	{Name: "COLUMN_NAME", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: StatisticsTableName},
	{Name: "COLLATION", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 1), Default: nil, Nullable: true, Source: StatisticsTableName},
	{Name: "CARDINALITY", Type: Int64, Default: nil, Nullable: true, Source: StatisticsTableName},
	{Name: "SUB_PART", Type: Int64, Default: nil, Nullable: true, Source: StatisticsTableName},
	{Name: "PACKED", Type: MustCreateBinary(sqltypes.Binary, 0), Default: nil, Nullable: true, Source: StatisticsTableName},
	{Name: "NULLABLE", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 3), Default: nil, Nullable: false, Source: StatisticsTableName},
	{Name: "INDEX_TYPE", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 11), Default: nil, Nullable: false, Source: StatisticsTableName},
	{Name: "COMMENT", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 8), Default: nil, Nullable: false, Source: StatisticsTableName},
	{Name: "INDEX_COMMENT", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 2048), Default: nil, Nullable: false, Source: StatisticsTableName},
	{Name: "IS_VISIBLE", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 3), Default: nil, Nullable: false, Source: StatisticsTableName},
	{Name: "EXPRESSION", Type: LongText, Default: nil, Nullable: true, Source: StatisticsTableName},
}

var tableConstraintsSchema = Schema{
	{Name: "CONSTRAINT_CATALOG", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: TableConstraintsTableName},
	{Name: "CONSTRAINT_SCHEMA", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: TableConstraintsTableName},
	{Name: "CONSTRAINT_NAME", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: TableConstraintsTableName},
	{Name: "TABLE_SCHEMA", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: TableConstraintsTableName},
	{Name: "TABLE_NAME", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: TableConstraintsTableName},
	{Name: "CONSTRAINT_TYPE", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 11), Default: nil, Nullable: false, Source: TableConstraintsTableName},
	{Name: "ENFORCED", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 3), Default: nil, Nullable: false, Source: TableConstraintsTableName},
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

var tablesSchema = Schema{
	{Name: "TABLE_CATALOG", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: TablesTableName},
	{Name: "TABLE_SCHEMA", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: TablesTableName},
	{Name: "TABLE_NAME", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: TablesTableName},
	{Name: "TABLE_TYPE", Type: MustCreateEnumType([]string{"BASE TABLE", "VIEW", "SYSTEM VIEW"}, Collation_Default), Default: nil, Nullable: false, Source: TablesTableName},
	{Name: "ENGINE", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: TablesTableName},
	{Name: "VERSION", Type: Int32, Default: nil, Nullable: true, Source: TablesTableName},
	{Name: "ROW_FORMAT", Type: MustCreateEnumType([]string{"Fixed", "Dynamic", "Compressed", "Redundant", "Compact", "Paged"}, Collation_Default), Default: nil, Nullable: true, Source: TablesTableName},
	{Name: "TABLE_ROWS", Type: Uint64, Default: nil, Nullable: true, Source: TablesTableName},
	{Name: "AVG_ROW_LENGTH", Type: Uint64, Default: nil, Nullable: true, Source: TablesTableName},
	{Name: "DATA_LENGTH", Type: Uint64, Default: nil, Nullable: true, Source: TablesTableName},
	{Name: "MAX_DATA_LENGTH", Type: Uint64, Default: nil, Nullable: true, Source: TablesTableName},
	{Name: "INDEX_LENGTH", Type: Uint64, Default: nil, Nullable: true, Source: TablesTableName},
	{Name: "DATA_FREE", Type: Uint64, Default: nil, Nullable: true, Source: TablesTableName},
	{Name: "AUTO_INCREMENT", Type: Uint64, Default: nil, Nullable: true, Source: TablesTableName},
	{Name: "CREATE_TIME", Type: Timestamp, Default: nil, Nullable: false, Source: TablesTableName},
	{Name: "UPDATE_TIME", Type: Datetime, Default: nil, Nullable: true, Source: TablesTableName},
	{Name: "CHECK_TIME", Type: Datetime, Default: nil, Nullable: true, Source: TablesTableName},
	{Name: "TABLE_COLLATION", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: TablesTableName},
	{Name: "CHECKSUM", Type: Int64, Default: nil, Nullable: true, Source: TablesTableName},
	{Name: "CREATE_OPTIONS", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 256), Default: nil, Nullable: true, Source: TablesTableName},
	{Name: "TABLE_COMMENT", Type: Text, Default: nil, Nullable: true, Source: TablesTableName},
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
	{Name: "EXTENT_SIZE", Type: Uint64, Default: nil, Nullable: true, Source: TablespacesTableName},
	{Name: "AUTOEXTEND_SIZE", Type: Uint64, Default: nil, Nullable: true, Source: TablespacesTableName},
	{Name: "MAXIMUM_SIZE", Type: Uint64, Default: nil, Nullable: true, Source: TablespacesTableName},
	{Name: "NODEGROUP_ID", Type: Uint64, Default: nil, Nullable: true, Source: TablespacesTableName},
	{Name: "TABLESPACE_COMMENT", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 2048), Default: nil, Nullable: true, Source: TablespacesTableName},
}

var tablespacesExtensionsSchema = Schema{
	{Name: "TABLESPACE_NAME", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 268), Default: nil, Nullable: false, Source: TablespacesExtensionsTableName},
	{Name: "ENGINE_ATTRIBUTE", Type: JSON, Default: nil, Nullable: true, Source: TablespacesExtensionsTableName},
}

var triggersSchema = Schema{
	{Name: "TRIGGER_CATALOG", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: TriggersTableName},
	{Name: "TRIGGER_SCHEMA", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: TriggersTableName},
	{Name: "TRIGGER_NAME", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: TriggersTableName},
	{Name: "EVENT_MANIPULATION", Type: MustCreateEnumType([]string{"INSERT", "UPDATE", "DELETE"}, Collation_Default), Default: nil, Nullable: false, Source: TriggersTableName},
	{Name: "EVENT_OBJECT_CATALOG", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: TriggersTableName},
	{Name: "EVENT_OBJECT_SCHEMA", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: TriggersTableName},
	{Name: "EVENT_OBJECT_TABLE", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: TriggersTableName},
	{Name: "ACTION_ORDER", Type: Uint32, Default: nil, Nullable: false, Source: TriggersTableName},
	{Name: "ACTION_CONDITION", Type: MustCreateBinary(sqltypes.Binary, 0), Default: nil, Nullable: true, Source: TriggersTableName},
	{Name: "ACTION_STATEMENT", Type: LongText, Default: nil, Nullable: false, Source: TriggersTableName},
	{Name: "ACTION_ORIENTATION", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 3), Default: nil, Nullable: false, Source: TriggersTableName},
	{Name: "ACTION_TIMING", Type: MustCreateEnumType([]string{"BEFORE", "AFTER"}, Collation_Default), Default: nil, Nullable: false, Source: TriggersTableName},
	{Name: "ACTION_REFERENCE_OLD_TABLE", Type: MustCreateBinary(sqltypes.Binary, 0), Default: nil, Nullable: true, Source: TriggersTableName},
	{Name: "ACTION_REFERENCE_NEW_TABLE", Type: MustCreateBinary(sqltypes.Binary, 0), Default: nil, Nullable: true, Source: TriggersTableName},
	{Name: "ACTION_REFERENCE_OLD_ROW", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 3), Default: nil, Nullable: false, Source: TriggersTableName},
	{Name: "ACTION_REFERENCE_NEW_ROW", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 3), Default: nil, Nullable: false, Source: TriggersTableName},
	{Name: "CREATED", Type: Timestamp, Default: nil, Nullable: false, Source: TriggersTableName},
	{Name: "SQL_MODE", Type: sqlModeSetType, Default: nil, Nullable: false, Source: TriggersTableName},
	{Name: "DEFINER", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 288), Default: nil, Nullable: false, Source: TriggersTableName},
	{Name: "CHARACTER_SET_CLIENT", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: TriggersTableName},
	{Name: "COLLATION_CONNECTION", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: TriggersTableName},
	{Name: "DATABASE_COLLATION", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: TriggersTableName},
}

var userAttributesSchema = Schema{
	{Name: "USER", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 32), Default: nil, Nullable: false, Source: UserAttributesTableName},
	{Name: "HOST", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 255), Default: nil, Nullable: false, Source: UserAttributesTableName},
	{Name: "ATTRIBUTE", Type: LongText, Default: nil, Nullable: true, Source: UserAttributesTableName},
}

var userPrivilegesSchema = Schema{
	{Name: "GRANTEE", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 292), Default: nil, Nullable: false, Source: UserPrivilegesTableName},
	{Name: "TABLE_CATALOG", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 512), Default: nil, Nullable: false, Source: UserPrivilegesTableName},
	{Name: "PRIVILEGE_TYPE", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: UserPrivilegesTableName},
	{Name: "IS_GRANTABLE", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 3), Default: nil, Nullable: false, Source: UserPrivilegesTableName},
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

var viewsSchema = Schema{
	{Name: "TABLE_CATALOG", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: ViewsTableName},
	{Name: "TABLE_SCHEMA", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: ViewsTableName},
	{Name: "TABLE_NAME", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: true, Source: ViewsTableName},
	{Name: "VIEW_DEFINITION", Type: LongText, Default: nil, Nullable: true, Source: ViewsTableName},
	{Name: "CHECK_OPTION", Type: MustCreateEnumType([]string{"NONE", "LOCAL", "CASCADED"}, Collation_Default), Default: nil, Nullable: true, Source: ViewsTableName},
	{Name: "IS_UPDATABLE", Type: MustCreateEnumType([]string{"NO", "YES"}, Collation_Default), Default: nil, Nullable: true, Source: ViewsTableName},
	{Name: "DEFINER", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 288), Default: nil, Nullable: true, Source: ViewsTableName},
	{Name: "SECURITY_TYPE", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 7), Default: nil, Nullable: true, Source: ViewsTableName},
	{Name: "CHARACTER_SET_CLIENT", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: ViewsTableName},
	{Name: "COLLATION_CONNECTION", Type: MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false, Source: ViewsTableName},
}

// characterSetsRowIter implements the sql.RowIter for the information_schema.CHARACTER_SETS table.
func characterSetsRowIter(ctx *Context, c Catalog) (RowIter, error) {
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

// checkConstraintsRowIter implements the sql.RowIter for the information_schema.CHECK_CONSTRAINTS table.
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

// collationCharacterSetApplicabilityRowIter implements the sql.RowIter for the information_schema.COLLATION_CHARACTER_SET_APPLICABILITY table.
func collationCharacterSetApplicabilityRowIter(ctx *Context, c Catalog) (RowIter, error) {
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

// collationsRowIter implements the sql.RowIter for the information_schema.COLLATIONS table.
func collationsRowIter(ctx *Context, c Catalog) (RowIter, error) {
	var rows []Row
	collIter := NewCollationsIterator()
	for c, ok := collIter.Next(); ok; c, ok = collIter.Next() {
		rows = append(rows, Row{
			c.Name,
			c.CharacterSet.Name(),
			uint64(c.ID),
			c.ID.IsDefault(),
			c.ID.IsCompiled(),
			c.ID.SortLength(),
			c.ID.PadAttribute(),
		})
	}
	return RowsToRowIter(rows...), nil
}

// columnStatisticsRowIter implements the sql.RowIter for the information_schema.COLUMN_STATISTICS table.
func columnStatisticsRowIter(ctx *Context, c Catalog) (RowIter, error) {
	var rows []Row
	statsTbl, err := c.Statistics(ctx)
	if err != nil {
		return nil, err
	}

	privSet, privSetCount := ctx.GetPrivilegeSet()
	for _, db := range c.AllDatabases(ctx) {
		dbName := db.Name()
		privSetDb := privSet.Database(dbName)

		err := DBTableIter(ctx, db, func(t Table) (cont bool, err error) {
			privSetTbl := privSetDb.Table(t.Name())
			tableHist, err := statsTbl.Hist(ctx, dbName, t.Name())
			if err != nil {
				return true, nil
			}

			if tableHist == nil {
				return true, nil
			}

			for _, col := range t.Schema() {
				privSetCol := privSetTbl.Column(col.Name)
				if privSetCount == 0 && privSetDb.Count() == 0 && privSetTbl.Count() == 0 && privSetCol.Count() == 0 {
					continue
				}
				if _, ok := col.Type.(StringType); ok {
					continue
				}

				hist, ok := tableHist[col.Name]
				if !ok {
					return false, fmt.Errorf("column histogram not found: %s", col.Name)
				}

				buckets := make([]interface{}, len(hist.Buckets))
				for i, b := range hist.Buckets {
					buckets[i] = []interface{}{fmt.Sprintf("%.2f", b.LowerBound), fmt.Sprintf("%.2f", b.UpperBound), fmt.Sprintf("%.2f", b.Frequency)}
				}

				// TODO: missing other key/value pairs in the JSON
				histogram := JSONDocument{Val: map[string]interface{}{"buckets": buckets}}

				rows = append(rows, Row{
					db.Name(), // table_schema
					t.Name(),  // table_name
					col.Name,  // column_name
					histogram, // histogram
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

// enginesRowIterimplements the sql.RowIter for the information_schema.ENGINES table.
func enginesRowIter(ctx *Context, c Catalog) (RowIter, error) {
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

// keyColumnUsageRowIter implements the sql.RowIter for the information_schema.KEY_COLUMN_USAGE table.
func keyColumnUsageRowIter(ctx *Context, c Catalog) (RowIter, error) {
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
			indexTable, ok := tbl.(IndexAddressable)
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

						referencedSchema := fk.ParentDatabase
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

// processListRowIter implements the sql.RowIter for the information_schema.PROCESSLIST table.
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
			uint64(proc.Connection),      // id
			proc.User,                    // user
			ctx.Session.Client().Address, // host
			db,                           // db
			"Query",                      // command
			int32(proc.Seconds()),        // time
			strings.Join(status, ", "),   // state
			proc.Query,                   // info
		}
	}

	return RowsToRowIter(rows...), nil
}

// referentialConstraintsRowIter implements the sql.RowIter for the information_schema.REFERENTIAL_CONSTRAINTS table.
func referentialConstraintsRowIter(ctx *Context, c Catalog) (RowIter, error) {
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

			// Get FKs
			fkTable, ok := tbl.(ForeignKeyTable)
			if ok {
				fks, err := fkTable.GetDeclaredForeignKeys(ctx)
				if err != nil {
					return nil, err
				}

				for _, fk := range fks {
					var uniqueConstName interface{}
					referencedSchema := fk.ParentDatabase
					referencedTableName := fk.ParentTable
					referencedCols := make(map[string]struct{})
					for _, refCol := range fk.ParentColumns {
						referencedCols[refCol] = struct{}{}
					}

					onUpdate := string(fk.OnUpdate)
					if fk.OnUpdate == ForeignKeyReferentialAction_DefaultAction {
						onUpdate = "NO ACTION"
					}
					onDelete := string(fk.OnDelete)
					if fk.OnDelete == ForeignKeyReferentialAction_DefaultAction {
						onDelete = "NO ACTION"
					}

					refTbl, _, rerr := c.Table(ctx, referencedSchema, referencedTableName)
					if rerr != nil {
						return nil, rerr
					}

					indexTable, iok := refTbl.(IndexAddressable)
					if iok {
						indexes, ierr := indexTable.GetIndexes(ctx)
						if ierr != nil {

						}
						for _, index := range indexes {
							if index.ID() != "PRIMARY" && !index.IsUnique() {
								continue
							}
							colNames := getColumnNamesFromIndex(index, refTbl)
							if len(colNames) == len(referencedCols) {
								var hasAll = true
								for _, colName := range colNames {
									_, hasAll = referencedCols[colName]
								}
								if hasAll {
									uniqueConstName = index.ID()
								}
							}
						}
					}

					rows = append(rows, Row{
						"def",               // constraint_catalog
						db.Name(),           // constraint_schema
						fk.Name,             // constraint_name
						"def",               // unique_constraint_catalog
						referencedSchema,    // unique_constraint_schema
						uniqueConstName,     // unique_constraint_name
						"NONE",              // match_option
						onUpdate,            // update_rule
						onDelete,            // delete_rule
						tbl.Name(),          // table_name
						referencedTableName, // referenced_table_name
					})
				}
			}
		}
	}

	return RowsToRowIter(rows...), nil
}

// schemataRowIter implements the sql.RowIter for the information_schema.SCHEMATA table.
func schemataRowIter(ctx *Context, c Catalog) (RowIter, error) {
	dbs := c.AllDatabases(ctx)

	var rows []Row
	for _, db := range dbs {
		collation := plan.GetDatabaseCollation(ctx, db)
		rows = append(rows, Row{
			"def",                             // catalog_name
			db.Name(),                         // schema_name
			collation.CharacterSet().String(), // default_character_set_name
			collation.String(),                // default_collation_name
			nil,                               // sql_path
			"NO",                              // default_encryption
		})
	}

	return RowsToRowIter(rows...), nil
}

// tableConstraintsRowIter implements the sql.RowIter for the information_schema.STATISTICS table.
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

			indexTable, ok := tbl.(IndexAddressable)
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
					for j, expr := range index.Expressions() {
						col := plan.GetColumnFromIndexExpr(expr, tbl)
						if col != nil {
							i += 1
							var (
								collation   string
								nullable    string
								cardinality int64
								subPart     interface{}
							)

							seqInIndex := i
							colName := strings.Replace(col.Name, "`", "", -1) // get rid of backticks

							// collation is "A" for ASC ; "D" for DESC ; "NULL" for not sorted
							collation = "A"

							// TODO : cardinality is an estimate of the number of unique values in the index.

							if j < len(index.PrefixLengths()) {
								subPart = int64(index.PrefixLengths()[j])
							}

							// if nullable, 'YES'; if not, ''
							if col.Nullable {
								nullable = "YES"
							} else {
								nullable = ""
							}

							// TODO: we currently don't support expression index such as ((i * 20))

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
								subPart,      // sub_part
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

// implements the sql.RowIter for the information_schema.TABLE_CONSTRAINTS table.
func tableConstraintsRowIter(ctx *Context, c Catalog) (RowIter, error) {
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
			indexTable, ok := tbl.(IndexAddressable)
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

// tablesRowIter implements the sql.RowIter for the information_schema.TABLES table.
func tablesRowIter(ctx *Context, cat Catalog) (RowIter, error) {
	var rows []Row
	var (
		tableType      string
		tableRows      uint64
		avgRowLength   uint64
		dataLength     uint64
		engine         interface{}
		rowFormat      interface{}
		tableCollation interface{}
		autoInc        interface{}
	)

	for _, db := range cat.AllDatabases(ctx) {
		if db.Name() == InformationSchemaDatabaseName {
			tableType = "SYSTEM VIEW"
		} else {
			tableType = "BASE TABLE"
			engine = "InnoDB"
			rowFormat = "Dynamic"
			tableCollation = Collation_Default.String()
		}

		y2k, _ := Timestamp.Convert("2000-01-01 00:00:00")
		err := DBTableIter(ctx, db, func(t Table) (cont bool, err error) {
			if db.Name() != InformationSchemaDatabaseName {
				if st, ok := t.(StatisticsTable); ok {
					tableRows, err = st.RowCount(ctx)
					if err != nil {
						return false, err
					}

					// TODO: correct values for avg_row_length, data_length, max_data_length are missing (current values varies on gms vs Dolt)
					//  index_length and data_free columns are not supported yet
					//  the data length values differ from MySQL
					// MySQL uses default page size (16384B) as data length, and it adds another page size, if table data fills the current page block.
					// https://stackoverflow.com/questions/34211377/average-row-length-higher-than-possible has good explanation.
					dataLength, err = st.DataLength(ctx)
					if err != nil {
						return false, err
					}

					if tableRows > uint64(0) {
						avgRowLength = dataLength / tableRows
					}
				}

				if ai, ok := t.(AutoIncrementTable); ok {
					autoInc, err = ai.PeekNextAutoIncrementValue(ctx)
					if !errors.Is(err, ErrNoAutoIncrementCol) && err != nil {
						return false, err
					}

					// table with no auto incremented column is qualified as AutoIncrementTable, and the nextAutoInc value is 0
					// table with auto incremented column and no rows, the nextAutoInc value is 1
					if autoInc == uint64(0) || autoInc == uint64(1) {
						autoInc = nil
					}
				}
			}

			rows = append(rows, Row{
				"def",          // table_catalog
				db.Name(),      // table_schema
				t.Name(),       // table_name
				tableType,      // table_type
				engine,         // engine
				10,             // version (protocol, always 10)
				rowFormat,      // row_format
				tableRows,      // table_rows
				avgRowLength,   // avg_row_length
				dataLength,     // data_length
				0,              // max_data_length
				0,              // index_length
				0,              // data_free
				autoInc,        // auto_increment
				y2k,            // create_time
				y2k,            // update_time
				nil,            // check_time
				tableCollation, // table_collation
				nil,            // checksum
				"",             // create_options
				"",             // table_comment
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
				"def",     // table_catalog
				db.Name(), // table_schema
				view.Name, // table_name
				"VIEW",    // table_type
				nil,       // engine
				nil,       // version (protocol, always 10)
				nil,       // row_format
				nil,       // table_rows
				nil,       // avg_row_length
				nil,       // data_length
				nil,       // max_data_length
				nil,       // max_data_length
				nil,       // data_free
				nil,       // auto_increment
				y2k,       // create_time
				nil,       // update_time
				nil,       // check_time
				nil,       // table_collation
				nil,       // checksum
				nil,       // create_options
				"VIEW",    // table_comment
			})
		}
	}

	return RowsToRowIter(rows...), nil
}

// triggersRowIter implements the sql.RowIter for the information_schema.TRIGGERS table.
func triggersRowIter(ctx *Context, c Catalog) (RowIter, error) {
	var rows []Row
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
	sysVal, err := ctx.Session.GetSessionVariable(ctx, "sql_mode")
	if err != nil {
		return nil, err
	}
	sqlMode, sok := sysVal.(string)
	if !sok {
		return nil, ErrSystemVariableCodeFail.New("sql_mode", sysVal)
	}
	privSet, _ := ctx.GetPrivilegeSet()
	hasGlobalTriggerPriv := privSet.Has(PrivilegeType_Trigger)
	for _, db := range c.AllDatabases(ctx) {
		triggerDb, ok := db.(TriggerDatabase)
		if ok {
			privDbSet := privSet.Database(db.Name())
			hasDbTriggerPriv := privDbSet.Has(PrivilegeType_Trigger)
			triggers, err := triggerDb.GetTriggers(ctx)
			if err != nil {
				return nil, err
			}

			if len(triggers) == 0 {
				continue
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

					// triggers cannot be created on table that is not in current schema, so the trigger_name = event_object_schema
					privTblSet := privDbSet.Table(tableName)

					// To see information about a table's triggers, you must have the TRIGGER privilege for the table.
					if hasGlobalTriggerPriv || hasDbTriggerPriv || privTblSet.Has(PrivilegeType_Trigger) {
						rows = append(rows, Row{
							"def",                   // trigger_catalog
							triggerDb.Name(),        // trigger_schema
							triggerPlan.TriggerName, // trigger_name
							triggerEvent,            // event_manipulation
							"def",                   // event_object_catalog
							triggerDb.Name(),        // event_object_schema
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
							sqlMode,                 // sql_mode
							triggerPlan.Definer,     // definer
							characterSetClient,      // character_set_client
							collationConnection,     // collation_connection
							collationServer,         // database_collation
						})
					}
				}
			}
		}
	}
	return RowsToRowIter(rows...), nil
}

// viewsRowIter implements the sql.RowIter for the information_schema.VIEWS table.
func viewsRowIter(ctx *Context, catalog Catalog) (RowIter, error) {
	var rows []Row
	privSet, _ := ctx.GetPrivilegeSet()
	hasGlobalShowViewPriv := privSet.Has(PrivilegeType_ShowView)
	for _, db := range catalog.AllDatabases(ctx) {
		dbName := db.Name()
		privDbSet := privSet.Database(dbName)
		hasDbShowViewPriv := privDbSet.Has(PrivilegeType_ShowView)

		views, err := viewsInDatabase(ctx, db)
		if err != nil {
			return nil, err
		}

		charset := Collation_Default.CharacterSet().String()
		collation := Collation_Default.String()

		for _, view := range views {
			privTblSet := privDbSet.Table(view.Name)
			if !hasGlobalShowViewPriv && !hasDbShowViewPriv && !privTblSet.Has(PrivilegeType_ShowView) {
				continue
			}
			parsedView, err := parse.Parse(ctx, view.CreateViewStatement)
			if err != nil {
				return nil, err
			}
			viewPlan, ok := parsedView.(*plan.CreateView)
			if !ok {
				return nil, ErrTriggerCreateStatementInvalid.New(view.CreateViewStatement)
			}

			viewDef := view.TextDefinition
			definer := viewPlan.Definer

			// TODO: WITH CHECK OPTION is not supported yet.
			checkOpt := viewPlan.CheckOpt
			if checkOpt == "" {
				checkOpt = "NONE"
			}

			isUpdatable := "YES"
			// TODO: this function call should be done at CREATE VIEW time, not here
			if !plan.GetIsUpdatableFromCreateView(viewPlan) {
				isUpdatable = "NO"
			}

			securityType := viewPlan.Security
			if securityType == "" {
				securityType = "DEFINER"
			}

			rows = append(rows, Row{
				"def",        // table_catalog
				dbName,       // table_schema
				view.Name,    // table_name
				viewDef,      // view_definition
				checkOpt,     // check_option
				isUpdatable,  // is_updatable
				definer,      // definer
				securityType, // security_type
				charset,      // character_set_client
				collation,    // collation_connection
			})
		}
	}

	return RowsToRowIter(rows...), nil
}

// emptyRowIter implements the sql.RowIter for empty table.
func emptyRowIter(ctx *Context, c Catalog) (RowIter, error) {
	return RowsToRowIter(), nil
}

func NewUpdatableInformationSchemaDatabase() Database {
	db := NewInformationSchemaDatabase().(*informationSchemaDatabase)
	db.tables[StatisticsTableName] = newUpdatableStatsTable()
	return db
}

// NewInformationSchemaDatabase creates a new INFORMATION_SCHEMA Database.
func NewInformationSchemaDatabase() Database {
	isDb := &informationSchemaDatabase{
		name: InformationSchemaDatabaseName,
		tables: map[string]Table{
			AdministrableRoleAuthorizationsTableName: &informationSchemaTable{
				name:   AdministrableRoleAuthorizationsTableName,
				schema: administrableRoleAuthorizationsSchema,
				reader: emptyRowIter,
			},
			ApplicableRolesTableName: &informationSchemaTable{
				name:   ApplicableRolesTableName,
				schema: applicableRolesSchema,
				reader: emptyRowIter,
			},
			CharacterSetsTableName: &informationSchemaTable{
				name:   CharacterSetsTableName,
				schema: characterSetsSchema,
				reader: characterSetsRowIter,
			},
			CheckConstraintsTableName: &informationSchemaTable{
				name:   CheckConstraintsTableName,
				schema: checkConstraintsSchema,
				reader: checkConstraintsRowIter,
			},
			CollationCharSetApplicabilityTableName: &informationSchemaTable{
				name:   CollationCharSetApplicabilityTableName,
				schema: collationCharacterSetApplicabilitySchema,
				reader: collationCharacterSetApplicabilityRowIter,
			},
			CollationsTableName: &informationSchemaTable{
				name:   CollationsTableName,
				schema: collationsSchema,
				reader: collationsRowIter,
			},
			ColumnPrivilegesTableName: &informationSchemaTable{
				name:   ColumnPrivilegesTableName,
				schema: columnPrivilegesSchema,
				reader: emptyRowIter,
			},
			ColumnStatisticsTableName: &informationSchemaTable{
				name:   ColumnStatisticsTableName,
				schema: columnStatisticsSchema,
				reader: columnStatisticsRowIter,
			},
			ColumnsTableName: &ColumnsTable{
				name:    ColumnsTableName,
				schema:  columnsSchema,
				rowIter: columnsRowIter,
			},
			ColumnsExtensionsTableName: &informationSchemaTable{
				name:   ColumnsExtensionsTableName,
				schema: columnsExtensionsSchema,
				reader: emptyRowIter,
			},
			EnabledRolesTablesName: &informationSchemaTable{
				name:   EnabledRolesTablesName,
				schema: enabledRolesSchema,
				reader: emptyRowIter,
			},
			EnginesTableName: &informationSchemaTable{
				name:   EnginesTableName,
				schema: enginesSchema,
				reader: enginesRowIter,
			},
			EventsTableName: &informationSchemaTable{
				name:   EventsTableName,
				schema: eventsSchema,
				reader: emptyRowIter,
			},
			FilesTableName: &informationSchemaTable{
				name:   FilesTableName,
				schema: filesSchema,
				reader: emptyRowIter,
			},
			KeyColumnUsageTableName: &informationSchemaTable{
				name:   KeyColumnUsageTableName,
				schema: keyColumnUsageSchema,
				reader: keyColumnUsageRowIter,
			},
			KeywordsTableName: &informationSchemaTable{
				name:   KeywordsTableName,
				schema: keywordsSchema,
				reader: emptyRowIter,
			},
			OptimizerTraceTableName: &informationSchemaTable{
				name:   OptimizerTraceTableName,
				schema: optimizerTraceSchema,
				reader: emptyRowIter,
			},
			ParametersTableName: &informationSchemaTable{
				name:   ParametersTableName,
				schema: parametersSchema,
				reader: emptyRowIter,
			},
			PartitionsTableName: &informationSchemaTable{
				name:   PartitionsTableName,
				schema: partitionsSchema,
				reader: emptyRowIter,
			},
			PluginsTableName: &informationSchemaTable{
				name:   PluginsTableName,
				schema: pluginsSchema,
				reader: emptyRowIter,
			},
			ProcessListTableName: &informationSchemaTable{
				name:   ProcessListTableName,
				schema: processListSchema,
				reader: processListRowIter,
			},
			ProfilingTableName: &informationSchemaTable{
				name:   ProfilingTableName,
				schema: profilingSchema,
				reader: emptyRowIter,
			},
			ReferentialConstraintsTableName: &informationSchemaTable{
				name:   ReferentialConstraintsTableName,
				schema: referentialConstraintsSchema,
				reader: referentialConstraintsRowIter,
			},
			ResourceGroupsTableName: &informationSchemaTable{
				name:   ResourceGroupsTableName,
				schema: resourceGroupsSchema,
				reader: emptyRowIter,
			},
			RoleColumnGrantsTableName: &informationSchemaTable{
				name:   RoleColumnGrantsTableName,
				schema: roleColumnGrantsSchema,
				reader: emptyRowIter,
			},
			RoleRoutineGrantsTableName: &informationSchemaTable{
				name:   RoleRoutineGrantsTableName,
				schema: roleRoutineGrantsSchema,
				reader: emptyRowIter,
			},
			RoleTableGrantsTableName: &informationSchemaTable{
				name:   RoleTableGrantsTableName,
				schema: roleTableGrantsSchema,
				reader: emptyRowIter,
			},
			RoutinesTableName: &routineTable{
				name:    RoutinesTableName,
				schema:  routinesSchema,
				rowIter: routinesRowIter,
			},
			SchemaPrivilegesTableName: &informationSchemaTable{
				name:   SchemaPrivilegesTableName,
				schema: schemaPrivilegesTableName,
				reader: emptyRowIter,
			},
			SchemataTableName: &informationSchemaTable{
				name:   SchemataTableName,
				schema: schemataSchema,
				reader: schemataRowIter,
			},
			SchemataExtensionsTableName: &informationSchemaTable{
				name:   SchemataExtensionsTableName,
				schema: schemataExtensionsTableName,
				reader: emptyRowIter,
			},
			StGeometryColumnsTableName: &informationSchemaTable{
				name:   StGeometryColumnsTableName,
				schema: stGeometryColumnsSchema,
				reader: emptyRowIter,
			},
			StSpatialReferenceSystemsTableName: &informationSchemaTable{
				name:   StSpatialReferenceSystemsTableName,
				schema: stSpatialReferenceSystemsSchema,
				reader: emptyRowIter,
			},
			StUnitsOfMeasureTableName: &informationSchemaTable{
				name:   StUnitsOfMeasureTableName,
				schema: stUnitsOfMeasureSchema,
				reader: emptyRowIter,
			},
			TableConstraintsTableName: &informationSchemaTable{
				name:   TableConstraintsTableName,
				schema: tableConstraintsSchema,
				reader: tableConstraintsRowIter,
			},
			TableConstraintsExtensionsTableName: &informationSchemaTable{
				name:   TableConstraintsExtensionsTableName,
				schema: tableConstraintsExtensionsSchema,
				reader: emptyRowIter,
			},
			TablePrivilegesTableName: &informationSchemaTable{
				name:   TablePrivilegesTableName,
				schema: tablePrivilegesSchema,
				reader: emptyRowIter,
			},
			TablesTableName: &informationSchemaTable{
				name:   TablesTableName,
				schema: tablesSchema,
				reader: tablesRowIter,
			},
			TablesExtensionsTableName: &informationSchemaTable{
				name:   TablesExtensionsTableName,
				schema: tablesExtensionsSchema,
				reader: emptyRowIter,
			},
			TablespacesTableName: &informationSchemaTable{
				name:   TablespacesTableName,
				schema: tablespacesSchema,
				reader: emptyRowIter,
			},
			TablespacesExtensionsTableName: &informationSchemaTable{
				name:   TablespacesExtensionsTableName,
				schema: tablespacesExtensionsSchema,
				reader: emptyRowIter,
			},
			TriggersTableName: &informationSchemaTable{
				name:   TriggersTableName,
				schema: triggersSchema,
				reader: triggersRowIter,
			},
			UserAttributesTableName: &informationSchemaTable{
				name:   UserAttributesTableName,
				schema: userAttributesSchema,
				reader: emptyRowIter,
			},
			UserPrivilegesTableName: &informationSchemaTable{
				name:   UserPrivilegesTableName,
				schema: userPrivilegesSchema,
				reader: emptyRowIter,
			},
			ViewRoutineUsageTableName: &informationSchemaTable{
				name:   ViewRoutineUsageTableName,
				schema: viewRoutineUsageSchema,
				reader: emptyRowIter,
			},
			ViewTableUsageTableName: &informationSchemaTable{
				name:   ViewTableUsageTableName,
				schema: viewTableUsageSchema,
				reader: emptyRowIter,
			},
			ViewsTableName: &informationSchemaTable{
				name:   ViewsTableName,
				schema: viewsSchema,
				reader: viewsRowIter,
			},
			InnoDBBufferPageName: &informationSchemaTable{
				name:   InnoDBBufferPageName,
				schema: innoDBBufferPageSchema,
				reader: emptyRowIter,
			},
			InnoDBBufferPageLRUName: &informationSchemaTable{
				name:   InnoDBBufferPageLRUName,
				schema: innoDBBufferPageLRUSchema,
				reader: emptyRowIter,
			},
			InnoDBBufferPoolStatsName: &informationSchemaTable{
				name:   InnoDBBufferPoolStatsName,
				schema: innoDBBufferPoolStatsSchema,
				reader: emptyRowIter,
			},
			InnoDBCachedIndexesName: &informationSchemaTable{
				name:   InnoDBCachedIndexesName,
				schema: innoDBCachedIndexesSchema,
				reader: emptyRowIter,
			},
			InnoDBCmpName: &informationSchemaTable{
				name:   InnoDBCmpName,
				schema: innoDBCmpSchema,
				reader: emptyRowIter,
			},
			InnoDBCmpResetName: &informationSchemaTable{
				name:   InnoDBCmpResetName,
				schema: innoDBCmpResetSchema,
				reader: emptyRowIter,
			},
			InnoDBCmpmemName: &informationSchemaTable{
				name:   InnoDBCmpmemName,
				schema: innoDBCmpmemSchema,
				reader: emptyRowIter,
			},
			InnoDBCmpmemResetName: &informationSchemaTable{
				name:   InnoDBCmpmemResetName,
				schema: innoDBCmpmemResetSchema,
				reader: emptyRowIter,
			},
			InnoDBCmpPerIndexName: &informationSchemaTable{
				name:   InnoDBCmpPerIndexName,
				schema: innoDBCmpPerIndexSchema,
				reader: emptyRowIter,
			},
			InnoDBCmpPerIndexResetName: &informationSchemaTable{
				name:   InnoDBCmpPerIndexResetName,
				schema: innoDBCmpPerIndexResetSchema,
				reader: emptyRowIter,
			},
			InnoDBColumnsName: &informationSchemaTable{
				name:   InnoDBColumnsName,
				schema: innoDBColumnsSchema,
				reader: emptyRowIter,
			},
			InnoDBDatafilesName: &informationSchemaTable{
				name:   InnoDBDatafilesName,
				schema: innoDBDatafilesSchema,
				reader: emptyRowIter,
			},
			InnoDBFieldsName: &informationSchemaTable{
				name:   InnoDBFieldsName,
				schema: innoDBFieldsSchema,
				reader: emptyRowIter,
			},
			InnoDBForeignName: &informationSchemaTable{
				name:   InnoDBForeignName,
				schema: innoDBForeignSchema,
				reader: emptyRowIter,
			},
			InnoDBForeignColsName: &informationSchemaTable{
				name:   InnoDBForeignColsName,
				schema: innoDBForeignColsSchema,
				reader: emptyRowIter,
			},
			InnoDBFtBeingDeletedName: &informationSchemaTable{
				name:   InnoDBFtBeingDeletedName,
				schema: innoDBFtBeingDeletedSchema,
				reader: emptyRowIter,
			},
			InnoDBFtConfigName: &informationSchemaTable{
				name:   InnoDBFtConfigName,
				schema: innoDBFtConfigSchema,
				reader: emptyRowIter,
			},
			InnoDBFtDefaultStopwordName: &informationSchemaTable{
				name:   InnoDBFtDefaultStopwordName,
				schema: innoDBFtDefaultStopwordSchema,
				reader: emptyRowIter,
			},
			InnoDBFtDeletedName: &informationSchemaTable{
				name:   InnoDBFtDeletedName,
				schema: innoDBFtDeletedSchema,
				reader: emptyRowIter,
			},
			InnoDBFtIndexCacheName: &informationSchemaTable{
				name:   InnoDBFtIndexCacheName,
				schema: innoDBFtIndexCacheSchema,
				reader: emptyRowIter,
			},
			InnoDBFtIndexTableName: &informationSchemaTable{
				name:   InnoDBFtIndexTableName,
				schema: innoDBFtIndexTableSchema,
				reader: emptyRowIter,
			},
			InnoDBIndexesName: &informationSchemaTable{
				name:   InnoDBIndexesName,
				schema: innoDBIndexesSchema,
				reader: emptyRowIter,
			},
			InnoDBMetricsName: &informationSchemaTable{
				name:   InnoDBMetricsName,
				schema: innoDBMetricsSchema,
				reader: emptyRowIter,
			},
			InnoDBSessionTempTablespacesName: &informationSchemaTable{
				name:   InnoDBSessionTempTablespacesName,
				schema: innoDBSessionTempTablespacesSchema,
				reader: emptyRowIter,
			},
			InnoDBTablesName: &informationSchemaTable{
				name:   InnoDBTablesName,
				schema: innoDBTablesSchema,
				reader: emptyRowIter,
			},
			InnoDBTablespacesName: &informationSchemaTable{
				name:   InnoDBTablespacesName,
				schema: innoDBTablespacesSchema,
				reader: emptyRowIter,
			},
			InnoDBTablespacesBriefName: &informationSchemaTable{
				name:   InnoDBTablespacesBriefName,
				schema: innoDBTablespacesBriefSchema,
				reader: emptyRowIter,
			},
			InnoDBTablestatsName: &informationSchemaTable{
				name:   InnoDBTablestatsName,
				schema: innoDBTablestatsSchema,
				reader: emptyRowIter,
			},
			InnoDBTempTableInfoName: &informationSchemaTable{
				name:   InnoDBTempTableInfoName,
				schema: innoDBTempTableSchema,
				reader: innoDBTempTableRowIter,
			},
			InnoDBTrxName: &informationSchemaTable{
				name:   InnoDBTrxName,
				schema: innoDBTrxSchema,
				reader: emptyRowIter,
			},
			InnoDBVirtualName: &informationSchemaTable{
				name:   InnoDBVirtualName,
				schema: innoDBVirtualSchema,
				reader: emptyRowIter,
			},
		},
	}

	isDb.tables[StatisticsTableName] = NewDefaultStats()

	return isDb
}

// Name implements the sql.Database interface.
func (db *informationSchemaDatabase) Name() string { return db.name }

func (db *informationSchemaDatabase) GetTableInsensitive(ctx *Context, tblName string) (Table, bool, error) {
	// The columns table has dynamic information that can't be cached across queries
	if strings.ToLower(tblName) == ColumnsTableName {
		return &ColumnsTable{}, true, nil
	}

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

// Collation implements the sql.Table interface.
func (t *informationSchemaTable) Collation() CollationID {
	return Collation_Default
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
	if t.reader == nil {
		return RowsToRowIter(), nil
	}
	if t.catalog == nil {
		return nil, fmt.Errorf("nil catalog for info schema table %s", t.name)
	}
	return t.reader(ctx, t.catalog)
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

func NewDefaultStats() *defaultStatsTable {
	return &defaultStatsTable{
		informationSchemaTable: &informationSchemaTable{
			name:   StatisticsTableName,
			schema: statisticsSchema,
			reader: statisticsRowIter,
		},
		stats: make(catalogStatistics),
	}
}

// catalogStatistics holds TableStatistics keyed by table and database
type catalogStatistics map[DbTable]*TableStatistics

// defaultStatsTable is a statistics table implementation
// with a cache to save ANALYZE results. RowCount defers to
// the underlying table in the absence of a cached statistic.
type defaultStatsTable struct {
	*informationSchemaTable
	stats catalogStatistics
}

var _ StatsReadWriter = (*defaultStatsTable)(nil)

func (n *defaultStatsTable) AssignCatalog(cat Catalog) Table {
	n.catalog = cat
	return n
}

func (n *defaultStatsTable) Hist(ctx *Context, db, table string) (HistogramMap, error) {
	if s, ok := n.stats[NewDbTable(db, table)]; ok {
		return s.Histograms, nil
	} else {
		err := fmt.Errorf("histogram not found for table '%s.%s'", db, table)
		return nil, err
	}
}

// RowCount returns a sql.StatisticsTable's row count, or false if the table does not
// implement the interface, or an error if the table was not found.
func (n *defaultStatsTable) RowCount(ctx *Context, db, table string) (uint64, bool, error) {
	s, ok := n.stats[NewDbTable(db, table)]
	if ok {
		return s.RowCount, true, nil
	}

	t, _, err := n.catalog.Table(ctx, db, table)
	if err != nil {
		return 0, false, err
	}
	st, ok := t.(StatisticsTable)
	if !ok {
		return 0, false, nil
	}
	cnt, err := st.RowCount(ctx)
	if err != nil {
		return 0, false, err
	}
	return cnt, true, nil
}

func (n *defaultStatsTable) Analyze(ctx *Context, db, table string) error {
	tableStats := &TableStatistics{
		CreatedAt: time.Now(),
	}

	t, _, err := n.catalog.Table(ctx, db, table)
	if err != nil {
		return err
	}
	histMap, err := NewHistogramMapFromTable(ctx, t)
	if err != nil {
		return err
	}

	tableStats.Histograms = histMap
	for _, v := range histMap {
		tableStats.RowCount = v.Count + v.NullCount
		break
	}

	n.stats[NewDbTable(db, table)] = tableStats
	return nil
}

func newUpdatableStatsTable() *updatableStatsTable {
	return &updatableStatsTable{
		defaultStatsTable: NewDefaultStats(),
	}
}

// updatableStatsTable provides a statistics table that can
// be edited with UPDATE statements.
type updatableStatsTable struct {
	*defaultStatsTable
}

var _ UpdatableTable = (*updatableStatsTable)(nil)
var _ StatsReadWriter = (*updatableStatsTable)(nil)

// AssignCatalog implements sql.CatalogTable
func (t *updatableStatsTable) AssignCatalog(cat Catalog) Table {
	t.catalog = cat
	return t
}

// Updater implements sql.UpdatableTable
func (t *updatableStatsTable) Updater(_ *Context) RowUpdater {
	return newStatsEditor(t.catalog, t.stats)
}

func newStatsEditor(c Catalog, stats map[DbTable]*TableStatistics) RowUpdater {
	return &statsEditor{c: c, s: stats}
}

// statsEditor is an internal-only object used to mock table
// statistics for testing.
type statsEditor struct {
	c Catalog
	s map[DbTable]*TableStatistics
}

var _ RowUpdater = (*statsEditor)(nil)

// StatementBegin implements sql.RowUpdater
func (s *statsEditor) StatementBegin(_ *Context) {}

// DiscardChanges implements sql.RowUpdater
func (s *statsEditor) DiscardChanges(_ *Context, _ error) error {
	return fmt.Errorf("discarding statsEditor changes not supported")
}

// StatementComplete implements sql.RowUpdater
func (s *statsEditor) StatementComplete(_ *Context) error { return nil }

// Update implements sql.RowUpdater
func (s *statsEditor) Update(ctx *Context, old, new Row) error {
	db, ok := old[1].(string)
	if !ok {
		return fmt.Errorf("expected string type databaseName; found type: '%T', value: '%v'", old[1], old[1])
	}
	table, ok := old[2].(string)
	if !ok {
		return fmt.Errorf("expected string type tableName; found type: '%T', value: '%v'", old[2], old[2])
	}

	_, _, err := s.c.Table(ctx, db, table)
	if err != nil {
		return err
	}

	card, ok := new[9].(int64)
	if !ok {
		return fmt.Errorf("expeceted integer cardinality; found type: '%T', value: '%s'", new[9], new[9])
	}
	stats := &TableStatistics{
		RowCount:   uint64(card),
		CreatedAt:  time.Now(),
		Histograms: make(HistogramMap),
	}
	s.s[NewDbTable(db, table)] = stats
	return nil
}

func (s *statsEditor) Close(context *Context) error {
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
			Name:                view.Name(),
			TextDefinition:      view.TextDefinition(),
			CreateViewStatement: view.CreateStatement(),
		})
	}

	return views, nil
}
