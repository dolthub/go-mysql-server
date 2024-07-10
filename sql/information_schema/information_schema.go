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

	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/dolthub/vitess/go/vt/sqlparser"

	gmstime "github.com/dolthub/go-mysql-server/internal/time"
	. "github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/mysql_db"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/planbuilder"
	"github.com/dolthub/go-mysql-server/sql/types"
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
	// defaultInfoSchemaRowCount is a default row count estimate
	defaultInfoSchemaRowCount = 1000
)

var sqlModeSetType = types.MustCreateSetType([]string{
	"ALLOW_INVALID_DATES", "ANSI", "ANSI_QUOTES", "ERROR_FOR_DIVISION_BY_ZERO", "HIGH_NOT_PRECEDENCE",
	"IGNORE_SPACE", "NOT_USED", "NOT_USED_10", "NOT_USED_11", "NOT_USED_12", "NOT_USED_13", "NOT_USED_14",
	"NOT_USED_15", "NOT_USED_16", "NOT_USED_17", "NOT_USED_18", "NOT_USED_29", "NOT_USED_9", "NO_AUTO_VALUE_ON_ZERO",
	"NO_BACKSLASH_ESCAPES", "NO_DIR_IN_CREATE", "NO_ENGINE_SUBSTITUTION", "NO_UNSIGNED_SUBTRACTION", "NO_ZERO_DATE",
	"NO_ZERO_IN_DATE", "ONLY_FULL_GROUP_BY", "PAD_CHAR_TO_FULL_LENGTH", "PIPES_AS_CONCAT", "REAL_AS_FLOAT",
	"STRICT_ALL_TABLES", "STRICT_TRANS_TABLES", "TIME_TRUNCATE_FRACTIONAL", "TRADITIONAL"}, Collation_Information_Schema_Default)

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
	_ Database        = (*informationSchemaDatabase)(nil)
	_ Table           = (*informationSchemaTable)(nil)
	_ StatisticsTable = (*informationSchemaTable)(nil)
	_ Databaseable    = (*informationSchemaTable)(nil)
	_ Partition       = (*informationSchemaPartition)(nil)
	_ PartitionIter   = (*informationSchemaPartitionIter)(nil)
)

var administrableRoleAuthorizationsSchema = Schema{
	{Name: "USER", Type: types.MustCreateString(sqltypes.VarChar, 97, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: AdministrableRoleAuthorizationsTableName},
	{Name: "HOST", Type: types.MustCreateString(sqltypes.VarChar, 256, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: AdministrableRoleAuthorizationsTableName},
	{Name: "GRANTEE", Type: types.MustCreateString(sqltypes.VarChar, 97, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: AdministrableRoleAuthorizationsTableName},
	{Name: "GRANTEE_HOST", Type: types.MustCreateString(sqltypes.VarChar, 256, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: AdministrableRoleAuthorizationsTableName},
	{Name: "ROLE_NAME", Type: types.MustCreateString(sqltypes.VarChar, 255, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: AdministrableRoleAuthorizationsTableName},
	{Name: "ROLE_HOST", Type: types.MustCreateString(sqltypes.VarChar, 256, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: AdministrableRoleAuthorizationsTableName},
	{Name: "IS_GRANTABLE", Type: types.MustCreateString(sqltypes.VarChar, 3, Collation_Information_Schema_Default), Default: planbuilder.MustStringToColumnDefaultValue(NewEmptyContext(), `""`, types.LongText, false), Nullable: false, Source: AdministrableRoleAuthorizationsTableName},
	{Name: "IS_DEFAULT", Type: types.MustCreateString(sqltypes.VarChar, 3, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: AdministrableRoleAuthorizationsTableName},
	{Name: "IS_MANDATORY", Type: types.MustCreateString(sqltypes.VarChar, 3, Collation_Information_Schema_Default), Default: planbuilder.MustStringToColumnDefaultValue(NewEmptyContext(), `""`, types.LongText, false), Nullable: false, Source: AdministrableRoleAuthorizationsTableName},
}

var applicableRolesSchema = Schema{
	{Name: "USER", Type: types.MustCreateString(sqltypes.VarChar, 97, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: ApplicableRolesTableName},
	{Name: "HOST", Type: types.MustCreateString(sqltypes.VarChar, 256, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: ApplicableRolesTableName},
	{Name: "GRANTEE", Type: types.MustCreateString(sqltypes.VarChar, 97, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: ApplicableRolesTableName},
	{Name: "GRANTEE_HOST", Type: types.MustCreateString(sqltypes.VarChar, 256, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: ApplicableRolesTableName},
	{Name: "ROLE_NAME", Type: types.MustCreateString(sqltypes.VarChar, 255, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: ApplicableRolesTableName},
	{Name: "ROLE_HOST", Type: types.MustCreateString(sqltypes.VarChar, 256, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: ApplicableRolesTableName},
	{Name: "IS_GRANTABLE", Type: types.MustCreateString(sqltypes.VarChar, 3, Collation_Information_Schema_Default), Default: planbuilder.MustStringToColumnDefaultValue(NewEmptyContext(), `""`, types.LongText, false), Nullable: false, Source: ApplicableRolesTableName},
	{Name: "IS_DEFAULT", Type: types.MustCreateString(sqltypes.VarChar, 3, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: ApplicableRolesTableName},
	{Name: "IS_MANDATORY", Type: types.MustCreateString(sqltypes.VarChar, 3, Collation_Information_Schema_Default), Default: planbuilder.MustStringToColumnDefaultValue(NewEmptyContext(), `""`, types.LongText, false), Nullable: false, Source: ApplicableRolesTableName},
}

var characterSetsSchema = Schema{
	{Name: "CHARACTER_SET_NAME", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: CharacterSetsTableName},
	{Name: "DEFAULT_COLLATE_NAME", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: CharacterSetsTableName},
	{Name: "DESCRIPTION", Type: types.MustCreateString(sqltypes.VarChar, 2048, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: CharacterSetsTableName},
	{Name: "MAXLEN", Type: types.Uint32, Default: nil, Nullable: false, Source: CharacterSetsTableName},
}

var checkConstraintsSchema = Schema{
	{Name: "CONSTRAINT_CATALOG", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: CheckConstraintsTableName},
	{Name: "CONSTRAINT_SCHEMA", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: CheckConstraintsTableName},
	{Name: "CONSTRAINT_NAME", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: CheckConstraintsTableName},
	{Name: "CHECK_CLAUSE", Type: types.LongText, Default: nil, Nullable: false, Source: CheckConstraintsTableName},
}

var collationCharacterSetApplicabilitySchema = Schema{
	{Name: "COLLATION_NAME", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: CollationCharSetApplicabilityTableName},
	{Name: "CHARACTER_SET_NAME", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: CollationCharSetApplicabilityTableName},
}

var collationsSchema = Schema{
	{Name: "COLLATION_NAME", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: CollationsTableName},
	{Name: "CHARACTER_SET_NAME", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: CollationsTableName},
	{Name: "ID", Type: types.Uint64, Default: planbuilder.MustStringToColumnDefaultValue(NewEmptyContext(), "0", types.Uint64, false), Nullable: false, Source: CollationsTableName},
	{Name: "IS_DEFAULT", Type: types.MustCreateString(sqltypes.VarChar, 3, Collation_Information_Schema_Default), Default: planbuilder.MustStringToColumnDefaultValue(NewEmptyContext(), `""`, types.LongText, false), Nullable: false, Source: CollationsTableName},
	{Name: "IS_COMPILED", Type: types.MustCreateString(sqltypes.VarChar, 3, Collation_Information_Schema_Default), Default: planbuilder.MustStringToColumnDefaultValue(NewEmptyContext(), `""`, types.LongText, false), Nullable: true, Source: CollationsTableName},
	{Name: "SORTLEN", Type: types.Uint32, Default: nil, Nullable: false, Source: CollationsTableName},
	{Name: "PAD_ATTRIBUTE", Type: types.MustCreateEnumType([]string{"PAD SPACE", "NO PAD"}, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: CollationsTableName},
}

var columnPrivilegesSchema = Schema{
	{Name: "GRANTEE", Type: types.MustCreateString(sqltypes.VarChar, 292, Collation_Information_Schema_Default), Default: planbuilder.MustStringToColumnDefaultValue(NewEmptyContext(), `""`, types.LongText, false), Nullable: false, Source: ColumnPrivilegesTableName},
	{Name: "TABLE_CATALOG", Type: types.MustCreateString(sqltypes.VarChar, 512, Collation_Information_Schema_Default), Default: planbuilder.MustStringToColumnDefaultValue(NewEmptyContext(), `""`, types.LongText, false), Nullable: false, Source: ColumnPrivilegesTableName},
	{Name: "TABLE_SCHEMA", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: planbuilder.MustStringToColumnDefaultValue(NewEmptyContext(), `""`, types.LongText, false), Nullable: false, Source: ColumnPrivilegesTableName},
	{Name: "TABLE_NAME", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: planbuilder.MustStringToColumnDefaultValue(NewEmptyContext(), `""`, types.LongText, false), Nullable: false, Source: ColumnPrivilegesTableName},
	{Name: "COLUMN_NAME", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: planbuilder.MustStringToColumnDefaultValue(NewEmptyContext(), `""`, types.LongText, false), Nullable: false, Source: ColumnPrivilegesTableName},
	{Name: "PRIVILEGE_TYPE", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: planbuilder.MustStringToColumnDefaultValue(NewEmptyContext(), `""`, types.LongText, false), Nullable: false, Source: ColumnPrivilegesTableName},
	{Name: "IS_GRANTABLE", Type: types.MustCreateString(sqltypes.VarChar, 3, Collation_Information_Schema_Default), Default: planbuilder.MustStringToColumnDefaultValue(NewEmptyContext(), `""`, types.LongText, false), Nullable: false, Source: ColumnPrivilegesTableName},
}

var columnStatisticsSchema = Schema{
	{Name: "SCHEMA_NAME", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: ColumnStatisticsTableName},
	{Name: "TABLE_NAME", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: ColumnStatisticsTableName},
	{Name: "COLUMN_NAME", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: ColumnStatisticsTableName},
	{Name: "HISTOGRAM", Type: types.JSON, Default: nil, Nullable: false, Source: ColumnStatisticsTableName},
}

var columnsSchema = Schema{
	{Name: "TABLE_CATALOG", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: ColumnsTableName},
	{Name: "TABLE_SCHEMA", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: ColumnsTableName},
	{Name: "TABLE_NAME", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: ColumnsTableName},
	{Name: "COLUMN_NAME", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: ColumnsTableName},
	{Name: "ORDINAL_POSITION", Type: types.Uint32, Default: nil, Nullable: false, Source: ColumnsTableName},
	{Name: "COLUMN_DEFAULT", Type: types.Text, Default: nil, Nullable: true, Source: ColumnsTableName},
	{Name: "IS_NULLABLE", Type: types.MustCreateString(sqltypes.VarChar, 3, Collation_Information_Schema_Default), Default: planbuilder.MustStringToColumnDefaultValue(NewEmptyContext(), `""`, types.MustCreateString(sqltypes.VarChar, 3, Collation_Information_Schema_Default), false), Nullable: false, Source: ColumnsTableName},
	{Name: "DATA_TYPE", Type: types.LongText, Default: nil, Nullable: true, Source: ColumnsTableName},
	{Name: "CHARACTER_MAXIMUM_LENGTH", Type: types.Int64, Default: nil, Nullable: true, Source: ColumnsTableName},
	{Name: "CHARACTER_OCTET_LENGTH", Type: types.Int64, Default: nil, Nullable: true, Source: ColumnsTableName},
	{Name: "NUMERIC_PRECISION", Type: types.Uint64, Default: nil, Nullable: true, Source: ColumnsTableName},
	{Name: "NUMERIC_SCALE", Type: types.Uint64, Default: nil, Nullable: true, Source: ColumnsTableName},
	{Name: "DATETIME_PRECISION", Type: types.Uint32, Default: nil, Nullable: true, Source: ColumnsTableName},
	{Name: "CHARACTER_SET_NAME", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: ColumnsTableName},
	{Name: "COLLATION_NAME", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: ColumnsTableName},
	{Name: "COLUMN_TYPE", Type: types.MediumText, Default: nil, Nullable: false, Source: ColumnsTableName},
	{Name: "COLUMN_KEY", Type: types.MustCreateEnumType([]string{"", "PRI", "UNI", "MUL"}, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: ColumnsTableName},
	{Name: "EXTRA", Type: types.MustCreateString(sqltypes.VarChar, 256, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: ColumnsTableName},
	{Name: "PRIVILEGES", Type: types.MustCreateString(sqltypes.VarChar, 154, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: ColumnsTableName},
	{Name: "COLUMN_COMMENT", Type: types.Text, Default: nil, Nullable: false, Source: ColumnsTableName},
	{Name: "GENERATION_EXPRESSION", Type: types.LongText, Default: nil, Nullable: false, Source: ColumnsTableName},
	{Name: "SRS_ID", Type: types.Uint32, Default: nil, Nullable: true, Source: ColumnsTableName},
}

var columnsExtensionsSchema = Schema{
	{Name: "TABLE_CATALOG", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: ColumnsExtensionsTableName},
	{Name: "TABLE_SCHEMA", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: ColumnsExtensionsTableName},
	{Name: "TABLE_NAME", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: ColumnsExtensionsTableName},
	{Name: "COLUMN_NAME", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: ColumnsExtensionsTableName},
	{Name: "ENGINE_ATTRIBUTE", Type: types.JSON, Default: nil, Nullable: true, Source: ColumnsExtensionsTableName},
	{Name: "SECONDARY_ENGINE_ATTRIBUTE", Type: types.JSON, Default: nil, Nullable: true, Source: ColumnsExtensionsTableName},
}

var enabledRolesSchema = Schema{
	{Name: "ROLE_NAME", Type: types.MustCreateString(sqltypes.VarChar, 255, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: EnabledRolesTablesName},
	{Name: "ROLE_HOST", Type: types.MustCreateString(sqltypes.VarChar, 255, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: EnabledRolesTablesName},
	{Name: "IS_DEFAULT", Type: types.MustCreateString(sqltypes.VarChar, 3, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: EnabledRolesTablesName},
	{Name: "IS_MANDATORY", Type: types.MustCreateString(sqltypes.VarChar, 3, Collation_Information_Schema_Default), Default: planbuilder.MustStringToColumnDefaultValue(NewEmptyContext(), `""`, types.LongText, false), Nullable: false, Source: EnabledRolesTablesName},
}

var enginesSchema = Schema{
	{Name: "ENGINE", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: planbuilder.MustStringToColumnDefaultValue(NewEmptyContext(), `""`, types.LongText, false), Nullable: false, Source: EnginesTableName},
	{Name: "SUPPORT", Type: types.MustCreateString(sqltypes.VarChar, 8, Collation_Information_Schema_Default), Default: planbuilder.MustStringToColumnDefaultValue(NewEmptyContext(), `""`, types.LongText, false), Nullable: false, Source: EnginesTableName},
	{Name: "COMMENT", Type: types.MustCreateString(sqltypes.VarChar, 80, Collation_Information_Schema_Default), Default: planbuilder.MustStringToColumnDefaultValue(NewEmptyContext(), `""`, types.LongText, false), Nullable: false, Source: EnginesTableName},
	{Name: "TRANSACTIONS", Type: types.MustCreateString(sqltypes.VarChar, 3, Collation_Information_Schema_Default), Default: planbuilder.MustStringToColumnDefaultValue(NewEmptyContext(), `""`, types.LongText, false), Nullable: true, Source: EnginesTableName},
	{Name: "XA", Type: types.MustCreateString(sqltypes.VarChar, 3, Collation_Information_Schema_Default), Default: planbuilder.MustStringToColumnDefaultValue(NewEmptyContext(), `""`, types.LongText, false), Nullable: true, Source: EnginesTableName},
	{Name: "SAVEPOINTS", Type: types.MustCreateString(sqltypes.VarChar, 3, Collation_Information_Schema_Default), Default: planbuilder.MustStringToColumnDefaultValue(NewEmptyContext(), `""`, types.LongText, false), Nullable: true, Source: EnginesTableName},
}

var eventsSchema = Schema{
	{Name: "EVENT_CATALOG", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: EventsTableName},
	{Name: "EVENT_SCHEMA", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: EventsTableName},
	{Name: "EVENT_NAME", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: EventsTableName},
	{Name: "DEFINER", Type: types.MustCreateString(sqltypes.VarChar, 288, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: EventsTableName},
	{Name: "TIME_ZONE", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: EventsTableName},
	{Name: "EVENT_BODY", Type: types.MustCreateString(sqltypes.VarChar, 3, Collation_Information_Schema_Default), Default: planbuilder.MustStringToColumnDefaultValue(NewEmptyContext(), `""`, types.LongText, false), Nullable: false, Source: EventsTableName},
	{Name: "EVENT_DEFINITION", Type: types.LongText, Default: nil, Nullable: false, Source: EventsTableName},
	{Name: "EVENT_TYPE", Type: types.MustCreateString(sqltypes.VarChar, 9, Collation_Information_Schema_Default), Default: planbuilder.MustStringToColumnDefaultValue(NewEmptyContext(), `""`, types.LongText, false), Nullable: false, Source: EventsTableName},
	{Name: "EXECUTE_AT", Type: types.Datetime, Default: nil, Nullable: true, Source: EventsTableName},
	{Name: "INTERVAL_VALUE", Type: types.MustCreateString(sqltypes.VarChar, 256, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: EventsTableName},
	{Name: "INTERVAL_FIELD", Type: types.MustCreateEnumType([]string{
		"YEAR", "QUARTER", "MONTH", "DAY", "HOUR", "MINUTE", "WEEK", "SECOND", "MICROSECOND", "YEAR_MONTH",
		"DAY_HOUR", "DAY_MINUTE", "DAY_SECOND", "HOUR_MINUTE", "HOUR_SECOND", "MINUTE_SECOND",
		"DAY_MICROSECOND", "HOUR_MICROSECOND", "MINUTE_MICROSECOND", "SECOND_MICROSECOND"}, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: EventsTableName},
	{Name: "SQL_MODE", Type: sqlModeSetType, Default: nil, Nullable: false, Source: EventsTableName},
	{Name: "STARTS", Type: types.Datetime, Default: nil, Nullable: true, Source: EventsTableName},
	{Name: "ENDS", Type: types.Datetime, Default: nil, Nullable: true, Source: EventsTableName},
	{Name: "STATUS", Type: types.MustCreateEnumType([]string{"ENABLED", "DISABLED", "SLAVESIDE_DISABLED"}, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: EventsTableName},
	{Name: "ON_COMPLETION", Type: types.MustCreateString(sqltypes.VarChar, 12, Collation_Information_Schema_Default), Default: planbuilder.MustStringToColumnDefaultValue(NewEmptyContext(), `""`, types.MustCreateString(sqltypes.VarChar, 12, Collation_Information_Schema_Default), false), Nullable: false, Source: EventsTableName},
	{Name: "CREATED", Type: types.Timestamp, Default: nil, Nullable: false, Source: EventsTableName},
	{Name: "LAST_ALTERED", Type: types.Timestamp, Default: nil, Nullable: false, Source: EventsTableName},
	{Name: "LAST_EXECUTED", Type: types.Datetime, Default: nil, Nullable: true, Source: EventsTableName},
	{Name: "EVENT_COMMENT", Type: types.MustCreateString(sqltypes.VarChar, 2048, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: EventsTableName},
	{Name: "ORIGINATOR", Type: types.Uint32, Default: nil, Nullable: false, Source: EventsTableName},
	{Name: "CHARACTER_SET_CLIENT", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: EventsTableName},
	{Name: "COLLATION_CONNECTION", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: EventsTableName},
	{Name: "DATABASE_COLLATION", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: EventsTableName},
}

var filesSchema = Schema{
	{Name: "FILE_ID", Type: types.Int64, Default: nil, Nullable: true, Source: FilesTableName},
	{Name: "FILE_NAME", Type: types.Text, Default: nil, Nullable: true, Source: FilesTableName},
	{Name: "FILE_TYPE", Type: types.MustCreateString(sqltypes.VarChar, 256, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: FilesTableName},
	{Name: "TABLESPACE_NAME", Type: types.MustCreateString(sqltypes.VarChar, 268, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: FilesTableName},
	{Name: "TABLE_CATALOG", Type: types.MustCreateString(sqltypes.Char, 0, Collation_Information_Schema_Default), Default: planbuilder.MustStringToColumnDefaultValue(NewEmptyContext(), `""`, types.MustCreateString(sqltypes.Char, 0, Collation_Information_Schema_Default), false), Nullable: true, Source: FilesTableName},
	{Name: "TABLE_SCHEMA", Type: types.MustCreateBinary(sqltypes.Binary, 0), Default: nil, Nullable: true, Source: FilesTableName},
	{Name: "TABLE_NAME", Type: types.MustCreateBinary(sqltypes.Binary, 0), Default: nil, Nullable: true, Source: FilesTableName},
	{Name: "LOGFILE_GROUP_NAME", Type: types.MustCreateString(sqltypes.VarChar, 256, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: FilesTableName},
	{Name: "LOGFILE_GROUP_NUMBER", Type: types.Int64, Default: nil, Nullable: true, Source: FilesTableName},
	{Name: "ENGINE", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: FilesTableName},
	{Name: "FULLTEXT_KEYS", Type: types.MustCreateBinary(sqltypes.Binary, 0), Default: nil, Nullable: true, Source: FilesTableName},
	{Name: "DELETED_ROWS", Type: types.MustCreateBinary(sqltypes.Binary, 0), Default: nil, Nullable: true, Source: FilesTableName},
	{Name: "UPDATE_COUNT", Type: types.MustCreateBinary(sqltypes.Binary, 0), Default: nil, Nullable: true, Source: FilesTableName},
	{Name: "FREE_EXTENTS", Type: types.Int64, Default: nil, Nullable: true, Source: FilesTableName},
	{Name: "TOTAL_EXTENTS", Type: types.Int64, Default: nil, Nullable: true, Source: FilesTableName},
	{Name: "EXTENT_SIZE", Type: types.Int64, Default: nil, Nullable: true, Source: FilesTableName},
	{Name: "INITIAL_SIZE", Type: types.Int64, Default: nil, Nullable: true, Source: FilesTableName},
	{Name: "MAXIMUM_SIZE", Type: types.Int64, Default: nil, Nullable: true, Source: FilesTableName},
	{Name: "AUTOEXTEND_SIZE", Type: types.Int64, Default: nil, Nullable: true, Source: FilesTableName},
	{Name: "CREATION_TIME", Type: types.MustCreateBinary(sqltypes.Binary, 0), Default: nil, Nullable: true, Source: FilesTableName},
	{Name: "LAST_UPDATE_TIME", Type: types.MustCreateBinary(sqltypes.Binary, 0), Default: nil, Nullable: true, Source: FilesTableName},
	{Name: "LAST_ACCESS_TIME", Type: types.MustCreateBinary(sqltypes.Binary, 0), Default: nil, Nullable: true, Source: FilesTableName},
	{Name: "RECOVER_TIME", Type: types.MustCreateBinary(sqltypes.Binary, 0), Default: nil, Nullable: true, Source: FilesTableName},
	{Name: "TRANSACTION_COUNTER", Type: types.MustCreateBinary(sqltypes.Binary, 0), Default: nil, Nullable: true, Source: FilesTableName},
	{Name: "VERSION", Type: types.Int64, Default: nil, Nullable: true, Source: FilesTableName},
	{Name: "ROW_FORMAT", Type: types.MustCreateString(sqltypes.VarChar, 256, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: FilesTableName},
	{Name: "TABLE_ROWS", Type: types.MustCreateBinary(sqltypes.Binary, 0), Default: nil, Nullable: true, Source: FilesTableName},
	{Name: "AVG_ROW_LENGTH", Type: types.MustCreateBinary(sqltypes.Binary, 0), Default: nil, Nullable: true, Source: FilesTableName},
	{Name: "DATA_LENGTH", Type: types.MustCreateBinary(sqltypes.Binary, 0), Default: nil, Nullable: true, Source: FilesTableName},
	{Name: "MAX_DATA_LENGTH", Type: types.MustCreateBinary(sqltypes.Binary, 0), Default: nil, Nullable: true, Source: FilesTableName},
	{Name: "INDEX_LENGTH", Type: types.MustCreateBinary(sqltypes.Binary, 0), Default: nil, Nullable: true, Source: FilesTableName},
	{Name: "DATA_FREE", Type: types.Int64, Default: nil, Nullable: true, Source: FilesTableName},
	{Name: "CREATE_TIME", Type: types.MustCreateBinary(sqltypes.Binary, 0), Default: nil, Nullable: true, Source: FilesTableName},
	{Name: "UPDATE_TIME", Type: types.MustCreateBinary(sqltypes.Binary, 0), Default: nil, Nullable: true, Source: FilesTableName},
	{Name: "CHECK_TIME", Type: types.MustCreateBinary(sqltypes.Binary, 0), Default: nil, Nullable: true, Source: FilesTableName},
	{Name: "CHECKSUM", Type: types.MustCreateBinary(sqltypes.Binary, 0), Default: nil, Nullable: true, Source: FilesTableName},
	{Name: "STATUS", Type: types.MustCreateString(sqltypes.VarChar, 256, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: FilesTableName},
	{Name: "EXTRA", Type: types.MustCreateString(sqltypes.VarChar, 256, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: FilesTableName},
}

var keyColumnUsageSchema = Schema{
	{Name: "CONSTRAINT_CATALOG", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: KeyColumnUsageTableName},
	{Name: "CONSTRAINT_SCHEMA", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: KeyColumnUsageTableName},
	{Name: "CONSTRAINT_NAME", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: KeyColumnUsageTableName},
	{Name: "TABLE_CATALOG", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: KeyColumnUsageTableName},
	{Name: "TABLE_SCHEMA", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: KeyColumnUsageTableName},
	{Name: "TABLE_NAME", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: KeyColumnUsageTableName},
	{Name: "COLUMN_NAME", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: KeyColumnUsageTableName},
	{Name: "ORDINAL_POSITION", Type: types.Uint32, Default: planbuilder.MustStringToColumnDefaultValue(NewEmptyContext(), "0", types.Uint32, false), Nullable: false, Source: KeyColumnUsageTableName},
	{Name: "POSITION_IN_UNIQUE_CONSTRAINT", Type: types.Uint32, Default: nil, Nullable: true, Source: KeyColumnUsageTableName},
	{Name: "REFERENCED_TABLE_SCHEMA", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: KeyColumnUsageTableName},
	{Name: "REFERENCED_TABLE_NAME", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: KeyColumnUsageTableName},
	{Name: "REFERENCED_COLUMN_NAME", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: KeyColumnUsageTableName},
}

var keywordsSchema = Schema{
	{Name: "WORD", Type: types.MustCreateString(sqltypes.VarChar, 128, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: KeywordsTableName},
	{Name: "RESERVED", Type: types.Int32, Default: nil, Nullable: true, Source: KeywordsTableName},
}

var optimizerTraceSchema = Schema{
	{Name: "QUERY", Type: types.MustCreateString(sqltypes.VarChar, 65535, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: OptimizerTraceTableName},
	{Name: "TRACE", Type: types.MustCreateString(sqltypes.VarChar, 65535, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: OptimizerTraceTableName},
	{Name: "MISSING_BYTES_BEYOND_MAX_MEM_SIZE", Type: types.Int32, Default: nil, Nullable: false, Source: OptimizerTraceTableName},
	{Name: "INSUFFICIENT_PRIVILEGES", Type: types.MustCreateBitType(1), Default: nil, Nullable: false, Source: OptimizerTraceTableName},
}

var parametersSchema = Schema{
	{Name: "SPECIFIC_CATALOG", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: ParametersTableName},
	{Name: "SPECIFIC_SCHEMA", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: ParametersTableName},
	{Name: "SPECIFIC_NAME", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: ParametersTableName},
	{Name: "ORDINAL_POSITION", Type: types.Uint64, Default: planbuilder.MustStringToColumnDefaultValue(NewEmptyContext(), "0", types.Uint64, false), Nullable: false, Source: ParametersTableName},
	{Name: "PARAMETER_MODE", Type: types.MustCreateString(sqltypes.VarChar, 5, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: ParametersTableName},
	{Name: "PARAMETER_NAME", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: ParametersTableName},
	{Name: "DATA_TYPE", Type: types.LongText, Default: nil, Nullable: true, Source: ParametersTableName},
	{Name: "CHARACTER_MAXIMUM_LENGTH", Type: types.Int64, Default: nil, Nullable: true, Source: ParametersTableName},
	{Name: "CHARACTER_OCTET_LENGTH", Type: types.Int64, Default: nil, Nullable: true, Source: ParametersTableName},
	{Name: "NUMERIC_PRECISION", Type: types.Uint32, Default: nil, Nullable: true, Source: ParametersTableName},
	{Name: "NUMERIC_SCALE", Type: types.Int64, Default: nil, Nullable: true, Source: ParametersTableName},
	{Name: "DATETIME_PRECISION", Type: types.Uint32, Default: nil, Nullable: true, Source: ParametersTableName},
	{Name: "CHARACTER_SET_NAME", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: ParametersTableName},
	{Name: "COLLATION_NAME", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: ParametersTableName},
	{Name: "DTD_IDENTIFIER", Type: types.MediumText, Default: nil, Nullable: false, Source: ParametersTableName},
	{Name: "ROUTINE_TYPE", Type: types.MustCreateEnumType([]string{"FUNCTION", "PROCEDURE"}, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: ParametersTableName},
}

var partitionsSchema = Schema{
	{Name: "TABLE_CATALOG", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: PartitionsTableName},
	{Name: "TABLE_SCHEMA", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: PartitionsTableName},
	{Name: "TABLE_NAME", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: PartitionsTableName},
	{Name: "PARTITION_NAME", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: PartitionsTableName},
	{Name: "SUBPARTITION_NAME", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: PartitionsTableName},
	{Name: "PARTITION_ORDINAL_POSITION", Type: types.Uint32, Default: nil, Nullable: true, Source: PartitionsTableName},
	{Name: "SUBPARTITION_ORDINAL_POSITION", Type: types.Uint32, Default: nil, Nullable: true, Source: PartitionsTableName},
	{Name: "PARTITION_METHOD", Type: types.MustCreateString(sqltypes.VarChar, 13, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: PartitionsTableName},
	{Name: "SUBPARTITION_METHOD", Type: types.MustCreateString(sqltypes.VarChar, 13, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: PartitionsTableName},
	{Name: "PARTITION_EXPRESSION", Type: types.MustCreateString(sqltypes.VarChar, 2048, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: PartitionsTableName},
	{Name: "SUBPARTITION_EXPRESSION", Type: types.MustCreateString(sqltypes.VarChar, 2048, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: PartitionsTableName},
	{Name: "PARTITION_DESCRIPTION", Type: types.Text, Default: nil, Nullable: true, Source: PartitionsTableName},
	{Name: "TABLE_ROWS", Type: types.Uint64, Default: nil, Nullable: true, Source: PartitionsTableName},
	{Name: "AVG_ROW_LENGTH", Type: types.Uint64, Default: nil, Nullable: true, Source: PartitionsTableName},
	{Name: "DATA_LENGTH", Type: types.Uint64, Default: nil, Nullable: true, Source: PartitionsTableName},
	{Name: "MAX_DATA_LENGTH", Type: types.Uint64, Default: nil, Nullable: true, Source: PartitionsTableName},
	{Name: "INDEX_LENGTH", Type: types.Uint64, Default: nil, Nullable: true, Source: PartitionsTableName},
	{Name: "DATA_FREE", Type: types.Uint64, Default: nil, Nullable: true, Source: PartitionsTableName},
	{Name: "CREATE_TIME", Type: types.Timestamp, Default: nil, Nullable: false, Source: PartitionsTableName},
	{Name: "UPDATE_TIME", Type: types.Datetime, Default: nil, Nullable: true, Source: PartitionsTableName},
	{Name: "CHECK_TIME", Type: types.Datetime, Default: nil, Nullable: true, Source: PartitionsTableName},
	{Name: "CHECKSUM", Type: types.Int64, Default: nil, Nullable: true, Source: PartitionsTableName},
	{Name: "PARTITION_COMMENT", Type: types.Text, Default: nil, Nullable: false, Source: PartitionsTableName},
	{Name: "NODEGROUP", Type: types.MustCreateString(sqltypes.VarChar, 256, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: PartitionsTableName},
	{Name: "TABLESPACE_NAME", Type: types.MustCreateString(sqltypes.VarChar, 268, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: PartitionsTableName},
}

var pluginsSchema = Schema{
	{Name: "PLUGIN_NAME", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: PluginsTableName},
	{Name: "PLUGIN_VERSION", Type: types.MustCreateString(sqltypes.VarChar, 20, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: PluginsTableName},
	{Name: "PLUGIN_STATUS", Type: types.MustCreateString(sqltypes.VarChar, 10, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: PluginsTableName},
	{Name: "PLUGIN_TYPE", Type: types.MustCreateString(sqltypes.VarChar, 80, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: PluginsTableName},
	{Name: "PLUGIN_TYPE_VERSION", Type: types.MustCreateString(sqltypes.VarChar, 20, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: PluginsTableName},
	{Name: "PLUGIN_LIBRARY", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: PluginsTableName},
	{Name: "PLUGIN_LIBRARY_VERSION", Type: types.MustCreateString(sqltypes.VarChar, 20, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: PluginsTableName},
	{Name: "PLUGIN_AUTHOR", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: PluginsTableName},
	{Name: "PLUGIN_DESCRIPTION", Type: types.MustCreateString(sqltypes.VarChar, 65535, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: PluginsTableName},
	{Name: "PLUGIN_LICENSE", Type: types.MustCreateString(sqltypes.VarChar, 80, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: PluginsTableName},
	{Name: "LOAD_OPTION", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: PluginsTableName},
}

var processListSchema = Schema{
	{Name: "ID", Type: types.Uint64, Default: nil, Nullable: false, Source: ProcessListTableName},
	{Name: "USER", Type: types.MustCreateString(sqltypes.VarChar, 32, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: ProcessListTableName},
	{Name: "HOST", Type: types.MustCreateString(sqltypes.VarChar, 261, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: ProcessListTableName},
	{Name: "DB", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: ProcessListTableName},
	{Name: "COMMAND", Type: types.MustCreateString(sqltypes.VarChar, 16, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: ProcessListTableName},
	{Name: "TIME", Type: types.Int32, Default: nil, Nullable: false, Source: ProcessListTableName},
	{Name: "STATE", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: ProcessListTableName},
	{Name: "INFO", Type: types.MustCreateString(sqltypes.VarChar, 65535, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: ProcessListTableName},
}

var profilingSchema = Schema{
	{Name: "QUERY_ID", Type: types.Int32, Default: nil, Nullable: false, Source: ProfilingTableName},
	{Name: "SEQ", Type: types.Int32, Default: nil, Nullable: false, Source: ProfilingTableName},
	{Name: "STATE", Type: types.MustCreateString(sqltypes.VarChar, 30, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: ProfilingTableName},
	{Name: "DURATION", Type: types.MustCreateDecimalType(types.DecimalTypeMaxPrecision, 0), Default: nil, Nullable: false, Source: ProfilingTableName},
	{Name: "CPU_USER", Type: types.MustCreateDecimalType(types.DecimalTypeMaxPrecision, 0), Default: nil, Nullable: true, Source: ProfilingTableName},
	{Name: "CPU_SYSTEM", Type: types.MustCreateDecimalType(types.DecimalTypeMaxPrecision, 0), Default: nil, Nullable: true, Source: ProfilingTableName},
	{Name: "CONTEXT_VOLUNTARY", Type: types.Int32, Default: nil, Nullable: true, Source: ProfilingTableName},
	{Name: "CONTEXT_INVOLUNTARY", Type: types.Int32, Default: nil, Nullable: true, Source: ProfilingTableName},
	{Name: "BLOCK_OPS_IN", Type: types.Int32, Default: nil, Nullable: true, Source: ProfilingTableName},
	{Name: "BLOCK_OPS_OUT", Type: types.Int32, Default: nil, Nullable: true, Source: ProfilingTableName},
	{Name: "MESSAGES_SENT", Type: types.Int32, Default: nil, Nullable: true, Source: ProfilingTableName},
	{Name: "MESSAGES_RECEIVED", Type: types.Int32, Default: nil, Nullable: true, Source: ProfilingTableName},
	{Name: "PAGE_FAULTS_MAJOR", Type: types.Int32, Default: nil, Nullable: true, Source: ProfilingTableName},
	{Name: "PAGE_FAULTS_MINOR", Type: types.Int32, Default: nil, Nullable: true, Source: ProfilingTableName},
	{Name: "SWAPS", Type: types.Int32, Default: nil, Nullable: true, Source: ProfilingTableName},
	{Name: "SOURCE_FUNCTION", Type: types.MustCreateString(sqltypes.VarChar, 30, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: ProfilingTableName},
	{Name: "SOURCE_FILE", Type: types.MustCreateString(sqltypes.VarChar, 20, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: ProfilingTableName},
	{Name: "SOURCE_LINE", Type: types.Int32, Default: nil, Nullable: true, Source: ProfilingTableName},
}

var referentialConstraintsSchema = Schema{
	{Name: "CONSTRAINT_CATALOG", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: ReferentialConstraintsTableName},
	{Name: "CONSTRAINT_SCHEMA", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: ReferentialConstraintsTableName},
	{Name: "CONSTRAINT_NAME", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: ReferentialConstraintsTableName},
	{Name: "UNIQUE_CONSTRAINT_CATALOG", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: ReferentialConstraintsTableName},
	{Name: "UNIQUE_CONSTRAINT_SCHEMA", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: ReferentialConstraintsTableName},
	{Name: "UNIQUE_CONSTRAINT_NAME", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: ReferentialConstraintsTableName},
	{Name: "MATCH_OPTION", Type: types.MustCreateEnumType([]string{"NONE", "PARTIAL", "FULL"}, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: ReferentialConstraintsTableName},
	{Name: "UPDATE_RULE", Type: types.MustCreateEnumType([]string{"NO ACTION", "RESTRICT", "CASCADE", "SET NULL", "SET DEFAULT"}, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: ReferentialConstraintsTableName},
	{Name: "DELETE_RULE", Type: types.MustCreateEnumType([]string{"NO ACTION", "RESTRICT", "CASCADE", "SET NULL", "SET DEFAULT"}, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: ReferentialConstraintsTableName},
	{Name: "TABLE_NAME", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: ReferentialConstraintsTableName},
	{Name: "REFERENCED_TABLE_NAME", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: ReferentialConstraintsTableName},
}

var resourceGroupsSchema = Schema{
	{Name: "RESOURCE_GROUP_NAME", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: ResourceGroupsTableName},
	{Name: "RESOURCE_GROUP_TYPE", Type: types.MustCreateEnumType([]string{"SYSTEM", "USER"}, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: ResourceGroupsTableName},
	{Name: "RESOURCE_GROUP_ENABLE", Type: types.MustCreateBitType(1), Default: nil, Nullable: false, Source: ResourceGroupsTableName},
	{Name: "VPCUS_IDS", Type: types.Blob, Default: nil, Nullable: true, Source: ResourceGroupsTableName},
	{Name: "THREAD_PRIORITY", Type: types.Int32, Default: nil, Nullable: false, Source: ResourceGroupsTableName},
}

var roleColumnGrantsSchema = Schema{
	{Name: "GRANTOR", Type: types.MustCreateString(sqltypes.VarChar, 97, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: RoleColumnGrantsTableName},
	{Name: "GRANTOR_HOST", Type: types.MustCreateString(sqltypes.VarChar, 256, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: RoleColumnGrantsTableName},
	{Name: "GRANTEE", Type: types.MustCreateString(sqltypes.Char, 32, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: RoleColumnGrantsTableName},
	{Name: "GRANTEE_HOST", Type: types.MustCreateString(sqltypes.Char, 255, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: RoleColumnGrantsTableName},
	{Name: "TABLE_CATALOG", Type: types.MustCreateString(sqltypes.VarChar, 3, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: RoleColumnGrantsTableName},
	{Name: "TABLE_SCHEMA", Type: types.MustCreateString(sqltypes.Char, 64, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: RoleColumnGrantsTableName},
	{Name: "TABLE_NAME", Type: types.MustCreateString(sqltypes.Char, 64, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: RoleColumnGrantsTableName},
	{Name: "COLUMN_NAME", Type: types.MustCreateString(sqltypes.Char, 64, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: RoleColumnGrantsTableName},
	{Name: "PRIVILEGE_TYPE", Type: types.MustCreateSetType([]string{"Select", "Insert", "Update", "References"}, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: RoleColumnGrantsTableName},
	{Name: "IS_GRANTABLE", Type: types.MustCreateString(sqltypes.VarChar, 3, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: RoleColumnGrantsTableName},
}

var roleRoutineGrantsSchema = Schema{
	{Name: "GRANTOR", Type: types.MustCreateString(sqltypes.VarChar, 97, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: RoleRoutineGrantsTableName},
	{Name: "GRANTOR_HOST", Type: types.MustCreateString(sqltypes.VarChar, 256, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: RoleRoutineGrantsTableName},
	{Name: "GRANTEE", Type: types.MustCreateString(sqltypes.Char, 32, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: RoleRoutineGrantsTableName},
	{Name: "GRANTEE_HOST", Type: types.MustCreateString(sqltypes.Char, 255, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: RoleRoutineGrantsTableName},
	{Name: "SPECIFIC_CATALOG", Type: types.MustCreateString(sqltypes.VarChar, 3, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: RoleRoutineGrantsTableName},
	{Name: "SPECIFIC_SCHEMA", Type: types.MustCreateString(sqltypes.Char, 64, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: RoleRoutineGrantsTableName},
	{Name: "SPECIFIC_NAME", Type: types.MustCreateString(sqltypes.Char, 64, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: RoleRoutineGrantsTableName},
	{Name: "ROUTINE_CATALOG", Type: types.MustCreateString(sqltypes.VarChar, 3, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: RoleRoutineGrantsTableName},
	{Name: "ROUTINE_SCHEMA", Type: types.MustCreateString(sqltypes.Char, 64, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: RoleRoutineGrantsTableName},
	{Name: "ROUTINE_NAME", Type: types.MustCreateString(sqltypes.Char, 64, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: RoleRoutineGrantsTableName},
	{Name: "PRIVILEGE_TYPE", Type: types.MustCreateSetType([]string{"Execute", "Alter Routine", "Grant"}, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: RoleRoutineGrantsTableName},
	{Name: "IS_GRANTABLE", Type: types.MustCreateString(sqltypes.VarChar, 3, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: RoleRoutineGrantsTableName},
}

var roleTableGrantsSchema = Schema{
	{Name: "GRANTOR", Type: types.MustCreateString(sqltypes.VarChar, 97, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: RoleTableGrantsTableName},
	{Name: "GRANTOR_HOST", Type: types.MustCreateString(sqltypes.VarChar, 256, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: RoleTableGrantsTableName},
	{Name: "GRANTEE", Type: types.MustCreateString(sqltypes.Char, 32, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: RoleTableGrantsTableName},
	{Name: "GRANTEE_HOST", Type: types.MustCreateString(sqltypes.Char, 255, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: RoleTableGrantsTableName},
	{Name: "TABLE_CATALOG", Type: types.MustCreateString(sqltypes.VarChar, 3, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: RoleTableGrantsTableName},
	{Name: "TABLE_SCHEMA", Type: types.MustCreateString(sqltypes.Char, 64, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: RoleTableGrantsTableName},
	{Name: "TABLE_NAME", Type: types.MustCreateString(sqltypes.Char, 64, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: RoleTableGrantsTableName},
	{Name: "PRIVILEGE_TYPE", Type: types.MustCreateSetType([]string{"Select", "Insert", "Update", "Delete", "Create", "Drop", "Grant", "References", "Index", "Alter", "Create View", "Show view", "Trigger"}, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: RoleTableGrantsTableName},
	{Name: "IS_GRANTABLE", Type: types.MustCreateString(sqltypes.VarChar, 3, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: RoleTableGrantsTableName},
}

var routinesSchema = Schema{
	{Name: "SPECIFIC_NAME", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: RoutinesTableName},
	{Name: "ROUTINE_CATALOG", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: RoutinesTableName},
	{Name: "ROUTINE_SCHEMA", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: RoutinesTableName},
	{Name: "ROUTINE_NAME", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: RoutinesTableName},
	{Name: "ROUTINE_TYPE", Type: types.MustCreateEnumType([]string{"FUNCTION", "PROCEDURE"}, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: RoutinesTableName},
	{Name: "DATA_TYPE", Type: types.LongText, Default: nil, Nullable: true, Source: RoutinesTableName},
	{Name: "CHARACTER_MAXIMUM_LENGTH", Type: types.Int64, Default: nil, Nullable: true, Source: RoutinesTableName},
	{Name: "CHARACTER_OCTET_LENGTH", Type: types.Int64, Default: nil, Nullable: true, Source: RoutinesTableName},
	{Name: "NUMERIC_PRECISION", Type: types.Uint32, Default: nil, Nullable: true, Source: RoutinesTableName},
	{Name: "NUMERIC_SCALE", Type: types.Uint32, Default: nil, Nullable: true, Source: RoutinesTableName},
	{Name: "DATETIME_PRECISION", Type: types.Uint32, Default: nil, Nullable: true, Source: RoutinesTableName},
	{Name: "CHARACTER_SET_NAME", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: RoutinesTableName},
	{Name: "COLLATION_NAME", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: RoutinesTableName},
	{Name: "DTD_IDENTIFIER", Type: types.LongText, Default: nil, Nullable: true, Source: RoutinesTableName},
	{Name: "ROUTINE_BODY", Type: types.MustCreateString(sqltypes.VarChar, 3, Collation_Information_Schema_Default), Default: planbuilder.MustStringToColumnDefaultValue(NewEmptyContext(), `""`, types.MustCreateString(sqltypes.VarChar, 3, Collation_Information_Schema_Default), false), Nullable: false, Source: RoutinesTableName},
	{Name: "ROUTINE_DEFINITION", Type: types.LongText, Default: nil, Nullable: true, Source: RoutinesTableName},
	{Name: "EXTERNAL_NAME", Type: types.MustCreateBinary(sqltypes.Binary, 0), Default: nil, Nullable: true, Source: RoutinesTableName},
	{Name: "EXTERNAL_LANGUAGE", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: planbuilder.MustStringToColumnDefaultValue(NewEmptyContext(), `"SQL"`, types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), false), Nullable: false, Source: RoutinesTableName},
	{Name: "PARAMETER_STYLE", Type: types.MustCreateString(sqltypes.VarChar, 3, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: RoutinesTableName},
	{Name: "IS_DETERMINISTIC", Type: types.MustCreateString(sqltypes.VarChar, 3, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: RoutinesTableName},
	{Name: "SQL_DATA_ACCESS", Type: types.MustCreateEnumType([]string{"CONTAINS SQL", "NO SQL", "READS SQL DATA", "MODIFIES SQL DATA"}, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: RoutinesTableName},
	{Name: "SQL_PATH", Type: types.MustCreateBinary(sqltypes.Binary, 0), Default: nil, Nullable: true, Source: RoutinesTableName},
	{Name: "SECURITY_TYPE", Type: types.MustCreateEnumType([]string{"DEFAULT", "INVOKER", "DEFINER"}, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: RoutinesTableName},
	{Name: "CREATED", Type: types.Timestamp, Default: nil, Nullable: false, Source: RoutinesTableName},
	{Name: "LAST_ALTERED", Type: types.Timestamp, Default: nil, Nullable: false, Source: RoutinesTableName},
	{Name: "SQL_MODE", Type: sqlModeSetType, Default: nil, Nullable: false, Source: RoutinesTableName},
	{Name: "ROUTINE_COMMENT", Type: types.Text, Default: nil, Nullable: false, Source: RoutinesTableName},
	{Name: "DEFINER", Type: types.MustCreateString(sqltypes.VarChar, 288, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: RoutinesTableName},
	{Name: "CHARACTER_SET_CLIENT", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: RoutinesTableName},
	{Name: "COLLATION_CONNECTION", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: RoutinesTableName},
	{Name: "DATABASE_COLLATION", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: RoutinesTableName},
}

var schemaPrivilegesSchema = Schema{
	{Name: "GRANTEE", Type: types.MustCreateString(sqltypes.VarChar, 292, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: SchemaPrivilegesTableName},
	{Name: "TABLE_CATALOG", Type: types.MustCreateString(sqltypes.VarChar, 512, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: SchemaPrivilegesTableName},
	{Name: "TABLE_SCHEMA", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: SchemaPrivilegesTableName},
	{Name: "PRIVILEGE_TYPE", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: SchemaPrivilegesTableName},
	{Name: "IS_GRANTABLE", Type: types.MustCreateString(sqltypes.VarChar, 3, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: SchemaPrivilegesTableName},
}

var schemataSchema = Schema{
	{Name: "CATALOG_NAME", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: SchemataTableName},
	{Name: "SCHEMA_NAME", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: SchemataTableName},
	{Name: "DEFAULT_CHARACTER_SET_NAME", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: SchemataTableName},
	{Name: "DEFAULT_COLLATION_NAME", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: SchemataTableName},
	{Name: "SQL_PATH", Type: types.MustCreateBinary(sqltypes.Binary, 0), Default: nil, Nullable: true, Source: SchemataTableName},
	{Name: "DEFAULT_ENCRYPTION", Type: types.MustCreateEnumType([]string{"NO", "YES"}, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: SchemataTableName},
}

var schemataExtensionsSchema = Schema{
	{Name: "CATALOG_NAME", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: SchemataExtensionsTableName},
	{Name: "SCHEMA_NAME", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: SchemataExtensionsTableName},
	{Name: "OPTIONS", Type: types.MustCreateString(sqltypes.VarChar, 256, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: SchemataExtensionsTableName},
}

var stGeometryColumnsSchema = Schema{
	{Name: "TABLE_CATALOG", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: StGeometryColumnsTableName},
	{Name: "TABLE_SCHEMA", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: StGeometryColumnsTableName},
	{Name: "TABLE_NAME", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: StGeometryColumnsTableName},
	{Name: "COLUMN_NAME", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: StGeometryColumnsTableName},
	{Name: "SRS_NAME", Type: types.MustCreateString(sqltypes.VarChar, 80, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: StGeometryColumnsTableName},
	{Name: "SRS_ID", Type: types.Uint32, Default: nil, Nullable: true, Source: StGeometryColumnsTableName},
	{Name: "GEOMETRY_TYPE_NAME", Type: types.LongText, Default: nil, Nullable: true, Source: StGeometryColumnsTableName},
}

var stSpatialReferenceSystemsSchema = Schema{
	{Name: "SRS_NAME", Type: types.MustCreateString(sqltypes.VarChar, 80, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: StSpatialReferenceSystemsTableName},
	{Name: "SRS_ID", Type: types.Uint32, Default: nil, Nullable: false, Source: StSpatialReferenceSystemsTableName},
	{Name: "ORGANIZATION", Type: types.MustCreateString(sqltypes.VarChar, 256, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: StSpatialReferenceSystemsTableName},
	{Name: "ORGANIZATION_COORDSYS_ID", Type: types.Uint32, Default: nil, Nullable: true, Source: StSpatialReferenceSystemsTableName},
	{Name: "DEFINITION", Type: types.MustCreateString(sqltypes.VarChar, 4096, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: StSpatialReferenceSystemsTableName},
	{Name: "DESCRIPTION", Type: types.MustCreateString(sqltypes.VarChar, 2048, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: StSpatialReferenceSystemsTableName},
}

var stUnitsOfMeasureSchema = Schema{
	{Name: "UNIT_NAME", Type: types.MustCreateString(sqltypes.VarChar, 255, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: StUnitsOfMeasureTableName},
	{Name: "UNIT_TYPE", Type: types.MustCreateString(sqltypes.VarChar, 7, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: StUnitsOfMeasureTableName},
	{Name: "CONVERSION_FACTOR", Type: types.Float64, Default: nil, Nullable: true, Source: StUnitsOfMeasureTableName},
	{Name: "DESCRIPTION", Type: types.MustCreateString(sqltypes.VarChar, 255, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: StUnitsOfMeasureTableName},
}

var statisticsSchema = Schema{
	{Name: "TABLE_CATALOG", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: StatisticsTableName},
	{Name: "TABLE_SCHEMA", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: StatisticsTableName},
	{Name: "TABLE_NAME", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: StatisticsTableName},
	{Name: "NON_UNIQUE", Type: types.Int32, Default: nil, Nullable: false, Source: StatisticsTableName},
	{Name: "INDEX_SCHEMA", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: StatisticsTableName},
	{Name: "INDEX_NAME", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: StatisticsTableName},
	{Name: "SEQ_IN_INDEX", Type: types.Uint32, Default: nil, Nullable: false, Source: StatisticsTableName},
	{Name: "COLUMN_NAME", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: StatisticsTableName},
	{Name: "COLLATION", Type: types.MustCreateString(sqltypes.VarChar, 1, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: StatisticsTableName},
	{Name: "CARDINALITY", Type: types.Int64, Default: nil, Nullable: true, Source: StatisticsTableName},
	{Name: "SUB_PART", Type: types.Int64, Default: nil, Nullable: true, Source: StatisticsTableName},
	{Name: "PACKED", Type: types.MustCreateBinary(sqltypes.Binary, 0), Default: nil, Nullable: true, Source: StatisticsTableName},
	{Name: "NULLABLE", Type: types.MustCreateString(sqltypes.VarChar, 3, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: StatisticsTableName},
	{Name: "INDEX_TYPE", Type: types.MustCreateString(sqltypes.VarChar, 11, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: StatisticsTableName},
	{Name: "COMMENT", Type: types.MustCreateString(sqltypes.VarChar, 8, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: StatisticsTableName},
	{Name: "INDEX_COMMENT", Type: types.MustCreateString(sqltypes.VarChar, 2048, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: StatisticsTableName},
	{Name: "IS_VISIBLE", Type: types.MustCreateString(sqltypes.VarChar, 3, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: StatisticsTableName},
	{Name: "EXPRESSION", Type: types.LongText, Default: nil, Nullable: true, Source: StatisticsTableName},
}

var tableConstraintsSchema = Schema{
	{Name: "CONSTRAINT_CATALOG", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: TableConstraintsTableName},
	{Name: "CONSTRAINT_SCHEMA", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: TableConstraintsTableName},
	{Name: "CONSTRAINT_NAME", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: TableConstraintsTableName},
	{Name: "TABLE_SCHEMA", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: TableConstraintsTableName},
	{Name: "TABLE_NAME", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: TableConstraintsTableName},
	{Name: "CONSTRAINT_TYPE", Type: types.MustCreateString(sqltypes.VarChar, 11, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: TableConstraintsTableName},
	{Name: "ENFORCED", Type: types.MustCreateString(sqltypes.VarChar, 3, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: TableConstraintsTableName},
}

var tableConstraintsExtensionsSchema = Schema{
	{Name: "CONSTRAINT_CATALOG", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: TableConstraintsExtensionsTableName},
	{Name: "CONSTRAINT_SCHEMA", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: TableConstraintsExtensionsTableName},
	{Name: "CONSTRAINT_NAME", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: TableConstraintsExtensionsTableName},
	{Name: "TABLE_NAME", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: TableConstraintsExtensionsTableName},
	{Name: "ENGINE_ATTRIBUTE", Type: types.JSON, Default: nil, Nullable: true, Source: TableConstraintsExtensionsTableName},
	{Name: "SECONDARY_ENGINE_ATTRIBUTE", Type: types.JSON, Default: nil, Nullable: true, Source: TableConstraintsExtensionsTableName},
}

var tablePrivilegesSchema = Schema{
	{Name: "GRANTEE", Type: types.MustCreateString(sqltypes.VarChar, 292, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: TablePrivilegesTableName},
	{Name: "TABLE_CATALOG", Type: types.MustCreateString(sqltypes.VarChar, 512, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: TablePrivilegesTableName},
	{Name: "TABLE_SCHEMA", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: TablePrivilegesTableName},
	{Name: "TABLE_NAME", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: TablePrivilegesTableName},
	{Name: "PRIVILEGE_TYPE", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: TablePrivilegesTableName},
	{Name: "IS_GRANTABLE", Type: types.MustCreateString(sqltypes.VarChar, 3, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: TablePrivilegesTableName},
}

var tablesSchema = Schema{
	{Name: "TABLE_CATALOG", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: TablesTableName},
	{Name: "TABLE_SCHEMA", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: TablesTableName},
	{Name: "TABLE_NAME", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: TablesTableName},
	{Name: "TABLE_TYPE", Type: types.MustCreateEnumType([]string{"BASE TABLE", "VIEW", "SYSTEM VIEW"}, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: TablesTableName},
	{Name: "ENGINE", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: TablesTableName},
	{Name: "VERSION", Type: types.Int32, Default: nil, Nullable: true, Source: TablesTableName},
	{Name: "ROW_FORMAT", Type: types.MustCreateEnumType([]string{"Fixed", "Dynamic", "Compressed", "Redundant", "Compact", "Paged"}, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: TablesTableName},
	{Name: "TABLE_ROWS", Type: types.Uint64, Default: nil, Nullable: true, Source: TablesTableName},
	{Name: "AVG_ROW_LENGTH", Type: types.Uint64, Default: nil, Nullable: true, Source: TablesTableName},
	{Name: "DATA_LENGTH", Type: types.Uint64, Default: nil, Nullable: true, Source: TablesTableName},
	{Name: "MAX_DATA_LENGTH", Type: types.Uint64, Default: nil, Nullable: true, Source: TablesTableName},
	{Name: "INDEX_LENGTH", Type: types.Uint64, Default: nil, Nullable: true, Source: TablesTableName},
	{Name: "DATA_FREE", Type: types.Uint64, Default: nil, Nullable: true, Source: TablesTableName},
	{Name: "AUTO_INCREMENT", Type: types.Uint64, Default: nil, Nullable: true, Source: TablesTableName},
	{Name: "CREATE_TIME", Type: types.Timestamp, Default: nil, Nullable: false, Source: TablesTableName},
	{Name: "UPDATE_TIME", Type: types.Datetime, Default: nil, Nullable: true, Source: TablesTableName},
	{Name: "CHECK_TIME", Type: types.Datetime, Default: nil, Nullable: true, Source: TablesTableName},
	{Name: "TABLE_COLLATION", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: TablesTableName},
	{Name: "CHECKSUM", Type: types.Int64, Default: nil, Nullable: true, Source: TablesTableName},
	{Name: "CREATE_OPTIONS", Type: types.MustCreateString(sqltypes.VarChar, 256, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: TablesTableName},
	{Name: "TABLE_COMMENT", Type: types.Text, Default: nil, Nullable: true, Source: TablesTableName},
}

var tablesExtensionsSchema = Schema{
	{Name: "TABLE_CATALOG", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: TablesExtensionsTableName},
	{Name: "TABLE_SCHEMA", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: TablesExtensionsTableName},
	{Name: "TABLE_NAME", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: TablesExtensionsTableName},
	{Name: "ENGINE_ATTRIBUTE", Type: types.JSON, Default: nil, Nullable: true, Source: TablesExtensionsTableName},
	{Name: "SECONDARY_ENGINE_ATTRIBUTE", Type: types.JSON, Default: nil, Nullable: true, Source: TablesExtensionsTableName},
}

var tablespacesSchema = Schema{
	{Name: "TABLESPACE_NAME", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: TablespacesTableName},
	{Name: "ENGINE", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: TablespacesTableName},
	{Name: "TABLESPACE_TYPE", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: TablespacesTableName},
	{Name: "LOGFILE_GROUP_NAME", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: TablespacesTableName},
	{Name: "EXTENT_SIZE", Type: types.Uint64, Default: nil, Nullable: true, Source: TablespacesTableName},
	{Name: "AUTOEXTEND_SIZE", Type: types.Uint64, Default: nil, Nullable: true, Source: TablespacesTableName},
	{Name: "MAXIMUM_SIZE", Type: types.Uint64, Default: nil, Nullable: true, Source: TablespacesTableName},
	{Name: "NODEGROUP_ID", Type: types.Uint64, Default: nil, Nullable: true, Source: TablespacesTableName},
	{Name: "TABLESPACE_COMMENT", Type: types.MustCreateString(sqltypes.VarChar, 2048, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: TablespacesTableName},
}

var tablespacesExtensionsSchema = Schema{
	{Name: "TABLESPACE_NAME", Type: types.MustCreateString(sqltypes.VarChar, 268, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: TablespacesExtensionsTableName},
	{Name: "ENGINE_ATTRIBUTE", Type: types.JSON, Default: nil, Nullable: true, Source: TablespacesExtensionsTableName},
}

var triggersSchema = Schema{
	{Name: "TRIGGER_CATALOG", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: TriggersTableName},
	{Name: "TRIGGER_SCHEMA", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: TriggersTableName},
	{Name: "TRIGGER_NAME", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: TriggersTableName},
	{Name: "EVENT_MANIPULATION", Type: types.MustCreateEnumType([]string{"INSERT", "UPDATE", "DELETE"}, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: TriggersTableName},
	{Name: "EVENT_OBJECT_CATALOG", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: TriggersTableName},
	{Name: "EVENT_OBJECT_SCHEMA", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: TriggersTableName},
	{Name: "EVENT_OBJECT_TABLE", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: TriggersTableName},
	{Name: "ACTION_ORDER", Type: types.Uint32, Default: nil, Nullable: false, Source: TriggersTableName},
	{Name: "ACTION_CONDITION", Type: types.MustCreateBinary(sqltypes.Binary, 0), Default: nil, Nullable: true, Source: TriggersTableName},
	{Name: "ACTION_STATEMENT", Type: types.LongText, Default: nil, Nullable: false, Source: TriggersTableName},
	{Name: "ACTION_ORIENTATION", Type: types.MustCreateString(sqltypes.VarChar, 3, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: TriggersTableName},
	{Name: "ACTION_TIMING", Type: types.MustCreateEnumType([]string{"BEFORE", "AFTER"}, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: TriggersTableName},
	{Name: "ACTION_REFERENCE_OLD_TABLE", Type: types.MustCreateBinary(sqltypes.Binary, 0), Default: nil, Nullable: true, Source: TriggersTableName},
	{Name: "ACTION_REFERENCE_NEW_TABLE", Type: types.MustCreateBinary(sqltypes.Binary, 0), Default: nil, Nullable: true, Source: TriggersTableName},
	{Name: "ACTION_REFERENCE_OLD_ROW", Type: types.MustCreateString(sqltypes.VarChar, 3, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: TriggersTableName},
	{Name: "ACTION_REFERENCE_NEW_ROW", Type: types.MustCreateString(sqltypes.VarChar, 3, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: TriggersTableName},
	{Name: "CREATED", Type: types.Timestamp, Default: nil, Nullable: false, Source: TriggersTableName},
	{Name: "SQL_MODE", Type: sqlModeSetType, Default: nil, Nullable: false, Source: TriggersTableName},
	{Name: "DEFINER", Type: types.MustCreateString(sqltypes.VarChar, 288, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: TriggersTableName},
	{Name: "CHARACTER_SET_CLIENT", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: TriggersTableName},
	{Name: "COLLATION_CONNECTION", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: TriggersTableName},
	{Name: "DATABASE_COLLATION", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: TriggersTableName},
}

var userAttributesSchema = Schema{
	{Name: "USER", Type: types.MustCreateString(sqltypes.VarChar, 32, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: UserAttributesTableName},
	{Name: "HOST", Type: types.MustCreateString(sqltypes.VarChar, 255, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: UserAttributesTableName},
	{Name: "ATTRIBUTE", Type: types.LongText, Default: nil, Nullable: true, Source: UserAttributesTableName},
}

var userPrivilegesSchema = Schema{
	{Name: "GRANTEE", Type: types.MustCreateString(sqltypes.VarChar, 292, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: UserPrivilegesTableName},
	{Name: "TABLE_CATALOG", Type: types.MustCreateString(sqltypes.VarChar, 512, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: UserPrivilegesTableName},
	{Name: "PRIVILEGE_TYPE", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: UserPrivilegesTableName},
	{Name: "IS_GRANTABLE", Type: types.MustCreateString(sqltypes.VarChar, 3, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: UserPrivilegesTableName},
}

var viewRoutineUsageSchema = Schema{
	{Name: "TABLE_CATALOG", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: ViewRoutineUsageTableName},
	{Name: "TABLE_SCHEMA", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: ViewRoutineUsageTableName},
	{Name: "TABLE_NAME", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: ViewRoutineUsageTableName},
	{Name: "SPECIFIC_CATALOG", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: ViewRoutineUsageTableName},
	{Name: "SPECIFIC_SCHEMA", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: ViewRoutineUsageTableName},
	{Name: "SPECIFIC_TABLE", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: ViewRoutineUsageTableName},
}

var viewTableUsageSchema = Schema{
	{Name: "VIEW_CATALOG", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: ViewTableUsageTableName},
	{Name: "VIEW_SCHEMA", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: ViewTableUsageTableName},
	{Name: "VIEW_NAME", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: ViewTableUsageTableName},
	{Name: "TABLE_CATALOG", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: ViewTableUsageTableName},
	{Name: "TABLE_SCHEMA", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: ViewTableUsageTableName},
	{Name: "TABLE_NAME", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: ViewTableUsageTableName},
}

var viewsSchema = Schema{
	{Name: "TABLE_CATALOG", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: ViewsTableName},
	{Name: "TABLE_SCHEMA", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: ViewsTableName},
	{Name: "TABLE_NAME", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: ViewsTableName},
	{Name: "VIEW_DEFINITION", Type: types.LongText, Default: nil, Nullable: true, Source: ViewsTableName},
	{Name: "CHECK_OPTION", Type: types.MustCreateEnumType([]string{"NONE", "LOCAL", "CASCADED"}, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: ViewsTableName},
	{Name: "IS_UPDATABLE", Type: types.MustCreateEnumType([]string{"NO", "YES"}, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: ViewsTableName},
	{Name: "DEFINER", Type: types.MustCreateString(sqltypes.VarChar, 288, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: ViewsTableName},
	{Name: "SECURITY_TYPE", Type: types.MustCreateString(sqltypes.VarChar, 7, Collation_Information_Schema_Default), Default: nil, Nullable: true, Source: ViewsTableName},
	{Name: "CHARACTER_SET_CLIENT", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: ViewsTableName},
	{Name: "COLLATION_CONNECTION", Type: types.MustCreateString(sqltypes.VarChar, 64, Collation_Information_Schema_Default), Default: nil, Nullable: false, Source: ViewsTableName},
}

// characterSetsRowIter implements the sql.RowIter for the information_schema.CHARACTER_SETS table.
func characterSetsRowIter(ctx *Context, c Catalog) (RowIter, error) {
	var rows []Row
	for _, c := range SupportedCharsets {
		rows = append(rows, Row{
			c.String(),                  // character_set_name
			c.DefaultCollation().Name(), // default_collation_name
			c.Description(),             // description
			uint64(c.MaxLength()),       // maxlen
		})
	}
	return RowsToRowIter(rows...), nil
}

// checkConstraintsRowIter implements the sql.RowIter for the information_schema.CHECK_CONSTRAINTS table.
func checkConstraintsRowIter(ctx *Context, c Catalog) (RowIter, error) {
	var rows []Row

	databases, err := allDatabases(ctx, c, false)
	if err != nil {
		return nil, err
	}

	for _, db := range databases {
		tableNames, err := db.database.GetTableNames(ctx)
		if err != nil {
			return nil, err
		}

		for _, tableName := range tableNames {
			tbl, _, err := c.DatabaseTable(ctx, db.database, tableName)
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
					rows = append(rows, Row{
						db.catalogName,                  // constraint_catalog
						db.schemaName,                   // constraint_schema
						checkDefinition.Name,            // constraint_name
						checkDefinition.CheckExpression, // check_clause
					})
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
			c.Name,                  // collation_name
			c.CharacterSet.String(), // character_set_name
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
			c.Name,                // collation_name
			c.CharacterSet.Name(), // character_set_name
			uint64(c.ID),          // id
			c.ID.IsDefault(),      // is_default
			c.ID.IsCompiled(),     // is_compiled
			c.ID.SortLength(),     // sortlen
			c.ID.PadAttribute(),   // pad_attribute
		})
	}
	return RowsToRowIter(rows...), nil
}

// columnStatisticsRowIter implements the sql.RowIter for the information_schema.COLUMN_STATISTICS table.
func columnStatisticsRowIter(ctx *Context, c Catalog) (RowIter, error) {
	var rows []Row
	privSet, privSetCount := ctx.GetPrivilegeSet()
	if privSetCount == 0 {
		return nil, nil
	}
	if privSet == nil {
		return RowsToRowIter(rows...), nil
	}

	databases, err := allDatabases(ctx, c, false)
	if err != nil {
		return nil, err
	}

	for _, db := range databases {
		dbName := db.database.Name()
		privSetDb := privSet.Database(dbName)

		err := DBTableIter(ctx, db.database, func(t Table) (cont bool, err error) {
			privSetTbl := privSetDb.Table(t.Name())
			tableStats, err := c.GetTableStats(ctx, dbName, t)
			if err != nil {
				return true, nil
			}
			for _, stats := range tableStats {
				for _, c := range stats.Columns() {
					if privSetTbl.Count() == 0 && privSetDb.Count() == 0 && privSetTbl.Column(c).Count() == 0 {
						continue
					}
				}
				rows = append(rows, Row{
					db.schemaName,                      // table_schema
					t.Name(),                           // table_name
					strings.Join(stats.Columns(), ","), // column_name
					stats,                              // histogram
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

// columnsExtensionsRowIter implements the sql.RowIter for the information_schema.COLUMNS_EXTENSIONS table.
func columnsExtensionsRowIter(ctx *Context, cat Catalog) (RowIter, error) {
	var rows []Row

	databases, err := allDatabases(ctx, cat, false)
	if err != nil {
		return nil, err
	}

	for _, db := range databases {
		err := DBTableIter(ctx, db.database, func(t Table) (cont bool, err error) {
			tblName := t.Name()
			for _, col := range t.Schema() {
				rows = append(rows, Row{
					db.catalogName, // table_catalog
					db.schemaName,  // table_schema
					tblName,        // table_name
					col.Name,       // column_name
					nil,            // engine_attribute // TODO: reserved for future use
					nil,            // secondary_engine_attribute // TODO: reserved for future use
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

// enginesRowIter implements the sql.RowIter for the information_schema.ENGINES table.
func enginesRowIter(ctx *Context, cat Catalog) (RowIter, error) {
	var rows []Row
	for _, c := range SupportedEngines {
		rows = append(rows, Row{
			c.String(),       // engine
			c.Support(),      // support
			c.Comment(),      // comment
			c.Transactions(), // transactions
			c.XA(),           // xa
			c.Savepoints(),   // savepoints
		})
	}
	return RowsToRowIter(rows...), nil
}

// eventsRowIter implements the sql.RowIter for the information_schema.EVENTS table.
func eventsRowIter(ctx *Context, c Catalog) (RowIter, error) {
	var rows []Row

	characterSetClient, err := ctx.GetSessionVariable(ctx, "character_set_client")
	if err != nil {
		return nil, err
	}
	collationConnection, err := ctx.GetSessionVariable(ctx, "collation_connection")

	databases, err := allDatabases(ctx, c, false)
	if err != nil {
		return nil, err
	}

	for _, db := range databases {
		dbCollation := plan.GetDatabaseCollation(ctx, db.database)

		eventDb, ok := db.database.(EventDatabase)
		if ok {
			eventDefs, _, err := eventDb.GetEvents(ctx)
			if err != nil {
				return nil, err
			}
			if len(eventDefs) == 0 {
				continue
			}

			for _, e := range eventDefs {
				ed := e.ConvertTimesFromUTCToTz(gmstime.SystemTimezoneOffset())
				var at, intervalVal, intervalField, starts, ends interface{}
				var eventType, status string
				if ed.HasExecuteAt {
					eventType = "ONE TIME"
					at = ed.ExecuteAt.Format(EventDateSpaceTimeFormat)
				} else {
					eventType = "RECURRING"
					interval, err := EventOnScheduleEveryIntervalFromString(ed.ExecuteEvery)
					if err != nil {
						return nil, err
					}
					intervalVal, intervalField = interval.GetIntervalValAndField()
					starts = ed.Starts.Format(EventDateSpaceTimeFormat)
					if ed.HasEnds {
						ends = ed.Ends.Format(EventDateSpaceTimeFormat)
					}
				}

				eventStatus, err := EventStatusFromString(ed.Status)
				if err != nil {
					return nil, err
				}
				switch eventStatus {
				case EventStatus_Enable:
					status = "ENABLED"
				case EventStatus_Disable:
					status = "DISABLED"
				case EventStatus_DisableOnSlave:
					status = "SLAVESIDE_DISABLED"
				}

				onCompPerserve := "NOT PRESERVE"
				if ed.OnCompletionPreserve {
					onCompPerserve = "PRESERVE"
				}

				created := ed.CreatedAt.Format(EventDateSpaceTimeFormat)
				lastAltered := ed.LastAltered.Format(EventDateSpaceTimeFormat)
				lastExecuted := ed.LastExecuted.Format(EventDateSpaceTimeFormat)
				// TODO: timezone should use e.TimezoneOffset, but is always 'SYSTEM' for now.

				rows = append(rows, Row{
					db.catalogName,       // event_catalog
					db.schemaName,        // event_schema
					ed.Name,              // event_name
					ed.Definer,           // definer
					"SYSTEM",             // time_zone
					"SQL",                // event_body
					ed.EventBody,         // event_definition
					eventType,            // event_type
					at,                   // execute_at
					intervalVal,          // interval_value
					intervalField,        // interval_field
					e.SqlMode,            // sql_mode
					starts,               // starts
					ends,                 // ends
					status,               // status
					onCompPerserve,       // on_completion
					created,              // created
					lastAltered,          // last_altered
					lastExecuted,         // last_executed
					ed.Comment,           // event_comment
					0,                    // originator
					characterSetClient,   // character_set_client
					collationConnection,  // collation_connection
					dbCollation.String(), // database_collation
				})
			}
		}
	}

	return RowsToRowIter(rows...), nil
}

// keyColumnUsageRowIter implements the sql.RowIter for the information_schema.KEY_COLUMN_USAGE table.
func keyColumnUsageRowIter(ctx *Context, c Catalog) (RowIter, error) {
	var rows []Row

	databases, err := allDatabases(ctx, c, false)
	if err != nil {
		return nil, err
	}

	for _, db := range databases {
		tableNames, err := db.database.GetTableNames(ctx)
		if err != nil {
			return nil, err
		}

		for _, tableName := range tableNames {
			tbl, _, err := c.DatabaseTable(ctx, db.database, tableName)
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

						rows = append(rows, Row{
							db.catalogName,  // constraint_catalog
							db.schemaName,   // constraint_schema
							index.ID(),      // constraint_name
							db.catalogName,  // table_catalog
							db.schemaName,   // table_schema
							tbl.Name(),      // table_name
							colName,         // column_name
							ordinalPosition, // ordinal_position
							nil,             // position_in_unique_constraint
							nil,             // referenced_table_schema
							nil,             // referenced_table_name
							nil,             // referenced_column_name
						})
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

						rows = append(rows, Row{
							db.catalogName,       // constraint_catalog
							db.schemaName,        // constraint_schema
							fk.Name,              // constraint_name
							db.catalogName,       // table_catalog
							db.schemaName,        // table_schema
							tbl.Name(),           // table_name
							colName,              // column_name
							ordinalPosition,      // ordinal_position
							ordinalPosition,      // position_in_unique_constraint
							referencedSchema,     // referenced_table_schema
							referencedTableName,  // referenced_table_name
							referencedColumnName, // referenced_column_name
						})
					}
				}
			}
		}
	}

	return RowsToRowIter(rows...), nil
}

// keywordsRowIter implements the sql.RowIter for the information_schema.KEYWORDS table.
func keywordsRowIter(ctx *Context, cat Catalog) (RowIter, error) {
	var rows []Row
	for _, spRef := range keywordsArray {
		rows = append(rows, Row{
			spRef.Word,     // word
			spRef.Reserved, // reserved
		})
	}

	return RowsToRowIter(rows...), nil
}

// processListRowIter implements the sql.RowIter for the information_schema.PROCESSLIST table.
func processListRowIter(ctx *Context, c Catalog) (RowIter, error) {
	processes := ctx.ProcessList.Processes()
	var rows = make([]Row, len(processes))

	for i, proc := range processes {
		var status []string
		for name, progress := range proc.Progress {
			status = append(status, fmt.Sprintf("%s(%s)", name, progress))
		}
		if len(status) == 0 && proc.Command == ProcessCommandQuery {
			status = []string{"running"}
		}
		sort.Strings(status)

		var db interface{}
		if proc.Database != "" {
			db = proc.Database
		}

		rows[i] = Row{
			uint64(proc.Connection),    // id
			proc.User,                  // user
			proc.Host,                  // host
			db,                         // db
			string(proc.Command),       // command
			int32(proc.Seconds()),      // time
			strings.Join(status, ", "), // state
			proc.Query,                 // info
		}
	}

	return RowsToRowIter(rows...), nil
}

// referentialConstraintsRowIter implements the sql.RowIter for the information_schema.REFERENTIAL_CONSTRAINTS table.
func referentialConstraintsRowIter(ctx *Context, c Catalog) (RowIter, error) {
	var rows []Row

	databases, err := allDatabases(ctx, c, false)
	if err != nil {
		return nil, err
	}

	for _, db := range databases {
		tableNames, err := db.database.GetTableNames(ctx)
		if err != nil {
			return nil, err
		}

		for _, tableName := range tableNames {
			tbl, _, err := c.DatabaseTable(ctx, db.database, tableName)
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

					// ErrTableNotFound is returned when the referenced table is dropped, so `unique_constraint_name` column will not be filled.
					refTbl, _, refErr := c.Table(ctx, referencedSchema, referencedTableName)
					if refErr == nil {
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
					} else if !ErrTableNotFound.Is(refErr) {
						return nil, refErr
					}

					rows = append(rows, Row{
						db.catalogName,      // constraint_catalog
						db.schemaName,       // constraint_schema
						fk.Name,             // constraint_name
						db.catalogName,      // unique_constraint_catalog
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

// schemaPrivilegesRowIter implements the sql.RowIter for the information_schema.SCHEMA_PRIVILEGES table.
func schemaPrivilegesRowIter(ctx *Context, c Catalog) (RowIter, error) {
	var rows []Row
	privSet, _ := ctx.GetPrivilegeSet()
	if privSet == nil {
		privSet = mysql_db.NewPrivilegeSet()
	}
	if privSet.Has(PrivilegeType_Select) || privSet.Database("mysql").Has(PrivilegeType_Select) {
		db, err := c.Database(ctx, "mysql")
		if err != nil {
			return nil, err
		}

		mysqlDb, ok := db.(*mysql_db.MySQLDb)
		if !ok {
			return nil, ErrDatabaseNotFound.New("mysql")
		}

		dbTbl, _, err := mysqlDb.GetTableInsensitive(ctx, "db")
		if err != nil {
			return nil, err
		}

		var keys []mysql_db.UserPrimaryKey
		err = iterRows(ctx, dbTbl, func(r Row) error {
			// mysql.db table will have 'Host', 'Db', 'User' as first 3 columns in string format.
			keys = append(keys, mysql_db.UserPrimaryKey{
				Host: r[0].(string),
				User: r[2].(string),
			})
			return nil
		})
		if err != nil {
			return nil, err
		}

		rd := mysqlDb.Reader()
		defer rd.Close()

		users := make(map[*mysql_db.User]struct{})
		for _, userKey := range keys {
			user := mysqlDb.GetUser(rd, userKey.User, userKey.Host, false)
			if user == nil {
				continue
			}
			users[user] = struct{}{}
		}
		for user := range users {
			grantee := user.UserHostToString("'")
			for _, privSetDb := range user.PrivilegeSet.GetDatabases() {
				rows = append(rows, getSchemaPrivsRowsFromPrivDbSet(privSetDb, grantee)...)
			}
		}
	} else {
		// If current client does not have SELECT privilege on 'mysql' db, only available schema privileges are
		// their current schema privileges.
		currClient := ctx.Session.Client()
		grantee := fmt.Sprintf("'%s'@'%s'", currClient.User, currClient.Address)
		dbs := c.AllDatabases(ctx)
		for _, db := range dbs {
			dbName := db.Name()
			privSetDb := privSet.Database(dbName)
			rows = append(rows, getSchemaPrivsRowsFromPrivDbSet(privSetDb, grantee)...)
		}
	}

	return RowsToRowIter(rows...), nil
}

// schemataRowIter implements the sql.RowIter for the information_schema.SCHEMATA table.
func schemataRowIter(ctx *Context, c Catalog) (RowIter, error) {
	dbs, err := allDatabases(ctx, c, false)
	if err != nil {
		return nil, err
	}

	var rows []Row

	for _, db := range dbs {
		collation := plan.GetDatabaseCollation(ctx, db.database)
		rows = append(rows, Row{
			db.catalogName,                    // catalog_name
			db.schemaName,                     // schema_name
			collation.CharacterSet().String(), // default_character_set_name
			collation.String(),                // default_collation_name
			nil,                               // sql_path
			"NO",                              // default_encryption
		})
	}

	if err != nil {
		return nil, err
	}

	return RowsToRowIter(rows...), nil
}

// schemataExtensionsRowIter implements the sql.RowIter for the information_schema.SCHEMATA_EXTENSIONS table.
func schemataExtensionsRowIter(ctx *Context, c Catalog) (RowIter, error) {
	var rows []Row

	databases, err := allDatabases(ctx, c, false)
	if err != nil {
		return nil, err
	}

	for _, db := range databases {
		var readOnly string
		if rodb, ok := db.database.(ReadOnlyDatabase); ok {
			if rodb.IsReadOnly() {
				readOnly = "READ ONLY=1"
			}
		}
		rows = append(rows, Row{
			db.catalogName, // catalog_name
			db.schemaName,  // schema_name
			readOnly,       // options
		})
	}

	return RowsToRowIter(rows...), nil
}

// stGeometryColumnsRowIter implements the sql.RowIter for the information_schema.ST_GEOMETRY_COLUMNS table.
func stGeometryColumnsRowIter(ctx *Context, cat Catalog) (RowIter, error) {
	var rows []Row

	databases, err := allDatabases(ctx, cat, false)
	if err != nil {
		return nil, err
	}

	for _, db := range databases {
		err := DBTableIter(ctx, db.database, func(t Table) (cont bool, err error) {
			tblName := t.Name()

			for _, col := range t.Schema() {
				s, ok := col.Type.(SpatialColumnType)
				if !ok {
					continue
				}
				var (
					colName = col.Name
					srsName interface{}
					srsId   interface{}
				)
				typeName, _ := getDtdIdAndDataType(col.Type)

				if srid, d := s.GetSpatialTypeSRID(); d {
					srsName = types.SupportedSRIDs[srid].Name
					srsId = srid
				}

				rows = append(rows, Row{
					db.catalogName, // table_catalog
					db.schemaName,  // table_schema
					tblName,        // table_name
					colName,        // column_name
					srsName,        // srs_name
					srsId,          // srs_id
					typeName,       // geometry_type_name
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

// stSpatialReferenceSystemsRowIter implements the sql.RowIter for the information_schema.ST_SPATIAL_REFERENCE_SYSTEMS table.
func stSpatialReferenceSystemsRowIter(ctx *Context, cat Catalog) (RowIter, error) {
	var rows []Row
	for _, spRef := range types.SupportedSRIDs {
		rows = append(rows, Row{
			spRef.Name,          // srs_name
			spRef.ID,            // srs_id
			spRef.Organization,  // organization
			spRef.OrgCoordsysId, // organization_coordsys_id
			spRef.Definition,    // definition
			spRef.Description,   // description
		})
	}

	return RowsToRowIter(rows...), nil
}

// stUnitsOfMeasureRowIter implements the sql.RowIter for the information_schema.ST_UNITS_OF_MEASURE table.
func stUnitsOfMeasureRowIter(ctx *Context, cat Catalog) (RowIter, error) {
	var rows []Row
	for _, spRef := range unitsOfMeasureArray {
		rows = append(rows, Row{
			spRef.Name,             // unit_name
			spRef.Type,             // unit_type
			spRef.ConversionFactor, // conversion_factor
			spRef.Description,      // description
		})
	}

	return RowsToRowIter(rows...), nil
}

// statisticsRowIter implements the sql.RowIter for the information_schema.STATISTICS table.
func statisticsRowIter(ctx *Context, c Catalog) (RowIter, error) {
	var rows []Row

	databases, err := allDatabases(ctx, c, false)
	if err != nil {
		return nil, err
	}

	for _, db := range databases {
		tableNames, tErr := db.database.GetTableNames(ctx)
		if tErr != nil {
			return nil, tErr
		}

		for _, tableName := range tableNames {
			tbl, _, err := c.DatabaseTable(ctx, db.database, tableName)
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
								db.catalogName, // table_catalog
								db.schemaName,  // table_schema
								tbl.Name(),     // table_name
								nonUnique,      // non_unique		NOT NULL
								db.schemaName,  // index_schema
								indexName,      // index_name
								seqInIndex,     // seq_in_index	NOT NULL
								colName,        // column_name
								collation,      // collation
								cardinality,    // cardinality
								subPart,        // sub_part
								nil,            // packed
								nullable,       // is_nullable	NOT NULL
								indexType,      // index_type		NOT NULL
								comment,        // comment		NOT NULL
								indexComment,   // index_comment	NOT NULL
								isVisible,      // is_visible		NOT NULL
								nil,            // expression
							})
						}
					}
				}
			}
		}
	}

	return RowsToRowIter(rows...), nil
}

// tableConstraintsRowIter implements the sql.RowIter for the information_schema.TABLE_CONSTRAINTS table.
func tableConstraintsRowIter(ctx *Context, c Catalog) (RowIter, error) {
	var rows []Row

	databases, err := allDatabases(ctx, c, false)
	if err != nil {
		return nil, err
	}

	for _, db := range databases {
		tableNames, err := db.database.GetTableNames(ctx)
		if err != nil {
			return nil, err
		}

		for _, tableName := range tableNames {
			tbl, _, err := c.DatabaseTable(ctx, db.database, tableName)
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
					rows = append(rows, Row{
						db.catalogName,       // constraint_catalog
						db.schemaName,        // constraint_schema
						checkDefinition.Name, // constraint_name
						db.schemaName,        // table_schema
						tbl.Name(),           // table_name
						"CHECK",              // constraint_type
						enforced,             // enforced
					})
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

					rows = append(rows, Row{
						db.catalogName, // constraint_catalog
						db.schemaName,  // constraint_schema
						index.ID(),     // constraint_name
						db.schemaName,  // table_schema
						tbl.Name(),     // table_name
						outputType,     // constraint_type
						"YES",          // enforced
					})
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
					rows = append(rows, Row{
						db.catalogName, // constraint_catalog
						db.schemaName,  // constraint_schema
						fk.Name,        // constraint_name
						db.schemaName,  // table_schema
						tbl.Name(),     // table_name
						"FOREIGN KEY",  // constraint_type
						"YES",          // enforced
					})
				}
			}
		}
	}

	return RowsToRowIter(rows...), nil
}

// tableConstraintsExtensionsRowIter implements the sql.RowIter for the information_schema.TABLE_CONSTRAINTS_EXTENSIONS table.
func tableConstraintsExtensionsRowIter(ctx *Context, c Catalog) (RowIter, error) {
	var rows []Row

	databases, err := allDatabases(ctx, c, false)
	if err != nil {
		return nil, err
	}

	for _, db := range databases {
		tableNames, err := db.database.GetTableNames(ctx)
		if err != nil {
			return nil, err
		}

		for _, tableName := range tableNames {
			tbl, _, err := c.DatabaseTable(ctx, db.database, tableName)
			if err != nil {
				return nil, err
			}
			tblName := tbl.Name()

			// Get UNIQUEs, PRIMARY KEYs
			// TODO: Doesn't correctly consider primary keys from table implementations that don't implement sql.IndexedTable
			indexTable, ok := tbl.(IndexAddressable)
			if ok {
				indexes, err := indexTable.GetIndexes(ctx)
				if err != nil {
					return nil, err
				}

				for _, index := range indexes {
					rows = append(rows, Row{
						db.catalogName, // constraint_catalog
						db.schemaName,  // constraint_schema
						index.ID(),     // constraint_name
						tblName,        // table_name
						nil,            // engine_attribute
						nil,            // secondary_engine_attribute
					})
				}
			}
		}
	}

	return RowsToRowIter(rows...), nil
}

type partitionIterable interface {
	Partitions(*Context) (PartitionIter, error)
	PartitionRows(*Context, Partition) (RowIter, error)
}

func iterRows(ctx *Context, pii partitionIterable, cb func(Row) error) (rerr error) {
	pi, err := pii.Partitions(ctx)
	if err != nil {
		return err
	}
	defer func() {
		err := pi.Close(ctx)
		if rerr == nil {
			rerr = err
		}
	}()
	for {
		p, err := pi.Next(ctx)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		ri, err := pii.PartitionRows(ctx, p)
		if err != nil {
			return err
		}
		for {
			r, err := ri.Next(ctx)
			if err == io.EOF {
				ri.Close(ctx)
				break
			}
			if err != nil {
				ri.Close(ctx)
				return err
			}
			err = cb(r)
			if err != nil {
				ri.Close(ctx)
				return err
			}
		}
	}
}

// tablePrivilegesRowIter implements the sql.RowIter for the information_schema.TABLE_PRIVILEGES table.
func tablePrivilegesRowIter(ctx *Context, c Catalog) (RowIter, error) {
	var rows []Row
	privSet, _ := ctx.GetPrivilegeSet()
	if privSet == nil {
		privSet = mysql_db.NewPrivilegeSet()
	}
	if privSet.Has(PrivilegeType_Select) || privSet.Database("mysql").Has(PrivilegeType_Select) {
		db, err := c.Database(ctx, "mysql")
		if err != nil {
			return nil, err
		}

		mysqlDb, ok := db.(*mysql_db.MySQLDb)
		if !ok {
			return nil, ErrDatabaseNotFound.New("mysql")
		}

		tblsPriv, _, err := mysqlDb.GetTableInsensitive(ctx, "tables_priv")
		if err != nil {
			return nil, err
		}

		var keys []mysql_db.UserPrimaryKey
		err = iterRows(ctx, tblsPriv, func(r Row) error {
			keys = append(keys, mysql_db.UserPrimaryKey{
				Host: r[0].(string),
				User: r[2].(string),
			})
			return nil
		})
		if err != nil {
			return nil, err
		}

		rd := mysqlDb.Reader()
		defer rd.Close()

		users := make(map[*mysql_db.User]struct{})
		for _, userKey := range keys {
			user := mysqlDb.GetUser(rd, userKey.User, userKey.Host, false)
			if user == nil {
				continue
			}
			users[user] = struct{}{}
		}
		for user := range users {
			grantee := user.UserHostToString("'")
			for _, privSetDb := range user.PrivilegeSet.GetDatabases() {
				dbName := privSetDb.Name()
				for _, privSetTbl := range privSetDb.GetTables() {
					rows = append(rows, getTablePrivsRowsFromPrivTblSet(privSetTbl, grantee, dbName)...)
				}
			}
		}
	} else {
		// If current client does not have SELECT privilege on 'mysql' db, only available table privileges are
		// their current table privileges.
		currClient := ctx.Session.Client()
		grantee := fmt.Sprintf("'%s'@'%s'", currClient.User, currClient.Address)
		dbs := c.AllDatabases(ctx)
		for _, db := range dbs {
			dbName := db.Name()
			privSetDb := privSet.Database(dbName)
			for _, privSetTbl := range privSetDb.GetTables() {
				rows = append(rows, getTablePrivsRowsFromPrivTblSet(privSetTbl, grantee, dbName)...)
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

	databases, err := allDatabases(ctx, cat, true)
	if err != nil {
		return nil, err
	}

	for _, db := range databases {
		if db.database.Name() == InformationSchemaDatabaseName {
			tableType = "SYSTEM VIEW"
		} else {
			tableType = "BASE TABLE"
			engine = "InnoDB"
			rowFormat = "Dynamic"
		}

		y2k, _, _ := types.Timestamp.Convert("2000-01-01 00:00:00")
		err := DBTableIter(ctx, db.database, func(t Table) (cont bool, err error) {
			tableCollation = t.Collation().String()
			comment := ""
			if db.database.Name() != InformationSchemaDatabaseName {
				if st, ok := t.(StatisticsTable); ok {
					tableRows, _, err = st.RowCount(ctx)
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

				if commentedTable, ok := t.(CommentedTable); ok {
					comment = commentedTable.Comment()
				}
			}

			rows = append(rows, Row{
				db.catalogName, // table_catalog
				db.schemaName,  // table_schema
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
				comment,        // table_comment
			})

			return true, nil
		})

		if err != nil {
			return nil, err
		}

		views, err := viewsInDatabase(ctx, db.database)
		if err != nil {
			return nil, err
		}

		for _, view := range views {
			rows = append(rows, Row{
				db.catalogName, // table_catalog
				db.schemaName,  // table_schema
				view.Name,      // table_name
				"VIEW",         // table_type
				nil,            // engine
				nil,            // version (protocol, always 10)
				nil,            // row_format
				nil,            // table_rows
				nil,            // avg_row_length
				nil,            // data_length
				nil,            // max_data_length
				nil,            // max_data_length
				nil,            // data_free
				nil,            // auto_increment
				y2k,            // create_time
				nil,            // update_time
				nil,            // check_time
				nil,            // table_collation
				nil,            // checksum
				nil,            // create_options
				"VIEW",         // table_comment
			})
		}
	}

	return RowsToRowIter(rows...), nil
}

// dbWithNames includes the Database with the catalog and schema names.
type dbWithNames struct {
	database    Database
	catalogName string
	schemaName  string
}

// allDatabases expands all databases in the catalog to include all schemas if
// present. For Postgres, it will only return the schemas for the current database.
func allDatabases(ctx *Context, cat Catalog, privCheck bool) ([]dbWithNames, error) {
	var dbs []dbWithNames

	currentDB := ctx.GetCurrentDatabase()

	for _, db := range cat.AllDatabases(ctx) {
		if privCheck {
			if privDatabase, ok := db.(mysql_db.PrivilegedDatabase); ok {
				db = privDatabase.Unwrap()
			}
		}

		sdb, ok := db.(SchemaDatabase)
		if ok {
			var schemaDbs []dbWithNames
			schemas, err := sdb.AllSchemas(ctx)
			if err != nil {
				return nil, err
			}

			for _, schema := range schemas {
				if schema.SchemaName() != "" && db.Name() == currentDB {
					schemaDbs = append(schemaDbs, dbWithNames{schema, schema.Name(), schema.SchemaName()})
				} else {
					dbs = append(dbs, dbWithNames{schema, "def", schema.Name()})
				}
			}

			if len(schemaDbs) > 0 {
				// TODO: information_schema should be included in the schema list
				infoSchemaDB, err := cat.Database(ctx, InformationSchemaDatabaseName)
				if err != nil {
					return nil, err
				}
				schemaDbs = append(schemaDbs, dbWithNames{infoSchemaDB, sdb.Name(), InformationSchemaDatabaseName})

				return schemaDbs, nil
			}
		} else {
			dbs = append(dbs, dbWithNames{db, "def", db.Name()})
		}
	}
	return dbs, nil
}

// tablesExtensionsRowIter implements the sql.RowIter for the information_schema.TABLES_EXTENSIONS table.
func tablesExtensionsRowIter(ctx *Context, cat Catalog) (RowIter, error) {
	var rows []Row

	databases, err := allDatabases(ctx, cat, false)
	if err != nil {
		return nil, err
	}

	for _, db := range databases {
		err := DBTableIter(ctx, db.database, func(t Table) (cont bool, err error) {
			rows = append(rows, Row{
				db.catalogName, // table_catalog
				db.schemaName,  // table_schema
				t.Name(),       // table_name
				nil,            // engine_attribute // TODO: reserved for future use
				nil,            // secondary_engine_attribute // TODO: reserved for future use
			})
			return true, nil
		})
		if err != nil {
			return nil, err
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

	privSet, _ := ctx.GetPrivilegeSet()
	if privSet == nil {
		return RowsToRowIter(rows...), nil
	}
	hasGlobalTriggerPriv := privSet.Has(PrivilegeType_Trigger)

	databases, err := allDatabases(ctx, c, false)
	if err != nil {
		return nil, err
	}

	for _, db := range databases {
		dbCollation := plan.GetDatabaseCollation(ctx, db.database)
		triggerDb, ok := db.database.(TriggerDatabase)
		if ok {
			privDbSet := privSet.Database(db.database.Name())
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
				triggerSqlMode := NewSqlModeFromString(trigger.SqlMode)
				parsedTrigger, err := planbuilder.ParseWithOptions(ctx, c, trigger.CreateStatement, triggerSqlMode.ParserOptions())
				if err != nil {
					return nil, err
				}
				triggerPlan, ok := parsedTrigger.(*plan.CreateTrigger)
				if !ok {
					return nil, ErrTriggerCreateStatementInvalid.New(trigger.CreateStatement)
				}
				triggerPlan.CreatedAt = trigger.CreatedAt // Keep stored created time
				triggerPlan.SqlMode = triggerSqlMode.String()
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
					tableName := triggerPlan.Table.(*plan.ResolvedTable).Name()
					definer := removeBackticks(triggerPlan.Definer)

					// triggers cannot be created on table that is not in current schema, so the trigger_name = event_object_schema
					privTblSet := privDbSet.Table(tableName)

					// To see information about a table's triggers, you must have the TRIGGER privilege for the table.
					if hasGlobalTriggerPriv || hasDbTriggerPriv || privTblSet.Has(PrivilegeType_Trigger) {
						rows = append(rows, Row{
							db.catalogName,          // trigger_catalog
							db.schemaName,           // trigger_schema
							triggerPlan.TriggerName, // trigger_name
							triggerEvent,            // event_manipulation
							db.catalogName,          // event_object_catalog
							db.schemaName,           // event_object_schema
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
							triggerPlan.SqlMode,     // sql_mode
							definer,                 // definer
							characterSetClient,      // character_set_client
							collationConnection,     // collation_connection
							dbCollation.String(),    // database_collation
						})
					}
				}
			}
		}
	}
	return RowsToRowIter(rows...), nil
}

// userAttributesRowIter implements the sql.RowIter for the information_schema.USER_ATTRIBUTES table.
func userAttributesRowIter(ctx *Context, catalog Catalog) (RowIter, error) {
	var rows []Row
	curUserPrivSet, _ := ctx.GetPrivilegeSet()
	if curUserPrivSet == nil {
		curUserPrivSet = mysql_db.NewPrivilegeSet()
	}
	// TODO: or has both of `CREATE USER` and `SYSTEM_USER` privileges
	if curUserPrivSet.Has(PrivilegeType_Select) || curUserPrivSet.Has(PrivilegeType_Update) || curUserPrivSet.Database("mysql").Has(PrivilegeType_Select) || curUserPrivSet.Database("mysql").Has(PrivilegeType_Update) {
		var users = make(map[*mysql_db.User]struct{})
		db, err := catalog.Database(ctx, "mysql")
		if err != nil {
			return nil, err
		}

		mysqlDb, ok := db.(*mysql_db.MySQLDb)
		if !ok {
			return nil, ErrDatabaseNotFound.New("mysql")
		}

		reader := mysqlDb.Reader()
		defer reader.Close()

		reader.VisitUsers(func(u *mysql_db.User) {
			users[u] = struct{}{}
		})

		for user := range users {
			var attributes interface{}
			if user.Attributes != nil {
				attributes = *user.Attributes
			}
			rows = append(rows, Row{
				user.User,  // user
				user.Host,  // host
				attributes, // attributes
			})
		}
	} else {
		// TODO: current user needs to be exposed to access user attribute from mysql_db
		currClient := ctx.Session.Client()
		rows = append(rows, Row{
			currClient.User,    // user
			currClient.Address, // host
			nil,                // attributes
		})
	}

	return RowsToRowIter(rows...), nil
}

// userPrivilegesRowIter implements the sql.RowIter for the information_schema.USER_PRIVILEGES table.
func userPrivilegesRowIter(ctx *Context, catalog Catalog) (RowIter, error) {
	var rows []Row
	curUserPrivSet, _ := ctx.GetPrivilegeSet()
	if curUserPrivSet == nil {
		curUserPrivSet = mysql_db.NewPrivilegeSet()
	}
	if curUserPrivSet.Has(PrivilegeType_Select) || curUserPrivSet.Database("mysql").Has(PrivilegeType_Select) {
		var users = make(map[*mysql_db.User]struct{})
		db, err := catalog.Database(ctx, "mysql")
		if err != nil {
			return nil, err
		}

		mysqlDb, ok := db.(*mysql_db.MySQLDb)
		if !ok {
			return nil, ErrDatabaseNotFound.New("mysql")
		}

		reader := mysqlDb.Reader()
		defer reader.Close()

		reader.VisitUsers(func(u *mysql_db.User) {
			users[u] = struct{}{}
		})

		for user := range users {
			grantee := user.UserHostToString("'")
			rows = append(rows, getGlobalPrivsRowsFromPrivSet(user.PrivilegeSet, grantee)...)
		}
	} else {
		// If current client does not have SELECT privilege on 'mysql' db, only available schema privileges are
		// their current schema privileges.
		currClient := ctx.Session.Client()
		grantee := fmt.Sprintf("'%s'@'%s'", currClient.User, currClient.Address)
		rows = getGlobalPrivsRowsFromPrivSet(curUserPrivSet, grantee)
	}

	return RowsToRowIter(rows...), nil
}

// viewsRowIter implements the sql.RowIter for the information_schema.VIEWS table.
func viewsRowIter(ctx *Context, catalog Catalog) (RowIter, error) {
	var rows []Row
	privSet, _ := ctx.GetPrivilegeSet()
	if privSet == nil {
		return RowsToRowIter(rows...), nil
	}
	hasGlobalShowViewPriv := privSet.Has(PrivilegeType_ShowView)

	databases, err := allDatabases(ctx, catalog, false)
	if err != nil {
		return nil, err
	}

	for _, db := range databases {
		privDbSet := privSet.Database(db.database.Name())
		hasDbShowViewPriv := privDbSet.Has(PrivilegeType_ShowView)

		views, err := viewsInDatabase(ctx, db.database)
		if err != nil {
			return nil, err
		}

		dbCollation := plan.GetDatabaseCollation(ctx, db.database)
		charset := dbCollation.CharacterSet().String()
		collation := dbCollation.String()

		for _, view := range views {
			privTblSet := privDbSet.Table(view.Name)
			if !hasGlobalShowViewPriv && !hasDbShowViewPriv && !privTblSet.Has(PrivilegeType_ShowView) {
				continue
			}
			parsedView, err := planbuilder.ParseWithOptions(ctx, catalog, view.CreateViewStatement, NewSqlModeFromString(view.SqlMode).ParserOptions())
			if err != nil {
				continue
			}
			viewPlan, ok := parsedView.(*plan.CreateView)
			if !ok {
				return nil, ErrTriggerCreateStatementInvalid.New(view.CreateViewStatement)
			}

			viewDef := viewPlan.Definition.TextDefinition
			definer := removeBackticks(viewPlan.Definer)

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
				db.catalogName, // table_catalog
				db.schemaName,  // table_schema
				view.Name,      // table_name
				viewDef,        // view_definition
				checkOpt,       // check_option
				isUpdatable,    // is_updatable
				definer,        // definer
				securityType,   // security_type
				charset,        // character_set_client
				collation,      // collation_connection
			})
		}
	}

	return RowsToRowIter(rows...), nil
}

// emptyRowIter implements the sql.RowIter for empty table.
func emptyRowIter(ctx *Context, c Catalog) (RowIter, error) {
	return RowsToRowIter(), nil
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
				reader: columnsExtensionsRowIter,
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
				reader: eventsRowIter,
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
				reader: keywordsRowIter,
			},
			OptimizerTraceTableName: &informationSchemaTable{
				name:   OptimizerTraceTableName,
				schema: optimizerTraceSchema,
				reader: emptyRowIter,
			},
			ParametersTableName: &routineTable{
				name:    ParametersTableName,
				schema:  parametersSchema,
				rowIter: parametersRowIter,
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
				schema: schemaPrivilegesSchema,
				reader: schemaPrivilegesRowIter,
			},
			SchemataTableName: &informationSchemaTable{
				name:   SchemataTableName,
				schema: schemataSchema,
				reader: schemataRowIter,
			},
			SchemataExtensionsTableName: &informationSchemaTable{
				name:   SchemataExtensionsTableName,
				schema: schemataExtensionsSchema,
				reader: schemataExtensionsRowIter,
			},
			StGeometryColumnsTableName: &informationSchemaTable{
				name:   StGeometryColumnsTableName,
				schema: stGeometryColumnsSchema,
				reader: stGeometryColumnsRowIter,
			},
			StSpatialReferenceSystemsTableName: &informationSchemaTable{
				name:   StSpatialReferenceSystemsTableName,
				schema: stSpatialReferenceSystemsSchema,
				reader: stSpatialReferenceSystemsRowIter,
			},
			StUnitsOfMeasureTableName: &informationSchemaTable{
				name:   StUnitsOfMeasureTableName,
				schema: stUnitsOfMeasureSchema,
				reader: stUnitsOfMeasureRowIter,
			},
			TableConstraintsTableName: &informationSchemaTable{
				name:   TableConstraintsTableName,
				schema: tableConstraintsSchema,
				reader: tableConstraintsRowIter,
			},
			TableConstraintsExtensionsTableName: &informationSchemaTable{
				name:   TableConstraintsExtensionsTableName,
				schema: tableConstraintsExtensionsSchema,
				reader: tableConstraintsExtensionsRowIter,
			},
			TablePrivilegesTableName: &informationSchemaTable{
				name:   TablePrivilegesTableName,
				schema: tablePrivilegesSchema,
				reader: tablePrivilegesRowIter,
			},
			TablesTableName: &informationSchemaTable{
				name:   TablesTableName,
				schema: tablesSchema,
				reader: tablesRowIter,
			},
			TablesExtensionsTableName: &informationSchemaTable{
				name:   TablesExtensionsTableName,
				schema: tablesExtensionsSchema,
				reader: tablesExtensionsRowIter,
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
				reader: userAttributesRowIter,
			},
			UserPrivilegesTableName: &informationSchemaTable{
				name:   UserPrivilegesTableName,
				schema: userPrivilegesSchema,
				reader: userPrivilegesRowIter,
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

// Database implements the sql.Databaseable interface.
func (c *informationSchemaTable) Database() string {
	return InformationSchemaDatabaseName
}

// Schema implements the sql.Table interface.
func (t *informationSchemaTable) Schema() Schema {
	return t.schema
}

func (t *informationSchemaTable) DataLength(_ *Context) (uint64, error) {
	return uint64(len(t.Schema()) * int(types.Text.MaxByteLength()) * defaultInfoSchemaRowCount), nil
}

func (t *informationSchemaTable) RowCount(ctx *Context) (uint64, bool, error) {
	return defaultInfoSchemaRowCount, false, nil
}

// Collation implements the sql.Table interface.
func (t *informationSchemaTable) Collation() CollationID {
	return Collation_Information_Schema_Default
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
	}
}

// defaultStatsTable is a statistics table implementation
// with a cache to save ANALYZE results. RowCount defers to
// the underlying table in the absence of a cached statistic.
type defaultStatsTable struct {
	*informationSchemaTable
}

func (n *defaultStatsTable) AssignCatalog(cat Catalog) Table {
	n.catalog = cat
	return n
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

	if vdb, ok := db.(ViewDatabase); ok {
		dbViews, err := vdb.AllViews(ctx)
		if err != nil {
			return nil, err
		}

		views = append(views, dbViews...)
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

func removeBackticks(s string) string {
	return strings.Replace(s, "`", "", -1)
}

// getGlobalPrivsRowsFromPrivSet returns USER_PRIVILEGES rows using given global privilege set and grantee name string.
func getGlobalPrivsRowsFromPrivSet(privSet PrivilegeSet, grantee string) []Row {
	var rows []Row
	hasGrantOpt := privSet.Has(PrivilegeType_GrantOption)
	for _, priv := range privSet.ToSlice() {
		if priv == PrivilegeType_GrantOption {
			continue
		}
		isGrantable := "NO"
		if hasGrantOpt {
			isGrantable = "YES"
		}
		rows = append(rows, Row{
			grantee,       // grantee
			"def",         // table_catalog
			priv.String(), // privilege_type
			isGrantable,   // is_grantable
		})
	}
	return rows
}

// getSchemaPrivsRowsFromPrivDbSet returns SCHEMA_PRIVILEGES rows using given Database privilege set and grantee string.
func getSchemaPrivsRowsFromPrivDbSet(privSetDb PrivilegeSetDatabase, grantee string) []Row {
	var rows []Row
	hasGrantOpt := privSetDb.Has(PrivilegeType_GrantOption)
	for _, privType := range privSetDb.ToSlice() {
		if privType == PrivilegeType_GrantOption {
			continue
		}
		isGrantable := "NO"
		if hasGrantOpt {
			isGrantable = "YES"
		}
		rows = append(rows, Row{
			grantee,           // grantee
			"def",             // table_catalog
			privSetDb.Name(),  // table_schema
			privType.String(), // privilege_type
			isGrantable,       // is_grantable
		})
	}
	return rows
}

// getTablePrivsRowsFromPrivTblSet returns TABLE_PRIVILEGES rows using given Table privilege set and grantee and database name strings.
func getTablePrivsRowsFromPrivTblSet(privSetTbl PrivilegeSetTable, grantee, dbName string) []Row {
	var rows []Row
	hasGrantOpt := privSetTbl.Has(PrivilegeType_GrantOption)
	for _, privType := range privSetTbl.ToSlice() {
		if privType == PrivilegeType_GrantOption {
			continue
		}
		isGrantable := "NO"
		if hasGrantOpt {
			isGrantable = "YES"
		}
		rows = append(rows, Row{
			grantee,           // grantee
			"def",             // table_catalog
			dbName,            // table_schema
			privSetTbl.Name(), // table_name
			privType.String(), // privilege_type
			isGrantable,       // is_grantable
		})
	}
	return rows
}
