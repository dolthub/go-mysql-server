// Copyright 2021-2022 Dolthub, Inc.
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

package mysql_db

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/in_mem_table"

	"github.com/dolthub/vitess/go/sqltypes"
)

const columnStatisticsTblName = "column_statistics"

var columnStatisticsTblSchema sql.Schema

// ColumnStatisticsPrimaryKey is a key that represents the primary key for the "column_statistics" table
type ColumnStatisticsPrimaryKey struct {
	SchemaName string
	TableName  string
	ColumnName string
}

// ColumnStatisticsSecondaryKey is a key that represents the secondary key for the "user" Grant Tables, which contains stats data.
type ColumnStatisticsSecondaryKey struct {
	Count     uint64
	NullCount uint64
	Mean      float64
	Min       float64
	Max       float64
	Histogram string
}

var _ in_mem_table.Key = ColumnStatisticsPrimaryKey{}
var _ in_mem_table.Key = ColumnStatisticsSecondaryKey{}

// KeyFromEntry implements the interface in_mem_table.Key.
func (c ColumnStatisticsPrimaryKey) KeyFromEntry(ctx *sql.Context, entry in_mem_table.Entry) (in_mem_table.Key, error) {
	col, ok := entry.(*ColumnStatistics)
	if !ok {
		return nil, sql.ErrInvalidTableEntry.New("primary", "column_statistics")
	}
	return ColumnStatisticsPrimaryKey{
		SchemaName: col.SchemaName,
		TableName:  col.TableName,
		ColumnName: col.ColumnName,
	}, nil
}

// KeyFromRow implements the interface in_mem_table.Key.
func (c ColumnStatisticsPrimaryKey) KeyFromRow(ctx *sql.Context, row sql.Row) (in_mem_table.Key, error) {
	if len(row) != len(columnStatisticsTblSchema) {
		return c, sql.ErrInvalidTableEntry.New("primary", "column_statistics")
	}
	schema, ok := row[columnStatisticsTblColIndex_Schema].(string)
	if !ok {
		return c, sql.ErrInvalidSchema.New("primary", "column_statistics")
	}
	table, ok := row[columnStatisticsTblColIndex_Table].(string)
	if !ok {
		return c, sql.ErrInvalidSchema.New("primary", "column_statistics")
	}
	col, ok := row[columnStatisticsTblColIndex_Column].(string)
	if !ok {
		return c, sql.ErrInvalidSchema.New("primary", "column_statistics")
	}

	return ColumnStatisticsPrimaryKey{
		SchemaName: schema,
		TableName:  table,
		ColumnName: col,
	}, nil
}

// KeyFromEntry implements the interface in_mem_table.Key.
func (u ColumnStatisticsSecondaryKey) KeyFromEntry(ctx *sql.Context, entry in_mem_table.Entry) (in_mem_table.Key, error) {
	colStats, ok := entry.(*ColumnStatistics)
	if !ok {
		return nil, sql.ErrInvalidTableEntry.New("secondary", "column_statistics")
	}
	return ColumnStatisticsSecondaryKey{
		Count:     colStats.Count,
		NullCount: colStats.NullCount,
		Mean:      colStats.Mean,
		Min:       colStats.Min,
		Max:       colStats.Max,
		Histogram: colStats.Histogram,
	}, nil
}

// KeyFromRow implements the interface in_mem_table.Key.
func (u ColumnStatisticsSecondaryKey) KeyFromRow(ctx *sql.Context, row sql.Row) (in_mem_table.Key, error) {
	if len(row) != len(columnStatisticsTblSchema) {
		return u, sql.ErrInvalidTableEntry.New("secondary", "column_statistics")
	}
	count, ok := row[columnStatisticsTblColIndex_Count].(uint64)
	if !ok {
		return u, sql.ErrInvalidSchema.New("secondary", "column_statistics")
	}
	nullCount, ok := row[columnStatisticsTblColIndex_NullCount].(uint64)
	if !ok {
		return u, sql.ErrInvalidSchema.New("secondary", "column_statistics")
	}
	mean, ok := row[columnStatisticsTblColIndex_Mean].(float64)
	if !ok {
		return u, sql.ErrInvalidSchema.New("secondary", "column_statistics")
	}
	min, ok := row[columnStatisticsTblColIndex_Min].(float64)
	if !ok {
		return u, sql.ErrInvalidSchema.New("secondary", "column_statistics")
	}
	max, ok := row[columnStatisticsTblColIndex_Max].(float64)
	if !ok {
		return u, sql.ErrInvalidSchema.New("secondary", "column_statistics")
	}
	histogram, ok := row[columnStatisticsTblColIndex_Histogram].(string)
	if !ok {
		return u, sql.ErrInvalidSchema.New("secondary", "column_statistics")
	}

	return ColumnStatisticsSecondaryKey{
		Count:     count,
		NullCount: nullCount,
		Mean:      mean,
		Min:       min,
		Max:       max,
		Histogram: histogram,
	}, nil
}

// init creates the schema for the "user" Grant Tables.
func init() {
	// Types
	char64_utf8_bin := sql.MustCreateString(sqltypes.Char, 64, sql.Collation_utf8_bin)
	char255_utf8_bin := sql.MustCreateString(sqltypes.Char, 255, sql.Collation_utf8_bin)

	// Column Templates
	char64_utf8_bin_not_null_default_empty := &sql.Column{
		Type:     char64_utf8_bin,
		Default:  mustDefault(expression.NewLiteral("", char64_utf8_bin), char64_utf8_bin, true, false),
		Nullable: false,
	}
	char255_utf8_bin_not_null_default_empty := &sql.Column{
		Type:     char255_utf8_bin,
		Default:  mustDefault(expression.NewLiteral("", char255_utf8_bin), char255_utf8_bin, true, false),
		Nullable: false,
	}
	uint64_default_nil := &sql.Column{
		Type:     sql.Uint64,
		Default:  nil,
		Nullable: true,
	}
	float64_default_nil := &sql.Column{
		Type:     sql.Float64,
		Default:  nil,
		Nullable: true,
	}

	// TODO: column template for histogram
	//json_nullable_default_nil := &sql.Column{
	//	Type:     sql.JSON,
	//	Default:  nil,
	//	Nullable: true,
	//}

	columnStatisticsTblSchema = sql.Schema{
		columnTemplate("Schema", columnStatisticsTblName, true, char64_utf8_bin_not_null_default_empty),
		columnTemplate("Tables", columnStatisticsTblName, true, char64_utf8_bin_not_null_default_empty),
		columnTemplate("Column", columnStatisticsTblName, true, char64_utf8_bin_not_null_default_empty),
		columnTemplate("Count", columnStatisticsTblName, true, uint64_default_nil),
		columnTemplate("NullCount", columnStatisticsTblName, true, uint64_default_nil),
		columnTemplate("Mean", columnStatisticsTblName, true, float64_default_nil),
		columnTemplate("Min", columnStatisticsTblName, true, float64_default_nil),
		columnTemplate("Max", columnStatisticsTblName, true, float64_default_nil),
		columnTemplate("Histogram", columnStatisticsTblName, true, char255_utf8_bin_not_null_default_empty),
	}
}

// These represent the column indexes of the "user" Grant Tables.
const (
	columnStatisticsTblColIndex_Schema int = iota
	columnStatisticsTblColIndex_Table
	columnStatisticsTblColIndex_Column
	columnStatisticsTblColIndex_Count
	columnStatisticsTblColIndex_NullCount
	columnStatisticsTblColIndex_Mean
	columnStatisticsTblColIndex_Min
	columnStatisticsTblColIndex_Max
	columnStatisticsTblColIndex_Histogram
)
