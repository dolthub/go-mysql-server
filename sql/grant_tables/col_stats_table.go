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

package grant_tables

import (
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/in_mem_table"

	"github.com/dolthub/vitess/go/sqltypes"
)

const colStatsTblName = "column_statistics"

var colStatsTblSchema sql.Schema

// TODO: should just make generic errors with fmt strings
var errColStatsPkEntry = fmt.Errorf("the primary key for the `colStats` table was given an unknown entry")
var errColStatsPkRow = fmt.Errorf("the primary key for the `colStats` table was given a row belonging to an unknown schema")
var errColStatsSkRow = fmt.Errorf("the secondary key for the `colStats` table was given a row belonging to an unknown schema")
var errColStatsSkEntry = fmt.Errorf("the secondary key for the `colStats` table was given an unknown entry")

// ColStatsPrimaryKey is a key that represents the primary key for the "column_statistics" table
type ColStatsPrimaryKey struct {
	SchemaName string
	TableName  string
	ColumnName string
}

// ColStatsSecondaryKey is a key that represents the secondary key for the "user" Grant Table, which contains stats data.
type ColStatsSecondaryKey struct {
	//TODO: eventually condense into histogram with json(?) type
	Count  uint64
	Mean   float64
	Median float64
}

var _ in_mem_table.Key = ColStatsPrimaryKey{}
var _ in_mem_table.Key = ColStatsSecondaryKey{}

// KeyFromEntry implements the interface in_mem_table.Key.
func (c ColStatsPrimaryKey) KeyFromEntry(ctx *sql.Context, entry in_mem_table.Entry) (in_mem_table.Key, error) {
	col, ok := entry.(*ColStats)
	if !ok {
		return nil, errColStatsPkEntry
	}
	return ColStatsPrimaryKey{
		SchemaName: col.SchemaName,
		TableName:  col.TableName,
		ColumnName: col.ColumnName,
	}, nil
}

// KeyFromRow implements the interface in_mem_table.Key.
func (c ColStatsPrimaryKey) KeyFromRow(ctx *sql.Context, row sql.Row) (in_mem_table.Key, error) {
	if len(row) != len(colStatsTblSchema) {
		return c, errColStatsPkEntry
	}
	schema, ok := row[colStatsTblColIndex_Schema].(string)
	if !ok {
		return c, errColStatsPkRow
	}
	table, ok := row[colStatsTblColIndex_Table].(string)
	if !ok {
		return c, errColStatsPkRow
	}
	col, ok := row[colStatsTblColIndex_Column].(string)
	if !ok {
		return c, errColStatsPkRow
	}

	return ColStatsPrimaryKey{
		SchemaName: schema,
		TableName:  table,
		ColumnName: col,
	}, nil
}

// KeyFromEntry implements the interface in_mem_table.Key.
func (u ColStatsSecondaryKey) KeyFromEntry(ctx *sql.Context, entry in_mem_table.Entry) (in_mem_table.Key, error) {
	colStats, ok := entry.(*ColStats)
	if !ok {
		return nil, errColStatsSkEntry
	}
	return ColStatsSecondaryKey{
		Count:  colStats.Count,
		Mean:   colStats.Mean,
		Median: colStats.Median,
	}, nil
}

// KeyFromRow implements the interface in_mem_table.Key.
func (u ColStatsSecondaryKey) KeyFromRow(ctx *sql.Context, row sql.Row) (in_mem_table.Key, error) {
	if len(row) != len(colStatsTblSchema) {
		return u, errColStatsSkRow
	}
	count, ok := row[colStatsTblColIndex_Count].(uint64)
	if !ok {
		return u, errColStatsSkRow
	}
	mean, ok := row[colStatsTblColIndex_Mean].(float64)
	if !ok {
		return u, errColStatsSkRow
	}
	median, ok := row[colStatsTblColIndex_Median].(float64)
	if !ok {
		return u, errColStatsSkRow
	}

	return ColStatsSecondaryKey{
		Count:  count,
		Mean:   mean,
		Median: median,
	}, nil
}

// init creates the schema for the "user" Grant Table.
func init() {
	// Types
	char64_utf8_bin := sql.MustCreateString(sqltypes.Char, 64, sql.Collation_utf8_bin)

	// Column Templates
	char64_utf8_bin_not_null_default_empty := &sql.Column{
		Type:     char64_utf8_bin,
		Default:  mustDefault(expression.NewLiteral("", char64_utf8_bin), char64_utf8_bin, true, false),
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

	colStatsTblSchema = sql.Schema{
		columnTemplate("Schema", colStatsTblName, true, char64_utf8_bin_not_null_default_empty),
		columnTemplate("Table", colStatsTblName, true, char64_utf8_bin_not_null_default_empty),
		columnTemplate("Column", colStatsTblName, true, char64_utf8_bin_not_null_default_empty),
		columnTemplate("Count", colStatsTblName, true, uint64_default_nil),
		columnTemplate("Mean", colStatsTblName, true, float64_default_nil),
		columnTemplate("Median", colStatsTblName, true, float64_default_nil),
		//columnTemplate("Histogram", colStatsTblName, true, json_nullable_default_nil),
	}
}

// These represent the column indexes of the "user" Grant Table.
const (
	colStatsTblColIndex_Schema int = iota
	colStatsTblColIndex_Table
	colStatsTblColIndex_Column
	colStatsTblColIndex_Count
	colStatsTblColIndex_Mean
	colStatsTblColIndex_Median
	// colStatsTblColIndex_Histogram
)
