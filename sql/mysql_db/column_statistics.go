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
	"encoding/json"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/in_mem_table"
)

// User represents a user from the user Grant Tables.
type ColumnStatistics struct {
	SchemaName string
	TableName  string
	ColumnName string
	Count      uint64
	NullCount  uint64
	Mean       float64
	Min        float64
	Max        float64
	Histogram  string
}

var _ in_mem_table.Entry = (*ColumnStatistics)(nil)

// NewFromRow implements the interface in_mem_table.Entry.
func (c *ColumnStatistics) NewFromRow(ctx *sql.Context, row sql.Row) (in_mem_table.Entry, error) {
	if err := columnStatisticsTblSchema.CheckRow(row); err != nil {
		return nil, err
	}
	histogram, err := json.Marshal(row[columnStatisticsTblColIndex_Histogram])
	if err != nil {
		return nil, err
	}
	return &ColumnStatistics{
		SchemaName: row[columnStatisticsTblColIndex_Schema].(string),
		TableName:  row[columnStatisticsTblColIndex_Table].(string),
		ColumnName: row[columnStatisticsTblColIndex_Column].(string),
		Count:      row[columnStatisticsTblColIndex_Count].(uint64),
		NullCount:  row[columnStatisticsTblColIndex_NullCount].(uint64),
		Mean:       row[columnStatisticsTblColIndex_Mean].(float64),
		Min:        row[columnStatisticsTblColIndex_Min].(float64),
		Max:        row[columnStatisticsTblColIndex_Max].(float64),
		Histogram:  string(histogram),
	}, nil
}

// UpdateFromRow implements the interface in_mem_table.Entry.
func (c *ColumnStatistics) UpdateFromRow(ctx *sql.Context, row sql.Row) (in_mem_table.Entry, error) {
	updatedEntry, err := c.NewFromRow(ctx, row)
	if err != nil {
		return nil, err
	}
	return updatedEntry, nil
}

// ToRow implements the interface in_mem_table.Entry.
func (c *ColumnStatistics) ToRow(ctx *sql.Context) sql.Row {
	row := make(sql.Row, len(columnStatisticsTblSchema))
	var err error
	for i, col := range columnStatisticsTblSchema {
		row[i], err = col.Default.Eval(ctx, nil)
		if err != nil {
			panic(err) // Should never happen, schema is static
		}
	}

	row[columnStatisticsTblColIndex_Schema] = c.SchemaName
	row[columnStatisticsTblColIndex_Table] = c.TableName
	row[columnStatisticsTblColIndex_Column] = c.ColumnName
	row[columnStatisticsTblColIndex_Count] = c.Count
	row[columnStatisticsTblColIndex_NullCount] = c.NullCount
	row[columnStatisticsTblColIndex_Mean] = c.Mean
	row[columnStatisticsTblColIndex_Min] = c.Min
	row[columnStatisticsTblColIndex_Max] = c.Max
	row[columnStatisticsTblColIndex_Histogram] = c.Max
	return row
}

// Equals implements the interface in_mem_table.Entry.
func (c *ColumnStatistics) Equals(ctx *sql.Context, otherEntry in_mem_table.Entry) bool {
	other, ok := otherEntry.(*ColumnStatistics)
	if !ok {
		return false
	}
	return c.SchemaName == other.SchemaName &&
		c.TableName == other.TableName &&
		c.ColumnName == other.ColumnName &&
		c.Count == other.Count &&
		c.NullCount == other.NullCount &&
		c.Mean == other.Mean &&
		c.Min == other.Min &&
		c.Max == other.Max
}

// Copy implements the interface in_mem_table.Entry.
func (c *ColumnStatistics) Copy(ctx *sql.Context) in_mem_table.Entry {
	uu := *c
	return &uu
}

// FromJson implements the interface in_mem_table.Entry.
func (c ColumnStatistics) FromJson(ctx *sql.Context, jsonStr string) (in_mem_table.Entry, error) {
	newColStats := &ColumnStatistics{}
	if err := json.Unmarshal([]byte(jsonStr), newColStats); err != nil {
		return nil, err
	}
	return newColStats, nil
}

// ToJson implements the interface in_mem_table.Entry.
func (c *ColumnStatistics) ToJson(ctx *sql.Context) (string, error) {
	jsonData, err := json.Marshal(*c)
	if err != nil {
		return "", err
	}
	return string(jsonData), nil
}
