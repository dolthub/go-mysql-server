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
	"encoding/json"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/in_mem_table"
)

// User represents a user from the user Grant Tables.
type ColStats struct {
	SchemaName string
	TableName  string
	ColumnName string
	Count      uint64
	Mean       float64
	Min        float64
	Max        float64
}

var _ in_mem_table.Entry = (*ColStats)(nil)

// NewFromRow implements the interface in_mem_table.Entry.
func (c *ColStats) NewFromRow(ctx *sql.Context, row sql.Row) (in_mem_table.Entry, error) {
	if err := colStatsTblSchema.CheckRow(row); err != nil {
		return nil, err
	}
	return &ColStats{
		SchemaName: row[colStatsTblColIndex_Schema].(string),
		TableName:  row[colStatsTblColIndex_Table].(string),
		ColumnName: row[colStatsTblColIndex_Column].(string),
		Count:      row[colStatsTblColIndex_Count].(uint64),
		Mean:       row[colStatsTblColIndex_Mean].(float64),
		Min:        row[colStatsTblColIndex_Min].(float64),
		Max:        row[colStatsTblColIndex_Max].(float64),
	}, nil
}

// UpdateFromRow implements the interface in_mem_table.Entry.
func (c *ColStats) UpdateFromRow(ctx *sql.Context, row sql.Row) (in_mem_table.Entry, error) {
	updatedEntry, err := c.NewFromRow(ctx, row)
	if err != nil {
		return nil, err
	}
	return updatedEntry, nil
}

// ToRow implements the interface in_mem_table.Entry.
func (c *ColStats) ToRow(ctx *sql.Context) sql.Row {
	row := make(sql.Row, len(colStatsTblSchema))
	var err error
	for i, col := range colStatsTblSchema {
		row[i], err = col.Default.Eval(ctx, nil)
		if err != nil {
			panic(err) // Should never happen, schema is static
		}
	}

	row[colStatsTblColIndex_Schema] = c.SchemaName
	row[colStatsTblColIndex_Table] = c.TableName
	row[colStatsTblColIndex_Column] = c.ColumnName
	row[colStatsTblColIndex_Count] = c.Count
	row[colStatsTblColIndex_Mean] = c.Mean
	row[colStatsTblColIndex_Min] = c.Min
	row[colStatsTblColIndex_Max] = c.Max
	return row
}

// Equals implements the interface in_mem_table.Entry.
func (c *ColStats) Equals(ctx *sql.Context, otherEntry in_mem_table.Entry) bool {
	other, ok := otherEntry.(*ColStats)
	if !ok {
		return false
	}
	return c.SchemaName == other.SchemaName &&
		c.TableName == other.TableName &&
		c.ColumnName == other.ColumnName &&
		c.Count == other.Count &&
		c.Mean == other.Mean &&
		c.Min == other.Min &&
		c.Max == other.Max
}

// Copy implements the interface in_mem_table.Entry.
func (c *ColStats) Copy(ctx *sql.Context) in_mem_table.Entry {
	// TODO: is this a shallow copy?
	uu := *c
	return &uu
}

// FromJson implements the interface in_mem_table.Entry.
func (c ColStats) FromJson(ctx *sql.Context, jsonStr string) (in_mem_table.Entry, error) {
	newColStats := &ColStats{}
	if err := json.Unmarshal([]byte(jsonStr), newColStats); err != nil {
		return nil, err
	}
	return newColStats, nil
}

// ToJson implements the interface in_mem_table.Entry.
func (c *ColStats) ToJson(ctx *sql.Context) (string, error) {
	jsonData, err := json.Marshal(*c)
	if err != nil {
		return "", err
	}
	return string(jsonData), nil
}
