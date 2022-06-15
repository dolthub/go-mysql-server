// Copyright 2022 Dolthub, Inc.
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
	"encoding/json"
	"fmt"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
)

// ColumnStatisticsTable describes the information_schema.columns table. It implements both sql.Node and sql.Table
// as way to handle resolving column defaults.
type ColumnStatisticsTable struct {
	// allColsWithDefaultValue is a list of all columns with a non nil default value. The default values should be
	// completely resolved.
	allColsWithDefaultValue []*sql.Column

	Catalog sql.Catalog
	name    string
}

var _ sql.Node = (*ColumnStatisticsTable)(nil)
var _ sql.Nameable = (*ColumnStatisticsTable)(nil)
var _ sql.Table = (*ColumnStatisticsTable)(nil)

// Resolved implements the sql.Node interface.
func (c *ColumnStatisticsTable) Resolved() bool {
	for _, col := range c.allColsWithDefaultValue {
		if !col.Default.Resolved() {
			return false
		}
	}

	return true
}

// String implements the sql.Node interface.
func (c *ColumnStatisticsTable) String() string {
	return fmt.Sprintf("ColumnsTable(%s)", c.name)
}

// Schema implements the sql.Node interface.
func (c *ColumnStatisticsTable) Schema() sql.Schema {
	return columnStatisticsSchema
}

// RowIter implements the sql.Node interface.
func (c *ColumnStatisticsTable) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	partitions, err := c.Partitions(ctx)
	if err != nil {
		return nil, err
	}

	return sql.NewTableRowIter(ctx, c, partitions), nil
}

// WithChildren implements the sql.Node interface.
func (c *ColumnStatisticsTable) WithChildren(children ...sql.Node) (sql.Node, error) {
	return c, nil
}

// Children implements the sql.Node interface.
func (c *ColumnStatisticsTable) Children() []sql.Node {
	return nil
}

// CheckPrivileges implements the sql.Node interface.
func (c *ColumnStatisticsTable) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	// Copied from the resolved table implementation
	return opChecker.UserHasPrivileges(ctx, sql.NewPrivilegedOperation("information_schema", c.name, "", sql.PrivilegeType_Select))
}

// Name implements the sql.Nameable interface.
func (c *ColumnStatisticsTable) Name() string {
	return c.name
}

// Partitions implements the sql.Table interface.
func (c *ColumnStatisticsTable) Partitions(context *sql.Context) (sql.PartitionIter, error) {
	return &informationSchemaPartitionIter{informationSchemaPartition: informationSchemaPartition{partitionKey(c.Name())}}, nil
}

// PartitionRows implements the sql.Table interface.
func (c *ColumnStatisticsTable) PartitionRows(context *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	if !bytes.Equal(partition.Key(), partitionKey(c.Name())) {
		return nil, sql.ErrPartitionNotFound.New(partition.Key())
	}

	if c.Catalog == nil {
		return nil, fmt.Errorf("nil catalog for info schema table %s", c.Name())
	}

	colToDefaults := make(map[string]*sql.ColumnDefaultValue)
	for _, col := range c.allColsWithDefaultValue {
		colToDefaults[col.Name] = col.Default
	}

	return columnsRowIter(context, c.Catalog, colToDefaults)
}

// WithAllColumns passes in a set of all columns.
func (c *ColumnStatisticsTable) WithAllColumns(cols []*sql.Column) sql.Node {
	nc := *c
	nc.allColsWithDefaultValue = cols
	return &nc
}

// columnStatisticsRowIter implements the custom sql.RowIter for the information_schema.columns table.
func columnStatisticsRowIter(ctx *sql.Context, cat sql.Catalog) (sql.RowIter, error) {
	var rows []sql.Row
	for _, db := range cat.AllDatabases(ctx) {
		// Get all Tables
		err := sql.DBTableIter(ctx, db, func(t sql.Table) (cont bool, err error) {
			// skip non-stats tables
			statsTbl, ok := t.(sql.StatisticsTable)
			if !ok {
				return true, nil
			}

			// TODO: nothing i do is cached???
			err = statsTbl.CalculateStatistics(ctx)
			if err != nil {
				return false, err
			}
			// Skip unanalyzed tables
			if !statsTbl.IsAnalyzed() {
				return true, nil
			}

			// Get statistics
			stats, err := statsTbl.GetStatistics(ctx)
			if err != nil {
				return false, err
			}

			for _, col := range t.Schema() {
				hist, err := stats.Histogram(col.Name)
				if err != nil {
					return false, err
				}

				jsonHist, err := json.Marshal(hist)
				if err != nil {
					return false, err
				}

				rows = append(rows, sql.Row{
					db.Name(),       // table_schema
					statsTbl.Name(), // table_name
					col.Name,        // column_name
					jsonHist,        // histogram
				})
			}
			return true, nil
		})

		if err != nil {
			return nil, err
		}
	}
	return sql.RowsToRowIter(rows...), nil
}

// trimColumnStatisticsDefaultOutput takes in a column default value and 1. Removes Double Quotes for literals 2. Ensures that the
// string NULL becomes nil.
func trimColumnStatisticsDefaultOutput(cd *sql.ColumnDefaultValue) interface{} {
	if cd == nil {
		return nil
	}

	colStr := cd.String()
	// TODO: We need to fix the ColumnDefault String() to prevent double quoting.
	if strings.HasPrefix(colStr, "\"") && strings.HasSuffix(colStr, "\"") {
		return strings.TrimSuffix(strings.TrimPrefix(colStr, "\""), "\"")
	}

	if strings.HasPrefix(colStr, "(") && strings.HasSuffix(colStr, ")") {
		return strings.TrimSuffix(strings.TrimPrefix(colStr, "("), ")")
	}

	if colStr == "NULL" {
		return nil
	}

	return colStr
}
