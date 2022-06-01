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
	"fmt"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
)

// ColumnsTable describes the information_schema.columns table. It implements both sql.Node and sql.Table
// as way to handle resolving column defaults.
type ColumnsTable struct {
	// allColsWithDefaultValue is a list of all columns with a non nil default value. The default values should be
	// completely resolved.
	allColsWithDefaultValue []*sql.Column

	Catalog sql.Catalog
	name    string
}

var _ sql.Node = (*ColumnsTable)(nil)
var _ sql.Nameable = (*ColumnsTable)(nil)
var _ sql.Table = (*ColumnsTable)(nil)

// Resolved implements the sql.Node interface.
func (c *ColumnsTable) Resolved() bool {
	for _, col := range c.allColsWithDefaultValue {
		if !col.Default.Resolved() {
			return false
		}
	}

	return true
}

// String implements the sql.Node interface.
func (c *ColumnsTable) String() string {
	return fmt.Sprintf("ColumnsTable(%s)", c.name)
}

// Schema implements the sql.Node interface.
func (c *ColumnsTable) Schema() sql.Schema {
	return columnsSchema
}

// RowIter implements the sql.Node interface.
func (c *ColumnsTable) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	partitions, err := c.Partitions(ctx)
	if err != nil {
		return nil, err
	}

	return sql.NewTableRowIter(ctx, c, partitions), nil
}

// WithChildren implements the sql.Node interface.
func (c *ColumnsTable) WithChildren(children ...sql.Node) (sql.Node, error) {
	return c, nil
}

// Children implements the sql.Node interface.
func (c *ColumnsTable) Children() []sql.Node {
	return nil
}

// CheckPrivileges implements the sql.Node interface.
func (c *ColumnsTable) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	// Copied from the resolved table implementation
	return opChecker.UserHasPrivileges(ctx, sql.NewPrivilegedOperation("information_schema", c.name, "", sql.PrivilegeType_Select))
}

// Name implements the sql.Nameable interface.
func (c *ColumnsTable) Name() string {
	return c.name
}

// Partitions implements the sql.Table interface.
func (c *ColumnsTable) Partitions(context *sql.Context) (sql.PartitionIter, error) {
	return &informationSchemaPartitionIter{informationSchemaPartition: informationSchemaPartition{partitionKey(c.Name())}}, nil
}

// PartitionRows implements the sql.Table interface.
func (c *ColumnsTable) PartitionRows(context *sql.Context, partition sql.Partition) (sql.RowIter, error) {
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
func (c *ColumnsTable) WithAllColumns(cols []*sql.Column) sql.Node {
	nc := *c
	nc.allColsWithDefaultValue = cols
	return &nc
}

// columnsRowIter implements the custom sql.RowIter for the information_schema.columns table.
func columnsRowIter(ctx *sql.Context, cat sql.Catalog, columnNameToDefault map[string]*sql.ColumnDefaultValue) (sql.RowIter, error) {
	var rows []sql.Row
	for _, db := range cat.AllDatabases(ctx) {
		// Get all Tables
		err := sql.DBTableIter(ctx, db, func(t sql.Table) (cont bool, err error) {
			var columnKeyMap = make(map[string]string)
			// Get UNIQUEs, PRIMARY KEYs
			hasPK := false
			if indexTable, ok := t.(sql.IndexedTable); ok {
				indexes, iErr := indexTable.GetIndexes(ctx)
				if iErr != nil {
					return false, iErr
				}

				for _, index := range indexes {
					idx := ""
					if index.ID() == "PRIMARY" {
						idx = "PRI"
						hasPK = true
					} else if index.IsUnique() {
						idx = "UNI"
					} else {
						idx = "MUL"
					}

					colNames := getColumnNamesFromIndex(index, t)
					// A UNIQUE index may display as MUL if several columns form a composite UNIQUE index
					if idx == "UNI" && len(colNames) > 1 {
						idx = "MUL"
						columnKeyMap[colNames[0]] = idx
					} else {
						for _, colName := range colNames {
							columnKeyMap[colName] = idx
						}
					}
				}
			}

			for i, c := range t.Schema() {
				var (
					charName   interface{}
					collName   interface{}
					colDefault interface{}
					charMaxLen interface{}
					columnKey  string
					nullable   = "NO"
					ordinalPos = uint64(i + 1)
					colType    = strings.ToLower(c.Type.String())
					dataType   = colType
					srsId      = "NULL"
				)

				if c.Nullable {
					nullable = "YES"
				}

				if sql.IsText(c.Type) {
					charName = sql.Collation_Default.CharacterSet().String()
					collName = sql.Collation_Default.String()
					if st, ok := c.Type.(sql.StringType); ok {
						charMaxLen = st.MaxCharacterLength()
					}
					dataType = strings.TrimSuffix(dataType, fmt.Sprintf("(%v)", charMaxLen))
				}

				if c.Type == sql.Boolean {
					colType = colType + "(1)"
				}

				fullColumnName := db.Name() + "." + t.Name() + "." + c.Name
				colDefault = trimColumnDefaultOutput(columnNameToDefault[fullColumnName])

				// Check column PK here first because there are PKs from table implementations that don't implement sql.IndexedTable
				if c.PrimaryKey {
					columnKey = "PRI"
				} else if val, ok := columnKeyMap[c.Name]; ok {
					columnKey = val
					// A UNIQUE index may be displayed as PRI if it cannot contain NULL values and there is no PRIMARY KEY in the table
					if !c.Nullable && !hasPK && columnKey == "UNI" {
						columnKey = "PRI"
						hasPK = true
					}
				}

				if s, ok := c.Type.(sql.SpatialColumnType); ok {
					if srid, d := s.GetSpatialTypeSRID(); d {
						srsId = fmt.Sprintf("%v", srid)
					}
				}

				rows = append(rows, sql.Row{
					"def",      // table_catalog
					db.Name(),  // table_schema
					t.Name(),   // table_name
					c.Name,     // column_name
					ordinalPos, // ordinal_position
					colDefault, // column_default
					nullable,   // is_nullable
					dataType,   // data_type
					charMaxLen, // character_maximum_length
					nil,        // character_octet_length
					nil,        // numeric_precision
					nil,        // numeric_scale
					nil,        // datetime_precision
					charName,   // character_set_name
					collName,   // collation_name
					colType,    // column_type
					columnKey,  // column_key
					c.Extra,    // extra
					"select",   // privileges
					c.Comment,  // column_comment
					"",         // generation_expression
					srsId,      // srs_id
				})
			}
			return true, nil
		})

		// TODO: View Definition is lacking information to properly fill out these table
		// TODO: Should somehow get reference to table(s) view is referencing
		// TODO: Each column that view references should also show up as unique entries as well
		views, err := viewsInDatabase(ctx, db)
		if err != nil {
			return nil, err
		}

		for _, view := range views {
			rows = append(rows, sql.Row{
				"def",     // table_catalog
				db.Name(), // table_schema
				view.Name, // table_name
				"",        // column_name
				uint64(0), // ordinal_position
				nil,       // column_default
				nil,       // is_nullable
				nil,       // data_type
				nil,       // character_maximum_length
				nil,       // character_octet_length
				nil,       // numeric_precision
				nil,       // numeric_scale
				nil,       // datetime_precision
				"",        // character_set_name
				"",        // collation_name
				"",        // column_type
				"",        // column_key
				"",        // extra
				"select",  // privileges
				"",        // column_comment
				"",        // generation_expression
				"NULL",    // srs_id
			})
		}
		if err != nil {
			return nil, err
		}
	}
	return sql.RowsToRowIter(rows...), nil
}

// trimColumnDefaultOutput takes in a column default value and 1. Removes Double Quotes for literals 2. Ensures that the
// string NULL becomes nil.
func trimColumnDefaultOutput(cd *sql.ColumnDefaultValue) interface{} {
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
