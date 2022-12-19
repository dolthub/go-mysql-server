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
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/dolthub/vitess/go/vt/proto/query"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/transform"
)

var typeToNumericPrecision = map[query.Type]int{
	sqltypes.Int8:    3,
	sqltypes.Uint8:   3,
	sqltypes.Int16:   5,
	sqltypes.Uint16:  5,
	sqltypes.Int24:   7,
	sqltypes.Uint24:  7,
	sqltypes.Int32:   10,
	sqltypes.Uint32:  10,
	sqltypes.Int64:   19,
	sqltypes.Uint64:  20,
	sqltypes.Float32: 12,
	sqltypes.Float64: 22,
}

// ColumnsTable describes the information_schema.columns table. It implements both sql.Node and sql.Table
// as way to handle resolving column defaults.
type ColumnsTable struct {
	// allColsWithDefaultValue is the full schema of all tables in all databases. We need this during analysis in order
	// to resolve the default values of some columns, so we pre-compute it.
	allColsWithDefaultValue sql.Schema

	catalog sql.Catalog
}

var _ sql.Table = (*ColumnsTable)(nil)

// String implements the sql.Table interface.
func (c *ColumnsTable) String() string {
	return fmt.Sprintf(ColumnsTableName)
}

// Schema implements the sql.Table interface.
func (c *ColumnsTable) Schema() sql.Schema {
	return columnsSchema
}

// Collation implements the sql.Table interface.
func (c *ColumnsTable) Collation() sql.CollationID {
	return sql.Collation_Default
}

// Name implements the sql.Table interface.
func (c *ColumnsTable) Name() string {
	return ColumnsTableName
}

func (c *ColumnsTable) AssignCatalog(cat sql.Catalog) sql.Table {
	c.catalog = cat
	return c
}

// WithAllColumns passes in a set of all columns.
func (c *ColumnsTable) WithAllColumns(cols []*sql.Column) sql.Table {
	nc := *c
	nc.allColsWithDefaultValue = cols
	return &nc
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

	if c.catalog == nil {
		return nil, fmt.Errorf("nil catalog for info schema table %s", c.Name())
	}

	return c.columnsRowIter(context)
}

// AllColumns returns all columns in the catalog, renamed to reflect their database and table names
func (c *ColumnsTable) AllColumns(ctx *sql.Context) (sql.Schema, error) {
	if len(c.allColsWithDefaultValue) > 0 {
		return c.allColsWithDefaultValue, nil
	}

	if c.catalog == nil {
		return nil, fmt.Errorf("nil catalog for info schema table %s", c.Name())
	}

	var allColumns sql.Schema

	for _, db := range c.catalog.AllDatabases(ctx) {
		err := sql.DBTableIter(ctx, db, func(t sql.Table) (cont bool, err error) {
			tableSch := t.Schema()
			for i := range tableSch {
				newCol := tableSch[i].Copy()
				newCol.DatabaseSource = db.Name()
				allColumns = append(allColumns, newCol)
			}
			return true, nil
		})

		if err != nil {
			return nil, err
		}
	}

	c.allColsWithDefaultValue = allColumns
	return c.allColsWithDefaultValue, nil
}

func (c ColumnsTable) WithColumnDefaults(columnDefaults []sql.Expression) (sql.Table, error) {
	if c.allColsWithDefaultValue == nil {
		return nil, fmt.Errorf("WithColumnDefaults called with nil columns for table %s", c.Name())
	}

	if len(columnDefaults) != len(c.allColsWithDefaultValue) {
		return nil, sql.ErrInvalidChildrenNumber.New(c, len(columnDefaults), len(c.allColsWithDefaultValue))
	}

	c.allColsWithDefaultValue = transform.SchemaWithDefaults(c.allColsWithDefaultValue, columnDefaults)
	return &c, nil
}

// columnsRowIter implements the custom sql.RowIter for the information_schema.columns table.
func (c *ColumnsTable) columnsRowIter(ctx *sql.Context) (sql.RowIter, error) {
	var rows []sql.Row
	for _, db := range c.catalog.AllDatabases(ctx) {
		// Get all Tables
		err := sql.DBTableIter(ctx, db, func(t sql.Table) (cont bool, err error) {
			var columnKeyMap = make(map[string]string)
			// Get UNIQUEs, PRIMARY KEYs
			hasPK := false
			if indexTable, ok := t.(sql.IndexAddressable); ok {
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
			tableName := t.Name()
			for i, col := range c.schemaForTable(t, db) {
				var (
					charName          interface{}
					collName          interface{}
					charMaxLen        interface{}
					charOctetLen      interface{}
					columnKey         string
					nullable          = "NO"
					ordinalPos        = uint32(i + 1)
					colType           = strings.Split(col.Type.String(), " COLLATE")[0]
					dataType          = colType
					datetimePrecision interface{}
					srsId             interface{}
				)

				if col.Nullable {
					nullable = "YES"
				}

				if sql.IsText(col.Type) {
					if sql.IsTextOnly(col.Type) {
						charName = sql.Collation_Default.CharacterSet().String()
						collName = sql.Collation_Default.String()
					}

					if st, ok := col.Type.(sql.StringType); ok {
						charMaxLen = st.MaxCharacterLength()
						charOctetLen = st.MaxByteLength()
					}
				} else if sql.IsEnum(col.Type) || sql.IsSet(col.Type) {
					charName = sql.Collation_Default.CharacterSet().String()
					collName = sql.Collation_Default.String()
					charOctetLen = int64(col.Type.MaxTextResponseByteLength())
					charMaxLen = int64(col.Type.MaxTextResponseByteLength()) / sql.Collation_Default.CharacterSet().MaxLength()
				}

				// The DATA_TYPE value is the type name only with no other information
				dataType = strings.Split(dataType, "(")[0]
				dataType = strings.Split(dataType, " ")[0]

				// Check column PK here first because there are PKs from table implementations that don't implement sql.IndexedTable
				if col.PrimaryKey {
					columnKey = "PRI"
				} else if val, ok := columnKeyMap[col.Name]; ok {
					columnKey = val
					// A UNIQUE index may be displayed as PRI if it cannot contain NULL values and there is no PRIMARY KEY in the table
					if !col.Nullable && !hasPK && columnKey == "UNI" {
						columnKey = "PRI"
						hasPK = true
					}
				}

				if s, ok := col.Type.(sql.SpatialColumnType); ok {
					if srid, d := s.GetSpatialTypeSRID(); d {
						srsId = srid
					}
				}

				numericPrecision, numericScale := getColumnPrecisionAndScale(col)
				if sql.IsDatetimeType(col.Type) || sql.IsTimestampType(col.Type) {
					datetimePrecision = 0
				} else if sql.IsTimespan(col.Type) {
					// TODO: TIME length not yet supported
					datetimePrecision = 6
				}

				columnDefault := getColumnDefault(ctx, col.Default)

				extra := col.Extra
				// If extra is not defined, fill it here.
				if extra == "" && !col.Default.IsLiteral() {
					extra = fmt.Sprintf("DEFAULT_GENERATED")
				}

				rows = append(rows, sql.Row{
					"def",             // table_catalog
					db.Name(),         // table_schema
					tableName,         // table_name
					col.Name,          // column_name
					ordinalPos,        // ordinal_position
					columnDefault,     // column_default
					nullable,          // is_nullable
					dataType,          // data_type
					charMaxLen,        // character_maximum_length
					charOctetLen,      // character_octet_length
					numericPrecision,  // numeric_precision
					numericScale,      // numeric_scale
					datetimePrecision, // datetime_precision
					charName,          // character_set_name
					collName,          // collation_name
					colType,           // column_type
					columnKey,         // column_key
					extra,             // extra
					"select",          // privileges
					col.Comment,       // column_comment
					"",                // generation_expression
					srsId,             // srs_id
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
				uint32(0), // ordinal_position
				nil,       // column_default
				"",        // is_nullable
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
				nil,       // srs_id
			})
		}
		if err != nil {
			return nil, err
		}
	}
	return sql.RowsToRowIter(rows...), nil
}

// getColumnDefault returns the column default value for given sql.ColumnDefaultValue
func getColumnDefault(ctx *sql.Context, cd *sql.ColumnDefaultValue) interface{} {
	if cd == nil {
		return nil
	}

	defStr := cd.String()
	if defStr == "NULL" {
		return nil
	}

	if !cd.IsLiteral() {
		if strings.HasPrefix(defStr, "(") && strings.HasSuffix(defStr, ")") {
			defStr = strings.TrimSuffix(strings.TrimPrefix(defStr, "("), ")")
		}
		if sql.IsTime(cd.Type()) && (strings.HasPrefix(defStr, "NOW") || strings.HasPrefix(defStr, "CURRENT_TIMESTAMP")) {
			defStr = strings.Replace(defStr, "NOW", "CURRENT_TIMESTAMP", -1)
			defStr = strings.TrimSuffix(defStr, "()")
		}
		return fmt.Sprint(defStr)
	}

	if sql.IsEnum(cd.Type()) || sql.IsSet(cd.Type()) {
		return strings.Trim(defStr, "'")
	}

	v, err := cd.Eval(ctx, nil)
	if err != nil {
		return ""
	}

	switch l := v.(type) {
	case time.Time:
		v = l.Format("2006-01-02 15:04:05")
	case []uint8:
		hexStr := hex.EncodeToString(l)
		v = fmt.Sprintf("0x%s", hexStr)
	}

	if sql.IsBit(cd.Type()) {
		if i, ok := v.(uint64); ok {
			bitStr := strconv.FormatUint(i, 2)
			v = fmt.Sprintf("b'%s'", bitStr)
		}
	}

	return fmt.Sprint(v)
}

func (c *ColumnsTable) schemaForTable(t sql.Table, db sql.Database) sql.Schema {
	start, end := -1, -1
	tableName := strings.ToLower(t.Name())

	for i, col := range c.allColsWithDefaultValue {
		dbName := strings.ToLower(db.Name())
		if start < 0 && strings.ToLower(col.Source) == tableName && strings.ToLower(col.DatabaseSource) == dbName {
			start = i
		} else if start >= 0 && (strings.ToLower(col.Source) != tableName || strings.ToLower(col.DatabaseSource) != dbName) {
			end = i
			break
		}
	}

	if start < 0 {
		return nil
	}

	if end < 0 {
		end = len(c.allColsWithDefaultValue)
	}

	return c.allColsWithDefaultValue[start:end]
}

// getColumnPrecisionAndScale returns the precision or a number of mysql type. For non-numeric or decimal types this
// function should return nil,nil.
func getColumnPrecisionAndScale(col *sql.Column) (interface{}, interface{}) {
	var numericScale interface{}
	switch t := col.Type.(type) {
	case sql.BitType:
		return int(t.NumberOfBits()), numericScale
	case sql.DecimalType:
		return int(t.Precision()), int(t.Scale())
	case sql.NumberType:
		switch col.Type.Type() {
		case sqltypes.Float32, sqltypes.Float64:
			numericScale = nil
		default:
			numericScale = 0
		}
		return typeToNumericPrecision[col.Type.Type()], numericScale
	default:
		return nil, nil
	}
}
