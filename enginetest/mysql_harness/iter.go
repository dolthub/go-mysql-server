// Copyright 2021 Dolthub, Inc.
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

package mysql_harness

import (
	dsql "database/sql"
	"io"
	"reflect"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// mysqlIter wraps an iterator returned by the MySQL connection.
type mysqlIter struct {
	rows        *dsql.Rows
	types       []reflect.Type
	dbTypeNames []string
}

var _ sql.RowIter = mysqlIter{}

// newMySQLIter returns a new mysqlIter.
func newMySQLIter(rows *dsql.Rows) mysqlIter {
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		panic(err)
	}
	types := make([]reflect.Type, len(columnTypes))
	dbTypeNames := make([]string, len(columnTypes))
	for i, columnType := range columnTypes {
		dbTypeNames[i] = columnType.DatabaseTypeName()
		scanType := columnType.ScanType()
		// Normalize nullable sql types to their underlying Go types for the Schema.
		// The actual scanning uses interface{} pointers, so this is only for schema reporting.
		switch scanType {
		case reflect.TypeOf(dsql.RawBytes{}):
			scanType = reflect.TypeOf("")
		case reflect.TypeOf(dsql.NullBool{}):
			scanType = reflect.TypeOf(true)
		case reflect.TypeOf(dsql.NullByte{}):
			scanType = reflect.TypeOf(byte(0))
		case reflect.TypeOf(dsql.NullFloat64{}):
			scanType = reflect.TypeOf(float64(0))
		case reflect.TypeOf(dsql.NullInt16{}):
			scanType = reflect.TypeOf(int16(0))
		case reflect.TypeOf(dsql.NullInt32{}):
			scanType = reflect.TypeOf(int32(0))
		case reflect.TypeOf(dsql.NullInt64{}):
			scanType = reflect.TypeOf(int64(0))
		case reflect.TypeOf(dsql.NullString{}):
			scanType = reflect.TypeOf("")
		case reflect.TypeOf(dsql.NullTime{}):
			scanType = reflect.TypeOf(time.Time{})
		case reflect.TypeOf(""):
			// already correct
		case reflect.TypeOf(float32(0)):
			scanType = reflect.TypeOf(float64(0))
		case reflect.TypeOf(float64(0)):
			// already correct
		case reflect.TypeOf(int32(0)):
			scanType = reflect.TypeOf(int64(0))
		case reflect.TypeOf(int64(0)):
			// already correct
		case reflect.TypeOf(uint32(0)):
			scanType = reflect.TypeOf(int64(0))
		case reflect.TypeOf(uint64(0)):
			scanType = reflect.TypeOf(int64(0))
		default:
			// For any unrecognized type, treat as string
			scanType = reflect.TypeOf("")
		}
		types[i] = scanType
	}
	return mysqlIter{rows, types, dbTypeNames}
}

func (m mysqlIter) Schema() sql.Schema {
	schema := make(sql.Schema, len(m.types))
	for i, typ := range m.types {
		var col sql.Column
		switch typ {
		case reflect.TypeOf(""):
			col = sql.Column{
				Type: types.Text,
			}
		case reflect.TypeOf(true):
			col = sql.Column{
				Type: types.Boolean,
			}
		case reflect.TypeOf(byte(0)):
			col = sql.Column{
				Type: types.Uint8,
			}
		case reflect.TypeOf(float64(0)):
			col = sql.Column{
				Type: types.Float64,
			}
		case reflect.TypeOf(int16(0)):
			col = sql.Column{
				Type: types.Int16,
			}
		case reflect.TypeOf(int32(0)):
			col = sql.Column{
				Type: types.Int32,
			}
		case reflect.TypeOf(int64(0)):
			col = sql.Column{
				Type: types.Int64,
			}
		case reflect.TypeOf(time.Time{}):
			col = sql.Column{
				Type: types.Time,
			}
		default:
			panic("unsupported scan type: " + typ.String())
		}
		schema[i] = &col
	}
	return schema
}

// Next implements the interface sql.RowIter.
func (m mysqlIter) Next(ctx *sql.Context) (sql.Row, error) {
	if m.rows.Next() {
		// Use []interface{} with pointers to handle NULLs properly
		scanDest := make([]interface{}, len(m.types))
		for i := range m.types {
			scanDest[i] = new(interface{})
		}
		err := m.rows.Scan(scanDest...)
		if err != nil {
			return nil, err
		}
		output := make(sql.Row, len(m.types))
		for i, dest := range scanDest {
			val := *(dest.(*interface{}))
			if val == nil {
				output[i] = nil
				continue
			}
			output[i] = normalizeValue(val, m.types[i], m.dbTypeNames[i])
		}
		return output, nil
	}
	return nil, io.EOF
}

// normalizeValue converts a value from the MySQL driver into the Go type that GMS tests expect.
func normalizeValue(val interface{}, expectedType reflect.Type, dbTypeName string) interface{} {
	// MySQL returns BIT(1) and TINYINT(1) as integer types for boolean results.
	// Convert 0/1 integers to false/true for boolean columns.
	if isBooleanColumn(dbTypeName) {
		return toBool(val)
	}

	switch v := val.(type) {
	case []byte:
		// Convert []byte to string or numeric type based on the expected type
		switch expectedType {
		case reflect.TypeOf(float64(0)):
			var f dsql.NullFloat64
			if err := f.Scan(val); err == nil && f.Valid {
				return f.Float64
			}
			return string(v)
		case reflect.TypeOf(int64(0)):
			var n dsql.NullInt64
			if err := n.Scan(val); err == nil && n.Valid {
				return n.Int64
			}
			return string(v)
		default:
			return string(v)
		}
	case int:
		return int64(v)
	case int64:
		return v
	case int32:
		return int64(v)
	case uint64:
		return int64(v)
	case uint32:
		return int64(v)
	case float32:
		return float64(v)
	case float64:
		return v
	default:
		return val
	}
}

// isBooleanColumn returns true if the MySQL column type represents a boolean.
func isBooleanColumn(dbTypeName string) bool {
	switch dbTypeName {
	case "BIT", "TINYINT":
		return true
	}
	return false
}

// toBool converts an integer-like value to a Go bool.
func toBool(val interface{}) interface{} {
	switch v := val.(type) {
	case int64:
		return v != 0
	case int32:
		return v != 0
	case uint64:
		return v != 0
	case uint8:
		return v != 0
	case []byte:
		// BIT(1) columns come back as []byte{0} or []byte{1}
		if len(v) == 1 {
			return v[0] != 0
		}
		return false
	default:
		return val
	}
}

// Close implements the interface sql.RowIter.
func (m mysqlIter) Close(ctx *sql.Context) error {
	return m.rows.Close()
}
