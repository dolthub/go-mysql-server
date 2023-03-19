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

package mysqlshim

import (
	dsql "database/sql"
	"io"
	"reflect"
	"time"

	"github.com/gabereiser/go-mysql-server/sql"
)

// mysqlIter wraps an iterator returned by the MySQL connection.
type mysqlIter struct {
	rows  *dsql.Rows
	types []reflect.Type
}

var _ sql.RowIter = mysqlIter{}

// newMySQLIter returns a new mysqlIter.
func newMySQLIter(rows *dsql.Rows) mysqlIter {
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		panic(err)
	}
	types := make([]reflect.Type, len(columnTypes))
	for i, columnType := range columnTypes {
		scanType := columnType.ScanType()
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
		}
		types[i] = scanType
	}
	return mysqlIter{rows, types}
}

// Next implements the interface sql.RowIter.
func (m mysqlIter) Next(ctx *sql.Context) (sql.Row, error) {
	if m.rows.Next() {
		output := make(sql.Row, len(m.types))
		for i, typ := range m.types {
			output[i] = reflect.New(typ).Interface()
		}
		err := m.rows.Scan(output...)
		if err != nil {
			return nil, err
		}
		for i, val := range output {
			reflectVal := reflect.ValueOf(val)
			if reflectVal.IsNil() {
				output[i] = nil
			} else {
				output[i] = reflectVal.Elem().Interface()
				if byteSlice, ok := val.([]byte); ok {
					output[i] = string(byteSlice)
				}
			}
		}
		return output, nil
	}
	return nil, io.EOF
}

// Close implements the interface sql.RowIter.
func (m mysqlIter) Close(ctx *sql.Context) error {
	return m.rows.Close()
}
