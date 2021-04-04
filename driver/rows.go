// Copyright 2020-2021 Dolthub, Inc.
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

package driver

import (
	"database/sql/driver"
	"encoding/json"
	"reflect"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/vitess/go/vt/proto/query"
)

// Rows is an iterator over an executed query's results.
type Rows struct {
	ctx  *sql.Context
	cols sql.Schema
	rows sql.RowIter
}

// Columns returns the names of the columns. The number of
// columns of the result is inferred from the length of the
// slice. If a particular column name isn't known, an empty
// string should be returned for that entry.
func (r *Rows) Columns() []string {
	names := make([]string, len(r.cols))
	for i, col := range r.cols {
		names[i] = col.Name
	}
	return names
}

// Close closes the rows iterator.
func (r *Rows) Close() error {
	return r.rows.Close(r.ctx)
}

// Next is called to populate the next row of data into
// the provided slice. The provided slice will be the same
// size as the Columns() are wide.
//
// Next should return io.EOF when there are no more rows.
//
// The dest should not be written to outside of Next. Care
// should be taken when closing Rows not to modify
// a buffer held in dest.
func (r *Rows) Next(dest []driver.Value) error {
	row, err := r.rows.Next()
	if err != nil {
		return err
	}

	for i := range row {
		dest[i] = convertRowValue(r.cols[i], row[i])
	}
	return nil
}

func convertRowValue(col *sql.Column, v driver.Value) interface{} {
	switch col.Type.Type() {
	case query.Type_NULL_TYPE:
		return nil

	case query.Type_INT8, query.Type_INT16, query.Type_INT24, query.Type_INT32, query.Type_INT64,
		query.Type_UINT8, query.Type_UINT16, query.Type_UINT24, query.Type_UINT32, query.Type_UINT64,
		query.Type_FLOAT32, query.Type_FLOAT64:
		rv := reflect.ValueOf(v)
		switch rv.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			return rv.Int()
		case reflect.Float32, reflect.Float64:
			return rv.Float()
		case reflect.String:
			return rv.String()
		}

	case query.Type_JSON:
		b, err := json.Marshal(v)
		if err == nil {
			return b
		}
	}
	return v
}
