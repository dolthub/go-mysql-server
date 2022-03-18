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
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
)

// tableEditor is used as a sql.TableEditor.
type tableEditor struct {
	table Table
	sch   sql.Schema
}

var _ sql.TableEditor = (*tableEditor)(nil)
var _ sql.RowInserter = (*tableEditor)(nil)
var _ sql.RowUpdater = (*tableEditor)(nil)
var _ sql.RowDeleter = (*tableEditor)(nil)
var _ sql.RowReplacer = (*tableEditor)(nil)
var _ sql.ForeignKeyUpdater = (*tableEditor)(nil)

// StatementBegin implements the interface sql.TableEditor.
func (t *tableEditor) StatementBegin(ctx *sql.Context) {
	err := t.table.db.shim.Exec(t.table.db.name, "BEGIN;")
	if err != nil {
		panic(err)
	}
}

// DiscardChanges implements the interface sql.TableEditor.
func (t *tableEditor) DiscardChanges(ctx *sql.Context, errorEncountered error) error {
	return t.table.db.shim.Exec(t.table.db.name, "ROLLBACK;")
}

// StatementComplete implements the interface sql.TableEditor.
func (t *tableEditor) StatementComplete(ctx *sql.Context) error {
	return t.table.db.shim.Exec(t.table.db.name, "COMMIT;")
}

// Insert implements the interface sql.RowInserter.
func (t *tableEditor) Insert(ctx *sql.Context, row sql.Row) error {
	sb := strings.Builder{}
	for i, val := range row {
		if i != 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(t.rowValToString(val))
	}
	return t.table.db.shim.Exec(t.table.db.name, fmt.Sprintf("INSERT INTO `%s` VALUES (%s);", t.table.name, sb.String()))
}

// Update implements the interface sql.RowUpdater.
func (t *tableEditor) Update(ctx *sql.Context, old sql.Row, new sql.Row) error {
	err := t.Delete(ctx, old)
	if err != nil {
		return err
	}
	return t.Insert(ctx, new)
}

// Delete implements the interface sql.RowDeleter.
func (t *tableEditor) Delete(ctx *sql.Context, row sql.Row) error {
	if len(row) != len(t.sch) {
		return fmt.Errorf("expected `%d` values but got `%d` for DELETE", len(t.sch), len(row))
	}
	sb := strings.Builder{}
	for i, val := range row {
		if i != 0 {
			sb.WriteString(" AND")
		}
		sb.WriteString(fmt.Sprintf(" `%s` = %s", t.sch[i].Name, t.rowValToString(val)))
	}
	return t.table.db.shim.Exec(t.table.db.name, fmt.Sprintf("DELETE FROM `%s` WHERE%s;", t.table.name, sb.String()))
}

// WithIndexLookup implements the interface sql.ForeignKeyUpdater.
func (t *tableEditor) WithIndexLookup(lookup sql.IndexLookup) sql.Table {
	// Not sure what to do here, will worry about that when the shim fully supports foreign keys
	return nil
}

// Close implements the interface sql.TableEditor.
func (t *tableEditor) Close(ctx *sql.Context) error {
	return nil
}

// rowValToString converts the given value from a sql.Row into a string representation.
func (t *tableEditor) rowValToString(val interface{}) string {
	switch val := val.(type) {
	case bool:
		if val {
			return "true"
		}
		return "false"
	case int:
		return strconv.FormatInt(int64(val), 10)
	case int8:
		return strconv.FormatInt(int64(val), 10)
	case int16:
		return strconv.FormatInt(int64(val), 10)
	case int32:
		return strconv.FormatInt(int64(val), 10)
	case int64:
		return strconv.FormatInt(val, 10)
	case uint:
		return strconv.FormatUint(uint64(val), 10)
	case uint8:
		return strconv.FormatUint(uint64(val), 10)
	case uint16:
		return strconv.FormatUint(uint64(val), 10)
	case uint32:
		return strconv.FormatUint(uint64(val), 10)
	case uint64:
		return strconv.FormatUint(val, 10)
	case float32:
		return strconv.FormatFloat(float64(val), 'g', -1, 32)
	case float64:
		return strconv.FormatFloat(val, 'g', -1, 64)
	case string:
		return "'" + strings.ReplaceAll(val, "'", `\'`) + "'"
	case []byte:
		return "'" + strings.ReplaceAll(string(val), "'", `\'`) + "'"
	case time.Time:
		return "'" + val.Format("2006-01-02T15:04:05") + "'"
	case nil:
		return "NULL"
	default:
		panic(fmt.Errorf("unknown type: %T", val))
	}
}
