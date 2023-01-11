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

package mysql_db

import (
	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/in_mem_table"
)

var errPrimaryKeyUnknownEntry = errors.NewKind("the primary key for the `%s` table was given an unknown entry")
var errPrimaryKeyUnknownSchema = errors.NewKind("the primary key for the `%s` table was given a row belonging to an unknown schema")

type mysqlTable struct {
	name string
	sch  sql.Schema
	data *in_mem_table.Data
	db   *MySQLDb
}

var _ sql.Table = (*mysqlTable)(nil)
var _ sql.InsertableTable = (*mysqlTable)(nil)
var _ sql.UpdatableTable = (*mysqlTable)(nil)
var _ sql.DeletableTable = (*mysqlTable)(nil)
var _ sql.ReplaceableTable = (*mysqlTable)(nil)
var _ sql.TruncateableTable = (*mysqlTable)(nil)

// newMySQLTable returns a new MySQL Table with the given schema and keys.
func newMySQLTable(
	name string,
	sch sql.Schema,
	db *MySQLDb,
	entryRef in_mem_table.Entry,
	primaryKey in_mem_table.Key,
	secondaryKeys ...in_mem_table.Key,
) *mysqlTable {
	return &mysqlTable{
		name: name,
		db:   db,
		sch:  sch,
		data: in_mem_table.NewData(entryRef, primaryKey, secondaryKeys),
	}
}

// Name implements the interface sql.Table.
func (t *mysqlTable) Name() string {
	return t.name
}

// String implements the interface sql.Table.
func (t *mysqlTable) String() string {
	return t.name
}

// Schema implements the interface sql.Table.
func (t *mysqlTable) Schema() sql.Schema {
	return t.sch.Copy()
}

// Collation implements the interface sql.Table.
func (t *mysqlTable) Collation() sql.CollationID {
	return sql.Collation_utf8mb3_bin
}

// Partitions implements the interface sql.Table.
func (t *mysqlTable) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	return sql.PartitionsToPartitionIter(dummyPartition{}), nil
}

// PartitionRows implements the interface sql.Table.
func (t *mysqlTable) PartitionRows(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	return t.data.ToRowIter(ctx), nil
}

// Inserter implements the interface sql.InsertableTable.
func (t *mysqlTable) Inserter(ctx *sql.Context) sql.RowInserter {
	return cacheClearingDataEditor{db: t.db, editor: in_mem_table.NewDataEditor(t.data)}
}

// Updater implements the interface sql.UpdatableTable.
func (t *mysqlTable) Updater(ctx *sql.Context) sql.RowUpdater {
	return cacheClearingDataEditor{db: t.db, editor: in_mem_table.NewDataEditor(t.data)}
}

// Deleter implements the interface sql.DeletableTable.
func (t *mysqlTable) Deleter(ctx *sql.Context) sql.RowDeleter {
	return cacheClearingDataEditor{db: t.db, editor: in_mem_table.NewDataEditor(t.data)}
}

// Replacer implements the interface sql.ReplaceableTable.
func (t *mysqlTable) Replacer(ctx *sql.Context) sql.RowReplacer {
	return cacheClearingDataEditor{db: t.db, editor: in_mem_table.NewDataEditor(t.data)}
}

// Truncate implements the interface sql.TruncateableTable.
func (t *mysqlTable) Truncate(ctx *sql.Context) (int, error) {
	if t.db != nil {
		t.db.updateCounter++
	}
	count := t.data.Count()
	t.data.Clear()
	return int(count), nil
}

// Data returns the in-memory table data for the grant table.
func (t *mysqlTable) Data() *in_mem_table.Data {
	return t.data
}

// cacheClearingDataEditor is a simple wrapper around a DataEditor that clears the mysql DB's user cache on every update.
type cacheClearingDataEditor struct {
	db     *MySQLDb
	editor *in_mem_table.DataEditor
}

func (c cacheClearingDataEditor) Insert(ctx *sql.Context, row sql.Row) error {
	if c.db != nil {
		c.db.updateCounter++
	}
	return c.editor.Insert(ctx, row)
}

func (c cacheClearingDataEditor) Update(ctx *sql.Context, old sql.Row, new sql.Row) error {
	if c.db != nil {
		c.db.updateCounter++
	}
	return c.editor.Update(ctx, old, new)
}

func (c cacheClearingDataEditor) Delete(ctx *sql.Context, row sql.Row) error {
	if c.db != nil {
		c.db.updateCounter++
	}
	return c.editor.Delete(ctx, row)
}

func (c cacheClearingDataEditor) StatementBegin(ctx *sql.Context) {
	c.editor.StatementBegin(ctx)
}

func (c cacheClearingDataEditor) DiscardChanges(ctx *sql.Context, errorEncountered error) error {
	return c.editor.DiscardChanges(ctx, errorEncountered)
}

func (c cacheClearingDataEditor) StatementComplete(ctx *sql.Context) error {
	return c.editor.StatementComplete(ctx)
}

func (c cacheClearingDataEditor) Close(ctx *sql.Context) error {
	return c.editor.Close(ctx)
}
