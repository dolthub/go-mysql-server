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
	"io"

	"github.com/gabereiser/go-mysql-server/sql"
	"github.com/gabereiser/go-mysql-server/sql/in_mem_table"
)

// mysqlTableShim acts as a kind of "view" on top of another Grant Table. It does not use its own underlying data
// storage, and instead it transforms the data of another table.
//
// To give an example, in MySQL global and database-level privileges are stored in two separate tables. This could be
// duplicated, or we could keep all privileges in one data store, with both tables referencing that same store. This
// means that, internally (since we don't actually care about whether they're tables or not) we can access that singular
// store to easily find and use our data, and externally show multiple tables which each manipulate that data in a
// unique way.
type mysqlTableShim struct {
	name      string
	sch       sql.Schema
	original  *mysqlTable
	converter in_mem_table.DataEditorConverter
}

var _ sql.Table = (*mysqlTableShim)(nil)
var _ sql.InsertableTable = (*mysqlTableShim)(nil)
var _ sql.UpdatableTable = (*mysqlTableShim)(nil)
var _ sql.DeletableTable = (*mysqlTableShim)(nil)
var _ sql.ReplaceableTable = (*mysqlTableShim)(nil)

// newMySQLTableShim returns a new shim for the given Grant Table with the given schema.
func newMySQLTableShim(
	name string,
	sch sql.Schema,
	original *mysqlTable,
	converter in_mem_table.DataEditorConverter,
) *mysqlTableShim {
	return &mysqlTableShim{
		name:      name,
		sch:       sch,
		original:  original,
		converter: converter,
	}
}

// Name implements the interface sql.Table.
func (g *mysqlTableShim) Name() string {
	return g.name
}

// String implements the interface sql.Table.
func (g *mysqlTableShim) String() string {
	return g.name
}

// Schema implements the interface sql.Table.
func (g *mysqlTableShim) Schema() sql.Schema {
	return g.sch.Copy()
}

// Collation implements the interface sql.Table.
func (g *mysqlTableShim) Collation() sql.CollationID {
	return g.original.Collation()
}

// Partitions implements the interface sql.Table.
func (g *mysqlTableShim) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	return sql.PartitionsToPartitionIter(dummyPartition{}), nil
}

// PartitionRows implements the interface sql.Table.
func (g *mysqlTableShim) PartitionRows(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	return &mysqlTableShimRowIter{
		buffer:    nil,
		entries:   g.original.data.ToSlice(ctx),
		converter: g.converter,
	}, nil
}

// Inserter implements the interface sql.InsertableTable.
func (g *mysqlTableShim) Inserter(ctx *sql.Context) sql.RowInserter {
	return in_mem_table.NewDataEditorView(g.original.data, g.converter)
}

// Updater implements the interface sql.UpdatableTable.
func (g *mysqlTableShim) Updater(ctx *sql.Context) sql.RowUpdater {
	return in_mem_table.NewDataEditorView(g.original.data, g.converter)
}

// Deleter implements the interface sql.DeletableTable.
func (g *mysqlTableShim) Deleter(ctx *sql.Context) sql.RowDeleter {
	return in_mem_table.NewDataEditorView(g.original.data, g.converter)
}

// Replacer implements the interface sql.ReplaceableTable.
func (g *mysqlTableShim) Replacer(ctx *sql.Context) sql.RowReplacer {
	return in_mem_table.NewDataEditorView(g.original.data, g.converter)
}

// mysqlTableShimRowIter handles the translation from the original row iterator output to the output expected from this
// table.
type mysqlTableShimRowIter struct {
	buffer    []sql.Row
	entries   []in_mem_table.Entry
	converter in_mem_table.DataEditorConverter
}

var _ sql.RowIter = (*mysqlTableShimRowIter)(nil)

// Next implements the interface sql.RowIter.
func (g *mysqlTableShimRowIter) Next(ctx *sql.Context) (sql.Row, error) {
	for {
		if len(g.buffer) == 0 {
			if len(g.entries) == 0 {
				return nil, io.EOF
			}
			nextEntry := g.entries[0]
			g.entries = g.entries[1:]
			rows, err := g.converter.EntryToRows(ctx, nextEntry)
			if err != nil {
				return nil, err
			}
			g.buffer = rows
		} else {
			nextRow := g.buffer[0]
			g.buffer = g.buffer[1:]
			return nextRow, nil
		}
	}
}

// Close implements the interface sql.RowIter.
func (g *mysqlTableShimRowIter) Close(ctx *sql.Context) error {
	g.buffer = nil
	g.entries = nil
	return nil
}
