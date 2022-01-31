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

package grant_tables

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/in_mem_table"
)

type grantTable struct {
	name string
	sch  sql.Schema
	data *in_mem_table.Data
}

var _ sql.Table = (*grantTable)(nil)
var _ sql.InsertableTable = (*grantTable)(nil)
var _ sql.UpdatableTable = (*grantTable)(nil)
var _ sql.DeletableTable = (*grantTable)(nil)
var _ sql.ReplaceableTable = (*grantTable)(nil)
var _ sql.TruncateableTable = (*grantTable)(nil)

// newGrantTable returns a new Grant Table with the given schema and keys.
func newGrantTable(
	name string,
	sch sql.Schema,
	entryRef in_mem_table.Entry,
	primaryKey in_mem_table.Key,
	secondaryKeys ...in_mem_table.Key,
) *grantTable {
	return &grantTable{
		name: name,
		sch:  sch,
		data: in_mem_table.NewData(entryRef, primaryKey, secondaryKeys),
	}
}

// Name implements the interface sql.Table.
func (g *grantTable) Name() string {
	return g.name
}

// String implements the interface sql.Table.
func (g *grantTable) String() string {
	return g.name
}

// Schema implements the interface sql.Table.
func (g *grantTable) Schema() sql.Schema {
	return g.sch.Copy()
}

// Partitions implements the interface sql.Table.
func (g *grantTable) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	return sql.PartitionsToPartitionIter(dummyPartition{}), nil
}

// PartitionRows implements the interface sql.Table.
func (g *grantTable) PartitionRows(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	return g.data.ToRowIter(ctx), nil
}

// Inserter implements the interface sql.InsertableTable.
func (g *grantTable) Inserter(ctx *sql.Context) sql.RowInserter {
	return in_mem_table.NewDataEditor(g.data)
}

// Updater implements the interface sql.UpdatableTable.
func (g *grantTable) Updater(ctx *sql.Context) sql.RowUpdater {
	return in_mem_table.NewDataEditor(g.data)
}

// Deleter implements the interface sql.DeletableTable.
func (g *grantTable) Deleter(ctx *sql.Context) sql.RowDeleter {
	return in_mem_table.NewDataEditor(g.data)
}

// Replacer implements the interface sql.ReplaceableTable.
func (g *grantTable) Replacer(ctx *sql.Context) sql.RowReplacer {
	return in_mem_table.NewDataEditor(g.data)
}

// Truncate implements the interface sql.TruncateableTable.
func (g *grantTable) Truncate(ctx *sql.Context) (int, error) {
	count := g.data.Count()
	g.data.Clear()
	return int(count), nil
}

// Data returns the in-memory table data for the grant table.
func (g *grantTable) Data() *in_mem_table.Data {
	return g.data
}
