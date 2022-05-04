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

package stats_tables

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/in_mem_table"
)

// TODO: this entire package is basically a copy of grant_tables; should probably refactor at some point
type statsTable struct {
	name string
	sch  sql.Schema
	data *in_mem_table.Data
}

var _ sql.Table = (*statsTable)(nil)
var _ sql.InsertableTable = (*statsTable)(nil)
var _ sql.UpdatableTable = (*statsTable)(nil)
var _ sql.DeletableTable = (*statsTable)(nil)
var _ sql.ReplaceableTable = (*statsTable)(nil)
var _ sql.TruncateableTable = (*statsTable)(nil)

// newGrantTable returns a new Grant Table with the given schema and keys.
func newStatsTable(
	name string,
	sch sql.Schema,
	entryRef in_mem_table.Entry,
	primaryKey in_mem_table.Key,
	secondaryKeys ...in_mem_table.Key,
) *statsTable {
	return &statsTable{
		name: name,
		sch:  sch,
		data: in_mem_table.NewData(entryRef, primaryKey, secondaryKeys),
	}
}

// Name implements the interface sql.Table.
func (s *statsTable) Name() string {
	return s.name
}

// String implements the interface sql.Table.
func (s *statsTable) String() string {
	return s.name
}

// Schema implements the interface sql.Table.
func (s *statsTable) Schema() sql.Schema {
	return s.sch.Copy()
}

// Partitions implements the interface sql.Table.
func (s *statsTable) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	return sql.PartitionsToPartitionIter(dummyPartition{}), nil
}

// PartitionRows implements the interface sql.Table.
func (s *statsTable) PartitionRows(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	return s.data.ToRowIter(ctx), nil
}

// Inserter implements the interface sql.InsertableTable.
func (s *statsTable) Inserter(ctx *sql.Context) sql.RowInserter {
	return in_mem_table.NewDataEditor(s.data)
}

// Updater implements the interface sql.UpdatableTable.
func (s *statsTable) Updater(ctx *sql.Context) sql.RowUpdater {
	return in_mem_table.NewDataEditor(s.data)
}

// Deleter implements the interface sql.DeletableTable.
func (s *statsTable) Deleter(ctx *sql.Context) sql.RowDeleter {
	return in_mem_table.NewDataEditor(s.data)
}

// Replacer implements the interface sql.ReplaceableTable.
func (s *statsTable) Replacer(ctx *sql.Context) sql.RowReplacer {
	return in_mem_table.NewDataEditor(s.data)
}

// Truncate implements the interface sql.TruncateableTable.
func (s *statsTable) Truncate(ctx *sql.Context) (int, error) {
	count := s.data.Count()
	s.data.Clear()
	return int(count), nil
}

// Data returns the in-memory table data for the grant table.
func (s *statsTable) Data() *in_mem_table.Data {
	return s.data
}
