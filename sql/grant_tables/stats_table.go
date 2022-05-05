package grant_tables

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/in_mem_table"
)

// TODO: there doesn't seem to be any difference between this and grant tables, probably need a refactor
// TODO: probably make some sort of interface for this and grant tables
type colStatsTable struct {
	name string
	sch  sql.Schema
	data *in_mem_table.Data
}

// newStatsTable returns a new Grant Table with the given schema and keys.
func newStatsTable(
	name string,
	sch sql.Schema,
	entryRef in_mem_table.Entry,
	primaryKey in_mem_table.Key,
	secondaryKeys ...in_mem_table.Key,
) *colStatsTable {
	return &colStatsTable{
		name: name,
		sch:  sch,
		data: in_mem_table.NewData(entryRef, primaryKey, secondaryKeys),
	}
}

// Name implements the interface sql.Table.
func (s *colStatsTable) Name() string {
	return s.name
}

// String implements the interface sql.Table.
func (s *colStatsTable) String() string {
	return s.name
}

// Schema implements the interface sql.Table.
func (s *colStatsTable) Schema() sql.Schema {
	return s.sch.Copy()
}

// Partitions implements the interface sql.Table.
func (s *colStatsTable) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	return sql.PartitionsToPartitionIter(dummyPartition{}), nil
}

// PartitionRows implements the interface sql.Table.
func (s *colStatsTable) PartitionRows(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	return s.data.ToRowIter(ctx), nil
}

// Inserter implements the interface sql.InsertableTable.
func (s *colStatsTable) Inserter(ctx *sql.Context) sql.RowInserter {
	return in_mem_table.NewDataEditor(s.data)
}

// Updater implements the interface sql.UpdatableTable.
func (s *colStatsTable) Updater(ctx *sql.Context) sql.RowUpdater {
	return in_mem_table.NewDataEditor(s.data)
}

// Deleter implements the interface sql.DeletableTable.
func (s *colStatsTable) Deleter(ctx *sql.Context) sql.RowDeleter {
	return in_mem_table.NewDataEditor(s.data)
}

// Replacer implements the interface sql.ReplaceableTable.
func (s *colStatsTable) Replacer(ctx *sql.Context) sql.RowReplacer {
	return in_mem_table.NewDataEditor(s.data)
}

// Truncate implements the interface sql.TruncateableTable.
func (s *colStatsTable) Truncate(ctx *sql.Context) (int, error) {
	count := s.data.Count()
	s.data.Clear()
	return int(count), nil
}

// Data returns the in-memory table data for the grant table.
func (s *colStatsTable) Data() *in_mem_table.Data {
	return s.data
}
