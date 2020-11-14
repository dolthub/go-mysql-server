// Copyright 2020 Liquidata, Inc.
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

package enginetest

import (
	"context"

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
)

type IndexDriverInitalizer func([]sql.Database) sql.IndexDriver

type MemoryHarness struct {
	name                   string
	parallelism            int
	numTablePartitions     int
	indexDriverInitializer IndexDriverInitalizer
	nativeIndexSupport     bool
	session                sql.Session
}

const testNumPartitions = 5

func NewMemoryHarness(name string, parallelism int, numTablePartitions int, useNativeIndexes bool, indexDriverInitalizer IndexDriverInitalizer) *MemoryHarness {
	return &MemoryHarness{
		name:                   name,
		numTablePartitions:     numTablePartitions,
		indexDriverInitializer: indexDriverInitalizer,
		parallelism:            parallelism,
		nativeIndexSupport:     useNativeIndexes,
	}
}

func NewDefaultMemoryHarness() *MemoryHarness {
	return NewMemoryHarness("default", 1, testNumPartitions, false, nil)
}

func NewSkippingMemoryHarness() *SkippingMemoryHarness {
	return &SkippingMemoryHarness{
		MemoryHarness: *NewDefaultMemoryHarness(),
	}
}

var _ Harness = (*MemoryHarness)(nil)
var _ IndexDriverHarness = (*MemoryHarness)(nil)
var _ IndexHarness = (*MemoryHarness)(nil)
var _ VersionedDBHarness = (*MemoryHarness)(nil)
var _ ForeignKeyHarness = (*MemoryHarness)(nil)
var _ SkippingHarness = (*SkippingMemoryHarness)(nil)

type SkippingMemoryHarness struct {
	MemoryHarness
}

func (s SkippingMemoryHarness) SkipQueryTest(query string) bool {
	return true
}

func (m *MemoryHarness) SupportsNativeIndexCreation() bool {
	return m.nativeIndexSupport
}

func (m *MemoryHarness) SupportsForeignKeys() bool {
	return true
}

func (m *MemoryHarness) Parallelism() int {
	return m.parallelism
}

func (m *MemoryHarness) NewContext() *sql.Context {
	if m.session == nil {
		m.session = NewBaseSession()
	}

	return sql.NewContext(
		context.Background(),
		sql.WithSession(m.session),
	)
}

func (m *MemoryHarness) NewTableAsOf(db sql.VersionedDatabase, name string, schema sql.Schema, asOf interface{}) sql.Table {
	table := memory.NewPartitionedTable(name, schema, m.numTablePartitions)
	if m.nativeIndexSupport {
		table.EnablePrimaryKeyIndexes()
	}
	db.(*memory.HistoryDatabase).AddTableAsOf(name, table, asOf)
	return table
}

func (m *MemoryHarness) SnapshotTable(db sql.VersionedDatabase, name string, asOf interface{}) error {
	// Nothing to do for this implementation: the NewTableAsOf method does all the work of creating the snapshot.
	return nil
}

func (m *MemoryHarness) IndexDriver(dbs []sql.Database) sql.IndexDriver {
	if m.indexDriverInitializer != nil {
		return m.indexDriverInitializer(dbs)
	}
	return nil
}

func (m *MemoryHarness) NewDatabase(name string) sql.Database {
	database := memory.NewHistoryDatabase(name)
	if m.nativeIndexSupport {
		database.EnablePrimaryKeyIndexes()
	}
	return database
}

func (m *MemoryHarness) NewTable(db sql.Database, name string, schema sql.Schema) (sql.Table, error) {
	table := memory.NewPartitionedTable(name, schema, m.numTablePartitions)
	if m.nativeIndexSupport {
		table.EnablePrimaryKeyIndexes()
	}
	db.(*memory.HistoryDatabase).AddTable(name, table)
	return table, nil
}
