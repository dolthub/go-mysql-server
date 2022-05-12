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

package enginetest

import (
	"context"
	"github.com/dolthub/go-mysql-server/sql/information_schema"
	"strings"
	"testing"

	sqle "github.com/dolthub/go-mysql-server"

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
)

type IndexDriverInitalizer func([]sql.Database) sql.IndexDriver

type MemoryHarness struct {
	name                   string
	parallelism            int
	numTablePartitions     int
	indexDriverInitializer IndexDriverInitalizer
	driver                 sql.IndexDriver
	nativeIndexSupport     bool
	skippedQueries         map[string]struct{}
	session                sql.Session
	checkpointTables       []*memory.Table
	dbOff                  []int
	dbNames                []string
	setupData              []string
}

func (m *MemoryHarness) RestoreCheckpoint(ctx *sql.Context, t *testing.T, e *sqle.Engine) *sqle.Engine {
	dbs := CreateTestData(t, m)
	engine := NewEngineWithDbs(t, m, dbs)
	return engine
}

func (m *MemoryHarness) NewEngineDepr(ctx *sql.Context, t *testing.T) *sqle.Engine {
	dbs := CreateTestData(t, m)
	engine := NewEngineWithDbs(t, m, dbs)
	err := m.copyDbs(ctx, dbs)
	if err != nil {
		panic(err)
	}
	return engine
}

func (m *MemoryHarness) copyDbs(ctx *sql.Context, dbs []sql.Database) error {
	checkpointTables := make([]*memory.Table, 0)
	dbOff := make([]int, len(dbs))
	dbNames := make([]string, len(dbs))
	for _, db := range dbs {
		names, err := db.GetTableNames(ctx)
		if err != nil {
			return err
		}
		dbNames = append(dbNames, db.Name())
		if len(dbOff) == 0 {
			dbOff = append(dbOff, len(names))
		} else {
			dbOff = append(dbOff, len(names)+dbOff[len(dbOff)-1])
		}
		for _, n := range names {
			t, _, err := db.GetTableInsensitive(ctx, n)
			if err != nil {
				return err
			}
			checkpointTables = append(checkpointTables, memory.CopyTable(t.(*memory.Table)))
		}
	}
	m.checkpointTables = checkpointTables
	m.dbOff = dbOff
	m.dbNames = dbNames
	return nil
}

func (m *MemoryHarness) InitializeIndexDriver(dbs []sql.Database) {
	if m.indexDriverInitializer != nil {
		m.driver = m.indexDriverInitializer(dbs)
	}
}

func (m *MemoryHarness) NewSession() *sql.Context {
	return m.NewContext()
}

const testNumPartitions = 5

func NewMemoryHarness(name string, parallelism int, numTablePartitions int, useNativeIndexes bool, indexDriverInitalizer IndexDriverInitalizer) *MemoryHarness {
	return &MemoryHarness{
		name:                   name,
		numTablePartitions:     numTablePartitions,
		indexDriverInitializer: indexDriverInitalizer,
		parallelism:            parallelism,
		nativeIndexSupport:     useNativeIndexes,
		skippedQueries:         make(map[string]struct{}),
	}
}

func NewDefaultMemoryHarness() *MemoryHarness {
	return NewMemoryHarness("default", 1, testNumPartitions, true, nil)
}

func (m *MemoryHarness) SkipQueryTest(query string) bool {
	_, ok := m.skippedQueries[strings.ToLower(query)]
	return ok
}

func (m *MemoryHarness) QueriesToSkip(queries ...string) {
	for _, query := range queries {
		m.skippedQueries[strings.ToLower(query)] = struct{}{}
	}
}

func NewSkippingMemoryHarness() *SkippingMemoryHarness {
	return &SkippingMemoryHarness{
		MemoryHarness: *NewDefaultMemoryHarness(),
	}
}

var _ Harness = (*MemoryHarness)(nil)
var _ IndexDriverHarness = (*MemoryHarness)(nil)
var _ IndexHarness = (*MemoryHarness)(nil)
var _ ForeignKeyHarness = (*MemoryHarness)(nil)
var _ KeylessTableHarness = (*MemoryHarness)(nil)
var _ ClientHarness = (*MemoryHarness)(nil)
var _ SkippingHarness = (*SkippingMemoryHarness)(nil)

type SkippingMemoryHarness struct {
	MemoryHarness
}

func (s SkippingMemoryHarness) SkipQueryTest(query string) bool {
	return true
}

func (m *MemoryHarness) SetSetup(setupData ...string) {
	m.setupData = setupData
	return
}

func (m *MemoryHarness) NewEngine(t *testing.T) (*sqle.Engine, error) {
	setup, err := newFileSetups(m.setupData...)
	if err != nil {
		return nil, err
	}
	return NewEngineWithSetup(t, m, setup)
}

func (m *MemoryHarness) SupportsNativeIndexCreation() bool {
	return m.nativeIndexSupport
}

func (m *MemoryHarness) SupportsForeignKeys() bool {
	return true
}

func (m *MemoryHarness) SupportsKeylessTables() bool {
	return true
}

func (m *MemoryHarness) Parallelism() int {
	return m.parallelism
}

func (m *MemoryHarness) NewContext() *sql.Context {
	if m.session == nil {
		m.session = NewBaseSession()
		if m.driver != nil {
			m.session.GetIndexRegistry().RegisterIndexDriver(m.driver)
		}
	}

	return sql.NewContext(
		context.Background(),
		sql.WithSession(m.session),
	)
}

func (m *MemoryHarness) NewContextWithClient(client sql.Client) *sql.Context {
	session := sql.NewBaseSessionWithClientServer("address", client, 1)

	return sql.NewContext(
		context.Background(),
		sql.WithSession(session),
	)
}

func (m *MemoryHarness) IndexDriver(dbs []sql.Database) sql.IndexDriver {
	if m.indexDriverInitializer != nil {
		return m.indexDriverInitializer(dbs)
	}
	return nil
}

func (m *MemoryHarness) NewDatabaseProvider(dbs ...sql.Database) sql.MutableDatabaseProvider {
	return memory.NewMemoryDBProvider(dbs...)
}

func (m *MemoryHarness) NewDatabase(name string) sql.Database {
	database := memory.NewHistoryDatabase(name)
	if m.nativeIndexSupport {
		database.EnablePrimaryKeyIndexes()
	}
	return database
}

func (m *MemoryHarness) NewDatabases(names ...string) []sql.Database {
	var dbs []sql.Database
	for _, name := range names {
		dbs = append(dbs, m.NewDatabase(name))
	}
	return dbs
}

func (m *MemoryHarness) NewTable(db sql.Database, name string, schema sql.PrimaryKeySchema) (sql.Table, error) {
	var fkColl *memory.ForeignKeyCollection
	if memDb, ok := db.(*memory.BaseDatabase); ok {
		fkColl = memDb.GetForeignKeyCollection()
	} else if memDb, ok := db.(*memory.Database); ok {
		fkColl = memDb.GetForeignKeyCollection()
	} else if memDb, ok := db.(*memory.HistoryDatabase); ok {
		fkColl = memDb.GetForeignKeyCollection()
	} else if memDb, ok := db.(*memory.ReadOnlyDatabase); ok {
		fkColl = memDb.GetForeignKeyCollection()
	}
	table := memory.NewPartitionedTable(name, schema, fkColl, m.numTablePartitions)
	if m.nativeIndexSupport {
		table.EnablePrimaryKeyIndexes()
	}

	if ro, ok := db.(memory.ReadOnlyDatabase); ok {
		ro.HistoryDatabase.AddTable(name, table)
	} else {
		db.(*memory.HistoryDatabase).AddTable(name, table)
	}
	return table, nil
}

type ExternalStoredProcedureMemoryHarness struct {
	*MemoryHarness
}

var _ Harness = ExternalStoredProcedureMemoryHarness{}

func NewExternalStoredProcedureMemoryHarness() *ExternalStoredProcedureMemoryHarness {
	return &ExternalStoredProcedureMemoryHarness{NewDefaultMemoryHarness()}
}

func (h ExternalStoredProcedureMemoryHarness) NewDatabase(name string) sql.Database {
	database := memory.NewExternalStoredProcedureDatabase(name)
	if h.nativeIndexSupport {
		database.EnablePrimaryKeyIndexes()
	}
	return database
}

func (h ExternalStoredProcedureMemoryHarness) NewDatabases(names ...string) []sql.Database {
	var dbs []sql.Database
	for _, name := range names {
		dbs = append(dbs, h.NewDatabase(name))
	}
	return dbs
}

type ReadOnlyMemoryHarness struct {
	MemoryHarness
}

var _ ReadOnlyDatabaseHarness = (*ReadOnlyMemoryHarness)(nil)

func (h *ReadOnlyMemoryHarness) NewReadOnlyDatabase(name string) sql.ReadOnlyDatabase {
	return memory.NewReadOnlyDatabase(name)
}

func (h *ReadOnlyMemoryHarness) NewEngine(t *testing.T) (*sqle.Engine, error) {
	dbs := make([]sql.Database, len(DefaultDatabases)+1)
	for i := range DefaultDatabases {
		dbs[i] = h.NewReadOnlyDatabase(DefaultDatabases[i])
	}
	dbs[len(DefaultDatabases)] = information_schema.NewInformationSchemaDatabase()

	pro := h.NewDatabaseProvider(dbs...)
	e := NewEngineWithProvider(t, h, pro)
	ctx := NewContext(h)

	setup, err := newFileSetups(h.setupData...)
	if err != nil {
		return nil, err
	}

	return RunEngineScripts(ctx, e, setup)
}

func NewVersionedMemoryHarness(name string, parallelism int, numTablePartitions int, useNativeIndexes bool, indexDriverInitalizer IndexDriverInitalizer) *VersionedMemoryHarness {
	return &VersionedMemoryHarness{
		MemoryHarness{
			name:                   name,
			numTablePartitions:     numTablePartitions,
			indexDriverInitializer: indexDriverInitalizer,
			parallelism:            parallelism,
			nativeIndexSupport:     useNativeIndexes,
			skippedQueries:         make(map[string]struct{}),
		},
	}
}

type VersionedMemoryHarness struct {
	MemoryHarness
}

var _ VersionedDBHarness = (*VersionedMemoryHarness)(nil)

func (h *VersionedMemoryHarness) NewReadOnlyDatabase(name string) sql.ReadOnlyDatabase {
	return memory.NewReadOnlyDatabase(name)
}

func (h *VersionedMemoryHarness) NewDatabases(names ...string) []sql.Database {
	dbs := make([]sql.Database, len(names))
	for i := range names {
		dbs[i] = h.NewReadOnlyDatabase(names[i])
	}
	return dbs
}

func (h *VersionedMemoryHarness) NewTableAsOf(db sql.VersionedDatabase, name string, schema sql.PrimaryKeySchema, asOf interface{}) sql.Table {
	var fkColl *memory.ForeignKeyCollection
	if memDb, ok := db.(*memory.HistoryDatabase); ok {
		fkColl = memDb.GetForeignKeyCollection()
	} else if memDb, ok := db.(*memory.ReadOnlyDatabase); ok {
		fkColl = memDb.GetForeignKeyCollection()
	}
	table := memory.NewPartitionedTable(name, schema, fkColl, h.numTablePartitions)
	if h.nativeIndexSupport {
		table.EnablePrimaryKeyIndexes()
	}
	if ro, ok := db.(memory.ReadOnlyDatabase); ok {
		ro.HistoryDatabase.AddTableAsOf(name, table, asOf)
	} else {
		db.(*memory.HistoryDatabase).AddTableAsOf(name, table, asOf)
	}
	return table
}

func (h *VersionedMemoryHarness) SnapshotTable(db sql.VersionedDatabase, name string, asOf interface{}) error {
	// Nothing to do for this implementation: the NewTableAsOf method does all the work of creating the snapshot.
	return nil
}
