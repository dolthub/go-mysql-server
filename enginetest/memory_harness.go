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
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/dolthub/vitess/go/mysql"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/enginetest/scriptgen/setup"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/mysql_db"
)

const testNumPartitions = 5

type IndexDriverInitializer func([]sql.Database) sql.IndexDriver

type MemoryHarness struct {
	name                      string
	parallelism               int
	numTablePartitions        int
	readonly                  bool
	provider                  *memory.DbProvider
	indexDriverInitializer    IndexDriverInitializer
	driver                    sql.IndexDriver
	nativeIndexSupport        bool
	skippedQueries            map[string]struct{}
	session                   sql.Session
	retainSession             bool
	setupData                 []setup.SetupScript
	externalProcedureRegistry sql.ExternalStoredProcedureRegistry
	server                    bool
	mu                        *sync.Mutex
}

var _ Harness = (*MemoryHarness)(nil)
var _ IndexDriverHarness = (*MemoryHarness)(nil)
var _ IndexHarness = (*MemoryHarness)(nil)
var _ VersionedDBHarness = (*MemoryHarness)(nil)
var _ ReadOnlyDatabaseHarness = (*MemoryHarness)(nil)
var _ ForeignKeyHarness = (*MemoryHarness)(nil)
var _ KeylessTableHarness = (*MemoryHarness)(nil)
var _ ClientHarness = (*MemoryHarness)(nil)
var _ ServerHarness = (*MemoryHarness)(nil)
var _ sql.ExternalStoredProcedureProvider = (*MemoryHarness)(nil)

func NewMemoryHarness(name string, parallelism int, numTablePartitions int, useNativeIndexes bool, driverInitalizer IndexDriverInitializer) *MemoryHarness {
	externalProcedureRegistry := sql.NewExternalStoredProcedureRegistry()
	for _, esp := range memory.ExternalStoredProcedures {
		externalProcedureRegistry.Register(esp)
	}

	return &MemoryHarness{
		name:                      name,
		numTablePartitions:        numTablePartitions,
		indexDriverInitializer:    driverInitalizer,
		parallelism:               parallelism,
		nativeIndexSupport:        useNativeIndexes,
		skippedQueries:            make(map[string]struct{}),
		externalProcedureRegistry: externalProcedureRegistry,
		mu:                        &sync.Mutex{},
	}
}

func NewDefaultMemoryHarness() *MemoryHarness {
	return NewMemoryHarness("default", 1, testNumPartitions, true, nil)
}

func NewReadOnlyMemoryHarness() *MemoryHarness {
	h := NewMemoryHarness("default", 1, testNumPartitions, true, nil)
	h.readonly = true
	return h
}

func (m *MemoryHarness) SessionBuilder() server.SessionBuilder {
	return func(ctx context.Context, c *mysql.Conn, addr string) (sql.Session, error) {
		host := ""
		user := ""
		mysqlConnectionUser, ok := c.UserData.(mysql_db.MysqlConnectionUser)
		if ok {
			host = mysqlConnectionUser.Host
			user = mysqlConnectionUser.User
		}
		client := sql.Client{Address: host, User: user, Capabilities: c.Capabilities}
		baseSession := sql.NewBaseSessionWithClientServer(addr, client, c.ConnectionID)
		return memory.NewSession(baseSession, m.getProvider()), nil
	}
}

// ExternalStoredProcedure implements the sql.ExternalStoredProcedureProvider interface
func (m *MemoryHarness) ExternalStoredProcedure(_ *sql.Context, name string, numOfParams int) (*sql.ExternalStoredProcedureDetails, error) {
	return m.externalProcedureRegistry.LookupByNameAndParamCount(name, numOfParams)
}

// ExternalStoredProcedures implements the sql.ExternalStoredProcedureProvider interface
func (m *MemoryHarness) ExternalStoredProcedures(_ *sql.Context, name string) ([]sql.ExternalStoredProcedureDetails, error) {
	return m.externalProcedureRegistry.LookupByName(name)
}

func (m *MemoryHarness) InitializeIndexDriver(dbs []sql.Database) {
	if m.indexDriverInitializer != nil {
		m.driver = m.indexDriverInitializer(dbs)
	}
}

func (m *MemoryHarness) NewSession() *sql.Context {
	m.session = m.newSession()
	return m.NewContext()
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

func (m *MemoryHarness) UseServer() {
	m.server = true
}

type SkippingMemoryHarness struct {
	MemoryHarness
}

var _ SkippingHarness = (*SkippingMemoryHarness)(nil)

func NewSkippingMemoryHarness() *SkippingMemoryHarness {
	return &SkippingMemoryHarness{
		MemoryHarness: *NewDefaultMemoryHarness(),
	}
}

func (s SkippingMemoryHarness) SkipQueryTest(query string) bool {
	return true
}

func (m *MemoryHarness) Setup(setupData ...[]setup.SetupScript) {
	m.setupData = nil
	for i := range setupData {
		m.setupData = append(m.setupData, setupData[i]...)
	}
	return
}

func (m *MemoryHarness) NewEngine(t *testing.T) (QueryEngine, error) {
	if !m.retainSession {
		m.session = nil
		m.provider = nil
	}
	engine, err := NewEngine(t, m, m.getProvider(), m.setupData)
	if err != nil {
		return nil, err
	}

	if m.server {
		return NewServerQueryEngine(t, engine)
	}

	return engine, nil
}

func (m *MemoryHarness) NewTableAsOf(db sql.VersionedDatabase, name string, schema sql.PrimaryKeySchema, asOf interface{}) sql.Table {
	var fkColl *memory.ForeignKeyCollection
	var baseDb *memory.BaseDatabase
	if memDb, ok := db.(*memory.HistoryDatabase); ok {
		fkColl = memDb.GetForeignKeyCollection()
		baseDb = memDb.BaseDatabase
	} else if memDb, ok := db.(*memory.ReadOnlyDatabase); ok {
		fkColl = memDb.GetForeignKeyCollection()
		baseDb = memDb.BaseDatabase
	} else {
		panic(fmt.Sprintf("unexpected database type %T", db))
	}
	table := memory.NewPartitionedTableRevision(baseDb, name, schema, fkColl, m.numTablePartitions)
	if m.nativeIndexSupport {
		table.EnablePrimaryKeyIndexes()
	}
	if ro, ok := db.(memory.ReadOnlyDatabase); ok {
		ro.HistoryDatabase.AddTableAsOf(name, table, asOf)
	} else {
		db.(*memory.HistoryDatabase).AddTableAsOf(name, table, asOf)
	}

	m.retainSession = true

	return table
}

func (m *MemoryHarness) SnapshotTable(db sql.VersionedDatabase, name string, asOf interface{}) error {
	// Nothing to do for this implementation: the NewTableAsOf method does all the work of creating the snapshot.
	return nil
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
		m.session = m.newSession()
	}

	return sql.NewContext(
		context.Background(),
		sql.WithSession(m.session),
	)
}

func (m *MemoryHarness) newSession() *memory.Session {
	baseSession := NewBaseSession()
	session := memory.NewSession(baseSession, m.getProvider())
	if m.driver != nil {
		session.GetIndexRegistry().RegisterIndexDriver(m.driver)
	}
	return session
}

func (m *MemoryHarness) NewContextWithClient(client sql.Client) *sql.Context {
	baseSession := sql.NewBaseSessionWithClientServer("address", client, 1)

	return sql.NewContext(
		context.Background(),
		sql.WithSession(memory.NewSession(baseSession, m.getProvider())),
	)
}

func (m *MemoryHarness) IndexDriver(dbs []sql.Database) sql.IndexDriver {
	if m.indexDriverInitializer != nil {
		return m.indexDriverInitializer(dbs)
	}
	return nil
}

func (m *MemoryHarness) newDatabase(name string) sql.Database {
	ctx := m.NewContext()

	err := m.getProvider().CreateDatabase(ctx, name)
	if err != nil {
		panic(err)
	}

	db, _ := m.getProvider().Database(ctx, name)
	return db
}

func (m *MemoryHarness) getProvider() *memory.DbProvider {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.provider == nil {
		m.provider = m.NewDatabaseProvider().(*memory.DbProvider)
	}

	return m.provider
}

func (m *MemoryHarness) NewDatabaseProvider() sql.MutableDatabaseProvider {
	return memory.NewDBProviderWithOpts(
		memory.NativeIndexProvider(m.nativeIndexSupport),
		memory.HistoryProvider(true))
}

func (m *MemoryHarness) Provider() *memory.DbProvider {
	return m.getProvider()
}

func (m *MemoryHarness) NewDatabases(names ...string) []sql.Database {
	var dbs []sql.Database
	for _, name := range names {
		dbs = append(dbs, m.newDatabase(name))
	}
	return dbs
}

func (m *MemoryHarness) NewReadOnlyEngine(provider sql.DatabaseProvider) (QueryEngine, error) {
	dbs := make([]sql.Database, 0)
	for _, db := range provider.AllDatabases(m.NewContext()) {
		dbs = append(dbs, memory.ReadOnlyDatabase{HistoryDatabase: db.(*memory.HistoryDatabase)})
	}

	readOnlyProvider := memory.NewDBProviderWithOpts(memory.WithDbsOption(dbs))
	m.provider = readOnlyProvider.(*memory.DbProvider)

	return NewEngineWithProvider(nil, m, readOnlyProvider), nil
}

func (m *MemoryHarness) ValidateEngine(ctx *sql.Context, e *sqle.Engine) error {
	return sanityCheckEngine(ctx, e)
}

func sanityCheckEngine(ctx *sql.Context, e *sqle.Engine) (err error) {
	for _, db := range e.Analyzer.Catalog.AllDatabases(ctx) {
		if err = sanityCheckDatabase(ctx, db); err != nil {
			return err
		}
	}
	return
}

func sanityCheckDatabase(ctx *sql.Context, db sql.Database) error {
	names, err := db.GetTableNames(ctx)
	if err != nil {
		return err
	}
	for _, name := range names {
		t, ok, err := db.GetTableInsensitive(ctx, name)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("expected to find table %s", name)
		}
		if t.Name() != name {
			return fmt.Errorf("unexpected table name (%s !=  %s)", name, t.Name())
		}
	}
	return nil
}
