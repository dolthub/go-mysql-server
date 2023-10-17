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
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/dolthub/go-mysql-server/memory"

	"github.com/dolthub/go-mysql-server/enginetest"
	"github.com/dolthub/go-mysql-server/enginetest/scriptgen/setup"
	"github.com/dolthub/go-mysql-server/sql"
)

// MySQLHarness is a harness for a local MySQL server. This will modify databases and tables as the tests see fit, which
// may delete pre-existing data. Ensure that the MySQL instance may freely be modified without worry.
type MySQLHarness struct {
	shim           *MySQLShim
	skippedQueries map[string]struct{}
	setupData      []setup.SetupScript
	session        sql.Session
}

// TODO: refactor to remove enginetest cycle
var _ enginetest.Harness = (*MySQLHarness)(nil)
var _ enginetest.IndexHarness = (*MySQLHarness)(nil)
var _ enginetest.ForeignKeyHarness = (*MySQLHarness)(nil)
var _ enginetest.KeylessTableHarness = (*MySQLHarness)(nil)
var _ enginetest.ClientHarness = (*MySQLHarness)(nil)
var _ enginetest.SkippingHarness = (*MySQLHarness)(nil)

func (m *MySQLHarness) Setup(setupData ...[]setup.SetupScript) {
	m.setupData = nil
	for i := range setupData {
		m.setupData = append(m.setupData, setupData[i]...)
	}
	return
}

func (m *MySQLHarness) NewEngine(t *testing.T) (enginetest.QueryEngine, error) {
	return enginetest.NewEngine(t, m, m.Provider(), m.setupData, memory.NewStatsProv())
}

func (m *MySQLHarness) NewContextWithClient(client sql.Client) *sql.Context {
	session := sql.NewBaseSessionWithClientServer("address", client, 1)
	return sql.NewContext(
		context.Background(),
		sql.WithSession(session),
	)
}

func (m *MySQLHarness) Cleanup() error {
	return nil
}

// MySQLDatabase represents a database for a local MySQL server.
type MySQLDatabase struct {
	harness *MySQLHarness
	dbName  string
}

// MySQLTable represents a table for a local MySQL server.
type MySQLTable struct {
	harness   *MySQLHarness
	tableName string
}

// NewMySQLHarness returns a new MySQLHarness.
func NewMySQLHarness(user string, password string, host string, port int) (*MySQLHarness, error) {
	shim, err := NewMySQLShim(user, password, host, port)
	if err != nil {
		return nil, err
	}
	return &MySQLHarness{shim, make(map[string]struct{}), nil, nil}, nil
}

// Parallelism implements the interface Harness.
func (m *MySQLHarness) Parallelism() int {
	return 1
}

// NewDatabase implements the interface Harness.
func (m *MySQLHarness) NewDatabase(name string) sql.Database {
	return m.NewDatabases(name)[0]
}

// NewDatabases implements the interface Harness.
func (m *MySQLHarness) NewDatabases(names ...string) []sql.Database {
	var dbs []sql.Database
	ctx := sql.NewEmptyContext()
	for _, name := range names {
		_ = m.shim.DropDatabase(ctx, name)
		err := m.shim.CreateDatabase(ctx, name)
		if err != nil {
			panic(err)
		}
		db, err := m.shim.Database(ctx, name)
		if err != nil {
			panic(err)
		}
		dbs = append(dbs, db)
	}
	return dbs
}

// NewDatabaseProvider implements the interface Harness.
func (m *MySQLHarness) NewDatabaseProvider() sql.MutableDatabaseProvider {
	return m.shim
}

func (m *MySQLHarness) Provider() sql.MutableDatabaseProvider {
	return m.shim
}

// NewTable implements the interface Harness.
func (m *MySQLHarness) NewTable(db sql.Database, name string, schema sql.PrimaryKeySchema) (sql.Table, error) {
	ctx := sql.NewEmptyContext()
	err := db.(sql.TableCreator).CreateTable(ctx, name, schema, sql.Collation_Default)
	if err != nil {
		return nil, err
	}
	tbl, ok, err := db.GetTableInsensitive(ctx, name)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("successfully created table `%s` but could not retrieve", name)
	}
	return tbl, nil
}

// NewContext implements the interface Harness.
func (m *MySQLHarness) NewContext() *sql.Context {
	if m.session == nil {
		m.session = enginetest.NewBaseSession()
	}

	return sql.NewContext(
		context.Background(),
		sql.WithSession(m.session),
	)
}

// SkipQueryTest implements the interface SkippingHarness.
func (m *MySQLHarness) SkipQueryTest(query string) bool {
	_, ok := m.skippedQueries[strings.ToLower(query)]
	return ok
}

// QueriesToSkip adds queries that should be skipped.
func (m *MySQLHarness) QueriesToSkip(queries ...string) {
	for _, query := range queries {
		m.skippedQueries[strings.ToLower(query)] = struct{}{}
	}
}

// SupportsNativeIndexCreation implements the interface IndexHarness.
func (m *MySQLHarness) SupportsNativeIndexCreation() bool {
	return true
}

// SupportsForeignKeys implements the interface ForeignKeyHarness.
func (m *MySQLHarness) SupportsForeignKeys() bool {
	return true
}

// SupportsKeylessTables implements the interface KeylessTableHarness.
func (m *MySQLHarness) SupportsKeylessTables() bool {
	return true
}

// Close closes the connection. This will drop all databases created and accessed during the tests.
func (m *MySQLHarness) Close() {
	m.shim.Close()
}
